/**
 * Token bucket rate limiter, per gateway.
 *
 * Why token bucket over fixed-window or sliding-log:
 * - Fixed window allows 2x burst at window boundaries (classic thundering-herd bug).
 * - Sliding log is accurate but O(n) memory per gateway at 50K+/hr — expensive.
 * - Token bucket is O(1) memory, allows short bursts (useful for payment spikes),
 *   and smooths out to a steady refill rate that matches what gateways actually
 *   advertise (e.g. "100 req/sec sustained, burst to 150").
 *
 * "Adaptive" behavior: on sustained failures from a gateway, we shrink the
 * bucket capacity and refill rate temporarily (a soft version of the circuit
 * breaker) so we back off before the breaker fully trips.
 */
export class TokenBucket {
  private tokens: number;
  private lastRefillMs: number;
  private currentCapacity: number;
  private currentRefillPerSec: number;

  constructor(
    private readonly baseCapacity: number,
    private readonly baseRefillPerSec: number,
    private readonly minCapacityFraction = 0.2, // never throttle below 20% of base
  ) {
    this.currentCapacity = baseCapacity;
    this.currentRefillPerSec = baseRefillPerSec;
    this.tokens = baseCapacity;
    this.lastRefillMs = Date.now();
  }

  /** Attempt to consume `cost` tokens. Returns true if allowed. */
  tryConsume(cost = 1): boolean {
    this.refill();
    if (this.tokens >= cost) {
      this.tokens -= cost;
      return true;
    }
    return false;
  }

  /** Milliseconds until at least `cost` tokens will be available. */
  msUntilAvailable(cost = 1): number {
    this.refill();
    if (this.tokens >= cost) return 0;
    const deficit = cost - this.tokens;
    return Math.ceil((deficit / this.currentRefillPerSec) * 1000);
  }

  private refill(): void {
    const now = Date.now();
    const elapsedSec = (now - this.lastRefillMs) / 1000;
    if (elapsedSec <= 0) return;
    this.tokens = Math.min(this.currentCapacity, this.tokens + elapsedSec * this.currentRefillPerSec);
    this.lastRefillMs = now;
  }

  /**
   * Called by the circuit breaker / processor on repeated failures.
   * `severity` in [0, 1]; 1 = shrink hard (near-breaker-trip), 0 = fully recovered.
   */
  adapt(severity: number): void {
    const clamped = Math.max(0, Math.min(1, severity));
    const floor = this.minCapacityFraction;
    const factor = 1 - clamped * (1 - floor); // interpolate between 1.0 and floor
    // Math.round, not Math.floor: floating-point arithmetic can put e.g.
    // 10 * 0.2 at 1.9999999999999996 instead of exactly 2 — flooring that
    // would silently under-shoot the intended capacity floor by one.
    this.currentCapacity = Math.max(1, Math.round(this.baseCapacity * factor));
    this.currentRefillPerSec = Math.max(0.5, this.baseRefillPerSec * factor);
    this.tokens = Math.min(this.tokens, this.currentCapacity);
  }

  /** Reset to full base capacity — called when a gateway recovers. */
  reset(): void {
    this.currentCapacity = this.baseCapacity;
    this.currentRefillPerSec = this.baseRefillPerSec;
  }

  snapshot() {
    this.refill();
    return {
      tokens: Math.floor(this.tokens),
      capacity: this.currentCapacity,
      refillPerSec: this.currentRefillPerSec,
      throttled: this.currentCapacity < this.baseCapacity,
    };
  }
}
