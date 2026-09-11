export enum CircuitState {
  CLOSED = 'closed', // normal operation
  OPEN = 'open', // failing fast, not calling gateway
  HALF_OPEN = 'half_open', // trial requests to test recovery
}

export interface CircuitBreakerOptions {
  failureThreshold: number; // consecutive failures to trip
  failureRateThreshold: number; // e.g. 0.5 = 50% failure rate over window trips it
  windowSize: number; // rolling window of recent outcomes to consider
  openDurationMs: number; // how long to stay OPEN before trying HALF_OPEN
  halfOpenTrialCount: number; // number of trial requests allowed in HALF_OPEN
}

const DEFAULTS: CircuitBreakerOptions = {
  failureThreshold: 5,
  failureRateThreshold: 0.5,
  windowSize: 20,
  openDurationMs: 30_000,
  halfOpenTrialCount: 3,
};

/**
 * Per-gateway circuit breaker. Trips on either N consecutive failures
 * (catches hard outages fast) or a rolling failure-rate threshold (catches
 * degraded-but-not-dead gateways). This dual condition matters: a gateway
 * that's failing 1-in-3 requests will never hit 5 *consecutive* failures by
 * chance, but is still unhealthy enough to shed load from.
 */
export class CircuitBreaker {
  private state: CircuitState = CircuitState.CLOSED;
  private consecutiveFailures = 0;
  private outcomes: boolean[] = []; // true = success
  private openedAt = 0;
  private halfOpenInFlight = 0;
  private halfOpenSuccesses = 0;

  constructor(
    public readonly gatewayId: string,
    private readonly opts: CircuitBreakerOptions = DEFAULTS,
    private readonly onStateChange?: (gatewayId: string, from: CircuitState, to: CircuitState) => void,
  ) {}

  /** Call before attempting a gateway request. Throws if the request should be rejected. */
  canAttempt(): boolean {
    if (this.state === CircuitState.CLOSED) return true;

    if (this.state === CircuitState.OPEN) {
      if (Date.now() - this.openedAt >= this.opts.openDurationMs) {
        this.transition(CircuitState.HALF_OPEN);
        this.halfOpenInFlight = 0;
        this.halfOpenSuccesses = 0;
      } else {
        return false;
      }
    }

    if (this.state === CircuitState.HALF_OPEN) {
      // Allow only a limited number of concurrent trial requests through.
      if (this.halfOpenInFlight >= this.opts.halfOpenTrialCount) return false;
      this.halfOpenInFlight++;
      return true;
    }

    return true;
  }

  recordSuccess(): void {
    this.consecutiveFailures = 0;
    this.pushOutcome(true);

    if (this.state === CircuitState.HALF_OPEN) {
      this.halfOpenSuccesses++;
      if (this.halfOpenSuccesses >= this.opts.halfOpenTrialCount) {
        this.transition(CircuitState.CLOSED);
      }
    }
  }

  recordFailure(): void {
    this.consecutiveFailures++;
    this.pushOutcome(false);

    if (this.state === CircuitState.HALF_OPEN) {
      // Any failure during trial immediately re-opens — don't burn more budget.
      this.transition(CircuitState.OPEN);
      this.openedAt = Date.now();
      return;
    }

    const rate = this.failureRate();
    if (
      this.consecutiveFailures >= this.opts.failureThreshold ||
      (this.outcomes.length >= this.opts.windowSize && rate >= this.opts.failureRateThreshold)
    ) {
      this.transition(CircuitState.OPEN);
      this.openedAt = Date.now();
    }
  }

  /** 0 (closed/healthy) .. 1 (open) — feeds the token bucket's adaptive throttling. */
  severity(): number {
    if (this.state === CircuitState.OPEN) return 1;
    if (this.state === CircuitState.HALF_OPEN) return 0.6;
    return Math.min(1, this.failureRate() * 1.5);
  }

  getState(): CircuitState {
    return this.state;
  }

  private failureRate(): number {
    if (this.outcomes.length === 0) return 0;
    const failures = this.outcomes.filter((o) => !o).length;
    return failures / this.outcomes.length;
  }

  private pushOutcome(success: boolean): void {
    this.outcomes.push(success);
    if (this.outcomes.length > this.opts.windowSize) this.outcomes.shift();
  }

  private transition(to: CircuitState): void {
    const from = this.state;
    if (from === to) return;
    this.state = to;
    this.onStateChange?.(this.gatewayId, from, to);
  }
}
