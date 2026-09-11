import { TokenBucket } from '../queue/token-bucket';

describe('TokenBucket', () => {
  it('allows consumption up to capacity, then blocks', () => {
    const bucket = new TokenBucket(3, 1); // 3 capacity, slow refill so it won't interfere
    expect(bucket.tryConsume()).toBe(true);
    expect(bucket.tryConsume()).toBe(true);
    expect(bucket.tryConsume()).toBe(true);
    expect(bucket.tryConsume()).toBe(false); // exhausted
  });

  it('refills over time', async () => {
    const bucket = new TokenBucket(1, 10); // refills fast: 10/sec
    expect(bucket.tryConsume()).toBe(true);
    expect(bucket.tryConsume()).toBe(false);
    await new Promise((r) => setTimeout(r, 150)); // ~1.5 tokens should refill
    expect(bucket.tryConsume()).toBe(true);
  });

  it('shrinks capacity and refill rate under adapt(1) and restores on reset', () => {
    const bucket = new TokenBucket(10, 10, 0.2);
    bucket.adapt(1); // max severity -> shrink to floor (20%)
    const snap = bucket.snapshot();
    expect(snap.capacity).toBe(2); // 10 * 0.2
    expect(snap.throttled).toBe(true);

    bucket.reset();
    const restored = bucket.snapshot();
    expect(restored.capacity).toBe(10);
    expect(restored.throttled).toBe(false);
  });

  it('reports msUntilAvailable > 0 once exhausted', () => {
    const bucket = new TokenBucket(1, 5);
    bucket.tryConsume();
    expect(bucket.msUntilAvailable()).toBeGreaterThan(0);
  });
});
