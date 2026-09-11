import { CircuitBreaker, CircuitState } from '../payment/circuit-breaker';

describe('CircuitBreaker', () => {
  it('trips OPEN after consecutive failures reach the threshold', () => {
    const breaker = new CircuitBreaker('test-gw', {
      failureThreshold: 3,
      failureRateThreshold: 0.9,
      windowSize: 20,
      openDurationMs: 1000,
      halfOpenTrialCount: 2,
    });

    expect(breaker.canAttempt()).toBe(true);
    breaker.recordFailure();
    breaker.recordFailure();
    expect(breaker.getState()).toBe(CircuitState.CLOSED);
    breaker.recordFailure(); // 3rd consecutive failure -> trips
    expect(breaker.getState()).toBe(CircuitState.OPEN);
    expect(breaker.canAttempt()).toBe(false);
  });

  it('trips OPEN on rolling failure rate even without consecutive run', () => {
    const breaker = new CircuitBreaker('test-gw', {
      failureThreshold: 100, // effectively disable the consecutive trigger
      failureRateThreshold: 0.5,
      windowSize: 4,
      openDurationMs: 1000,
      halfOpenTrialCount: 2,
    });

    breaker.recordSuccess();
    breaker.recordFailure();
    breaker.recordSuccess();
    breaker.recordFailure(); // 2/4 = 50% failure rate, window full -> trips
    expect(breaker.getState()).toBe(CircuitState.OPEN);
  });

  it('transitions OPEN -> HALF_OPEN after openDurationMs, then CLOSED after enough trial successes', async () => {
    const breaker = new CircuitBreaker('test-gw', {
      failureThreshold: 1,
      failureRateThreshold: 0.5,
      windowSize: 10,
      openDurationMs: 50,
      halfOpenTrialCount: 2,
    });

    breaker.recordFailure(); // trips immediately (threshold 1)
    expect(breaker.getState()).toBe(CircuitState.OPEN);

    await new Promise((r) => setTimeout(r, 60));
    expect(breaker.canAttempt()).toBe(true); // transitions to HALF_OPEN on this check
    expect(breaker.getState()).toBe(CircuitState.HALF_OPEN);

    breaker.recordSuccess();
    breaker.recordSuccess(); // 2 successes = halfOpenTrialCount -> CLOSED
    expect(breaker.getState()).toBe(CircuitState.CLOSED);
  });

  it('any failure during HALF_OPEN immediately re-opens the circuit', async () => {
    const breaker = new CircuitBreaker('test-gw', {
      failureThreshold: 1,
      failureRateThreshold: 0.5,
      windowSize: 10,
      openDurationMs: 50,
      halfOpenTrialCount: 2,
    });

    breaker.recordFailure();
    await new Promise((r) => setTimeout(r, 60));
    breaker.canAttempt(); // moves to HALF_OPEN
    breaker.recordFailure();
    expect(breaker.getState()).toBe(CircuitState.OPEN);
  });

  it('severity() reflects state: 1 for OPEN, 0 for healthy CLOSED', () => {
    const breaker = new CircuitBreaker('test-gw');
    expect(breaker.severity()).toBe(0);
    for (let i = 0; i < 10; i++) breaker.recordFailure();
    expect(breaker.severity()).toBe(1);
  });
});
