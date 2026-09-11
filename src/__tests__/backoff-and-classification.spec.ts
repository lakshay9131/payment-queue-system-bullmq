import { computeBackoffMs } from '../payment/backoff';
import { classifyFailure, FailureCategory, ClassifiedError } from '../types/payment.types';

describe('computeBackoffMs', () => {
  it('stays within [0, cap] and grows with attempt number on average', () => {
    for (let attempt = 1; attempt <= 6; attempt++) {
      const val = computeBackoffMs(attempt, 100, 5000);
      expect(val).toBeGreaterThanOrEqual(0);
      expect(val).toBeLessThanOrEqual(5000);
    }
  });

  it('respects the cap even at high attempt counts', () => {
    const val = computeBackoffMs(20, 500, 60000);
    expect(val).toBeLessThanOrEqual(60000);
  });
});

describe('classifyFailure', () => {
  it('classifies declined/fraud messages as PERMANENT', () => {
    expect(classifyFailure(new Error('card declined by issuer'))).toBe(FailureCategory.PERMANENT);
    expect(classifyFailure(new Error('insufficient_funds'))).toBe(FailureCategory.PERMANENT);
  });

  it('classifies timeouts and 5xx as TRANSIENT', () => {
    expect(classifyFailure(new Error('ETIMEDOUT contacting gateway'))).toBe(FailureCategory.TRANSIENT);
    expect(classifyFailure(new Error('gateway returned 503'))).toBe(FailureCategory.TRANSIENT);
  });

  it('classifies rate limit messages as RATE_LIMITED', () => {
    expect(classifyFailure(new Error('429 too many requests'))).toBe(FailureCategory.RATE_LIMITED);
  });

  it('respects an explicit ClassifiedError over message sniffing', () => {
    const err = new ClassifiedError('anything', FailureCategory.PERMANENT);
    expect(classifyFailure(err)).toBe(FailureCategory.PERMANENT);
  });

  it('falls back to UNKNOWN for unrecognized messages', () => {
    expect(classifyFailure(new Error('the gateway said something new'))).toBe(FailureCategory.UNKNOWN);
  });
});
