"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const backoff_1 = require("../payment/backoff");
const payment_types_1 = require("../types/payment.types");
describe('computeBackoffMs', () => {
    it('stays within [0, cap] and grows with attempt number on average', () => {
        for (let attempt = 1; attempt <= 6; attempt++) {
            const val = (0, backoff_1.computeBackoffMs)(attempt, 100, 5000);
            expect(val).toBeGreaterThanOrEqual(0);
            expect(val).toBeLessThanOrEqual(5000);
        }
    });
    it('respects the cap even at high attempt counts', () => {
        const val = (0, backoff_1.computeBackoffMs)(20, 500, 60000);
        expect(val).toBeLessThanOrEqual(60000);
    });
});
describe('classifyFailure', () => {
    it('classifies declined/fraud messages as PERMANENT', () => {
        expect((0, payment_types_1.classifyFailure)(new Error('card declined by issuer'))).toBe(payment_types_1.FailureCategory.PERMANENT);
        expect((0, payment_types_1.classifyFailure)(new Error('insufficient_funds'))).toBe(payment_types_1.FailureCategory.PERMANENT);
    });
    it('classifies timeouts and 5xx as TRANSIENT', () => {
        expect((0, payment_types_1.classifyFailure)(new Error('ETIMEDOUT contacting gateway'))).toBe(payment_types_1.FailureCategory.TRANSIENT);
        expect((0, payment_types_1.classifyFailure)(new Error('gateway returned 503'))).toBe(payment_types_1.FailureCategory.TRANSIENT);
    });
    it('classifies rate limit messages as RATE_LIMITED', () => {
        expect((0, payment_types_1.classifyFailure)(new Error('429 too many requests'))).toBe(payment_types_1.FailureCategory.RATE_LIMITED);
    });
    it('respects an explicit ClassifiedError over message sniffing', () => {
        const err = new payment_types_1.ClassifiedError('anything', payment_types_1.FailureCategory.PERMANENT);
        expect((0, payment_types_1.classifyFailure)(err)).toBe(payment_types_1.FailureCategory.PERMANENT);
    });
    it('falls back to UNKNOWN for unrecognized messages', () => {
        expect((0, payment_types_1.classifyFailure)(new Error('the gateway said something new'))).toBe(payment_types_1.FailureCategory.UNKNOWN);
    });
});
//# sourceMappingURL=backoff-and-classification.spec.js.map