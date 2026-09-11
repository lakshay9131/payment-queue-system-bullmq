"use strict";
// ============================================================================
// Core domain types
// ============================================================================
Object.defineProperty(exports, "__esModule", { value: true });
exports.ClassifiedError = exports.FailureCategory = exports.PaymentStatus = void 0;
exports.classifyFailure = classifyFailure;
var PaymentStatus;
(function (PaymentStatus) {
    PaymentStatus["QUEUED"] = "queued";
    PaymentStatus["PROCESSING"] = "processing";
    PaymentStatus["COMPLETED"] = "completed";
    PaymentStatus["FAILED"] = "failed";
    PaymentStatus["DEAD_LETTER"] = "dead_letter";
    PaymentStatus["COMPENSATING"] = "compensating";
    PaymentStatus["COMPENSATED"] = "compensated";
})(PaymentStatus || (exports.PaymentStatus = PaymentStatus = {}));
// ============================================================================
// Failure classification — this is the single most important modeling
// decision in the whole system. Everything downstream (retry policy,
// circuit breaker, alerting) branches on this.
// ============================================================================
var FailureCategory;
(function (FailureCategory) {
    FailureCategory["TRANSIENT"] = "transient";
    FailureCategory["RATE_LIMITED"] = "rate_limited";
    FailureCategory["PERMANENT"] = "permanent";
    FailureCategory["UNKNOWN"] = "unknown";
})(FailureCategory || (exports.FailureCategory = FailureCategory = {}));
class ClassifiedError extends Error {
    constructor(message, category, gatewayCode) {
        super(message);
        this.category = category;
        this.gatewayCode = gatewayCode;
        this.name = 'ClassifiedError';
    }
}
exports.ClassifiedError = ClassifiedError;
function classifyFailure(err) {
    if (err instanceof ClassifiedError)
        return err.category;
    const message = err instanceof Error ? err.message.toLowerCase() : String(err).toLowerCase();
    // Permanent: gateway told us definitively "no" — retrying wastes money on
    // fees and can trigger fraud flags on some gateways.
    if (/declined|insufficient_funds|invalid_card|fraud|account_closed|invalid_account/.test(message)) {
        return FailureCategory.PERMANENT;
    }
    if (/rate.?limit|429|too many requests/.test(message)) {
        return FailureCategory.RATE_LIMITED;
    }
    if (/timeout|etimedout|econnreset|econnrefused|502|503|504|network/.test(message)) {
        return FailureCategory.TRANSIENT;
    }
    return FailureCategory.UNKNOWN;
}
//# sourceMappingURL=payment.types.js.map