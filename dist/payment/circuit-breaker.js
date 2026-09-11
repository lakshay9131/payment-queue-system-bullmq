"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CircuitBreaker = exports.CircuitState = void 0;
var CircuitState;
(function (CircuitState) {
    CircuitState["CLOSED"] = "closed";
    CircuitState["OPEN"] = "open";
    CircuitState["HALF_OPEN"] = "half_open";
})(CircuitState || (exports.CircuitState = CircuitState = {}));
const DEFAULTS = {
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
class CircuitBreaker {
    constructor(gatewayId, opts = DEFAULTS, onStateChange) {
        this.gatewayId = gatewayId;
        this.opts = opts;
        this.onStateChange = onStateChange;
        this.state = CircuitState.CLOSED;
        this.consecutiveFailures = 0;
        this.outcomes = []; // true = success
        this.openedAt = 0;
        this.halfOpenInFlight = 0;
        this.halfOpenSuccesses = 0;
    }
    /** Call before attempting a gateway request. Throws if the request should be rejected. */
    canAttempt() {
        if (this.state === CircuitState.CLOSED)
            return true;
        if (this.state === CircuitState.OPEN) {
            if (Date.now() - this.openedAt >= this.opts.openDurationMs) {
                this.transition(CircuitState.HALF_OPEN);
                this.halfOpenInFlight = 0;
                this.halfOpenSuccesses = 0;
            }
            else {
                return false;
            }
        }
        if (this.state === CircuitState.HALF_OPEN) {
            // Allow only a limited number of concurrent trial requests through.
            if (this.halfOpenInFlight >= this.opts.halfOpenTrialCount)
                return false;
            this.halfOpenInFlight++;
            return true;
        }
        return true;
    }
    recordSuccess() {
        this.consecutiveFailures = 0;
        this.pushOutcome(true);
        if (this.state === CircuitState.HALF_OPEN) {
            this.halfOpenSuccesses++;
            if (this.halfOpenSuccesses >= this.opts.halfOpenTrialCount) {
                this.transition(CircuitState.CLOSED);
            }
        }
    }
    recordFailure() {
        this.consecutiveFailures++;
        this.pushOutcome(false);
        if (this.state === CircuitState.HALF_OPEN) {
            // Any failure during trial immediately re-opens — don't burn more budget.
            this.transition(CircuitState.OPEN);
            this.openedAt = Date.now();
            return;
        }
        const rate = this.failureRate();
        if (this.consecutiveFailures >= this.opts.failureThreshold ||
            (this.outcomes.length >= this.opts.windowSize && rate >= this.opts.failureRateThreshold)) {
            this.transition(CircuitState.OPEN);
            this.openedAt = Date.now();
        }
    }
    /** 0 (closed/healthy) .. 1 (open) — feeds the token bucket's adaptive throttling. */
    severity() {
        if (this.state === CircuitState.OPEN)
            return 1;
        if (this.state === CircuitState.HALF_OPEN)
            return 0.6;
        return Math.min(1, this.failureRate() * 1.5);
    }
    getState() {
        return this.state;
    }
    failureRate() {
        if (this.outcomes.length === 0)
            return 0;
        const failures = this.outcomes.filter((o) => !o).length;
        return failures / this.outcomes.length;
    }
    pushOutcome(success) {
        this.outcomes.push(success);
        if (this.outcomes.length > this.opts.windowSize)
            this.outcomes.shift();
    }
    transition(to) {
        const from = this.state;
        if (from === to)
            return;
        this.state = to;
        this.onStateChange?.(this.gatewayId, from, to);
    }
}
exports.CircuitBreaker = CircuitBreaker;
//# sourceMappingURL=circuit-breaker.js.map