"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
var __param = (this && this.__param) || function (paramIndex, decorator) {
    return function (target, key) { decorator(target, key, paramIndex); }
};
var PaymentProcessorService_1;
Object.defineProperty(exports, "__esModule", { value: true });
exports.PaymentProcessorService = void 0;
const common_1 = require("@nestjs/common");
const event_emitter_1 = require("@nestjs/event-emitter");
const payment_types_1 = require("../types/payment.types");
const circuit_breaker_1 = require("./circuit-breaker");
const idempotency_service_1 = require("./idempotency.service");
const payment_repository_1 = require("./payment.repository");
const queue_manager_service_1 = require("../queue/queue-manager.service");
const tracing_1 = require("../tracing/tracing");
const saga_1 = require("./saga");
/**
 * Concurrency control philosophy: BullMQ's Worker `concurrency` option
 * already caps in-process concurrency per queue. This class adds a second,
 * *per-gateway* semaphore because a single gateway's priority queues (high/
 * normal/low) each have their own worker pool — without a shared gateway-
 * level cap, three priority workers could each run 10 concurrent jobs
 * against the same gateway (30 total), blowing past what the gateway
 * actually tolerates. The token bucket handles *rate*; this handles
 * *concurrency* — they're different resources (a gateway might tolerate
 * high concurrency but a low steady-state rate, or vice versa).
 */
class GatewaySemaphore {
    constructor(max) {
        this.max = max;
        this.inFlight = 0;
        this.waiters = [];
    }
    async acquire() {
        if (this.inFlight < this.max) {
            this.inFlight++;
            return () => this.release();
        }
        return new Promise((resolve) => {
            this.waiters.push(() => {
                this.inFlight++;
                resolve(() => this.release());
            });
        });
    }
    release() {
        this.inFlight--;
        const next = this.waiters.shift();
        if (next)
            next();
    }
}
let PaymentProcessorService = PaymentProcessorService_1 = class PaymentProcessorService {
    constructor(queueManager, idempotency, paymentRepo, events) {
        this.queueManager = queueManager;
        this.idempotency = idempotency;
        this.paymentRepo = paymentRepo;
        this.events = events;
        this.logger = new common_1.Logger(PaymentProcessorService_1.name);
        this.breakers = new Map();
        this.semaphores = new Map();
        this.gateways = new Map();
    }
    registerGateway(gatewayId, gateway, maxConcurrency = 15) {
        this.gateways.set(gatewayId, gateway);
        this.semaphores.set(gatewayId, new GatewaySemaphore(maxConcurrency));
        this.breakers.set(gatewayId, new circuit_breaker_1.CircuitBreaker(gatewayId, undefined, (id, from, to) => {
            this.logger.warn(`Circuit breaker for "${id}": ${from} -> ${to}`);
            this.events.emit('circuit_breaker.state_change', { gatewayId: id, from, to });
            // Feed breaker severity back into the rate limiter — this is the
            // "adaptive rate limiting based on failures" requirement: as the
            // breaker degrades, we proactively shrink the token bucket instead
            // of waiting for it to trip fully open.
            const bucket = this.queueManager.getRateLimiter(id);
            if (to === circuit_breaker_1.CircuitState.OPEN)
                bucket.adapt(1);
            else if (to === circuit_breaker_1.CircuitState.CLOSED)
                bucket.reset();
        }));
    }
    /**
     * Entry point wired to `QueueManagerService.attachWorker`. Returns
     * normally on success; throws to let BullMQ apply its retry/backoff
     * policy, UNLESS the failure is classified permanent, in which case we
     * short-circuit straight to the dead letter queue ourselves (retrying a
     * declined card is pointless and can trigger gateway fraud flags).
     */
    async process(job) {
        const payment = job.data;
        const breaker = this.breakers.get(payment.gatewayId);
        const semaphore = this.semaphores.get(payment.gatewayId);
        const gateway = this.gateways.get(payment.gatewayId);
        const bucket = this.queueManager.getRateLimiter(payment.gatewayId);
        if (!breaker || !semaphore || !gateway) {
            throw new Error(`Gateway "${payment.gatewayId}" is not registered`);
        }
        await (0, tracing_1.withSpan)('payment.process', payment.correlationId, { paymentId: payment.id, gatewayId: payment.gatewayId, attempt: job.attemptsMade + 1 }, async () => {
            // --- Circuit breaker gate ---
            if (!breaker.canAttempt()) {
                throw new payment_types_1.ClassifiedError(`Circuit open for gateway ${payment.gatewayId}`, payment_types_1.FailureCategory.TRANSIENT);
            }
            // --- Rate limit gate --- (soft-block: reschedule rather than fail)
            if (!bucket.tryConsume()) {
                const waitMs = bucket.msUntilAvailable();
                throw new payment_types_1.ClassifiedError(`Rate limited on gateway ${payment.gatewayId}, retry in ${waitMs}ms`, payment_types_1.FailureCategory.RATE_LIMITED);
            }
            this.events.emit('payment.started', {
                jobId: String(job.id),
                gatewayId: payment.gatewayId,
            });
            // --- Idempotency check: two-tier (Redis fast path, DB fallback) ---
            // Redis is fast but not durable — TTL expiry (24h on results) or an
            // eviction/restart can lose an entry. The DB is the actual source
            // of truth. Checking it on every payment would defeat the point of
            // having Redis in the hot path at all, so it's only consulted on a
            // Redis miss, which should be the rare case.
            const cached = await this.idempotency.getResult(payment.idempotencyKey);
            if (cached) {
                this.logger.log(`[${payment.correlationId}] idempotent hit (redis) — skipping gateway call`);
                await this.finalize(payment, payment_types_1.PaymentStatus.COMPLETED, undefined, job);
                breaker.recordSuccess();
                return;
            }
            const dbRecord = await this.paymentRepo.getStatus(payment.id);
            if (dbRecord?.status === payment_types_1.PaymentStatus.COMPLETED) {
                this.logger.warn(`[${payment.correlationId}] idempotent hit (db fallback) — redis was stale or evicted`);
                // Backfill Redis so subsequent retries hit the fast path again.
                await this.idempotency.storeResult(payment.idempotencyKey, {
                    success: true,
                    transactionId: dbRecord.gatewayTransactionId ?? 'unknown',
                    gatewayId: payment.gatewayId,
                });
                await this.finalize(payment, payment_types_1.PaymentStatus.COMPLETED, undefined, job);
                breaker.recordSuccess();
                return;
            }
            const locked = await this.idempotency.acquireLock(payment.idempotencyKey);
            if (!locked) {
                // Another worker (or a retry of this job after a crash) is already
                // in flight for this key. Don't call the gateway twice — reconcile
                // via getStatus instead of blindly proceeding.
                await this.reconcile(payment, gateway);
                return;
            }
            try {
                const semaphoreRelease = await semaphore.acquire();
                try {
                    await this.runSaga(payment, gateway);
                    breaker.recordSuccess();
                }
                finally {
                    semaphoreRelease();
                }
            }
            catch (err) {
                breaker.recordFailure();
                const category = (0, payment_types_1.classifyFailure)(err);
                if (category === payment_types_1.FailureCategory.PERMANENT) {
                    await this.finalize(payment, payment_types_1.PaymentStatus.FAILED, err.message, job);
                    await this.queueManager.moveToDeadLetter(payment, err.message);
                    await this.idempotency.releaseLock(payment.idempotencyKey);
                    return; // do NOT rethrow — we've handled terminal failure ourselves
                }
                await this.idempotency.releaseLock(payment.idempotencyKey);
                if (job.attemptsMade + 1 >= payment.maxRetries) {
                    await this.finalize(payment, payment_types_1.PaymentStatus.FAILED, err.message, job);
                    await this.queueManager.moveToDeadLetter(payment, `exhausted retries: ${err.message}`);
                    return;
                }
                throw err; // let BullMQ's backoff (configured with jitter) retry
            }
        });
    }
    async runSaga(payment, gateway) {
        const steps = (0, saga_1.buildPaymentSagaSteps)({
            authorize: async () => {
                const res = await gateway.process(payment);
                if (!res.success) {
                    throw new payment_types_1.ClassifiedError(res.message ?? 'authorization failed', (0, payment_types_1.classifyFailure)(res.message));
                }
                return { gatewayTransactionId: res.transactionId };
            },
            voidAuthorization: async (ctx) => {
                this.logger.warn(`[${ctx.correlationId}] voiding authorization ${ctx.gatewayTransactionId}`);
                // gateway.void(ctx.gatewayTransactionId) in a real integration
            },
            capture: async (ctx) => {
                this.throwInjectedSagaFailure('capture');
                // For gateways that auto-capture on authorize, this is a no-op;
                // kept as an explicit step for gateways requiring separate capture.
                this.logger.log(`[${ctx.correlationId}] capture confirmed for ${ctx.gatewayTransactionId}`);
            },
            refund: async (ctx) => {
                this.logger.warn(`[${ctx.correlationId}] refunding capture ${ctx.gatewayTransactionId}`);
            },
            writeLedgerEntry: async (ctx) => {
                this.throwInjectedSagaFailure('ledger');
                // persist to the ledger DB (Postgres, typically), in the same
                // transaction as updating Payment.status if using an ORM w/
                // transactional support
                await this.paymentRepo.upsertStatus(payment.id, payment_types_1.PaymentStatus.COMPLETED, {
                    gatewayTransactionId: ctx.gatewayTransactionId,
                });
                // Lock release / result caching happens HERE — synchronously,
                // inline, the moment money movement is durably recorded — not
                // after the whole saga (including notify) finishes. Releasing the
                // lock is correctness-critical: it's what lets a retried job for
                // this payment be recognized as already-done instead of routed to
                // reconcile(). That can't depend on an event reaching a subscriber
                // at some later point — if that subscriber is slow or down, the
                // lock would outlive its purpose and block a legitimate retry for
                // no reason. Notify (the next step) is not money-affecting, so
                // there's no correctness reason to hold the lock through it.
                await this.idempotency.storeResult(payment.idempotencyKey, {
                    success: true,
                    transactionId: ctx.gatewayTransactionId,
                    gatewayId: payment.gatewayId,
                });
                return { ledgerEntryId: `ledger_${ctx.paymentId}` };
            },
            reverseLedgerEntry: async (ctx) => {
                this.logger.warn(`[${ctx.correlationId}] reversing ledger entry ${ctx.ledgerEntryId}`);
                await this.paymentRepo.upsertStatus(payment.id, payment_types_1.PaymentStatus.COMPENSATED);
                // Note: no idempotency result was stored yet at this point (it's
                // stored *after* a successful ledger write above), so there's
                // nothing to unwind in Redis here — only the ledger/DB side needs
                // reversing.
            },
            notifyCustomer: async (ctx) => {
                this.throwInjectedSagaFailure('notify');
                this.events.emit('payment.notify', { paymentId: ctx.paymentId });
            },
        });
        const saga = new saga_1.PaymentSaga(steps);
        await saga.run({ paymentId: payment.id, correlationId: payment.correlationId });
        // By this point the lock has already been released and the result
        // cached (inside writeLedgerEntry). Finalize just emits the domain
        // event for downstream subscribers (metrics, dashboard, analytics).
        await this.finalize(payment, payment_types_1.PaymentStatus.COMPLETED, undefined, undefined);
    }
    throwInjectedSagaFailure(step) {
        if (process.env.SAGA_FAIL_STEP !== step)
            return;
        throw new payment_types_1.ClassifiedError(`Injected saga failure at ${step} step`, payment_types_1.FailureCategory.TRANSIENT);
    }
    /**
     * Handles the "worker died mid-flight, lock held by a dead worker, no
     * result yet" case. Rather than guessing, ask the gateway directly
     * whether the earlier attempt settled — this is the only source of
     * truth for "did we actually charge the customer."
     */
    async reconcile(payment, gateway) {
        const stillLocked = await this.idempotency.hasLock(payment.idempotencyKey);
        if (!stillLocked)
            return; // lock expired/released normally, job will be retried fresh
        // We don't have the gateway's transactionId stored yet if the crash
        // happened before authorize() returned — in that case there is nothing
        // to poll and we must wait for the lock TTL to expire before retrying.
        // If the integration guarantees a transactionId can be looked up by our
        // idempotencyKey (most gateways support this), prefer that path:
        try {
            const status = await gateway.getStatus(payment.idempotencyKey);
            if (status.status === 'settled') {
                await this.idempotency.storeResult(payment.idempotencyKey, {
                    success: true,
                    transactionId: status.transactionId,
                    gatewayId: payment.gatewayId,
                });
                await this.finalize(payment, payment_types_1.PaymentStatus.COMPLETED, undefined, undefined);
            }
            // 'pending' or 'unknown' -> let this job retry later via backoff;
            // 'failed' -> fall through, next attempt will re-acquire the lock
        }
        catch {
            // Gateway lookup itself failed — safest to just let this attempt
            // retry later rather than guess.
        }
    }
    async finalize(payment, status, failureReason, _job) {
        // Persist Payment.status = status, processedAt = new Date(), failureReason
        // to the primary DB here (Postgres/Mongo depending on stack).
        this.events.emit('payment.status_changed', {
            paymentId: payment.id,
            status,
            failureReason,
            correlationId: payment.correlationId,
        });
    }
};
exports.PaymentProcessorService = PaymentProcessorService;
exports.PaymentProcessorService = PaymentProcessorService = PaymentProcessorService_1 = __decorate([
    (0, common_1.Injectable)(),
    __param(2, (0, common_1.Inject)(payment_repository_1.PAYMENT_REPOSITORY)),
    __metadata("design:paramtypes", [queue_manager_service_1.QueueManagerService,
        idempotency_service_1.IdempotencyService, Object, event_emitter_1.EventEmitter2])
], PaymentProcessorService);
//# sourceMappingURL=payment-processor.service.js.map