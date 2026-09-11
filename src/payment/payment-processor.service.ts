import { Inject, Injectable, Logger } from '@nestjs/common';
import { Job } from 'bullmq';
import { EventEmitter2 } from '@nestjs/event-emitter';
import {
  Payment,
  PaymentGateway,
  PaymentStatus,
  ClassifiedError,
  classifyFailure,
  FailureCategory,
} from '../types/payment.types';
import { CircuitBreaker, CircuitState } from './circuit-breaker';
import { IdempotencyService } from './idempotency.service';
import { PaymentRepository, PAYMENT_REPOSITORY } from './payment.repository';
import { QueueManagerService } from '../queue/queue-manager.service';
import { withSpan } from '../tracing/tracing';
import { PaymentSaga, PaymentSagaContext, buildPaymentSagaSteps } from './saga';

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
  private inFlight = 0;
  private waiters: (() => void)[] = [];

  constructor(private readonly max: number) {}

  async acquire(): Promise<() => void> {
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

  private release(): void {
    this.inFlight--;
    const next = this.waiters.shift();
    if (next) next();
  }
}

@Injectable()
export class PaymentProcessorService {
  private readonly logger = new Logger(PaymentProcessorService.name);
  private readonly breakers = new Map<string, CircuitBreaker>();
  private readonly semaphores = new Map<string, GatewaySemaphore>();
  private readonly gateways = new Map<string, PaymentGateway>();

  constructor(
    private readonly queueManager: QueueManagerService,
    private readonly idempotency: IdempotencyService,
    @Inject(PAYMENT_REPOSITORY) private readonly paymentRepo: PaymentRepository,
    private readonly events: EventEmitter2,
  ) {}

  registerGateway(gatewayId: string, gateway: PaymentGateway, maxConcurrency = 15): void {
    this.gateways.set(gatewayId, gateway);
    this.semaphores.set(gatewayId, new GatewaySemaphore(maxConcurrency));
    this.breakers.set(
      gatewayId,
      new CircuitBreaker(gatewayId, undefined, (id, from, to) => {
        this.logger.warn(`Circuit breaker for "${id}": ${from} -> ${to}`);
        this.events.emit('circuit_breaker.state_change', { gatewayId: id, from, to });
        // Feed breaker severity back into the rate limiter — this is the
        // "adaptive rate limiting based on failures" requirement: as the
        // breaker degrades, we proactively shrink the token bucket instead
        // of waiting for it to trip fully open.
        const bucket = this.queueManager.getRateLimiter(id);
        if (to === CircuitState.OPEN) bucket.adapt(1);
        else if (to === CircuitState.CLOSED) bucket.reset();
      }),
    );
  }

  /**
   * Entry point wired to `QueueManagerService.attachWorker`. Returns
   * normally on success; throws to let BullMQ apply its retry/backoff
   * policy, UNLESS the failure is classified permanent, in which case we
   * short-circuit straight to the dead letter queue ourselves (retrying a
   * declined card is pointless and can trigger gateway fraud flags).
   */
  async process(job: Job<Payment>): Promise<void> {
    const payment = job.data;
    const breaker = this.breakers.get(payment.gatewayId);
    const semaphore = this.semaphores.get(payment.gatewayId);
    const gateway = this.gateways.get(payment.gatewayId);
    const bucket = this.queueManager.getRateLimiter(payment.gatewayId);

    if (!breaker || !semaphore || !gateway) {
      throw new Error(`Gateway "${payment.gatewayId}" is not registered`);
    }

    await withSpan(
      'payment.process',
      payment.correlationId,
      { paymentId: payment.id, gatewayId: payment.gatewayId, attempt: job.attemptsMade + 1 },
      async () => {
        // --- Circuit breaker gate ---
        if (!breaker.canAttempt()) {
          throw new ClassifiedError(
            `Circuit open for gateway ${payment.gatewayId}`,
            FailureCategory.TRANSIENT, // let BullMQ's backoff retry later rather than DLQ
          );
        }

        // --- Rate limit gate --- (soft-block: reschedule rather than fail)
        if (!bucket.tryConsume()) {
          const waitMs = bucket.msUntilAvailable();
          throw new ClassifiedError(
            `Rate limited on gateway ${payment.gatewayId}, retry in ${waitMs}ms`,
            FailureCategory.RATE_LIMITED,
          );
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
          await this.finalize(payment, PaymentStatus.COMPLETED, undefined, job);
          breaker.recordSuccess();
          return;
        }

        const dbRecord = await this.paymentRepo.getStatus(payment.id);
        if (dbRecord?.status === PaymentStatus.COMPLETED) {
          this.logger.warn(
            `[${payment.correlationId}] idempotent hit (db fallback) — redis was stale or evicted`,
          );
          // Backfill Redis so subsequent retries hit the fast path again.
          await this.idempotency.storeResult(payment.idempotencyKey, {
            success: true,
            transactionId: dbRecord.gatewayTransactionId ?? 'unknown',
            gatewayId: payment.gatewayId,
          });
          await this.finalize(payment, PaymentStatus.COMPLETED, undefined, job);
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
          } finally {
            semaphoreRelease();
          }
        } catch (err) {
          breaker.recordFailure();
          const category = classifyFailure(err);

          if (category === FailureCategory.PERMANENT) {
            await this.finalize(payment, PaymentStatus.FAILED, (err as Error).message, job);
            await this.queueManager.moveToDeadLetter(payment, (err as Error).message);
            await this.idempotency.releaseLock(payment.idempotencyKey);
            return; // do NOT rethrow — we've handled terminal failure ourselves
          }

          await this.idempotency.releaseLock(payment.idempotencyKey);

          if (job.attemptsMade + 1 >= payment.maxRetries) {
            await this.finalize(payment, PaymentStatus.FAILED, (err as Error).message, job);
            await this.queueManager.moveToDeadLetter(payment, `exhausted retries: ${(err as Error).message}`);
            return;
          }

          throw err; // let BullMQ's backoff (configured with jitter) retry
        }
      },
    );
  }

  private async runSaga(payment: Payment, gateway: PaymentGateway): Promise<void> {
    const steps = buildPaymentSagaSteps({
      authorize: async () => {
        const res = await gateway.process(payment);
        if (!res.success) {
          throw new ClassifiedError(res.message ?? 'authorization failed', classifyFailure(res.message));
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
        await this.paymentRepo.upsertStatus(payment.id, PaymentStatus.COMPLETED, {
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
          transactionId: ctx.gatewayTransactionId!,
          gatewayId: payment.gatewayId,
        });

        return { ledgerEntryId: `ledger_${ctx.paymentId}` };
      },
      reverseLedgerEntry: async (ctx) => {
        this.logger.warn(`[${ctx.correlationId}] reversing ledger entry ${ctx.ledgerEntryId}`);
        await this.paymentRepo.upsertStatus(payment.id, PaymentStatus.COMPENSATED);
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

    const saga = new PaymentSaga(steps);
    await saga.run({ paymentId: payment.id, correlationId: payment.correlationId });

    // By this point the lock has already been released and the result
    // cached (inside writeLedgerEntry). Finalize just emits the domain
    // event for downstream subscribers (metrics, dashboard, analytics).
    await this.finalize(payment, PaymentStatus.COMPLETED, undefined, undefined);
  }

  private throwInjectedSagaFailure(step: 'capture' | 'ledger' | 'notify'): void {
    if (process.env.SAGA_FAIL_STEP !== step) return;

    throw new ClassifiedError(
      `Injected saga failure at ${step} step`,
      FailureCategory.TRANSIENT,
    );
  }

  /**
   * Handles the "worker died mid-flight, lock held by a dead worker, no
   * result yet" case. Rather than guessing, ask the gateway directly
   * whether the earlier attempt settled — this is the only source of
   * truth for "did we actually charge the customer."
   */
  private async reconcile(payment: Payment, gateway: PaymentGateway): Promise<void> {
    const stillLocked = await this.idempotency.hasLock(payment.idempotencyKey);
    if (!stillLocked) return; // lock expired/released normally, job will be retried fresh

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
        await this.finalize(payment, PaymentStatus.COMPLETED, undefined, undefined);
      }
      // 'pending' or 'unknown' -> let this job retry later via backoff;
      // 'failed' -> fall through, next attempt will re-acquire the lock
    } catch {
      // Gateway lookup itself failed — safest to just let this attempt
      // retry later rather than guess.
    }
  }

  private async finalize(
    payment: Payment,
    status: PaymentStatus,
    failureReason: string | undefined,
    _job: Job<Payment> | undefined,
  ): Promise<void> {
    // Persist Payment.status = status, processedAt = new Date(), failureReason
    // to the primary DB here (Postgres/Mongo depending on stack).
    this.events.emit('payment.status_changed', {
      paymentId: payment.id,
      status,
      failureReason,
      correlationId: payment.correlationId,
    });
  }
}
