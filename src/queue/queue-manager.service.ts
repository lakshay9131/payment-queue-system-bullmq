import { Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { Queue, Worker, Job, JobsOptions } from 'bullmq';
// Note: BullMQ v5 removed the standalone QueueScheduler class — delayed-job
// promotion and stalled-job recovery are now handled automatically by any
// active Worker on the queue, so no separate scheduler instance is needed.
// (Earlier versions of this file, and some BullMQ docs/tutorials still in
// circulation, reference QueueScheduler — worth knowing that version drift
// exists if you see it elsewhere.)
import { EventEmitter2 } from '@nestjs/event-emitter';
import { Payment, Priority } from '../types/payment.types';
import { TokenBucket } from './token-bucket';

/**
 * Queue naming uses hyphens because BullMQ 5 rejects colons in queue names.
 * Names are `payments-{gatewayId}-{priority}`, plus two special queues:
 * `payments-dlq` and `payments-scheduled`.
 *
 * Why per-gateway *and* per-priority instead of one global priority queue:
 * a single slow/degraded gateway must never starve payments destined for
 * healthy gateways. BullMQ priority within one queue only orders jobs
 * *within* that queue — it doesn't isolate gateways from each other. So the
 * gateway dimension gives isolation (bulkheading), and priority within each
 * gateway's queue gives ordering. This also lets each gateway have its own
 * worker concurrency tuned to that gateway's real rate limit.
 */
interface ManagedQueue {
  queue: Queue;
  workers: Worker[];
  gatewayId: string;
  priority: Priority | 'scheduled' | 'dlq';
}

export interface QueueMetricsSnapshot {
  queueName: string;
  waiting: number;
  active: number;
  delayed: number;
  failed: number;
  completed: number;
  avgLatencyMs: number;
}

const PRIORITY_WEIGHT: Record<Priority, number> = { high: 1, normal: 5, low: 10 }; // BullMQ: lower number = higher priority

@Injectable()
export class QueueManagerService implements OnModuleDestroy {
  private readonly logger = new Logger(QueueManagerService.name);
  private readonly queues = new Map<string, ManagedQueue>();
  private readonly rateLimiters = new Map<string, TokenBucket>();
  private readonly connection = { host: process.env.REDIS_HOST ?? 'localhost', port: 6379 };
  private shuttingDown = false;

  constructor(private readonly events: EventEmitter2) {}

  // --------------------------------------------------------------------
  // Dynamic queue creation
  // --------------------------------------------------------------------

  /** Idempotent — returns the existing managed queue if the gateway is already registered. */
  registerGateway(gatewayId: string, opts: { concurrency?: number; rateLimit?: { capacity: number; perSecond: number } } = {}): void {
    const priorities: Priority[] = ['high', 'normal', 'low'];
    for (const priority of priorities) {
      const name = this.queueName(gatewayId, priority);
      if (this.queues.has(name)) continue;

      const queue = new Queue(name, { connection: this.connection });

      this.queues.set(name, { queue, workers: [], gatewayId, priority });
    }

    if (!this.rateLimiters.has(gatewayId)) {
      const rl = opts.rateLimit ?? { capacity: 50, perSecond: 20 };
      this.rateLimiters.set(gatewayId, new TokenBucket(rl.capacity, rl.perSecond));
    }

    this.ensureSpecialQueues();
    this.logger.log(`Registered gateway "${gatewayId}" with priority queues + rate limiter`);
  }

  private ensureSpecialQueues(): void {
    for (const name of ['payments-dlq', 'payments-scheduled']) {
      if (this.queues.has(name)) continue;
      const queue = new Queue(name, { connection: this.connection });
      this.queues.set(name, {
        queue,
        workers: [],
        gatewayId: '*',
        priority: name === 'payments:dlq' ? 'dlq' : 'scheduled',
      });
    }
  }

  // --------------------------------------------------------------------
  // Worker pool management
  // --------------------------------------------------------------------

  /**
   * Attaches a worker pool to a gateway's priority queue. Concurrency is
   * capped by both the caller's setting AND the gateway's own rate limiter
   * (checked inside the processor), so bumping concurrency alone can't
   * exceed a gateway's real throughput ceiling.
   */
  attachWorker(
    gatewayId: string,
    priority: Priority,
    processFn: (job: Job<Payment>) => Promise<void>,
    concurrency = 10,
  ): Worker {
    const name = this.queueName(gatewayId, priority);
    const managed = this.queues.get(name);
    if (!managed) throw new Error(`Queue ${name} not registered — call registerGateway first`);

    const worker = new Worker<Payment>(name, processFn, {
      connection: this.connection,
      concurrency,
    });

    worker.on('completed', (job) => this.events.emit('payment.completed', { jobId: job.id, gatewayId, priority }));
    worker.on('failed', (job, err) =>
      this.events.emit('payment.failed', { jobId: job?.id, gatewayId, priority, error: err.message }),
    );

    managed.workers.push(worker);
    return worker;
  }

  // --------------------------------------------------------------------
  // Enqueue
  // --------------------------------------------------------------------

  async enqueue(payment: Payment, opts: { delayMs?: number } = {}): Promise<Job<Payment>> {
    if (opts.delayMs && opts.delayMs > 0) {
      const q = this.queues.get('payments-scheduled')!.queue;
      return q.add('scheduled-payment', payment, {
        delay: opts.delayMs,
        jobId: payment.idempotencyKey, // dedupe at the queue level too
      });
    }

    const name = this.queueName(payment.gatewayId, payment.priority);
    const managed = this.queues.get(name);
    if (!managed) throw new Error(`No queue registered for gateway "${payment.gatewayId}" — call registerGateway first`);

    const jobOpts: JobsOptions = {
      jobId: payment.idempotencyKey, // BullMQ dedupes on jobId automatically
      priority: PRIORITY_WEIGHT[payment.priority],
      attempts: payment.maxRetries + 1,
      backoff: { type: 'exponentialWithJitter', delay: 500 }, // see PaymentProcessor for the actual jitter math used pre-BullMQ-6
      removeOnComplete: { age: 3600, count: 10_000 },
      removeOnFail: false, // keep failed jobs until explicitly moved to DLQ
    };

    return managed.queue.add('process-payment', payment, jobOpts);
  }

  async moveToDeadLetter(payment: Payment, reason: string): Promise<void> {
    const dlq = this.queues.get('payments-dlq')!.queue;
    await dlq.add('dead-letter', { ...payment, failureReason: reason }, { jobId: `dlq:${payment.idempotencyKey}` });
    this.events.emit('payment.dead_letter', { paymentId: payment.id, reason });
  }

  getRateLimiter(gatewayId: string): TokenBucket {
    const rl = this.rateLimiters.get(gatewayId);
    if (!rl) throw new Error(`No rate limiter for gateway "${gatewayId}"`);
    return rl;
  }

  // --------------------------------------------------------------------
  // Metrics
  // --------------------------------------------------------------------

  async collectMetrics(): Promise<QueueMetricsSnapshot[]> {
    const snapshots: QueueMetricsSnapshot[] = [];
    for (const [name, managed] of this.queues) {
      const counts = await managed.queue.getJobCounts('waiting', 'active', 'delayed', 'failed', 'completed');
      snapshots.push({
        queueName: name,
        waiting: counts.waiting ?? 0,
        active: counts.active ?? 0,
        delayed: counts.delayed ?? 0,
        failed: counts.failed ?? 0,
        completed: counts.completed ?? 0,
        avgLatencyMs: 0, // populated by MetricsCollector, which tracks timestamps job-by-job
      });
    }
    return snapshots;
  }

  private queueName(gatewayId: string, priority: Priority): string {
    return `payments-${gatewayId}-${priority}`;
  }

  // --------------------------------------------------------------------
  // Graceful shutdown
  // --------------------------------------------------------------------

  async onModuleDestroy(): Promise<void> {
    await this.shutdown();
  }

  async shutdown(gracePeriodMs = 30_000): Promise<void> {
    if (this.shuttingDown) return;
    this.shuttingDown = true;
    this.logger.warn(`Shutting down: draining in-flight jobs (grace period ${gracePeriodMs}ms)`);

    // 1. Stop accepting new jobs by pausing every queue.
    await Promise.all([...this.queues.values()].map((m) => m.queue.pause()));

    // 2. Close workers — BullMQ's Worker.close() waits for active jobs to
    //    finish (up to its own lock-renewal timeout) before returning.
    const workerCloses = [...this.queues.values()].flatMap((m) => m.workers.map((w) => w.close()));
    const timeout = new Promise((resolve) => setTimeout(resolve, gracePeriodMs));
    await Promise.race([Promise.all(workerCloses), timeout]);

    // 3. Close queue clients. (No separate scheduler to close — see the
    // BullMQ v5 note on QueueScheduler above.)
    await Promise.all([...this.queues.values()].map((m) => m.queue.close()));

    this.logger.warn('Shutdown complete');
  }
}
