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
var QueueManagerService_1;
Object.defineProperty(exports, "__esModule", { value: true });
exports.QueueManagerService = void 0;
const common_1 = require("@nestjs/common");
const bullmq_1 = require("bullmq");
// Note: BullMQ v5 removed the standalone QueueScheduler class — delayed-job
// promotion and stalled-job recovery are now handled automatically by any
// active Worker on the queue, so no separate scheduler instance is needed.
// (Earlier versions of this file, and some BullMQ docs/tutorials still in
// circulation, reference QueueScheduler — worth knowing that version drift
// exists if you see it elsewhere.)
const event_emitter_1 = require("@nestjs/event-emitter");
const token_bucket_1 = require("./token-bucket");
const PRIORITY_WEIGHT = { high: 1, normal: 5, low: 10 }; // BullMQ: lower number = higher priority
let QueueManagerService = QueueManagerService_1 = class QueueManagerService {
    constructor(events) {
        this.events = events;
        this.logger = new common_1.Logger(QueueManagerService_1.name);
        this.queues = new Map();
        this.rateLimiters = new Map();
        this.connection = { host: process.env.REDIS_HOST ?? 'localhost', port: 6379 };
        this.shuttingDown = false;
    }
    // --------------------------------------------------------------------
    // Dynamic queue creation
    // --------------------------------------------------------------------
    /** Idempotent — returns the existing managed queue if the gateway is already registered. */
    registerGateway(gatewayId, opts = {}) {
        const priorities = ['high', 'normal', 'low'];
        for (const priority of priorities) {
            const name = this.queueName(gatewayId, priority);
            if (this.queues.has(name))
                continue;
            const queue = new bullmq_1.Queue(name, { connection: this.connection });
            this.queues.set(name, { queue, workers: [], gatewayId, priority });
        }
        if (!this.rateLimiters.has(gatewayId)) {
            const rl = opts.rateLimit ?? { capacity: 50, perSecond: 20 };
            this.rateLimiters.set(gatewayId, new token_bucket_1.TokenBucket(rl.capacity, rl.perSecond));
        }
        this.ensureSpecialQueues();
        this.logger.log(`Registered gateway "${gatewayId}" with priority queues + rate limiter`);
    }
    ensureSpecialQueues() {
        for (const name of ['payments-dlq', 'payments-scheduled']) {
            if (this.queues.has(name))
                continue;
            const queue = new bullmq_1.Queue(name, { connection: this.connection });
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
    attachWorker(gatewayId, priority, processFn, concurrency = 10) {
        const name = this.queueName(gatewayId, priority);
        const managed = this.queues.get(name);
        if (!managed)
            throw new Error(`Queue ${name} not registered — call registerGateway first`);
        const worker = new bullmq_1.Worker(name, processFn, {
            connection: this.connection,
            concurrency,
        });
        worker.on('completed', (job) => this.events.emit('payment.completed', { jobId: job.id, gatewayId, priority }));
        worker.on('failed', (job, err) => this.events.emit('payment.failed', { jobId: job?.id, gatewayId, priority, error: err.message }));
        managed.workers.push(worker);
        return worker;
    }
    // --------------------------------------------------------------------
    // Enqueue
    // --------------------------------------------------------------------
    async enqueue(payment, opts = {}) {
        if (opts.delayMs && opts.delayMs > 0) {
            const q = this.queues.get('payments-scheduled').queue;
            return q.add('scheduled-payment', payment, {
                delay: opts.delayMs,
                jobId: payment.idempotencyKey, // dedupe at the queue level too
            });
        }
        const name = this.queueName(payment.gatewayId, payment.priority);
        const managed = this.queues.get(name);
        if (!managed)
            throw new Error(`No queue registered for gateway "${payment.gatewayId}" — call registerGateway first`);
        const jobOpts = {
            jobId: payment.idempotencyKey, // BullMQ dedupes on jobId automatically
            priority: PRIORITY_WEIGHT[payment.priority],
            attempts: payment.maxRetries + 1,
            backoff: { type: 'exponentialWithJitter', delay: 500 }, // see PaymentProcessor for the actual jitter math used pre-BullMQ-6
            removeOnComplete: { age: 3600, count: 10_000 },
            removeOnFail: false, // keep failed jobs until explicitly moved to DLQ
        };
        return managed.queue.add('process-payment', payment, jobOpts);
    }
    async moveToDeadLetter(payment, reason) {
        const dlq = this.queues.get('payments-dlq').queue;
        await dlq.add('dead-letter', { ...payment, failureReason: reason }, { jobId: `dlq:${payment.idempotencyKey}` });
        this.events.emit('payment.dead_letter', { paymentId: payment.id, reason });
    }
    getRateLimiter(gatewayId) {
        const rl = this.rateLimiters.get(gatewayId);
        if (!rl)
            throw new Error(`No rate limiter for gateway "${gatewayId}"`);
        return rl;
    }
    // --------------------------------------------------------------------
    // Metrics
    // --------------------------------------------------------------------
    async collectMetrics() {
        const snapshots = [];
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
    queueName(gatewayId, priority) {
        return `payments-${gatewayId}-${priority}`;
    }
    // --------------------------------------------------------------------
    // Graceful shutdown
    // --------------------------------------------------------------------
    async onModuleDestroy() {
        await this.shutdown();
    }
    async shutdown(gracePeriodMs = 30_000) {
        if (this.shuttingDown)
            return;
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
};
exports.QueueManagerService = QueueManagerService;
exports.QueueManagerService = QueueManagerService = QueueManagerService_1 = __decorate([
    (0, common_1.Injectable)(),
    __metadata("design:paramtypes", [event_emitter_1.EventEmitter2])
], QueueManagerService);
//# sourceMappingURL=queue-manager.service.js.map