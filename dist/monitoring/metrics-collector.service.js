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
var MetricsCollectorService_1;
Object.defineProperty(exports, "__esModule", { value: true });
exports.MetricsCollectorService = void 0;
const common_1 = require("@nestjs/common");
const event_emitter_1 = require("@nestjs/event-emitter");
const queue_manager_service_1 = require("../queue/queue-manager.service");
/**
 * Fixed-size ring buffer of recent latencies per gateway, used to compute
 * P95/P99 without unbounded memory growth. At 50K/hr (~14/sec) across a
 * handful of gateways, a few thousand samples per gateway is more than
 * enough resolution and stays cheap to sort on demand.
 */
class LatencyWindow {
    constructor(maxSize = 5000) {
        this.maxSize = maxSize;
        this.samples = [];
    }
    record(ms) {
        this.samples.push(ms);
        if (this.samples.length > this.maxSize)
            this.samples.shift();
    }
    percentile(p) {
        if (this.samples.length === 0)
            return 0;
        const sorted = [...this.samples].sort((a, b) => a - b);
        const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
        return sorted[idx];
    }
    get count() {
        return this.samples.length;
    }
}
let MetricsCollectorService = MetricsCollectorService_1 = class MetricsCollectorService {
    constructor(queueManager) {
        this.queueManager = queueManager;
        this.logger = new common_1.Logger(MetricsCollectorService_1.name);
        this.counters = new Map();
        this.startTimes = new Map(); // jobId -> processing start ts
        this.alertThresholds = {
            successRateFloor: 0.9,
            p99LatencyCeilingMs: 5000,
            queueDepthCeiling: 5000,
        };
    }
    markStart(jobId) {
        this.startTimes.set(jobId, Date.now());
    }
    onCompleted({ jobId, gatewayId }) {
        this.recordOutcome(gatewayId, jobId, true);
    }
    onStarted({ jobId }) {
        this.markStart(jobId);
    }
    onFailed({ jobId, gatewayId }) {
        this.recordOutcome(gatewayId, jobId, false);
    }
    recordOutcome(gatewayId, jobId, success) {
        const counters = this.getOrCreate(gatewayId);
        const startedAt = this.startTimes.get(jobId);
        if (startedAt) {
            counters.latency.record(Date.now() - startedAt);
            this.startTimes.delete(jobId);
        }
        if (success)
            counters.succeeded++;
        else
            counters.failed++;
        const now = Date.now();
        counters.recentTimestamps.push(now);
        // prune anything older than 60s for the TPS window
        const cutoff = now - 60_000;
        while (counters.recentTimestamps.length && counters.recentTimestamps[0] < cutoff) {
            counters.recentTimestamps.shift();
        }
        this.checkAlerts(gatewayId, counters);
    }
    getOrCreate(gatewayId) {
        let c = this.counters.get(gatewayId);
        if (!c) {
            c = { succeeded: 0, failed: 0, latency: new LatencyWindow(), recentTimestamps: [] };
            this.counters.set(gatewayId, c);
        }
        return c;
    }
    snapshot(gatewayId) {
        const c = this.getOrCreate(gatewayId);
        const total = c.succeeded + c.failed;
        return {
            gatewayId,
            tps: c.recentTimestamps.length / 60,
            successRate: total === 0 ? 1 : c.succeeded / total,
            p95LatencyMs: c.latency.percentile(95),
            p99LatencyMs: c.latency.percentile(99),
            totalProcessed: total,
        };
    }
    allSnapshots() {
        return [...this.counters.keys()].map((id) => this.snapshot(id));
    }
    async fullDashboardPayload() {
        const queueMetrics = await this.queueManager.collectMetrics();
        return { timestamp: new Date().toISOString(), gateways: this.allSnapshots(), queues: queueMetrics };
    }
    checkAlerts(gatewayId, counters) {
        const total = counters.succeeded + counters.failed;
        if (total < 20)
            return; // avoid noisy alerts on tiny samples
        const successRate = counters.succeeded / total;
        if (successRate < this.alertThresholds.successRateFloor) {
            this.logger.error(`ALERT: ${gatewayId} success rate ${(successRate * 100).toFixed(1)}% below floor`);
            // In production: push to PagerDuty/Slack via an AlertingService,
            // not just a log line.
        }
        const p99 = counters.latency.percentile(99);
        if (p99 > this.alertThresholds.p99LatencyCeilingMs) {
            this.logger.error(`ALERT: ${gatewayId} P99 latency ${p99}ms exceeds ceiling`);
        }
    }
};
exports.MetricsCollectorService = MetricsCollectorService;
__decorate([
    (0, event_emitter_1.OnEvent)('payment.completed'),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Object]),
    __metadata("design:returntype", void 0)
], MetricsCollectorService.prototype, "onCompleted", null);
__decorate([
    (0, event_emitter_1.OnEvent)('payment.started'),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Object]),
    __metadata("design:returntype", void 0)
], MetricsCollectorService.prototype, "onStarted", null);
__decorate([
    (0, event_emitter_1.OnEvent)('payment.failed'),
    __metadata("design:type", Function),
    __metadata("design:paramtypes", [Object]),
    __metadata("design:returntype", void 0)
], MetricsCollectorService.prototype, "onFailed", null);
exports.MetricsCollectorService = MetricsCollectorService = MetricsCollectorService_1 = __decorate([
    (0, common_1.Injectable)(),
    __metadata("design:paramtypes", [queue_manager_service_1.QueueManagerService])
], MetricsCollectorService);
//# sourceMappingURL=metrics-collector.service.js.map