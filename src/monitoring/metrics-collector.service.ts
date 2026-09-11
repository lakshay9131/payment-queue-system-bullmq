import { Injectable, Logger } from '@nestjs/common';
import { OnEvent } from '@nestjs/event-emitter';
import { QueueManagerService } from '../queue/queue-manager.service';

/**
 * Fixed-size ring buffer of recent latencies per gateway, used to compute
 * P95/P99 without unbounded memory growth. At 50K/hr (~14/sec) across a
 * handful of gateways, a few thousand samples per gateway is more than
 * enough resolution and stays cheap to sort on demand.
 */
class LatencyWindow {
  private samples: number[] = [];
  constructor(private readonly maxSize = 5000) {}

  record(ms: number): void {
    this.samples.push(ms);
    if (this.samples.length > this.maxSize) this.samples.shift();
  }

  percentile(p: number): number {
    if (this.samples.length === 0) return 0;
    const sorted = [...this.samples].sort((a, b) => a - b);
    const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
    return sorted[idx];
  }

  get count(): number {
    return this.samples.length;
  }
}

interface GatewayCounters {
  succeeded: number;
  failed: number;
  latency: LatencyWindow;
  // sliding 60s window of timestamps, used for TPS
  recentTimestamps: number[];
}

export interface MetricsSnapshot {
  gatewayId: string;
  tps: number;
  successRate: number;
  p95LatencyMs: number;
  p99LatencyMs: number;
  totalProcessed: number;
}

@Injectable()
export class MetricsCollectorService {
  private readonly logger = new Logger(MetricsCollectorService.name);
  private readonly counters = new Map<string, GatewayCounters>();
  private readonly startTimes = new Map<string, number>(); // jobId -> processing start ts
  private readonly alertThresholds = {
    successRateFloor: 0.9,
    p99LatencyCeilingMs: 5000,
    queueDepthCeiling: 5000,
  };

  constructor(private readonly queueManager: QueueManagerService) {}

  markStart(jobId: string): void {
    this.startTimes.set(jobId, Date.now());
  }

  @OnEvent('payment.completed')
  onCompleted({ jobId, gatewayId }: { jobId: string; gatewayId: string }): void {
    this.recordOutcome(gatewayId, jobId, true);
  }
  @OnEvent('payment.started')
  onStarted({ jobId }: { jobId: string; gatewayId: string }): void {
    this.markStart(jobId);
  }

  @OnEvent('payment.failed')
  onFailed({ jobId, gatewayId }: { jobId: string; gatewayId: string }): void {
    this.recordOutcome(gatewayId, jobId, false);
  }

  private recordOutcome(gatewayId: string, jobId: string, success: boolean): void {
    const counters = this.getOrCreate(gatewayId);
    const startedAt = this.startTimes.get(jobId);
    if (startedAt) {
      counters.latency.record(Date.now() - startedAt);
      this.startTimes.delete(jobId);
    }
    if (success) counters.succeeded++;
    else counters.failed++;

    const now = Date.now();
    counters.recentTimestamps.push(now);
    // prune anything older than 60s for the TPS window
    const cutoff = now - 60_000;
    while (counters.recentTimestamps.length && counters.recentTimestamps[0] < cutoff) {
      counters.recentTimestamps.shift();
    }

    this.checkAlerts(gatewayId, counters);
  }

  private getOrCreate(gatewayId: string): GatewayCounters {
    let c = this.counters.get(gatewayId);
    if (!c) {
      c = { succeeded: 0, failed: 0, latency: new LatencyWindow(), recentTimestamps: [] };
      this.counters.set(gatewayId, c);
    }
    return c;
  }

  snapshot(gatewayId: string): MetricsSnapshot {
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

  allSnapshots(): MetricsSnapshot[] {
    return [...this.counters.keys()].map((id) => this.snapshot(id));
  }

  async fullDashboardPayload() {
    const queueMetrics = await this.queueManager.collectMetrics();
    return { timestamp: new Date().toISOString(), gateways: this.allSnapshots(), queues: queueMetrics };
  }

  private checkAlerts(gatewayId: string, counters: GatewayCounters): void {
    const total = counters.succeeded + counters.failed;
    if (total < 20) return; // avoid noisy alerts on tiny samples

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
}
