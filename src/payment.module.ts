import { Module, OnModuleInit } from '@nestjs/common';
import { EventEmitterModule } from '@nestjs/event-emitter';
import Redis from 'ioredis';
import { QueueManagerService } from './queue/queue-manager.service';
import { PaymentProcessorService } from './payment/payment-processor.service';
import { IdempotencyService } from './payment/idempotency.service';
import { MetricsCollectorService } from './monitoring/metrics-collector.service';
import { DashboardGateway } from './monitoring/dashboard.gateway';
import { PaymentGateway } from './types/payment.types';
import { InMemoryPaymentRepository, PAYMENT_REPOSITORY } from './payment/payment.repository';

@Module({
  imports: [EventEmitterModule.forRoot()],
  providers: [
    QueueManagerService,
    PaymentProcessorService,
    MetricsCollectorService,
    DashboardGateway,
    {
      provide: Redis,
      useFactory: () => new Redis({ host: process.env.REDIS_HOST ?? 'localhost' }),
    },
    IdempotencyService,
    // Swap for a real Postgres/Mongo-backed repository in production —
    // this in-memory stand-in exists only so PaymentProcessorService has
    // something to inject for the DB-fallback idempotency check.
    { provide: PAYMENT_REPOSITORY, useClass: InMemoryPaymentRepository },
  ],
  exports: [QueueManagerService, PaymentProcessorService],
})
export class PaymentModule implements OnModuleInit {
  constructor(
    private readonly queueManager: QueueManagerService,
    private readonly processor: PaymentProcessorService,
  ) {}

  async onModuleInit(): Promise<void> {
    // Example wiring — in production this would be driven by a gateway
    // config table/service, not hardcoded, so ops can add a gateway without
    // a deploy.
    const gatewayConfigs: { id: string; concurrency: number; rateLimit: { capacity: number; perSecond: number } }[] = [
      { id: 'stripe', concurrency: 25, rateLimit: { capacity: 100, perSecond: 50 } },
      { id: 'adyen', concurrency: 20, rateLimit: { capacity: 80, perSecond: 40 } },
      { id: 'razorpay', concurrency: 15, rateLimit: { capacity: 60, perSecond: 30 } },
    ];

    for (const cfg of gatewayConfigs) {
      this.queueManager.registerGateway(cfg.id, { concurrency: cfg.concurrency, rateLimit: cfg.rateLimit });

      const mockGateway: PaymentGateway = {
        // Replace with real HTTP client per gateway in production.
        process: async (payment) => ({
          success: Math.random() > 0.05,
          transactionId: `txn_${payment.id}`,
          gatewayId: cfg.id,
        }),
        getStatus: async (transactionId) => ({ transactionId, status: 'settled' }),
      };
      this.processor.registerGateway(cfg.id, mockGateway, cfg.concurrency);

      for (const priority of ['high', 'normal', 'low'] as const) {
        this.queueManager.attachWorker(
          cfg.id,
          priority,
          (job) => this.processor.process(job),
          priority === 'high' ? cfg.concurrency : Math.ceil(cfg.concurrency / 2),
        );
      }
    }
  }
}
