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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.PaymentModule = void 0;
const common_1 = require("@nestjs/common");
const event_emitter_1 = require("@nestjs/event-emitter");
const ioredis_1 = __importDefault(require("ioredis"));
const queue_manager_service_1 = require("./queue/queue-manager.service");
const payment_processor_service_1 = require("./payment/payment-processor.service");
const idempotency_service_1 = require("./payment/idempotency.service");
const metrics_collector_service_1 = require("./monitoring/metrics-collector.service");
const dashboard_gateway_1 = require("./monitoring/dashboard.gateway");
const payment_repository_1 = require("./payment/payment.repository");
let PaymentModule = class PaymentModule {
    constructor(queueManager, processor) {
        this.queueManager = queueManager;
        this.processor = processor;
    }
    async onModuleInit() {
        // Example wiring — in production this would be driven by a gateway
        // config table/service, not hardcoded, so ops can add a gateway without
        // a deploy.
        const gatewayConfigs = [
            { id: 'stripe', concurrency: 25, rateLimit: { capacity: 100, perSecond: 50 } },
            { id: 'adyen', concurrency: 20, rateLimit: { capacity: 80, perSecond: 40 } },
            { id: 'razorpay', concurrency: 15, rateLimit: { capacity: 60, perSecond: 30 } },
        ];
        for (const cfg of gatewayConfigs) {
            this.queueManager.registerGateway(cfg.id, { concurrency: cfg.concurrency, rateLimit: cfg.rateLimit });
            const mockGateway = {
                // Replace with real HTTP client per gateway in production.
                process: async (payment) => ({
                    success: Math.random() > 0.05,
                    transactionId: `txn_${payment.id}`,
                    gatewayId: cfg.id,
                }),
                getStatus: async (transactionId) => ({ transactionId, status: 'settled' }),
            };
            this.processor.registerGateway(cfg.id, mockGateway, cfg.concurrency);
            for (const priority of ['high', 'normal', 'low']) {
                this.queueManager.attachWorker(cfg.id, priority, (job) => this.processor.process(job), priority === 'high' ? cfg.concurrency : Math.ceil(cfg.concurrency / 2));
            }
        }
    }
};
exports.PaymentModule = PaymentModule;
exports.PaymentModule = PaymentModule = __decorate([
    (0, common_1.Module)({
        imports: [event_emitter_1.EventEmitterModule.forRoot()],
        providers: [
            queue_manager_service_1.QueueManagerService,
            payment_processor_service_1.PaymentProcessorService,
            metrics_collector_service_1.MetricsCollectorService,
            dashboard_gateway_1.DashboardGateway,
            {
                provide: ioredis_1.default,
                useFactory: () => new ioredis_1.default({ host: process.env.REDIS_HOST ?? 'localhost' }),
            },
            idempotency_service_1.IdempotencyService,
            // Swap for a real Postgres/Mongo-backed repository in production —
            // this in-memory stand-in exists only so PaymentProcessorService has
            // something to inject for the DB-fallback idempotency check.
            { provide: payment_repository_1.PAYMENT_REPOSITORY, useClass: payment_repository_1.InMemoryPaymentRepository },
        ],
        exports: [queue_manager_service_1.QueueManagerService, payment_processor_service_1.PaymentProcessorService],
    }),
    __metadata("design:paramtypes", [queue_manager_service_1.QueueManagerService,
        payment_processor_service_1.PaymentProcessorService])
], PaymentModule);
//# sourceMappingURL=payment.module.js.map