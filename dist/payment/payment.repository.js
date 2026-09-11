"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var InMemoryPaymentRepository_1;
Object.defineProperty(exports, "__esModule", { value: true });
exports.InMemoryPaymentRepository = exports.PAYMENT_REPOSITORY = void 0;
const common_1 = require("@nestjs/common");
// A TypeScript interface has no runtime representation, so it can't be used
// as a NestJS DI token directly (`provide: PaymentRepository` fails to
// compile — TS2693). This symbol is the actual token; PaymentProcessorService
// injects it with @Inject(PAYMENT_REPOSITORY).
exports.PAYMENT_REPOSITORY = Symbol('PaymentRepository');
let InMemoryPaymentRepository = InMemoryPaymentRepository_1 = class InMemoryPaymentRepository {
    constructor() {
        this.logger = new common_1.Logger(InMemoryPaymentRepository_1.name);
        this.store = new Map();
    }
    async getStatus(paymentId) {
        return this.store.get(paymentId) ?? null;
    }
    async upsertStatus(paymentId, status, extra = {}) {
        this.store.set(paymentId, { status, ...extra });
        this.logger.debug(`DB status for ${paymentId} -> ${status}`);
    }
};
exports.InMemoryPaymentRepository = InMemoryPaymentRepository;
exports.InMemoryPaymentRepository = InMemoryPaymentRepository = InMemoryPaymentRepository_1 = __decorate([
    (0, common_1.Injectable)()
], InMemoryPaymentRepository);
//# sourceMappingURL=payment.repository.js.map