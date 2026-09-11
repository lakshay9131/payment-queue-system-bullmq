import { Injectable, Logger } from '@nestjs/common';
import { PaymentStatus } from '../types/payment.types';

/**
 * The DB is the source of truth; Redis is a fast cache in front of it.
 * This interface exists specifically for the idempotency fallback path:
 * Redis has a TTL on results (24h) and can lose data on restart or under
 * memory pressure — if that happens, a payment that's already COMPLETED
 * in the DB must not look "never processed" just because Redis forgot.
 *
 * Swap `InMemoryPaymentRepository` for a real Postgres/Mongo-backed
 * implementation in production; the interface is what the processor
 * depends on.
 */
export interface PaymentRepository {
  getStatus(paymentId: string): Promise<{ status: PaymentStatus; gatewayTransactionId?: string } | null>;
  upsertStatus(paymentId: string, status: PaymentStatus, extra?: Record<string, unknown>): Promise<void>;
}

// A TypeScript interface has no runtime representation, so it can't be used
// as a NestJS DI token directly (`provide: PaymentRepository` fails to
// compile — TS2693). This symbol is the actual token; PaymentProcessorService
// injects it with @Inject(PAYMENT_REPOSITORY).
export const PAYMENT_REPOSITORY = Symbol('PaymentRepository');

@Injectable()
export class InMemoryPaymentRepository implements PaymentRepository {
  private readonly logger = new Logger(InMemoryPaymentRepository.name);
  private readonly store = new Map<string, { status: PaymentStatus; gatewayTransactionId?: string }>();

  async getStatus(paymentId: string) {
    return this.store.get(paymentId) ?? null;
  }

  async upsertStatus(paymentId: string, status: PaymentStatus, extra: Record<string, unknown> = {}) {
    this.store.set(paymentId, { status, ...extra });
    this.logger.debug(`DB status for ${paymentId} -> ${status}`);
  }
}
