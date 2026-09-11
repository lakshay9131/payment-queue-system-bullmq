import { Logger } from '@nestjs/common';

/**
 * A single step in the payment saga. `compensate` must be safe to call even
 * if `execute` partially failed (e.g. never called, or called but the
 * response was lost) — compensations should be written to be idempotent
 * themselves, not just the forward steps.
 */
export interface SagaStep<TContext> {
  name: string;
  execute: (ctx: TContext) => Promise<Partial<TContext>>;
  compensate: (ctx: TContext) => Promise<void>;
}

export interface PaymentSagaContext {
  paymentId: string;
  correlationId: string;
  reserved?: boolean; // funds reserved/authorized
  gatewayTransactionId?: string;
  captured?: boolean;
  ledgerEntryId?: string;
  notified?: boolean;
}

/**
 * Why a saga (vs. a single try/catch around one gateway call): a real
 * payment isn't one operation. It's "reserve funds -> capture -> write
 * ledger entry -> notify customer", each against a different system
 * (gateway, internal ledger DB, notification service). If step 3 fails
 * after step 2 succeeded, we must not leave a captured payment with no
 * ledger record — that's a silent money leak. The saga runs steps in
 * order and, on any failure, compensates completed steps in *reverse*
 * order, so the system is left in a consistent state either way.
 *
 * This is an orchestration-based saga (a central coordinator), not
 * choreography (event-driven, each service reacting to the last). For a
 * payments pipeline specifically, orchestration is preferable because the
 * failure semantics need to be centrally auditable for compliance — with
 * choreography, reconstructing "what happened and in what order" from a
 * stream of independent events is much harder to prove correct in an audit.
 */
export class PaymentSaga<TContext extends PaymentSagaContext> {
  private readonly logger = new Logger('PaymentSaga');
  private completedSteps: SagaStep<TContext>[] = [];

  constructor(private readonly steps: SagaStep<TContext>[]) {}

  async run(initialContext: TContext): Promise<TContext> {
    let ctx = initialContext;
    this.completedSteps = [];

    for (const step of this.steps) {
      try {
        this.logger.log(`[${ctx.correlationId}] saga step "${step.name}" starting`);
        const patch = await step.execute(ctx);
        ctx = { ...ctx, ...patch };
        this.completedSteps.push(step);
      } catch (err) {
        this.logger.error(`[${ctx.correlationId}] saga step "${step.name}" failed: ${(err as Error).message}`);
        await this.compensate(ctx);
        throw err; // re-throw so the caller (PaymentProcessor) can classify + retry/DLQ
      }
    }

    return ctx;
  }

  private async compensate(ctx: TContext): Promise<void> {
    for (const step of [...this.completedSteps].reverse()) {
      try {
        this.logger.warn(`[${ctx.correlationId}] compensating step "${step.name}"`);
        await step.compensate(ctx);
      } catch (compErr) {
        // Compensation failures are the worst case: manual intervention
        // territory. Log loudly with everything needed for a human to
        // reconcile, and never swallow this silently.
        this.logger.error(
          `[${ctx.correlationId}] COMPENSATION FAILED for step "${step.name}": ${(compErr as Error).message}. ` +
            `Requires manual reconciliation — paymentId=${ctx.paymentId}`,
        );
      }
    }
  }
}

/**
 * Concrete steps for a card payment. Wired up with real gateway/ledger/
 * notification clients via constructor injection in PaymentProcessor.
 */
export function buildPaymentSagaSteps(deps: {
  authorize: (ctx: PaymentSagaContext) => Promise<{ gatewayTransactionId: string }>;
  voidAuthorization: (ctx: PaymentSagaContext) => Promise<void>;
  capture: (ctx: PaymentSagaContext) => Promise<void>;
  refund: (ctx: PaymentSagaContext) => Promise<void>;
  writeLedgerEntry: (ctx: PaymentSagaContext) => Promise<{ ledgerEntryId: string }>;
  reverseLedgerEntry: (ctx: PaymentSagaContext) => Promise<void>;
  notifyCustomer: (ctx: PaymentSagaContext) => Promise<void>;
}): SagaStep<PaymentSagaContext>[] {
  return [
    {
      name: 'authorize',
      execute: async (ctx) => {
        const { gatewayTransactionId } = await deps.authorize(ctx);
        return { reserved: true, gatewayTransactionId };
      },
      compensate: async (ctx) => {
        if (ctx.reserved) await deps.voidAuthorization(ctx);
      },
    },
    {
      name: 'capture',
      execute: async (ctx) => {
        await deps.capture(ctx);
        return { captured: true };
      },
      compensate: async (ctx) => {
        if (ctx.captured) await deps.refund(ctx);
      },
    },
    {
      name: 'write_ledger',
      execute: async (ctx) => {
        const { ledgerEntryId } = await deps.writeLedgerEntry(ctx);
        return { ledgerEntryId };
      },
      compensate: async (ctx) => {
        if (ctx.ledgerEntryId) await deps.reverseLedgerEntry(ctx);
      },
    },
    {
      name: 'notify_customer',
      execute: async (ctx) => {
        await deps.notifyCustomer(ctx);
        return { notified: true };
      },
      // A failed notification does NOT need to unwind money movement —
      // it's not compensated with a refund. It's just retried out-of-band
      // (e.g. via a notification-retry queue), which is why it's the last
      // step: everything before it is money-affecting and must be safe to
      // compensate; this step alone is safe to leave "pending" without
      // rolling back the whole payment.
      compensate: async () => {
        /* no-op by design */
      },
    },
  ];
}
