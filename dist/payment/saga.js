"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.PaymentSaga = void 0;
exports.buildPaymentSagaSteps = buildPaymentSagaSteps;
const common_1 = require("@nestjs/common");
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
class PaymentSaga {
    constructor(steps) {
        this.steps = steps;
        this.logger = new common_1.Logger('PaymentSaga');
        this.completedSteps = [];
    }
    async run(initialContext) {
        let ctx = initialContext;
        this.completedSteps = [];
        for (const step of this.steps) {
            try {
                this.logger.log(`[${ctx.correlationId}] saga step "${step.name}" starting`);
                const patch = await step.execute(ctx);
                ctx = { ...ctx, ...patch };
                this.completedSteps.push(step);
            }
            catch (err) {
                this.logger.error(`[${ctx.correlationId}] saga step "${step.name}" failed: ${err.message}`);
                await this.compensate(ctx);
                throw err; // re-throw so the caller (PaymentProcessor) can classify + retry/DLQ
            }
        }
        return ctx;
    }
    async compensate(ctx) {
        for (const step of [...this.completedSteps].reverse()) {
            try {
                this.logger.warn(`[${ctx.correlationId}] compensating step "${step.name}"`);
                await step.compensate(ctx);
            }
            catch (compErr) {
                // Compensation failures are the worst case: manual intervention
                // territory. Log loudly with everything needed for a human to
                // reconcile, and never swallow this silently.
                this.logger.error(`[${ctx.correlationId}] COMPENSATION FAILED for step "${step.name}": ${compErr.message}. ` +
                    `Requires manual reconciliation — paymentId=${ctx.paymentId}`);
            }
        }
    }
}
exports.PaymentSaga = PaymentSaga;
/**
 * Concrete steps for a card payment. Wired up with real gateway/ledger/
 * notification clients via constructor injection in PaymentProcessor.
 */
function buildPaymentSagaSteps(deps) {
    return [
        {
            name: 'authorize',
            execute: async (ctx) => {
                const { gatewayTransactionId } = await deps.authorize(ctx);
                return { reserved: true, gatewayTransactionId };
            },
            compensate: async (ctx) => {
                if (ctx.reserved)
                    await deps.voidAuthorization(ctx);
            },
        },
        {
            name: 'capture',
            execute: async (ctx) => {
                await deps.capture(ctx);
                return { captured: true };
            },
            compensate: async (ctx) => {
                if (ctx.captured)
                    await deps.refund(ctx);
            },
        },
        {
            name: 'write_ledger',
            execute: async (ctx) => {
                const { ledgerEntryId } = await deps.writeLedgerEntry(ctx);
                return { ledgerEntryId };
            },
            compensate: async (ctx) => {
                if (ctx.ledgerEntryId)
                    await deps.reverseLedgerEntry(ctx);
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
//# sourceMappingURL=saga.js.map