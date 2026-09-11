// ============================================================================
// Core domain types
// ============================================================================

export type Priority = 'high' | 'normal' | 'low';

export enum PaymentStatus {
  QUEUED = 'queued',
  PROCESSING = 'processing',
  COMPLETED = 'completed',
  FAILED = 'failed',
  DEAD_LETTER = 'dead_letter',
  COMPENSATING = 'compensating',
  COMPENSATED = 'compensated',
}

export interface Payment {
  id: string;
  amount: number;
  currency: string;
  customerId: string;
  gatewayId: string;
  priority: Priority;
  retryCount: number;
  maxRetries: number;
  status: PaymentStatus;
  metadata: Record<string, any>;
  createdAt: Date;
  processedAt?: Date;
  failureReason?: string;
  // Added for idempotency / tracing — the interview spec's Payment shape is
  // extended here rather than replaced, since real gateways need these.
  idempotencyKey: string;
  correlationId: string;
}

export interface GatewayResponse {
  success: boolean;
  transactionId: string;
  gatewayId: string;
  rawCode?: string;
  message?: string;
}

export interface GatewayStatus {
  transactionId: string;
  status: 'pending' | 'settled' | 'failed' | 'unknown';
}

export interface PaymentGateway {
  process(payment: Payment): Promise<GatewayResponse>;
  getStatus(transactionId: string): Promise<GatewayStatus>;
}

// ============================================================================
// Failure classification — this is the single most important modeling
// decision in the whole system. Everything downstream (retry policy,
// circuit breaker, alerting) branches on this.
// ============================================================================

export enum FailureCategory {
  TRANSIENT = 'transient', // network blip, gateway 5xx, timeout — retry
  RATE_LIMITED = 'rate_limited', // gateway said slow down — retry with backoff, don't trip breaker as hard
  PERMANENT = 'permanent', // card declined, invalid account — never retry
  UNKNOWN = 'unknown', // couldn't classify — treat conservatively as transient but cap retries lower
}

export class ClassifiedError extends Error {
  constructor(
    message: string,
    public readonly category: FailureCategory,
    public readonly gatewayCode?: string,
  ) {
    super(message);
    this.name = 'ClassifiedError';
  }
}

export function classifyFailure(err: unknown): FailureCategory {
  if (err instanceof ClassifiedError) return err.category;

  const message = err instanceof Error ? err.message.toLowerCase() : String(err).toLowerCase();

  // Permanent: gateway told us definitively "no" — retrying wastes money on
  // fees and can trigger fraud flags on some gateways.
  if (/declined|insufficient_funds|invalid_card|fraud|account_closed|invalid_account/.test(message)) {
    return FailureCategory.PERMANENT;
  }
  if (/rate.?limit|429|too many requests/.test(message)) {
    return FailureCategory.RATE_LIMITED;
  }
  if (/timeout|etimedout|econnreset|econnrefused|502|503|504|network/.test(message)) {
    return FailureCategory.TRANSIENT;
  }
  return FailureCategory.UNKNOWN;
}
