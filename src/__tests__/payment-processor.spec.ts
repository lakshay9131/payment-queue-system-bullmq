import { PaymentProcessorService } from '../payment/payment-processor.service';
import { PaymentStatus, Payment, PaymentGateway } from '../types/payment.types';

function makePayment(overrides: Partial<Payment> = {}): Payment {
  return {
    id: 'pay_1',
    amount: 1000,
    currency: 'USD',
    customerId: 'cust_1',
    gatewayId: 'stripe',
    priority: 'normal',
    retryCount: 0,
    maxRetries: 3,
    status: PaymentStatus.QUEUED,
    metadata: {},
    createdAt: new Date(),
    idempotencyKey: 'pay_1',
    correlationId: 'corr_1',
    ...overrides,
  };
}

function makeJob(payment: Payment, attemptsMade = 0) {
  return { data: payment, attemptsMade, id: 'job_1' } as any;
}

describe('PaymentProcessorService — two-tier idempotency', () => {
  let queueManager: any;
  let idempotency: any;
  let paymentRepo: any;
  let events: any;
  let gateway: PaymentGateway;
  let processor: PaymentProcessorService;

  beforeEach(() => {
    queueManager = {
      getRateLimiter: jest.fn().mockReturnValue({ tryConsume: () => true, msUntilAvailable: () => 0 }),
      moveToDeadLetter: jest.fn(),
    };
    idempotency = {
      getResult: jest.fn(),
      acquireLock: jest.fn().mockResolvedValue(true),
      releaseLock: jest.fn(),
      hasLock: jest.fn(),
      storeResult: jest.fn(),
    };
    paymentRepo = {
      getStatus: jest.fn(),
      upsertStatus: jest.fn(),
    };
    events = { emit: jest.fn() };
    gateway = {
      process: jest.fn().mockResolvedValue({ success: true, transactionId: 'txn_1', gatewayId: 'stripe' }),
      getStatus: jest.fn(),
    };

    processor = new PaymentProcessorService(queueManager, idempotency, paymentRepo, events);
    processor.registerGateway('stripe', gateway, 10);
  });

  it('skips the gateway entirely on a Redis cache hit', async () => {
    idempotency.getResult.mockResolvedValue({ success: true, transactionId: 'txn_old', gatewayId: 'stripe' });

    await processor.process(makeJob(makePayment()));

    expect(gateway.process).not.toHaveBeenCalled();
    expect(paymentRepo.getStatus).not.toHaveBeenCalled(); // never falls through to DB check
    expect(events.emit).toHaveBeenCalledWith(
      'payment.status_changed',
      expect.objectContaining({ status: PaymentStatus.COMPLETED }),
    );
  });

  it('falls back to the DB on a Redis miss and skips the gateway if DB says COMPLETED', async () => {
    idempotency.getResult.mockResolvedValue(null); // redis miss (evicted/expired)
    paymentRepo.getStatus.mockResolvedValue({ status: PaymentStatus.COMPLETED, gatewayTransactionId: 'txn_old' });

    await processor.process(makeJob(makePayment()));

    expect(paymentRepo.getStatus).toHaveBeenCalledWith('pay_1');
    expect(gateway.process).not.toHaveBeenCalled(); // DB fallback caught it — no double charge
    expect(idempotency.storeResult).toHaveBeenCalledWith(
      'pay_1',
      expect.objectContaining({ transactionId: 'txn_old' }),
    ); // backfills redis so the fast path works next time
  });

  it('proceeds to acquire the lock and call the gateway when both Redis and DB miss', async () => {
    idempotency.getResult.mockResolvedValue(null);
    paymentRepo.getStatus.mockResolvedValue(null); // never processed before

    await processor.process(makeJob(makePayment()));

    expect(idempotency.acquireLock).toHaveBeenCalledWith('pay_1');
    expect(gateway.process).toHaveBeenCalledTimes(1);
  });

  it('stores the idempotency result during the ledger step, not after notify', async () => {
    idempotency.getResult.mockResolvedValue(null);
    paymentRepo.getStatus.mockResolvedValue(null);

    const callOrder: string[] = [];
    idempotency.storeResult.mockImplementation(async () => {
      callOrder.push('storeResult');
    });
    events.emit.mockImplementation((eventName: string) => {
      if (eventName === 'payment.notify') callOrder.push('notify');
    });

    await processor.process(makeJob(makePayment()));

    // storeResult (lock release) must happen before the notify event fires —
    // this is the corrected "release inline in ledger step" behavior.
    expect(callOrder.indexOf('storeResult')).toBeLessThan(callOrder.indexOf('notify'));
    expect(paymentRepo.upsertStatus).toHaveBeenCalledWith(
      'pay_1',
      PaymentStatus.COMPLETED,
      expect.objectContaining({ gatewayTransactionId: 'txn_1' }),
    );
  });

  it('routes a permanently-failing gateway response straight to the DLQ without retry', async () => {
    idempotency.getResult.mockResolvedValue(null);
    paymentRepo.getStatus.mockResolvedValue(null);
    (gateway.process as jest.Mock).mockResolvedValue({ success: false, message: 'card declined', transactionId: '', gatewayId: 'stripe' });

    await processor.process(makeJob(makePayment()));

    expect(queueManager.moveToDeadLetter).toHaveBeenCalledTimes(1);
    expect(idempotency.releaseLock).toHaveBeenCalledWith('pay_1');
  });
});
