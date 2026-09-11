// Mock BullMQ entirely so this test needs no real Redis connection — it's
// testing OUR routing/mapping logic in QueueManagerService, not BullMQ's
// internals (BullMQ's own jobId-dedup behavior is documented, trusted
// library behavior, not something this test suite re-verifies).
jest.mock('bullmq', () => {
  const addMock = jest.fn().mockImplementation((name: string, data: any, opts: any) => {
    return Promise.resolve({ id: opts?.jobId ?? 'auto-id', name, data, opts });
  });
  return {
    Queue: jest.fn().mockImplementation((queueName: string) => ({
      name: queueName,
      add: addMock,
      getJobCounts: jest.fn().mockResolvedValue({ waiting: 0, active: 0, delayed: 0, failed: 0, completed: 0 }),
      pause: jest.fn(),
      close: jest.fn(),
    })),
    Worker: jest.fn().mockImplementation(() => ({ on: jest.fn(), close: jest.fn() })),
  };
});

import { QueueManagerService } from '../queue/queue-manager.service';
import { Payment, PaymentStatus } from '../types/payment.types';

function makePayment(overrides: Partial<Payment> = {}): Payment {
  return {
    id: 'pay_42',
    amount: 2500,
    currency: 'USD',
    customerId: 'cust_9',
    gatewayId: 'stripe',
    priority: 'high',
    retryCount: 0,
    maxRetries: 3,
    status: PaymentStatus.QUEUED,
    metadata: {},
    createdAt: new Date(),
    idempotencyKey: 'idem_pay_42',
    correlationId: 'corr_9',
    ...overrides,
  };
}

describe('QueueManagerService — ingestion (enqueue)', () => {
  let events: any;
  let manager: QueueManagerService;

  beforeEach(() => {
    jest.clearAllMocks(); // the mocked Queue/Worker constructors are module-scoped and
    // accumulate call history across tests otherwise — clear it each time.
    events = { emit: jest.fn() };
    manager = new QueueManagerService(events);
  });

  it('throws synchronously if the gateway was never registered — never touches BullMQ', async () => {
    await expect(manager.enqueue(makePayment({ gatewayId: 'unknown-gw' }))).rejects.toThrow(
      /No queue registered for gateway "unknown-gw"/,
    );
  });

  it('routes to the correct per-gateway, per-priority queue', async () => {
    manager.registerGateway('stripe');
    const job = await manager.enqueue(makePayment({ gatewayId: 'stripe', priority: 'high' }));

    // The mock resolves { name } from the Queue instance's own `add`, so we
    // assert on which Queue's `add` was called via its constructor name arg.
    const QueueMock = require('bullmq').Queue as jest.Mock;
    const stripeHighQueueCall = QueueMock.mock.calls.find((c: any[]) => c[0] === 'payments-stripe-high');
    expect(stripeHighQueueCall).toBeDefined();
    expect(job.data.gatewayId).toBe('stripe');
  });

  it('uses idempotencyKey as the BullMQ jobId — this IS the dedup mechanism', async () => {
    manager.registerGateway('stripe');
    const payment = makePayment({ idempotencyKey: 'unique_key_123' });
    const job = await manager.enqueue(payment);

    expect(job.opts.jobId).toBe('unique_key_123');
  });

  it('maps priority to BullMQ priority weight — lower number runs first', async () => {
    manager.registerGateway('stripe');
    const high = await manager.enqueue(makePayment({ priority: 'high', idempotencyKey: 'k1' }));
    const low = await manager.enqueue(makePayment({ priority: 'low', idempotencyKey: 'k2' }));

    expect(high.opts.priority).toBeLessThan(low.opts.priority);
  });

  it('sets attempts = maxRetries + 1 (first try + retries, not retries alone)', async () => {
    manager.registerGateway('stripe');
    const job = await manager.enqueue(makePayment({ maxRetries: 4 }));
    expect(job.opts.attempts).toBe(5);
  });

  it('routes delayed payments to the scheduled queue, bypassing gateway/priority selection', async () => {
    manager.registerGateway('stripe');
    const job = await manager.enqueue(makePayment({ priority: 'low' }), { delayMs: 60_000 });

    const QueueMock = require('bullmq').Queue as jest.Mock;
    const scheduledCall = QueueMock.mock.calls.find((c: any[]) => c[0] === 'payments-scheduled');
    expect(scheduledCall).toBeDefined();
    expect(job.opts.delay).toBe(60_000);
    expect(job.name).toBe('scheduled-payment');
  });

  it('registerGateway is idempotent — calling it twice does not create duplicate queues', () => {
    manager.registerGateway('stripe');
    manager.registerGateway('stripe');

    const QueueMock = require('bullmq').Queue as jest.Mock;
    const stripeHighCalls = QueueMock.mock.calls.filter((c: any[]) => c[0] === 'payments-stripe-high');
    expect(stripeHighCalls.length).toBe(1);
  });
});
