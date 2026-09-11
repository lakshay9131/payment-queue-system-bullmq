import { PaymentSaga, SagaStep, PaymentSagaContext } from '../payment/saga';

describe('PaymentSaga', () => {
  it('runs all steps in order on success, compensating nothing', async () => {
    const order: string[] = [];
    const steps: SagaStep<PaymentSagaContext>[] = ['a', 'b', 'c'].map((name) => ({
      name,
      execute: async () => {
        order.push(`execute:${name}`);
        return {};
      },
      compensate: async () => {
        order.push(`compensate:${name}`);
      },
    }));

    const saga = new PaymentSaga(steps);
    await saga.run({ paymentId: 'p1', correlationId: 'c1' });

    expect(order).toEqual(['execute:a', 'execute:b', 'execute:c']);
  });

  it('compensates only completed steps, in reverse order, when a later step fails', async () => {
    const order: string[] = [];
    const steps: SagaStep<PaymentSagaContext>[] = [
      {
        name: 'authorize',
        execute: async () => {
          order.push('execute:authorize');
          return {};
        },
        compensate: async () => { order.push('compensate:authorize'); },
      },
      {
        name: 'capture',
        execute: async () => {
          order.push('execute:capture');
          return {};
        },
        compensate: async () => { order.push('compensate:capture'); },
      },
      {
        name: 'write_ledger',
        execute: async () => {
          order.push('execute:write_ledger');
          throw new Error('db write failed');
        },
        compensate: async () => { order.push('compensate:write_ledger'); },
      },
      {
        name: 'notify',
        execute: async () => {
          order.push('execute:notify'); // must never run
          return {};
        },
        compensate: async () => { order.push('compensate:notify'); }, // must never run either
      },
    ];

    const saga = new PaymentSaga(steps);
    await expect(saga.run({ paymentId: 'p1', correlationId: 'c1' })).rejects.toThrow('db write failed');

    // capture and authorize compensated in REVERSE order; write_ledger itself
    // was never "completed" so it's not compensated; notify never executed.
    expect(order).toEqual([
      'execute:authorize',
      'execute:capture',
      'execute:write_ledger',
      'compensate:capture',
      'compensate:authorize',
    ]);
  });

  it('documents raw saga behavior if a final (notify) step throws', async () => {
    // This is the scenario behind the corrected design rule: a notify
    // failure must NOT trigger a refund of the capture in the real
    // PaymentProcessorService. This test shows the saga's generic
    // mechanical behavior in isolation (it compensates anything failure
    // reaches, with no special-casing of "the last step") — the processor
    // itself is responsible for never letting a notify failure take this
    // path in production (it's caught and retried out-of-band instead).
    const order: string[] = [];
    const steps: SagaStep<PaymentSagaContext>[] = [
      { name: 'authorize', execute: async () => { order.push('authorize'); return {}; }, compensate: async () => { order.push('COMPENSATE:authorize'); } },
      { name: 'capture', execute: async () => { order.push('capture'); return {}; }, compensate: async () => { order.push('COMPENSATE:capture'); } },
      { name: 'write_ledger', execute: async () => { order.push('write_ledger'); return {}; }, compensate: async () => { order.push('COMPENSATE:write_ledger'); } },
      { name: 'notify', execute: async () => { order.push('notify'); throw new Error('notification delivery failed'); }, compensate: async () => { order.push('COMPENSATE:notify'); } },
    ];

    const saga = new PaymentSaga(steps);
    await expect(saga.run({ paymentId: 'p1', correlationId: 'c1' })).rejects.toThrow('notification delivery failed');

    expect(order).toEqual([
      'authorize', 'capture', 'write_ledger', 'notify',
      'COMPENSATE:write_ledger', 'COMPENSATE:capture', 'COMPENSATE:authorize',
    ]);
  });
});
