const { Queue } = require('bullmq');
const crypto = require('node:crypto');

const connection = {
  host: process.env.REDIS_HOST || 'localhost',
  port: Number(process.env.REDIS_PORT || 6379),
};

const gateways = ['stripe', 'adyen', 'razorpay'];
const priorities = ['high', 'normal', 'low'];
const durationSeconds = Number(process.env.DURATION_SECONDS || 60);
const ratePerSecond = Number(process.env.RATE_PER_SECOND || 100);
const intervalMs = Math.max(1, Math.floor(1000 / ratePerSecond));

const queues = new Map();

for (const gateway of gateways) {
  for (const priority of priorities) {
    queues.set(
      `${gateway}:${priority}`,
      new Queue(`payments-${gateway}-${priority}`, { connection }),
    );
  }
}

function createPayment(index) {
  const gatewayId = gateways[index % gateways.length];
  const priority = priorities[index % priorities.length];
  const id = crypto.randomUUID();

  return {
    id,
    amount: 1000 + (index % 100) * 100,
    currency: 'USD',
    customerId: `load-test-customer-${index % 100}`,
    gatewayId,
    priority,
    retryCount: 0,
    maxRetries: 3,
    status: 'queued',
    metadata: {
      source: 'load-test',
      sequence: index,
    },
    createdAt: new Date().toISOString(),
    idempotencyKey: `load-test-${id}`,
    correlationId: `load-test-correlation-${id}`,
  };
}

async function main() {
  const startedAt = Date.now();
  const deadline = startedAt + durationSeconds * 1000;
  let index = 0;
  let lastReportAt = startedAt;

  console.log(
    `Sending payments for ${durationSeconds}s at approximately ${ratePerSecond} payments/sec...`,
  );

  while (Date.now() < deadline) {
    const payment = createPayment(index);
    const queue = queues.get(`${payment.gatewayId}:${payment.priority}`);

    await queue.add('process-payment', payment, {
      jobId: payment.idempotencyKey,
      priority: payment.priority === 'high'
        ? 1
        : payment.priority === 'normal'
          ? 5
          : 10,
      attempts: payment.maxRetries + 1,
      removeOnComplete: {
        age: 3600,
        count: 10000,
      },
      removeOnFail: false,
    });

    index++;

    if (Date.now() - lastReportAt >= 5000) {
      const elapsedSeconds = ((Date.now() - startedAt) / 1000).toFixed(0);
      console.log(`Elapsed ${elapsedSeconds}s: enqueued ${index} payments`);
      lastReportAt = Date.now();
    }

    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }

  await Promise.all([...queues.values()].map((queue) => queue.close()));

  console.log(`Load test complete. Enqueued ${index} payments over ${durationSeconds}s.`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});