// Real Redis required — see idempotency.integration.ts header.
//
// This proves the ACTUAL claim made throughout this project: "jobId =
// idempotencyKey gives you dedup for free." That's a claim about BullMQ's
// real behavior against real Redis — mocking Queue.add() (as the unit
// suite does) can only assert we PASS the right jobId, never that BullMQ
// actually honors it as a dedup key. This is the test that closes that gap.
import { Queue } from 'bullmq';
import Redis from 'ioredis';

describe('BullMQ jobId dedup — real Redis integration', () => {
  const connection = { host: 'localhost', port: 6379 };
  let queue: Queue;
  let redis: Redis;

  beforeAll(() => {
    queue = new Queue('integration-test-dedup', { connection });
    redis = new Redis(connection);
  });

  afterAll(async () => {
    await queue.obliterate({ force: true });
    await queue.close();
    await redis.quit();
  });

  it('adding the same jobId twice does NOT create a second job', async () => {
    const idempotencyKey = 'dedup_test_key';

    const first = await queue.add('process-payment', { amount: 1000 }, { jobId: idempotencyKey });
    const second = await queue.add('process-payment', { amount: 1000 }, { jobId: idempotencyKey });

    // BullMQ returns the SAME job instance for a repeated jobId — this is
    // the real mechanism behind "duplicate enqueue is handled for free."
    expect(second.id).toBe(first.id);

    const counts = await queue.getJobCounts('waiting');
    expect(counts.waiting).toBe(1); // not 2
  });

  it('a different jobId for otherwise-identical data IS enqueued separately', async () => {
    const before = await queue.getJobCounts('waiting');
    await queue.add('process-payment', { amount: 500 }, { jobId: 'unique_a' });
    await queue.add('process-payment', { amount: 500 }, { jobId: 'unique_b' });
    const after = await queue.getJobCounts('waiting');

    // Confirms dedup is keyed on jobId specifically, not on payload content
    // — two payments with identical amounts are NOT treated as duplicates
    // unless they share an idempotencyKey.
    expect(after.waiting).toBe(before.waiting + 2);
  });
});
