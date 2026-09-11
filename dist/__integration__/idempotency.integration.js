"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
// Real Redis required — start one locally first:
//   redis-server --daemonize yes --port 6379
// Run with: npm run test:integration
//
// This is the test the mocked unit suite structurally cannot give you: it
// proves the lock is atomic under real concurrent access, not just that the
// code calls redis.set() with the right arguments.
const ioredis_1 = __importDefault(require("ioredis"));
const idempotency_service_1 = require("../payment/idempotency.service");
describe('IdempotencyService — real Redis integration', () => {
    let redis;
    let idempotency;
    beforeAll(() => {
        redis = new ioredis_1.default({ host: 'localhost', port: 6379 });
        idempotency = new idempotency_service_1.IdempotencyService(redis);
    });
    afterAll(async () => {
        await redis.quit();
    });
    afterEach(async () => {
        await redis.flushdb(); // isolate each test from the last
    });
    it('only lets ONE of two concurrent acquireLock calls for the same key succeed', async () => {
        // This is the scenario reconcile() exists for: two workers (or a retried
        // job racing a still-processing one) hitting the same idempotencyKey at
        // effectively the same instant. SET NX is atomic in real Redis — this
        // cannot be faithfully proven against a mock, only against the real
        // thing, because a mock's "atomicity" is just whatever we coded it to
        // be, which begs the question.
        const key = 'race_test_key';
        const results = await Promise.all([
            idempotency.acquireLock(key),
            idempotency.acquireLock(key),
            idempotency.acquireLock(key),
            idempotency.acquireLock(key),
            idempotency.acquireLock(key),
        ]);
        const successes = results.filter((r) => r === true);
        expect(successes.length).toBe(1); // exactly one winner, no matter how many raced
    });
    it('storeResult makes the result visible via getResult, and releases the lock', async () => {
        const key = 'roundtrip_key';
        await idempotency.acquireLock(key);
        expect(await idempotency.hasLock(key)).toBe(true);
        await idempotency.storeResult(key, { success: true, transactionId: 'txn_abc', gatewayId: 'stripe' });
        const cached = await idempotency.getResult(key);
        expect(cached).toEqual({ success: true, transactionId: 'txn_abc', gatewayId: 'stripe' });
        expect(await idempotency.hasLock(key)).toBe(false); // storeResult releases the lock (see its multi().del())
    });
    it('a released lock lets a subsequent acquireLock for the same key succeed again', async () => {
        const key = 'reacquire_key';
        expect(await idempotency.acquireLock(key)).toBe(true);
        await idempotency.releaseLock(key);
        expect(await idempotency.acquireLock(key)).toBe(true); // would be false if release didn't actually work
    });
    it('an expired lock (simulated via a very short TTL override) lets a stuck job be retried', async () => {
        // Simulates the "worker crashed, never released the lock" scenario —
        // proves the TTL is what saves you, not application logic.
        const key = 'ttl_test_key';
        await redis.set(`idem:lock:${key}`, '1', 'EX', 1, 'NX'); // 1s TTL, bypassing the service's real 120s for test speed
        expect(await idempotency.hasLock(key)).toBe(true);
        await new Promise((r) => setTimeout(r, 1200));
        expect(await idempotency.hasLock(key)).toBe(false);
        expect(await idempotency.acquireLock(key)).toBe(true); // now succeeds — the crashed "worker's" lock expired
    });
});
//# sourceMappingURL=idempotency.integration.js.map