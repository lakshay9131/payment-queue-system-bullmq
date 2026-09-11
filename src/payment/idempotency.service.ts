import { Inject, Injectable } from '@nestjs/common';
import Redis from 'ioredis';
import { GatewayResponse } from '../types/payment.types';

/**
 * Idempotency has two failure modes this design has to cover:
 *
 * 1. Duplicate *enqueue* (client retries an API call) — handled at the
 *    queue layer already, since BullMQ's jobId = idempotencyKey dedupes
 *    on add(). That's necessary but not sufficient.
 *
 * 2. Duplicate *processing* (worker crashes after calling the gateway but
 *    before persisting the result, then BullMQ retries the job) — this is
 *    the dangerous one, because it can mean charging a customer twice. This
 *    service exists specifically for case 2.
 *
 * Approach: SET NX to claim a processing lock before calling the gateway,
 * then overwrite it with the *result* on completion, keyed by idempotency
 * key. A gateway call is only ever made if no result exists yet; if a lock
 * exists but no result (worker died mid-flight), we don't blindly retry —
 * we first call getStatus() on the gateway to check whether the earlier
 * attempt actually went through (see PaymentProcessor.reconcile).
 */
@Injectable()
export class IdempotencyService {
  private readonly LOCK_TTL_SEC = 120; // generous vs. expected gateway timeout
  private readonly RESULT_TTL_SEC = 86_400; // 24h — covers reasonable client retry windows

  constructor(@Inject(Redis) private readonly redis: Redis) {}

  private lockKey(key: string) {
    return `idem:lock:${key}`;
  }
  private resultKey(key: string) {
    return `idem:result:${key}`;
  }

  /** Returns true if this call successfully acquired the processing lock. */
  async acquireLock(idempotencyKey: string): Promise<boolean> {
    const res = await this.redis.set(this.lockKey(idempotencyKey), '1', 'EX', this.LOCK_TTL_SEC, 'NX');
    return res === 'OK';
  }

  async releaseLock(idempotencyKey: string): Promise<void> {
    await this.redis.del(this.lockKey(idempotencyKey));
  }

  async hasLock(idempotencyKey: string): Promise<boolean> {
    const v = await this.redis.get(this.lockKey(idempotencyKey));
    return v !== null;
  }

  async getResult(idempotencyKey: string): Promise<GatewayResponse | null> {
    const raw = await this.redis.get(this.resultKey(idempotencyKey));
    return raw ? (JSON.parse(raw) as GatewayResponse) : null;
  }

  async storeResult(idempotencyKey: string, result: GatewayResponse): Promise<void> {
    await this.redis
      .multi()
      .set(this.resultKey(idempotencyKey), JSON.stringify(result), 'EX', this.RESULT_TTL_SEC)
      .del(this.lockKey(idempotencyKey))
      .exec();
  }
}
