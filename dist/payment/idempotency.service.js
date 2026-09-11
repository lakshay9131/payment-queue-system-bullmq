"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
var __param = (this && this.__param) || function (paramIndex, decorator) {
    return function (target, key) { decorator(target, key, paramIndex); }
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.IdempotencyService = void 0;
const common_1 = require("@nestjs/common");
const ioredis_1 = __importDefault(require("ioredis"));
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
let IdempotencyService = class IdempotencyService {
    constructor(redis) {
        this.redis = redis;
        this.LOCK_TTL_SEC = 120; // generous vs. expected gateway timeout
        this.RESULT_TTL_SEC = 86_400; // 24h — covers reasonable client retry windows
    }
    lockKey(key) {
        return `idem:lock:${key}`;
    }
    resultKey(key) {
        return `idem:result:${key}`;
    }
    /** Returns true if this call successfully acquired the processing lock. */
    async acquireLock(idempotencyKey) {
        const res = await this.redis.set(this.lockKey(idempotencyKey), '1', 'EX', this.LOCK_TTL_SEC, 'NX');
        return res === 'OK';
    }
    async releaseLock(idempotencyKey) {
        await this.redis.del(this.lockKey(idempotencyKey));
    }
    async hasLock(idempotencyKey) {
        const v = await this.redis.get(this.lockKey(idempotencyKey));
        return v !== null;
    }
    async getResult(idempotencyKey) {
        const raw = await this.redis.get(this.resultKey(idempotencyKey));
        return raw ? JSON.parse(raw) : null;
    }
    async storeResult(idempotencyKey, result) {
        await this.redis
            .multi()
            .set(this.resultKey(idempotencyKey), JSON.stringify(result), 'EX', this.RESULT_TTL_SEC)
            .del(this.lockKey(idempotencyKey))
            .exec();
    }
};
exports.IdempotencyService = IdempotencyService;
exports.IdempotencyService = IdempotencyService = __decorate([
    (0, common_1.Injectable)(),
    __param(0, (0, common_1.Inject)(ioredis_1.default)),
    __metadata("design:paramtypes", [ioredis_1.default])
], IdempotencyService);
//# sourceMappingURL=idempotency.service.js.map