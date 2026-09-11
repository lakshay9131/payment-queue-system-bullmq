# Distributed Payment Processing Queue System

A NestJS + BullMQ implementation for a 50K+ payments/hour queue system, plus
written answers for the Part 2 architecture discussion.

# Commands
docker run -d --name redis-test -p 6379:6379 redis

## Layout

```
src/
  types/payment.types.ts        Domain types, failure classification
  queue/
    token-bucket.ts             Adaptive rate limiter (per gateway)
    queue-manager.service.ts    Multi-tier queues, workers, graceful shutdown
  payment/
    circuit-breaker.ts          Per-gateway circuit breaker
    backoff.ts                  Exponential backoff with full jitter
    idempotency.service.ts      Redis-backed idempotency (lock + result cache)
    saga.ts                     Saga orchestrator + payment saga steps
    payment-processor.service.ts  Ties it all together per job
  monitoring/
    metrics-collector.service.ts  TPS, success rate, P95/P99
    dashboard.gateway.ts          WebSocket push to dashboard clients
  tracing/tracing.ts             OTel spans keyed by correlation ID
  payment.module.ts              Wiring / example gateway registration
```

---

## Part 1 design notes (the "why", not just the "what")

**Queue topology.** Queues are named `payments:{gatewayId}:{priority}`, so
isolation is per-gateway *and* ordering is per-priority within that. A single
degraded gateway can never starve payments to a healthy one — that's the
bulkhead pattern. BullMQ's own priority field only orders within one queue,
so priority alone doesn't give isolation; the two dimensions solve different
problems and both are needed.

**Two separate throttles, on purpose.** The token bucket controls *rate*
(requests/sec a gateway will tolerate); the `GatewaySemaphore` in the
processor controls *concurrency* (how many requests are in flight at once).
A gateway can be rate-tolerant but concurrency-sensitive or vice versa —
collapsing these into one knob would make the system less accurate at
respecting whatever the real gateway's actual constraint is.

**Idempotency is two different problems.** Duplicate *enqueue* (client
retries the API call) is solved for free by setting BullMQ's `jobId` to the
idempotency key. Duplicate *processing* (worker dies after calling the
gateway but before persisting the result) is the dangerous one — it's how
customers get double-charged — and needs its own lock+result cache in Redis,
plus a reconciliation path that asks the gateway directly what happened
rather than guessing.

**Saga over a single try/catch.** A payment is "authorize → capture → write
ledger → notify," each against a different system. A single try/catch
around one gateway call can't express "money moved but the ledger write
failed, unwind the money." The saga runs steps in order and compensates
completed steps in reverse on any failure. Orchestration (a central
coordinator) was chosen over choreography (event-driven) specifically
because payments need a centrally auditable record of what happened and in
what order — that's much harder to reconstruct from a stream of independent
events after the fact, which matters for compliance review.

**Failure classification drives everything downstream.** `classifyFailure`
splits errors into transient / rate-limited / permanent / unknown.
Permanent failures (declined card, invalid account) go straight to the DLQ
without burning retries — retrying those wastes gateway fees and can trigger
fraud flags on some processors. Transient failures retry with full-jitter
backoff. This one branch point is what makes the rest of the error handling
correct instead of ad hoc.

**Circuit breaker feeds the rate limiter.** Rather than the breaker and
rate limiter acting independently, the breaker's state transitions call
`bucket.adapt()` — as a gateway degrades, the token bucket proactively
shrinks before the breaker fully trips, and resets on recovery. This is the
"adaptive rate limiting based on failures" requirement implemented as a
feedback loop rather than two unrelated mechanisms.

---

## Payment lifecycle: inception to completion

This is the runtime path for one payment. The `payment.id`, `idempotencyKey`,
and `correlationId` travel through the flow so duplicate requests can be
recognized and the attempt can be traced.

### 1. A payment enters the queue

The application creates a payment job for a gateway and priority queue. The
idempotency key is used as the BullMQ job ID, so a client retry does not create
another queue job for the same payment.

```text
client request
  -> payment status = QUEUED
  -> payments:{gatewayId}:{priority}
```

### 2. The worker accepts the job

`PaymentProcessorService.process(job)` is the worker entry point. It checks
that the gateway is registered, the circuit breaker allows an attempt, and the
token bucket has capacity. A failed gate throws a classified error for later
retry. The per-gateway semaphore then limits in-flight gateway work across all
priority workers.

```text
process(job)
  -> circuit breaker check
  -> rate-limit check
  -> emit payment.started
  -> acquire gateway semaphore
```

### 3. Idempotency is checked

Redis is checked first. A cached result skips gateway work and finalizes the
payment. On a Redis miss, the primary database is checked; a completed record
backfills Redis and also skips the gateway.

```text
Redis hit             -> finalize COMPLETED, stop
Redis miss + DB done  -> backfill Redis, finalize COMPLETED, stop
Both miss             -> acquire distributed idempotency lock
```

If the lock is already held, another worker owns the attempt. The processor
calls `reconcile()` and asks the gateway for status instead of making a second
charge. A settled result is cached and finalized; pending or unknown results
wait for a later retry.

### 4. The saga is constructed

`runSaga()` calls `buildPaymentSagaSteps()` with payment-specific operations.
The generic `PaymentSaga` does not know about gateways or databases; it only
coordinates ordered `execute` and `compensate` functions.

```typescript
const saga = new PaymentSaga(buildPaymentSagaSteps(dependencies));
await saga.run({
  paymentId: payment.id,
  correlationId: payment.correlationId,
});
```

The saga starts with identifiers in its context. Each successful step returns
a partial context update, and the saga merges that update before the next
step starts.

### 5. Authorization reserves the funds

The `authorize` step calls `gateway.process(payment)`. A successful response
adds the transaction ID and marks the context as reserved:

```text
gateway.process(payment)
  -> reserved = true
  -> gatewayTransactionId = returned transaction ID
```

A failed response becomes a `ClassifiedError`. Because the step did not
complete, the saga has nothing to compensate yet.

### 6. Capture confirms the money movement

The `capture` step confirms the capture. For gateways that auto-capture during
authorization it can be a no-op, while the explicit step still represents the
same lifecycle. On success, the context receives `captured = true`. If a later
step fails, its compensation calls `refund(ctx)`.

### 7. The ledger is written and the result becomes durable

The `write_ledger` step persists the completed payment and transaction ID with
`paymentRepo.upsertStatus()`. It then stores the idempotency result in Redis.
This is the durable money-processing boundary: a retry after this point should
be recognized as completed instead of calling the gateway again.

The step returns a ledger entry ID, which lets compensation reverse the ledger
record if a later money-affecting operation fails.

### 8. The customer notification is emitted

The final step emits `payment.notify`. After it succeeds, the saga returns its
final context and `runSaga()` emits `payment.status_changed` with `COMPLETED`.

Notification is last because it does not move money. **Current caveat:** if
`notifyCustomer` throws, the generic saga still compensates earlier completed
steps. That can currently trigger ledger reversal, refund, and authorization
voiding, even though the intended design treats notification as an out-of-band
retryable operation. This behavior should be corrected before relying on
notification failures in production.

### 9. Failure and compensation

If a step fails after earlier steps completed, compensation runs in reverse
order. The failed step itself is not included because it is recorded only after
its `execute()` function resolves.

```text
authorize succeeds
capture succeeds
write_ledger fails
  -> compensate capture: refund
  -> compensate authorize: void authorization
  -> re-throw the original error
```

Compensation errors are logged for manual reconciliation. They do not replace
the original failure, because the processor still needs to classify it for
retry or dead-letter handling.

### 10. Retry, DLQ, and final status

After the saga error is re-thrown, the processor releases the gateway
semaphore and classifies the failure:

```text
permanent failure
  -> status FAILED
  -> move to DLQ
  -> do not rethrow to BullMQ

transient/rate-limited failure with retries left
  -> release idempotency lock
  -> rethrow for BullMQ backoff and jitter

transient failure at max retries
  -> status FAILED
  -> move to DLQ
```

On success, the idempotency result was already stored during the ledger step.
Finalization emits the completed domain event for metrics, dashboards, and
other subscribers.

## Run commands

Run these commands from the repository root.

### Start Redis

```bash
docker run -d --name redis-test -p 6379:6379 redis
```

If the container already exists, use:

```bash
docker start redis-test
```

### Install, build, and start

```bash
npm install
npm run build
npm start
```

The Nest application listens on port `3000` by default and serves the main
dashboard from the static public directory. Open this URL in a browser after
the application starts:

```text
http://localhost:3000/dashboard.html
```

The dashboard connects to the `/payments-dashboard` Socket.IO namespace and
shows total processed payments, average TPS, waiting and active jobs, failed
jobs, per-gateway latency/success metrics, and queue counts. To use another
application port, set `PORT` before starting:

```powershell
$env:PORT = '3001'; npm start
```

### Run tests

```bash
# All unit tests
npm test

# Saga and processor tests only
npx jest src/__tests__/saga.spec.ts src/__tests__/payment-processor.spec.ts

# Integration tests, one worker process
npm run test:integration

# Watch tests while developing
npm run test:watch

# Generate coverage
npm run test:cov
```

### Exercise failure paths

The processor has a development failure switch for the capture, ledger, and
notification steps:

```powershell
$env:SAGA_FAIL_STEP = 'capture'; npm test
$env:SAGA_FAIL_STEP = 'ledger'; npm test
$env:SAGA_FAIL_STEP = 'notify'; npm test
Remove-Item Env:SAGA_FAIL_STEP
```

For Command Prompt:

```cmd
set SAGA_FAIL_STEP=capture&& npm test
set SAGA_FAIL_STEP=ledger&& npm test
set SAGA_FAIL_STEP=notify&& npm test
```

### Run the load test

```bash
npm run load-test
```

The load test expects Redis to be available and enqueues payments across the
`stripe`, `adyen`, and `razorpay` gateways and the `high`, `normal`, and `low`
priority queues. Defaults are 60 seconds and approximately 100 payments per
second. Adjust the run with environment variables:

```powershell
$env:DURATION_SECONDS = '30'
$env:RATE_PER_SECOND = '250'
$env:REDIS_HOST = 'localhost'
$env:REDIS_PORT = '6379'
npm run load-test
```

For a quick local smoke test:

```powershell
$env:DURATION_SECONDS = '10'; $env:RATE_PER_SECOND = '10'; npm run load-test
```

Watch `http://localhost:3000/dashboard.html` while the load test is running to
see queue depth, throughput, failures, and gateway metrics change. The load
test exercises queue throughput and does not replace focused tests.

---

## Part 2: Architecture Discussion

### 1. Scaling Strategy

**Scaling to 500K/hour (~140/sec sustained, plan for 3-5x burst).**
The current design scales along three independent axes without a rewrite:

- *Horizontal workers*: BullMQ workers are stateless consumers — running
  more worker processes/pods per gateway queue is the first lever, capped
  by each gateway's real rate limit (which is why gateway-level throttling
  is separate from worker concurrency — adding workers can't exceed what
  the gateway allows, it just reduces idle time).
- *Queue partitioning*: at high volume, a single Redis instance backing
  BullMQ becomes the bottleneck before the workers do. Partition by
  `hash(customerId) % N` into N Redis Cluster shards, each running its own
  set of gateway queues. Partitioning by customerId (not payment ID) keeps
  a given customer's payments processing in a consistent order, which
  matters for retry/idempotency reasoning and for any per-customer velocity
  checks.
- *Database sharding*: the payment/ledger tables shard the same way (by
  customerId), so a payment's queue partition and its DB shard are
  co-located — avoids cross-shard transactions for the common case of
  "read/write this customer's own payment."

**Auto-scaling triggers**: queue depth (waiting count) sustained above a
threshold for >60s, and worker CPU/memory. Depth is the better leading
indicator than CPU alone — a queue can back up while workers sit mostly
idle waiting on slow gateway responses, which is an I/O-bound scaling
signal CPU-based autoscaling would miss.

### 2. Reliability & Resilience

**Multi-region**: active-active for the queue/worker tier (each region runs
its own Redis Cluster + workers, processing its own regional traffic slice),
active-passive for the ledger DB with async replication and a defined RPO.
Full active-active on the ledger is avoidable complexity here — payments
don't need cross-region strong consistency for the common path, they need
each region to not lose data before it's durably written, which failover
+ replication gives more simply than a multi-region consensus DB.

**Disaster recovery**: RPO target driven by the async replication lag
(seconds, typically); RTO driven by how fast a region can be marked
unhealthy and traffic redirected (DNS/load-balancer health checks, target
minutes not hours). In-flight jobs at the moment of a regional failure are
the hard part — BullMQ jobs held by a dead region's Redis are recoverable
once that Redis comes back, but if the region is truly gone, they need to
be reconciled against the ledger DB (which has its own replica) rather than
assumed lost, since "job disappeared" and "payment didn't happen" are not
the same fact.

**Data consistency guarantees**: strong consistency within a shard
(payment write + ledger write in one DB transaction), eventual consistency
across the async pipeline to notifications/analytics. The saga pattern is
what makes "eventual" safe here — every step either completes or is
compensated, so there's no window where money-affecting state is
ambiguous, even though the full result (customer notified) may lag by
seconds.

**Network partitions**: the risky case is a worker that can reach the
gateway but not Redis (can't ack the job) — this is exactly why the
idempotency lock lives in Redis with a TTL rather than in-memory: if the
worker is partitioned after charging the customer but before writing the
result, the lock expires, another worker picks up the job, and the
reconciliation path (`getStatus` against the gateway) finds the prior
charge rather than double-charging.

### 3. Performance Optimization

- **Batching**: gateways that support batch submission APIs (some ACH/bank
  rails do) get a small batching window (e.g. 100ms or 50 items) at the
  worker layer — trades a little latency for meaningfully lower per-item
  gateway overhead. Card-network gateways typically don't support this, so
  it's applied selectively per gateway type, not globally.
- **Caching**: gateway metadata (fee schedules, supported currencies) and
  customer risk-scoring lookups are cached (Redis, short TTL) since they're
  read far more often than they change; the payment record itself is never
  cached for reads that affect processing decisions — it's always read
  fresh, since a stale read there is a correctness bug, not a performance
  one.
- **DB query optimization**: index on `(gatewayId, status, createdAt)` for
  the DLQ/reconciliation sweep queries and `(customerId, createdAt)` for
  customer-facing history; write-heavy payment status updates use a
  narrow `UPDATE ... SET status, processedAt WHERE id = ?` rather than
  full-row rewrites.
- **Message compression**: payment job payloads are small (a few hundred
  bytes of JSON) so compression isn't worth the CPU trade at this size —
  it starts to matter if `metadata` grows unbounded, which argues for
  capping/validating metadata size at ingestion instead.

### 4. Security Considerations

- **PCI compliance**: the queue/worker tier should never see raw PAN/CVV —
  card capture happens at the gateway's hosted field or via tokenization
  before a payment even reaches this system, so the `Payment` object only
  ever carries a gateway token, not card data. This keeps the queue
  infrastructure out of PCI SAQ scope for cardholder data, which is a much
  cheaper compliance posture than trying to secure raw PANs in Redis/BullMQ.
- **Encryption**: TLS in transit everywhere (gateway calls, Redis with TLS
  enabled, DB connections); at rest, DB-level encryption plus
  field-level encryption for anything sensitive that does need to be
  stored (e.g. `metadata` fields containing PII), so a DB snapshot leak
  doesn't expose plaintext PII on its own.
- **Audit logging**: every state transition (`payment.status_changed`,
  circuit breaker trips, DLQ moves) is already emitted as a domain event in
  this design — in production these get persisted to an append-only audit
  log (not just the WebSocket dashboard), since "who/what changed a
  payment's status and when" needs to be reconstructable independent of the
  live system's current state.
- **Access control**: worker processes get gateway credentials scoped to
  just the gateways they process (secrets manager, not env vars in a
  shared config), and the DLQ requeue action (moving a dead-lettered
  payment back into processing) is a privileged operation requiring
  explicit human action, not automatic — DLQ entries got there because
  something needed a human look, and silently auto-requeuing defeats that.

### 5. Testing Strategy

- **Chaos engineering**: fault injection at the gateway client boundary
  first (timeouts, 5xx, malformed responses) since that's where most real
  incidents originate — before reaching for infra-level chaos (killing
  Redis nodes, network partition simulation). Verify the circuit breaker
  actually trips and the rate limiter actually adapts under injected
  failure, not just that the code compiles.
- **Load testing**: ramp to target (50K/hr) and then to the burst multiple
  (3-5x) to find the point where queue depth starts growing faster than
  workers drain it — that's the real capacity number, not the steady-state
  one. Also test the *recovery* curve after an artificial gateway outage:
  how long until queue depth returns to baseline once the circuit closes.
- **Contract testing** against each gateway's sandbox/mock, particularly
  around the specific failure response shapes `classifyFailure` depends on
  — a misclassified "permanent" error masquerading as transient (or vice
  versa) is a correctness bug that only shows up under this kind of test,
  not general integration tests.
- **Monitoring test coverage**: alerts (success-rate floor, P99 ceiling)
  are tested by feeding the `MetricsCollectorService` a synthetic bad
  window and asserting the alert fires — untested alerting code is the
  classic way monitoring silently rots.

---

## Code Review Questions

**Choice of data structures.** Token bucket over sliding-log for rate
limiting (O(1) memory vs O(n), and bursts are actually desirable for
payment traffic spikes). A ring buffer (fixed-size array with shift) for
latency percentiles rather than a full distribution structure — at this
scale a few thousand recent samples gives accurate-enough P95/P99 without
unbounded growth.

**Trade-offs.** The biggest one: idempotency locks add a Redis round-trip
to the hot path of every payment. Accepted because the alternative
(occasional double-charges) is categorically worse than added latency for
a payments system — this is a correctness-over-speed trade made
deliberately, not accidentally. Second: the saga's compensation steps are
best-effort with loud logging on failure rather than an automatic retry
loop for compensations themselves — that's deliberate, because an
automatically-retrying compensation that itself keeps failing silently is
worse than surfacing it for manual reconciliation immediately.

**Handling a gateway outage.** Circuit breaker trips after either
consecutive failures or a rolling failure-rate threshold (catches both
hard outages and degraded-but-alive gateways). While open, the rate
limiter is proactively shrunk, jobs fail fast with a transient
classification (retried later via backoff, not DLQ'd), and a
`circuit_breaker.state_change` event fires so the dashboard/alerting layer
surfaces it immediately. Half-open trial requests test recovery without
throwing full traffic back at a gateway that just came back.

**Debugging approach for production issues.** Every payment carries a
`correlationId` threaded through queue jobs, log lines, OTel spans, and
outbound gateway call headers — the first step in any incident is grepping
that one ID across services rather than trying to correlate by timestamp.
The domain events (`payment.status_changed`, `payment.dead_letter`,
`circuit_breaker.state_change`) form a de facto timeline of what the system
believed was happening, which is usually enough to distinguish "gateway
issue" from "our bug" before touching a debugger.

---

## Expected Discussion Points

- **Why BullMQ vs alternatives (SQS, Kafka, RabbitMQ)**: BullMQ gives
  delayed jobs, per-job retry/backoff, priority, and rate-limiting
  primitives natively on Redis, which this problem needs directly — SQS
  would need more glue code for delayed/priority semantics, and Kafka is a
  better fit for event streaming/replay than for per-item retry-with-backoff
  job semantics. The trade-off accepted: BullMQ ties the system to Redis as
  a single dependency for both queueing and idempotency state, so Redis
  Cluster availability becomes more load-bearing than it would be in a
  Kafka-based design — mitigated by Redis Cluster + persistence (AOF) and
  the reconciliation path in `IdempotencyService`/`reconcile()` that doesn't
  fully trust the queue's state as ground truth.
- **Preventing duplicate processing**: covered above — jobId dedup at
  enqueue, Redis lock + result cache at processing time, gateway
  `getStatus` reconciliation as the tiebreaker when both are ambiguous.
- **Database vs Redis for state storage**: Redis holds *ephemeral*
  coordination state (locks, rate limiter buckets, queue jobs) — anything
  that's fine to lose and rebuild. The DB holds the *durable* payment/ledger
  record — the source of truth for "did this payment happen." The
  reconciliation logic exists specifically because these two stores can
  disagree after a crash, and the DB always wins that disagreement.
- **Microservices vs monolithic**: for this specific  assesment, a single
  payment-processing service (as implemented here) is preferable to
  splitting queue-management/processing/monitoring into separate services
  — they share fate anyway (a processing failure needs to update metrics
  and possibly the breaker atomically-ish), and splitting them would mean
  more network calls for state that's naturally cohesive. The natural
  service boundary is elsewhere: this whole subsystem as one service,
  separate from the customer-facing API and from notification delivery.
- **Event sourcing considerations**: the domain events already emitted
  (`payment.status_changed` etc.) are a natural fit for event sourcing the
  payment's history for audit purposes, but the *processing decisions*
  (should we retry, is the breaker open) are intentionally kept as regular
  mutable state, not derived by replaying events — replaying to determine
  "should I call the gateway right now" would add latency to the hot path
  for a benefit (perfect historical reconstruction) that an append-only
  audit log already provides more simply.
