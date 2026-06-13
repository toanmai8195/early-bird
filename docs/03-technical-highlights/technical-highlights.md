# Technical Highlights & Notes

## 1. Redis Lua gate — one atomic EVAL, SCARD before SISMEMBER

The entire gate decision lives in **one** atomic `EVAL` over 3 keys (`claimed_set:{opp}`,
`opp_meta:{opp}`, `pg_health`), checked in cheap→expensive order:

```lua
if redis.call('GET', KEYS[3]) == 'down' then return 'DOWN' end      -- kill switch
local meta = redis.call('HMGET', KEYS[2], 'capacity', 'window_start')
if not meta[1] then return 'CLOSED' end                              -- opp does not exist
if now < tonumber(meta[2]) then return 'CLOSED' end                  -- outside window
if redis.call('SCARD', KEYS[1]) >= tonumber(meta[1]) then return 'FULL' end
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1 then return 'DUP' end
redis.call('SADD', KEYS[1], ARGV[1])
return 'OK'
```

**SCARD before SISMEMBER:** once the opp is full (~90% of the time) every request is rejected at
`SCARD` without calling `SISMEMBER` — saving one op on bulk traffic. The SET both **counts** (SCARD)
and **dedupes** (SISMEMBER) in the same atomic op; PG `UNIQUE(opportunity_id, driver)` is the dedup
backstop, so even dropping SISMEMBER would not break correctness. A single-key script ⇒ ~100K
ops/s/node, more than enough for one hot opp.

## 2. PG bulk-settle CTE — one statement / opp / sub-batch

The manager groups claim events by `opportunity_id`, splits each opp into sub-batches
(`settleBatchSize`), and each sub-batch is **one** CTE:

```sql
WITH input(driver_id, idempotency_key, pos) AS (VALUES ...),
existing AS (SELECT driver_id FROM bookings JOIN input WHERE opportunity_id=$opp),
cap AS (SELECT CASE WHEN now() BETWEEN window_start AND window_end
                    THEN remaining ELSE 0 END AS remaining
        FROM opportunities WHERE opportunity_id=$opp FOR UPDATE),     -- lock + window backstop
cand AS (SELECT driver_id, idempotency_key, row_number() OVER (ORDER BY pos) rn
         FROM input WHERE driver_id NOT IN existing),
adm  AS (SELECT ... FROM cand CROSS JOIN cap WHERE rn <= cap.remaining),
ins  AS (INSERT INTO bookings ... SELECT ... FROM adm ON CONFLICT DO NOTHING RETURNING driver_id),
upd  AS (UPDATE opportunities SET remaining = remaining - (SELECT count(*) FROM ins) ...)
SELECT driver_id, CASE WHEN driver_id IN ins THEN 'COMMITTED'
                       WHEN driver_id IN existing THEN 'DUPLICATE'
                       ELSE 'REJECTED' END ...
```

**Why:** N claims → 1 round-trip instead of N. `FOR UPDATE` on the opp row ⇒ atomic decrement,
admitting only `rn <= remaining` drivers by arrival order, so it **never oversells**. `cap` is also
the **window backstop** — outside the window admittable = 0 even if the Redis gate is bypassed.
`ON CONFLICT DO NOTHING` + the `existing` branch handle Kafka at-least-once **within the same
statement**. For one hot opp, settle serializes on that row anyway ⇒ raising `settleBatchSize` cuts
the number of round-trips (the main lever).

## 3. The hot partition is an intentional trade-off

Kafka key = `opportunity_id` → every claim for the same opp goes to one partition/consumer. This
creates a hot partition when one opp has 10K+ drivers, but:
- Correctness does **not** depend on ordering (the Redis gate + PG atomics are enough).
- Bulk-settle exploits locality: one consumer gathers all of an opp's events in one poll → fewer DB
  round-trips.
- Opps are fully independent; opps in the same batch settle **in parallel** (worker pool), while
  sub-batches within the same opp settle **sequentially** (to avoid lock contention on the same
  `opportunities` row). Scaling: increase the partition count.

## 4. Two circuit breakers + a global kill switch (no degraded pass-through)

This is the most notable part — the system **chooses to shed load** rather than pass through when
infrastructure is sick:

- **Redis CB (server)** around `gate.claim`. OPEN (Redis down/timeout) → skip the gate, return
  `503 THROTTLED`. Not rate-limit-then-pass-through: losing the gate means losing fairness +
  fast-reject, so we deliberately shed, letting PG be the correctness backstop rather than piling
  blind traffic onto it.
- **PG CB (manager)** around each settle (an error or >5s → OPEN). The batch fast-fails
  `circuit_open`, does **not** release Redis / notify, and the offset is not committed → Kafka
  redelivers when half-open. On the CB transition to OPEN → `PgHealth.markDown()` sets
  `pg_health=down` (TTL 30s).
- The **`pg_health` kill switch** propagates to the Lua gate (`DOWN`) → the server returns
  `503 PG_UNAVAILABLE`, stopping events from piling onto a sick PG **even before Kafka**.
- **`PgHealthProbe`** solves the deadlock: once the server has shed everything and Kafka is quiet,
  the CB has no settle to learn PG is healthy ⇒ it probes `SELECT 1` through the *same* breaker on a
  timer; HALF_OPEN + probe ok → close → `markUp()` clears the flag → the server reopens.

## 5. Recovery & idempotency — no outbox, relying on PG + warmup

**There is no pending-set/outbox.** A claim that returns `OK` is published straight to Kafka; a
**publish fail** → `gate.release()` (SREM returns the slot) + `503 UNAVAILABLE`, and a claim not yet
published is treated as never having happened. Reliability comes from two other places:

- **Idempotency**: the `idempotency_key` flows end-to-end (client → Redis dedup → Kafka event → PG
  `idempotency_key` column). PG `UNIQUE(opportunity_id, driver)` + `ON CONFLICT DO NOTHING` ⇒ Kafka
  redelivery / double-publish both yield `DUPLICATE`, a no-op. Offset commit *after* settle ⇒
  at-least-once with no lost events.
- **`RedisWarmupService`** (runs periodically on the server) self-heals when Redis loses its
  dataset: the `warmup:heartbeat` sentinel (no TTL) disappears after a flush/restart → a single
  `EXISTS` is enough to detect total data loss **without scanning each opp** every tick →
  `restoreAll()` reads PG to rebuild `opp_meta` + `claimed_set` for every open opp, then resets the
  heartbeat. During warmup, the PG backstop holds correctness.

## 6. `REJECTED` closes the slot permanently (capacity − 1)

When a driver passes the Redis gate (SADD) but PG returns `REJECTED` (`remaining=0` — Redis miscounts
due to degrade/restart), the manager calls `gate.reject()`: `SREM` the driver **and** `HINCRBY
capacity -1`. Lowering capacity keeps `SCARD >= capacity` true after removing the driver, so the gate
keeps returning `FULL` **without admitting a replacement driver** that PG would reject again —
converging Redis back to PG's true state.

## 7. Non-blocking server; sync manager on a dedicated daemon thread

The server path (Redis gate + Kafka produce) runs entirely on the Vert.x event loop, with the
gate/producer returning `Future`, **never** `executeBlocking`. The manager is throughput-bound
(Kafka poll + JDBC), so it is intentionally sync, optimized with a batch CTE + many partitions. The
poll loop runs on a **dedicated daemon thread** (not a Vert.x worker context) because `handleBatch`
blocks on `Future.all(...).get()`; on a worker context, Vert.x would try to dispatch the Future's
completion callback back to the very thread that is blocking → deadlock. On a plain thread, the
callback runs directly on the completing thread (the `pg-claim-store` pool). JDBC settle / DAO work
is still pushed down to a `WorkerExecutor` so it does not occupy the event loop.
