# Tech Stack & Rationale

## Server — Java + Vert.x

**Why Vert.x:**
- The non-blocking event loop suits an I/O-bound workload (Redis + Kafka produce); no thread-per-request needed.
- `vertx-redis-client` and `vertx-kafka-client` return native `Future<T>` → compose the pipeline without blocking.
- Lighter than Spring Boot, with no reflection overhead on the hot path.

**Alternatives considered:** Spring WebFlux (reactive but heavier), plain Netty (lower-level than needed), Quarkus (good but less mature than Vert.x for Redis/Kafka).

## Manager — Java + Vert.x Worker + JDBC

The manager uses synchronous JDBC intentionally:
- The Kafka poll loop is throughput-bound, not latency-bound.
- A bulk CTE of one statement/opp/sub-batch is more efficient than reactive JDBC (R2DBC), because the batch size outweighs the async overhead.
- The poll loop runs on a **dedicated daemon thread** (not a Vert.x worker context) because `handleBatch` blocks on `Future.all(...).get()`; running it on a worker context would deadlock when Vert.x dispatches the completion callback back to the very thread that is blocking.
- JDBC settle is still pushed down to a `WorkerExecutor` (a separate pool) → many opps settle in parallel without occupying the event loop. The verticle deploys with `setInstances(2)` → 2 consumers split partitions within the same group.

## Redis — Lua atomic gate

**Why Lua EVAL:**
- The atomic multi-step operation (HMGET + SCARD + SISMEMBER + SADD) can't be done with a regular pipeline.
- Single-key Lua on one Redis node: ~100K ops/s, enough for 10K drivers/opp at a realistic throughput of ~20K RPS.
- Fallback: if a single opp must scale to millions, shard it into N sub-counter keys.

**Alternatives:** Redisson (JVM overhead), Redis transactions WATCH/MULTI (optimistic retry performs poorly under high contention).

## Kafka — claim event hand-off

**Why Kafka:**
- Decouples the server (gate) from the manager (commit) → the server is not blocked by PG latency.
- Partitioning by `opportunity_id` → locality for bulk-settle.
- At-least-once delivery + an idempotency key → enough to guarantee correctness without the exactly-once overhead.

**Alternatives:** RabbitMQ (no native partition-by-key), Redis Streams (adds a Redis dependency as a queue), inserting into the DB directly from the server (coupling + latency).

## PostgreSQL — source of truth

**Why PG:**
- `FOR UPDATE` + atomic decrement in one CTE: the strongest correctness guarantee.
- `UNIQUE(opp, driver)` + `ON CONFLICT DO NOTHING`: idempotency built in.
- Row-level locking by opp_id → different opps don't block each other.
- Connection pooling via **HikariCP** (`PgPool`), shared between settle + opportunity CRUD.

**Alternatives:** MySQL (equivalent row locking but less powerful CTEs), NoSQL (no transactions strong enough for a backstop).

## Bazel 8 + Bzlmod

A monorepo with the Go client and the Java server/manager/common in the same workspace. Bazel ensures:
- A hermetic build (independent of the local JDK/Go version).
- `rules_oci` builds the Docker image from a Bazel target, no Dockerfile needed.
- Incremental builds: only rebuild when a dependency changes.

## Dagger 2 — Dependency Injection (Java)

Constructor injection with `@Inject` + `@Binds` for interface→impl. Compile-time DI: DI-graph errors are caught at build time, not runtime. No reflection overhead at startup.

## Resilience4j — Circuit breaker (degrade)

Uses **`CircuitBreaker`** (not a rate limiter) in two independent places, both following the
philosophy of **shed load → 503** rather than degraded pass-through:
- The **server** wraps `gate.claim`: Redis down/timeout → OPEN → returns `503 THROTTLED`.
- The **manager** wraps each PG settle: an error or a settle >5s → OPEN → the batch fast-fails, the
  offset is not committed (Kafka redelivers), and at the same time it sets the kill switch
  `pg_health=down` so the server also sheds (`503 PG_UNAVAILABLE`). `PgHealthProbe` probes
  `SELECT 1` through the *same* breaker to recover once traffic has been fully shed.

**Why:** an explicit API to wrap `Future`, measure the latency window + count slow calls (>5s) —
exactly the "detect a slow PG" need that a rate limiter can't serve. Toggle with a flag
(`--disable-circuit-breaker`).

## Micrometer — Metrics

Counters + Timers (P99) by the convention `booking.<area>.<thing>`. Scraped by Prometheus, dashboarded in Grafana. Ready for a Datadog/CloudWatch migration if needed (Micrometer is an abstraction layer).
