# Early Bird — Booking Claim System

A system that lets drivers claim a **delivery opportunity** with a limited number of slots within a short time window. The core problem: **~10K drivers compete to claim a single opportunity with capacity ~1K simultaneously**, requiring no overselling, fast rejection when full, and idempotency on retry.

---

## Problem statement

When a booking window opens, thousands of drivers send claim requests almost at the same instant against an opportunity with limited `capacity`. The system must guarantee:

- **No overselling** — the number of recorded bookings never exceeds `capacity`, even under race conditions.
- **Low-latency fast-reject** — the ~90% of requests that are rejected (FULL) must be returned fast, without touching the DB.
- **Fault tolerance** — losing Redis or Kafka must not lose a booking; idempotent under at-least-once delivery.

Load characteristics: extremely high contention on a single partition (`opportunity_id`), with many opportunities running in parallel and independently of each other.

---

## Design assumptions

1. **Capacity is small enough to use a Redis Set as the claim registry** — each opportunity has one key `claimed_set:{opp_id}`, and SCARD is an exact count. If capacity grows to millions of drivers/opp, sub-counter sharding is required.
2. **Redis is the fast gate, PG is the source of truth** — Redis may lose state (restart, eviction); PG atomic decrement + UNIQUE constraint is the real backstop against overselling. Correctness does not depend entirely on Redis.
3. **Kafka at-least-once is sufficient** — an idempotency key + UNIQUE(opp, driver) in PG guarantee the write lands exactly once even if Kafka replays multiple times. Exactly-once Kafka is not needed.
4. **The booking window is short** (minutes to hours) — the Redis key TTL follows the window end; no complex cleanup needed.
5. **Each driver claims at most once per opp** — dedup via `claimed_set` (Redis) and the UNIQUE constraint (PG); no more complex business-level quota is required.
6. **A hot partition is acceptable** — all claims for the same opp go to one Kafka partition/consumer to bulk-settle one statement/opp/poll. If a single opp is extremely hot, that is a bottleneck consciously accepted in exchange for simplified ordering.
7. **The app has its own realtime channel (MQTT / WebSocket) with the backend** — the final booking result (SUCCESS / FAILED) is pushed to the app over MQTT/WS + push, **not** returned in the HTTP response. HTTP only returns `202 ACCEPTED` (received, processing via Kafka → PG); the real confirmation arrives after the consumer finishes writing to PG. This keeps the request path non-blocking, holding no connection open while waiting on the DB.
8. **Single-node infrastructure for the prototype** — single-node Redis, single-broker Kafka, single-instance PG. Production needs replication, but the design already separates the correctness layer so each part can scale independently.

---

## Documentation

### 1. [System Design](docs/01-system-design/system-design.md)
Overall architecture, end-to-end flow of a single claim request, sequence diagram, failure modes, and recovery.

### 2. [Tech Stack & Rationale](docs/02-techstack/techstack.md)
Why Vert.x, Redis Lua, Kafka, PostgreSQL, HikariCP, Resilience4j, Bazel, Dagger, and Micrometer were chosen — and the trade-offs versus alternatives.

### 3. [Technical Highlights & Notes](docs/03-technical-highlights/technical-highlights.md)
Notable design points: Lua gate ordering, PG bulk-settle CTE, the hot-partition trade-off, the two circuit breakers + kill switch, recovery, and idempotency.

### 4. [API Reference](docs/04-api/api.md)
HTTP endpoints: `POST /opportunities/:id/bookings`, opportunity CRUD, health check — request/response schemas and status codes.

### 5. [Load Tests](docs/05-loadtest/loadtest.md)
Three load tests, each isolating one question: [Redis Gate](docs/05-loadtest/redis.md) (reject ceiling + no-oversell), [PG Backstop](docs/05-loadtest/pg.md) (bulk-settle throughput), and [Full Flow](docs/05-loadtest/fullflow.md) (HTTP → Redis → Kafka → PG end-to-end: gate, idempotent, ramp).

### 6. [Test Cases](docs/06-test-case/test-case.md)
Manual end-to-end test cases (happy path, no-oversell, idempotency, window, degrade/recovery) — each case includes `curl` commands and the steps to run.

---

## Quick start

```bash
# Start infra (Redis, Kafka, Postgres, Prometheus, Grafana)
make infra-up

# Build and run the server + manager
make server
make manager

# Run the Redis gate load test (isolation)
make loadtest-redis

# Run the PG backstop load test (isolation)
make loadtest-pg

# Run the full-flow end-to-end load test (HTTP → Redis → Kafka → PG)
# Brings up infra + server + manager itself, runs gate → idempotent → ramp.
make loadtest-server
```

Grafana dashboard: [http://localhost:3000](http://localhost:3000)
