# Booking System — Delivery Opportunity Claiming

## The problem
One booking window opens → many drivers claim an opportunity with limited `capacity`, almost
simultaneously. Framed as: **high contention on a single partition** (`opportunity_id`).
Assumed scale: capacity ~1K, ~10K drivers/opp; many opps running in parallel.

Requirements: **(1) No overselling** (bookings ≤ capacity), **(2) Low latency** (fast-reject when
full), **(3) Fault tolerance** (idempotent, at-least-once). `opportunity_id` is the partition
key throughout Redis → (Kafka) → PG.

## Architecture & flow
Tech: Java+Vert.x (HTTP), Redis+Lua (atomic gate), Kafka (offload), PostgreSQL (source of
truth), Resilience4j (degrade), MQTT/WS+push (confirm to the app).

One booking attempt flow:
1. Vert.x receives `POST /opportunities/{id}/bookings` (`X-Driver-Id`, `X-Idempotency-Key` headers).
2. Redis Lua atomic `EVAL` on 3 keys (`claimed_set:{opp}`, `opp_meta:{opp}`, `pg_health`), cheap→
   expensive: `pg_health=down`→`DOWN`; opp_meta missing or before window→`CLOSED`; `SCARD>=capacity`
   →`FULL`; `SISMEMBER`→`DUP`; else `SADD`→`OK`. The SET both counts and dedupes → ~90% rejected
   here, no DB touch.
3. `OK` → publish to Kafka (key = opp) → response **202 ACCEPTED** (not a synchronous SUCCESS).
   **Publish fail → `gate.release()` (SREM, returns the slot) + 503 UNAVAILABLE.** No outbox/
   pending-set: an unpublished claim is treated as never having happened.
4. Manager consumer polls a batch, groups by `opportunity_id`, splits into sub-batches. Opps settle
   in parallel; sub-batches within one opp settle sequentially (same `opportunities` row).
5. PG bulk-settle, ONE CTE/sub-batch: `FOR UPDATE` lock → dedup already-booked drivers → admit
   `rn<=remaining` by arrival order → `INSERT ... ON CONFLICT DO NOTHING` → decrement `remaining` by
   the insert count. Per-driver `COMMITTED`/`DUPLICATE`/`REJECTED`; outside the window admittable=0.
6. After settle: notify the driver (TODO MQTT/WS). `REJECTED` (passed the gate but PG full) →
   `gate.reject()` closes the slot permanently (SREM + `capacity-1`). Commit the Kafka offset only
   after the whole batch settles (`commitSync`) → at-least-once, idempotent via UNIQUE on redelivery.

## Design principles
- **Two-layer correctness**: Redis = prevent oversell *fast* (atomic Lua); PG = prevent oversell
  *correctly* (atomic decrement + UNIQUE). Do not trust Redis absolutely.
- **Degrade = shed load, not pass-through** (two independent Resilience4j circuit breakers + a global
  kill switch, no rate limiter):
  - **Redis down/timeout** → server CB around `gate.claim` opens → `503 THROTTLED`. PG still holds
    correctness, but without the gate we lose fairness/fast-reject, so we shed deliberately.
  - **Redis dataset loss** (restart/flush) → `RedisWarmupService` detects a missing
    `warmup:heartbeat` and rebuilds `opp_meta` + `claimed_set` from PG (`ReconciliationDao`).
  - **PG down/slow** → manager CB around each settle opens → batch fast-fails, offset not committed
    (Kafka redelivers), and sets the `pg_health=down` kill switch → server gate returns
    `503 PG_UNAVAILABLE`. `PgHealthProbe` (`SELECT 1` via the same breaker) recovers when traffic is
    fully shed.
- **Kafka partition by `opportunity_id`** (key = opp): every claim of the same opp goes to one
  partition/consumer → the manager **groups by opp + bulk-settles one statement/opp/poll**.
  Trade-off: an ultra-hot opp = a hot partition (accepted to enable bulk-loading); correctness is
  still decided by the Redis gate + PG, no ordering needed.
- **Scaling**: opps are independent, no global lock (Redis one key/opp, PG one row/opp). The
  remaining concern: Redis hot-slot (one key/node) — single-key Lua ~100K ops/s is still plenty; if
  needed, shard one opp into N sub-counters. A hot Kafka partition → increase the partition count.

## Language & structure (Bazel 8 + Bzlmod + rules_oci)
- **client = Go**; **server / manager / common = Java** (Vert.x, vertx-redis-client,
  vertx-kafka-client, kafka-clients, JDBC, Micrometer, Dagger).
- **server fully non-blocking** on the Vert.x event loop: gate + producer return `Future`,
  NO `executeBlocking`. **manager intentionally sync** (Kafka poll + JDBC are
  throughput-bound; optimized by **batch settle one tx/poll** + many partitions, not the
  event loop). Settle keeps `remaining` exact: per-claim CTE (locked decrement +
  dedup, then insert), grouped/sorted by opportunity_id to avoid deadlock.
```
MODULE.bazel/.bazelversion(8.1.1)/.bazelrc   # rules_go, gazelle, rules_java, rules_jvm_external, rules_oci, rules_pkg
go.mod                                       # module github.com/tm (Go client; gazelle prefix at com/tm)
bazel/oci.bzl                                # go_service_image + java_service_image
tools/dagger/                                # java_plugin(dagger-compiler) + export dagger+javax.inject
com/tm/
  infra/                  # docker-compose (redis/kafka/pg), grafana/dashboards/<svc>.json
  services/
    client/cmd  (Go)      # load-gen / simulation driver client
    server/     (Java)    # Vert.x gate → Kafka → 202 ACCEPTED; BookingVerticle, Main, handler/ (claim
                          #   + opportunity CRUD), dao/ (reconciliation), redis/ (warmup), di/, config/
    manager/    (Java)    # Kafka consumer → PG; Main+ConsumerRunner, ManagerVerticle, PgHealthProbe,
                          #   di/, config/, dao/, handler/
  common/                 # SPLIT java/ and go/; each class has an interface + impl (e.g. ClaimStore/PgClaimStore)
    java/                 #   config, redis, kafka, pg, metric, exception (shared custom exceptions)
    go/result/            #   Outcome + Tally
  loadtest/               # lt/ (shared harness), redis-counter/ (gate), pg/ (backstop), server/ (full flow)
```
- Java package = path (`com.tm.services.server`, `com.tm.common.redis`). Go import =
  `github.com/tm/...`. Bazel does not require the path to match the Java package.
- Each target has **tests in a `test/` subfolder** (its own BUILD).
- DI: constructor `@Inject` for classes you own; `ManagerModule` (@Provides) only for external;
  `ManagerBindings` (@Binds) binds interface→impl.

## Build / test
```bash
bazel build //com/tm/services/{client/cmd:client,server:server,manager:manager}   # code
bazel build //com/tm/services/server:server_image                                 # OCI image (native arch)
bazel run   //com/tm/services/manager:manager_load                                # + load docker
bazel test  --build_tests_only //com/tm/common/... //com/tm/services/...           # tests
```
Note: the Go SDK is pinned to **1.23.4** (rules_go 0.55.1 injects GOEXPERIMENT `coverageredesign`,
which Go 1.25 rejects); `.bazelrc` sets **Java 21** (records); the base image pulls **multi-arch**
(amd64+arm64/v8) so images build natively on mac arm64 (no `--config` needed); to force
amd64 for prod add `--config=linux-amd64`; the base digest in MODULE.bazel is already a real digest.

## Rules (path-scoped, lazy-loaded in `.claude/rules/`)
Loaded only when touching a file matching the path — indexed here so you know ahead of time:
- **unit test** — every `common/**` target + every `services/**/{dao,handler}` has tests (`test/` subfolder).
- **observability** — API/consumer/producer/dao have a metric counter (+ P99 latency for API & dao).
- **grafana** — each metric has a panel; each service / common-stack with a metric = 1 dashboard.
- **interface** — every dao/handler/common (with behavior) is exposed via an interface (`@Binds`); DTOs exempt.

## Deliverable
Design doc (~2-5 pages, Markdown + Mermaid): architecture diagram, request flow, correctness
under concurrency, failure/retry, tradeoffs (sync vs async ACCEPTED, Redis gate vs Kafka
serialize, partition strategy, Redis degrade).
