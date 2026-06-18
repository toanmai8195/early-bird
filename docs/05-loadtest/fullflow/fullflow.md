# Load Test — Full Flow

Tests the whole stack end-to-end: HTTP → Redis gate → Kafka → PG commit → confirm.
Unlike `redis-counter` (gate only) and `pg` (CTE only), here **every component runs** and is
exercised through `POST /opportunities/{id}/bookings`.

Source: `com/tm/loadtest/server/`

This page **consolidates** the five full-flow scenarios. Each has a detailed report (with
`caller`-filtered Grafana screenshots) under `correctness/` and `ramp/`:

- [`correctness/gate`](correctness/gate/gate.md) — no oversell
- [`correctness/idempotent`](correctness/idempotent/idempotent.md) — no double-booking
- [`ramp/contended`](ramp/contended/contended.md) — one hot opp, full happy path
- [`ramp/diverse`](ramp/diverse/diverse.md) — load spread across 100 opps
- [`ramp/realistic`](ramp/realistic/realistic.md) — real booking window (fill then fast-reject)

---

## Infrastructure

| | Redis | Kafka | Postgres | Server / Manager | Load Generator |
|--|-------|-------|----------|------------------|----------------|
| **CPU** | 1 core | 4 cores | 4 cores | unlimited | unlimited |
| **RAM** | 2 GB | 2 GB | 2 GB | unlimited | unlimited |
| **Pool / config** | — | — | `max_connections` 300 | manager `DB_POOL_SIZE=16`, `SETTLE_BATCH_SIZE=200`, `MAX_POLL_RECORDS=2000` | 500 workers |
| **Image** | `redis:7-alpine` | `apache/kafka:3.8.0` | `postgres:16-alpine` | Bazel-built JVM (Java 21) | Bazel-built Go binary |

The server runs with `DISABLE_CIRCUIT_BREAKER=true` so the correctness checks test the gate's logic
directly, not Resilience4j's degrade behavior.

---

## Modes (`--run`)

| Mode | Description |
|------|-------------|
| `gate` | 3-phase correctness over HTTP: seed unique → 202, replay seed → 200 DUP, rest → fill the remainder + 409 FULL. Asserts `accepted == capacity` (no oversell), `dup == effectiveSeedN`, `full+throttled == restN-(cap-claimed)`, then `GET /opportunities/:id` to confirm PG. |
| `idempotent` | Round 0 sends a batch of unique drivers → all 202. Rounds 1..N replay the exact same batch → all 200 DUP. Verifies Redis dedup end-to-end. |
| `ramp` | Ramps RPS 1K→2K→5K→10K across 3 patterns (contended / diverse / realistic); stops a pattern when error% > 5% or p99 > 200ms. |
| `throughput` | Sustained load (contended + diverse) runs indefinitely; metrics on `/metrics`. |
| `correctness` | gate → idempotent; exits non-zero on violation. |
| `full` | gate → idempotent → ramp; exits non-zero on violation. Used by `make loadtest-server`. |
| `all` | gate → idempotent → throughput (runs forever). |

---

## Ramp patterns

| Pattern | Setup | What it tests |
|---------|-------|---------------|
| **contended** | 1 opp, `capacity = 10M` (never fills) | Every request runs the **full happy path** HTTP → Redis SADD → Kafka → PG commit. Hot Redis key + hot Kafka partition + one hot PG row. |
| **diverse** | 100 opps round-robin, `capacity = 10M` | The same happy path, load spread across many keys/partitions/rows — the throughput baseline. |
| **realistic** | a new opp each step, `capacity = 1000` | Real booking window: the opp fills in the first second, then ~99% of the step is pure fast-reject (`SCARD ≥ capacity → FULL`). Each step re-verified against PG. |

### Caller tag — measuring each pattern on both sides

Every load-test request carries an `X-Caller-Id` header set to its pattern name
(`contended` / `diverse` / `realistic`, plus `gate` / `idempotent`). The server tags all its metrics
with it **and** embeds it in the `ClaimEvent`, so it rides Kafka to the manager, which tags every
manager + PG metric with the same `caller`. One claim is therefore measurable at two points:

- **API latency (load-gen side)** — the tables below (`booking.api.claim.latency`).
- **PG latency (manager side)** — same pattern, filtered by `caller` on the manager / common-pg /
  loadtest-server dashboards:
  - `booking_dao_commit_latency_seconds{quantile="0.99", caller="<pattern>"}` (per CTE)
  - `booking_pg_settle_latency_seconds{quantile="0.99", caller="<pattern>"}` (per CTE)
  - throughput split: `booking_pg_settle_total` (drivers/s) vs `booking_pg_settle_batch_total`
    (CTE/s), both `by (caller)`.

So the API p99 (HTTP → 202) and the PG settle p99 (CTE commit) for, say, `contended` can be read side
by side instead of guessing which layer a latency came from.

---

## Run configuration (`make loadtest-server`, `--run=full`)

```
--capacity=1000
--gate-rps=2000
--gate-requests=20000
--gate-rounds=3
--idempotent-batch=200
--idempotent-rounds=5
--ramp-step=2m
--ramp-cooldown=15s
--disable-cb            (server: DISABLE_CIRCUIT_BREAKER=true)
```

---

## Results — summary

| Scenario | Proves | Headline | Detail |
|----------|--------|----------|--------|
| **gate** | no oversell | 1000 accepted == capacity == PG booked, p99 ≈ 13ms, 3 rounds, 0% err | [→](correctness/gate/gate.md) |
| **idempotent** | no double-booking | 6× replay of 200 → 200 booked, every replay DUP | [→](correctness/idempotent/idempotent.md) |
| **contended** | 1 hot opp, full path | 10K RPS, p99 ≈ 15ms, 0% err | [→](ramp/contended/contended.md) |
| **diverse** | spread load, full path | 10K RPS, p99 ≈ 15ms, 0% err | [→](ramp/diverse/diverse.md) |
| **realistic** | real booking window | 10K RPS, p99 ≈ 5–6ms, 0 oversell/step | [→](ramp/realistic/realistic.md) |

### Correctness

**Gate — no oversell** · [detail](correctness/gate/gate.md)

```
rounds=3  capacity=1000  requests=20000  dup=5%  rps=2000

round   p50       p99        accepted  full    dup    verify (remaining / booked)
1       973µs     13.376ms   1000      18500   500    0 / 1000
2       1.586ms   12.937ms   1000      18500   500    0 / 1000
3       1.675ms   12.680ms   1000      18500   500    0 / 1000
```

Exactly **1000 accepted == capacity == PG booked** every round; ~92.5% (18500/20000) fast-rejected
FULL at Redis, never touching Kafka or PG. p99 ≈ 13ms is the full happy path of the accepted claims.

**Idempotency — no double-booking** · [detail](correctness/idempotent/idempotent.md)

```
capacity=1000  batch=200  replay-rounds=5  rps=2000

round 0:      accepted=200   dup=0
rounds 1..5:  accepted=0     dup=200   (each)
verify:       booked=200  remaining=800   (1200 requests sent → 200 distinct bookings)
```

5 replays of the same 200 drivers each return **200 DUP, 0 accepted**. Redis `SISMEMBER` dedups every
replay before Kafka; PG `UNIQUE(opp, driver)` is the backstop.

### Ramp (1K → 10K RPS, 2 min/step)

**contended** — 1 opp, cap 10M · [detail](ramp/contended/contended.md)

```
target  actual  p50   p99   max   err     → ceiling ~9999
1000    999     11ms  15ms  24ms  0.0%
2000    1999    12ms  15ms  30ms  0.0%
5000    4998    12ms  17ms  56ms  0.0%
10000   9999    12ms  16ms  29ms  0.0%
```

**diverse** — 100 opps, cap 10M · [detail](ramp/diverse/diverse.md)

```
target  actual  p50   p99   max   err     → ceiling ~9997
1000    999     12ms  15ms  25ms  0.0%
2000    1999    11ms  16ms  39ms  0.0%
5000    4998    12ms  16ms  51ms  0.0%
10000   9997    12ms  17ms  38ms  0.0%
```

**realistic** — new opp/step, cap 1000, PG-verified · [detail](ramp/realistic/realistic.md)

```
target  actual  p50  p99  max   err    verify (remaining / booked)   → ceiling ~9997
1000    999     1ms  5ms  18ms  0.0%   0 / 1000
2000    1999    2ms  6ms  45ms  0.0%   0 / 1000
5000    4999    2ms  6ms  50ms  0.0%   0 / 1000
10000   9997    2ms  6ms  50ms  0.0%   0 / 1000
```

No saturation up to 10K RPS in any pattern, 0% error throughout. Note p99 **drops to 5–6ms** for
realistic (vs ~15ms for contended/diverse): ~99% of its traffic short-circuits at the Redis gate on
the FULL path without touching Kafka or PG.

### Same input, opposite PG shape (contended vs diverse)

contended and diverse both push 10K drivers/s; the `caller` filter shows the PG side is the mirror
image — read on the **loadtest-server** dashboard, `caller=contended` vs `caller=diverse`:

| caller | drivers/s | CTE/s | drivers ÷ CTE | settle p99 / CTE | why |
|--|--|--|--|--|--|
| **contended** (1 opp) | ~10K | ~80 | **~125** | ~6–8ms | one `FOR UPDATE` row → survives only by big batches |
| **diverse** (100 opps) | ~10K | ~6K | **~1.6** | ~2ms | no row contention → thousands of small parallel CTEs |
| **realistic** (cap 1000) | ~1000/step (burst) | spiky | — | spiky | gate sheds ~99%, PG only sees the fills |

`booking.pg.settle` = **drivers/s**, `booking.pg.settle.batch` = **CTE/s**; their ratio is the
effective sub-batch size. This is the concrete picture behind the `dao.commit = 10K` vs
`settle.batch = 6K` question — 6K is CTE/s, 10K is drivers/s, ratio ≈ 1.6 on diverse because load
spreads thin, vs ≈ 125 on one hot contended row.

---

## Conclusion — does the whole stack hold up end-to-end?

**Yes.** Every correctness assertion passes and throughput stays flat up to the tested ceiling.

| Criterion | Requirement | Actual | Result |
|-----------|-------------|--------|--------|
| No oversell (gate, 3 rounds × 20K req) | accepted == capacity | 1000 == 1000, PG booked == 1000 | ✓ |
| Idempotent (replay 5×) | no double-booking | 200 booked, every replay DUP | ✓ |
| Realistic ramp (PG verify) | 0 oversell per step | remaining+booked == 1000 every step | ✓ |
| Happy-path throughput (contended) | sustains target load | p99 ≈ 15ms at 10K RPS, 0% err | ✓ |
| Spread throughput (diverse) | sustains target load | p99 ≈ 15ms at 10K RPS, 0% err | ✓ |
| Fast-reject path (realistic) | low latency when full | p99 ≈ 5–6ms at 10K RPS | ✓ |
| Error rate | 0% | 0% across every pattern | ✓ |

**Observations:**

- **Two-layer correctness holds end-to-end.** Redis's accept count, the gate's ACCEPTED responses,
  and the final `booked` in PG match exactly (1000) — Redis rejects fast, PG commits correctly, never
  diverging under concurrency.
- **Happy path vs fast-reject.** contended/diverse (full path) sit at p99 ≈ 15ms; realistic (mostly
  FULL) drops to ≈ 5–6ms — the expensive part is the ACCEPTED path (Kafka publish + PG settle), and
  ~90–99% of real traffic short-circuits cheaply at the gate.
- **One hot opp survives via batching.** contended pushes 10K drivers/s through ~80 CTE/s on one
  `FOR UPDATE` row (≈125 drivers/CTE); diverse pushes the same 10K through ~6K parallel CTEs
  (≈1.6 drivers/CTE). Same input, opposite PG shape — visible per `caller`.
- **The gate shields PG.** Under the realistic window, PG only ever commits the ~1000 fills per step;
  the 9K+ rejected requests never become settle work.

**Notes / ceilings:**

- The ramp is capped at **10K RPS** (load-gen config), not pushed to breaking — per-component ceilings
  are measured separately in `redis.md` (~78K gate RPS) and `pg.md` (~1,500 drivers/s on one hot opp
  at batch=10). Full-flow confirms they combine correctly at 10K with headroom.
- A single opp filled *sustainably* above PG's per-row settle rate would make the manager accumulate
  lag — mitigate by raising `SETTLE_BATCH_SIZE`, adding partitions, or sharding the sub-opp.

**Verdict:** the entire pipeline — HTTP gate → Kafka offload → PG settle → confirm — is both correct
(no oversell, idempotent) and fast (p99 5–15ms) at the target scale. The per-component load tests
establish the ceilings; this test confirms they combine end-to-end.
