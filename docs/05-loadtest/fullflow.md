# Load Test — Full Flow

Tests the whole stack end-to-end: HTTP → Redis gate → Kafka → PG commit → confirm.
Unlike `redis-counter` (gate only) and `pg` (CTE only), here **every component runs** and is
exercised through `POST /opportunities/{id}/bookings`.

Source: `com/tm/loadtest/server/`

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
| `gate` | 3-phase correctness over HTTP (like `redis-counter`'s gate): seed unique → 202, replay seed → 200 DUP, rest → fill the remainder + 409 FULL. Asserts `accepted == capacity` (no oversell), `dup == effectiveSeedN`, `full+throttled == restN-(cap-claimed)`, then `GET /opportunities/:id` to confirm the state in PG. |
| `idempotent` | Round 0 sends a batch of unique drivers → all 202. Rounds 1..N replay the exact same batch → all 200 DUP. Verifies Redis dedup works end-to-end across the stack. |
| `ramp` | Ramps RPS up through 1K→2K→5K→10K across 3 patterns; stops a pattern when error% > 5% or p99 > 200ms and reports the ceiling. |
| `throughput` | Sustained load (contended + diverse) runs indefinitely; metrics on `/metrics`. |
| `correctness` | gate → idempotent; exits non-zero on violation. |
| `full` | gate → idempotent → ramp; exits non-zero on violation. Used by `make loadtest-server`. |
| `all` | gate → idempotent → throughput (runs forever). |

---

## Ramp patterns

| Pattern | Setup | What it tests |
|---------|-------|---------------|
| **contended** | 1 opp, `capacity = 10M` (never fills) | Every request runs the **full happy path** HTTP → Redis SADD → Kafka → PG commit. Hot Redis key + hot Kafka partition + the server's event-loop throughput. |
| **diverse** | 100 opps round-robin, `capacity = 10M` | The same happy path, load spread evenly across many keys/partitions — the throughput baseline. |
| **realistic** | a new opp each step, `capacity = 1000` | Simulates a real booking window: the opp fills in the first second, then ~99% of the step is pure fast-reject (`SCARD ≥ capacity → FULL`). Each step is re-verified against PG. |

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

## Results

### Gate Correctness — 3 rounds

```
rounds=3  capacity=1000  requests=20000  dup=5%  rps=2000

round   p50       p99        accepted  full    dup    verify (remaining / booked)
─────────────────────────────────────────────────────────────────────────────────
1       1.266ms   13.628ms   1000      18500   500    0 / 1000
2       1.425ms   12.839ms   1000      18500   500    0 / 1000
3       1.572ms   12.705ms   1000      18500   500    0 / 1000
─────────────────────────────────────────────────────────────────────────────────
want               —          1000      18500   500    remaining+booked == capacity
```

Every round, all assertions pass:

```
✓ no oversell: accepted == capacity
✓ full+throttled(dup+rest) == restN-(cap-claimedBeforeRest)
✓ dup == effectiveSeedN
✓ no connection errors
✓ no lost responses: total == sent
✓ CB disabled: throttled == 0
✓ remaining >= 0   ✓ booked <= capacity   ✓ remaining+booked == 1000
```

**Notes:**
- End-to-end (HTTP → Redis → Kafka → PG): exactly 1000 accepted == capacity, **no oversell** across
  all 3 rounds. The PG verify confirms `booked == 1000`, `remaining == 0` — the gate's accept count
  matches exactly what actually committed to the source of truth.
- p99 ≈ 13ms is the **full happy path** for 1000 accepted + 19000 rejected (the gate decision is
  under 1ms; the tail is HTTP + Kafka publish + JVM GC, not Redis).
- ~92.5% of traffic (18500/20000) is fast-rejected FULL at Redis — never touching Kafka or PG.

### Idempotency — replay 5×

```
capacity=1000  batch=200  replay-rounds=5  rps=2000

round   accepted  dup    full   throttled  errors
──────────────────────────────────────────────────
0       200       0      0      0          0        ✓ all accepted
1       0         200    0      0          0        ✓ all dup
2       0         200    0      0          0        ✓ all dup
3       0         200    0      0          0        ✓ all dup
4       0         200    0      0          0        ✓ all dup
5       0         200    0      0          0        ✓ all dup
──────────────────────────────────────────────────
verify: capacity=1000  remaining=800  booked=200   ✓ remaining+booked == 1000
```

**Notes:**
- 5 replays of the same batch of 200 drivers (simulating Kafka at-least-once): every replay returns
  200 DUP, **never** ACCEPTs twice. PG shows exactly 200 booked — no double-booking across the stack.
- Redis `SISMEMBER` dedup short-circuits replays before they touch Kafka; PG `UNIQUE(opp, driver)`
  is the backstop if Redis loses state.

### Ramp — contended (1 opp, capacity 10M, full happy path)

```
target    actual    sent       p50     p99     max     error%
──────────────────────────────────────────────────────────────
1000      999       120000     11ms    15ms    28ms    0.0%
2000      1999      239980     12ms    15ms    28ms    0.0%
5000      4999      600000     12ms    15ms    32ms    0.0%
10000     9999      1200000    11ms    15ms    39ms    0.0%
──────────────────────────────────────────────────────────────
Max sustainable RPS (error<5%, p99<200ms): ~9999
```

**No saturation up to 10K RPS.** Every request runs HTTP → Redis SADD → Kafka publish → 202; p99
stays flat at 15ms even as the full 10K RPS hits one hot Redis key + one hot Kafka partition. The
server's event loop never blocks (gate + producer return `Future`, no `executeBlocking`).

### Ramp — diverse (100 opps round-robin, capacity 10M)

```
target    actual    sent       p50     p99     max     error%
──────────────────────────────────────────────────────────────
1000      999       119990     11ms    15ms    31ms    0.0%
2000      1998      239880     11ms    15ms    56ms    0.0%
5000      4998      599950     11ms    15ms    39ms    0.0%
10000     9997      1199700    12ms    17ms    48ms    0.0%
──────────────────────────────────────────────────────────────
Max sustainable RPS (error<5%, p99<200ms): ~9997
```

**No saturation up to 10K RPS.** Spreading load across 100 keys/partitions gives the same p99 as
contended (≈15ms) — at this RPS the bottleneck is neither Redis's hot slot nor Kafka's hot
partition; the happy-path cost is dominated by HTTP + publish latency.

### Ramp — realistic (a new opp each step, capacity=1000, with PG verify)

```
target    actual    sent       p50     p99     max     error%   verify (remaining / booked)
──────────────────────────────────────────────────────────────────────────────────────────
1000      999       120000     2ms     5ms     19ms    0.0%     0 / 1000
2000      1999      239980     2ms     6ms     16ms    0.0%     0 / 1000
5000      4999      599950     2ms     6ms     37ms    0.0%     0 / 1000
10000     9997      1199700    2ms     6ms     37ms    0.0%     0 / 1000
──────────────────────────────────────────────────────────────────────────────────────────
Max sustainable RPS (error<5%, p99<200ms): ~9997
```

**No saturation up to 10K RPS, and every step verifies zero oversell against PG.** This is the real
booking-window shape: the opp fills (1000 ACCEPTs) in the first second, then ~99% of the rest of the
step is pure FULL fast-reject. p99 **drops to 5–6ms** (vs 15ms contended) because nearly every
request short-circuits at the Redis gate (`SCARD ≥ capacity`) without touching Kafka or PG. The
ceiling here is Redis's reject rate on the FULL path, far above 10K (see `redis.md` — ~78K RPS).

---

## Grafana

End-to-end across the whole stack — HTTP gate, Kafka offload, PG settle, confirm push.

![HTTP API + Kafka + Gate outcome rate](../assets/lt-full/Screenshot%202026-06-14%20at%2000.46.02.png)

The claim rate at the API tracks the ramp steps; the Kafka publish rate and the manager's
`consumer.handle` rate move together — every accepted claim is published and consumed exactly once.

![PG commit + settle + consumer notify rate](../assets/lt-full/Screenshot%202026-06-14%20at%2000.46.15.png)

On the PG side: the P99 latency of DAO commit and the `pg.settle` batch spikes briefly at cold start
(JIT / connection-pool warmup, ~25–30ms) then settles to a few ms. The consumer notify rate (pushing
confirmations to the app) peaks at ~7.5K/s, matching the settle throughput.

![End-to-end latency by status](../assets/lt-full/Screenshot%202026-06-14%20at%2000.46.25.png)

End-to-end: `api.claim.latency` P99 holds ≈20ms across ok/full/dup. `e2e.notify.latency` (HTTP
accept → confirm pushed to the app) spikes to ~3s on the first batch (consumer group join + JVM
warmup) then drops to **~17.8ms steady** — a fast ACCEPTED → CONFIRMED loop once the consumer
stabilizes.

---

## Metrics

| Metric | Source | Description |
|--------|--------|-------------|
| `booking.api.claim_total{result}` | server | Claim outcome over HTTP: ok / full / dup / error |
| `booking.api.claim.latency` | server | P99 latency of the HTTP claim (by status) |
| `booking.api.claim.publish_total{result}` | server | Kafka publish ok / fail |
| `booking.consumer.handle_total` | manager | Records consumed from Kafka |
| `booking.dao.commit_total{result}` | manager | committed / duplicate / rejected / error |
| `booking.dao.commit.latency` / `booking.pg.settle.latency` | manager | P99 PG commit / settle-batch |
| `booking.consumer.notify_total{result}` | manager | Confirmations pushed to the app |
| `booking.e2e.notify.latency` | manager | P99 end-to-end latency accept → confirmed |
| `loadtest_server_claims_total{scenario,result}` | load-gen | Outcomes observed from the client |
| `loadtest_server_latency_{p50,p99,max}_seconds{scenario}` | load-gen | Client-side latency |
| `loadtest_server_oversell_violations_total` | load-gen | must be 0 |
| `loadtest_server_idempotent_violations_total` | load-gen | must be 0 |

Grafana dashboard: `server`, `manager`, `loadtest-server`

---

## Conclusion — does the whole stack hold up end-to-end?

**Yes.** Every correctness assertion passes and throughput stays flat up to the tested ceiling.

| Criterion | Requirement | Actual | Result |
|-----------|-------------|--------|--------|
| No oversell (gate, 3 rounds × 20K req) | accepted == capacity | 1000 == 1000, PG booked == 1000 | ✓ |
| Idempotent (replay 5×) | no double-booking | 200 booked, every replay is DUP | ✓ |
| Realistic ramp (PG verify) | 0 oversell per step | remaining+booked == 1000 every step | ✓ |
| Happy-path throughput (contended) | sustains target load | p99 = 15ms at 10K RPS, 0% error | ✓ |
| Fast-reject path (realistic) | low latency when full | p99 = 5–6ms at 10K RPS | ✓ |
| End-to-end confirm latency | low | ~17.8ms steady (accept → confirmed) | ✓ |
| Error rate | 0% | 0% across every pattern | ✓ |

**Observations:**

- **The two-layer correctness model works end-to-end.** Redis's accept count, the gate's ACCEPTED
  responses, and the final `booked` in PG all match exactly (1000) — Redis rejects fast, PG commits
  correctly, and they never diverge under concurrency.
- **Happy path vs fast-reject.** contended/diverse (every request runs the full path) sit at p99 ≈
  15ms; realistic (mostly FULL) drops to p99 ≈ 5–6ms. The expensive part is the ACCEPTED path (Kafka
  publish + PG settle), exactly as designed — ~90% of real traffic short-circuits cheaply at the
  gate.
- **The non-blocking server holds up under one hot partition.** 10K RPS into one Redis key + one
  Kafka partition shows no p99 degradation — the Vert.x event loop never blocks.
- **Cold-start tail.** The only significant latency is the first batch (~3s e2e), from consumer-group
  join + JVM warmup. Steady-state is ~18ms. A pre-warm / readiness-gate step before accepting traffic
  would remove this.

**Notes / ceilings:**

- The ramp is limited to **10K RPS** (the load-gen config), not pushed to the breaking point — each
  component's ceiling is measured separately in `redis.md` (~78K gate RPS) and `pg.md` (~150 CTE/s
  contended on one hot opp). Full-flow confirms those components combine correctly at 10K with
  headroom to spare.
- The PG ceiling for one hot opp (~1500 drivers/s settled, see `pg.md`) is **not** stressed here
  because the realistic pattern only commits 1000 drivers once per step before the opp fills. A
  single opp filled sustainably above PG's settle rate would make the manager accumulate lag —
  mitigate by increasing partitions or sharding the sub-opp.

**Verdict:** the entire pipeline — HTTP gate → Kafka offload → PG settle → confirm — is both correct
(no oversell, idempotent) and fast (p99 5–15ms) at the target scale. The per-component load tests
establish the ceilings; this test confirms they hold up combined end-to-end.
