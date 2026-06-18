# Load Test

Three load tests, each answering one distinct question about the claim system. The first two
isolate a single component to measure its **ceiling** and **correctness**; the third combines
everything to confirm they hold up **end-to-end**.

| Test | Scope | Question | Doc |
|------|-------|----------|-----|
| **Redis Gate** | Only the Lua EVAL gate (no HTTP/Kafka/PG) | How fast does the gate reject, and does it oversell? | [redis.md](redis.md) |
| **PG Backstop** | Only the bulk-settle CTE (no HTTP/Redis/Kafka) | Up to what throughput does PG hold correctness? | [pg.md](pg.md) |
| **Full Flow** | The whole stack: HTTP → Redis → Kafka → PG → confirm | Combined, is it still correct + fast? | [fullflow/fullflow.md](fullflow/fullflow.md) |

All run in Docker (the `loadtest` profile), with metrics exposed via Prometheus and dashboards in
Grafana `:3000`. The load generator (Go) is not resource-limited; the services are capped to
simulate a single production node.

```bash
make loadtest-redis    # Redis gate (isolation)
make loadtest-pg       # PG backstop (isolation)
make loadtest-server   # Full flow end-to-end
```

---

## Infrastructure limits

Each component is capped on CPU/RAM to measure the real ceiling of one node, not the whole cluster.

| Service | CPU | RAM | Notes |
|---------|-----|-----|-------|
| Redis | 1 core | 2 GB | Lua EVAL is single-threaded/key — scaling CPU doesn't apply |
| Postgres | 2–4 cores | 2 GB | `max_connections` 300 |
| Kafka | 4 cores | 2 GB | partition key = `opportunity_id` |
| Server / Manager | — | — | JVM Java 21; manager `DB_POOL_SIZE=16`, `SETTLE_BATCH_SIZE=200`, `MAX_POLL_RECORDS=2000` |

> Note: PG is capped at 4 cores in the isolated test (`pg.md`), and full-flow also leaves it at 4
> cores. The ceiling numbers below are tied to each specific configuration — see the details in
> each doc.

---

## Results summary

### Redis Gate — throughput ceiling & fast-reject

| Criterion | Requirement | Actual |
|-----------|-------------|--------|
| Throughput | ~10K RPS/opp | **~78K RPS** (1 CPU) — ~8× headroom |
| p99 @ 20K RPS | < 10ms | 1.437ms |
| Oversell (10M requests) | 0 | 0 |
| Fast-reject when full | ~90% don't touch DB | 89.95% FULL at Redis |

The single-key Lua EVAL reaches ~78K RPS on 1 CPU, p99 < 1ms all the way to 50K. The three-way
correctness check (local tally = Redis `SCARD` = Prometheus) matches exactly across 10M requests,
zero oversell. **The bottleneck is the single key**: if one opp needs > ~80K RPS, it must be
sharded into N sub-counters.

### PG Backstop — correctness & hot-opportunity ceiling

The isolated PG test is a **batch × traffic sweep** on one hot `FOR UPDATE` row (the worst case),
varying both the CTE rate and the batch size (drivers per CTE).

| Criterion | Requirement | Actual |
|-----------|-------------|--------|
| Hot-opp driver throughput | as high as possible | batch=10 ~800/s → **batch=100–200 ~10–16K drivers/s** on one row |
| Low-batch latency | sub-10ms median | p50 single-digit ms across the whole sweep until deep saturation |
| Oversell correctness | 0 violations | 0 |
| Idempotent (at-least-once) | no double-booking | 0 violations |

**Batching is the throughput lever on a hot row.** A bigger CTE holds the single `FOR UPDATE` row
longer, so the *CTE/s* ceiling falls as the batch grows — but each CTE settles more drivers, so
sustained *drivers/s* rises ~10–20×: from ~800/s (batch=10) to ~10–16K/s (batch=100–200). The old
"~1,500 drivers/s per opp" ceiling was a batch-size artifact, not a PG limit. p50 stays a few ms
throughout; only the queued tail (p99/max) explodes past the knee. PG is the *correctness* backstop
that also scales on a hot row precisely because the manager bulk-settles large batches.

### Full Flow — the whole stack end-to-end

| Criterion | Requirement | Actual |
|-----------|-------------|--------|
| No oversell (gate, 3 rounds × 20K req) | accepted == capacity | 1000 == 1000 == PG booked, p99 ≈ 13ms |
| Idempotent (replay 5×) | no double-booking | every replay is DUP, 200 booked |
| Contended (1 hot opp, full path) | sustains load | p99 ≈ 15ms @ 10K RPS, 0% error |
| Diverse (spread, full path) | sustains load | p99 ≈ 15ms @ 10K RPS, 0% error |
| Realistic (real booking window) | low latency when full | p99 ≈ 5–6ms @ 10K RPS, 0 oversell/step |

Redis's accept count, the gate's ACCEPTED responses, and the final `booked` in PG **match exactly at
1000** throughout the concurrency — the two-layer correctness model works end-to-end. The
non-blocking server keeps p99 flat under one hot Redis key + one hot Kafka partition at 10K RPS, 0%
error in every pattern. Same 10K input, opposite PG shape: contended settles it through ~80 CTE/s on
one row (~125 drivers/CTE) while diverse spreads it across ~6K parallel CTEs (~1.6 drivers/CTE).

---

## The combined picture

```
                  isolated ceiling                       target load        headroom
  Redis gate      ~78K RPS / opp                         ~10K drivers/opp    ~8×
  PG (1 hot opp)  ~10–16K drivers/s @ batch 100–200       1 hot opp          scales with batch size
  Full flow       10K RPS (load-gen cap)                 —                   not pushed to break
```

The two isolated tests **establish the ceiling**; full-flow **confirms** the components combine
correctly at 10K RPS with headroom to spare. The key link between them: the gate rejects ~90% of
traffic, so PG never sees the full 10K burst as raw RPS — it sees a smaller stream of *large
batches*, which is exactly the regime where one hot row sustains ~10–16K drivers/s. Remove the gate
and PG would face the full burst as small CTEs, where the hot-row ceiling is much lower.

**Remaining risks** (detailed in each doc):
- **Redis hot-slot**: one opp > ~80K RPS → shard into sub-counter keys.
- **PG hot opp**: the per-row settle rate falls if batches shrink; a single opp filled sustainably
  beyond ~10–16K drivers/s → the manager accumulates lag; mitigate by raising `SETTLE_BATCH_SIZE`,
  adding partitions, or sharding the sub-opp.

**Verdict**: the entire pipeline — HTTP gate → Kafka offload → PG settle → confirm — is both correct
(no oversell, idempotent) and fast (p99 5–15ms) at the target scale (~10K drivers/opp). No Redis
Cluster or sub-counter sharding is needed at this scale.

---

## Grafana dashboards

`loadtest-redis-counter` · `loadtest-pg` · `loadtest-server` (+ `server`, `manager` for
full-flow). Each load-gen metric has a panel; `*_oversell_violations_total` and
`*_idempotent_violations_total` must always be 0 (red on Grafana if > 0).
