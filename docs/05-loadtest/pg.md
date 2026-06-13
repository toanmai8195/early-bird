# Load Test — PG Claim Store

Tests the PG bulk-settle CTE in isolation — no HTTP server, no Redis gate, no Kafka.

Source: `com/tm/loadtest/pg/`

---

## Infrastructure

| | Postgres | Load Generator |
|--|----------|----------------|
| **CPU** | 4 cores (Docker cap) | unlimited |
| **RAM** | 2 GB (Docker cap) | unlimited |
| **max_connections** | 300 | — |
| **Connection pool** | — | min(contended+diverse+100, 250) |
| **Image** | `postgres:16-alpine` | Bazel-built Go binary |

---

## Modes (`--run`)

| Mode | Description |
|------|-------------|
| `ramp` | Runs 4 patterns sequentially; each pattern ramps the CTE rate from `--ramp-start-rate` to `--ramp-max-rate` by `--ramp-step-size`, holding each level for `--ramp-step-dur`. Cooldown between patterns. |
| `oversell` | Fires `capacity + extra` concurrent goroutines (1 unique driver per CTE). Asserts: `remaining ≥ 0`, `booked ≤ capacity`, `remaining + booked == capacity`. |
| `idempotent` | Settles the same batch of drivers `--idempotent-rounds` times (simulating Kafka at-least-once). Asserts: `booked == batch_size`, no double-booking. |
| `throughput` | `contended` (N opp × B drivers/tick) + `diverse` (M opp × 1 driver/tick) run indefinitely. |
| `all` | oversell → idempotent (exits non-zero on violation) → throughput loop. |

---

## Ramp patterns

Each pattern runs the same CTE (`settleMulti`) with `--ramp-batch-size=10` drivers per call.

| Pattern | Setup | What it tests |
|---------|-------|---------------|
| **contended** | 1 opp, every CTE targets the same row | `FOR UPDATE` serialization — the worst case of a hot opportunity |
| **diverse** | Opp pool, round-robin per tick | Locks spread across many rows — the throughput baseline without contention |
| **oversell** | 1 opp `capacity = rate/2` | CTE correctness under pressure — ~half COMMITTED, ~half REJECTED |
| **idempotent** | 1 opp, a fixed driver pool replayed | Dedup cost — every CTE must return DUPLICATE after the first round |

---

## Run configuration (docker-compose)

```
--run=ramp
--ramp-start-rate=100
--ramp-step-size=50
--ramp-steps=5
--ramp-step-dur=2m
--ramp-cooldown=15s
--ramp-batch-size=10
```

Rate = CTE/s. Driver throughput = `rate × batch` (e.g. 300 CTE/s × 10 = 3,000 drivers/s).

---

## Results

**Config:** 4 CPU / 2 GB RAM · batch=10 · 5 steps × 2 minutes · start=100, step=50

### contended

One opp shared by every CTE — maximum `FOR UPDATE` lock contention on a single row.

```
rate/s   p50     p99       max
100      3ms     5ms       28ms
150      2ms     5ms       26ms
200      2ms     954ms     2.632s   ← queue builds up
250      2ms     6.466s    11.056s
300      2ms     11.457s   19.524s
```

**Saturation point: ~150 CTE/s** (p99 jumps from 5ms → 954ms at 200 CTE/s).

p50 stays at 2ms even at 300 CTE/s — requests that grab the lock quickly are fine; the tail
(p99/max) explodes because goroutines queue behind a single `FOR UPDATE` row.

### diverse

Opp pool rotated — locks spread across many rows, minimal contention.

```
rate/s   p50     p99     max
100      3ms     5ms     28ms
150      2ms     4ms     31ms
200      2ms     4ms     12ms
250      2ms     4ms     25ms
300      1ms     3ms     25ms
```

**No saturation up to 300 CTE/s.** p99 stays ≤ 5ms through every step.
PG carries 300 CTE/s × 10 drivers = 3,000 settled drivers/s without degrading.

### oversell

One opp with `capacity = rate/2`, so ~half of the CTEs are REJECTED by the CTE backstop.

```
rate/s   p50     p99     max
100      3ms     5ms     15ms
150      2ms     4ms     28ms
200      2ms     3ms     13ms
250      2ms     3ms     13ms
300      1ms     3ms     30ms
```

**No saturation up to 300 CTE/s.** The REJECTED path is fast (no INSERT, no UPDATE beyond the
`FOR UPDATE` read). Latency is similar to diverse — the `WHERE rn <= remaining` filter
short-circuits before touching `bookings`.

### idempotent

A fixed driver pool already settled; every subsequent call returns DUPLICATE.

```
rate/s   p50     p99     max
100      3ms     4ms     9ms
150      2ms     4ms     10ms
200      2ms     4ms     10ms
250      2ms     4ms     16ms
300      1ms     3ms     16ms
```

**No saturation up to 300 CTE/s.** The CTE `existing` join short-circuits early; the DUPLICATE
path skips INSERT and UPDATE entirely, keeping latency flat.

---

## Grafana

![Grafana — ramp results overview](../assets/lt-pg/Screenshot%202026-06-13%20at%2018.06.46.png)

---

## Metrics

| Metric | Description |
|--------|-------------|
| `loadtest_pg_claims_total{scenario,result}` | Counter: committed / duplicate / rejected / error |
| `loadtest_pg_cte_total{scenario}` | Counter: number of CTE runs by scenario |
| `loadtest_pg_oversell_violations_total` | Must be 0 — red on Grafana if > 0 |
| `loadtest_pg_idempotent_violations_total` | Must be 0 — red on Grafana if > 0 |
| `loadtest_pg_latency_p50_seconds{scenario}` | p50 gauge by scenario |
| `loadtest_pg_latency_p99_seconds{scenario}` | p99 gauge by scenario |
| `loadtest_pg_latency_max_seconds{scenario}` | max gauge by scenario |

Grafana dashboard: `loadtest-pg`

---

## Conclusion — is PG a good backstop?

**Yes**, with one important caveat about a hot opportunity.

| Criterion | Requirement | Actual | Result |
|-----------|-------------|--------|--------|
| Diverse throughput (3K drivers/s) | sustains target load | p99 = 3ms at 300 CTE/s | ✓ |
| Idempotent (at-least-once) | no double-booking | p99 = 4ms, flat across every step | ✓ |
| Oversell correctness | 0 violations | 0 | ✓ |
| Contended saturation point | as high as possible | ~150 CTE/s (~1,500 drivers/s) | ⚠ |

**Notes:**

- **Hot-opportunity bottleneck**: contended saturates at ~150 CTE/s because every CTE serializes
  through a single `FOR UPDATE` row. At batch=10 that's ~1,500 drivers/s per opportunity —
  acceptable for normal load but a ceiling to keep in mind.
- **The Redis gate is essential**: the gate rejects ~90% of traffic before it touches PG. Without
  it, a burst of 10K drivers/opp would need ~1K CTE/s, far above the contended ceiling.
- **PG is the correctness backstop, not a throughput engine**: diverse and idempotent show PG
  carries high throughput when load is spread across many opportunities. The design is right —
  Redis handles single-opp hot traffic; PG settles across many opps.
- **Scaling**: if a single opportunity must carry > 1,500 drivers/s at PG, shard the opportunity
  into N sub-opportunities or rely on the Redis gate to absorb the peak.

**Verdict**: the PG bulk-settle CTE is a good correctness backstop for the target workload (~10K
drivers/opp). The Redis gate's headroom (~8×) ensures PG never sees the full burst. The combination
of `UNIQUE(opp, driver)` + atomic decrement prevents overselling correctly even under concurrent
load.
