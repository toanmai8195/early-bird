# Full Flow — Ramp: diverse

Throughput ramp with load **spread across many opportunities**, full happy path (HTTP → Redis
`SADD` → Kafka publish → PG settle), driven over real HTTP by `com/tm/loadtest/server` with
`--run=ramp`. Every request carries `X-Caller-Id: diverse`, so the server / manager / PG metrics
below are filtered to `caller="diverse"` on Grafana.

Source: `com/tm/loadtest/server` · Dashboard: **Loadtest: Server (Full Flow)**, variable `caller=diverse`

---

## What it stresses

100 opps round-robin, each `capacity = 10,000,000` (never fills), RPS ramped `1K → 2K → 5K → 10K`,
2 min/step. Same happy path as `contended`, but the load is spread evenly across many Redis keys,
Kafka partitions, and PG rows — so there's no single-row `FOR UPDATE` serialisation. This is the
throughput baseline: what the stack does when contention is removed.

A step is the ceiling when `error% > 5%` or `p99 > 200ms`.

---

## Results

```
target    actual    sent       p50     p99     max     error%
──────────────────────────────────────────────────────────────
1000      999       120000     12ms    15ms    25ms    0.0%
2000      1999      239980     11ms    16ms    39ms    0.0%
5000      4998      599950     12ms    16ms    51ms    0.0%
10000     9997      1199800    12ms    17ms    38ms    0.0%
──────────────────────────────────────────────────────────────
Max sustainable RPS (error<5%, p99<200ms): ~9997
```

**No saturation up to 10K RPS.** p99 holds **15–17ms** at 0% error — the *same* p99 as `contended`.
At this RPS the bottleneck is neither Redis's hot slot nor Kafka's hot partition; the happy-path cost
is dominated by HTTP + publish latency, identical whether load hits one key or a hundred.

---

## Grafana — `caller="diverse"`

### HTTP API · Kafka · Redis gate

![diverse — HTTP API, Kafka publish, gate outcome (caller=diverse)](../../../../assets/lt-full/ramp/diverse/Screenshot%202026-06-18%20at%2021.55.24.png)

- **API Claim Rate** (`diverse / ok`) steps up 1K→2K→5K→10K; **API Claim Latency P99** stays flat at
  ~15ms — visually indistinguishable from the contended dashboard.
- **Kafka Publish Rate** and **Consumer Handle Rate per instance**
  (`manager-… diverse committed`) track the claim rate one-for-one — no consumer lag.
- **Gate Release/Reject** = *No data* — capacity is 10M, nothing fills or REJECTs.

### PG (drivers vs CTE) · end-to-end latency

![diverse — PG drivers/s vs CTE/s, settle latency, e2e (caller=diverse)](../../../../assets/lt-full/ramp/diverse/Screenshot%202026-06-18%20at%2021.55.33.png)

This is where `diverse` diverges sharply from `contended`:

- **PG settle — DRIVERS/s** (`booking.dao.commit`) peaks ≈ **10K ops/s**, and **PG settle — CTE/s**
  (`booking.pg.settle.batch`) peaks ≈ **6K ops/s** → **≈ 1.6 drivers per CTE**. Spread across 100
  opps, each poll only finds 1–2 records per opp, so the manager settles tiny sub-batches — CTE/s sits
  right under drivers/s. The 10K drivers/s is carried by **parallel CTEs on different rows**, not by
  big batches.
- **PG settle latency P99 — per CTE** stays ~**2ms** (`booking.pg.settle.latency`) — *lower* than
  contended's ~6–8ms, because there's no single-row lock contention; each CTE grabs a different row
  and commits immediately.
- **E2E notify latency P99** (`diverse / confirmed`) ~15–20ms, same shape as contended.

---

## Reading: same input, opposite PG shape

`diverse` and `contended` both push 10K drivers/s, but the PG side is the mirror image — and the
`caller` filter lets you read them on the same panels:

| | drivers/s | CTE/s | drivers ÷ CTE | settle p99 / CTE |
|--|--|--|--|--|
| **contended** (1 opp) | ~10K | ~80 | **~125** | ~6–8ms |
| **diverse** (100 opps) | ~10K | ~6K | **~1.6** | ~2ms |

- **contended** is bottlenecked on one `FOR UPDATE` row, so it survives only by amortising — big
  batches (~125 drivers/CTE), few CTEs.
- **diverse** has no single-row contention, so it doesn't *need* batching — tiny batches (~1.6),
  thousands of parallel CTEs, lower per-CTE latency.

This is the concrete picture behind the `booking.dao.commit = 10K` vs `booking.pg.settle.batch = 6K`
question: the 6K is **CTE/s**, the 10K is **drivers/s**, and their ratio is just the effective
sub-batch size — ~1.6 here because load is spread thin.

## Verdict

Spreading 10K RPS across 100 opportunities sustains **p99 ≈ 15ms, 0% error** — the same API latency
as one hot opp, but PG carries it through thousands of small parallel CTEs (settle p99 ~2ms) instead
of one heavily-batched hot row. With contention removed, neither Redis, Kafka, nor PG is the limit at
10K; the ceiling is the load-gen, not the stack.
