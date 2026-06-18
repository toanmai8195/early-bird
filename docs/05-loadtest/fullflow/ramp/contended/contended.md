# Full Flow — Ramp: contended

Throughput ramp on a **single hot opportunity**, full happy path (HTTP → Redis `SADD` → Kafka
publish → PG settle), driven over real HTTP by `com/tm/loadtest/server` with `--run=ramp`. Every
request carries `X-Caller-Id: contended`, so the server / manager / PG metrics below are filtered to
`caller="contended"` on Grafana.

Source: `com/tm/loadtest/server` · Dashboard: **Loadtest: Server (Full Flow)**, variable `caller=contended`

---

## What it stresses

One opp, `capacity = 10,000,000` (never fills), RPS ramped `1K → 2K → 5K → 10K`, 2 min/step. Because
the opp never fills, **every** request runs the full path — so this hammers one hot Redis key, one
hot Kafka partition, and one hot PG row (`FOR UPDATE` serialised) all at once. It's the worst case
for a single ultra-hot opportunity.

A step is the ceiling when `error% > 5%` or `p99 > 200ms`.

---

## Results

```
target    actual    sent       p50     p99     max     error%
──────────────────────────────────────────────────────────────
1000      999       120000     11ms    15ms    24ms    0.0%
2000      1999      240000     12ms    15ms    30ms    0.0%
5000      4998      599900     12ms    17ms    56ms    0.0%
10000     9999      1200000    12ms    16ms    29ms    0.0%
──────────────────────────────────────────────────────────────
Max sustainable RPS (error<5%, p99<200ms): ~9999
```

**No saturation up to 10K RPS.** p99 stays flat at **15–17ms** and error rate at **0%** even as the
full 10K RPS lands on one Redis key + one Kafka partition. The server's event loop never blocks (gate
+ producer return `Future`, no `executeBlocking`); p50 holds ~12ms across every step.

---

## Grafana — `caller="contended"`

### HTTP API · Kafka · Redis gate

![contended — HTTP API, Kafka publish, gate outcome (caller=contended)](../../../../assets/lt-full/ramp/contended/Screenshot%202026-06-18%20at%2021.53.11.png)

- **API Claim Rate** (`contended / ok`) steps up cleanly 1K→2K→5K→10K; **API Claim Latency P99** holds
  a flat ~15ms across all four steps — the full happy path cost (Redis + Kafka publish), unaffected by
  the rising RPS on one key.
- **Kafka Publish Rate (via API ok)** and **Consumer Handle Rate per instance**
  (`manager-… contended committed`) track the claim rate one-for-one — every accepted claim is
  published and consumed exactly once, no consumer lag building.
- **Gate Release/Reject** = *No data* — capacity is 10M so nothing ever fills or REJECTs; the manager
  never has to release/reject a slot.

### PG (drivers vs CTE) · end-to-end latency

![contended — PG drivers/s vs CTE/s, settle latency, e2e (caller=contended)](../../../../assets/lt-full/ramp/contended/Screenshot%202026-06-18%20at%2021.53.26.png)

This is the panel that explains how one hot row keeps up:

- **PG settle — DRIVERS/s** (`booking.dao.commit`) peaks ≈ **10K ops/s**, while **PG settle — CTE/s**
  (`booking.pg.settle.batch`) peaks only ≈ **80 ops/s** → **≈ 125 drivers per CTE**. All 10K claims
  for one opp land on one partition, and the manager bulk-settles them ~125 at a time
  (`SETTLE_BATCH_SIZE=200`) through one `FOR UPDATE` row. That amortisation is *why* a single
  serialised row sustains 10K drivers/s — at batch=10 the same row tops out at ~1,500/s (see
  `pg.md`).
- **PG settle latency P99 — per CTE** (`booking.dao.commit.latency` and `booking.pg.settle.latency`)
  stays ~4–8ms even at peak: bigger batches, but the per-CTE cost grows sub-linearly, so the row
  never becomes the bottleneck at 10K.
- **E2E notify latency P99** (`contended / confirmed`) sits around ~20ms (HTTP accept → confirmation
  pushed), tracking the settle throughput.

---

## Reading: why a single hot opp survives 10K RPS

`contended` is the pessimal single-row case, yet shows no saturation — because the bottleneck
(`FOR UPDATE` serialisation on one `opportunities` row) is amortised by bulk-settle:

```
drivers/s = batch × (1 / lock_hold_time)
~10K       ≈ 125  × (1 / ~12ms)
```

The dashboard makes this concrete: DRIVERS/s ≫ CTE/s (≈125×) for `caller="contended"`, vs the
`diverse` pattern where load spreads across rows and CTE/s ≈ drivers/s. Same input rate, opposite PG
shape — now readable side by side via the `caller` filter.

## Verdict

A single ultra-hot opportunity sustains **10K RPS at p99 ≈ 15ms, 0% error**, end-to-end. Redis's hot
key, Kafka's hot partition, and PG's hot row all hold — the non-blocking server keeps the event loop
free, and the manager's bulk-settle turns ~10K drivers/s into only ~80 CTE/s on the contended row.
The only ceiling left for one opp is PG's per-row settle rate, pushed far out by batching.
