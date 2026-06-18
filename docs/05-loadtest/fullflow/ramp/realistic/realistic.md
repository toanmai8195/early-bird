# Full Flow — Ramp: realistic

Throughput ramp modelling a **real booking window**: a fresh opp `capacity=1000` per step that fills
in the first second, after which ~99% of the step is pure fast-reject at the Redis gate. Driven over
real HTTP by `com/tm/loadtest/server` with `--run=ramp`. Every request carries
`X-Caller-Id: realistic`, so the server / manager / PG metrics below are filtered to
`caller="realistic"` on Grafana.

Source: `com/tm/loadtest/server` · Dashboard: **Loadtest: Server (Full Flow)**, variable `caller=realistic`

---

## What it stresses

A new opp with `capacity=1000` each step, RPS ramped `1K → 2K → 5K → 10K`, 2 min/step. The opp fills
(1000 ACCEPTs) within the first ~second, then the remaining ~99% of the step hits
`SCARD ≥ capacity → FULL` and is rejected at Redis **without touching Kafka or PG**. So this measures
the **fast-reject path** — the dominant shape of real traffic — and, after each step, re-verifies the
opp against PG for oversell.

A step is the ceiling when `error% > 5%` or `p99 > 200ms`.

---

## Results

```
target    actual    sent       p50     p99     max     error%   verify (remaining / booked)
──────────────────────────────────────────────────────────────────────────────────────────
1000      999       119990     1ms     5ms     18ms    0.0%     0 / 1000
2000      1999      240000     2ms     6ms     45ms    0.0%     0 / 1000
5000      4999      599900     2ms     6ms     50ms    0.0%     0 / 1000
10000     9997      1199800    2ms     6ms     50ms    0.0%     0 / 1000
──────────────────────────────────────────────────────────────────────────────────────────
Max sustainable RPS (error<5%, p99<200ms): ~9997
```

**No saturation up to 10K RPS, and every step verifies zero oversell against PG.** Two things stand
out versus `contended` / `diverse`:

- **p99 drops to 5–6ms** (vs ~15ms on the full-happy-path patterns), because ~99% of requests
  short-circuit at the Redis gate (`SCARD ≥ capacity`) without ever touching Kafka or PG. The gate
  decision is sub-millisecond; only the ~1000 ACCEPTs per step pay the full path.
- **PG verify is exact every step**: `booked == 1000`, `remaining == 0` → the opp fills to capacity,
  no oversell, even at 10K RPS hammering it.

---

## Grafana — `caller="realistic"`

### HTTP API · Kafka · Redis gate

![realistic — HTTP API, Kafka publish, gate outcome (caller=realistic)](../../../../assets/lt-full/ramp/realistic/Screenshot%202026-06-18%20at%2021.56.35.png)

- **API Claim Rate** is split `realistic / full` (yellow) vs `realistic / ok` (green): the `full`
  plateau dominates and climbs with RPS, while `ok` is just a thin spike at the start of each step —
  the opp filling its 1000 slots before everything else turns FULL.
- **API Claim Latency P99** shows the same split: `full` sits low (~5ms, gate-only), `ok` briefly
  spikes (~15–20ms, the full HTTP→Redis→Kafka path) only while the opp is filling.
- **Kafka Publish Rate** and **Consumer Handle Rate** show **isolated spikes** at each step start
  (only the ~1000 `ok` claims publish/consume), then drop to zero — the FULL traffic never reaches
  Kafka.
- **Gate Release/Reject** = *No data* — the opp fills to exactly capacity, so PG never REJECTs.

### PG (drivers vs CTE) · end-to-end latency

![realistic — PG drivers/s vs CTE/s, settle latency, e2e (caller=realistic)](../../../../assets/lt-full/ramp/realistic/Screenshot%202026-06-18%20at%2021.56.43.png)

- **PG settle — DRIVERS/s** and **CTE/s** show **brief bursts** at each step start (~1000 drivers
  committed in a short window) then go quiet — PG only ever sees the accepted claims, ~1000 per step,
  regardless of whether the step fired 1K or 10K RPS. This is the gate **shielding PG**: the 9K+
  rejected requests never become settle work.
- **PG settle latency P99 — per CTE** spikes during the fill burst then idles.
- **E2E notify latency P99** (`realistic / confirmed`) spikes ~20ms at each fill, matching the brief
  settle bursts.

---

## Reading: the gate shields PG

`realistic` is the pattern that proves the two-layer design pays off under real traffic shape:

| | what hits PG | API p99 |
|--|--|--|
| **contended / diverse** (cap 10M) | every request (10K drivers/s) | ~15ms (full path) |
| **realistic** (cap 1000) | only ~1000 per step, in a burst | ~5–6ms (FULL fast-reject) |

At 10K RPS into a capacity-1000 opp, **~92–99% of requests are fast-rejected at Redis** and PG sees
roughly **1000 drivers per step** — not 10K/s. So the PG settle rate here is *not* stressed (its hot-
opp ceiling is measured separately in `pg.md`); what's stressed is the gate's FULL-reject rate, whose
ceiling is far above 10K (see `redis.md`, ~78K RPS).

## Verdict

The realistic booking-window shape sustains **10K RPS at p99 ≈ 5–6ms, 0% error, zero oversell every
step** (`booked == 1000`, `remaining == 0`). The Redis gate absorbs the ~99% of traffic that arrives
after the opp fills, so PG only ever commits the real 1000 bookings — exactly the load-shedding the
design intends. This is the cheapest path in the system and the one most real traffic takes.
