# Full Flow — Gate Correctness

**No-oversell** test for the whole stack (HTTP → Redis gate → Kafka → PG), driven over real HTTP by
`com/tm/loadtest/server` with `--run=gate`. Every request carries `X-Caller-Id: gate`, so the
server / manager / PG metrics below can be filtered to `caller="gate"` on Grafana.

Source: `com/tm/loadtest/server` · Dashboard: **Loadtest: Server (Full Flow)**, variable `caller=gate`

---

## What it checks

A fresh opp with `capacity=1000`, three phases at 2000 rps:

1. **seed** — `seedN` unique drivers → expect **202 ACCEPTED**.
2. **dup** — replay the seed drivers → expect **200 DUP** (`SISMEMBER` hit, opp not yet full).
3. **rest** — new unique drivers → first `capacity − claimed` get **202**, the remainder **409 FULL**.

Because `dupN = 20000 × 5% = 1000 ≥ capacity`, the harness halves the seed: `seedN = dupN = 500`,
`restN = 19000`. So `accepted` must end at exactly `capacity = 1000` (500 seed + 500 rest), with the
other 18500 rest requests fast-rejected FULL, and the 500 replays returning DUP.

The decisive assertion: **`accepted == capacity`** at Redis *and* `booked == 1000`, `remaining == 0`
in PG — the gate's accept count matches exactly what the source of truth commits, no oversell.

## Run configuration

```
rounds=3  capacity=1000  requests=20000  dup=5%  rps=2000
(--disable-cb → server DISABLE_CIRCUIT_BREAKER=true, so throttled must be 0)
```

---

## Results — 3 rounds

```
round   p50        p99        accepted  full    dup    verify (remaining / booked)
────────────────────────────────────────────────────────────────────────────────────
1       973µs      13.376ms   1000      18500   500    0 / 1000
2       1.586ms    12.937ms   1000      18500   500    0 / 1000
3       1.675ms    12.680ms   1000      18500   500    0 / 1000
────────────────────────────────────────────────────────────────────────────────────
want                —         1000      18500   500    remaining + booked == 1000
```

Every round, all assertions pass:

```
✓ no oversell: accepted == capacity
✓ full+throttled(dup+rest) == restN-(cap-claimedBeforeRest)
✓ dup == effectiveSeedN              (effectiveSeedN = 500)
✓ no connection errors
✓ no lost responses: total == sent
✓ CB disabled: throttled == 0
✓ remaining >= 0   ✓ booked <= capacity   ✓ remaining + booked == 1000
```

- **End-to-end, no oversell.** Exactly 1000 accepted == capacity across all 3 rounds; the PG verify
  confirms `booked == 1000`, `remaining == 0` — Redis's accept count equals what actually committed.
- **~92.5% fast-rejected.** 18500 / 20000 hit `SCARD ≥ capacity → FULL` at Redis, never touching
  Kafka or PG.
- **p99 ≈ 13ms** is the full happy path (gate decision < 1ms; the tail is HTTP + Kafka publish +
  JVM GC, not Redis).

---

## Grafana — `caller="gate"`

### HTTP API · Kafka · Redis gate

![Gate — HTTP API, Kafka publish, gate outcome (caller=gate)](../../../../assets/lt-full/correctness/gate/Screenshot%202026-06-18%20at%2021.39.29.png)

- **API Claim Rate / Latency P99** split by result (`gate / ok`, `gate / full`, `gate / dup`): the
  `ok` path carries the full p99 (≈13–15ms, Redis + Kafka publish), while `full` / `dup` short-circuit
  at the gate and sit near ~1ms.
- **Kafka Publish Rate (via API ok)** tracks only the accepted claims — one publish per `ok`.
- **Consumer Handle Rate per instance** moves with the publish rate (`manager-… gate committed`):
  every accepted claim is consumed once.
- **Gate Release/Reject** = *No data* — the gate test fills capacity exactly, so PG never REJECTs and
  the manager never has to release/reject a slot (expected for a clean gate run).

### PG (drivers vs CTE) · end-to-end latency

![Gate — PG drivers/s vs CTE/s, settle latency, e2e (caller=gate)](../../../../assets/lt-full/correctness/gate/Screenshot%202026-06-18%20at%2021.39.53.png)

- **PG settle — DRIVERS/s** (`booking.dao.commit`) peaks ≈ 40 ops/s while **PG settle — CTE/s**
  (`booking.pg.settle.batch`) peaks only ≈ 1.2 ops/s → **≈ 33 drivers per CTE**. The 1000 accepted
  claims for one opp land on one partition, and the manager bulk-settles them a few dozen at a time —
  exactly the drivers/s ÷ CTE/s = effective sub-batch story, now visible per caller.
- **PG settle latency P99 — per CTE** (both `booking.dao.commit.latency` and
  `booking.pg.settle.latency`) stays in the few-ms range (≈ 6–8ms): one hot opp, but small batches.
- **E2E notify latency P99** (`gate / confirmed`) settles around ~25ms (HTTP accept → confirmation),
  with the cold-start tail at the first batch.

---

## Verdict

The two-layer correctness model holds end-to-end for a hot opportunity: **Redis rejects fast, PG
commits correctly, and the two never diverge** — accepted == 1000 == booked, zero oversell, stable
across 3 rounds. The `caller="gate"` slice makes it easy to read the same run from the API side
(p99 ≈ 13ms) and the PG side (drivers/s ≫ CTE/s, settle p99 ≈ few ms) on one dashboard.
