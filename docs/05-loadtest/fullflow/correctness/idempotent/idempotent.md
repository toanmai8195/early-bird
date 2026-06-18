# Full Flow — Idempotency

**No-double-booking** test for the whole stack under at-least-once delivery (HTTP → Redis gate →
Kafka → PG), driven over real HTTP by `com/tm/loadtest/server` with `--run=idempotent`. Every
request carries `X-Caller-Id: idempotent`, so the server / manager / PG metrics can be filtered to
`caller="idempotent"` on Grafana.

Source: `com/tm/loadtest/server` · Dashboard: **Loadtest: Server (Full Flow)**, variable `caller=idempotent`

---

## What it checks

A fresh opp with `capacity=1000`, one fixed batch of `batch=200` unique drivers, replayed
`replay-rounds=5` times at 2000 rps (simulating Kafka re-delivery / client retries):

1. **round 0** — the batch of 200 unique drivers → expect all **202 ACCEPTED**.
2. **rounds 1..5** — the *exact same* batch re-sent → expect all **200 DUP**, never ACCEPTED twice.

The decisive assertion: across 6 sends of the same 200 drivers, PG ends with **exactly 200 booked**
(not 1200) and `remaining == capacity − 200`. Redis `SISMEMBER` short-circuits replays before they
touch Kafka; PG's `UNIQUE(opp, driver)` is the backstop if Redis ever lost the set.

## Run configuration

```
capacity=1000  batch=200  replay-rounds=5  rps=2000
```

---

## Results

```
round   accepted  dup    full   throttled  errors   check
──────────────────────────────────────────────────────────────────
0       200       0      0      0          0        ✓ all accepted (202)
1       0         200    0      0          0        ✓ all dup (200)
2       0         200    0      0          0        ✓ all dup (200)
3       0         200    0      0          0        ✓ all dup (200)
4       0         200    0      0          0        ✓ all dup (200)
5       0         200    0      0          0        ✓ all dup (200)
──────────────────────────────────────────────────────────────────
verify: capacity=1000  remaining=800  booked=200
  ✓ remaining >= 0
  ✓ booked (200) <= capacity (1000)
  ✓ remaining + booked == capacity (1000)
```

- **Round 0**: all 200 unique drivers ACCEPTED (202) → published to Kafka → settled once in PG.
- **Rounds 1–5**: 5 replays of the same 200 drivers each return **200 DUP**, **0 accepted** — the
  gate's `SISMEMBER` recognises every driver already in `claimed_set` and rejects the replay before
  Kafka, so no duplicate event is ever produced.
- **PG verify**: `booked == 200`, `remaining == 800` — out of 1200 total requests sent (200 × 6),
  the source of truth holds exactly the 200 distinct bookings. **No double-booking.**

---

## Why it holds end-to-end

Two independent layers each prevent the duplicate, so a replay is a no-op even if one layer is
bypassed:

- **Redis gate (fast path)** — `SISMEMBER claimed_set:{opp}` returns DUP on every replay, so rounds
  1–5 never reach Kafka or PG. This is what the `dup=200 / accepted=0` rows show.
- **PG `UNIQUE(opp, driver)` (backstop)** — if the gate's set were lost (Redis restart/flush) and a
  replay slipped through to the settle CTE, `INSERT ... ON CONFLICT (opportunity_id, driver_id) DO
  NOTHING` makes it a no-op and `remaining` is decremented only by the *inserted* count, so a
  re-delivered batch still books each driver exactly once.

This is exactly the at-least-once contract the manager relies on: the Kafka offset is committed only
after a batch settles, so on crash/replay the batch is reprocessed — and the `DUPLICATE` outcome
keeps it harmless.

## Verdict

Replaying the same 200-driver batch 6× yields **200 booked, 0 double-booked**, every replay cleanly
classified DUP at the gate. Redis dedup short-circuits replays for free; PG's UNIQUE constraint
guarantees correctness even if Redis state is lost — idempotency holds across the full stack.
