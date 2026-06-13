# Test Cases

Manual end-to-end test cases for the booking claim system. Each case includes `curl` commands plus
the steps to run + expected results. For detailed API see [api.md](../04-api/api.md).

## Environment setup

```bash
# 1. Infra: Redis, Kafka, Postgres, Prometheus, Grafana
make infra-up

# 2. Server (HTTP :8080, metrics :9404) + Manager (consumer, metrics :9405)
make server
make manager

# 3. Check the server is alive
curl -sf http://localhost:8080/health && echo
```

Shared variables used in the commands below (epoch seconds for the booking window):

```bash
BASE=http://localhost:8080
NOW=$(date +%s)
OPEN_START=$((NOW - 60))       # already open
OPEN_END=$((NOW + 3600))       # 1h left
gen() { command -v uuidgen >/dev/null && uuidgen || echo "idem-$RANDOM-$RANDOM"; }
```

> Note: a claim returning `202 ACCEPTED` is **async** — the manager settles PG afterward. Cases that
> check `remaining` should wait ~1s (`sleep 1`) before `GET` to let the manager commit.

---

## TC-01 — Create an opportunity

**Goal:** create a new opportunity, PG writes + Redis syncs `opp_meta`.

```bash
OPP=opp-tc01
curl -i -X POST "$BASE/opportunities/$OPP" \
  -H 'Content-Type: application/json' \
  -d "{\"region_id\":\"hanoi\",\"zone_id\":\"zone-1\",\"capacity\":1000,
       \"booking_window_start\":$OPEN_START,\"booking_window_end\":$OPEN_END}"
```

**Expected:** `201 Created`, body is the opportunity JSON with `remaining: 1000`.

---

## TC-02 — Get an opportunity

**Goal:** read state from PG (the source of truth).

```bash
curl -s "$BASE/opportunities/$OPP" | jq
```

**Expected:** `200 OK`; `capacity == remaining == 1000`; `booking_window_start/end` are epoch
seconds (numbers).

---

## TC-03 — Claim a slot (new driver, happy path)

**Goal:** gate `OK` → publish to Kafka → manager settles → `remaining` drops by 1.

```bash
curl -i -X POST "$BASE/opportunities/$OPP/bookings" \
  -H "X-Driver-Id: driver-001" \
  -H "X-Idempotency-Key: $(gen)"

sleep 1
curl -s "$BASE/opportunities/$OPP" | jq '.remaining'
```

**Expected:** the claim returns `202` `{"status":"ACCEPTED"}`; after settle `remaining == 999`.

---

## TC-04 — Duplicate claim (DUP, idempotent at the gate)

**Goal:** the same `driver-001` claims a second time → gate `DUP`, no new booking created.

```bash
curl -i -X POST "$BASE/opportunities/$OPP/bookings" \
  -H "X-Driver-Id: driver-001" \
  -H "X-Idempotency-Key: $(gen)"

sleep 1
curl -s "$BASE/opportunities/$OPP" | jq '.remaining'
```

**Expected:** `200 OK` `{"status":"ACCEPTED"}` (DUP); `remaining` is still `999` (no further drop).

---

## TC-05 — Missing required header

**Goal:** request validation.

```bash
# Missing X-Idempotency-Key
curl -i -X POST "$BASE/opportunities/$OPP/bookings" -H "X-Driver-Id: driver-002"
# Missing X-Driver-Id
curl -i -X POST "$BASE/opportunities/$OPP/bookings" -H "X-Idempotency-Key: $(gen)"
```

**Expected:** both return `400 Bad Request` `{"status":"BAD_REQUEST"}`; `remaining` unchanged.

---

## TC-06 — No overselling (FULL under contention)

**Goal:** small capacity + many drivers claiming concurrently → exactly `capacity` ACCEPTED, the
rest `409 FULL`, `remaining` never goes negative.

```bash
OPP=opp-tc06
curl -s -o /dev/null -X POST "$BASE/opportunities/$OPP" \
  -H 'Content-Type: application/json' \
  -d "{\"region_id\":\"hn\",\"zone_id\":\"z1\",\"capacity\":5,
       \"booking_window_start\":$OPEN_START,\"booking_window_end\":$OPEN_END}"

# 50 distinct drivers claim in parallel
seq 1 50 | xargs -P 50 -I{} curl -s -o /dev/null -w "%{http_code}\n" \
  -X POST "$BASE/opportunities/$OPP/bookings" \
  -H "X-Driver-Id: drv-{}" -H "X-Idempotency-Key: k-{}" | sort | uniq -c

sleep 1
curl -s "$BASE/opportunities/$OPP" | jq '.remaining'
```

**Expected:** exactly **5** requests `202`, **45** requests `409` (FULL); `remaining == 0` (not
negative, no oversell).

---

## TC-07 — Booking window closed (CLOSED)

**Goal:** claim outside the window → `409 CLOSED`, no touch to Kafka/PG.

```bash
OPP=opp-tc07
PAST_START=$((NOW - 7200)); PAST_END=$((NOW - 3600))   # window already ended
curl -s -o /dev/null -X POST "$BASE/opportunities/$OPP" \
  -H 'Content-Type: application/json' \
  -d "{\"region_id\":\"hn\",\"zone_id\":\"z1\",\"capacity\":10,
       \"booking_window_start\":$PAST_START,\"booking_window_end\":$PAST_END}"

curl -i -X POST "$BASE/opportunities/$OPP/bookings" \
  -H "X-Driver-Id: drv-x" -H "X-Idempotency-Key: $(gen)"
```

**Expected:** `409 Conflict` `{"status":"CLOSED"}`. (A future window — `booking_window_start` >
now — also returns `CLOSED`.)

---

## TC-08 — Opportunity does not exist

**Goal:** claim an opp that hasn't been created → the gate doesn't find `opp_meta` → `CLOSED`.

```bash
curl -i -X POST "$BASE/opportunities/opp-does-not-exist/bookings" \
  -H "X-Driver-Id: drv-x" -H "X-Idempotency-Key: $(gen)"
```

**Expected:** `409 Conflict` `{"status":"CLOSED"}`.

---

## TC-09 — Update an opportunity

**Goal:** change capacity/window, re-sync `opp_meta`.

```bash
OPP=opp-tc01
curl -i -X PUT "$BASE/opportunities/$OPP" \
  -H 'Content-Type: application/json' \
  -d "{\"region_id\":\"hanoi\",\"zone_id\":\"zone-2\",\"capacity\":2000,\"remaining\":1500,
       \"booking_window_start\":$OPEN_START,\"booking_window_end\":$OPEN_END}"
```

**Expected:** `200 OK`, body reflects `capacity:2000`, `remaining:1500`, `zone_id:"zone-2"`.

---

## TC-10 — Delete then claim

**Goal:** delete an opp → `opp_meta` + `claimed_set` are removed → a subsequent claim returns
`CLOSED`. DELETE also **cascades** to remove all `bookings` of the opp (FK `ON DELETE CASCADE`), so
it can be deleted even when the opp already has bookings.

```bash
OPP=opp-tc10
curl -s -o /dev/null -X POST "$BASE/opportunities/$OPP" \
  -H 'Content-Type: application/json' \
  -d "{\"region_id\":\"hn\",\"zone_id\":\"z1\",\"capacity\":10,
       \"booking_window_start\":$OPEN_START,\"booking_window_end\":$OPEN_END}"

curl -i -X DELETE "$BASE/opportunities/$OPP"                          # → 204
curl -s -o /dev/null -w "%{http_code}\n" "$BASE/opportunities/$OPP"   # GET → 404
curl -i -X POST "$BASE/opportunities/$OPP/bookings" \
  -H "X-Driver-Id: drv-x" -H "X-Idempotency-Key: $(gen)"             # → 409 CLOSED
```

**Expected:** DELETE `204`; GET `404`; claim `409 CLOSED`.

---

## TC-11 — Idempotency / at-least-once (no double booking)

**Goal:** replay the same driver multiple times through the full stack → at most 1 booking,
`remaining` drops by exactly 1.

```bash
OPP=opp-tc11
curl -s -o /dev/null -X POST "$BASE/opportunities/$OPP" \
  -H 'Content-Type: application/json' \
  -d "{\"region_id\":\"hn\",\"zone_id\":\"z1\",\"capacity\":100,
       \"booking_window_start\":$OPEN_START,\"booking_window_end\":$OPEN_END}"

for i in 1 2 3 4 5; do
  curl -s -o /dev/null -w "claim $i: %{http_code}\n" \
    -X POST "$BASE/opportunities/$OPP/bookings" \
    -H "X-Driver-Id: drv-dup" -H "X-Idempotency-Key: $(gen)"
done

sleep 1
curl -s "$BASE/opportunities/$OPP" | jq '.remaining'
```

**Expected:** the 1st time `202`, subsequent times `200` (DUP); `remaining == 99` (only 1 booking).
PG `UNIQUE(opportunity_id, driver_id)` guarantees no double-book even when Kafka redelivers.

---

## TC-12 — Redis down → circuit breaker shed (THROTTLED)

**Goal:** Redis dies → the CB on the server opens → claims return `503 THROTTLED` (shed load, no
degraded pass-through). The CB must be **enabled** (default `DISABLE_CIRCUIT_BREAKER=false`).

```bash
docker compose -f com/tm/infra/docker-compose.yaml stop redis

# Fire a few requests to accumulate errors so the CB opens
for i in $(seq 1 10); do
  curl -s -o /dev/null -w "%{http_code} " \
    -X POST "$BASE/opportunities/opp-tc01/bookings" \
    -H "X-Driver-Id: drv-$i" -H "X-Idempotency-Key: $(gen)"
done; echo

docker compose -f com/tm/infra/docker-compose.yaml start redis   # recover
```

**Expected:** the first few requests return `503 UNAVAILABLE` (gate error) → once the CB opens,
subsequent requests return `503` `{"status":"THROTTLED"}`. No `202`. After Redis comes back + the
CB half-opens, claims return to normal.

---

## TC-13 — PG down → kill switch (PG_UNAVAILABLE)

**Goal:** PG dies → the CB on the manager opens → sets `pg_health=down` → the server's gate returns
`503 PG_UNAVAILABLE`. Requires the manager running + traffic for the CB to open.

```bash
docker compose -f com/tm/infra/docker-compose.yaml stop postgres

# Generate traffic so the manager's settle fails and the CB opens (settle error/>5s)
for i in $(seq 1 20); do
  curl -s -o /dev/null -X POST "$BASE/opportunities/opp-tc11/bookings" \
    -H "X-Driver-Id: pgdrv-$i" -H "X-Idempotency-Key: $(gen)"
done
sleep 6   # wait for the CB to open + markDown to set pg_health

curl -i -X POST "$BASE/opportunities/opp-tc11/bookings" \
  -H "X-Driver-Id: pgdrv-x" -H "X-Idempotency-Key: $(gen)"

docker compose -f com/tm/infra/docker-compose.yaml start postgres   # recover
```

**Expected:** after the manager's CB opens, claims return `503` `{"status":"PG_UNAVAILABLE"}`. After
PG comes back, `PgHealthProbe` (`SELECT 1`) closes the CB → `markUp` clears `pg_health` → the server
accepts claims again (the `pg_health` flag also auto-expires after the 30s TTL if the probe hasn't
run yet).

---

## TC-14 — Redis restart → warmup auto-rebuilds

**Goal:** Redis loses its entire dataset (restart) → `RedisWarmupService` reads PG and rebuilds
`opp_meta` + `claimed_set`; meanwhile the PG backstop holds no-oversell.

```bash
# Precondition: opp-tc11 is open and already has 1 booking (drv-dup) from TC-11
docker compose -f com/tm/infra/docker-compose.yaml restart redis

sleep 6   # > redis-warmup-interval-ms (default 5s)

# The previously-booked driver claims again → still DUP (claimed_set was rebuilt from PG)
curl -i -X POST "$BASE/opportunities/opp-tc11/bookings" \
  -H "X-Driver-Id: drv-dup" -H "X-Idempotency-Key: $(gen)"
```

**Expected:** after warmup, `drv-dup`'s claim returns `200` (DUP) — proving `claimed_set` was
restored from PG; the opp still accepts new claims normally, with no oversell.

---

## TC-15 — Health check

```bash
curl -i "$BASE/health"
```

**Expected:** `200 OK` `{"status":"ok"}`.

---

## TC-16 — Metrics (observation)

**Goal:** confirm the counters/latency emit the correct outcomes.

```bash
# Server: claim outcomes + latency
curl -s http://localhost:9404/metrics | grep -E 'booking_api_claim'
# Manager: settle outcomes + e2e latency
curl -s http://localhost:9405/metrics | grep -E 'booking_consumer_handle|booking_e2e_notify'
```

**Expected:** see `booking_api_claim_total{result="ok|full|dup|closed|throttled|..."}` increase with
the cases above; `booking_consumer_handle_total{result="committed|duplicate|rejected",instance=...}`
on the manager. Visual dashboard: Grafana `http://localhost:3000`.

---

## Cleanup

```bash
make down        # tear down server + manager + loadtest
make infra-down  # tear down all infra
```

---

## Checklist

Tick after running each case:

- [x] TC-01 — Create an opportunity → `201`, `remaining: 1000`
- [x] TC-02 — Get an opportunity → `200`, `capacity == remaining`, window epoch
- [x] TC-03 — Claim a new driver → `202 ACCEPTED`, `remaining == 999`
- [x] TC-04 — Duplicate claim (DUP) → `200 ACCEPTED`, `remaining` no further drop
- [x] TC-05 — Missing header → `400 BAD_REQUEST`, `remaining` unchanged
- [x] TC-06 — No overselling → 5×`202`, 45×`409 FULL`, `remaining == 0`
- [x] TC-07 — Window closed → `409 CLOSED`
- [x] TC-08 — Opp does not exist → `409 CLOSED`
- [x] TC-09 — Update an opportunity → `200`, reflects new capacity/remaining/zone
- [x] TC-10 — Delete then claim → `204` / GET `404` / claim `409 CLOSED`
- [x] TC-11 — Idempotency replay → 1×`202` + N×`200`, `remaining == 99`
- [x] TC-12 — Redis down → `503 THROTTLED` (CB open), recovers after Redis is back
- [x] TC-13 — PG down → `503 PG_UNAVAILABLE` (kill switch), recovers via probe
- [x] TC-14 — Redis restart → warmup rebuild, `drv-dup` still `200 DUP`
- [x] TC-15 — Health check → `200`, `{"status":"ok"}`
- [x] TC-16 — Metrics → see `booking_api_claim_total` / `booking_consumer_handle_total`
