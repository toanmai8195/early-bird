# API Reference

Base URL: `http://localhost:8080`

Every JSON response uses a `status` field (claim) or an opportunity body. Times
(`booking_window_start` / `booking_window_end`) are **epoch seconds** (integers), matching
`opp_meta:{opp}` in Redis and the `opportunities` columns in PG — **not** ISO strings.

## Postman

Import [`early-bird.postman_collection.json`](early-bird.postman_collection.json) into Postman:

1. Postman → **Import** → select the file above.
2. Go to the collection's **Variables** and set `baseUrl` (default `http://localhost:8080`),
   `oppId`, and `driverId` to the opportunity/driver you want to test.
3. Run each request individually or use the **Collection Runner** to run them all.

The collection includes test scripts that automatically check the status code and body of each request.

---

## POST /opportunities/:id/bookings

Claim a slot. The driver identity + idempotency are passed via **HTTP headers** (not the body):

### Request

```http
POST /opportunities/opp-123/bookings
X-Driver-Id: driver-456
X-Idempotency-Key: uuid-or-hash
```

> The body is ignored. Missing either header (or an empty path id) → `400 BAD_REQUEST`.

### Response

The response is **ACCEPTED** (async) — it does not mean the booking has been written to the DB; the
manager settles PG asynchronously and then pushes a confirmation. The body is always of the form
`{"status":"<X>"}`.

| Status | Body | Meaning |
|--------|------|---------|
| `202 Accepted` | `{"status":"ACCEPTED"}` | Gate pass (OK), Kafka publish ok, settling async |
| `200 OK` | `{"status":"ACCEPTED"}` | DUP — the driver already claimed earlier, idempotent |
| `409 Conflict` | `{"status":"FULL"}` | No slots left — fast-reject at Redis (~90% of traffic) |
| `409 Conflict` | `{"status":"CLOSED"}` | Outside the booking window, or the opp does not exist |
| `503 Service Unavailable` | `{"status":"PG_UNAVAILABLE"}` | Kill switch `pg_health=down` — PG is sick, server sheds the claim |
| `503 Service Unavailable` | `{"status":"THROTTLED"}` | Redis circuit breaker open (Redis down/timeout) |
| `503 Service Unavailable` | `{"status":"UNAVAILABLE"}` | Kafka publish fail — the slot was released back |
| `400 Bad Request` | `{"status":"BAD_REQUEST"}` | Missing a required header |

> **Note:** `FULL`/`CLOSED` are expected fast-rejects. The `503`s are degrade signals (shed
> load) — the client should back off and retry with the *same* `X-Idempotency-Key` (safe thanks to
> PG UNIQUE).

---

## POST /opportunities/:id

Create a new opportunity. `opportunity_id` comes from the path, not the body.

### Request

```http
POST /opportunities/opp-123
Content-Type: application/json

{
  "region_id": "hanoi",
  "zone_id": "zone-1",
  "capacity": 1000,
  "booking_window_start": 1767225600,
  "booking_window_end": 4102444799
}
```

`region_id`, `zone_id`, `capacity` (>0), `booking_window_start`, `booking_window_end`
(end > start) are required. `remaining` is optional (defaults to `capacity`, used for updates).

### Response

| Status | Meaning |
|--------|---------|
| `201 Created` | Created; returns the opportunity JSON; `opp_meta:{opp}` is synced into Redis |
| `400 Bad Request` | Body missing a field / wrong range — `{"error":"..."}` |
| `409 Conflict` | ID already exists (PG UNIQUE) — `{"error":"..."}` |

---

## GET /opportunities/:id

Fetch opportunity info (including the current `remaining` slots from PG).

### Response

| Status | Meaning |
|--------|---------|
| `200 OK` | Opportunity JSON (see below) |
| `404 Not Found` | Does not exist — `{"error":"not found"}` |
| `503 Service Unavailable` | PG error — `{"error":"..."}` |

```json
{
  "opportunity_id": "opp-123",
  "region_id": "hanoi",
  "zone_id": "zone-1",
  "capacity": 1000,
  "remaining": 423,
  "booking_window_start": 1767225600,
  "booking_window_end": 4102444799
}
```

---

## PUT /opportunities/:id

Update an opportunity. The body matches POST (may include `remaining`).

| Status | Meaning |
|--------|---------|
| `200 OK` | Updated; returns the opportunity JSON; re-syncs `opp_meta` |
| `400 Bad Request` | Bad body |
| `404 Not Found` | Does not exist |
| `503 Service Unavailable` | PG error |

---

## DELETE /opportunities/:id

Delete an opportunity; also removes `opp_meta:{opp}` + `claimed_set:{opp}` in Redis, and
**cascades** to delete all `bookings` of the opp (FK `ON DELETE CASCADE`).

| Status | Meaning |
|--------|---------|
| `204 No Content` | Deleted |
| `404 Not Found` | Does not exist |
| `503 Service Unavailable` | PG error |

---

## GET /health

```json
{"status": "ok"}
```

---

## Metrics

Prometheus text format, with a separate endpoint per service (see
[infra/prometheus.yml](../../com/tm/infra/prometheus.yml)):

- **Server**: `GET http://localhost:9404/metrics`
- **Manager**: `GET http://localhost:9405/metrics`

In Prometheus the names replace `.`→`_`, counters get the `_total` suffix, and timers are `_seconds`
with `quantile="0.99"`; every metric is split by the `result` tag (the manager adds an `instance`
tag).

| Metric (Prometheus) | Service | Description |
|---------------------|---------|-------------|
| `booking_api_claim_total{result}` | server | Claim outcomes: ok / full / dup / closed / down / throttled / error / bad_request |
| `booking_api_claim_latency_seconds{result,quantile="0.99"}` | server | P99 latency of the claim endpoint |
| `booking_api_opportunity_total{result}` | server | Opportunity CRUD outcomes |
| `booking_dao_opportunity_total{result}` | server | Opportunity CRUD DAO |
| `booking_redis_warmup_total{result}` | server | Warmup: skipped / restored / error |
| `booking_consumer_handle_total{result,instance}` | manager | Settle outcomes: committed / duplicate / rejected / error / circuit_open |
| `booking_dao_commit_total{result}` | manager | DAO commit outcomes |
| `booking_dao_commit_latency_seconds{quantile="0.99"}` | manager | P99 latency of the PG settle round-trip |
| `booking_pg_settle_total{result}` / `booking_pg_settle_batch_total{result}` | manager | Per-driver / per-batch settle |
| `booking_e2e_notify_latency_seconds{result}` | manager | E2E latency: receive request → notify driver |

Full list + dashboards: [.claude/rules/observability-metrics.md](../../.claude/rules/observability-metrics.md)
and [infra/grafana/dashboards](../../com/tm/infra/grafana/dashboards).
