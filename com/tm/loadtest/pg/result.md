# Load test — Postgres (commit + backstop)

Scenario: manager consumes claim events and commits to Postgres, including
Kafka at-least-once redelivery.

| Metric | Value |
|--------|-------|
| Commit throughput (claims/s) | _TBD_ |
| Duplicate deliveries handled | _TBD (UNIQUE constraint)_ |
| Oversell after backstop | _expected 0_ |
| Commit p50 / p99 latency | _TBD_ |

Notes:
- Validates PG as source of truth + final backstop (atomic decrement + UNIQUE).
