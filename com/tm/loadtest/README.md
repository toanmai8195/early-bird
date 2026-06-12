# loadtest

Load test scenarios and results for the booking system. Each subdirectory holds
the scenario config and recorded results (latency, throughput, oversell check).

- `redis-counter/` — gate-only test: 10K concurrent drivers vs one opportunity
  (capacity 1K). Validates fast-reject ratio (~90% FULL), p99 latency, and that
  exactly `capacity` claims return OK (no oversell at the gate).
- `pg/` — end-to-end / backstop test: throughput of the manager committing claims
  to Postgres, and the oversell guarantee under Kafka at-least-once redelivery.

Drive load with the client:
```bash
bazel run //com/tm/services/client/cmd:client -- \
  --target=http://localhost:8080 --opportunity=opp-1 --drivers=10000
```

Record results as `result.md` / CSV / charts inside each scenario directory.
