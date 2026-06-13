---
paths:
  - "com/tm/services/**"
  - "com/tm/common/java/kafka/**"
  - "com/tm/common/java/pg/**"
---

# Required metrics (counter + latency)

Use `com.tm.common.metric` (Micrometer) for all instrumentation. Units of measurement:

- **API endpoint** (HTTP handler in `services/server`): each endpoint MUST have
  - a counter (counting requests, split by outcome: ok / full / dup / error), AND
  - a latency timer publishing the **P99** percentile (`Timer` with
    `.publishPercentiles(0.99)` or `.percentilePrecision(...)`).
- **Kafka consumer** (`services/manager`): each consumer/handler MUST have a counter
  (messages processed, split by success / error).
- **Kafka producer** (`common/java/kafka`, call site in services): each publish
  path MUST have a counter (messages published, split by success / error).
- **DAO function** (every DB-accessing method in `common/java/pg`, e.g. `ClaimStore`):
  each function MUST have
  - a counter (number of calls, split by outcome), AND
  - a latency timer publishing **P99**.

Conventions:
- Do not add/change an API, consumer, producer, or DAO function without a corresponding metric.
- Split counters by a `result`/`outcome` tag to distinguish success from the various error types.
- Measure latency with a `Timer` (don't compute it yourself), enabling the P99 percentile so it's usable for alerting/SLO.
- Name metrics consistently: `booking.<area>.<thing>` (e.g. `booking.claim.outcome`,
  `booking.dao.commit.latency`).
