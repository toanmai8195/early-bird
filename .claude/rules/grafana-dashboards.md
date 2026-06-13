---
paths:
  - "com/tm/services/**"
  - "com/tm/common/java/**"
  - "com/tm/infra/grafana/**"
---

# A Grafana dashboard for every metric

Every metric created (counter / timer via `com.tm.common.metric.Metrics`) MUST have a
corresponding Grafana panel. Do not add/change a metric without a panel.

## Organization convention
- **Each service = 1 dashboard**: `com/tm/infra/grafana/dashboards/<service>.json`
  (e.g. `server.json`, `manager.json`).
- **Each common-lib "stack" with a metric = 1 dashboard**:
  `com/tm/infra/grafana/dashboards/common-<stack>.json` (e.g. `common-pg.json`,
  `common-redis.json`). Create one only if that stack actually emits metrics.
- Dashboards are stored as JSON models (Grafana export format), versioned in the repo.

## Each panel needs
- To live in the right dashboard based on where the metric is **emitted** (whichever service
  calls `Metrics` gets the panel in that service's dashboard; a metric emitted from common-lib
  code goes in the `common-<stack>` dashboard).
- A query matching the metric name + tags (e.g. `booking_claim_outcome_total{result=~"ok|full|dup"}`).
- **Counter** → a rate panel (e.g. `rate(<metric>_total[1m])`), split by the `result` tag.
- **Timer (latency)** → a panel showing **P99** (e.g.
  `histogram_quantile(0.99, ...)` or the series `..._seconds{quantile="0.99"}`), matching the
  P99 requirement in [observability-metrics.md](observability-metrics.md).

## When adding a new metric
1. Add the metric in code (per observability-metrics.md).
2. Add a panel to the corresponding dashboard JSON (create the dashboard if it doesn't exist).
3. Keep the panel title + metric name consistent with the `booking.<area>.<thing>` naming.
