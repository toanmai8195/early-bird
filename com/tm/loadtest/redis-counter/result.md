# Load test — Redis counter (claim gate)

Scenario: 10,000 concurrent drivers claim opportunity `opp-1` (capacity 1,000).

| Metric | Value |
|--------|-------|
| Total requests | _TBD_ |
| OK (slot acquired) | _expected 1,000_ |
| FULL (fast-rejected) | _expected ~9,000_ |
| DUP (idempotent retry) | _TBD_ |
| Oversell | _expected 0_ |
| p50 / p99 latency | _TBD_ |
| Throughput (req/s) | _TBD_ |

Notes:
- Validates atomic single-key Lua gate under contention on one hot opportunity.
