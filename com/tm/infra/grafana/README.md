# grafana

Dashboards versioned cùng code (xem .claude/rules/grafana-dashboards.md).

- `dashboards/<service>.json` — 1 dashboard / service.
- `dashboards/common-<stack>.json` — chỉ tạo khi code trong common-lib stack đó tự
  emit metric có tên.

## Quy ước metric → panel (Micrometer + Prometheus)
- Counter `booking.dao.commit` → `booking_dao_commit_total{result=...}` → panel
  `rate(...[1m])` tách theo `result`.
- Timer `booking.dao.commit.latency` (publishPercentiles(0.99)) →
  `booking_dao_commit_latency_seconds{quantile="0.99"}` → panel P99.

## Dashboards hiện có
- `dashboards/manager.json` — DAO commit rate, DAO commit latency P99, consumer handle rate.

## Mỗi khi thêm metric mới
Thêm panel vào dashboard service tương ứng (hoặc tạo `common-<stack>.json` nếu metric
emit từ common-lib). Giữ tên panel khớp tên metric.
