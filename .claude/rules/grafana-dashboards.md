---
paths:
  - "com/tm/services/**"
  - "com/tm/common/java/**"
  - "com/tm/infra/grafana/**"
---

# Grafana dashboard cho mỗi metric

Mọi metric tạo ra (counter / timer qua `com.tm.common.metric.Metrics`) PHẢI có một
Grafana panel tương ứng. Không thêm/sửa metric mà thiếu panel.

## Quy ước tổ chức
- **Mỗi service = 1 dashboard**: `com/tm/infra/grafana/dashboards/<service>.json`
  (vd `server.json`, `manager.json`).
- **Mỗi "stack" trong common-lib có metric = 1 dashboard**:
  `com/tm/infra/grafana/dashboards/common-<stack>.json` (vd `common-pg.json`,
  `common-redis.json`). Chỉ tạo nếu stack đó thực sự phát metric.
- Dashboard lưu dưới dạng JSON model (Grafana export format), versioned trong repo.

## Mỗi panel cần
- Đặt đúng dashboard theo nơi metric được **emit** (service nào gọi `Metrics` thì panel
  nằm ở dashboard service đó; metric emit từ trong code common-lib thì nằm ở dashboard
  `common-<stack>`).
- Query khớp tên metric + tag (vd `booking_claim_outcome_total{result=~"ok|full|dup"}`).
- **Counter** → panel rate (vd `rate(<metric>_total[1m])`), tách theo tag `result`.
- **Timer (latency)** → panel hiển thị **P99** (vd
  `histogram_quantile(0.99, ...)` hoặc series `..._seconds{quantile="0.99"}`), khớp với
  yêu cầu P99 ở [observability-metrics.md](observability-metrics.md).

## Khi thêm metric mới
1. Thêm metric trong code (theo observability-metrics.md).
2. Thêm panel vào dashboard JSON tương ứng (tạo dashboard nếu chưa có).
3. Panel title + metric name nhất quán với naming `booking.<area>.<thing>`.
