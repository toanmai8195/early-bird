---
paths:
  - "com/tm/services/**"
  - "com/tm/common/java/kafka/**"
  - "com/tm/common/java/pg/**"
---

# Metric bắt buộc (counter + latency)

Dùng `com.tm.common.metric` (Micrometer) cho mọi instrumentation. Đơn vị đo:

- **API endpoint** (HTTP handler trong `services/server`): mỗi endpoint PHẢI có
  - counter (đếm số request, tách theo outcome: ok / full / dup / error), VÀ
  - latency timer publish percentile **P99** (`Timer` với
    `.publishPercentiles(0.99)` hoặc `.percentilePrecision(...)`).
- **Kafka consumer** (`services/manager`): mỗi consumer/handler PHẢI có counter
  (số message xử lý, tách theo success / error).
- **Kafka producer** (`common/java/kafka`, call site trong services): mỗi đường
  publish PHẢI có counter (số message publish, tách theo success / error).
- **DAO function** (mọi method truy cập DB trong `common/java/pg`, vd `ClaimStore`):
  mỗi func PHẢI có
  - counter (số lần gọi, tách theo outcome), VÀ
  - latency timer publish **P99**.

Quy ước:
- Không thêm/sửa API, consumer, producer, hay DAO func mà thiếu metric tương ứng.
- Counter tách theo `result`/`outcome` tag để phân biệt success vs các loại lỗi.
- Latency đo bằng `Timer` (không tự tính), bật percentile P99 để alert/SLO dùng được.
- Đặt tên metric nhất quán: `booking.<area>.<thing>` (vd `booking.claim.outcome`,
  `booking.dao.commit.latency`).
