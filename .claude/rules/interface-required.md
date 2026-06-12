---
paths:
  - "com/tm/common/**"
  - "com/tm/services/**/handler/**"
  - "com/tm/services/**/dao/**"
---

# Interface bắt buộc cho dao / handler / common

Mọi `dao`, `handler`, và mọi thư viện **có hành vi** trong `com/tm/common` PHẢI expose qua
**interface**, không để code khác phụ thuộc trực tiếp vào class implementation.

**Miễn trừ:** pure data/value types (record, DTO, enum — vd `ClaimEvent`, `result.Outcome`,
`result.Tally`) KHÔNG cần interface.

## Quy ước
- Định nghĩa interface (vd `BookingDao`, `ClaimHandler`, `IClaimStore`) + một class
  implementation (vd `JdbcBookingDao`, `ClaimHandlerImpl`, `PgClaimStore`).
- Dependency của component/handler khác chỉ tham chiếu **interface**, không tham chiếu
  class impl.
- **Dagger**: bind interface → impl bằng `@Binds` (ưu tiên, gọn hơn `@Provides`):
  ```java
  @Module
  interface ManagerBindings {
      @Binds @Singleton BookingDao bookingDao(JdbcBookingDao impl);
      @Binds @Singleton ClaimHandler claimHandler(ClaimHandlerImpl impl);
  }
  ```
  Impl giữ `@Inject` constructor; interface không cần annotation.
- **Common**: target export interface ở public; impl có thể cùng `java_library` hoặc
  tách target tùy mức độ tái sử dụng.

## Lý do
- Test mock theo interface, không phụ thuộc impl (khớp rule unit-test).
- Cho phép thay impl (vd in-memory cho test, JDBC cho prod) mà không đổi call site.

Không thêm/sửa dao, handler, hay common stack mà thiếu interface tương ứng.
