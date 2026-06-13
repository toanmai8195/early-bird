---
paths:
  - "com/tm/common/**"
  - "com/tm/services/**/handler/**"
  - "com/tm/services/**/dao/**"
---

# Interface required for dao / handler / common

Every `dao`, `handler`, and every library **with behavior** in `com/tm/common` MUST be exposed via
an **interface**; don't let other code depend directly on an implementation class.

**Exemption:** pure data/value types (records, DTOs, enums — e.g. `ClaimEvent`, `result.Outcome`,
`result.Tally`) do NOT need an interface.

## Convention
- Define an interface (e.g. `BookingDao`, `ClaimHandler`, `IClaimStore`) + one implementation
  class (e.g. `JdbcBookingDao`, `ClaimHandlerImpl`, `PgClaimStore`).
- Other components'/handlers' dependencies reference only the **interface**, never the
  implementation class.
- **Dagger**: bind interface → impl with `@Binds` (preferred, more concise than `@Provides`):
  ```java
  @Module
  interface ManagerBindings {
      @Binds @Singleton BookingDao bookingDao(JdbcBookingDao impl);
      @Binds @Singleton ClaimHandler claimHandler(ClaimHandlerImpl impl);
  }
  ```
  The impl keeps the `@Inject` constructor; the interface needs no annotation.
- **Common**: the target exports the interface publicly; the impl can live in the same
  `java_library` or a separate target depending on how reusable it is.

## Rationale
- Tests mock against the interface, not the impl (matching the unit-test rule).
- Allows swapping the impl (e.g. in-memory for tests, JDBC for prod) without changing call sites.

Do not add/change a dao, handler, or common stack without a corresponding interface.
