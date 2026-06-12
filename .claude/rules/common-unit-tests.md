---
paths:
  - "com/tm/common/**"
  - "com/tm/services/**/handler/**"
  - "com/tm/services/**/dao/**"
---

# Unit test bắt buộc

Phải có unit test đi kèm cho:
- Mọi target trong `com/tm/common` (bất kỳ `java_library` / `go_library` nào).
- Mọi `handler/` và `dao/` trong từng service `com/tm/services/<svc>/` — đây là nơi
  chứa business logic (xử lý event/request và truy cập dữ liệu) nên bắt buộc test.

(Các tầng wiring như `config/`, `di/`, `Main`, verticle/poll-loop không bắt buộc test.)

## Vị trí & build
- Test đặt trong thư mục con **`test/`** của chính target đó, **build riêng** bằng
  một `BUILD.bazel` riêng trong `test/` (không để chung BUILD với library).
- Layout:
  ```
  com/tm/common/java/pg/
    ClaimStore.java
    BUILD.bazel          # chỉ java_library
    test/
      ClaimStoreTest.java
      BUILD.bazel        # chỉ java_test, deps tới //com/tm/common/java/pg
  ```

## Quy ước
- **Java**: `test/<Name>Test.java` + `java_test` trong `test/BUILD.bazel`
  (JUnit4 `@maven//:junit_junit`, mock bằng `@maven//:org_mockito_mockito_core`).
- **Go**: `test/<name>_test.go` (package `<pkg>_test`, import library qua importpath)
  + `go_test` trong `test/BUILD.bazel`.
- Cùng layout cho service: `com/tm/services/<svc>/dao/test/...`,
  `com/tm/services/<svc>/handler/test/...`.
- **Verify**: `bazel test --build_tests_only //com/tm/common/... //com/tm/services/...`
  phải xanh trước khi coi là xong (cờ `--build_tests_only` để khỏi build kèm OCI image
  target cho nhanh / khỏi cần network).

Nếu một class cần I/O ngoài (Redis/PG/Kafka) thì tách logic thuần ra để test trực tiếp,
hoặc dùng mock / embedded; không được bỏ test vì "cần hạ tầng".
