---
paths:
  - "com/tm/common/**"
---

# Unit test bắt buộc cho com/tm/common

Mọi target trong `com/tm/common` PHẢI có unit test đi kèm. Bất kỳ `java_library` /
`go_library` nào dưới `com/tm/common/...` phải có một target test tương ứng trong cùng
package, cover logic của library đó. Không được thêm/sửa target trong `com/tm/common`
mà thiếu test.

- **Java**: thêm `<Name>Test.java` + một `java_test` trong cùng `BUILD.bazel`
  (JUnit4 `@maven//:junit_junit`, mock bằng `@maven//:org_mockito_mockito_core`; dùng
  test runner mặc định của Bazel).
- **Go**: thêm `<name>_test.go` + một `go_test` trong cùng `BUILD.bazel`.
- **Verify**: `bazel test //com/tm/common/...` phải xanh trước khi coi là xong.

Nếu một class cần I/O ngoài (Redis/PG/Kafka) thì tách logic thuần ra để test trực tiếp,
hoặc dùng mock / embedded; không được bỏ test vì "cần hạ tầng".
