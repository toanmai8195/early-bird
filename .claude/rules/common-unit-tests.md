---
paths:
  - "com/tm/common/**"
  - "com/tm/services/**/handler/**"
  - "com/tm/services/**/dao/**"
---

# Unit tests required

Accompanying unit tests are required for:
- Every target in `com/tm/common` (any `java_library` / `go_library`).
- Every `handler/` and `dao/` in each service `com/tm/services/<svc>/` — this is where the
  business logic lives (handling events/requests and accessing data), so tests are mandatory.

(Wiring layers like `config/`, `di/`, `Main`, and the verticle/poll-loop are not required to be tested.)

## Location & build
- Tests live in the target's own **`test/`** subfolder, **built separately** via
  a dedicated `BUILD.bazel` inside `test/` (don't share a BUILD with the library).
- Layout:
  ```
  com/tm/common/java/pg/
    ClaimStore.java
    BUILD.bazel          # java_library only
    test/
      ClaimStoreTest.java
      BUILD.bazel        # java_test only, deps on //com/tm/common/java/pg
  ```

## Convention
- **Java**: `test/<Name>Test.java` + `java_test` in `test/BUILD.bazel`
  (JUnit4 `@maven//:junit_junit`, mocking via `@maven//:org_mockito_mockito_core`).
- **Go**: `test/<name>_test.go` (package `<pkg>_test`, importing the library via its importpath)
  + `go_test` in `test/BUILD.bazel`.
- Same layout for services: `com/tm/services/<svc>/dao/test/...`,
  `com/tm/services/<svc>/handler/test/...`.
- **Verify**: `bazel test --build_tests_only //com/tm/common/... //com/tm/services/...`
  must be green before considering it done (the `--build_tests_only` flag avoids building the OCI
  image targets, for speed / to avoid needing network).

If a class needs external I/O (Redis/PG/Kafka), extract the pure logic to test it directly,
or use a mock / embedded instance; do not skip tests because they "need infrastructure".
