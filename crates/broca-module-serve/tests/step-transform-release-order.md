# Step-transform release-order checks

The staged compatibility reader is `ck-broca 0.3.191`: it knows the `step-transforms/v1` WAL feature
and optional record fields, but installs no production hook plane and advertises no ordered phases.
Its admission guard refuses both submitted and frozen hook plans before actor opening. The rollback
check consumes this staged binary instead of checking out or building an old tag from this repository.

The test needs a separately staged reader binary, not a renamed writer; its command is:

```sh
CARGO_BUILD_JOBS=2 BROCA_REQUIRE_SUBC=1 BROCA_REQUIRE_STEP_READER=1 \
  BROCA_STEP_TRANSFORM_READER_BIN="/path/to/staged-reader-v0.3.191/ck-broca" \
  cargo test -p broca-module-serve --test llm_runner_conformance \
  reader_release_reads_writer_records_and_refuses_hooked_advancement
```

Provide `SUBC_CORE_BIN` and `FAKE_AFT_STUB_BIN` as paths to built daemon and tool-provider test binaries. Ordinary runs without a reader artifact print an explicit `SKIP`; release verification sets `BROCA_REQUIRE_STEP_READER=1` to make omission a failure. The test writes all seven hooked record types using the production writer over real ck-subc routes, then reads that WAL through the separate reader. It also attempts sends/resume against complete, in-flight, and fresh sessions, with and without a submitted plan, checking exact WAL bytes and zero hook/declaration/tool calls.

`step_transform_crash` cuts every record boundary of a hooked tool run, including the four named durability points. It verifies the per-cut hook table, physical WAL prefix and canonical rendered prefix against the uncut live provider request, not another replay of the same WAL. A child runner is also forcibly killed after post_tool answers but before the result append. The child test is only a process entry point; the parent test asserts that it actually reached the cut and was killed. Separate cases cover an answered pre_user lost before its send record, replayed Already results, and sequential post_tool-before-next-dispatch ordering.

`it::step_transforms` uses a real ck-subc daemon and SDK-served scripted transform provider with the actor/core serve rig. `production_executor_installs_hooks_and_describes_ordered_phases` independently checks the real ck-broca executor so installing hooks only in a test executor cannot satisfy the wiring claim.

The current input check is membership plus input byte size, not JSON Schema validation. The invalid-mutation test deliberately uses an oversized input and does not establish schema-invalid inputs as a supported contract. Schema validation is not established by this test.

The describe golden can be regenerated from the production serve path, preserving JSON member order and normalizing only `implementation_version`:

```sh
CARGO_BUILD_JOBS=2 BROCA_REQUIRE_SUBC=1 BROCA_REGENERATE_ROLE_DESCRIBE_GOLDEN=1 \
  cargo test --locked -p broca-module-serve --test llm_runner_conformance \
  production_executor_installs_hooks_and_describes_ordered_phases -- --exact
shasum -a 256 crates/broca-wire/tests/golden/role_describe.json
```
