# Frozen compaction-provider vectors

`setup.json`, `answers.json`, and `status.json` are verbatim copies from
`cortexkit/commons` revision `9abd24916a087162b7650ad7db71cf287be1330c`,
`test-vectors/compaction-provider-v1/`. The reader tests decode the fields that
the WAL stores into broca-owned types; protocol behavior remains owned by the
shared role crate, not these storage mirrors.
