# Vendored account-status vectors

`route-account-status-v1/` is a byte-for-byte copy of `test-vectors/route-account-status-v1/`
from the prefrontal repository at commit `c38c2c395bfdd5024f08cb25b0d27cfe79f453c9`
at the last commit touching that directory. `SOURCE_COMMIT` records provenance.

To update, replace the payload files from a source commit and update this provenance.
Keep the local README focused on fixture format and reproduction, not routing policy.

## Compaction joint transcript

`compaction-joint/` is a byte-identical copy from the `cortexkit/commons`
repository (https://github.com/cortexkit/commons), commit
`afe80c7ef4f16730be43d998f2ea708a481c411c`, directory
`test-vectors/compaction-provider-v1/joint/`. `SHA256SUMS` pins its three
payload files; the fixture test verifies each digest and all seven case names.
The magic-context client test uses a writable scratch copy of a schema-v97 database
and a development binary, never a user store.
Its fixture's symbolic compaction id is normalized only for comparison with magic-context's
session-derived id; every replacement byte, range, version and request id stays
literal. The copied upstream README uses “Setup” for the provider's per-session
compaction initialization and “initial view” for the history-head messages it
returns before the first model request; the payload files remain unmodified.
