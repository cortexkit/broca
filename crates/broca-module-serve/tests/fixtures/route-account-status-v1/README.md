# Account-status vectors

Source: prefrontal repository, commit `c38c2c395bfdd5024f08cb25b0d27cfe79f453c9`,
path `test-vectors/route-account-status-v1/`. `SOURCE_COMMIT` records the source
revision. Each JSON file contains a request and reply for `route.account_status`.
The consumer uses the reply's `warm_ok` value; it does not reconstruct the
producer's routing or reserve policy from the example windows.

To update, copy the JSON payloads from a source revision, update `SOURCE_COMMIT`
and the parent README's provenance, and run the account-status fixture tests.
Keep this README limited to fixture provenance and reproduction.
