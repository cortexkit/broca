# Session-ref vector provenance

`session-ref.json` is a byte-for-byte copy of
`test-vectors/llm-runner-v1/session-ref.json` from cortexkit/commons commit
`8ebc49271012537fe976bbe979067dea8bea13b0` (the `session_ref` contract shipped
in `cortexkit-role-llm-runner` 0.2.1).

SHA-256: `4bc23abb3e5e82db7f02472244c196e97a38a4bea5c37c25e737f4601b32af0c`.

`session_ref_fixture_bytes_are_pinned` pins these original bytes independently
of the dependency's bundled vectors. `session_reference_matches_shared_vectors`
checks broca's ref mapping against all four cases, including the distinction
between paths with and without a trailing slash and the escaped/non-ASCII case.
Do not normalise paths or replace this fixture with newly calculated values.
