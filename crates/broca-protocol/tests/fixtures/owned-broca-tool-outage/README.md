# Tool-outage vector provenance

These files are byte-for-byte copies of
`test-vectors/owned-broca-tool-outage/` in `cortexkit-role-llm-runner` 0.7.9
(cortexkit/commons commit `9dee1de1a88599291c82be037169c890630c926f`). The
crate published them from broca's own producer captures, and keeps them in
its package without exposing them in its API, so broca carries this copy.

| File | SHA-256 |
|---|---|
| `valid-error-outputs.json` | `afa820c9b5c574dd1564b60c9851f09b0ea5d0ac93913c76c03379da33fe2f46` |
| `session_history.display.row.json` | `065bfb2773e4d779e0ee41cf41166fb6592da6963a9bb179e70fbc041cf392e6` |
| `session_history.display.plain-error.row.json` | `69cfbfabd98c8690b9b0814f45b6441892bf682798c4d09e650712c96ca41402` |
| `tool_result.record.json` | `fecf64a5acb977fe0349a5b36846bb0fae631b4b50210e51242ca72d13dd41c3` |

`tests/owned_broca_tool_outage.rs` checks that broca still builds, encodes and
renders these outputs exactly as captured. Copy newer files only from a
published crate version, and update the hashes here when you do. Never
replace them with values broca computes.
