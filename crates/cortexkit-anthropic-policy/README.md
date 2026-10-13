# cortexkit-anthropic-policy

Policy without Cargo feature flags or transport code, shared by Broca and Anthropic gateways. The
crate uses only std, serde_json and httpdate; it does not depend on an async
runtime, HTTP client, Broca renderer or Broca protocol types. It is not published
yet; consumers should pin a repository revision.

- `header_policy(AuthKind, Features)` returns credential-header placement,
  OAuth's `x-app: cli`, and ordered beta additions. Interleaved thinking, binding
  controls and fast mode are explicit features, independent of auth kind.
- `merge_beta_headers` forms a stable first-seen union across any number of beta
  lines; `remove_beta_token` removes only an exact token. Both refuse non-ASCII
  and invalid HTTP field values, including invalid bytes on a line that would
  otherwise be trimmed or removed. Empty tokens are ignored; matching is
  case-sensitive. The resulting field uses commas without added spaces.
- `classify_account_limit_exhausted(status, headers, body, now)` returns an
  account wall only for HTTP 429 with unified status `rejected`. Reset preference
  is the unified epoch-seconds header, then Retry-After; unknown resets remain
  unknown. The `_with_retry_after` entry point accepts a previously normalized
  hint so a transport can retain its original observation time. Independently
  classified service-tier refusals must be excluded by the caller before this
  rule: waiting for a reset cannot fix a tier refusal.
- `anthropic_credit_balance_too_low` tests the out-of-credit message phrase;
  `credit_balance_exhausted` additionally checks the body's invalid-request
  discriminator.
- `collect_unified_quota_headers` keeps every `anthropic-ratelimit-unified-*`
  name (including future extensions), lowercases names and retains the first
  value per name, without parsing or rounding it. Values use lossy UTF-8 only
  where necessary, matching Broca's existing transport behavior.

The crate never renders a body or supplies a user-agent/system identity. A native
Claude Code proxy retains its client's identity and unrelated betas, merging
OAuth additions or removing only `OAUTH_BETA` on the API-key path. It must remove
all incoming credential headers before attaching its single owned credential.
Broca separately maps its frozen generated-body identity to these policy inputs,
preserving existing wire headers even when using a token-free render test.

Run `cargo test -p cortexkit-anthropic-policy` and
`python3 scripts/test-anthropic-policy-deps.py`. CI runs the latter against the
actual Cargo tree as well as manifest edges to guard the dependency boundary.
