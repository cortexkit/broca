# Step-transform routes

`SubcHookPlane::new(Arc<SubcConnection>)` implements `broca_core::HookPlane`.
It opens an unscoped module route with `step-transform-provider: v1` and the
connection's tool-module bind harness (`runner`). `transform.hook` uses the
commons `OpRequest<HookCall>` wire envelope, preserving the caller's harness,
session, lineage and flattened subject. Discovery uses commons `check_describe`
before any declaration or hook is sent.

Routes are cached by the complete caller `BindIdentity` and provider. Calls on
the same route serialize, including discovery and any retries; that wait is
inside the deadline. A refusal, transport/decode failure, timeout or cancellation
evicts the route, and the next call opens and discovers again. Failed known
handles are closed best-effort outside the hook's window; the connection handles
late responses to cancelled route opens.

`HookPlane` has no budget parameter. Core wraps the entire trait future with
`min(budget_ms, STEP_TRANSFORM_HOOK_CAP)`, including the cache wait, route-open
retry loop, discovery and at most one transient hook retry. The adapter also
bounds direct trait calls by the core cap, **30,000 ms**. The pinned commons
contract requires a positive budget and does not set a maximum; this is the
runner's own cap, not a contract maximum. Callers with an absolute deadline can
use `SubcHookPlane::hook_with_deadline`.

## Declarations

`SubcHookPlane::declare(provider, bind, &DeclareRequest, deadline)` sends
`transform.declare` on the same role route and discovery, with the caller's
shared `tokio::time::Instant` deadline. It returns the complete declaration
JSON and propagates `FetchError`, including refusal details and retryability.
The caller owns declaration retries; the hook's one-transient-retry policy
does not apply to declarations. Session retirement calls
`close_session_routes(&BindIdentity)`.

## Proofs

`tests/it/hook_routes.rs` uses real `ck-subc` and an in-process SDK provider.
Frozen vectors under `tests/fixtures/step-transform/` were copied byte-for-byte
from commons commit `50093e9b09b6adadd57f74994f9aacbaa5145622`; the role's vectors
are unchanged from `9abd24916a087162b7650ad7db71cf287be1330c`. Wire assertions
compare JSON values against those independent fixtures: object member order is
not pinned, and commons structs and JSON maps serialize in different orders.

The route's guards are the `subc-hook-*` rows in the root `mutations.toml`.
Replay them with the test prerequisite environment set (as `scripts/gate.sh`
does), so none of their tests can skip.
