# Step-transform routes

`SubcHookPlane::new(Arc<SubcConnection>)` implements `broca_core::HookPlane`.
It opens a session-bound module route with `step-transform-provider: v2` and the
session's actual harness. `transform.hook` uses the commons `OpRequest<HookCall>`
wire envelope; the route supplies session identity, so bodies carry no harness,
session or lineage. Discovery uses commons `check_describe`
before any declaration or hook is sent.

Scoped sessions open under the same frozen `(owner, ref, epoch)` selector as tool
routes, including admission's declaration and every hook phase. Unscoped sessions
omit the selector as before. A scoped-open refusal is returned without falling
back to an unscoped route. Compaction uses the same scope selection and role-route
machinery for discovery, Setup, `view.get` and ride, but keeps the session's real
harness rather than translating it to `runner`.

Daemon refusals of scoped role opens are counted by code in
`health.check.metrics.scoped_role_route_refusals`. The routing WARN names the
session identity, provider, role and refusal code, at most once per minute per
`(provider, code)`. Counts include suppressed warnings. Setup and hook failure
classification is unchanged: these diagnostics do not turn a failed call into
a successful unscoped retry or change the run's terminal error.

Routes are cached by the complete caller `BindIdentity`, provider and scope selector
(including owner and frozen epoch). Calls on
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

`SubcHookPlane::declare(provider, bind, scope, &DeclareRequest, deadline)` sends
`transform.declare` on the same role route and discovery, with the caller's
shared `tokio::time::Instant` deadline. It returns the complete declaration
JSON and propagates `FetchError`, including refusal details and retryability.
The caller owns declaration retries; the hook's one-transient-retry policy
does not apply to declarations. Session retirement calls
`close_session_routes(&BindIdentity)`.

## Proofs

`tests/it/hook_routes.rs` uses real `ck-subc` and an in-process SDK provider.
Frozen vectors under `tests/fixtures/step-transform/` were copied byte-for-byte
from commons commit `498a5edfcc8563fef121d5c9727807c95d6bf24c`, step-transform-provider/v2. Wire assertions
compare JSON values against those independent fixtures: object member order is
not pinned, and commons structs and JSON maps serialize in different orders.
