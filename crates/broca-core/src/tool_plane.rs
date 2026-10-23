//! The tool-dispatch seam. The loop dispatches tool calls ONLY through this
//! trait, so the deterministic in-process stub and the real subc consumer
//! connection (runner → subc → AFT) are two impls of one contract, never a
//! rewrite.
//!
//! The trait deliberately models the REAL subc dispatch contract so the stub
//! can't diverge:
//!  - **batch lifecycle**: `batch_started{call_ids}` → per-call `dispatch` →
//!    `batch_finished` brackets one assistant turn's tool calls as an atomic unit.
//!  - **tool_call_id fence**: a repeated `tool_call_id` returns the PRIOR result
//!    and never re-executes — the at-most-once guarantee that lets resume safely
//!    re-dispatch a call whose result wasn't durably recorded.
//!  - **assembled result shape**: `ToolResult { tool_call_id, output, is_error }`.
//!
//! Orchestration the LOOP owns (not the plane): parallel-vs-sequential
//! (`execution_mode`), source ordering of results, the approval gate, and the
//! WAL durability barrier. The plane executes one call and fences it.

use async_trait::async_trait;
use broca_protocol::{BindIdentity, RunId, SessionScope};
use broca_protocol::{ToolCall, ToolCallId, ToolResult};

use crate::capability::ToolManifest;

use broca_protocol::BatchId;

/// Per-batch dispatch context. `bind` scopes routes/capabilities to the session.
#[derive(Debug, Clone)]
pub struct BatchCtx {
    pub batch_id: BatchId,
    pub bind: BindIdentity,
    /// The run this batch belongs to. When the daemon reports the run's scope as not live, the
    /// tool plane refuses the rest of this run's calls and remembers that under this id; a
    /// later run of the same session is checked afresh.
    pub run_id: RunId,
    /// The scope the run was admitted under, as its episode's `RunStarted` froze it; `None` for
    /// an unscoped run. A plane opens every provider route for this batch under exactly this
    /// scope, and never opens an unscoped route for a scoped run.
    pub scope: Option<SessionScope>,
}

/// A failure of the tool PLANE itself (transport/route), distinct from a tool
/// EXECUTION failure (which is a `ToolResult { is_error }`, conversation data).
/// The loop stays graceful on these — it synthesizes `is_error` results so the
/// model can adapt; it never hangs or crashes the run.
#[derive(Debug, Clone)]
pub enum ToolPlaneError {
    /// The plane could not reach the executor after its bounded transport retry
    /// (subc unreachable / route gone / timeout).
    Unavailable(String),
    /// A scoped run's route could not be opened for a reason to do with its scope, named by
    /// `code`: `scope_unsupported` (the daemon does not admit scoped routes),
    /// `scope_unverifiable` (the scope's state could not be confirmed), or a refusal the daemon
    /// never lifts for this scope, such as `scope_not_live` or `scope_ended`. Nothing was sent.
    ScopeRefused { code: String, detail: String },
    /// A route of this session closed while no call was in flight on it, and the daemon says
    /// (asked with `scope.describe`) that the scope or its parent is gone (`reason`: `scope_ended`, `scope_not_live` or
    /// `scope_parent_ended`). The loop gives every call of the batch a `route_drained` result
    /// naming `reason` and stops the run Interrupted. Nothing was sent.
    ScopeDrained { reason: String },
}

impl std::fmt::Display for ToolPlaneError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // This text reaches the MODEL as a synthesized tool result, and it is the only
            // account of the failure a caller gets. "tool plane unavailable" alone cost a
            // peer a two-module trace with error-byte capture to learn that their bind had
            // been REJECTED and by whom -- the message named a missing capability and hid
            // both the refusal and its source. Keep whatever the plane reports verbatim.
            ToolPlaneError::Unavailable(m) => {
                write!(
                    f,
                    "tool plane unavailable (route bind or transport failed): {m}"
                )
            }
            ToolPlaneError::ScopeRefused { code, detail } => write!(f, "{code}: {detail}"),
            ToolPlaneError::ScopeDrained { reason } => write!(
                f,
                "route_drained: the session's scope route was closed ({reason}); nothing was sent"
            ),
        }
    }
}

impl std::error::Error for ToolPlaneError {}

/// The outcome of dispatching ONE call. The durability barrier needs to know
/// whether a side effect could have occurred, so a plane failure is NOT a single
/// opaque error — it is classified by whether the request LEFT the process:
///  - `Completed`  — a definite assembled result (the tool ran; `is_error` may be
///    set by the tool itself).
///  - `NotSent`    — the request never left the process (connect failed / route
///    gone / refused before send). SAFE: no side effect occurred → the loop makes a
///    normal `is_error` result the model can retry.
///  - `SentButOutcomeUnknown` — the request WAS sent but no result came back
///    (timeout / connection reset / executor crash AFTER send). The side effect
///    MAY have applied → AMBIGUOUS → the run stops for review rather than feeding
///    the unknown back to the model. NEVER a normal "unavailable" error (which the
///    model would retry = a DUPLICATE side effect) and NEVER an auto-retry.
#[derive(Debug, Clone)]
pub enum DispatchResult {
    Completed(ToolResult),
    NotSent {
        reason: String,
    },
    SentButOutcomeUnknown {
        reason: String,
    },
    /// The route the call was to use, opened under the run's scope, closed while the call was in
    /// flight. `reason` names what the daemon said about the scope when asked
    /// (`scope.describe`): the scope or its parent is gone, the scope is now bound to a different
    /// agent so a fresh route would carry another identity (`scope_agent_changed`), or the
    /// scope's state could not be confirmed (`scope_unverifiable`). Whether the request had
    /// left decides the outcome, not the reason:
    ///  - `sent: false` — the request never left: the call gets `route_drained` naming the
    ///    reason and is never re-dispatched, not even on a fresh route;
    ///  - `sent: true` — the request left and no result came back: the outcome is unknown and
    ///    the run stops Interrupted, never Cancelled.
    ScopeDrained {
        reason: String,
        sent: bool,
    },
}

/// Run signals carried to an approved adapter's final physical-send boundary.
/// Clones observe the same tokens while route and flow-control waits are pending.
#[derive(Clone, Default)]
pub struct DispatchSignals {
    cancel: Option<tokio_util::sync::CancellationToken>,
    stop: Option<tokio_util::sync::CancellationToken>,
}
impl DispatchSignals {
    pub fn new(
        cancel: Option<&tokio_util::sync::CancellationToken>,
        stop: Option<&tokio_util::sync::CancellationToken>,
    ) -> Self {
        Self {
            cancel: cancel.cloned(),
            stop: stop.cloned(),
        }
    }
    pub fn interrupted(&self) -> bool {
        self.cancel
            .as_ref()
            .is_some_and(tokio_util::sync::CancellationToken::is_cancelled)
            || self
                .stop
                .as_ref()
                .is_some_and(tokio_util::sync::CancellationToken::is_cancelled)
    }
    pub async fn cancelled(&self) {
        async fn wait(token: Option<&tokio_util::sync::CancellationToken>) {
            match token {
                Some(token) => token.cancelled().await,
                None => std::future::pending().await,
            }
        }
        tokio::select! {
            _ = wait(self.cancel.as_ref()) => {}
            _ = wait(self.stop.as_ref()) => {}
        }
    }
}

/// The tool-dispatch contract. One batch = one assistant turn's tool calls.
#[async_trait]
pub trait ToolPlane: Send + Sync {
    /// Prepare routing state for this batch before dispatch starts. `calls` is
    /// the full set in this batch, and `manifest` is the immutable tool manifest captured when
    /// the run was admitted. On `Err` the loop synthesizes `is_error` results for every
    /// call so the batch fails gracefully.
    async fn batch_started(
        &self,
        ctx: &BatchCtx,
        calls: &[ToolCall],
        manifest: &ToolManifest,
    ) -> Result<(), ToolPlaneError>;

    /// Dispatch ONE tool call and return its outcome. FENCES by `tool_call_id`: a
    /// repeated id returns the prior result and does NOT re-execute (at-most-once).
    /// Tool EXECUTION failures come back as `Completed(ToolResult { is_error })`
    /// (conversation data); a PLANE failure is classified `NotSent` (safe — never
    /// left) vs `SentButOutcomeUnknown`
    /// (ambiguous — side effect may have applied) so the loop can treat each case
    /// correctly (retry the safe one, stop the run on the ambiguous one).
    async fn dispatch(&self, ctx: &BatchCtx, call: &ToolCall) -> DispatchResult;

    /// Dispatch approved arguments without parsing and reserializing them. Adapters must
    /// preserve `args_jcs` verbatim and forward the deferred call's `call_key`. Core verifies
    /// that the answer binds to these arguments; consent issues no tool token. The approval
    /// digest remains durable history, not provider request data.
    /// Check `signals` after all pre-send waits, immediately before the physical
    /// request. An interrupted unsent request returns `NotSent`, never Unknown.
    /// Until the daemon installs an approval-capable adapter, approval fails closed.
    async fn dispatch_approved(
        &self,
        _ctx: &BatchCtx,
        _call: &ToolCall,
        _args_jcs: &str,
        _call_key: &str,
        _signals: &DispatchSignals,
    ) -> DispatchResult {
        DispatchResult::NotSent {
            reason: "approved_dispatch_unavailable".into(),
        }
    }

    /// Open a fresh route under the session scope saved in ToolCallDeferred before
    /// executing an approved call. A cached route cannot prove the scope is still live.
    async fn recheck_approval_scope(
        &self,
        _ctx: &BatchCtx,
        _call: &ToolCall,
        _module: Option<&str>,
    ) -> Result<(), ToolPlaneError> {
        Err(ToolPlaneError::Unavailable(
            "approval_scope_recheck_unavailable".into(),
        ))
    }

    /// The tool provider module the call `tool_call_id` will be sent to, as resolved by
    /// [`batch_started`](Self::batch_started), or `None` when the plane routes calls to no
    /// module (an in-process plane) or the call resolves to no module and will fail without
    /// being sent. The loop records it on the call's dispatch intent, which `session.read`
    /// reports as the call's `dispatched_to`. Asked after `batch_started` and before the intent
    /// is written, so it must answer from state `batch_started` already settled.
    fn dispatch_target(&self, _ctx: &BatchCtx, _tool_call_id: &ToolCallId) -> Option<String> {
        None
    }

    /// Close a batch — modelling `tool_batch_finished{batch_id}`.
    async fn batch_finished(&self, ctx: &BatchCtx) -> Result<(), ToolPlaneError>;
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalCursor {
    pub provider_incarnation: String,
    pub seq: u64,
}

#[derive(Debug, Clone)]
pub struct ApprovalPage {
    pub records: Vec<serde_json::Value>,
    pub cursor: ApprovalCursor,
}

#[derive(Debug, Clone)]
pub struct ApprovalFiling {
    pub elicitation_id: String,
    pub args_hash: String,
}

#[derive(Debug, Clone)]
pub struct ApprovalRouteError {
    pub code: String,
    pub retryable: bool,
}

#[derive(Debug, Clone)]
pub struct ApprovalQuestion {
    pub terminal: bool,
    pub record: serde_json::Value,
}

/// An error shown to the model may be synthesized before any provider request, so
/// execution feedback must distinguish actual execution from the displayed result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalExecution {
    Executed,
    ApprovedNotExecuted,
    OutcomeUnknown,
}

#[derive(Debug, Clone)]
pub enum ApprovalWithdrawal {
    Acknowledged,
    NotSent { reason: String },
    Retry(ApprovalRouteError),
}

/// Scoped requester operations. Core owns retries, deadlines and durable cursor intake;
/// adapters own route opens and wire translation. The daemon must explicitly install
/// this plane; an absent adapter never authorizes tool execution.
#[async_trait]
pub trait ElicitationPlane: Send + Sync {
    async fn request(
        &self,
        _ctx: &BatchCtx,
        _call_key: &str,
        _prepared: &broca_wal::WalRecord,
        _now_ms: u64,
    ) -> Result<ApprovalFiling, ApprovalRouteError> {
        Err(ApprovalRouteError {
            code: "elicitation_unavailable".into(),
            retryable: false,
        })
    }
    async fn answers(
        &self,
        _ctx: &BatchCtx,
        _since: Option<&ApprovalCursor>,
    ) -> Result<ApprovalPage, ApprovalRouteError> {
        Err(ApprovalRouteError {
            code: "elicitation_unavailable".into(),
            retryable: false,
        })
    }
    async fn get(
        &self,
        _ctx: &BatchCtx,
        _elicitation_id: &str,
    ) -> Result<ApprovalQuestion, ApprovalRouteError> {
        Err(ApprovalRouteError {
            code: "elicitation_unavailable".into(),
            retryable: false,
        })
    }
    /// Wait on one card whose id has been synced to the WAL. Dropping this future
    /// must close the transport route carrying the wait request.
    async fn r#await(
        &self,
        _ctx: &BatchCtx,
        _elicitation_id: &str,
        _wait_ms: u64,
    ) -> Result<ApprovalQuestion, ApprovalRouteError> {
        Err(ApprovalRouteError {
            code: "elicitation_unavailable".into(),
            retryable: false,
        })
    }
    async fn ack(
        &self,
        _ctx: &BatchCtx,
        _cursor: &ApprovalCursor,
    ) -> Result<(), ApprovalRouteError> {
        Err(ApprovalRouteError {
            code: "elicitation_unavailable".into(),
            retryable: false,
        })
    }
    async fn report_execution(
        &self,
        _ctx: &BatchCtx,
        _elicitation_id: &str,
        _execution: ApprovalExecution,
    ) -> Result<(), ApprovalRouteError> {
        Err(ApprovalRouteError {
            code: "elicitation_unavailable".into(),
            retryable: false,
        })
    }
    /// Release a locally closed call whose card can still be visible; never authorizes it.
    async fn detach(
        &self,
        _ctx: &BatchCtx,
        _elicitation_id: &str,
    ) -> Result<ApprovalQuestion, ApprovalRouteError> {
        Err(ApprovalRouteError {
            code: "elicitation_unavailable".into(),
            retryable: false,
        })
    }
    async fn withdraw(&self, _ctx: &BatchCtx, _elicitation_id: &str) -> ApprovalWithdrawal {
        ApprovalWithdrawal::Retry(ApprovalRouteError {
            code: "elicitation_unavailable".into(),
            retryable: true,
        })
    }
}

// Adapter crates can name the durable filing payload without an additional WAL dependency.
pub use broca_wal::{LateExecution, WalRecord};
