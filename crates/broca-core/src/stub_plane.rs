//! An in-process [`ToolPlane`] stub — the FIRST impl, used to prove the loop
//! deterministically offline. It models the real subc dispatch contract (fence,
//! batch lifecycle, result shape) so the real subc impl drops in beside it.
//!
//! It records a lifecycle log + per-id execution count so tests can assert batch
//! bracketing, source ordering, and that the `tool_call_id` fence prevents
//! double-execution.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use broca_protocol::{ToolCall, ToolCallId, ToolOutput, ToolResult};

use crate::tool_plane::{BatchCtx, DispatchResult, ToolPlane, ToolPlaneError};
use crate::ToolManifest;

/// What a stub tool handler returns for a call.
pub enum StubReply {
    /// A successful tool result.
    Output(ToolOutput),
    /// A tool EXECUTION error → `Completed(ToolResult { is_error: true })`.
    ExecError(String),
    /// A plane failure where the request NEVER left → `NotSent` (safe).
    PlaneUnavailable(String),
    /// A plane failure where the request WAS sent but no result came back →
    /// `SentButOutcomeUnknown` (the request left but no result came back, so the
    /// side effect may or may not have applied).
    OutcomeUnknown(String),
    /// The call's scoped route closed while the call was in flight, so `dispatch` answers
    /// `ScopeDrained`. With `sent: false` the request never left (`route_drained`); with
    /// `sent: true` it left and its outcome is unknown.
    ScopeDrained { reason: String, sent: bool },
}

type Handler = Box<dyn Fn(&ToolCall) -> StubReply + Send + Sync>;

/// Recorded lifecycle events, in call order — for test assertions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StubEvent {
    BatchStarted {
        call_ids: Vec<ToolCallId>,
    },
    /// A real execution happened (fence MISS).
    Dispatched(ToolCallId),
    /// A repeated id was served from the fence cache (no re-execution).
    Fenced(ToolCallId),
    BatchFinished,
}

#[derive(Default)]
struct State {
    fence: HashMap<ToolCallId, ToolResult>,
    exec_count: HashMap<ToolCallId, u32>,
    log: Vec<StubEvent>,
    /// The error the next `batch_started` call returns instead of `Ok`; used once.
    batch_start_error: Option<ToolPlaneError>,
    /// The scope every batch context carried, in batch order.
    batch_scopes: Vec<Option<broca_protocol::SessionScope>>,
}

/// In-process tool plane. Register handlers by tool name; unknown tools yield an
/// `is_error` result (a tool the manifest shouldn't have admitted).
pub struct StubToolPlane {
    handlers: HashMap<String, Handler>,
    state: Mutex<State>,
    /// The module every call reports as its dispatch target, as a subc plane reports the
    /// module it routes the call to. `None` by default: an in-process plane routes to none.
    dispatch_target: Option<String>,
}

impl Default for StubToolPlane {
    fn default() -> Self {
        Self::new()
    }
}

impl StubToolPlane {
    /// Register a handler for `tool_name`.
    pub fn with_tool<F>(mut self, tool_name: &str, handler: F) -> Self
    where
        F: Fn(&ToolCall) -> StubReply + Send + Sync + 'static,
    {
        self.handlers
            .insert(tool_name.to_owned(), Box::new(handler));
        self
    }

    /// Register a tool that always returns the same text output.
    pub fn with_canned_text(self, tool_name: &str, text: &str) -> Self {
        let text = text.to_owned();
        self.with_tool(tool_name, move |_| {
            StubReply::Output(ToolOutput::Text {
                text: text.clone().into(),
            })
        })
    }

    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
            state: Mutex::new(State::default()),
            dispatch_target: None,
        }
    }
}

#[async_trait]
impl ToolPlane for StubToolPlane {
    async fn batch_started(
        &self,
        ctx: &BatchCtx,
        calls: &[ToolCall],
        _manifest: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        let call_ids = calls.iter().map(|c| c.tool_call_id.clone()).collect();
        let mut state = self.state.lock().unwrap();
        state.log.push(StubEvent::BatchStarted { call_ids });
        state.batch_scopes.push(ctx.scope.clone());
        match state.batch_start_error.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn dispatch_target(&self, _ctx: &BatchCtx, _tool_call_id: &ToolCallId) -> Option<String> {
        self.dispatch_target.clone()
    }

    async fn dispatch(&self, _ctx: &BatchCtx, call: &ToolCall) -> DispatchResult {
        // Fence: a repeated tool_call_id returns the prior result, no re-exec.
        {
            let mut st = self.state.lock().unwrap();
            if let Some(prior) = st.fence.get(&call.tool_call_id).cloned() {
                st.log.push(StubEvent::Fenced(call.tool_call_id.clone()));
                return DispatchResult::Completed(prior);
            }
        }

        let reply = match self.handlers.get(&call.tool_name) {
            Some(h) => h(call),
            None => StubReply::ExecError(format!("unknown tool: {}", call.tool_name)),
        };

        let result = match reply {
            StubReply::Output(output) => ToolResult {
                tool_call_id: call.tool_call_id.clone(),
                output,
                is_error: false,
            },
            StubReply::ExecError(message) => ToolResult {
                tool_call_id: call.tool_call_id.clone(),
                output: ToolOutput::error(message),
                is_error: true,
            },
            // The request never left → SAFE. Not cached (not a result).
            StubReply::PlaneUnavailable(m) => return DispatchResult::NotSent { reason: m },
            // Sent but no outcome → AMBIGUOUS. Not cached.
            StubReply::OutcomeUnknown(m) => {
                return DispatchResult::SentButOutcomeUnknown { reason: m }
            }
            StubReply::ScopeDrained { reason, sent } => {
                return DispatchResult::ScopeDrained { reason, sent }
            }
        };

        let mut st = self.state.lock().unwrap();
        *st.exec_count.entry(call.tool_call_id.clone()).or_insert(0) += 1;
        st.fence.insert(call.tool_call_id.clone(), result.clone());
        st.log
            .push(StubEvent::Dispatched(call.tool_call_id.clone()));
        DispatchResult::Completed(result)
    }

    async fn batch_finished(&self, _ctx: &BatchCtx) -> Result<(), ToolPlaneError> {
        self.state
            .lock()
            .unwrap()
            .log
            .push(StubEvent::BatchFinished);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::BatchId;
    use broca_protocol::BindIdentity;
    use serde_json::json;

    fn ctx() -> BatchCtx {
        BatchCtx {
            media_capture: None,
            batch_id: BatchId("b1".into()),
            bind: BindIdentity {
                project_root: "/tmp/x".into(),
                harness: "test".into(),
                session: "s1".into(),
            },
            run_id: broca_protocol::RunId("r1".into()),
            scope: None,
        }
    }

    fn call(id: &str, name: &str) -> ToolCall {
        ToolCall {
            thought_signature: None,
            provider_state: None,
            tool_call_id: ToolCallId(id.into()),
            tool_name: name.into(),
            input: json!({}).into(),
        }
    }

    #[tokio::test]
    async fn unknown_tool_is_error_result_not_plane_error() {
        let plane = StubToolPlane::new();
        let r = completed(plane.dispatch(&ctx(), &call("t1", "nope")).await);
        assert!(r.is_error);
        assert!(matches!(r.output, ToolOutput::Error { .. }));
    }

    fn completed(d: DispatchResult) -> ToolResult {
        match d {
            DispatchResult::Completed(r) => r,
            other => panic!("expected Completed, got {other:?}"),
        }
    }
}
