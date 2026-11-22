//! The run loop's calls into the session's compaction provider, and the WAL records that make
//! them durable.
//!
//! Setup is a call the run waits on before its first model call. After it, the provider
//! announces each view it publishes with a `view_ready` event ([`CompactionViews`]). The run
//! fetches the view with `view.get` only at a step boundary and applies it there
//! ([`Runtime::views_at_boundary`], [`Runtime::apply_view`]). When the request's prefix is
//! being rewritten anyway, the run also asks the provider for a view itself, with a short
//! `compaction.ride`. Advance notices are recorded here and published from the durable WAL by
//! the bus publisher ([`Runtime::record_notice`]).

use super::*;
use crate::compaction_provider::{
    notice::ViewReady,
    ride::{RideAnswer, RideRequest},
    setup::{SetupAnswer, SetupRequest},
    view::{ViewGetAnswer, ViewGetRequest},
};
use async_trait::async_trait;
use broca_protocol::BindIdentity;

#[derive(Debug, Clone)]
pub struct CompactionUnavailable;

#[derive(Debug, Clone)]
pub struct CompactionConfig {
    pub call_budget_ms: u64,
}
impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            call_budget_ms: COMPACTION_CALL_BUDGET_MS,
        }
    }
}

/// What a catalog read establishes about a compaction provider. A failed read is not evidence
/// that the provider has no declarations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProviderCapabilities {
    /// The provider's declared `provides` list, possibly kept from a successful earlier read.
    Declared(Vec<String>),
    /// A successful catalog read contained no entry for the provider.
    NotInCatalog,
    /// The catalog could not be read and no declared list is available.
    Unknown,
}

/// A sibling of HookPlane on the same module-level route machinery. Route errors and answers
/// that do not decode are failed calls.
/// Each call carries the session's frozen scope so opening a provider route cannot drop it.
#[async_trait]
pub trait CompactionPlane: Send + Sync {
    /// Content-free mode-3 calls on a separate session-bound v2 route. Keeping
    /// this seam distinct prevents a legacy Setup body from reaching v2.
    async fn call_v2(
        &self,
        _provider: &str,
        _bind: &BindIdentity,
        _scope: Option<&broca_protocol::SessionScope>,
        _method: &str,
        _params: serde_json::Value,
        _deadline_ms: u64,
    ) -> Result<Box<serde_json::value::RawValue>, crate::compaction_v2::runtime::CallError> {
        Err(crate::compaction_v2::runtime::CallError::Unavailable)
    }

    /// Read the daemon's catalog afresh, with a bounded wait. A failed read may use the last
    /// successfully read list, but must not claim that the provider declared nothing.
    async fn provider_capabilities(&self, _provider: &str) -> ProviderCapabilities {
        ProviderCapabilities::Unknown
    }

    async fn setup(
        &self,
        provider: &str,
        bind: &BindIdentity,
        scope: Option<&broca_protocol::SessionScope>,
        request: SetupRequest,
    ) -> Result<SetupAnswer, CompactionUnavailable>;

    /// `view.get`: fetch the view a `view_ready` event announced (compaction-provider/v1 §6).
    /// The default fails the call, which leaves the run on its current view, as a provider
    /// that cannot be reached does.
    async fn view_get(
        &self,
        _provider: &str,
        _bind: &BindIdentity,
        _scope: Option<&broca_protocol::SessionScope>,
        _request: ViewGetRequest,
    ) -> Result<ViewGetAnswer, CompactionUnavailable> {
        Err(CompactionUnavailable)
    }

    /// `compaction.ride`: ask the provider for a view now (compaction-provider/v1 §8). The
    /// caller bounds the call by `request.deadline_ms`. The default fails the call, which the
    /// run treats as the answer `nothing`.
    async fn ride(
        &self,
        _provider: &str,
        _bind: &BindIdentity,
        _scope: Option<&broca_protocol::SessionScope>,
        _request: RideRequest,
    ) -> Result<RideAnswer, CompactionUnavailable> {
        Err(CompactionUnavailable)
    }
}

/// The `view_ready` events the session's compaction provider published, waiting for the run's
/// next step boundary. An event only says that a view exists. At a boundary where no ride
/// applied a view, the run loop asks for the newest one and fetches that view with `view.get`
/// there, never mid-step, then records, before the step, what it did with it.
pub trait CompactionViews: Send + Sync {
    /// The newest `view_ready` for the session `session_ref` whose version is above `after`.
    /// `lineage_id` is the lineage the run's transcript counts in. A `view_ready` event names
    /// no lineage, so a source fed from the bus ignores it; the fetched view's own lineage is
    /// checked when it is offered, and a view of another lineage is never applied.
    fn newest_ready(
        &self,
        session_ref: &str,
        lineage_id: &str,
        after: Option<u64>,
    ) -> Option<ViewReady>;
}

/// The newest `view_ready` event for up to 1,000 sessions. The bus intake commits receipts
/// before enqueueing and retries them when the inbox is full; restart rebuilds from receipts.
#[derive(Default)]
pub struct ViewReadyInbox {
    newest: std::sync::Mutex<HashMap<String, ViewReady>>,
}

impl CompactionViews for ViewReadyInbox {
    fn newest_ready(
        &self,
        session_ref: &str,
        _lineage_id: &str,
        after: Option<u64>,
    ) -> Option<ViewReady> {
        self.newest
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(session_ref)
            .filter(|ready| after.is_none_or(|current| ready.view_version > current))
            .cloned()
    }
}
