//! The run loop's calls into the session's compaction provider, and the WAL records that make
//! them durable.
//!
//! Only Setup is a call the run waits on. Views reach the run asynchronously and are applied at
//! step boundaries ([`Runtime::apply_view`]); advance notices are recorded here and published
//! from the durable WAL by the bus publisher ([`Runtime::record_notice`]).

use super::*;
use crate::compaction_provider::{
    errors::RefuseCode,
    notice::AdvanceNotice,
    setup::{SetupAnswer, SetupRequest},
    view::CompactionView,
};
use async_trait::async_trait;
use broca_protocol::{
    error::{ErrorClass, ProviderError, ProviderRefusal},
    BindIdentity, RunFinishReason,
};

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

async fn clock_deadline(clock: Option<&crate::Clock>, deadline_ms: u64) {
    #[cfg(feature = "test-clock")]
    if let Some(clock) = clock {
        let mut changes = clock.subscribe();
        loop {
            let remaining = deadline_ms.saturating_sub(clock.now_ms());
            if remaining == 0 {
                return;
            }
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_millis(remaining)) => {},
                _ = changes.changed() => {},
            }
        }
    }
    let now = clock.map_or_else(super::now, crate::Clock::now_ms);
    tokio::time::sleep(std::time::Duration::from_millis(
        deadline_ms.saturating_sub(now),
    ))
    .await;
}

/// A sibling of HookPlane on the same module-level route machinery. Route errors and answers
/// that do not decode are failed calls.
#[async_trait]
pub trait CompactionPlane: Send + Sync {
    async fn setup(
        &self,
        provider: &str,
        bind: &BindIdentity,
        request: SetupRequest,
    ) -> Result<SetupAnswer, CompactionUnavailable>;
}

/// Views fetched from the session's compaction provider (on its `view_ready` events, with
/// `view.get`), waiting for the run's next step boundary. The run loop asks at every boundary
/// and records, before the step, what it did with the answer.
pub trait CompactionViews: Send + Sync {
    /// The newest view fetched for the session `session_ref` whose version is above `after`,
    /// with the `view_ready` event it answered. `lineage_id` is the lineage the run's
    /// transcript counts in; a view of another lineage is never applied.
    fn newest(
        &self,
        session_ref: &str,
        lineage_id: &str,
        after: Option<u64>,
    ) -> Option<(
        CompactionView,
        crate::compaction_provider::notice::ViewReady,
    )>;
}

pub(crate) struct Runtime<'a> {
    pub provider: &'a str,
    pub bind: &'a BindIdentity,
    pub plane: Option<&'a Arc<dyn CompactionPlane>>,
    pub writer: &'a crate::WalHandle,
    pub control: &'a crate::RunControl,
}
/// How a run ends when a recorded compaction wait seals it.
pub(crate) struct Terminal(pub RunFinishReason, pub Option<Box<ProviderError>>);

fn unavailable() -> Box<ProviderError> {
    Box::new(error(
        None,
        "compaction Setup unavailable",
        Some("compaction_unavailable".into()),
        false,
    ))
}
pub(crate) fn error(
    refusal: Option<Box<ProviderRefusal>>,
    reason: &str,
    provider_code: Option<String>,
    retryable: bool,
) -> ProviderError {
    ProviderError {
        provider_subcode: None,
        refusal,
        class: if retryable {
            ErrorClass::Transient
        } else {
            ErrorClass::Permanent
        },
        message: reason.into(),
        status: None,
        retry_after_secs: None,
        provider_code,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    }
}
fn refusal(
    provider: &str,
    code: RefuseCode,
    reason: &str,
    provider_code: Option<String>,
) -> ProviderError {
    let retryable = code.retryable();
    error(
        Some(Box::new(ProviderRefusal {
            provider: provider.into(),
            reason: Some(reason.into()),
            provider_detail_code: provider_code,
        })),
        reason,
        Some(code.as_str().into()),
        retryable,
    )
}

/// Seal a wait the removed synchronous client left open when the process stopped. Recovery
/// is never permission to call the provider again: a terminal intent already recorded ends the
/// run with that exit, and a wait with none ends `interrupted`, recorded before the terminal.
pub(crate) async fn seal_recorded_wait(
    writer: &crate::WalHandle,
    state: &mut CompactionState,
) -> Result<Option<Terminal>, crate::RunError> {
    let Some(wait) = state.open_wait.clone() else {
        return Ok(None);
    };
    let exit = wait
        .terminal_exit
        .unwrap_or(CompactionWaitExit::Interrupted);
    if wait.terminal_exit.is_none() {
        let record = WalRecord::CompactionWait {
            exited: exit,
            reason: wait.wait.reason.clone(),
        };
        writer.record(record.clone());
        writer.sync().await?;
        state.fold(&record);
    }
    Ok(Some(terminal_exit(exit)))
}

impl Runtime<'_> {
    fn now(&self) -> u64 {
        self.control
            .clock
            .as_ref()
            .map_or_else(super::now, crate::Clock::now_ms)
    }
    async fn commit(
        &self,
        state: &mut CompactionState,
        record: WalRecord,
    ) -> Result<(), crate::RunError> {
        self.writer.record(record.clone());
        self.writer.sync().await?;
        state.fold(&record);
        Ok(())
    }

    /// Call Setup once for the session and record its answer before the first model call
    /// (compaction-provider/v1 §4). A failure, a timeout, an answer for another request, an
    /// initial message broca cannot render or a malformed ceiling is `compaction_unavailable`;
    /// a refusal carries the provider's role code. Either way nothing is recorded, so the next
    /// send calls Setup again.
    pub async fn setup(
        &self,
        mut request: SetupRequest,
        raw: &[Message],
    ) -> Result<CompactionState, Box<ProviderError>> {
        let Some(plane) = self.plane else {
            return Err(unavailable());
        };
        request.now = self.now();
        let request_id = request.request_id.clone();
        let answer = tokio::select! {
            answer = plane.setup(self.provider, self.bind, request) => answer.ok(),
            _ = clock_deadline(self.control.clock.as_ref(), self.now().saturating_add(self.control.compaction_config.call_budget_ms)) => None,
        }.ok_or_else(unavailable)?;
        if answer.request_id() != request_id {
            return Err(unavailable());
        }
        let (initial, ceiling) = match answer {
            SetupAnswer::Refuse {
                code,
                reason,
                provider_code,
                ..
            } => {
                return Err(Box::new(refusal(
                    self.provider,
                    code,
                    &reason,
                    provider_code,
                )))
            }
            SetupAnswer::Ready {
                initial, ceiling, ..
            } => (initial, ceiling),
            // The answer decodes strictly on `answer`, so no other kind reaches here.
            #[allow(unreachable_patterns)]
            _ => return Err(unavailable()),
        };
        if ceiling
            .as_ref()
            .is_some_and(|policy| policy.check().is_err())
        {
            return Err(unavailable());
        }
        let messages = validate_initial(&initial, raw).ok_or_else(unavailable)?;
        let record = WalRecord::CompactionMessage {
            provider: self.provider.into(),
            kind: CompactionMessageKind::Setup,
            compaction_id: initial.compaction_id,
            version: initial.version,
            range: CompactionRange {
                from: initial.range.from,
                to: initial.range.to,
            },
            sha256: broca_wal::compaction_replacement_sha256(&messages)
                .map_err(|_| unavailable())?,
            replacement: Arc::new(messages),
            request_id: None,
            cursor: None,
            call_when: None,
            stability: None,
            ceiling: ceiling.map(|policy| {
                OpaqueJson::from_json_text(serde_json::to_string(&policy).expect("role JSON"))
                    .expect("role JSON")
            }),
        };
        let mut state = CompactionState::default();
        self.commit(&mut state, record)
            .await
            .map_err(|_| unavailable())?;
        state
            .applied
            .as_mut()
            .expect("Setup applied")
            .thinking_prefix_len = raw.len() as u64;
        Ok(state)
    }

    /// Offer a fetched view at a step boundary: decide what to do with it
    /// ([`view::decide`]), record it when it applies, and return the event the next notice
    /// reports for it, if any. The caller holds a deferred view and offers it again at a later
    /// boundary.
    ///
    #[allow(clippy::too_many_arguments)]
    pub async fn offer_view(
        &self,
        state: &mut CompactionState,
        fetched: &CompactionView,
        ready: &crate::compaction_provider::notice::ViewReady,
        lineage_id: &str,
        prompt: &[Message],
        stored: &[Message],
        prefix_bound: bool,
    ) -> Result<
        (
            view::Decision,
            Option<crate::compaction_provider::notice::Event>,
        ),
        crate::RunError,
    > {
        let decision = view::decide(
            fetched,
            ready,
            state,
            lineage_id,
            prompt,
            stored,
            prefix_bound,
        );
        if let view::Decision::Apply(choice) = &decision {
            self.apply_view(state, fetched, choice.clone()).await?;
        }
        let event = decision.event(fetched.view_version);
        Ok((decision, event))
    }

    /// Record `view` as applied with `choice`, at the step boundary before the step that first
    /// renders it, and fold it into `state`. Durable before it returns, so a crash after this
    /// point resumes rendering the view without asking the provider again.
    async fn apply_view(
        &self,
        state: &mut CompactionState,
        view: &CompactionView,
        choice: broca_wal::CompactionViewChoice,
    ) -> Result<(), crate::RunError> {
        let json = serde_json::to_string(view).map_err(|e| crate::RunError::Wal(e.to_string()))?;
        self.commit(
            state,
            WalRecord::CompactionViewApplied {
                provider: self.provider.into(),
                view_version: view.view_version,
                view: OpaqueJson::from_json_text(json)
                    .map_err(|e| crate::RunError::Wal(e.to_string()))?,
                choice,
            },
        )
        .await
    }

    /// Record an advance notice and fold it into `state`. The bus publisher projects notices
    /// from the durable WAL, so this is the only place a notice comes into being, and it is
    /// durable before anything can publish it. A notice the role would refuse is not recorded.
    pub async fn record_notice(
        &self,
        state: &mut CompactionState,
        notice: AdvanceNotice,
    ) -> Result<(), crate::RunError> {
        if let Err(problem) = notice.check() {
            tracing::warn!(problem = %problem.name(), "not recording a malformed advance notice");
            return Ok(());
        }
        let record = WalRecord::CompactionNotice {
            seq: notice.seq,
            headers: notice.to_headers(),
        };
        self.writer.record(record.clone());
        // The publisher projects only durable WAL ranges, so this barrier is what keeps a
        // notice from reaching the bus before a crash could still lose it.
        self.writer.sync().await?;
        state.fold(&record);
        Ok(())
    }
}

fn terminal_exit(exit: CompactionWaitExit) -> Terminal {
    match exit {
        CompactionWaitExit::Cancel => Terminal(RunFinishReason::Cancelled, None),
        // A continuing exit closes the wait in the fold, so only a terminal one is left open;
        // treat anything else as the interruption it would have been.
        CompactionWaitExit::Stopped
        | CompactionWaitExit::Interrupted
        | CompactionWaitExit::Ready
        | CompactionWaitExit::Bound
        | CompactionWaitExit::Cap => Terminal(RunFinishReason::Interrupted, None),
        CompactionWaitExit::Exceeded => Terminal(
            RunFinishReason::Error,
            Some(Box::new(error(
                None,
                "compaction wait exceeded; request cannot be shown to fit",
                Some("compaction_wait_exceeded".into()),
                true,
            ))),
        ),
    }
}
