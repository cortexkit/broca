use super::*;
use crate::compaction_provider::{
    errors::RefuseCode,
    ready::{CompactionReady, ReadyOutcome},
    setup::{SetupAnswer, SetupRequest},
    status::StepStatus,
};
use async_trait::async_trait;
use broca_protocol::{
    error::{ErrorClass, ProviderError, ProviderRefusal},
    BindIdentity, RunFinishReason,
};
use std::sync::Mutex;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct CompactionUnavailable;

#[derive(Clone)]
pub(crate) struct LateReply {
    answer: StepAnswer,
    fence: FenceState,
    raw: Vec<Message>,
    cursor: CompactionCursor,
    #[cfg(feature = "test-clock")]
    acknowledge: bool,
}

pub(crate) struct CompactionReplies {
    tx: tokio::sync::mpsc::Sender<LateReply>,
    rx: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<LateReply>>,
    tasks: Mutex<Vec<tokio::task::AbortHandle>>,
    dropped: Arc<std::sync::atomic::AtomicUsize>,
    #[cfg(feature = "test-clock")]
    test_root: Option<std::path::PathBuf>,
    #[cfg(feature = "test-clock")]
    fences: Mutex<std::collections::VecDeque<LateReply>>,
}
impl CompactionReplies {
    pub fn new() -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        Self {
            tx,
            rx: tokio::sync::Mutex::new(rx),
            tasks: Mutex::new(Vec::new()),
            dropped: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            #[cfg(feature = "test-clock")]
            test_root: None,
            #[cfg(feature = "test-clock")]
            fences: Mutex::new(std::collections::VecDeque::new()),
        }
    }
    pub async fn next(&self) -> LateReply {
        let mut rx = self.rx.lock().await;
        loop {
            tokio::select! {
                reply = rx.recv() => return reply.expect("run owns reply sender"),
                _ = self.poll_test_receipt() => {}
            }
        }
    }
    #[cfg(feature = "test-clock")]
    pub fn with_test_root(mut self, root: Option<std::path::PathBuf>) -> Self {
        self.test_root = root;
        self
    }
    async fn poll_test_receipt(&self) {
        #[cfg(feature = "test-clock")]
        if let Some(root) = &self.test_root {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            let path = root.join("receipt");
            if let Ok(bytes) = std::fs::read(&path) {
                let receipt: super::test_control::Receipt =
                    serde_json::from_slice(&bytes).expect("test receipt JSON");
                let mut reply = self
                    .fences
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|reply| reply.fence.newest_request_id == receipt.original_request_id)
                    .cloned()
                    .expect("test receipt must name a real issued request");
                reply.answer = receipt.answer;
                reply.acknowledge = true;
                if self.tx.try_send(reply).is_err() {
                    Self::count_drop(&self.dropped);
                }
                std::fs::remove_file(path).expect("consume test receipt");
            }
            return;
        }
        std::future::pending::<()>().await;
    }
    fn retain(
        &self,
        call: impl std::future::Future<Output = Result<StepAnswer, CompactionUnavailable>>
            + Send
            + 'static,
        mut reply: LateReply,
        clock: Option<crate::Clock>,
        cap_ms: u64,
    ) {
        let tx = self.tx.clone();
        let dropped = self.dropped.clone();
        let mut tasks = self.tasks.lock().unwrap();
        tasks.retain(|task| !task.is_finished());
        if tasks.len() >= 8 {
            Self::count_drop(&dropped);
            return;
        }
        let task = tokio::spawn(async move {
            tokio::pin!(call);
            let received = tokio::select! {
                result = &mut call => result.ok(),
                _ = clock_deadline(clock.as_ref(), cap_ms) => None,
            };
            if let Some(answer) = received {
                reply.answer = answer;
                if matches!(
                    tx.try_send(reply),
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_))
                ) {
                    Self::count_drop(&dropped);
                }
            }
        });
        tasks.push(task.abort_handle());
    }
    fn count_drop(dropped: &std::sync::atomic::AtomicUsize) {
        if dropped.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 0 {
            tracing::warn!(
                "compaction late-reply capacity exhausted; receipts dropped in this run"
            );
        }
    }
}
impl Drop for CompactionReplies {
    fn drop(&mut self) {
        // Run termination closes the inbox. Replies still queued are dropped unrecorded,
        // and retained calls are cancelled: only the active run loop may write receipts.
        for task in self.tasks.get_mut().unwrap().drain(..) {
            task.abort();
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompactionConfig {
    pub call_budget_ms: u64,
    pub step_wait_cap_ms: u64,
}
impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            call_budget_ms: COMPACTION_CALL_BUDGET_MS,
            step_wait_cap_ms: COMPACTION_STEP_WAIT_CAP_MS,
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
/// A sibling of HookPlane on the same module-level route machinery. The adapter returns
/// shared, strictly tagged answers; route errors and unknown tags are failed calls.
#[async_trait]
pub trait CompactionPlane: Send + Sync {
    async fn setup(
        &self,
        provider: &str,
        bind: &BindIdentity,
        request: SetupRequest,
    ) -> Result<SetupAnswer, CompactionUnavailable>;
    async fn step(
        &self,
        provider: &str,
        bind: &BindIdentity,
        request: StepStatus,
    ) -> Result<StepAnswer, CompactionUnavailable>;
}

/// Session-owned ready mailbox. Publishing the issued high-water before dispatch avoids
/// losing a hint that races the wait answer. Only the daemon-verified provider can publish.
#[derive(Default)]
pub struct CompactionInbox {
    current: Mutex<Option<String>>,
    ready: Mutex<Option<String>>,
    notify: Notify,
}
impl CompactionInbox {
    pub fn issue(&self, request: &str) {
        *self.current.lock().unwrap() = Some(request.into());
        *self.ready.lock().unwrap() = None;
    }
    pub fn ready(
        &self,
        caller: &str,
        provider: Option<&str>,
        request: &CompactionReady,
    ) -> Result<(), &'static str> {
        let current = self.current.lock().unwrap();
        if request.check(caller, provider, current.as_deref())? == ReadyOutcome::CallAgain {
            *self.ready.lock().unwrap() = Some(request.request_id.clone());
            self.notify.notify_one();
        }
        Ok(())
    }
    async fn wait(&self, request_id: &str) {
        loop {
            let notified = self.notify.notified();
            if self.ready.lock().unwrap().as_deref() == Some(request_id) {
                return;
            }
            notified.await;
        }
    }
}

pub(crate) struct Runtime<'a> {
    pub provider: &'a str,
    pub bind: &'a BindIdentity,
    pub plane: Option<&'a Arc<dyn CompactionPlane>>,
    pub inbox: Option<&'a Arc<CompactionInbox>>,
    pub writer: &'a crate::WalHandle,
    pub control: &'a crate::RunControl,
    pub replies: &'a CompactionReplies,
}
pub(crate) enum StepResult {
    Continue,
    Terminal(RunFinishReason, Option<ProviderError>),
}
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
async fn cancelled(token: Option<&CancellationToken>) {
    match token {
        Some(token) => token.cancelled().await,
        None => std::future::pending().await,
    }
}
impl Runtime<'_> {
    pub async fn with_replies<F: std::future::Future>(
        &self,
        state: &mut CompactionState,
        future: F,
    ) -> Result<F::Output, crate::RunError> {
        tokio::pin!(future);
        loop {
            tokio::select! {
                result = &mut future => return Ok(result),
                reply = self.replies.next() => self.discard_reply(state, reply).await?,
            }
        }
    }
    pub async fn discard_reply(
        &self,
        state: &mut CompactionState,
        reply: LateReply,
    ) -> Result<(), crate::RunError> {
        let mut fence = reply.fence;
        fence.newest_request_id = state.newest_request_id.clone().unwrap_or_default();
        fence.last_applied_version = state.applied.as_ref().map(|applied| applied.version);
        // The original call has timed out or consumed its answer. Even a malformed or
        // lower-version duplicate belongs to that closed request, not a new pending answer.
        let disposition = match validate_answer(&fence, &reply.answer, self.now(), &reply.raw) {
            Err(CompactionDisposition::Late) => CompactionDisposition::Late,
            _ => CompactionDisposition::SupersededRequest,
        };
        let rejected = match &reply.answer {
            StepAnswer::CompactionMessage { compaction, .. } => Some(RejectedCompaction {
                compaction_id: compaction.compaction_id.clone(),
                version: compaction.version,
                range: CompactionRange {
                    from: compaction.range.from,
                    to: compaction.range.to,
                },
            }),
            _ => None,
        };
        self.commit(
            state,
            WalRecord::CompactionAnswer {
                provider: self.provider.into(),
                request_id: reply.answer.request_id().into(),
                cursor: state.cursor.clone().unwrap_or(reply.cursor),
                disposition,
                rejected,
                wait: None,
            },
        )
        .await?;
        #[cfg(feature = "test-clock")]
        if reply.acknowledge {
            super::test_control::acknowledge(
                self.replies.test_root.as_ref().expect("test ingress root"),
            )?;
        }
        Ok(())
    }
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
        match answer {
            SetupAnswer::Refuse {
                code,
                reason,
                provider_code,
                ..
            } => Err(Box::new(refusal(
                self.provider,
                code,
                &reason,
                provider_code,
            ))),
            SetupAnswer::Ready {
                initial,
                stability,
                call_when,
                ..
            } => {
                if call_when
                    .as_ref()
                    .is_some_and(|when| !when.shares_in_range())
                {
                    return Err(unavailable());
                }
                let answer = StepAnswer::CompactionMessage {
                    request_id: request_id.clone(),
                    compaction: initial.clone(),
                    coverage: None,
                    channel2_directive: None,
                    bootstrap_complete: None,
                };
                let fence = FenceState {
                    newest_request_id: request_id,
                    last_applied_version: None,
                    newest_ordinal: raw.last().and_then(|m| m.ordinal),
                    newest_deadline_ms: None,
                };
                let messages = validate_answer(&fence, &answer, self.now(), raw)
                    .ok()
                    .flatten()
                    .ok_or_else(unavailable)?;
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
                    call_when: call_when.map(|when| {
                        OpaqueJson::from_json_text(serde_json::to_string(&when).expect("role JSON"))
                            .expect("role JSON")
                    }),
                    stability: Some(
                        stability
                            .into_iter()
                            .map(|rank| CompactionStabilityRank {
                                index: rank.index,
                                rank: rank.rank,
                            })
                            .collect(),
                    ),
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
        }
    }
    pub async fn recover_wait(
        &self,
        state: &mut CompactionState,
    ) -> Result<Option<StepResult>, crate::RunError> {
        let Some(wait) = state.open_wait.clone() else {
            return Ok(None);
        };
        let exit = wait
            .terminal_exit
            .unwrap_or(CompactionWaitExit::Interrupted);
        if wait.terminal_exit.is_none() {
            self.exit(state, exit, &wait.wait.reason).await?;
        }
        Ok(Some(terminal_exit(exit)))
    }
    async fn exit(
        &self,
        state: &mut CompactionState,
        exited: CompactionWaitExit,
        reason: &str,
    ) -> Result<(), crate::RunError> {
        self.commit(
            state,
            WalRecord::CompactionWait {
                exited,
                reason: reason.into(),
            },
        )
        .await
    }
    pub async fn step(
        &self,
        state: &mut CompactionState,
        mut status: StepStatus,
        raw: &[Message],
        estimate: Option<u64>,
        cache: Option<crate::CacheClockReading>,
    ) -> Result<StepResult, crate::RunError> {
        if state.pending_request {
            // A crash left no receipt, so ingestion is unknown. File the interrupted call
            // without moving its cursor, then issue a fresh fence and resend its messages.
            let cursor = state.cursor.clone().unwrap_or_else(|| CompactionCursor {
                lineage_id: status.lineage_id.clone(),
                after_ordinal: None,
            });
            let request_id = state
                .newest_request_id
                .clone()
                .expect("pending issued request");
            self.commit(
                state,
                WalRecord::CompactionAnswer {
                    provider: self.provider.into(),
                    request_id,
                    cursor,
                    disposition: CompactionDisposition::FailedCall,
                    rejected: None,
                    wait: None,
                },
            )
            .await?;
        }
        let cap_ms = cache.and_then(|cache| cache.expires_at_ms()).map_or(
            self.control.compaction_config.step_wait_cap_ms,
            |expires| {
                expires
                    .saturating_sub(self.now())
                    .min(self.control.compaction_config.step_wait_cap_ms)
            },
        );
        let cap = self.now().saturating_add(cap_ms);
        loop {
            status.request_id = format!(
                "{}:compaction:{}",
                status.lineage_id,
                self.writer.next_record_seq()
            );
            status.now = self.now();
            state.fill_status(&mut status, raw);
            self.commit(
                state,
                WalRecord::CompactionRequest {
                    provider: self.provider.into(),
                    request_id: status.request_id.clone(),
                },
            )
            .await?;
            if let Some(inbox) = self.inbox {
                inbox.issue(&status.request_id);
            }
            let fence = FenceState {
                newest_request_id: status.request_id.clone(),
                last_applied_version: state.applied.as_ref().map(|applied| applied.version),
                newest_ordinal: status.newest.as_ref().map(|newest| newest.ordinal),
                newest_deadline_ms: Some(
                    self.now()
                        .saturating_add(self.control.compaction_config.call_budget_ms),
                ),
            };
            #[cfg(feature = "test-clock")]
            {
                let mut fences = self.replies.fences.lock().unwrap();
                if fences.len() == 8 {
                    fences.pop_front();
                }
                fences.push_back(LateReply {
                    answer: StepAnswer::Noop {
                        request_id: status.request_id.clone(),
                        channel2_directive: None,
                        bootstrap_complete: None,
                    },
                    fence: fence.clone(),
                    raw: raw.to_vec(),
                    cursor: CompactionCursor {
                        lineage_id: status.lineage_id.clone(),
                        after_ordinal: status.after_ordinal,
                    },
                    acknowledge: false,
                });
            }
            let answer = match self.plane {
                Some(plane) => {
                    let plane = Arc::clone(plane);
                    let provider = self.provider.to_owned();
                    let bind = self.bind.clone();
                    let request = status.clone();
                    let mut call =
                        Box::pin(async move { plane.step(&provider, &bind, request).await });
                    loop {
                        tokio::select! {
                            biased;
                            _ = clock_deadline(self.control.clock.as_ref(), fence.newest_deadline_ms.expect("deadline set")) => {
                                self.replies.retain(call, LateReply {
                                    answer:StepAnswer::Noop { request_id:status.request_id.clone(), channel2_directive: None, bootstrap_complete: None },
                                    fence:fence.clone(), raw:raw.to_vec(),
                                cursor:CompactionCursor { lineage_id:status.lineage_id.clone(), after_ordinal:status.after_ordinal },
                                #[cfg(feature = "test-clock")]
                                acknowledge: false,
                                }, self.control.clock.clone(), self.now().saturating_add(self.control.compaction_config.step_wait_cap_ms.min(COMPACTION_STEP_WAIT_CAP_MS)));
                                break Err(())
                            },
                            answer = &mut call => break Ok(answer),
                            reply = self.replies.next() => self.discard_reply(state, reply).await?,
                        }
                    }
                }
                None => Ok(Err(CompactionUnavailable)),
            };
            let mut cursor = CompactionCursor {
                lineage_id: status.lineage_id.clone(),
                after_ordinal: status.next_cursor(),
            };
            let (answer, disposition, replacement) = match answer {
                Err(_) => (None, CompactionDisposition::Timeout, None),
                Ok(Err(_)) => (None, CompactionDisposition::FailedCall, None),
                Ok(Ok(answer)) => {
                    let validation = validate_answer(&fence, &answer, self.now(), raw);
                    let (disposition, replacement) = match validation {
                        Err(disposition) => (disposition, None),
                        Ok(replacement) => (
                            match &answer {
                                StepAnswer::Noop { .. } => CompactionDisposition::Noop,
                                StepAnswer::Wait { .. } => CompactionDisposition::Wait,
                                StepAnswer::Refuse { .. } => CompactionDisposition::Refuse,
                                StepAnswer::CompactionMessage { .. } => CompactionDisposition::Noop,
                            },
                            replacement,
                        ),
                    };
                    (Some(answer), disposition, replacement)
                }
            };
            // The role has no outcome-specific next_cursor helper. Its fence says late and
            // superseded answers ingest nothing; other durable answers advance to sent messages.
            if matches!(
                disposition,
                CompactionDisposition::Late | CompactionDisposition::SupersededRequest
            ) {
                cursor.after_ordinal = status.after_ordinal;
            }
            if let (Some(StepAnswer::CompactionMessage { compaction, .. }), Some(messages)) =
                (&answer, replacement)
            {
                let record = WalRecord::CompactionMessage {
                    provider: self.provider.into(),
                    kind: CompactionMessageKind::Step,
                    compaction_id: compaction.compaction_id.clone(),
                    version: compaction.version,
                    range: CompactionRange {
                        from: compaction.range.from,
                        to: compaction.range.to,
                    },
                    sha256: broca_wal::compaction_replacement_sha256(&messages)
                        .expect("native JSON"),
                    replacement: Arc::new(messages),
                    request_id: Some(status.request_id.clone()),
                    cursor: Some(cursor),
                    call_when: None,
                    stability: None,
                };
                self.commit(state, record).await?;
                state
                    .applied
                    .as_mut()
                    .expect("Step applied")
                    .thinking_prefix_len = raw.len() as u64;
                #[cfg(feature = "test-clock")]
                super::test_control::barrier(
                    self.control.clock.as_ref(),
                    &self.bind.session,
                    "CompactionApplied",
                )
                .await?;
                return Ok(StepResult::Continue);
            }
            let rejected = match &answer {
                Some(StepAnswer::CompactionMessage { compaction, .. }) => {
                    Some(RejectedCompaction {
                        compaction_id: compaction.compaction_id.clone(),
                        version: compaction.version,
                        range: CompactionRange {
                            from: compaction.range.from,
                            to: compaction.range.to,
                        },
                    })
                }
                _ => None,
            };
            let wait = match &answer {
                Some(StepAnswer::Wait {
                    reason, bound_ms, ..
                }) if disposition == CompactionDisposition::Wait => Some(CompactionWait {
                    reason: reason.clone(),
                    bound_ms: *bound_ms,
                }),
                _ => None,
            };
            self.commit(
                state,
                WalRecord::CompactionAnswer {
                    provider: self.provider.into(),
                    request_id: answer.as_ref().map_or_else(
                        || status.request_id.clone(),
                        |answer| answer.request_id().to_owned(),
                    ),
                    cursor,
                    disposition,
                    rejected,
                    wait: wait.clone(),
                },
            )
            .await?;
            if disposition == CompactionDisposition::Refuse {
                if let Some(StepAnswer::Refuse {
                    code,
                    reason,
                    provider_code,
                    ..
                }) = answer
                {
                    return Ok(StepResult::Terminal(
                        RunFinishReason::Error,
                        Some(refusal(self.provider, code, &reason, provider_code)),
                    ));
                }
            }
            let Some(wait) = wait else {
                return Ok(StepResult::Continue);
            };
            // The provider's bound and the step's cap share one timer, due at whichever
            // comes first. Separate timers would each read the clock on their own, so when
            // the bound and the cap fall due at the same instant, a clock change between
            // those reads could let the bound fire at the cap, and the step would ask the
            // provider again instead of taking the cap's decision. When the timer fires, it
            // counts as the bound only if the bound comes first and the clock has not already
            // reached the cap; otherwise the runner decides from its own estimate.
            let bound = self.now().saturating_add(wait.bound_ms);
            let wake = bound.min(cap);
            let exit = self.with_replies(state, async { tokio::select! {
                biased;
                _ = cancelled(self.control.cancel.as_ref()) => CompactionWaitExit::Cancel,
                _ = cancelled(self.control.stop_requested.as_ref()) => CompactionWaitExit::Stopped,
                _ = async { match &self.control.shutdown_seal { Some(seal) => seal.tripped().await, None => std::future::pending().await } } => CompactionWaitExit::Stopped,
                _ = clock_deadline(self.control.clock.as_ref(), wake) => if bound < cap && self.now() < cap { CompactionWaitExit::Bound } else if self.estimate_fits(estimate, status.context_window) { CompactionWaitExit::Cap } else { CompactionWaitExit::Exceeded },
                _ = async { match self.inbox { Some(inbox) => inbox.wait(&status.request_id).await, None => std::future::pending().await } } => CompactionWaitExit::Ready,
            } }).await?;
            self.exit(state, exit, &wait.reason).await?;
            match exit {
                CompactionWaitExit::Ready | CompactionWaitExit::Bound => continue,
                CompactionWaitExit::Cap => return Ok(StepResult::Continue),
                _ => return Ok(terminal_exit(exit)),
            }
        }
    }
    fn estimate_fits(&self, estimate: Option<u64>, window: Option<u64>) -> bool {
        #[cfg(feature = "test-clock")]
        if let Some(config) = &self.control.test_compaction {
            return config.fits();
        }
        estimate_fits(estimate, window)
    }
}
fn terminal_exit(exit: CompactionWaitExit) -> StepResult {
    match exit {
        CompactionWaitExit::Cancel => StepResult::Terminal(RunFinishReason::Cancelled, None),
        CompactionWaitExit::Exceeded => StepResult::Terminal(
            RunFinishReason::Error,
            Some(error(
                None,
                "compaction wait exceeded; request cannot be shown to fit",
                Some("compaction_wait_exceeded".into()),
                true,
            )),
        ),
        CompactionWaitExit::Stopped | CompactionWaitExit::Interrupted => {
            StepResult::Terminal(RunFinishReason::Interrupted, None)
        }
        _ => StepResult::Continue,
    }
}

#[cfg(test)]
mod reply_tests {
    use super::*;

    #[cfg(feature = "test-clock")]
    #[tokio::test(start_paused = true)]
    async fn compaction_deadline_observes_advance_before_and_after_subscription() {
        let root = broca_test_support::scratch_root().join("compaction-deadline-subscription");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("test-clock");
        for subscribe_first in [false, true] {
            std::fs::write(&path, r#"{"now_ms":0}"#).unwrap();
            let clock = crate::Clock::for_state_root(&root).unwrap();
            let mut deadline = Box::pin(clock_deadline(Some(&clock), 100));
            if subscribe_first {
                assert!(futures::poll!(&mut deadline).is_pending());
            }
            std::fs::write(&path, r#"{"now_ms":99}"#).unwrap();
            clock.refresh().unwrap();
            if subscribe_first {
                assert!(futures::poll!(&mut deadline).is_pending());
            }
            std::fs::write(&path, r#"{"now_ms":100}"#).unwrap();
            clock.refresh().unwrap();
            assert!(futures::poll!(&mut deadline).is_ready());
        }
    }

    use std::sync::atomic::Ordering;

    fn receipt() -> LateReply {
        LateReply {
            answer: StepAnswer::Noop {
                request_id: "old".into(),
                channel2_directive: None,
                bootstrap_complete: None,
            },
            fence: FenceState {
                newest_request_id: "old".into(),
                last_applied_version: None,
                newest_ordinal: None,
                newest_deadline_ms: Some(0),
            },
            raw: vec![],
            cursor: CompactionCursor {
                lineage_id: "lineage".into(),
                after_ordinal: None,
            },
            #[cfg(feature = "test-clock")]
            acknowledge: false,
        }
    }

    #[tokio::test]
    async fn full_late_reply_inbox_counts_every_drop_and_preserves_queued_receipts() {
        let inbox = CompactionReplies::new();
        for _ in 0..8 {
            inbox
                .tx
                .try_send(receipt())
                .unwrap_or_else(|_| panic!("inbox unexpectedly full"));
        }
        for _ in 0..2 {
            inbox.retain(
                async {
                    Ok(StepAnswer::Noop {
                        request_id: "overflow".into(),
                        channel2_directive: None,
                        bootstrap_complete: None,
                    })
                },
                receipt(),
                None,
                super::super::now() + 1000,
            );
        }
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while inbox.dropped.load(Ordering::Relaxed) != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("overflow tasks must finish");
        assert_eq!(inbox.rx.lock().await.len(), 8);
        for _ in 0..8 {
            assert_eq!(inbox.next().await.answer.request_id(), "old");
        }
    }

    #[tokio::test]
    async fn run_teardown_closes_inbox_drops_queued_receipts_and_cancels_retained_calls() {
        struct Dropped(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let inbox = CompactionReplies::new();
        let tx = inbox.tx.clone();
        tx.try_send(receipt())
            .unwrap_or_else(|_| panic!("empty inbox"));
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let guard = Dropped(dropped.clone());
        inbox.retain(
            async move {
                let _guard = guard;
                std::future::pending().await
            },
            receipt(),
            None,
            super::super::now() + 30_000,
        );
        tokio::task::yield_now().await;
        drop(inbox);
        tokio::time::timeout(std::time::Duration::from_secs(2), tx.closed())
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !dropped.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn retained_call_expires_without_receipt_and_task_capacity_is_bounded() {
        let inbox = CompactionReplies::new();
        for _ in 0..8 {
            inbox.retain(
                std::future::pending(),
                receipt(),
                None,
                super::super::now() + 30_000,
            );
        }
        let retained = inbox.tasks.lock().unwrap().len();
        assert_eq!(retained, 8);
        inbox.retain(
            async {
                Ok(StepAnswer::Noop {
                    request_id: "ninth".into(),
                    channel2_directive: None,
                    bootstrap_complete: None,
                })
            },
            receipt(),
            None,
            super::super::now() + 30_000,
        );
        let retained = inbox.tasks.lock().unwrap().len();
        assert_eq!(retained, 8);
        assert_eq!(inbox.dropped.load(Ordering::Relaxed), 1);
        assert!(inbox.rx.lock().await.is_empty());
        drop(inbox);

        let inbox = CompactionReplies::new();
        inbox.retain(std::future::pending(), receipt(), None, 0);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if inbox
                    .tasks
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|task| task.is_finished())
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("retained transport must expire at its cap");
        assert!(inbox.rx.lock().await.is_empty());
    }

    #[tokio::test]
    async fn stale_version_and_malformed_duplicates_never_create_pending_answers() {
        let root = broca_test_support::scratch_root().join("duplicate-discard");
        std::fs::create_dir_all(&root).unwrap();
        let writer = crate::WalHandle::spawn(
            Box::new(
                broca_wal::FileWal::open(root.join("wal"))
                    .unwrap()
                    .with_feature_floor(root.join(broca_wal::FEATURE_FLOOR_FILENAME)),
            ),
            1,
        )
        .unwrap();
        let bind = BindIdentity::new(root, "caller", "duplicate");
        let control = crate::RunControl::default();
        let replies = CompactionReplies::new();
        let runtime = Runtime {
            provider: "provider",
            bind: &bind,
            plane: None,
            inbox: None,
            writer: &writer,
            control: &control,
            replies: &replies,
        };
        let cursor = CompactionCursor {
            lineage_id: "current".into(),
            after_ordinal: Some(0),
        };
        let mut state = CompactionState {
            newest_request_id: Some("old".into()),
            cursor: Some(cursor.clone()),
            applied: Some(super::super::AppliedCompaction {
                compaction_id: "c".into(),
                version: 2,
                range: CompactionRange { from: 0, to: 0 },
                replacement: Arc::new(vec![]),
                sha256: "".into(),
                thinking_prefix_len: 0,
            }),
            ..Default::default()
        };
        for (version, replacement) in [
            (
                2,
                serde_json::json!({"role":"user","content":[{"type":"text","text":"old"}]}),
            ),
            (3, serde_json::json!({"role":"unknown","content":[]})),
        ] {
            let mut reply = receipt();
            reply.fence.newest_deadline_ms = None;
            reply.answer = StepAnswer::CompactionMessage {
                request_id: "old".into(),
                compaction: crate::compaction_provider::answer::CompactionMessage::new(
                    "c",
                    version,
                    crate::compaction_provider::answer::Range::new(0, 0),
                    vec![replacement],
                ),
                coverage: None,
                channel2_directive: None,
                bootstrap_complete: None,
            };
            runtime.discard_reply(&mut state, reply).await.unwrap();
            assert!(!state.pending_answer);
            assert!(!state.pending_request);
            assert_eq!(state.cursor, Some(cursor.clone()));
            assert_eq!(state.applied.as_ref().unwrap().version, 2);
            assert_eq!(
                state.last_not_applied.as_ref().unwrap().disposition,
                CompactionDisposition::SupersededRequest
            );
        }
        let _ = writer.shutdown().await;
    }
}
