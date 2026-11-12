//! Rebuild the request before a recorded tool turn, never the next request after its results.

use std::sync::Arc;

use broca_core::RecordedRequest;
use broca_protocol::{BindIdentity, RunId, StepId};
use broca_session_loop::{parts::Parts, writer::Writer};
use broca_session_store::fold::{conversation::Conversation, operations::Operations};
use broca_session_store::record::Record;
use broca_session_store::record::Stream;
use broca_session_store::session::{Carry, EnrollmentOverlay, SessionError, StoredEntry};

/// Build the request of run `run_id`'s last recorded attempt, or of its last attempt for step
/// `step` when one is named, from a snapshot: the actor and the running loop must keep the
/// complete conversation. `writer` is the session as it stands now; `records` must reach from
/// the run's start through that attempt.
pub(crate) async fn request<W: Writer>(
    writer: &W,
    records: &[(u64, Record)],
    parts: &dyn Parts,
    bind: &BindIdentity,
    run_id: &str,
    step: Option<u64>,
) -> Result<RecordedRequest, String> {
    let (attempt_at, attempt) = records
        .iter()
        .enumerate()
        .rev()
        .find_map(|(at, (_, record))| match record {
            Record::Attempt(attempt)
                if attempt.run_id == run_id && step.is_none_or(|step| attempt.step_id == step) =>
            {
                Some((at, attempt))
            }
            _ => None,
        })
        .ok_or("the run's request has no recorded attempt")?;
    let step = attempt.step_id;
    let (start_at, mid) = records[..attempt_at]
        .iter()
        .enumerate()
        .rev()
        .find_map(|(at, (_, record))| match record {
            Record::StepStart(start) if start.run_id == run_id && start.step_id == step => {
                start.mid.as_deref().map(|mid| (at, mid))
            }
            _ => None,
        })
        .ok_or("the recorded step has no assistant message id")?;
    let conversation = writer.conversation();
    let ordinal = conversation
        .open_entry()
        .filter(|(_, _, entry)| entry.facts.as_ref().is_some_and(|facts| facts.mid == mid))
        .map(|(ordinal, _, _)| ordinal)
        .or_else(|| {
            conversation
                .entries
                .iter()
                .position(|entry| entry.facts.as_ref().is_some_and(|facts| facts.mid == mid))
                .map(|at| conversation.window_start() + at as u64)
        })
        .ok_or("the recorded step's tool turn is outside the conversation window")?;
    let run_at = records[..=attempt_at]
        .iter()
        .rposition(
            |(_, record)| matches!(record, Record::RunStart(start) if start.run_id == run_id),
        )
        .ok_or("the recorded run's start is outside the readable history")?;
    let mut prefix = Operations::default();
    for (seq, record) in &records[run_at..=attempt_at] {
        if record.stream() == Stream::Ledger {
            prefix
                .apply_at(Some(*seq), record)
                .map_err(|error| format!("the recorded step's state: {error:?}"))?;
        }
    }
    let mut operations = writer.operations().clone();
    operations.run = prefix.run;
    let before = Before {
        writer,
        conversation: conversation
            .before_entry(ordinal)
            .ok_or("the recorded step is outside the window")?,
        operations,
    };
    let run = before
        .operations
        .run
        .as_ref()
        .ok_or("the recorded step has no run")?;
    let start = run.start.as_ref().ok_or("the recorded run has no start")?;
    let config = super::warm::run_config(run, start, parts)
        .map_err(|error| format!("the recorded run's config: {error:?}"))?;
    let mut opts = broca_session_loop::run::offline_media_request(&before, parts, bind, step)
        .await
        .map_err(|error| format!("the recorded step's request: {error:?}"))?;
    let cap = records[start_at..attempt_at]
        .iter()
        .rev()
        .find_map(|(_, record)| match record {
            Record::OutputCapLowered(cap) if cap.step_id == step => Some(cap),
            _ => None,
        });
    if let Some(context) = opts.step_context.as_mut() {
        context.max_output_tokens = cap.map(|cap| cap.max_output_tokens);
    }
    let route = records[attempt_at..]
        .iter()
        .find_map(|(_, record)| match record {
            Record::StepEnd(end) if end.run_id == run_id && end.step_id == step => {
                end.fallback.as_ref()
            }
            _ => None,
        })
        .and_then(|fallback| fallback.get("route"))
        .map(|route| serde_json::from_value::<broca_wal::StepRoute>(route.clone()))
        .transpose()
        .map_err(|error| format!("the recorded step's route: {error}"))?;
    if let Some(route) = &route {
        opts.target_model = Some(route.model.clone());
    }
    Ok(RecordedRequest {
        run_id: RunId(run.run_id.clone()),
        step_id: StepId(step),
        config,
        opts,
        request_body_sha256: attempt
            .request_body_sha256
            .0
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
        route,
    })
}

/// The request `session` last sent for run `run_id` (for its step `step`, when one is named),
/// rebuilt from the session's records: its open log when that holds the run's start and the
/// attempt, else its whole history read through `readers`, the backend of the root holding its
/// sealed logs. Writes nothing.
pub(crate) async fn recorded_request<B, R>(
    session: &broca_session_store::session::Session<B>,
    readers: Option<R>,
    parts: &dyn Parts,
    bind: &BindIdentity,
    run_id: &str,
    step: Option<u64>,
) -> Result<RecordedRequest, String>
where
    B: broca_session_store::backend::SessionBackend,
    R: broca_session_store::backend::SessionBackend,
{
    let mut records = session
        .open_log_records()
        .await
        .map_err(|error| format!("read the recorded request: {error:?}"))?;
    let attempted = |records: &[(u64, Record)]| {
        records.iter().any(|(_, record)| {
            matches!(record, Record::Attempt(attempt)
                if attempt.run_id == run_id && step.is_none_or(|step| attempt.step_id == step))
        })
    };
    if !attempted(&records)
        || !records
            .iter()
            .any(|(_, record)| matches!(record, Record::RunStart(start) if start.run_id == run_id))
    {
        let readers = readers.ok_or("the recorded request's sealed history cannot be read")?;
        records = session
            .stored_records(readers)
            .records_between(1, session.next_seq().saturating_sub(1))
            .await
            .map_err(|error| format!("read the recorded request's history: {error:?}"))?;
    }
    request(session, &records, parts, bind, run_id, step).await
}

impl<B: broca_session_store::backend::SessionBackend + Send + Sync + 'static> super::StoreActor<B> {
    /// Called while the first attempt is finishing, before that attempt is appended. A
    /// diagnostic read needs only the newest attempted run and the setup recorded for it.
    pub(super) async fn cache_predecessor(
        &self,
    ) -> Result<Option<broca_core::provider::CachePredecessor>, String> {
        let mut run_id = None;
        let mut records = self
            .session
            .ledger_tail_until(|record| {
                if let Record::Attempt(attempt) = record {
                    run_id.get_or_insert_with(|| attempt.run_id.clone());
                }
                matches!(record, Record::RunStart(start) if Some(&start.run_id) == run_id.as_ref())
            })
            .await
            .map_err(|error| format!("read the cache predecessor: {error:?}"))?;
        records.reverse();
        predecessor(&self.session, &records, &self.parts, &self.key).await
    }

    pub(super) async fn recorded_warm_request(&self) -> Result<RecordedRequest, String> {
        let run_id = self
            .session
            .operations
            .run
            .as_ref()
            .ok_or("no recorded run")?
            .run_id
            .clone();
        recorded_request(
            &self.session,
            self.readers.as_ref().map(|readers| readers()),
            &self.parts,
            &self.key,
            &run_id,
            None,
        )
        .await
    }
}

async fn predecessor<W: Writer>(
    writer: &W,
    records: &[(u64, Record)],
    parts: &dyn Parts,
    bind: &BindIdentity,
) -> Result<Option<broca_core::provider::CachePredecessor>, String> {
    let Some((seq, attempt)) = records.iter().rev().find_map(|(seq, record)| match record {
        Record::Attempt(attempt) => Some((*seq, attempt)),
        _ => None,
    }) else {
        return Ok(None);
    };
    let start = records
        .iter()
        .position(|(_, record)| {
            matches!(record,
        Record::RunStart(start) if start.run_id == attempt.run_id)
        })
        .ok_or("the cache predecessor's run start is outside readable history")?;
    let mut prefix = Operations::default();
    for (at, record) in &records[start..] {
        if *at > seq {
            break;
        }
        prefix
            .apply_at(Some(*at), record)
            .map_err(|error| format!("the cache predecessor's state: {error:?}"))?;
    }
    let mut operations = writer.operations().clone();
    operations.run = prefix.run;
    let snapshot = Before {
        writer,
        conversation: writer.conversation().clone(),
        operations,
    };
    let request = request(
        &snapshot,
        &records[start..],
        parts,
        bind,
        &attempt.run_id,
        None,
    )
    .await?;
    Ok(Some(broca_core::provider::CachePredecessor {
        request,
        sent_at_ms: attempt.sent_at_ms,
        account: attempt.served_account_ref.clone(),
        locator: format!("{}:{seq}", writer.session_id()),
        view: None,
    }))
}

/// Share the run's media delivery with a warm that rebuilds one recorded request: both render
/// from the same session index, and neither changes it.
pub(super) fn media(
    wait: &mut Option<crate::store_media::StoreRunMediaWait>,
) -> Result<Arc<crate::store_media::LazyRenderer>, String> {
    let original = wait
        .take()
        .ok_or("the resumed run has no media readiness wait")?;
    let (live, live_wait) = tokio::sync::oneshot::channel();
    let (recorded, recorded_wait) = tokio::sync::oneshot::channel();
    *wait = Some(live_wait);
    tokio::spawn(async move {
        if let Ok(media) = original.await {
            let _ = recorded.send(media.clone());
            let _ = live.send(media);
        }
    });
    Ok(crate::store_media::LazyRenderer::new(
        recorded_wait,
        Box::new(|media| Ok(media.renderer())),
    ))
}

/// Offline rendering reads the snapshot's conversation and the session's durable lookups. No
/// write is permitted through this adapter, including a compaction seal.
struct Before<'a, W> {
    writer: &'a W,
    conversation: Conversation,
    operations: Operations,
}

impl<W: Writer> Writer for Before<'_, W> {
    async fn append(&mut self, _: &[Record], _: bool) -> Result<u64, SessionError> {
        Err(SessionError::OwnerGone)
    }
    async fn send_records(&mut self, _: &str) -> Result<Vec<Record>, SessionError> {
        Err(SessionError::OwnerGone)
    }
    async fn append_with(
        &mut self,
        _: &mut (dyn for<'s> FnMut(&'s Conversation, &'s Operations, u64) -> Vec<Record> + Send),
        _: bool,
    ) -> Result<Vec<Record>, SessionError> {
        Err(SessionError::OwnerGone)
    }
    fn conversation(&self) -> &Conversation {
        &self.conversation
    }
    fn operations(&self) -> &Operations {
        &self.operations
    }
    fn session_id(&self) -> String {
        self.writer.session_id()
    }
    fn next_seq(&self) -> u64 {
        self.writer.next_seq()
    }
    fn message_id_prefix(&self) -> String {
        self.writer.message_id_prefix()
    }
    fn retired_tag_floor(&self) -> u64 {
        self.writer.retired_tag_floor()
    }
    async fn open_log_records(&self) -> Result<Vec<(u64, Record)>, SessionError> {
        self.writer.open_log_records().await
    }
    async fn run_start(
        &self,
        run_id: &str,
    ) -> Result<Option<broca_session_store::record::RunStart>, SessionError> {
        self.writer.run_start(run_id).await
    }
    async fn tool_intent(
        &self,
        key: &str,
    ) -> Result<Option<broca_session_store::record::ToolIntent>, SessionError> {
        self.writer.tool_intent(key).await
    }
    async fn enrollment(&self) -> Result<Option<EnrollmentOverlay>, SessionError> {
        self.writer.enrollment().await
    }
    async fn put_enrollment(
        &mut self,
        _: &broca_session_store::record::EnrolledEntries,
    ) -> Result<broca_session_store::record::EnrollmentOverlayRef, SessionError> {
        Err(SessionError::OwnerGone)
    }
    async fn seal_at_entry(&mut self, _: u64, _: &Carry<'_>, _: u64) -> Result<(), SessionError> {
        Err(SessionError::OwnerGone)
    }
    async fn seal_ledger(&mut self, _: &Carry<'_>, _: u64) -> Result<(), SessionError> {
        Err(SessionError::OwnerGone)
    }
    async fn entry_by_mid(&self, mid: &str) -> Result<Option<StoredEntry>, SessionError> {
        self.writer.entry_by_mid(mid).await
    }
    async fn entries_by_mid(&self, mids: &[String]) -> Result<Vec<StoredEntry>, SessionError> {
        self.writer.entries_by_mid(mids).await
    }
}
