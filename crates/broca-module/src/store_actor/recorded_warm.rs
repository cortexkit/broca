//! Rebuild the request before a recorded tool turn, never the next request after its results.

use broca_core::RecordedRequest;
use broca_protocol::BindIdentity;
use broca_session_loop::recorded_request::rebuild;
pub(crate) use broca_session_loop::recorded_request::Through;
use broca_session_loop::{parts::Parts, writer::Writer};
use broca_session_store::record::Record;
use std::sync::Arc;

/// Build the request of run `run_id`'s last recorded attempt, or of its last attempt for step
/// `step` when one is named, from a snapshot: the actor and the running loop must keep the
/// complete conversation. `records` must contain the session's history through the attempt.
#[cfg(test)]
pub(crate) async fn request<W: Writer>(
    writer: &W,
    records: &[(u64, Record)],
    parts: &dyn Parts,
    bind: &BindIdentity,
    run_id: &str,
    step: Option<u64>,
) -> Result<RecordedRequest, String> {
    rebuild(writer, records, parts, bind, run_id, step, Through::Request).await
}

pub(crate) struct RequestReplay {
    pub request: RecordedRequest,
    pub media: Option<MediaReplay>,
}

pub(crate) struct MediaReplay {
    /// The media renderer sees the index before the transport's attempt is recorded.
    pub before_attempt: u64,
    pub render_tags: bool,
}

/// Build the request step `step` of run `run_id` sent, as the session stood when it sent it, for
/// a step that may be long past: the views it renders with and the session's other state are
/// folded from `records`, which must be the session's whole history through the step's attempt,
/// not taken from the session as it stands now. Only the conversation comes from `writer`, cut
/// before the step's tool turn. A view applied since, or a seal since that moved the step's
/// records out of the open log, therefore doesn't change the rebuild.
pub(crate) async fn request_as_of<W: Writer>(
    writer: &W,
    records: &[(u64, Record)],
    parts: &dyn Parts,
    bind: &BindIdentity,
    run_id: &str,
    step: u64,
) -> Result<RecordedRequest, String> {
    rebuild(
        writer,
        records,
        parts,
        bind,
        run_id,
        Some(step),
        Through::Request,
    )
    .await
}

/// The request `session` last sent for run `run_id` (for its step `step`, when one is named),
/// rebuilt from the session's records: its open log when that holds the run's start and the
/// attempt, else its whole history read through `readers`, the backend of the root holding its
/// sealed logs. With [`Through::Answer`], the step's reply follows the request. Writes nothing.
pub(crate) async fn recorded_request<B, R>(
    session: &broca_session_store::session::Session<B>,
    readers: Option<R>,
    parts: &dyn Parts,
    bind: &BindIdentity,
    run_id: &str,
    step: Option<u64>,
    through: Through,
) -> Result<RequestReplay, String>
where
    B: broca_session_store::backend::SessionBackend,
    R: broca_session_store::backend::SessionBackend,
{
    let mut records = session
        .open_log_records()
        .await
        .map_err(|error| format!("read the recorded request: {error:?}"))?;
    let attempt = records.iter().rev().find_map(|(seq, record)| match record {
        Record::Attempt(attempt)
            if broca_session_loop::writer::is_cut_attempt(record)
                && attempt.run_id == run_id
                && step.is_none_or(|step| attempt.step_id == step) =>
        {
            Some((*seq, attempt.step_id))
        }
        _ => None,
    });
    let start = records
        .iter()
        .rev()
        .find_map(|(_, record)| match record {
            Record::RunStart(start) if start.run_id == run_id => Some(start.as_ref().clone()),
            _ => None,
        })
        .or_else(|| {
            session
                .operations
                .run
                .as_ref()
                .filter(|run| run.run_id == run_id)
                .and_then(|run| run.start.clone())
        });
    let start = if start.is_none() && attempt.is_some() {
        Writer::run_start(session, run_id)
            .await
            .map_err(|error| format!("read the open request's run start: {error:?}"))?
    } else {
        start
    };
    let mut checkpoint = None;
    // A compaction seal between StepStart and Attempt leaves only the attempt in the tail.
    let open_step = attempt.filter(|(seq, step)| records.iter().any(|(at, record)| at < seq && matches!(record, Record::StepStart(start) if start.run_id == run_id && start.step_id == *step && start.mid.is_some())));
    if let Some((seq, _)) = open_step {
        if records.first().is_none_or(|(seq, _)| *seq != 1) {
            checkpoint = Some(Box::new(
                Box::pin(session.from_log_holding(seq, seq))
                    .await
                    .map_err(|error| format!("read the open request's checkpoint: {error:?}"))?
                    .checkpoint,
            ));
        }
    } else if open_step.is_none()
        || start.is_none()
        || records.first().is_none_or(|(seq, _)| *seq != 1)
    {
        let readers = readers.ok_or("the recorded request's sealed history cannot be read")?;
        records = session
            .stored_records(readers)
            .records_between(1, session.next_seq().saturating_sub(1))
            .await
            .map_err(|error| format!("read the recorded request's history: {error:?}"))?;
    }
    let request = Box::pin(broca_session_loop::recorded_request::rebuild_at(
        session, &records, parts, bind, run_id, step, through, checkpoint,
    ))
    .await
    .map_err(|error| error.to_string())?;
    let media = if request
        .opts
        .prompt
        .iter()
        .any(|message| !broca_session_store::session::message_media(message).is_empty())
    {
        let before_attempt = records
            .iter()
            .rev()
            .find_map(|(seq, record)| match record {
                Record::Attempt(attempt)
                    if broca_session_loop::writer::is_cut_attempt(record)
                        && attempt.run_id == run_id
                        && attempt.step_id == request.step_id.0 =>
                {
                    Some(seq.saturating_sub(1))
                }
                _ => None,
            })
            .ok_or("the recorded media request has no attempt")?;
        let start = start
            .as_ref()
            .or_else(|| {
                records.iter().find_map(|(_, record)| match record {
                    Record::RunStart(start) if start.run_id == run_id => Some(start.as_ref()),
                    _ => None,
                })
            })
            .ok_or("the recorded media run has no start")?;
        let render_tags = request.config.render_tags
            || broca_session_loop::run::frozen_manifest(start, parts)
                .map_err(|error| format!("the recorded run's plan: {error:?}"))?
                .as_deref()
                .is_some_and(broca_core::tags::plan_renders_tags);
        Some(MediaReplay {
            before_attempt,
            render_tags,
        })
    } else {
        None
    };
    Ok(RequestReplay { request, media })
}

impl<B: broca_session_store::backend::SessionBackend + Send + Sync + 'static> super::StoreActor<B> {
    /// Called before the newest attempt is appended. Locate the preceding attempt first, then
    /// rebuild it at that cut rather than inheriting the active run's plan.
    pub(super) async fn cache_predecessor(
        &self,
    ) -> Result<Option<broca_core::provider::CachePredecessor>, String> {
        let tail = self
            .session
            .ledger_tail_until(broca_session_loop::writer::is_cut_attempt)
            .await
            .map_err(|error| format!("read the cache predecessor: {error:?}"))?;
        let Some((seq, attempt)) = tail.iter().find_map(|(seq, record)| match record {
            Record::Attempt(attempt) if broca_session_loop::writer::is_cut_attempt(record) => {
                Some((*seq, attempt))
            }
            _ => None,
        }) else {
            return Ok(None);
        };
        let replay = recorded_request(
            &self.session,
            self.readers.as_ref().map(|readers| readers()),
            &self.parts,
            &self.key,
            &attempt.run_id,
            Some(attempt.step_id),
            Through::Request,
        )
        .await?;
        Ok(Some(broca_core::provider::CachePredecessor {
            request: replay.request,
            sent_at_ms: attempt.sent_at_ms,
            account: attempt.served_account_ref.clone(),
            locator: format!("{}:{seq}", self.session.session_id()),
            view: None,
        }))
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
            Through::Request,
        )
        .await
        .map(|replay| replay.request)
    }
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
