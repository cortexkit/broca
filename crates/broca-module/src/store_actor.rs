//! The session actor for a session kept in the new storage (`broca_session_store`).
//!
//! It takes the same commands as today's actor ([`crate::actor::LineageActor`]) and answers with
//! the same outcome types, so the registry and the serve layer can route a session to either.
//! It owns the session's `Session` for the session's whole life and is its only writer. A run
//! writes through a [`broca_session_loop::shared::ActorWriter`], whose requests this actor
//! serves in its own command loop, so the run's appends and the actor's own (a cancel request,
//! later steers and queued sends) take their seqs in one order.
//!
//! What it serves so far:
//! - a plain send that starts a run on an idle session;
//! - a keyed send's retry, answered from the records of the run it started, and a different
//!   payload under a used key, refused naming what differs;
//! - the session's status;
//! - cancelling the active run, now or at its next step boundary;
//! - going away when idle.
//!
//! Every other command, and every send that needs an admission rule not built here yet, is
//! refused by name with nothing written. Today's actor is the reference for each rule still to
//! come; the rules move into shared functions as each one is added here.

use std::sync::Arc;

use broca_core::{RunControl, RunRequest};
use broca_protocol::{BindIdentity, RunId, SubmissionRequest};
use broca_session_loop::parts::Parts;
use broca_session_loop::shared::{ActorWriter, RunLink};
use broca_session_loop::writer::Writer;
use broca_session_store::backend::SessionBackend;
use broca_session_store::idindex::IdKind;
use broca_session_store::record::{CancelPoint, CancelRequest, Record};
use broca_session_store::session::Session;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::actor::{CancelOutcome, LineageCmd, LineageStatus, SendOutcome};
use crate::executor::{RunExecutor, RunKind, StoreRunEnd, StoreRunError, StoreRunJob};

/// The refusal every unserved command and send gets: what it was, and that this storage doesn't
/// serve it yet.
fn unserved(what: &str) -> String {
    format!("{what} is not served yet for a session kept in the new storage")
}

/// A session actor over the new storage.
pub struct StoreActor<B: SessionBackend> {
    key: BindIdentity,
    session: Session<B>,
    executor: Arc<dyn RunExecutor>,
    /// The session's setup parts, lent to the active run and handed back when it ends.
    parts: Option<Box<dyn Parts>>,
    run: Option<ActiveRun>,
}

/// The run the session is running.
struct ActiveRun {
    run_id: RunId,
    /// The actor's end of the run's writer.
    link: RunLink,
    /// Set once the run dropped its writer: it appends nothing more, and its end follows.
    writer_gone: bool,
    cancel: CancellationToken,
    /// The reply of the send that started the run, held until the run's start is durable, so
    /// the sender hears `active` only for a run that exists.
    admitting: Option<oneshot::Sender<SendOutcome>>,
    done: JoinHandle<StoreRunEnd>,
}

/// How a keyed send is answered.
enum Keyed {
    /// The key names a send the session already took: this is the whole answer.
    Answered(SendOutcome),
    /// The key is new: the send starts a run that records it with this identity digest.
    Fresh(String),
}

/// What woke the actor.
enum Event {
    Command(Option<LineageCmd>),
    Request(Option<broca_session_loop::shared::Request>),
    Done(Result<StoreRunEnd, tokio::task::JoinError>),
}

impl<B: SessionBackend + Send + Sync + 'static> StoreActor<B> {
    pub fn new(
        key: BindIdentity,
        session: Session<B>,
        executor: Arc<dyn RunExecutor>,
        parts: Box<dyn Parts>,
    ) -> Self {
        Self {
            key,
            session,
            executor,
            parts: Some(parts),
            run: None,
        }
    }

    /// Serves `commands` until the session goes away: when the channel closes, or when it is
    /// idle at an idle tick. Returns the session, for whoever closes it.
    pub async fn serve(mut self, mut commands: mpsc::UnboundedReceiver<LineageCmd>) -> Session<B> {
        loop {
            let event = match self.run.as_mut() {
                // Until the run's start is durable, commands wait, as they wait in today's actor
                // while it awaits a run's admission: a cancel or a second send can only meet a
                // run that exists.
                Some(active) => tokio::select! {
                    request = active.link.next(), if !active.writer_gone => Event::Request(request),
                    end = &mut active.done => Event::Done(end),
                    command = commands.recv(), if active.admitting.is_none() => Event::Command(command),
                },
                None => Event::Command(commands.recv().await),
            };
            match event {
                Event::Request(Some(request)) => {
                    let active = self
                        .run
                        .as_mut()
                        .expect("a request comes from the active run");
                    active.link.serve(&mut self.session, request).await;
                    self.answer_admission();
                }
                Event::Request(None) => {
                    if let Some(active) = self.run.as_mut() {
                        active.writer_gone = true;
                    }
                }
                Event::Done(end) => self.finish_run(end),
                Event::Command(None) => break,
                Event::Command(Some(command)) => {
                    if self.handle(command).await {
                        break;
                    }
                }
            }
        }
        if let Some(active) = self.run.take() {
            // Nobody is left to send commands: the run is cut, as a module stop would cut it.
            active.cancel.cancel();
            let end = active.done.await;
            self.parts = end.ok().map(|end| end.parts);
        }
        self.session
    }

    /// Handles one command. Returns whether the actor should go away.
    async fn handle(&mut self, command: LineageCmd) -> bool {
        match command {
            LineageCmd::Send {
                session_class,
                retention,
                stop_sequences_named: _,
                conversation_key,
                title,
                run_id,
                request,
                kind,
                key,
                skip_materialization: _,
                delivery,
                plan,
                order,
                reply,
            } => {
                let unserved_part = if plan.is_some() {
                    Some("a send carrying a fetch plan")
                } else if order.is_some() {
                    Some("an ordered delivery")
                } else if delivery != broca_protocol::Delivery::Queue {
                    Some("a steer or interrupt")
                } else if conversation_key.is_some() {
                    // Routing reads it when it picks the run's account, which a store session's
                    // runs don't do yet.
                    Some("a send naming a conversation key")
                } else {
                    None
                };
                if let Some(what) = unserved_part {
                    let _ = reply.send(SendOutcome::Error(unserved(what)));
                    return false;
                }
                // As in today's actor, a running run's routing decision is fixed, and a send naming
                // another is refused before its key is looked at.
                if let Some(fixed) = self.running().map(|run| {
                    run.start
                        .as_ref()
                        .and_then(|start| start.route_decision_id.as_ref())
                }) {
                    if let Some(requested) = &request.route_decision_id {
                        if Some(requested) != fixed {
                            let _ = reply.send(SendOutcome::RouteDecisionFixed);
                            return false;
                        }
                    }
                }
                let mut identity = None;
                if key.is_keyed() {
                    match self
                        .keyed(
                            &run_id,
                            &key,
                            &request,
                            delivery,
                            conversation_key.as_deref(),
                        )
                        .await
                    {
                        Ok(Keyed::Answered(outcome)) => {
                            let _ = reply.send(outcome);
                            return false;
                        }
                        Ok(Keyed::Fresh(digest)) => identity = Some(digest),
                        Err(error) => {
                            let _ = reply.send(SendOutcome::Error(error));
                            return false;
                        }
                    }
                }
                let unserved_part = if self.run.is_some() {
                    Some("a send while a run is active")
                } else if self.running().is_some() {
                    Some("a send while a run is paused")
                } else if retention.is_some() {
                    Some("a send setting retention")
                } else if session_class.is_some() {
                    Some("a send setting a session class")
                } else if kind == RunKind::ResumeAfterAuth {
                    Some("resuming after sign-in")
                } else {
                    refuse_unbuilt_request(&request)
                };
                if let Some(what) = unserved_part {
                    let _ = reply.send(SendOutcome::Error(unserved(what)));
                    return false;
                }
                let send_id = key.send_id().map(str::to_owned);
                self.start_run(run_id, *request, title, send_id, identity, reply);
                false
            }
            LineageCmd::Status { reply } => {
                let status = match &self.run {
                    Some(active) => LineageStatus::Active {
                        run_id: active.run_id.clone(),
                        step: self
                            .session
                            .operations()
                            .run
                            .as_ref()
                            .map_or(0, |run| run.completed_steps),
                    },
                    None => LineageStatus::Idle,
                };
                let _ = reply.send(status);
                false
            }
            LineageCmd::Cancel { run_id, at, reply } => {
                let _ = reply.send(self.cancel(&run_id, at).await);
                false
            }
            LineageCmd::IdleTick => self.run.is_none(),
            other => {
                refuse(other);
                false
            }
        }
    }

    /// Starts a run of `request` on the idle session. The sender's reply waits for the run's
    /// start to be durable, or for its refusal.
    /// `send_id` and `identity` are a keyed send's key and identity digest, which the run's start
    /// records so a retry can be answered from it.
    fn start_run(
        &mut self,
        run_id: RunId,
        request: SubmissionRequest,
        title: Option<String>,
        send_id: Option<String>,
        identity: Option<String>,
        reply: oneshot::Sender<SendOutcome>,
    ) {
        let mut request = request;
        // The executor resolves the send's credential selection; only the result is frozen.
        let auth = request.auth.take();
        let mut req = RunRequest::from_submission(
            request,
            run_id.clone(),
            self.key.clone(),
            broca_protocol::SubmissionId(String::new()),
        );
        // A run started directly consumes no queued send; its input was written now.
        req.submission_id = None;
        req.input_written_at_ms = broca_core::run::epoch_millis();
        req.title = title;
        req.send_id = send_id;
        req.request_identity_digest = identity;
        let cancel = CancellationToken::new();
        let control = RunControl {
            cancel: Some(cancel.clone()),
            hook_plane: self.executor.hook_plane(),
            provider_held_lifecycle: self.executor.provider_held_lifecycle(),
            compaction_plane: self.executor.compaction_plane(),
            ..Default::default()
        };
        let (writer, link) = ActorWriter::new(&self.session, None);
        let parts = self
            .parts
            .take()
            .expect("the session holds its parts while idle");
        let executor = Arc::clone(&self.executor);
        let (out, mut events) = mpsc::channel(256);
        // Nothing subscribes to a store session's events yet; they're drained so the run never
        // waits on a full channel.
        tokio::spawn(async move { while events.recv().await.is_some() {} });
        let job = StoreRunJob {
            req,
            auth,
            control,
            writer,
            parts,
            resume: false,
        };
        let done = tokio::spawn(async move { executor.execute_on_store(job, out).await });
        self.run = Some(ActiveRun {
            run_id,
            link,
            writer_gone: false,
            cancel,
            admitting: Some(reply),
            done,
        });
    }

    /// The session's latest run while it is unfinished and either running here or paused: the
    /// run a new send would have to wait behind.
    fn running(&self) -> Option<&broca_session_store::fold::operations::Run> {
        let run = self.session.operations().run.as_ref()?;
        (run.finished.is_none() && (self.run.is_some() || run.paused.is_some())).then_some(run)
    }

    /// A keyed send, looked up by the run id its key names, as today's actor looks it up: a run
    /// started under the key answers the retry from that run's records when the payload is the
    /// same, and is refused naming what differs when it is not. A key no run used starts a run.
    async fn keyed(
        &self,
        run_id: &RunId,
        key: &crate::actor::SendKey,
        request: &SubmissionRequest,
        delivery: broca_protocol::Delivery,
        conversation_key: Option<&str>,
    ) -> Result<Keyed, String> {
        let digest = broca_core::send_identity_digest_with_conversation_key(
            request,
            delivery,
            conversation_key,
        );
        let find = |kind, id: String| async move {
            self.session
                .find_id(kind, &id)
                .await
                .map_err(|error| format!("{error:?}"))
        };
        let by_run = find(IdKind::RunId, run_id.0.clone()).await?;
        let submission = find(IdKind::SubmissionId, format!("sub-{}", run_id.0)).await?;
        let Some(start) = by_run.iter().find_map(|(_, record)| match record {
            Record::RunStart(start) => Some(start),
            _ => None,
        }) else {
            // Today's actor answers a key a queued send or a steer holds from that send's own
            // records, which this actor can't read yet.
            let mut holders = submission;
            if let Some(send_id) = key.send_id() {
                holders.extend(find(IdKind::SendId, send_id.to_owned()).await?);
            }
            return Ok(if holders.is_empty() {
                Keyed::Fresh(digest)
            } else {
                Keyed::Answered(SendOutcome::Error(unserved(
                    "a retry of a queued or steered send",
                )))
            });
        };
        // A run an ordered delivery started is a different send from this plain one.
        if submission
            .iter()
            .any(|(_, record)| matches!(record, Record::DeliveryOrdered(_)))
        {
            return Ok(Keyed::Answered(SendOutcome::SendIdReuse {
                differing: "delivery",
            }));
        }
        let Some(stored) = start.request_identity_digest.as_deref() else {
            // Nothing can prove the payload is the same send, so the retry neither runs again nor
            // passes as the same one.
            return Ok(Keyed::Answered(SendOutcome::SendIdReuse {
                differing: "request identity because the run's start recorded no digest",
            }));
        };
        if stored != digest {
            let differing = broca_core::admission::send_identity_difference_with_conversation_key(
                stored,
                request,
                delivery,
                conversation_key,
            )
            .unwrap_or("request identity");
            return Ok(Keyed::Answered(SendOutcome::SendIdReuse { differing }));
        }
        let outcome = match self
            .session
            .operations()
            .run
            .as_ref()
            .filter(|run| run.run_id == run_id.0)
        {
            Some(run) => match (run.finished, run.paused) {
                (Some(reason), _) => finished(run_id, reason),
                _ if self.run.is_some() => SendOutcome::Active {
                    run_id: run_id.clone(),
                    delivered: None,
                },
                // Runner-owned pauses are reported; a run paused for sign-in, or unfinished with
                // nothing running it, is driven again by its retry in today's actor.
                (
                    None,
                    Some(
                        reason @ (broca_protocol::PauseReason::Restart
                        | broca_protocol::PauseReason::AccountLimit
                        | broca_protocol::PauseReason::ToolHeld),
                    ),
                ) => SendOutcome::Paused {
                    run_id: run_id.clone(),
                    reason,
                    delivered: None,
                },
                _ => SendOutcome::Error(unserved("driving an unfinished run again by its retry")),
            },
            // An earlier run: a later run started only after this one ended.
            None => match by_run.iter().find_map(|(_, record)| match record {
                Record::RunEnd(end) => Some(end.reason),
                _ => None,
            }) {
                Some(reason) => finished(run_id, reason),
                None => SendOutcome::Error(format!(
                    "the run {} that a recorded send started has no end, yet a later run started",
                    run_id.0
                )),
            },
        };
        Ok(Keyed::Answered(outcome))
    }

    /// Tells the sender its run is active, once the run's start is in the session.
    fn answer_admission(&mut self) {
        let Some(active) = self.run.as_mut() else {
            return;
        };
        let started = self
            .session
            .operations()
            .run
            .as_ref()
            .and_then(|run| run.start.as_ref())
            .is_some_and(|start| start.run_id == active.run_id.0);
        if started {
            if let Some(reply) = active.admitting.take() {
                let _ = reply.send(SendOutcome::Active {
                    run_id: active.run_id.clone(),
                    delivered: None,
                });
            }
        }
    }

    /// The run's task ended: take the parts back, and answer a sender still waiting, whose run
    /// never started.
    fn finish_run(&mut self, end: Result<StoreRunEnd, tokio::task::JoinError>) {
        let Some(active) = self.run.take() else {
            return;
        };
        let (parts, result) = match end {
            Ok(end) => (Some(end.parts), end.result),
            // A run task that panicked took the parts with it. The session can't start
            // another run, and its sends say why.
            Err(error) => (
                None,
                Err(StoreRunError::Admission(broca_core::RunError::Config(
                    format!("the run's task ended without finishing: {error}"),
                ))),
            ),
        };
        self.parts = parts;
        if let Some(reply) = active.admitting {
            let _ = reply.send(match result {
                Ok(_) => SendOutcome::Error("the run ended without writing its start".into()),
                Err(StoreRunError::Admission(error)) => SendOutcome::Error(error.to_string()),
                Err(StoreRunError::Loop(error)) => SendOutcome::Error(format!("{error:?}")),
            });
        } else if let Err(error) = result {
            tracing::error!(
                target: "broca.store_actor",
                session = %self.key.session,
                run_id = %active.run_id.0,
                error = ?error,
                "a run on the new storage stopped without a recorded end",
            );
        }
    }

    /// Cancels the active run `run_id`. The request is durable before the run is told, so a
    /// crash after the reply still leaves the run's cancel in its log.
    async fn cancel(
        &mut self,
        run_id: &RunId,
        at: broca_wal::record::RunCancelAt,
    ) -> CancelOutcome {
        let Some(active) = self.run.as_mut().filter(|active| &active.run_id == run_id) else {
            return CancelOutcome::NotActive;
        };
        let point = match at {
            broca_wal::record::RunCancelAt::Now => CancelPoint::Now,
            broca_wal::record::RunCancelAt::StepBoundary => CancelPoint::StepBoundary,
        };
        let record = Record::CancelRequest(CancelRequest {
            run_id: run_id.0.clone(),
            at: point,
        });
        match Writer::append(&mut self.session, std::slice::from_ref(&record), true).await {
            Ok(seq) => active.link.note_own(seq, &[record]),
            Err(error) => return CancelOutcome::Error(format!("{error:?}")),
        }
        if point == CancelPoint::Now {
            active.cancel.cancel();
        }
        CancelOutcome::Ack
    }
}

fn finished(run_id: &RunId, reason: broca_protocol::RunFinishReason) -> SendOutcome {
    SendOutcome::Finished {
        run_id: run_id.clone(),
        reason,
        delivered: None,
    }
}

/// The request fields a store session doesn't take yet, refused by name before anything is
/// written.
fn refuse_unbuilt_request(request: &SubmissionRequest) -> Option<&'static str> {
    if request.scope != broca_protocol::SendScope::Internal {
        Some("a send whose route names an owner or scope")
    } else if request.structured_output.is_some() {
        Some("structured output")
    } else if request.keep_warm.is_some() {
        Some("keep-warm")
    } else if request.service_tier_level.is_some() {
        Some("a service tier level")
    } else if broca_provider::person_media::references(&request.input)
        .next()
        .is_some()
    {
        Some("a send with media")
    } else {
        None
    }
}

/// Answers a command this actor doesn't serve yet with its refusal, writing nothing.
fn refuse(command: LineageCmd) {
    match command {
        LineageCmd::CutLock { reply, .. } => {
            let (release, _) = oneshot::channel();
            let _ = reply.send(crate::actor::CutLockReply {
                busy: Some(unserved("moving or folding a session")),
                release,
                retired: None,
            });
        }
        LineageCmd::RepairRetention { reply } => {
            let _ = reply.send(Err(unserved("retention repair")));
        }
        LineageCmd::ResumeRestart { reply } => {
            let _ = reply.send(Err(unserved("resuming after a restart")));
        }
        LineageCmd::ResumeHeld { reply } => {
            let _ = reply.send(Err(unserved("a late tool result")));
        }
        LineageCmd::Import { reply, .. } => {
            let _ = reply.send(crate::actor::ImportOutcome::Error(
                broca_core::ImportError::Wal(unserved("an import")),
            ));
        }
        LineageCmd::CancelFrom { reply, .. } => {
            let _ = reply.send(CancelOutcome::Error(unserved(
                "a cancel from a scoped route",
            )));
        }
        LineageCmd::SenderPolicy { reply, .. } => {
            let _ = reply.send(Err(crate::serve::ServeError::Transient(unserved(
                "a sender check",
            ))));
        }
        LineageCmd::Retract { reply, .. } => {
            let _ = reply.send(crate::actor::RetractOutcome::Error(unserved(
                "a retraction",
            )));
        }
        LineageCmd::SessionRefresh { reply, .. } => {
            let _ = reply.send(crate::actor::SessionRefreshOutcome::Error(unserved(
                "session.refresh",
            )));
        }
        LineageCmd::SessionRefreshPolicy { reply, .. } => {
            let _ = reply.send(crate::actor::SessionRefreshPolicyOutcome::Error(unserved(
                "a refresh policy change",
            )));
        }
        LineageCmd::SessionFlushPrefix { reply, .. } => {
            let _ = reply.send(crate::actor::SessionFlushPrefixOutcome::Error(unserved(
                "a prefix flush",
            )));
        }
        LineageCmd::RunState { reply, .. } => {
            let _ = reply.send(Err(unserved("run.status")));
        }
        LineageCmd::Subscribe { reply, .. } => {
            let _ = reply.send(crate::actor::SubscribeOutcome::Error(unserved("subscribe")));
        }
        LineageCmd::Warm { reply, .. } => {
            let _ = reply.send(crate::actor::WarmOutcome::Error(unserved("session.warm")));
        }
        LineageCmd::CompactionChanged { reply } => {
            let _ = reply.send(Err(unserved("a compaction change")));
        }
        LineageCmd::ApplyCompaction { reply, .. } => {
            let _ = reply.send(Err(unserved("applying a compaction view")));
        }
        LineageCmd::CompactionV2 { method, reply, .. } => {
            let _ = reply.send(Err(
                broca_core::compaction_v2::protocol::wire::Refusal::new(
                    "unsupported",
                    unserved(&method),
                    None,
                ),
            ));
        }
        // Today's actor sends these to itself; nothing sends them to this one.
        LineageCmd::WarmFinished { .. } | LineageCmd::RunFinished { .. } => {}
        #[cfg(test)]
        LineageCmd::RetainedAdmissionIdentityState { reply } => {
            let _ = reply.send((0, 0));
        }
        #[cfg(test)]
        LineageCmd::PanicInActor => {}
        LineageCmd::Send { .. }
        | LineageCmd::Status { .. }
        | LineageCmd::Cancel { .. }
        | LineageCmd::IdleTick => unreachable!("served by the actor"),
    }
}

#[cfg(test)]
mod differential;
#[cfg(test)]
mod tests;
