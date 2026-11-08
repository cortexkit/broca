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
//! - a send while the latest run is running or paused, queued durably and started when that run
//!   ends, and a queued send's retry;
//! - a steer into the running run, placed at its next step boundary;
//! - retracting a queued send before it runs;
//! - a run paused for sign-in, resumed by a retry of its own send;
//! - the session's status;
//! - cancelling the active run, now or at its next step boundary;
//! - on opening the session, its unfinished run: a running one is ended `Interrupted`, since
//!   only a process that ended without stopping it leaves one, and with no unfinished run the
//!   queue drains;
//! - going away when idle: in a module, through the registry's close, so a command racing the
//!   close is answered `Reopen` and re-routed;
//! - a module stop: the running run is ended `Interrupted` with today's shutdown seal, never
//!   cancelled, since a cancel records a decision no caller made;
//! - a head's file-source import into the empty session, and installing its compaction
//!   provider's first view before the session's first step (see `store_import`);
//! - a plan's `pre_user` input transforms, on a send that starts a run, a steer and a queued
//!   send, as today's actor runs them; a queued send's run reuses what they did when it
//!   drains;
//! - a send with an ordered delivery list, from the owner or a module the plan names, queued
//!   durably for the running run's boundaries, ending the undelivered send it supersedes, and
//!   starting a run on an idle session when its list names `idle_start` (see `ordered`).
//! - a send's attachments, staged from its uploads and stored as the session's own objects
//!   before the record that names them: a send's run start, its queued record or its steer;
//! - `session.warm` on an idle session, re-sending the request its loop would send next (see
//!   `warm`).
//!
//! Every other command, and every send that needs an admission rule not built here yet, is
//! refused by name with nothing written. Today's actor is the reference for each rule still to
//! come; the rules move into shared functions as each one is added here.

use std::sync::Arc;

use broca_core::{RunControl, RunRequest};
use broca_protocol::{BindIdentity, RunId, SubmissionRequest};
use broca_session_loop::parts::{Parts, SharedParts};
use broca_session_loop::queue;
use broca_session_loop::shared::{ActorWriter, RunLink};
use broca_session_loop::writer::Writer;
use broca_session_store::backend::SessionBackend;
use broca_session_store::idindex::IdKind;
use broca_session_store::record::{
    Billed, CancelPoint, CancelRequest, QueuedSend, Record, Retraction,
};
use broca_session_store::session::{Session, SessionError};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::actor::{CancelOutcome, LineageCmd, LineageStatus, RetractOutcome, SendOutcome};
use crate::executor::{
    RunExecutor, RunKind, StoreRunEnd, StoreRunError, StoreRunJob, StoreRunMode,
};

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
    /// The setup parts store. The actor stores a queued send's tool list in it while the active
    /// run holds its own handle.
    parts: SharedParts,
    /// Where setup parts are stored durably before any record names them: the deployment's
    /// setup store. `None` keeps them in memory only, as tests do.
    storage: Option<broca_session_loop::durable::SetupParts<B>>,
    run: Option<ActiveRun>,
    /// Set once the module stops the session: no queued send starts after it.
    stopping: bool,
    /// Where the session's spend facts are written. Without it, a run that ends stays owed
    /// until an actor that has it opens the session.
    billing: Option<crate::store_billing::StoreBilling>,
    /// The session's compaction state, kept for the actor's whole life and handed to every run,
    /// as today's actor keeps it. A run restores it from the session's records the first time;
    /// after that it carries what holds only for this actor's life, such as whether the
    /// compaction provider was sent Setup yet.
    compaction_session: Arc<broca_core::compaction_v2::session::Session>,
    /// `session.apply_view` commands waiting for the compaction state, which a run holds while
    /// it writes through this actor (see `apply_view`).
    pending_views: Vec<(
        crate::store_import::ApplyViewCommand,
        oneshot::Sender<crate::store_import::StoreOutcome>,
    )>,
    /// The module's stop, once the actor serves in a module: work a send waits on, such as its
    /// input transforms, gives up when it fires.
    seal: CancellationToken,
    /// The session's subscriptions, once the actor can read its records apart from the writer
    /// (see [`StoreActor::with_readers`]); without it `session.subscribe` is refused by name.
    fanout: Option<crate::store_fanout::StoreFanout<B>>,
    /// Where finished runs' facts are kept, shared with the module's reads (see `with_settled`).
    settled: Option<Arc<crate::store_status::SettledRuns>>,
    /// Where a send's attachments are read from: the uploads its `content` names, committed
    /// beside the session. Without it a send attaching any is refused, as today's actor refuses
    /// one when upload staging is unavailable.
    uploads: Option<broca_core::media_uploads::Uploads>,
    /// The media of the send whose run is starting, held until the run's start that places its
    /// messages is appended, which stores them first (see `HeldInputMedia`).
    held: broca_session_loop::HeldInputMedia,
    /// The session's media fold, built when its first run starts and kept for the actor's life,
    /// so each run renders its media from where the last one left off (see `store_media`).
    media_fold: Option<crate::store_media::MediaFold>,
    /// Handles on the session's storage that only read, which a run reads its media objects
    /// through while the actor writes. Without them a run that renders media refuses.
    readers: Option<Arc<dyn Fn() -> B + Send + Sync>>,
    /// The media fold being built from the session's records, off the actor, which keeps
    /// answering commands meanwhile.
    media_build: Option<JoinHandle<Box<crate::store_media::MediaFold>>>,
    /// The run waiting for the media fold, which gets its media once the fold is ready.
    media_waiting: Option<oneshot::Sender<crate::store_media::StoreRunMedia>>,
    /// Holds each media fold build until notified, so a test can act while one is building.
    #[cfg(test)]
    media_build_gate: Option<Arc<tokio::sync::Notify>>,
    /// The module's registry, once the actor serves in one: what a warm reads the session's
    /// cache policy from. Without it no warm is supported.
    registry: Option<crate::registry::RegistryHandle>,
    /// The warm the executor is sending, if any.
    warm: Option<warm::PendingStoreWarm>,
}

/// How an actor lives in the module's registry. Tests serve an actor without one.
pub struct Lifecycle {
    pub registry: crate::registry::RegistryHandle,
    /// The generation of the registry slot this actor serves.
    pub gen: u64,
    /// The session lease, held for the actor's whole life and released only after its session
    /// is closed, so no other process opens a writer beside it.
    pub lease: Box<dyn std::any::Any + Send>,
    /// How long the session waits idle before it closes.
    pub idle_ttl: std::time::Duration,
    /// The health gauge of whether the session is running a run, shared with its handle.
    pub run_in_flight: Arc<std::sync::atomic::AtomicU64>,
}

/// Where the actor's commands arrive: the registry's bounded channel in a module, an unbounded
/// one in tests.
pub enum Commands {
    Bounded(mpsc::Receiver<LineageCmd>),
    Unbounded(mpsc::UnboundedReceiver<LineageCmd>),
}

impl Commands {
    async fn recv(&mut self) -> Option<LineageCmd> {
        match self {
            Self::Bounded(commands) => commands.recv().await,
            Self::Unbounded(commands) => commands.recv().await,
        }
    }

    fn try_recv(&mut self) -> Option<LineageCmd> {
        match self {
            Self::Bounded(commands) => commands.try_recv().ok(),
            Self::Unbounded(commands) => commands.try_recv().ok(),
        }
    }
}

impl From<mpsc::Receiver<LineageCmd>> for Commands {
    fn from(commands: mpsc::Receiver<LineageCmd>) -> Self {
        Self::Bounded(commands)
    }
}

impl From<mpsc::UnboundedReceiver<LineageCmd>> for Commands {
    fn from(commands: mpsc::UnboundedReceiver<LineageCmd>) -> Self {
        Self::Unbounded(commands)
    }
}

/// Why the actor stopped serving commands.
enum Exit {
    /// Every sender is gone.
    Closed,
    /// The session went idle: at its idle deadline, or on an idle tick.
    Idle,
    /// The module is stopping.
    Stop,
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
    /// the sender hears `active` only for a run that exists. A run drained from the queue has no
    /// sender waiting.
    admitting: Option<oneshot::Sender<SendOutcome>>,
    /// Whether the run's start is in the session yet. Until it is, commands wait.
    started: bool,
    /// The queued send the run started from, when it drained from the queue.
    submission: Option<broca_protocol::SubmissionId>,
    done: JoinHandle<StoreRunEnd>,
    /// The run's in-flight model step, which the run loop keeps current and `run.status` serves
    /// for the active run, as today's actor serves its run's.
    step: broca_core::provider::LiveStepProgress,
    /// The media the run's tool calls captured, held until the record naming them is appended.
    tool_media: Arc<broca_session_loop::PendingMedia>,
}

/// A run to start: the send's request and what the run's start records about the send.
struct Start {
    run_id: RunId,
    request: SubmissionRequest,
    title: Option<String>,
    /// A keyed send's key and identity digest, which the run's start records so a retry can be
    /// answered from it.
    send_id: Option<String>,
    identity: Option<String>,
    /// Who sent the input, which the run's start records: today's actor names the request's
    /// principal.
    sent_by: Option<broca_protocol::BindPrincipal>,
    /// The scope the run's start records, decided by today's rule (`broca_core::decide_scope`)
    /// against the session's owner summary: the scope a send freezes, adopts or repeats.
    scope: Option<broca_protocol::SessionScope>,
    /// The fetch plan's manifest the run starts under: the one the session's first run froze,
    /// or, for that first run, the one its send's plan fetched. Its start records it.
    manifest: Option<Arc<broca_wal::PlanManifest>>,
    /// The session's label its start records: the one the session's first run froze, or, for
    /// that first run, the send's.
    session_class: Option<String>,
    /// What happened to the run's input before the run.
    prepared: Prepared,
}

/// What happened to a run's input before the run, which the run's control hands the loop, as
/// today's actor hands it to today's loop.
#[derive(Default)]
struct Prepared {
    /// Whether the send named no tools of its own, before a plan filled them in. The run's
    /// start records it.
    tools_were_omitted: bool,
    /// When a fresh run's input was accepted: the time its `pre_user` transforms were told, and
    /// the one its message keeps. `None` takes the time the run starts.
    at_ms: Option<u64>,
    /// What the input transforms did to the run's input, which its start records.
    hooks: Option<Box<broca_wal::Hooks>>,
    /// The error the run ends with before any model call, when a transform refused its input.
    pre_model_error: Option<broca_protocol::ProviderError>,
    /// The pending plan refresh the run applies by freezing its manifest, which its start's
    /// batch records.
    applies_refresh: Option<broca_protocol::session_change::AppliedChange>,
}

/// What a run drained from the queue starts from.
struct Drained {
    submission: broca_protocol::SubmissionId,
    /// When the queued send's message was accepted, which the message keeps.
    written_at_ms: Option<u64>,
    /// What the send's input transforms did when it was queued, as its record kept it.
    hooks: Option<Box<broca_wal::Hooks>>,
    /// Where the queued record stored the send's media, which the run's input points at.
    media: broca_session_loop::HeldInputMedia,
}

/// How a keyed send is answered.
enum Keyed {
    /// The key names a send the session already took: this is the whole answer.
    Answered(SendOutcome),
    /// The key is new: the send starts a run that records it with this identity digest.
    Fresh(String),
    /// The key names the latest run, paused for sign-in, and the payload is the same: as in
    /// today's actor, the retry resumes that run.
    Resume,
}

/// What woke the actor.
enum Event {
    Command(Option<LineageCmd>),
    Request(Option<broca_session_loop::shared::Request>),
    Done(Result<StoreRunEnd, tokio::task::JoinError>),
    MediaBuilt(Result<Box<crate::store_media::MediaFold>, tokio::task::JoinError>),
    /// A warm's task ended.
    Warmed(
        Result<Result<broca_core::WarmSent, crate::executor::WarmError>, tokio::task::JoinError>,
    ),
}

/// The media fold build's end, or never when none is building.
async fn media_built(
    build: &mut Option<JoinHandle<Box<crate::store_media::MediaFold>>>,
) -> Result<Box<crate::store_media::MediaFold>, tokio::task::JoinError> {
    match build {
        Some(build) => build.await,
        None => std::future::pending().await,
    }
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
            parts: SharedParts::new(parts),
            storage: None,
            run: None,
            stopping: false,
            billing: None,
            compaction_session: Arc::default(),
            pending_views: Vec::new(),
            seal: CancellationToken::new(),
            fanout: None,
            settled: None,
            uploads: None,
            held: broca_session_loop::HeldInputMedia::default(),
            media_fold: None,
            readers: None,
            media_build: None,
            media_waiting: None,
            #[cfg(test)]
            media_build_gate: None,
            registry: None,
            warm: None,
        }
    }

    /// The actor reads a send's attachments from `uploads`.
    pub fn with_uploads(mut self, uploads: broca_core::media_uploads::Uploads) -> Self {
        self.uploads = Some(uploads);
        self
    }

    /// The actor serves `session.subscribe`, catching subscribers up over backends `readers`
    /// makes: handles on the session's storage that only read.
    pub fn with_readers(mut self, readers: Arc<dyn Fn() -> B + Send + Sync>) -> Self {
        self.readers = Some(Arc::clone(&readers));
        self.fanout = Some(crate::store_fanout::StoreFanout::new(
            &mut self.session,
            readers,
        ));
        self
    }

    /// Holds each build of the session's media fold until `gate` is notified.
    #[cfg(test)]
    pub(crate) fn with_media_build_gate(mut self, gate: Arc<tokio::sync::Notify>) -> Self {
        self.media_build_gate = Some(gate);
        self
    }

    /// [`StoreActor::with_readers`], with each seeding of the live projection held until `gate`
    /// is notified.
    #[cfg(test)]
    pub(crate) fn with_readers_held(
        mut self,
        readers: Arc<dyn Fn() -> B + Send + Sync>,
        gate: Arc<tokio::sync::Notify>,
    ) -> Self {
        self.readers = Some(Arc::clone(&readers));
        self.fanout = Some(crate::store_fanout::StoreFanout::held(
            &mut self.session,
            readers,
            gate,
        ));
        self
    }

    /// Attaches a subscriber from `from`, or after the session's last record when `live`.
    fn subscribe(
        &mut self,
        from: crate::fanout::FromCursor,
        live: bool,
        sink: mpsc::Sender<crate::fanout::Delivery>,
    ) -> crate::actor::SubscribeOutcome {
        let Some(fanout) = self.fanout.as_mut() else {
            return crate::actor::SubscribeOutcome::Error(unserved("subscribe"));
        };
        let from = if live {
            crate::fanout::AttachFrom::Live
        } else {
            crate::fanout::AttachFrom::Cursor(from)
        };
        fanout.attach(&self.session, from, sink);
        crate::actor::SubscribeOutcome::Attached
    }

    /// The actor serves finished runs' facts from `settled`, and keeps the ones it reads there,
    /// as the module's reads of a session no actor holds do.
    pub fn with_settled(mut self, settled: Arc<crate::store_status::SettledRuns>) -> Self {
        self.settled = Some(settled);
        self
    }

    /// Answers a read of the session from what the actor holds (see `store_reads`): made durable
    /// first when the last batch was appended without a barrier, then read from memory only.
    async fn store_read(
        &mut self,
        request: &crate::store_reads::StoreReadRequest,
    ) -> crate::store_reads::StoreReadOutcome {
        // Made durable first, so the live projection has published every batch the read sees
        // and its last unit is the read's head.
        if let Some(refused) = crate::store_reads::durable_for_read(&mut self.session).await {
            return refused;
        }
        if let Some(fanout) = self.fanout.as_mut() {
            fanout.ensure_seeded(&self.session);
        }
        let addr = broca_core::session_addr(&self.key);
        let live_step = self
            .run
            .as_ref()
            .map(|active| crate::store_reads::LiveStep {
                run_id: active.run_id.0.clone(),
                step: active.step.lock().unwrap().clone(),
            });
        let context = crate::store_reads::ReadContext {
            parts: &self.parts,
            held: true,
            open: true,
            settled: self
                .settled
                .as_deref()
                .map(|settled| (settled, addr.as_str())),
            session_name: &self.key.session,
            live_step: live_step.as_ref(),
            stream_head: self.fanout.as_ref().and_then(|fanout| fanout.head()),
        };
        crate::store_reads::answer_in_memory(&mut self.session, &context, request).await
    }

    /// The actor writes the spend facts of each run that ends to `billing`.
    pub fn with_billing(mut self, billing: crate::store_billing::StoreBilling) -> Self {
        self.billing = Some(billing);
        self
    }

    /// The actor stores setup parts in `storage` before each append that could name them.
    pub fn with_storage(mut self, storage: broca_session_loop::durable::SetupParts<B>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// The session's writer, storing setup parts not yet durable before each append.
    fn durable(
        &mut self,
    ) -> broca_session_loop::durable::Durable<
        '_,
        Session<B>,
        broca_session_loop::durable::SetupParts<B>,
    > {
        broca_session_loop::durable::Durable {
            writer: &mut self.session,
            parts: &mut self.parts,
            store: self.storage.as_ref(),
            media: None,
        }
    }

    /// Appends `record`, one the actor writes on its own, synced, after storing the bytes of each
    /// medium of `staged` its messages name, and tells the active run's copy of the folds. Returns
    /// the record as appended, which says where those bytes are.
    async fn append_own_with_media(
        &mut self,
        record: Record,
        staged: &[crate::person_media::StagedMedia],
    ) -> Result<Record, SessionError> {
        let mut appended = self
            .append_own_batch_with_media(std::slice::from_ref(&record), staged)
            .await?;
        Ok(appended.pop().expect("one record was appended"))
    }

    /// [`Self::append_own_with_media`] for a batch of records, appended together.
    async fn append_own_batch_with_media(
        &mut self,
        records: &[Record],
        staged: &[crate::person_media::StagedMedia],
    ) -> Result<Vec<Record>, SessionError> {
        let mut media = broca_session_loop::HeldInputMedia::default();
        for item in staged {
            media.hold(&item.reference, Arc::clone(&item.bytes));
        }
        let (seq, appended) = media.append(&mut self.durable(), records, true).await?;
        if let Some(running) = self.run.as_mut() {
            running.link.note_own(seq, &appended);
        }
        Ok(appended)
    }

    /// Serves `commands` until the session goes away: when the channel closes, or when it is
    /// idle at an idle tick. Returns the session, for whoever closes it. A run still running
    /// when the channel closes is ended as a module stop ends it.
    pub async fn serve(mut self, commands: mpsc::UnboundedReceiver<LineageCmd>) -> Session<B> {
        let mut commands = Commands::from(commands);
        self.serve_until_exit(&mut commands, None, None, None).await;
        self.stop_for_shutdown().await;
        self.session
    }

    /// Serves the session as the module's actor for it, until it closes idle or the module
    /// stops, then gives its registry slot up. The order matches today's actor: the session is
    /// closed and the executor told before the lease goes, and only then does the slot read
    /// closed, so a reopen never finds another writer still on the session.
    pub async fn serve_in_registry(mut self, commands: impl Into<Commands>, life: Lifecycle) {
        let mut commands = commands.into();
        let seal = life.registry.seal_signal();
        self.registry = Some(life.registry.clone());
        let exit = self
            .serve_until_exit(
                &mut commands,
                Some(&seal),
                Some(life.idle_ttl),
                Some(&life.run_in_flight),
            )
            .await;
        let close = match exit {
            Exit::Idle => life.registry.begin_close(&self.key, life.gen),
            Exit::Closed | Exit::Stop => None,
        };
        if close.is_some() {
            // Commands that raced the close are answered so their callers re-route through the
            // registry, which reopens the session once this close is done.
            while let Some(command) = commands.try_recv() {
                reopen(command);
            }
        }
        self.stop_for_shutdown().await;
        life.run_in_flight
            .store(0, std::sync::atomic::Ordering::SeqCst);
        let _ = tokio::time::timeout(
            crate::actor::ON_SESSION_CLOSE_TIMEOUT,
            self.executor.on_session_close(&self.key),
        )
        .await;
        let key = self.key.clone();
        drop(self);
        drop(life.lease);
        match close {
            Some(close) => close.commit(),
            None => life.registry.finalize_exit(&key, life.gen),
        }
    }

    /// The command loop. With `seal`, the module's stop ends it; with `idle_ttl`, the session's
    /// idle deadline does; `gauge` reads whether a run is running.
    async fn serve_until_exit(
        &mut self,
        commands: &mut Commands,
        seal: Option<&CancellationToken>,
        idle_ttl: Option<std::time::Duration>,
        gauge: Option<&Arc<std::sync::atomic::AtomicU64>>,
    ) -> Exit {
        let never = CancellationToken::new();
        let seal = seal.unwrap_or(&never);
        self.seal = seal.clone();
        self.recover().await;
        let idle_after = |now: tokio::time::Instant| idle_ttl.map(|ttl| now + ttl);
        let mut idle_deadline = idle_after(tokio::time::Instant::now());
        loop {
            if let Some(gauge) = gauge {
                gauge.store(
                    u64::from(self.run.is_some()),
                    std::sync::atomic::Ordering::SeqCst,
                );
            }
            let built = media_built(&mut self.media_build);
            // A warm the executor is sending ends whatever else the actor waits on.
            let has_warm = self.warm.is_some();
            let warm_task = self.warm.as_mut().map(|pending| &mut pending.task);
            let warmed = async move {
                match warm_task {
                    Some(task) => task.await,
                    None => std::future::pending().await,
                }
            };
            let event = match self.run.as_mut() {
                // Until the run's start is durable, commands wait, as they wait in today's actor
                // while it awaits a run's admission, a drained run's included: a cancel, a
                // retraction or a second send can only meet a run that exists.
                Some(active) => tokio::select! {
                    biased;
                    _ = seal.cancelled() => return Exit::Stop,
                    request = active.link.next(), if !active.writer_gone => Event::Request(request),
                    end = &mut active.done => Event::Done(end),
                    result = warmed, if has_warm => Event::Warmed(result),
                    built = built => Event::MediaBuilt(built),
                    command = commands.recv(), if active.started => Event::Command(command),
                },
                None => tokio::select! {
                    biased;
                    _ = seal.cancelled() => return Exit::Stop,
                    result = warmed, if has_warm => Event::Warmed(result),
                    built = built => Event::MediaBuilt(built),
                    command = commands.recv() => Event::Command(command),
                    _ = async {
                        match idle_deadline {
                            Some(deadline) => tokio::time::sleep_until(deadline).await,
                            None => std::future::pending().await,
                        }
                    }, if !has_warm => return Exit::Idle,
                },
            };
            match event {
                Event::Request(Some(request)) => self.serve_request(request).await,
                Event::Request(None) => {
                    if let Some(active) = self.run.as_mut() {
                        active.writer_gone = true;
                    }
                }
                Event::Done(end) => self.finish_run(end).await,
                Event::MediaBuilt(built) => self.media_built(built).await,
                Event::Warmed(result) => self.finish_warm(result).await,
                Event::Command(None) => return Exit::Closed,
                Event::Command(Some(command)) => {
                    idle_deadline = idle_after(tokio::time::Instant::now());
                    if self.handle(command).await {
                        return Exit::Idle;
                    }
                }
            }
            self.retry_views().await;
        }
    }

    /// Serves one write the active run asks for.
    async fn serve_request(&mut self, request: broca_session_loop::shared::Request) {
        let active = self
            .run
            .as_mut()
            .expect("a request comes from the active run");
        let parts = self.parts.clone();
        let mut appends = crate::store_media::RunAppends {
            session: &mut self.session,
            // A result's media is stored with the result that names it.
            tool_media: &active.tool_media,
            fold: self.media_fold.as_mut(),
            parts: &parts,
        };
        let mut writer = broca_session_loop::durable::Durable {
            writer: &mut appends,
            parts: &mut self.parts,
            store: self.storage.as_ref(),
            // The run's start names the media of the send it places, which are stored with it.
            media: Some(&mut self.held),
        };
        active.link.serve(&mut writer, request).await;
        self.answer_admission();
    }

    /// Serves the active run, and nothing else, until no run is active.
    async fn run_to_end(&mut self) {
        while self.run.is_some() {
            self.run_current_to_end().await;
        }
    }

    /// Serves the active run, and nothing else, until it ends and its end is recorded. A run the
    /// queue then starts is left for the command loop.
    async fn run_current_to_end(&mut self) {
        loop {
            let Some(active) = self.run.as_mut() else {
                return;
            };
            let event = tokio::select! {
                request = active.link.next(), if !active.writer_gone => Event::Request(request),
                end = &mut active.done => Event::Done(end),
                built = media_built(&mut self.media_build) => Event::MediaBuilt(built),
            };
            match event {
                Event::Request(Some(request)) => self.serve_request(request).await,
                Event::Request(None) => {
                    if let Some(active) = self.run.as_mut() {
                        active.writer_gone = true;
                    }
                }
                Event::Done(end) => return self.finish_run(end).await,
                Event::MediaBuilt(built) => self.media_built(built).await,
                Event::Command(_) | Event::Warmed(_) => unreachable!("no command is read here"),
            }
        }
    }

    /// Stops the session for a module stop, or for a close with nobody left to send commands.
    /// A running run is stopped where it is and its unfinished run ended `Interrupted` by
    /// today's shutdown seal (`broca_core::resume::SealCause::Shutdown`): never cancelled, which
    /// would record a caller's decision nobody made. A run paused or held by a provider is left
    /// as it is, as today's actor leaves one. Nothing queued starts afterwards.
    async fn stop_for_shutdown(&mut self) {
        use broca_session_store::fold::operations::Resolution;
        self.stopping = true;
        let Some(active) = self.run.take() else {
            return;
        };
        active.done.abort();
        let _ = active.done.await;
        if let Some(reply) = active.admitting {
            let _ = reply.send(SendOutcome::Transient(
                "the module stopped before the run's start was written".into(),
            ));
        }
        let operations = self.session.operations();
        let Some(run) = operations.run.as_ref().filter(|run| run.finished.is_none()) else {
            return;
        };
        if matches!(
            run.resolution(),
            Resolution::Paused(_) | Resolution::ProviderHeld(_)
        ) {
            return;
        }
        let start = match self.recovery_start(run) {
            Ok(start) => start,
            Err(error) => {
                tracing::error!(
                    target: "broca.store_actor",
                    session = %self.key.session,
                    error = %error,
                    "the stopped run's start could not be read back to seal it; the next open seals it",
                );
                return;
            }
        };
        self.start_run(
            start,
            None,
            None,
            StoreRunMode::Seal(broca_core::resume::SealCause::Shutdown),
        )
        .await;
        if tokio::time::timeout(crate::actor::SHUTDOWN_SEAL_BOUND, self.run_to_end())
            .await
            .is_err()
        {
            // Left unfinished, the run is sealed as a crash orphan when the session next opens.
            tracing::warn!(
                target: "broca.store_actor",
                session = %self.key.session,
                "the shutdown seal did not finish in time; the next open seals the run",
            );
        }
    }

    /// Handles one command. Returns whether the actor should go away.
    async fn handle(&mut self, command: LineageCmd) -> bool {
        match command {
            LineageCmd::Send {
                session_class,
                retention,
                stop_sequences_named,
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
                mut reply,
            } => {
                // An ordered send is admitted below, by today's rules; how it is delivered is the
                // order's, not the string `delivery` beside it. So is an interrupt.
                if let Some(what) = unserved_route(conversation_key.is_some()) {
                    let _ = reply.send(SendOutcome::Error(unserved(what)));
                    return false;
                }
                // An imported session runs only once `session.adopt` gives it the plan its head
                // ran under, which isn't built yet: without it, a run would start with none.
                if self.session.operations().imported_ids.is_some() {
                    let _ = reply.send(SendOutcome::Error(
                        "an imported session takes no send until session.adopt gives it a plan"
                            .into(),
                    ));
                    return false;
                }
                // As in today's actor, a send's label and its structured output are checked against
                // the session before the owner check, so a retry naming another label is refused
                // rather than answered.
                let session_class = match self.session_class(session_class.as_deref()) {
                    Ok(class) => class,
                    Err(outcome) => {
                        let _ = reply.send(outcome);
                        return false;
                    }
                };
                if let Some(structured_output) = &request.structured_output {
                    let joins_active_run =
                        delivery == broca_protocol::Delivery::Steer && self.run.is_some();
                    let latest = match self.latest_admission() {
                        Ok(latest) => latest,
                        Err(error) => {
                            let _ = reply.send(SendOutcome::Error(error));
                            return false;
                        }
                    };
                    if let Err(field) = crate::actor::check_structured_output(
                        structured_output,
                        &request,
                        plan.as_deref(),
                        latest
                            .as_ref()
                            .map(|latest| (&latest.render_config, latest.tools.as_slice())),
                        joins_active_run,
                    ) {
                        let _ = reply.send(SendOutcome::StructuredOutputInvalid { field });
                        return false;
                    }
                }
                // As in today's actor, a send from a module other than the session's owner is
                // admitted next: an ordered list from a module the plan names, or a string
                // `queue` or `steer` from one, which stands for its list, or its granted
                // `interrupt`. Its template renders now, and the send then continues under the
                // session's frozen principal, as an internal send. The sender is recorded on its
                // `DeliveryOrdered` record alone.
                let mut request = request;
                let mut order = order.map(|order| *order);
                let mut ordered_by = None;
                if order.is_none() {
                    match self.string_sender(&request, delivery) {
                        Ok(crate::actor::StringSender::Owner) => {}
                        Ok(crate::actor::StringSender::AsOrder(as_order)) => order = Some(as_order),
                        Ok(crate::actor::StringSender::Interrupt(owner)) => {
                            ordered_by = request.principal.replace(owner);
                            request.scope = broca_protocol::SendScope::Internal;
                        }
                        Err(outcome) => {
                            let _ = reply.send(outcome);
                            return false;
                        }
                    }
                }
                // An interrupt, from the owner or a sender the plan grants one, goes on as today's
                // actor serves it: made durable, then the running run is cancelled, and its
                // message runs next.
                let interrupting =
                    order.is_none() && delivery == broca_protocol::Delivery::Interrupt;
                if let Some(order) = &order {
                    let admitted = match self.ordered_sender(&request, order) {
                        Ok(owner) => match &order.template {
                            Some(template) => self
                                .render_ordered_template(&mut request, template)
                                .await
                                .map(|()| owner),
                            None => Ok(owner),
                        },
                        Err(outcome) => Err(outcome),
                    };
                    match admitted {
                        Ok(Some(owner)) => {
                            ordered_by = request.principal.replace(owner);
                            request.scope = broca_protocol::SendScope::Internal;
                        }
                        Ok(None) => {}
                        Err(outcome) => {
                            let _ = reply.send(outcome);
                            return false;
                        }
                    }
                }
                // Whether the queue holds work that starts a run on its own, which a send that
                // would freeze the session's scope, or start a run now, has to wait behind.
                let queue_starts_runs = match self.queue_starts_runs().await {
                    Ok(starts) => starts,
                    Err(error) => {
                        let _ = reply.send(SendOutcome::Transient(error));
                        return false;
                    }
                };
                // As in today's actor, the owner and scope check comes first: a send the session's
                // history refuses is refused before any retry is answered.
                let scope = match self.send_scope(&run_id, &request, queue_starts_runs) {
                    Ok(scope) => scope,
                    Err(outcome) => {
                        let _ = reply.send(outcome);
                        return false;
                    }
                };
                // As in today's actor, the session's fetch plan comes next. It owns the session's
                // tools for its whole life, so a send naming tools is refused before any retry,
                // queue or steer path could ignore them; and a send's own plan meets the frozen
                // one before its key is looked at, so a retry under the same key with another plan
                // is refused by name. The first send freezes the plan its send fetched.
                let frozen = match self.frozen_manifest() {
                    Ok(frozen) => frozen,
                    Err(error) => {
                        let _ = reply.send(SendOutcome::Transient(error));
                        return false;
                    }
                };
                if request.tools.is_some() && frozen.is_some() {
                    let _ = reply.send(crate::plan_episode::tools_frozen_refusal());
                    return false;
                }
                let mut manifest = frozen.clone();
                let model_check = plan.as_ref().and_then(|plan| plan.model_check.clone());
                if let Some(plan) = plan {
                    if self.session.operations().owner.episodes == 0 {
                        match plan.manifest {
                            Some(fetched) => manifest = Some(fetched),
                            // The serve layer saw a run when it checked the plan, so it fetched
                            // nothing, yet the session has none: the caller retries the send so
                            // the plan is checked against one history, as today.
                            None => {
                                let _ = reply.send(SendOutcome::Transient(
                                    "the session's history changed while its plan was checked; \
                                     retry the send"
                                        .into(),
                                ));
                                return false;
                            }
                        }
                    } else if let Err(refusal) = crate::plan_change::later_send_plan(
                        frozen.as_ref().map(|frozen| frozen.plan_identity.as_str()),
                        self.refreshed_identities(),
                        &plan.identity,
                    ) {
                        let _ = reply.send(SendOutcome::PlanRefused(refusal));
                        return false;
                    }
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
                            order.as_ref(),
                        )
                        .await
                    {
                        Ok(Keyed::Answered(outcome)) => {
                            let _ = reply.send(outcome);
                            return false;
                        }
                        Ok(Keyed::Fresh(digest)) => identity = Some(digest),
                        Ok(Keyed::Resume) => {
                            let start = Start {
                                run_id: run_id.clone(),
                                sent_by: request.principal.clone(),
                                request: *request,
                                title,
                                send_id: key.send_id().map(str::to_owned),
                                identity: None,
                                scope,
                                manifest: None,
                                session_class: None,
                                prepared: Prepared::default(),
                            };
                            self.start_run(start, None, Some(reply), StoreRunMode::Resume)
                                .await;
                            return false;
                        }
                        Err(error) => {
                            let _ = reply.send(SendOutcome::Error(error));
                            return false;
                        }
                    }
                }
                if let Some(what) = unserved_request(&request, kind, retention.is_some()) {
                    let _ = reply.send(SendOutcome::Error(unserved(what)));
                    return false;
                }
                let mut request = *request;
                // Today's actor refuses a send that declares tools on a scoped session unless a
                // plan gave the session its tools: on a scoped route a tool provider refuses a call
                // that doesn't name the plan's settings for that tool, and only a plan supplies
                // them.
                if manifest.is_none()
                    && request
                        .tools
                        .as_ref()
                        .is_some_and(|tools| !tools.is_empty())
                    && (scope.is_some()
                        || matches!(request.scope, broca_protocol::SendScope::Stamped(_)))
                {
                    let _ = reply.send(SendOutcome::PlanRefused(
                        broca_fetch_plan::Refusal::ScopedToolsRequirePlan,
                    ));
                    return false;
                }
                let steering = order.is_none() && delivery == broca_protocol::Delivery::Steer;
                // Today's actor lets a plain send leave its model to the session only when a
                // plan selected the session's next-run model (`plan.model`), and then runs it on
                // that model: the latest refresh's choice, else the first run's plan's. A steer
                // may leave it: it joins a run whose model is already frozen. So may an ordered
                // send whose list names a mode delivered into a running run; it takes the latest
                // run's model, unless the plan's selection applies to a run it starts now.
                let joins = order.as_ref().is_some_and(ordered::joins_a_run);
                if !steering
                    && request
                        .inherit_frozen
                        .contains(&broca_protocol::request::InheritedParam::Model)
                {
                    // A session's first send runs on the plan it fetched; a later one on the
                    // session's choice.
                    let selection = if self.session.operations().owner.episodes == 0 {
                        Ok(manifest
                            .as_deref()
                            .and_then(broca_core::head::manifest_selection))
                    } else {
                        broca_session_loop::plan::selection(self.session.operations(), &self.parts)
                    };
                    let selection = match selection {
                        Ok(selection) => selection,
                        Err(error) => {
                            let _ = reply.send(SendOutcome::Transient(error));
                            return false;
                        }
                    };
                    if selection.is_none() && !joins {
                        let _ = reply.send(SendOutcome::DeliveryRefused {
                            code: "invalid_params",
                            field: "model",
                            message: "model is required unless plan.model selected the \
                                      session's next-run model"
                                .into(),
                        });
                        return false;
                    }
                    match selection.filter(|_| order.is_none() || self.run.is_none()) {
                        Some(selection) => {
                            request
                                .inherit_frozen
                                .remove(&broca_protocol::request::InheritedParam::Model);
                            request.model = broca_protocol::ModelHandle::new(
                                &selection.provider,
                                &selection.model,
                            );
                            request.variant = selection.variant.clone();
                        }
                        // As today's actor records it (`record_frozen_model`), the model stays
                        // left to the session.
                        None => match self.latest_admission() {
                            Ok(Some(latest)) => {
                                request.model = latest.model;
                                request.variant = latest.variant;
                            }
                            Ok(None) => {
                                let _ = reply.send(SendOutcome::Error(
                                    "a send left its model to the session, which has no \
                                     episode to take it from"
                                        .into(),
                                ));
                                return false;
                            }
                            Err(error) => {
                                let _ = reply.send(SendOutcome::Error(error));
                                return false;
                            }
                        },
                    }
                }
                // A send that left its model to the session checks its plan's model-bearing
                // items against the model it now runs on, as today's actor does once the model
                // is resolved. A steer that left it would be checked against the joined run's
                // frozen model and variant, which this actor doesn't read back yet.
                if model_check.is_some()
                    && request
                        .inherit_frozen
                        .contains(&broca_protocol::request::InheritedParam::Model)
                {
                    let _ = reply.send(SendOutcome::Error(unserved(
                        "a steer leaving its model to the session with a plan to check",
                    )));
                    return false;
                }
                if let Some(plan) = &model_check {
                    if let Err(refusal) = broca_fetch_plan::admit::check_plan_model(
                        plan,
                        request.model.provider_module_id.as_str(),
                        &request.model.model_id,
                        request.variant.as_deref(),
                    ) {
                        let _ = reply.send(SendOutcome::PlanRefused(refusal));
                        return false;
                    }
                }
                // A run paused for sign-in or an account limit still owns the session: today's
                // actor neither queues another send behind it nor drives it again for one. A
                // steer can join no paused run but one paused for a restart, which today's actor
                // steers into; this actor doesn't resume such a run, so it refuses that steer. An
                // ordered send waits behind any paused run, unless its list could start one. An
                // interrupt can't end a run waiting on calls a provider holds, as today; today's
                // actor ends any other paused run for it, which this actor doesn't do yet.
                if self.run.is_none() {
                    if let Some((paused, reason)) = self.paused() {
                        let starts_runs = order
                            .as_ref()
                            .is_none_or(broca_protocol::DeliveryOrder::starts_runs);
                        let refusal = match reason {
                            broca_protocol::PauseReason::ToolHeld if interrupting => {
                                Some(SendOutcome::RunPaused {
                                    run_id: paused,
                                    reason,
                                })
                            }
                            _ if interrupting => Some(SendOutcome::Error(unserved(
                                "an interrupt ending a paused run",
                            ))),
                            broca_protocol::PauseReason::Restart if steering => {
                                Some(SendOutcome::Error(unserved(
                                    "a steer into a run paused for a restart",
                                )))
                            }
                            broca_protocol::PauseReason::AuthRequired
                            | broca_protocol::PauseReason::AccountLimit
                                if starts_runs =>
                            {
                                Some(SendOutcome::RunPaused {
                                    run_id: paused,
                                    reason,
                                })
                            }
                            _ if steering => Some(SendOutcome::RunPaused {
                                run_id: paused,
                                reason,
                            }),
                            _ => None,
                        };
                        if let Some(refusal) = refusal {
                            let _ = reply.send(refusal);
                            return false;
                        }
                    }
                }
                let start = Start {
                    run_id,
                    // A plan sender's interrupt shows its sender, as today's actor records it,
                    // though it runs under the session's principal.
                    sent_by: ordered_by.clone().or_else(|| request.principal.clone()),
                    request,
                    title,
                    send_id: key.send_id().map(str::to_owned),
                    identity,
                    scope,
                    manifest,
                    session_class,
                    prepared: Prepared::default(),
                };
                if let Some(order) = order {
                    self.queue_ordered(start, order, ordered_by, reply).await;
                } else if steering && self.run.is_some() {
                    let outcome = self.steer(start, stop_sequences_named, &mut reply).await;
                    let _ = reply.send(outcome);
                } else if interrupting && self.run.is_some() {
                    // Today's order is the guarantee: the interrupt's message is durable, queued
                    // ahead of every plain send, before the running run is told to stop. A crash
                    // in between leaves the run unfinished with the interrupt at the queue's
                    // head, which the next open seals and drains (`recover`). The run ends
                    // `Cancelled`, and its end drains the interrupt.
                    let outcome = self.queue(start, delivery, &mut reply).await;
                    if matches!(outcome, SendOutcome::Pending { .. }) {
                        if let Some(active) = &self.run {
                            active.cancel.cancel();
                        }
                    }
                    let _ = reply.send(outcome);
                } else if self.running().is_some() {
                    let outcome = self.queue(start, delivery, &mut reply).await;
                    let _ = reply.send(outcome);
                } else if queue_starts_runs {
                    // The queue drains at a run's end, so an idle session with sends still queued
                    // that start runs holds sends this actor doesn't drain yet (see `drain`);
                    // today's actor would start them first.
                    let _ = reply.send(SendOutcome::Error(unserved(
                        "a send while sends are queued on an idle session",
                    )));
                } else {
                    let mut start = start;
                    let admitted = match self.freeze_plan(&mut start) {
                        // As today's actor starts a run, the send's attachments are staged into its
                        // input before its transforms, and held until the run's start, which
                        // stores them, is appended.
                        Ok(()) => match self.stage_media(&mut start.request, None).await {
                            Ok(staged) => {
                                for item in &staged {
                                    self.held.hold(&item.reference, Arc::clone(&item.bytes));
                                }
                                self.transform_fresh_input(&mut start, delivery, &mut reply)
                                    .await
                            }
                            Err(outcome) => Err(outcome),
                        },
                        Err(outcome) => Err(outcome),
                    };
                    match admitted {
                        Ok(()) => {
                            self.start_run(start, None, Some(reply), StoreRunMode::Fresh)
                                .await
                        }
                        Err(outcome) => {
                            self.held.clear();
                            let _ = reply.send(outcome);
                        }
                    }
                }
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
            LineageCmd::Retract {
                submission_id,
                reply,
            } => {
                let _ = reply.send(self.retract(submission_id).await);
                false
            }
            LineageCmd::SessionRefresh {
                generation,
                policy,
                manifest,
                reply,
            } => {
                let _ = reply.send(self.session_refresh(generation, policy, manifest).await);
                false
            }
            LineageCmd::SessionRefreshPolicy {
                generation,
                policy,
                send_id,
                reply,
            } => {
                let _ = reply.send(
                    self.session_refresh_policy(generation, policy, send_id)
                        .await,
                );
                false
            }
            LineageCmd::SessionFlushPrefix { send_id, reply } => {
                let _ = reply.send(self.session_flush_prefix(send_id).await);
                false
            }
            // The registry decides whether a sender may cancel from the session's stored owner
            // summary, which carries the senders the latest accepted refresh grants
            // (`broca_session_loop::owner::ownership_with_plan`), as it decides for a session no
            // actor holds. This actor keeps no cached answer of its own to give first.
            LineageCmd::SenderPolicy { reply, .. } => {
                let _ = reply.send(Ok(None));
                false
            }
            LineageCmd::IdleTick => self.run.is_none() && self.warm.is_none(),
            LineageCmd::Warm { lineage_id, reply } => {
                if self.stopping {
                    let _ = reply.send(crate::actor::WarmOutcome::Reopen);
                } else {
                    self.warm(lineage_id, reply).await;
                }
                false
            }
            LineageCmd::Store {
                command: crate::store_import::StoreCommand::ApplyView(command),
                reply,
            } => {
                self.apply_view_or_wait(*command, reply).await;
                false
            }
            LineageCmd::Store { command, reply } => {
                let (outcome, exit) = self.store_command(command).await;
                let _ = reply.send(outcome);
                exit
            }
            LineageCmd::Subscribe {
                from,
                live,
                sink,
                reply,
            } if self.fanout.is_some() => {
                let _ = reply.send(self.subscribe(from, live, sink));
                false
            }
            LineageCmd::StoreRead { request, reply } => {
                let _ = reply.send(self.store_read(&request).await);
                false
            }
            other => {
                refuse(other);
                false
            }
        }
    }

    /// Starts a run of `start` on the idle session: a send's own run, whose sender's `reply`
    /// waits for the run's start to be durable or for its refusal, or one `drained` from the
    /// queue, which no sender waits on. In any other `mode` the session's latest run goes on or
    /// ends instead of a new one starting: its start is already durable, so a sender hears
    /// `active` at once, as today's actor answers a resumed run at its admission.
    async fn start_run(
        &mut self,
        start: Start,
        drained: Option<Drained>,
        mut reply: Option<oneshot::Sender<SendOutcome>>,
        mode: StoreRunMode,
    ) {
        let media = match mode {
            StoreRunMode::Seal(_) => None,
            _ => Some(self.run_media().await),
        };
        let Start {
            run_id,
            mut request,
            title,
            send_id,
            identity,
            sent_by,
            scope,
            manifest,
            session_class,
            prepared,
        } = start;
        // The executor resolves the send's credential selection; only the result is frozen.
        let auth = request.auth.take();
        let mut req = RunRequest::from_submission(
            request,
            run_id.clone(),
            self.key.clone(),
            broca_protocol::SubmissionId(String::new()),
        );
        match &drained {
            // The run consumes the queued send, whose message keeps the time it was accepted.
            Some(drained) => {
                req.submission_id = Some(drained.submission.clone());
                req.input_written_at_ms = drained.written_at_ms;
            }
            // A run started directly consumes no queued send; its input was written when it
            // was accepted, now unless its transforms were told another time.
            None => {
                req.submission_id = None;
                req.input_written_at_ms = prepared.at_ms.or_else(broca_core::run::epoch_millis);
            }
        }
        req.title = title;
        req.send_id = send_id;
        req.request_identity_digest = identity;
        req.scope = scope;
        req.session_class = session_class;
        let cancel = CancellationToken::new();
        let step: broca_core::provider::LiveStepProgress = Arc::default();
        let control = RunControl {
            cancel: Some(cancel.clone()),
            step_progress: Some(Arc::clone(&step)),
            hook_plane: self.executor.hook_plane(),
            provider_held_lifecycle: self.executor.provider_held_lifecycle(),
            compaction_plane: self.executor.compaction_plane(),
            compaction_session: Some(Arc::clone(&self.compaction_session)),
            display_sent_by: sent_by,
            // The version the run's plan asks for. The loop decides the session's mode from it
            // and the session's latest run: a session on the compaction contract stays on it,
            // and one that ran outside it moves onto it at the first run whose plan names a
            // compaction provider (`broca_session_loop::admit::session_contract`).
            new_log_compaction_plan_version: crate::plan_episode::plan_compaction_version(
                manifest.as_deref(),
            ),
            manifest,
            tools_were_omitted: prepared.tools_were_omitted,
            input_hooks: prepared.hooks,
            pre_model_error: prepared.pre_model_error,
            applies_refresh: prepared.applies_refresh,
            ..Default::default()
        };
        let tool_media = Arc::new(broca_session_loop::PendingMedia::default());
        let (writer, link) = ActorWriter::new(
            &self.session,
            Some(Arc::clone(&tool_media) as Arc<dyn broca_core::tool_media::MediaCapture>),
        );
        let parts: Box<dyn Parts> = Box::new(self.parts.clone());
        let executor = Arc::clone(&self.executor);
        let (out, mut events) = mpsc::channel(256);
        match &self.fanout {
            Some(fanout) => {
                crate::store_fanout::drain_display(events, fanout.display());
            }
            // Without subscriptions the run's output is drained so it never waits on a full
            // channel.
            None => {
                tokio::spawn(async move { while events.recv().await.is_some() {} });
            }
        }
        let job = StoreRunJob {
            req,
            auth,
            control,
            writer,
            parts,
            mode,
            media,
        };
        let resume = mode != StoreRunMode::Fresh;
        if resume {
            if let Some(reply) = reply.take() {
                let _ = reply.send(SendOutcome::Active {
                    run_id: run_id.clone(),
                    delivered: None,
                });
            }
        }
        let done = tokio::spawn(async move { executor.execute_on_store(job, out).await });
        self.run = Some(ActiveRun {
            run_id,
            link,
            writer_gone: false,
            cancel,
            admitting: reply,
            started: resume,
            submission: drained.map(|drained| drained.submission),
            done,
            step,
            tool_media,
        });
    }

    /// Where a run starting now gets what it renders its media from (see `store_media`). Once
    /// the session's media fold exists, it is brought up to the session's last record and sent
    /// at once. The first time, it is built from the session's first record, off the actor: the
    /// run waits for it before its first model call, while the actor keeps answering commands.
    async fn run_media(&mut self) -> crate::store_media::StoreRunMediaWait {
        let (sender, wait) = oneshot::channel();
        // Only the starting run waits: an earlier run that waited has ended.
        self.media_waiting = Some(sender);
        if self.media_fold.is_none() && self.media_build.is_none() {
            self.build_media();
        }
        self.deliver_media().await;
        wait
    }

    /// Starts building the session's media fold from its records, read over a handle of its
    /// own, on a blocking thread.
    fn build_media(&mut self) {
        let key = self.key.clone();
        let Some(readers) = &self.readers else {
            let fold = crate::store_media::MediaFold::new(key);
            fold.stop(
                "the session's records can't be read apart from its writer, so its media fold \
                 can't be built"
                    .into(),
            );
            self.media_fold = Some(fold);
            return;
        };
        let records = self.session.stored_records(readers());
        let last = self.session.next_seq().saturating_sub(1);
        let parts = self.parts.clone();
        #[cfg(test)]
        let gate = self.media_build_gate.clone();
        self.media_build = Some(tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            if let Some(gate) = gate {
                futures::executor::block_on(gate.notified());
            }
            let mut fold = crate::store_media::MediaFold::new(key);
            if last > 0 {
                match futures::executor::block_on(records.records_between(1, last)) {
                    Ok(records) => fold.take(&records, &parts),
                    Err(error) => fold.stop(format!(
                        "the session's media fold couldn't read its records: {error:?}"
                    )),
                }
            }
            Box::new(fold)
        }));
    }

    /// Takes in the media fold a build ended with, and sends the waiting run its media.
    async fn media_built(
        &mut self,
        built: Result<Box<crate::store_media::MediaFold>, tokio::task::JoinError>,
    ) {
        self.media_build = None;
        let fold = built.map(|fold| *fold).unwrap_or_else(|error| {
            let fold = crate::store_media::MediaFold::new(self.key.clone());
            fold.stop(format!("the session's media fold failed: {error}"));
            fold
        });
        self.media_fold = Some(fold);
        self.deliver_media().await;
    }

    /// Sends the waiting run its media, once the session's media fold exists: brought up to the
    /// session's last record, including what the run appended while the fold was building.
    async fn deliver_media(&mut self) {
        let Some(fold) = self.media_fold.as_mut() else {
            return;
        };
        let Some(waiting) = self.media_waiting.take() else {
            return;
        };
        let parts = self.parts.clone();
        fold.catch_up(&self.session, &parts).await;
        let bytes = self
            .readers
            .as_ref()
            .map(|readers| fold.bytes(self.session.media_objects(readers())));
        let _ = waiting.send(fold.for_run(bytes));
    }

    /// Recovers the session's unfinished work on opening it, before any command, as today's
    /// actor does on open for the cases this storage serves:
    /// - a running run is ended `Interrupted` without calling the model, as today's startup
    ///   pass seals a run a crashed process left running (`runner_crash`). The actor owns the
    ///   session while a run runs, and a graceful stop seals or pauses it, so only a process
    ///   that ended without doing either leaves one; today needs its run index to tell such a
    ///   run from one still running elsewhere, and this storage needs nothing;
    /// - a running run with a cancel pending resumes instead, so the cancel ends it, as today;
    /// - a run whose last step ended it naturally, with only its end lost, resumes, which
    ///   writes just that end;
    /// - with no unfinished run, the first queued send drains.
    ///
    /// A paused run stays paused: a sign-in pause waits for its retry. Resuming after a
    /// restart, the account-limit pause's timer, and calls a provider holds aren't recovered on
    /// this storage yet, so nothing is written for them.
    async fn recover(&mut self) {
        self.bill_owed().await;
        let operations = self.session.operations();
        let Some(run) = operations.run.as_ref().filter(|run| run.finished.is_none()) else {
            return self.drain().await;
        };
        let mode = match recovery_mode(run) {
            Some(mode) => mode,
            None => {
                if !matches!(run.paused, Some(broca_protocol::PauseReason::AuthRequired)) {
                    tracing::warn!(
                        target: "broca.store_actor",
                        session = %self.key.session,
                        run_id = %run.run_id,
                        paused = ?run.paused,
                        "an unfinished run is left as it is: recovering it isn't built for the new storage yet",
                    );
                }
                return;
            }
        };
        match self.recovery_start(run) {
            Ok(start) => {
                self.start_run(start, None, None, mode).await;
                // Today's actor seals a crash's orphan before it takes any command, so no caller
                // sees the orphan as running; the seal sends nothing, so it ends at once.
                if matches!(mode, StoreRunMode::Seal(_)) {
                    self.run_current_to_end().await;
                }
            }
            Err(error) => tracing::error!(
                target: "broca.store_actor",
                session = %self.key.session,
                run_id = %run.run_id,
                error = %error,
                "the unfinished run's start could not be read back to recover it",
            ),
        }
    }

    /// Bills every run whose billing is owed, oldest first: its spend facts are inserted into
    /// the usage export, then a `Billed` record settles it, never before the insert commits.
    /// A run that can't be billed stays owed, and the next run's end or the next open tries
    /// again; billing never fails a run.
    async fn bill_owed(&mut self) {
        let Some(billing) = self.billing.clone() else {
            return;
        };
        let owed = self.session.operations().billing_owed.clone();
        for run_id in owed {
            let inserted = billing
                .insert(&self.session, &self.key, &self.parts, &run_id)
                .await;
            let settled = match inserted {
                Ok(()) => {
                    let record = Record::Billed(Billed {
                        run_id: run_id.clone(),
                    });
                    Writer::append(&mut self.durable(), std::slice::from_ref(&record), false)
                        .await
                        .map(drop)
                        .map_err(|error| {
                            format!("the run's billing could not be recorded: {error:?}")
                        })
                }
                Err(error) => Err(error),
            };
            if let Err(error) = settled {
                tracing::warn!(
                    target: "broca.store_actor",
                    session = %self.key.session,
                    run_id = %run_id,
                    error = %error,
                    "the run's spend facts were not written; it stays owed and is billed again at the next run's end or open",
                );
            }
        }
    }

    /// The run recovery carries on or ends: the session's unfinished `run`, under the request
    /// its start froze. The resumed run renders from its start record, not from this request;
    /// the request names what the executor resolves the run's provider by.
    fn recovery_start(
        &self,
        run: &broca_session_store::fold::operations::Run,
    ) -> Result<Start, String> {
        let start = run
            .start
            .as_ref()
            .ok_or_else(|| format!("the run {} has no start in the session", run.run_id))?;
        let admission = broca_session_loop::run::admission_of_start(start, &self.parts)
            .map_err(|error| format!("{error:?}"))?;
        let request = serde_json::from_value(serde_json::json!({
            "model": admission.model,
            "input": [],
            "tools": admission.tools,
            "policy_allowed": admission.tools.iter().map(|tool| &tool.name).collect::<Vec<_>>(),
            "tool_choice": admission.tool_choice,
            "generation": admission.generation,
            "stop_when": admission.stop_when,
            "context_limit": admission.context_limit,
            "on_restart": admission.on_restart,
        }))
        .map_err(|error| format!("the run's frozen request: {error}"))?;
        Ok(Start {
            run_id: RunId(run.run_id.clone()),
            request,
            title: None,
            send_id: None,
            identity: None,
            sent_by: None,
            scope: self.ownership()?.scope,
            // The run renders from its own start, which recorded any manifest and label.
            manifest: None,
            session_class: None,
            prepared: Prepared::default(),
        })
    }

    /// The label the session's first run froze; `None` for a session created unlabelled.
    fn frozen_session_class(&self) -> Option<String> {
        self.session
            .operations()
            .owner
            .first_start
            .as_ref()
            .and_then(|start| start.session_class.clone())
    }

    /// The label a send's run records, by today's rule (`crate::session_class::admit`): the
    /// session's first run freezes the label its send names, a later send may only repeat it,
    /// and a session created unlabelled stays unlabelled whatever a later send names.
    fn session_class(&self, requested: Option<&str>) -> Result<Option<String>, SendOutcome> {
        let frozen = self
            .session
            .operations()
            .owner
            .first_start
            .as_ref()
            .map(|start| start.session_class.as_deref());
        let Some(requested) = requested else {
            return Ok(frozen.flatten().map(str::to_owned));
        };
        match crate::session_class::admit(Some(requested), frozen) {
            Ok(crate::session_class::Admitted::Class(class)) => Ok(class),
            Ok(crate::session_class::Admitted::IgnoredOnUnlabelled(label)) => {
                tracing::info!(
                    target: "broca.session_class",
                    session = %self.key.session,
                    label = %label,
                    "session_class ignored: the session was created unlabelled"
                );
                Ok(None)
            }
            Err(message) => Err(SendOutcome::SessionClassInvalid(message)),
        }
    }

    /// The session's latest run as its start froze it, read back from the setup parts it
    /// names; `None` before the session's first run.
    fn latest_admission(&self) -> Result<Option<broca_session_loop::run::Admission>, String> {
        let Some(start) = self
            .session
            .operations()
            .run
            .as_ref()
            .and_then(|run| run.start.as_ref())
        else {
            return Ok(None);
        };
        broca_session_loop::run::admission_of_start(start, &self.parts)
            .map(Some)
            .map_err(|error| format!("the session's latest run start: {error:?}"))
    }

    /// The fetch plan's manifest the session's first run froze, read back from the setup parts
    /// its start names; `None` for a session admitted without a plan, or with no run yet.
    fn frozen_manifest(&self) -> Result<Option<Arc<broca_wal::PlanManifest>>, String> {
        let Some(first) = self.session.operations().owner.first_start.as_ref() else {
            return Ok(None);
        };
        broca_session_loop::run::admission_of_start(first, &self.parts)
            .map(|admission| admission.manifest)
            .map_err(|error| format!("the session's first run start: {error:?}"))
    }

    /// What a later send's plan is checked against once the session took a refresh, as today's
    /// actor checks it (`crate::plan_change::later_send_plan`): the plan identities of the latest
    /// run's manifest and of the latest accepted refresh. `None` before any refresh.
    fn refreshed_identities(&self) -> Option<(Option<&str>, Option<&str>)> {
        let plan = self
            .session
            .operations()
            .plan
            .as_ref()
            .filter(|plan| plan.accepted_generation > 0)?;
        Some((
            plan.current
                .as_ref()
                .and_then(|current| current.plan_identity()),
            plan.accepted
                .as_ref()
                .and_then(|accepted| accepted.plan_identity()),
        ))
    }

    /// The manifest a new run on `model` and `variant` freezes on a session that already ran,
    /// chosen by today's rule (`crate::plan_change::run_manifest`) from the session's fold: the
    /// pending refresh's once its policy's trigger came and its model-bearing items accept the
    /// run's model, else the latest run's. With the pending refresh's, also what the run start
    /// records about applying it. A pending refresh whose plan refuses the model refuses the
    /// run, as today's does.
    fn run_plan(
        &self,
        model: &broca_protocol::ModelHandle,
        variant: Option<&str>,
    ) -> Result<
        (
            Option<Arc<broca_wal::PlanManifest>>,
            Option<broca_protocol::session_change::AppliedChange>,
        ),
        SendOutcome,
    > {
        use broca_fetch_plan::session_change::{
            applies_at_run_start, model_key, Applying, EffectiveState, PendingState,
        };
        let default = broca_session_store::fold::operations::Plan::default();
        let plan = self.session.operations().plan.as_ref().unwrap_or(&default);
        let stored = |manifest| {
            broca_session_loop::run::stored_manifest(manifest, &self.parts).map_err(|error| {
                SendOutcome::Transient(format!("a plan manifest the session froze: {error:?}"))
            })
        };
        // The latest run's manifest; a session whose runs froze none has none.
        let current = match &plan.current {
            Some(current) => Some(stored(current)?),
            None => self.frozen_manifest().map_err(SendOutcome::Transient)?,
        };
        let pending = plan
            .pending
            .as_ref()
            .map(|pending| {
                Ok::<_, SendOutcome>(PendingState {
                    generation: pending.generation,
                    policy: pending.policy.clone(),
                    manifest: stored(&pending.manifest)?,
                })
            })
            .transpose()?;
        let rebuild = plan.rebuild_since_accept
            || crate::plan_change::adds_compaction(
                self.session.operations().on_contract(),
                current.as_deref(),
                pending.as_ref(),
            );
        let applying = crate::plan_change::pending_applying(
            pending.as_ref(),
            rebuild,
            plan.flushed_since_accept,
            plan.last_model.as_deref(),
            model,
        );
        let manifest = crate::plan_change::run_manifest(current, applying, false, model, variant)
            .map_err(SendOutcome::PlanRefused)?;
        let applies = applying.map(|pending| {
            let applied_by = applies_at_run_start(
                &pending.policy,
                rebuild,
                plan.flushed_since_accept,
                plan.last_model.as_deref(),
                &model_key(model),
            )
            .expect("the pending change applies at this run");
            EffectiveState::of(
                &pending.manifest,
                Applying {
                    generation: pending.generation,
                    applied_by,
                },
            )
            .applied_change()
        });
        Ok((manifest, applies))
    }

    /// Freezes the session's fetch plan into a run about to start, by the rules today's actor
    /// applies on every path that starts an episode (`crate::plan_episode`): a plan that confines
    /// its tools needs a scope, and the run takes the manifest's tools and, as the session's
    /// first run, its system text first. A later run first takes the manifest today's rule
    /// chooses (see [`Self::run_plan`]). Answers the refusal a sender gets otherwise. Whether
    /// the send named tools of its own is noted first, for the run's start to record.
    fn freeze_plan(&self, start: &mut Start) -> Result<(), SendOutcome> {
        // Today's actor records whether the send named tools as the send arrived, before its
        // plan's tools fill them in.
        let tools_were_omitted = start.request.tools.is_none();
        start.prepared.tools_were_omitted = tools_were_omitted;
        let first_run = self.session.operations().owner.episodes == 0;
        if !first_run {
            let (manifest, applies) =
                self.run_plan(&start.request.model, start.request.variant.as_deref())?;
            start.manifest = manifest;
            start.prepared.applies_refresh = applies;
        }
        crate::plan_episode::require_confining_scope(
            start.manifest.as_deref(),
            start.scope.as_ref(),
        )
        .map_err(SendOutcome::ScopeRefused)?;
        let Some(manifest) = start.manifest.clone() else {
            // Without a plan, a send that leaves its tools out runs on the latest run's, as
            // today's actor does; the session's first run has none to take, so it runs with none.
            if tools_were_omitted {
                let inherited = if first_run {
                    Vec::new()
                } else {
                    self.latest_admission()
                        .map_err(SendOutcome::Transient)?
                        .map(|latest| latest.tools)
                        .unwrap_or_default()
                };
                start.request.policy_allowed =
                    inherited.iter().map(|tool| tool.name.clone()).collect();
                start.request.tools = Some(inherited);
            }
            return Ok(());
        };
        crate::plan_episode::freeze_manifest(
            &manifest,
            &mut start.request,
            first_run,
            tools_were_omitted,
        )
        .map_err(|error| SendOutcome::ClassifiedError {
            class: error.admission_error_class(),
            reason: error.to_string(),
        })
    }

    /// Runs a fresh run's own input through the session's `pre_user` transforms, as today's
    /// actor does before it starts a run that consumes no queued send: the run sends what they
    /// left and its start records what they did. A transform that refuses the input doesn't
    /// stop the run from starting: the run ends with that refusal before any model call, as
    /// today's does. A failure to run them refuses the send.
    async fn transform_fresh_input(
        &self,
        start: &mut Start,
        delivery: broca_protocol::Delivery,
        reply: &mut oneshot::Sender<SendOutcome>,
    ) -> Result<(), SendOutcome> {
        let at_ms = broca_core::run::epoch_millis();
        let prepared = self
            .transform_input(
                start.request.input.clone(),
                delivery,
                start.manifest.as_deref(),
                start.scope.as_ref(),
                at_ms,
                reply,
            )
            .await
            .map_err(|outcome| {
                SendOutcome::Error(format!("input preparation failed: {outcome:?}"))
            })?;
        start.prepared.at_ms = at_ms;
        if let Some(provider) = &prepared.refused {
            start.prepared.pre_model_error = Some(crate::actor::pre_user::refusal_error(provider));
        }
        start.request.input = prepared.input;
        start.prepared.hooks = prepared.hooks;
        Ok(())
    }

    /// `input`, a send delivered as `delivery` and accepted at `at_ms`, through the `pre_user`
    /// transforms `manifest`'s plan declares, by the rules today's actor runs them
    /// (`crate::actor::pre_user`). They run under `scope`, the scope a session's first send
    /// freezes, else under the one the session froze. They give up, with today's answers, when
    /// the module stops or the sender waiting on `reply` stops waiting.
    async fn transform_input(
        &self,
        input: Vec<broca_protocol::Message>,
        delivery: broca_protocol::Delivery,
        manifest: Option<&broca_wal::PlanManifest>,
        scope: Option<&broca_protocol::SessionScope>,
        at_ms: Option<u64>,
        reply: &mut oneshot::Sender<SendOutcome>,
    ) -> Result<crate::actor::pre_user::PreparedInput, SendOutcome> {
        use crate::actor::pre_user;
        let prepared = pre_user::PreparedInput {
            at_ms,
            input,
            hooks: None,
            refused: None,
        };
        let Some((plan, plane)) = pre_user::input_transforms(manifest, self.executor.hook_plane())?
        else {
            return Ok(prepared);
        };
        let scope = match scope {
            Some(scope) => Some(scope.clone()),
            None => self.ownership().map_err(SendOutcome::Transient)?.scope,
        };
        let seal = self.seal.clone();
        let abandon = async move {
            tokio::select! {
                biased;
                _ = seal.cancelled() => SendOutcome::Transient("module is stopping".into()),
                _ = reply.closed() => SendOutcome::Transient("send cancelled".into()),
            }
        };
        pre_user::transform_input(
            &plan,
            &plane,
            &self.key,
            scope.as_ref(),
            delivery,
            prepared,
            abandon,
        )
        .await
    }

    /// The session's owner summary, from its run starts, as today's admission rules read it.
    fn ownership(&self) -> Result<broca_core::SessionOwnershipV1, String> {
        broca_session_loop::owner::ownership(&self.session.operations().owner)
            .map_err(|error| format!("the session's owner summary: {error}"))
    }

    /// The scope a send's run would record, decided as today's actor decides it: refused by the
    /// shared rule when the session's history refuses the send's principal or scope, and refused
    /// as busy when the send would freeze a scope while a run, or queued work that starts a run
    /// (`queue_starts_runs`), holds the session, since only a run's start can carry it. A send from broca itself (`SendScope::Internal`)
    /// carries the scope the session already froze.
    ///
    /// Reopening a scope at a later epoch, and agent-run scopes, are refused by name: today's
    /// actor checks more for each (whether the run can start now; for an agent run, that the
    /// send declares no tools of its own).
    fn send_scope(
        &self,
        run_id: &RunId,
        request: &SubmissionRequest,
        queue_starts_runs: bool,
    ) -> Result<Option<broca_protocol::SessionScope>, SendOutcome> {
        let ownership = self.ownership().map_err(SendOutcome::Transient)?;
        if let broca_protocol::SendScope::Stamped(bind) = &request.scope {
            if bind.scope.run_id.is_some() {
                return Err(SendOutcome::Error(unserved(
                    "a send under an agent-run scope",
                )));
            }
        }
        match broca_core::decide_scope(&ownership, request.principal.as_ref(), &request.scope) {
            Err(refusal) => Err(SendOutcome::ScopeRefused(refusal)),
            Ok(broca_core::ScopeDecision::Reopen(_)) => Err(SendOutcome::Error(unserved(
                "reopening the session's scope at a later epoch",
            ))),
            Ok(decision @ broca_core::ScopeDecision::Freeze(_)) => {
                if let Some(active) = self.run.as_ref().filter(|active| active.run_id != *run_id) {
                    return Err(SendOutcome::ScopeFreezeBusy {
                        reason: crate::actor::ScopeFreezeBusyReason::RunActive,
                        run_id: Some(active.run_id.clone()),
                    });
                }
                if self.run.is_none() && self.paused().is_none() && queue_starts_runs {
                    return Err(SendOutcome::ScopeFreezeBusy {
                        reason: crate::actor::ScopeFreezeBusyReason::QueueNonempty,
                        run_id: None,
                    });
                }
                Ok(decision.scope().cloned())
            }
            Ok(decision) => Ok(decision.scope().cloned()),
        }
    }

    /// The session's latest run and why it paused, while it is paused.
    fn paused(&self) -> Option<(RunId, broca_protocol::PauseReason)> {
        let run = self.session.operations().run.as_ref()?;
        let reason = run.paused.filter(|_| run.finished.is_none())?;
        Some((RunId(run.run_id.clone()), reason))
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
        order: Option<&broca_protocol::DeliveryOrder>,
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
            let mut holders = submission;
            if let Some(send_id) = key.send_id() {
                holders.extend(find(IdKind::SendId, send_id.to_owned()).await?);
            }
            // A key whose send is still queued is answered by the queue (`queue`), as in today's
            // actor. Today's actor also answers a key held by a steer, a retraction or an ended
            // send, from records this actor doesn't read yet.
            let queued_id = format!("sub-{}", run_id.0);
            let still_queued = !holders.is_empty()
                && holders.iter().all(|(_, record)| {
                    matches!(record, Record::QueuedSend(_) | Record::DeliveryOrdered(_))
                })
                && self
                    .session
                    .operations()
                    .queue
                    .iter()
                    .any(|pending| pending.submission_id == queued_id);
            return Ok(if holders.is_empty() || still_queued {
                Keyed::Fresh(digest)
            } else {
                Keyed::Answered(SendOutcome::Error(unserved(
                    "a retry of a steered, retracted or ended send",
                )))
            });
        };
        // The delivery order is part of a keyed send's identity: a run an ordered send started
        // is answered only to a retry asking for the same order, and a run a plain send started
        // only to a plain retry.
        let earlier = submission.iter().find_map(|(_, record)| match record {
            Record::DeliveryOrdered(ordered) => Some(&ordered.order),
            _ => None,
        });
        let same_order = match (earlier, order) {
            (None, None) => true,
            (Some(earlier), Some(retry)) => {
                serde_json::from_value::<broca_protocol::DeliveryOrder>(earlier.clone())
                    .map_err(|error| format!("the send's recorded order: {error}"))
                    .map(|earlier| crate::actor::same_order(retry, &earlier))?
            }
            _ => false,
        };
        if !same_order {
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
                // A run paused for sign-in is resumed by its retry, as in today's actor; whether the
                // credential is ready now is the executor's to decide. Runner-owned pauses are
                // reported. Today's actor also drives an unfinished run that nothing runs, which
                // is boot recovery's to do here.
                (None, Some(broca_protocol::PauseReason::AuthRequired)) => {
                    return Ok(Keyed::Resume)
                }
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

    /// Marks the run started once its start is in the session, and tells a waiting sender it
    /// is active.
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
            active.started = true;
            if let Some(reply) = active.admitting.take() {
                let _ = reply.send(SendOutcome::Active {
                    run_id: active.run_id.clone(),
                    delivered: None,
                });
            }
        }
    }

    /// The run's task ended: answer a sender still waiting, whose run never started, and start
    /// the next queued send. A drained run refused at admission ends its queued send with the
    /// refusal, as today's actor ends it, and the next one is tried.
    async fn finish_run(&mut self, end: Result<StoreRunEnd, tokio::task::JoinError>) {
        let Some(active) = self.run.take() else {
            return;
        };
        // Whatever the run's start stored of its send's media is the session's now; a run that
        // never wrote its start leaves nothing for the next one.
        self.held.clear();
        let result = match end {
            Ok(end) => end.result,
            Err(error) => Err(StoreRunError::Admission(broca_core::RunError::Config(
                format!("the run's task ended without finishing: {error}"),
            ))),
        };
        let started = self
            .session
            .operations()
            .run
            .as_ref()
            .and_then(|run| run.start.as_ref())
            .is_some_and(|start| start.run_id == active.run_id.0);
        if let (Some(submission), false, Err(error)) = (&active.submission, started, &result) {
            let error = match error {
                StoreRunError::Admission(error) => error.clone(),
                StoreRunError::Loop(error) => broca_core::RunError::Config(format!("{error:?}")),
            };
            if let Err(error) = self
                .end_refused_drain(submission, &active.run_id, error)
                .await
            {
                // The send stays queued, and the queue behind it waits.
                tracing::error!(
                    target: "broca.store_actor",
                    session = %self.key.session,
                    submission_id = %submission.0,
                    error = %error,
                    "a refused queued send could not be ended",
                );
                if let Some(reply) = active.admitting {
                    let _ = reply.send(SendOutcome::Error(error));
                }
                return;
            }
            // A send whose sender waited on the run it drained into was queued first: as in
            // today's actor, it stays answered `pending`, now ended with the refusal.
            if let Some(reply) = active.admitting {
                let _ = reply.send(SendOutcome::Pending {
                    submission_id: submission.clone(),
                });
            }
        } else if let Some(reply) = active.admitting {
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
        // The run's end, if it wrote one, is durable now: a run's last batch ends with a
        // barrier, and the run's task has returned.
        self.bill_owed().await;
        self.drain().await;
    }

    /// Ends a queued send whose run was refused at admission: its retraction carries the
    /// refusal, as today's actor writes it, so the sender's status reads why.
    async fn end_refused_drain(
        &mut self,
        submission: &broca_protocol::SubmissionId,
        run_id: &RunId,
        error: broca_core::RunError,
    ) -> Result<(), String> {
        let failure = broca_wal::record::QueuedAdmissionFailure {
            run_id: run_id.clone(),
            error: crate::serve::admission_refusal::queued_admission_error(error),
        };
        let record = Record::Retraction(Retraction {
            submission_id: submission.0.clone(),
            failure: Some(serde_json::to_value(failure).map_err(|error| error.to_string())?),
        });
        Writer::append(&mut self.durable(), std::slice::from_ref(&record), true)
            .await
            .map(drop)
            .map_err(|error| format!("{error:?}"))
    }

    /// Starts the first queued send that starts a run when the session is idle and its latest
    /// run ended, as today's actor drains its queue at a run's end. Queued ordered sends whose
    /// expiry passed end first, and an ordered send whose list doesn't name `idle_start` is
    /// passed over: it waits for a run other work starts. A paused run holds the queue. The
    /// run's end comes back through `finish_run`, which drains again.
    async fn drain(&mut self) {
        self.drain_for(None).await;
    }

    /// [`Self::drain`], with the sender of the queued send `waiting` waiting on it: that sender
    /// hears `active` once the run its send starts is durable, and `pending` when the drain
    /// starts no run for it.
    pub(super) async fn drain_for(
        &mut self,
        mut waiting: Option<(broca_protocol::SubmissionId, oneshot::Sender<SendOutcome>)>,
    ) {
        Box::pin(self.drain_once(&mut waiting)).await;
        if let Some((submission_id, reply)) = waiting {
            let _ = reply.send(SendOutcome::Pending { submission_id });
        }
    }

    async fn drain_once(
        &mut self,
        waiting: &mut Option<(broca_protocol::SubmissionId, oneshot::Sender<SendOutcome>)>,
    ) {
        if self.stopping
            || self.run.is_some()
            || self
                .session
                .operations()
                .run
                .as_ref()
                .is_some_and(|run| run.finished.is_none())
        {
            return;
        }
        if let Err(error) = self.expire_ordered().await {
            // The sends stay queued, and the queue waits for the next drain.
            tracing::error!(
                target: "broca.store_actor",
                session = %self.key.session,
                error = %error,
                "expired ordered sends could not be ended",
            );
            return;
        }
        let mut first = None;
        for pending in self.session.operations().queue.clone() {
            if !pending.ordered {
                first = Some(pending);
                break;
            }
            match self.queued_order(&pending.submission_id).await {
                Ok(Some(queued)) if queued.order.starts_runs() => {
                    first = Some(pending);
                    break;
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::error!(
                        target: "broca.store_actor",
                        session = %self.key.session,
                        submission_id = %pending.submission_id,
                        error = %error,
                        "a queued ordered send could not be read back",
                    );
                    return;
                }
            }
        }
        let Some(first) = first else {
            return;
        };
        // An interrupt is first in the queue (`Operations::queue` keeps interrupts ahead of every
        // plain send), so it drains first, as today's queue drains it.
        match self.drained_start(&first.submission_id).await {
            Ok((mut start, mut drained)) => match self.freeze_plan(&mut start) {
                Ok(()) => {
                    start.prepared.hooks = crate::actor::pre_user::drained_hooks(
                        drained.hooks.take(),
                        &start.request.input,
                    );
                    let reply = waiting
                        .take_if(|(id, _)| *id == drained.submission)
                        .map(|(_, reply)| reply);
                    // The run's input points at the media the queued record stored.
                    self.held = std::mem::take(&mut drained.media);
                    self.start_run(start, Some(drained), reply, StoreRunMode::Fresh)
                        .await
                }
                Err(outcome) => {
                    // The send can't start under the session's plan: it ends with the refusal,
                    // as a drained send refused at admission ends, and the next one is tried.
                    let error = broca_core::RunError::Config(format!("{outcome:?}"));
                    match self
                        .end_refused_drain(&drained.submission, &start.run_id, error)
                        .await
                    {
                        Ok(()) => Box::pin(self.drain_once(waiting)).await,
                        Err(error) => tracing::error!(
                            target: "broca.store_actor",
                            session = %self.key.session,
                            submission_id = %drained.submission.0,
                            error = %error,
                            "a queued send refused by the session's plan could not be ended",
                        ),
                    }
                }
            },
            Err(error) => tracing::error!(
                target: "broca.store_actor",
                session = %self.key.session,
                submission_id = %first.submission_id,
                error = %error,
                "a queued send could not be read back to start its run",
            ),
        }
    }

    /// The run a queued send drains into, as today's actor builds it from the queued entry: the
    /// request it was queued with, under the principal, sender, title and key its record kept.
    async fn drained_start(&self, submission_id: &str) -> Result<(Start, Drained), String> {
        let send = self
            .queued(submission_id)
            .await?
            .ok_or_else(|| format!("the queued send {submission_id} has no record"))?;
        let principal = |name: &str| {
            queue::receipt_field(&send, name)
                .map(|value| serde_json::from_value(value.clone()))
                .transpose()
                .map_err(|error| format!("the queued send's {name}: {error}"))
        };
        let mut request = queue::request_of(&send, &self.parts)?;
        request.principal = principal("principal")?;
        let identity = match queue::receipt_field(&send, "request_identity_digest") {
            Some(serde_json::Value::String(digest)) => digest.clone(),
            // A send queued without a digest gets one computed from its request, as today.
            _ => broca_core::submission_identity_digest(&request),
        };
        let submission = broca_protocol::SubmissionId(submission_id.to_owned());
        let start = Start {
            run_id: crate::actor::run_id_of_queued(&submission),
            title: queue::receipt_field(&send, "title")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            send_id: send.send_id.clone(),
            identity: Some(identity),
            sent_by: principal("sent_by")?,
            request,
            // A drained send is broca's own, so it carries the scope the session already froze,
            // as today's drain (`SendScope::Internal`) carries it.
            scope: self.ownership()?.scope,
            manifest: self.frozen_manifest()?,
            session_class: self.frozen_session_class(),
            prepared: Prepared::default(),
        };
        // The send's transforms ran when it was queued; its record keeps what they did, and
        // their final value is the input the run sends.
        let hooks = queue::receipt_field(&send, "hooks")
            .map(|hooks| serde_json::from_value(hooks.clone()))
            .transpose()
            .map_err(|error| format!("the queued send's hooks: {error}"))?;
        let written_at_ms = send.written_at_ms;
        let mut media = broca_session_loop::HeldInputMedia::default();
        media.hold_stored(&Record::QueuedSend(send));
        Ok((
            start,
            Drained {
                submission,
                written_at_ms,
                hooks,
                media,
            },
        ))
    }

    /// The record of `submission_id` while it waits in the queue.
    async fn queued(&self, submission_id: &str) -> Result<Option<QueuedSend>, String> {
        if !self
            .session
            .operations()
            .queue
            .iter()
            .any(|pending| pending.submission_id == submission_id)
        {
            return Ok(None);
        }
        let records = self
            .session
            .find_id(IdKind::SubmissionId, submission_id)
            .await
            .map_err(|error| format!("{error:?}"))?;
        Ok(records
            .into_iter()
            .rev()
            .find_map(|(_, record)| match record {
                Record::QueuedSend(send) => Some(send),
                _ => None,
            }))
    }

    /// Withdraws a queued send before it runs, by today's rule (`broca_core::retract_decision`):
    /// a send a run's start already names is too late, one still queued gets a `Retraction`,
    /// durable before the reply, and anything else is not pending.
    async fn retract(&mut self, submission_id: broca_protocol::SubmissionId) -> RetractOutcome {
        let records = match self
            .session
            .find_id(IdKind::SubmissionId, &submission_id.0)
            .await
        {
            Ok(records) => records,
            Err(error) => return RetractOutcome::Error(format!("{error:?}")),
        };
        if records
            .iter()
            .any(|(_, record)| matches!(record, Record::RunStart(_)))
        {
            return RetractOutcome::AlreadyStarted;
        }
        let Some(pending) = self
            .session
            .operations()
            .queue
            .iter()
            .find(|pending| pending.submission_id == submission_id.0)
        else {
            return RetractOutcome::NotPending;
        };
        // Today's actor withdraws an ordered send through the running run, which may be
        // delivering it at that moment; withdrawing one here isn't built yet.
        if pending.ordered {
            return RetractOutcome::Error(unserved("retracting an ordered send"));
        }
        let record = Record::Retraction(Retraction {
            submission_id: submission_id.0,
            failure: None,
        });
        match Writer::append(&mut self.durable(), std::slice::from_ref(&record), true).await {
            Ok(seq) => {
                if let Some(active) = self.run.as_mut() {
                    active.link.note_own(seq, &[record]);
                }
                RetractOutcome::Retracted
            }
            Err(error) => RetractOutcome::Error(format!("{error:?}")),
        }
    }

    /// Appends `record`, which this actor decided on its own, durable before it returns, and
    /// tells a running run's copy of the folds about it.
    async fn append_own(&mut self, record: Record) -> Result<(), String> {
        let seq = Writer::append(&mut self.durable(), std::slice::from_ref(&record), true)
            .await
            .map_err(|error| format!("{error:?}"))?;
        if let Some(active) = self.run.as_mut() {
            active.link.note_own(seq, &[record]);
        }
        Ok(())
    }

    /// The plan-change facts the session holds, for a request naming `generation` and
    /// `send_id`.
    async fn change_facts(
        &self,
        generation: Option<u64>,
        send_id: Option<&str>,
    ) -> Result<crate::plan_change::ChangeFacts, String> {
        crate::store_plan_change::facts(&self.session, &self.parts, generation, send_id).await
    }

    /// Accepts a plan refresh the serve layer fetched, by the checks today's actor makes where
    /// it writes (`crate::plan_change::accept_refresh`): an identical repeat is answered with its
    /// original flags, and a new refresh is answered once its record, with the fetched
    /// manifest's parts, is durable.
    async fn session_refresh(
        &mut self,
        generation: u64,
        policy: String,
        manifest: Box<broca_wal::PlanManifest>,
    ) -> crate::actor::SessionRefreshOutcome {
        use crate::actor::SessionRefreshOutcome;
        use crate::plan_change::RefreshAcceptance;
        let facts = match self.change_facts(Some(generation), None).await {
            Ok(facts) => facts,
            Err(error) => return SessionRefreshOutcome::Error(error),
        };
        let (run_in_progress, applies_from_next_run) = match crate::plan_change::accept_refresh(
            &facts,
            generation,
            &policy,
            &manifest,
            self.run.is_some(),
        ) {
            RefreshAcceptance::Repeat {
                run_in_progress,
                applies_from_next_run,
            } => {
                return SessionRefreshOutcome::Accepted {
                    run_in_progress,
                    applies_from_next_run,
                }
            }
            RefreshAcceptance::Refused(refusal) => return SessionRefreshOutcome::Refused(refusal),
            RefreshAcceptance::Accept {
                run_in_progress,
                applies_from_next_run,
            } => (run_in_progress, applies_from_next_run),
        };
        let mut setup = Vec::new();
        let rest =
            match broca_session_loop::run::manifest_parts(&manifest, &mut self.parts, &mut setup) {
                Ok(rest) => rest,
                Err(error) => return SessionRefreshOutcome::Error(format!("{error:?}")),
            };
        let record = Record::SessionRefresh(broca_session_store::record::SessionRefresh {
            generation,
            policy,
            run_in_progress,
            applies_from_next_run,
            setup,
            manifest: rest,
            selection: broca_session_loop::plan::recorded_selection(&manifest),
        });
        match self.append_own(record).await {
            Ok(()) => SessionRefreshOutcome::Accepted {
                run_in_progress,
                applies_from_next_run,
            },
            Err(error) => SessionRefreshOutcome::Error(error),
        }
    }

    /// Changes the pending refresh's policy, by today's checks (`crate::plan_change::check_policy`):
    /// a repeat under the same `send_id` is answered as before, and a new change is answered once
    /// its record is durable.
    async fn session_refresh_policy(
        &mut self,
        generation: u64,
        policy: String,
        send_id: String,
    ) -> crate::actor::SessionRefreshPolicyOutcome {
        use crate::actor::SessionRefreshPolicyOutcome;
        let facts = match self.change_facts(Some(generation), Some(&send_id)).await {
            Ok(facts) => facts,
            Err(error) => return SessionRefreshPolicyOutcome::Error(error),
        };
        match crate::plan_change::check_policy(&facts, generation, &policy, &send_id) {
            Ok(true) => return SessionRefreshPolicyOutcome::Changed,
            Err(refusal) => return SessionRefreshPolicyOutcome::Refused(refusal),
            Ok(false) => {}
        }
        let record = Record::RefreshPolicy(broca_session_store::record::RefreshPolicy {
            generation,
            policy,
            send_id,
        });
        match self.append_own(record).await {
            Ok(()) => SessionRefreshPolicyOutcome::Changed,
            Err(error) => SessionRefreshPolicyOutcome::Error(error),
        }
    }

    /// Flushes the session's frozen prefix, so the next run applies any pending refresh, by
    /// today's rule (`crate::plan_change::flush`): a repeat under the same `send_id` is answered
    /// with the pending generation it reported, and a new flush is answered once its record is
    /// durable.
    async fn session_flush_prefix(
        &mut self,
        send_id: String,
    ) -> crate::actor::SessionFlushPrefixOutcome {
        use crate::actor::SessionFlushPrefixOutcome;
        let facts = match self.change_facts(None, Some(&send_id)).await {
            Ok(facts) => facts,
            Err(error) => return SessionFlushPrefixOutcome::Error(error),
        };
        let pending_generation = match crate::plan_change::flush(&facts) {
            Err(pending_generation) => {
                return SessionFlushPrefixOutcome::Flushed { pending_generation }
            }
            Ok(pending_generation) => pending_generation,
        };
        let record = Record::PrefixFlush(broca_session_store::record::PrefixFlush {
            send_id,
            pending_generation,
        });
        match self.append_own(record).await {
            Ok(()) => SessionFlushPrefixOutcome::Flushed { pending_generation },
            Err(error) => SessionFlushPrefixOutcome::Error(error),
        }
    }

    /// Joins `start` to the running run as a steer, as today's actor steers it: refused when it
    /// names a setting the run froze otherwise, and queued as a steer when the run has already
    /// written its end. Otherwise its input goes through the session's `pre_user` transforms,
    /// and its record is written, durable before the reply, and the run places its messages at
    /// its next step boundary. The actor writes nothing while the run builds a batch, so the run
    /// either takes the steer or has ended before it. `reply` is the sender's, which the
    /// transforms stop for when it stops waiting.
    async fn steer(
        &mut self,
        start: Start,
        stop_sequences_named: bool,
        reply: &mut oneshot::Sender<SendOutcome>,
    ) -> SendOutcome {
        let Some(active) = self.run.as_ref().map(|active| active.run_id.clone()) else {
            return SendOutcome::Error("a steer needs a running run".into());
        };
        let run = self.session.operations().run.as_ref();
        if run.is_some_and(|run| run.finished.is_some()) {
            return self
                .queue(start, broca_protocol::Delivery::Steer, reply)
                .await;
        }
        let Some(run_start) = run.and_then(|run| run.start.as_ref()) else {
            return SendOutcome::Transient("the running run has no start yet".into());
        };
        // This reads the run's start: a refreeze (`ConfigRefrozen`, `AuthRefrozen`) is written
        // only when a paused run resumes, which this actor doesn't do.
        let frozen = match broca_session_loop::run::admission_of_start(run_start, &self.parts) {
            Ok(frozen) => frozen,
            Err(error) => return SendOutcome::Error(format!("{error:?}")),
        };
        let settings = crate::actor::SteerFrozen {
            model: &frozen.model,
            variant: frozen.variant.as_deref(),
            generation: &frozen.generation,
            stop_when: &frozen.stop_when,
            directives: frozen.directives.as_deref(),
            auth_selection: frozen.auth_selection.as_ref(),
        };
        if let Some(field) =
            crate::actor::steer_settings_conflict(&start.request, settings, stop_sequences_named)
        {
            return SendOutcome::SteerSettingsConflict { field };
        }
        // As today's actor steers, the attachments are checked against the media the running
        // run's model takes, as its start froze them.
        let mut start = start;
        let staged = match self
            .stage_media(&mut start.request, Some(frozen_media(&frozen)))
            .await
        {
            Ok(staged) => staged,
            Err(outcome) => return outcome,
        };
        // The running run already has its system prompt; a steer adds conversation only.
        let input = start
            .request
            .input
            .iter()
            .filter(|message| message.role != broca_protocol::content::Role::System)
            .cloned()
            .collect();
        let manifest = match self.frozen_manifest() {
            Ok(manifest) => manifest,
            Err(error) => return SendOutcome::Error(error),
        };
        let prepared = match self
            .transform_input(
                input,
                broca_protocol::Delivery::Steer,
                manifest.as_deref(),
                None,
                broca_core::run::epoch_millis(),
                reply,
            )
            .await
        {
            Ok(prepared) => prepared,
            Err(outcome) => return outcome,
        };
        if let Some(provider) = prepared.refused {
            return SendOutcome::PreUserUnavailable { provider };
        }
        let receipt = queue::SteerReceipt {
            sent_by: start.sent_by,
            send_id: start.send_id,
            request_identity_digest: start.identity,
            title: start.title,
            delivery: Some(broca_protocol::Delivery::Steer),
            hooks: prepared.hooks,
            ..queue::SteerReceipt::default()
        };
        let record = match queue::steer(prepared.input, prepared.at_ms, &receipt) {
            Ok(steer) => Record::Steer(steer),
            Err(error) => return SendOutcome::Error(error),
        };
        if let Err(error) = self.append_own_with_media(record, &staged).await {
            return SendOutcome::Error(format!("{error:?}"));
        }
        SendOutcome::Active {
            run_id: active,
            delivered: None,
        }
    }

    /// Queues `start` behind the latest run, which is running or paused, as today's actor
    /// queues it: durable before the reply, which is `pending`. A retry of a send still queued
    /// is compared with its record instead. A steer queues with its mode, which today's queued
    /// record keeps; a plain send's record names none. A new send's input goes through the
    /// session's `pre_user` transforms first; the record keeps the request as sent, and what
    /// they did, with their final input, in its receipt. `reply` is the sender's, which the
    /// transforms stop for when it stops waiting.
    async fn queue(
        &mut self,
        start: Start,
        delivery: broca_protocol::Delivery,
        reply: &mut oneshot::Sender<SendOutcome>,
    ) -> SendOutcome {
        let Start {
            run_id,
            mut request,
            title,
            send_id,
            identity,
            sent_by,
            scope: _,
            // A queued send keeps its request as sent; the plan is frozen into it when it
            // drains, as today's queue freezes it at the episode's start.
            manifest: _,
            session_class: _,
            prepared: _,
        } = start;
        let submission_id = broca_protocol::SubmissionId(format!("sub-{}", run_id.0));
        match self.queued(&submission_id.0).await {
            Err(error) => return SendOutcome::Error(error),
            Ok(Some(existing)) => {
                // A plain retry of an ordered send asks for another delivery.
                match self.queued_order(&submission_id.0).await {
                    Err(error) => return SendOutcome::Error(error),
                    Ok(Some(_)) => {
                        return SendOutcome::SendIdReuse {
                            differing: "delivery",
                        }
                    }
                    Ok(None) => {}
                }
                return retry_of_queued(
                    &existing,
                    request,
                    identity.as_deref(),
                    delivery,
                    &self.parts,
                    submission_id,
                );
            }
            Ok(None) => {}
        }
        // As today's actor queues a send, its attachments are staged into its input and checked
        // before its input transforms, and the queued record keeps them.
        let staged = match self.stage_media(&mut request, None).await {
            Ok(staged) => staged,
            Err(outcome) => return outcome,
        };
        let manifest = match self.frozen_manifest() {
            Ok(manifest) => manifest,
            Err(error) => return SendOutcome::Error(error),
        };
        let prepared = match self
            .transform_input(
                request.input.clone(),
                delivery,
                manifest.as_deref(),
                None,
                broca_core::run::epoch_millis(),
                reply,
            )
            .await
        {
            Ok(prepared) => prepared,
            Err(outcome) => return outcome,
        };
        if let Some(provider) = prepared.refused.clone() {
            return SendOutcome::PreUserUnavailable { provider };
        }
        request.staged_uploads = None;
        let receipt = queue::Receipt {
            sent_by,
            principal: request.principal.clone(),
            request_identity_digest: identity,
            title,
            hooks: prepared.queued_hooks(),
            ..queue::Receipt::default()
        };
        let at = queue::Queued {
            written_at_ms: prepared.at_ms,
            delivery: (delivery != broca_protocol::Delivery::Queue).then_some(delivery),
            ..queue::Queued::default()
        };
        let record = match queue::queued_send(
            &submission_id.0,
            send_id.as_deref(),
            &request,
            &receipt,
            at,
            &mut self.parts,
        ) {
            Ok(send) => Record::QueuedSend(send),
            Err(error) => return SendOutcome::Error(error),
        };
        if let Err(error) = self.append_own_with_media(record, &staged).await {
            return SendOutcome::Error(format!("{error:?}"));
        }
        SendOutcome::Pending { submission_id }
    }

    /// Stages the attachments a send's `content` names into its last user message, as today's
    /// actor stages them, and checks every medium its input then holds against the media its
    /// model takes: `frozen`'s profile and model for a steer, which joins a running run, else the
    /// profile the executor quotes for the send's model. Returns the staged bytes, or the
    /// refusal today's actor answers.
    async fn stage_media(
        &self,
        request: &mut SubmissionRequest,
        frozen: Option<(
            Option<Box<broca_provider::media_profile::MediaProfile>>,
            String,
        )>,
    ) -> Result<Vec<crate::person_media::StagedMedia>, SendOutcome> {
        let staged = crate::person_media::stage(self.uploads.as_ref(), &self.key, request)
            .map_err(crate::person_media::outcome)?;
        let (profile, model) = match frozen {
            Some(frozen) => frozen,
            None => {
                let profile = if broca_provider::person_media::references(&request.input)
                    .next()
                    .is_some()
                {
                    self.executor
                        .media_profile(request)
                        .await
                        .map_err(crate::person_media::outcome)?
                } else {
                    None
                };
                let model = format!(
                    "{}/{}",
                    request.model.provider_module_id, request.model.model_id
                );
                (profile, model)
            }
        };
        crate::person_media::check(profile.as_deref(), &model, &request.input)
            .map_err(crate::person_media::outcome)?;
        Ok(staged)
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
        // The writer is built from its fields here, not by `durable`, because `active` still
        // borrows the run.
        let mut writer = broca_session_loop::durable::Durable {
            writer: &mut self.session,
            parts: &mut self.parts,
            store: self.storage.as_ref(),
            media: None,
        };
        match Writer::append(&mut writer, std::slice::from_ref(&record), true).await {
            Ok(seq) => active.link.note_own(seq, &[record]),
            Err(error) => return CancelOutcome::Error(format!("{error:?}")),
        }
        if point == CancelPoint::Now {
            active.cancel.cancel();
        }
        CancelOutcome::Ack
    }
}

/// A retry of a send still queued, compared with its record as today's actor compares it with
/// the queued entry: the same payload is `pending`, and anything else is refused naming what
/// differs. A retry that omits the routing decision or tier its first send fixed takes them
/// from the record, when the two sends' identities are the same.
fn retry_of_queued(
    existing: &QueuedSend,
    mut request: SubmissionRequest,
    identity: Option<&str>,
    delivery: broca_protocol::Delivery,
    parts: &dyn Parts,
    submission_id: broca_protocol::SubmissionId,
) -> SendOutcome {
    let reuse = |differing| SendOutcome::SendIdReuse { differing };
    if queue::receipt_field(existing, "conversation_key").is_some() {
        return reuse("conversation_key");
    }
    // A plain send's record names no mode, as `queue` writes it.
    let mode = (delivery != broca_protocol::Delivery::Queue)
        .then(|| serde_json::to_value(delivery).ok())
        .flatten()
        .and_then(|mode| mode.as_str().map(str::to_owned));
    if existing.delivery != mode {
        return reuse("delivery");
    }
    let stored = queue::receipt_field(existing, "request_identity_digest")
        .and_then(serde_json::Value::as_str);
    // The queued request doesn't keep the client message id; the digest does.
    if stored.and_then(broca_core::digest_client_message_id) != request.client_message_id {
        return reuse("client_message_id");
    }
    let queued = match queue::request_of(existing, parts) {
        Ok(queued) => queued,
        Err(error) => return SendOutcome::Error(error),
    };
    let same_send = identity.is_some() && stored == identity;
    if same_send && request.route_decision_id.is_none() {
        request
            .route_decision_id
            .clone_from(&queued.route_decision_id);
    }
    if same_send && broca_protocol::service_tier_enabled() && request.service_tier_level.is_none() {
        request
            .service_tier_level
            .clone_from(&queued.service_tier_level);
    }
    match broca_core::submission_difference(&queued, &request) {
        None => SendOutcome::Pending { submission_id },
        Some(differing) => reuse(differing),
    }
}

/// What opening the session does with its unfinished latest run `run` before it takes any
/// command: seal it as a crash's orphan, or resume it to its end. `None` leaves it as it is: a
/// run paused, or waiting on calls a provider holds, whose recovery isn't built for this storage
/// yet. A read of the session that no actor holds predicts the run's end from this same decision
/// (`store_status`), so what it serves is what the next open records.
pub(crate) fn recovery_mode(
    run: &broca_session_store::fold::operations::Run,
) -> Option<StoreRunMode> {
    use broca_session_store::fold::operations::Resolution;
    match run.resolution() {
        Resolution::Resume | Resolution::Interrupted(_) => Some(match run.cancel {
            None => StoreRunMode::Seal(broca_core::resume::SealCause::CrashOrphan),
            Some(_) => StoreRunMode::Resume,
        }),
        Resolution::Idle(_) => Some(StoreRunMode::Resume),
        Resolution::ProviderHeld(_) | Resolution::Paused(_) => None,
    }
}

fn finished(run_id: &RunId, reason: broca_protocol::RunFinishReason) -> SendOutcome {
    SendOutcome::Finished {
        run_id: run_id.clone(),
        reason,
        delivered: None,
    }
}

/// Whether a send creating a session asks only for what this actor serves, so the session may
/// start in the new storage. It makes the same checks the actor makes on a send before writing
/// anything; a send failing one creates its session on today's storage instead, where it is
/// served. A send carrying media still creates its session there too: a session's first send
/// carrying media isn't yet proven against today's on the new storage, though later photo sends
/// and the runs rendering them are.
pub fn serves_creating_send(
    request: &SubmissionRequest,
    kind: RunKind,
    delivery: broca_protocol::Delivery,
    conversation_key: bool,
    retention: bool,
    ordered: bool,
) -> bool {
    unserved_delivery(ordered, delivery, conversation_key).is_none()
        && unserved_request(request, kind, retention).is_none()
        && !carries_media(request)
}

/// The media a run's model takes, as the run's start froze them, and the model's name, which a
/// refusal names: what a send joining that run is checked against.
fn frozen_media(
    frozen: &broca_session_loop::Admission,
) -> (
    Option<Box<broca_provider::media_profile::MediaProfile>>,
    String,
) {
    let profile = serde_json::from_value::<broca_provider::wire::FrozenRenderConfig>(
        frozen.render_config.clone(),
    )
    .ok()
    .and_then(|config| config.media.map(Box::new));
    let model = format!(
        "{}/{}",
        frozen.model.provider_module_id, frozen.model.model_id
    );
    (profile, model)
}

/// The delivery a send creating a session asks for that keeps the new session out of the new
/// storage, which then starts on today's: an ordered list or an interrupt. This actor serves both
/// only on a session it already holds.
fn unserved_delivery(
    ordered: bool,
    delivery: broca_protocol::Delivery,
    conversation_key: bool,
) -> Option<&'static str> {
    if ordered {
        Some("an ordered delivery")
    } else if delivery == broca_protocol::Delivery::Interrupt {
        Some("an interrupt")
    } else {
        unserved_route(conversation_key)
    }
}

/// How a send asks to be routed, where this actor doesn't serve it yet.
fn unserved_route(conversation_key: bool) -> Option<&'static str> {
    // Routing reads a conversation key when it picks the run's account, which a store session's
    // runs don't do yet.
    conversation_key.then_some("a send naming a conversation key")
}

/// What a send's request asks for, where this actor doesn't serve it yet.
fn unserved_request(
    request: &SubmissionRequest,
    kind: RunKind,
    retention: bool,
) -> Option<&'static str> {
    if retention {
        Some("a send setting retention")
    } else if kind == RunKind::ResumeAfterAuth {
        Some("resuming after sign-in")
    } else {
        refuse_unbuilt_request(request)
    }
}

/// The request fields a store session doesn't take yet, refused by name before anything is
/// written.
fn refuse_unbuilt_request(request: &SubmissionRequest) -> Option<&'static str> {
    if request.keep_warm.is_some() {
        Some("keep-warm")
    } else if request.service_tier_level.is_some() {
        Some("a service tier level")
    } else {
        None
    }
}

/// Whether `request` carries media: attachments its `content` names, or media in its input.
fn carries_media(request: &SubmissionRequest) -> bool {
    !request.content.is_empty()
        || broca_provider::person_media::references(&request.input)
            .next()
            .is_some()
}

/// Answers a command that raced the session's close, as today's actor answers one: the caller
/// re-routes through the registry and reaches the reopened session.
fn reopen(command: LineageCmd) {
    const CLOSING: &str = "session actor is closing";
    match command {
        LineageCmd::RepairRetention { reply }
        | LineageCmd::ResumeRestart { reply }
        | LineageCmd::ResumeHeld { reply } => {
            let _ = reply.send(Err(CLOSING.into()));
        }
        LineageCmd::Send { reply, .. } => {
            let _ = reply.send(SendOutcome::Reopen);
        }
        LineageCmd::Import { reply, .. } => {
            let _ = reply.send(crate::actor::ImportOutcome::Reopen);
        }
        LineageCmd::Store { reply, .. } => {
            let _ = reply.send(crate::store_import::StoreOutcome::Reopen);
        }
        LineageCmd::StoreRead { reply, .. } => {
            let _ = reply.send(crate::store_reads::StoreReadOutcome::Reopen);
        }
        LineageCmd::Cancel { reply, .. } | LineageCmd::CancelFrom { reply, .. } => {
            let _ = reply.send(CancelOutcome::Reopen);
        }
        LineageCmd::SenderPolicy { reply, .. } => {
            let _ = reply.send(Ok(None));
        }
        LineageCmd::Retract { reply, .. } => {
            let _ = reply.send(RetractOutcome::Reopen);
        }
        LineageCmd::SessionRefresh { reply, .. } => {
            let _ = reply.send(crate::actor::SessionRefreshOutcome::Reopen);
        }
        LineageCmd::SessionRefreshPolicy { reply, .. } => {
            let _ = reply.send(crate::actor::SessionRefreshPolicyOutcome::Reopen);
        }
        LineageCmd::SessionFlushPrefix { reply, .. } => {
            let _ = reply.send(crate::actor::SessionFlushPrefixOutcome::Reopen);
        }
        LineageCmd::Status { reply } => {
            let _ = reply.send(LineageStatus::Closing);
        }
        LineageCmd::RunState { reply, .. } => {
            let _ = reply.send(Err(CLOSING.into()));
        }
        LineageCmd::Subscribe { reply, .. } => {
            let _ = reply.send(crate::actor::SubscribeOutcome::Reopen);
        }
        LineageCmd::Warm { reply, .. } => {
            let _ = reply.send(crate::actor::WarmOutcome::Reopen);
        }
        LineageCmd::CutLock { reply, .. } => drop(reply),
        // Today's actor sends these two to itself; nothing sends them to this one.
        LineageCmd::WarmFinished { .. }
        | LineageCmd::RunFinished { .. }
        | LineageCmd::IdleTick
        | LineageCmd::CompactionChanged { .. }
        | LineageCmd::ApplyCompaction { .. }
        | LineageCmd::CompactionV2 { .. } => {}
        #[cfg(test)]
        LineageCmd::RetainedAdmissionIdentityState { reply } => {
            let _ = reply.send((usize::MAX, usize::MAX));
        }
        #[cfg(test)]
        LineageCmd::PanicInActor => {}
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
        // Served by `handle`; never refused here.
        LineageCmd::Store { reply, .. } => {
            let _ = reply.send(crate::store_import::StoreOutcome::Reopen);
        }
        LineageCmd::StoreRead { reply, .. } => {
            let _ = reply.send(crate::store_reads::StoreReadOutcome::Reopen);
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
        | LineageCmd::Retract { .. }
        | LineageCmd::SenderPolicy { .. }
        | LineageCmd::SessionRefresh { .. }
        | LineageCmd::SessionRefreshPolicy { .. }
        | LineageCmd::SessionFlushPrefix { .. }
        | LineageCmd::IdleTick => unreachable!("served by the actor"),
    }
}

mod apply_view;
mod import;
mod ordered;
mod warm;

#[cfg(test)]
mod billing_tests;
#[cfg(test)]
mod differential;
#[cfg(test)]
mod entries_comparison;
#[cfg(test)]
mod import_tests;
#[cfg(test)]
mod key_tests;
#[cfg(test)]
mod media_wait_tests;
#[cfg(test)]
mod read_cost;
#[cfg(test)]
mod reads_tests;
#[cfg(test)]
mod status_tests;
#[cfg(test)]
mod subscribe_tests;
#[cfg(test)]
mod tests;
