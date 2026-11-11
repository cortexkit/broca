//! The same sends through today's session actor and the store actor, compared reply by reply.
//!
//! Today's side is the module's real path: a registry over a file lease store and a file WAL,
//! today's actor, and today's loop ([`broca_core::run_loop_on_writer_admitted_with_prior_snapshot`])
//! over a scripted provider. The store side is [`super::StoreActor`] over the new loop and the
//! same script. Both replies must be equal; what the store actor doesn't serve yet is pinned by
//! its own tests, each refused by name.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use broca_core::{
    run_loop_on_writer_admitted_with_prior_snapshot, AllowAll, FileLeaseStore, FileWalStore,
    OutputEvent, RunError, RunSummary, StartMode, WalStore,
};
use broca_protocol::content::ContentBlock;
use broca_protocol::{BindIdentity, NormalizedEvent, RunId, SubmissionRequest};
use broca_session_store::backend::MemoryBackend;
use broca_session_store::session::Session;
use tokio::sync::{mpsc, Notify};

use super::tests::{auth_failure, executor, request, texts, Gated};
use crate::actor::{LineageStatus, SendKey, SendOutcome};
use crate::executor::{RunExecutor, RunJob, RunKind};
use crate::registry::LineageRegistry;

/// Turns async test functions that take a `Cipher` into tests run once in each mode, as the
/// session store's own tests are: each named function `f` becomes the tests `f::encrypted` and
/// `f::plain`, whose module creates new-storage sessions in that mode. A test function left out
/// of the list is never called, which the build refuses as dead code, so no test is silently
/// dropped.
macro_rules! cipher_tests {
    ($($name:ident),+ $(,)?) => {
        $(
            mod $name {
                #[tokio::test]
                async fn encrypted() {
                    super::$name(broca_session_store::keys::Cipher::XChaCha20Poly1305).await;
                }

                #[tokio::test]
                async fn plain() {
                    super::$name(broca_session_store::keys::Cipher::None).await;
                }
            }
        )+
    };
}

/// Today's executor in this harness: today's loop over the scripted provider, on the writer the
/// actor hands it.
pub(super) struct TodayLoop {
    pub(super) provider: Gated,
    pub(super) wal: Arc<FileWalStore>,
    /// The planes the executor offers the session's runs, the same ones the store side's offers.
    pub(super) planes: super::tests::Planes,
}

#[async_trait]
impl RunExecutor for TodayLoop {
    fn hook_plane(&self) -> Option<Arc<dyn broca_core::HookPlane>> {
        self.planes.hooks.clone()
    }

    fn compaction_plane(&self) -> Option<Arc<dyn broca_core::CompactionPlane>> {
        self.planes.compaction.clone()
    }

    async fn execute(
        &self,
        mut job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        // As the store side's executor does: the caller's context limit is the admitted one.
        job.control.admitted_context_limit =
            job.control.admitted_context_limit.or(job.req.context_limit);
        // As the serve executor picks it (`start_mode_after_auth`) when the run's credential
        // resolves, which this harness's always does: an ordinary caller's send, which arrives
        // as `FreshOrResume`, resumes a sign-in pause.
        let mode = match job.kind {
            RunKind::FreshOrResume | RunKind::ResumeAfterAuth => StartMode::ResumeAfterAuth,
            RunKind::AppendEpisode => StartMode::AppendEpisode,
        };
        let tools = super::tests::GatedTools::new("file contents", self.planes.tool_gate.clone());
        Box::pin(run_loop_on_writer_admitted_with_prior_snapshot(
            job.req,
            &job.writer,
            mode,
            &self.provider,
            &tools,
            &AllowAll,
            self.wal.as_ref(),
            None,
            job.control,
            &out,
            job.on_admitted,
            Some(&job.prior_snapshot),
        ))
        .await
    }
}

/// Today's module over a scratch folder, which it removes when dropped.
struct Today {
    dir: std::path::PathBuf,
    registry: LineageRegistry,
    wal: Arc<FileWalStore>,
    key: BindIdentity,
    /// The module's executor, whose provider records what each model call was sent.
    executor: Arc<TodayLoop>,
}

impl Drop for Today {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn today(steps: Vec<Vec<NormalizedEvent>>, gate: Option<Arc<Notify>>) -> Today {
    today_with(steps, gate, super::tests::Planes::default())
}

/// [`today`] with an executor that offers `planes`.
fn today_with(
    steps: Vec<Vec<NormalizedEvent>>,
    gate: Option<Arc<Notify>>,
    planes: super::tests::Planes,
) -> Today {
    today_opened(steps, gate, planes, false)
}

/// [`today`] whose WAL writer records each ride with its envelope (its call's key, sender and
/// forms), as the writer does once rides are enabled, where a ride is a notice and its text
/// isn't joined into the result. The new storage always records rides that way.
fn today_riding(
    steps: Vec<Vec<NormalizedEvent>>,
    gate: Option<Arc<Notify>>,
    planes: super::tests::Planes,
) -> Today {
    today_opened(steps, gate, planes, true)
}

/// A fresh scratch folder for one of today's modules.
fn scratch_dir() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "store-actor-differential-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn today_opened(
    steps: Vec<Vec<NormalizedEvent>>,
    gate: Option<Arc<Notify>>,
    planes: super::tests::Planes,
    rides: bool,
) -> Today {
    let dir = scratch_dir();
    let key = BindIdentity::new(dir.join("project"), "store-actor", "s1");
    today_at(dir, key, steps, gate, planes, rides)
}

/// Today's module over a copy of `crashed`'s log as it stands, as a process that died now and
/// started again finds it: no lease held, and nothing but the durable records. It rides as
/// [`today_riding`]'s module does.
fn today_restarted(crashed: &Today, steps: Vec<Vec<NormalizedEvent>>) -> Today {
    fn copy(from: &std::path::Path, to: &std::path::Path) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let target = to.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), target).unwrap();
            }
        }
    }
    let dir = scratch_dir();
    copy(&crashed.dir.join("wal"), &dir.join("wal"));
    today_at(
        dir,
        crashed.key.clone(),
        steps,
        None,
        super::tests::Planes::default(),
        true,
    )
}

fn today_at(
    dir: std::path::PathBuf,
    key: BindIdentity,
    steps: Vec<Vec<NormalizedEvent>>,
    gate: Option<Arc<Notify>>,
    planes: super::tests::Planes,
    rides: bool,
) -> Today {
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let executor = Arc::new(TodayLoop {
        provider: Gated {
            render_config: None,
            notes_sends: false,
            inner: broca_core::provider::StubProvider::new(steps),
            gate,
        },
        wal: Arc::clone(&wal),
        planes,
    });
    let mut opener = crate::opener::DefaultOpener::new(
        Arc::new(FileLeaseStore::new(dir.join("leases"))),
        wal.clone(),
        executor.clone(),
    )
    .with_idle_ttl(Duration::from_secs(60));
    if rides {
        opener = opener.with_ride_writer_for_test();
    }
    // A reader over the WAL alone: `session.read` serves from the log when no store is given.
    let registry = LineageRegistry::new_with_reader(Arc::new(opener), wal.clone(), None);
    Today {
        dir,
        registry,
        wal,
        key,
        executor,
    }
}

impl Today {
    async fn send(
        &self,
        run_id: &str,
        request: SubmissionRequest,
        key: SendKey,
        delivery: broca_protocol::Delivery,
        session_class: Option<&str>,
    ) -> SendOutcome {
        self.registry
            .route_submission_with_order(
                &self.key,
                RunId(run_id.into()),
                request,
                RunKind::FreshOrResume,
                key,
                false,
                delivery,
                None,
                None,
                None,
                None,
                session_class.map(str::to_owned),
                false,
                None,
            )
            .await
            .expect("today's actor opens")
    }

    async fn idle(&self) {
        for _ in 0..500 {
            if self.registry.route_status(&self.key).await.unwrap() == LineageStatus::Idle {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("today's session never went idle");
    }

    /// What each run's start recorded about its send, in the shape [`store_starts`] reads from
    /// the store session.
    fn starts(&self) -> Vec<serde_json::Value> {
        self.wal
            .read_records(&self.key)
            .unwrap()
            .into_iter()
            .filter_map(|frame| match frame.record {
                broca_wal::WalRecord::RunStarted {
                    run_id,
                    submission_id,
                    send_id,
                    request_identity_digest,
                    title,
                    sent_by,
                    principal,
                    scope,
                    session_class,
                    config,
                    ..
                } => Some(serde_json::json!({
                    "run_id": run_id.0,
                    "submission_id": submission_id.map(|id| id.0),
                    "send_id": send_id,
                    "request_identity_digest": request_identity_digest,
                    "title": title,
                    "sent_by": sent_by,
                    "principal": principal,
                    "scope": scope,
                    "session_class": session_class,
                    "model": config.model,
                })),
                _ => None,
            })
            .collect()
    }

    /// The text of every message today's served history holds, in order, as [`texts`] reads
    /// the store session's conversation. Read through `session.read`'s own path, so a steer or a
    /// queued send lands where today's actor places it.
    async fn texts(&self) -> Vec<String> {
        let mut out = Vec::new();
        let mut from = None;
        loop {
            let page = self
                .registry
                .route_read(&self.key, from, None)
                .await
                .expect("today's session reads");
            for message in &page.messages {
                out.extend(
                    message
                        .message
                        .content
                        .iter()
                        .filter_map(|block| match block {
                            ContentBlock::Text { text } => Some(text.clone()),
                            _ => None,
                        }),
                );
            }
            match page.next_from_ordinal {
                Some(next) if !page.messages.is_empty() => from = Some(next),
                _ => return out,
            }
        }
    }
}

/// One step of a scenario, done on both sides.
enum Step {
    Send {
        run_id: &'static str,
        text: &'static str,
        key: Option<&'static str>,
        route_decision: Option<&'static str>,
        /// Changes the request before it is sent, the same on both sides.
        tweak: Option<fn(&mut SubmissionRequest)>,
        delivery: broca_protocol::Delivery,
        /// The session label the send names.
        class: Option<&'static str>,
    },
    /// Retracts the queued send `submission_id` on both sides; the replies must be equal.
    Retract(&'static str),
    /// Lets the gated first model step through on both sides.
    Release,
    /// Waits until both sessions are idle.
    Idle,
}

fn send(run_id: &'static str, text: &'static str) -> Step {
    Step::Send {
        run_id,
        text,
        key: None,
        route_decision: None,
        tweak: None,
        delivery: broca_protocol::Delivery::Queue,
        class: None,
    }
}

/// A send keyed by the caller's `send_id` `key`, whose run id is `r-<key>` as the serve layer
/// derives it.
fn keyed(key: &'static str, text: &'static str) -> Step {
    Step::Send {
        run_id: Box::leak(format!("r-{key}").into_boxed_str()),
        text,
        key: Some(key),
        route_decision: None,
        tweak: None,
        delivery: broca_protocol::Delivery::Queue,
        class: None,
    }
}

fn labelled(mut step: Step, label: &'static str) -> Step {
    if let Step::Send { class, .. } = &mut step {
        *class = Some(label);
    }
    step
}

fn routed(mut step: Step, decision: &'static str) -> Step {
    if let Step::Send { route_decision, .. } = &mut step {
        *route_decision = Some(decision);
    }
    step
}

fn steered(mut step: Step) -> Step {
    if let Step::Send { delivery, .. } = &mut step {
        *delivery = broca_protocol::Delivery::Steer;
    }
    step
}

fn interrupting(mut step: Step) -> Step {
    if let Step::Send { delivery, .. } = &mut step {
        *delivery = broca_protocol::Delivery::Interrupt;
    }
    step
}

fn tweaked(mut step: Step, change: fn(&mut SubmissionRequest)) -> Step {
    if let Step::Send { tweak, .. } = &mut step {
        *tweak = Some(change);
    }
    step
}

/// Runs `steps` on both sides over the same provider script, the first model step gated when
/// `gated`. Every reply is compared as its step says; when every step is served alike, the two
/// sessions' conversations must also be the same. Returns today's reply to each send, in order.
async fn compare(
    script: Vec<Vec<NormalizedEvent>>,
    gated: bool,
    steps: Vec<Step>,
) -> Vec<SendOutcome> {
    let today_gate = gated.then(|| Arc::new(Notify::new()));
    let store_gate = gated.then(|| Arc::new(Notify::new()));
    let today = today(script.clone(), today_gate.clone());
    // The store actor's setup parts, kept so its session's reads after the steps can name the
    // tools and plans its runs started with.
    let parts = broca_session_loop::parts::SharedParts::new(Box::new(
        broca_session_loop::parts::MemoryParts::default(),
    ));
    let (commands, served, _) =
        super::tests::actor_with(executor(script, store_gate.clone()), parts.clone()).await;
    // Both sides get the request one caller shapes.
    let caller = super::tests::Caller::default();
    let mut answers = Vec::new();
    for (index, step) in steps.into_iter().enumerate() {
        match step {
            Step::Send {
                run_id,
                text,
                key,
                route_decision,
                tweak,
                delivery,
                class,
            } => {
                let send_key = || match key {
                    Some(key) => SendKey::SendId(key.into()),
                    None => SendKey::Minted,
                };
                let mut sent = caller.shape(run_id, &send_key(), request(text));
                sent.route_decision_id = route_decision.map(str::to_owned);
                if let Some(tweak) = tweak {
                    tweak(&mut sent);
                }
                let old = today
                    .send(run_id, sent.clone(), send_key(), delivery, class)
                    .await;
                let new = super::tests::send_exactly(
                    &commands,
                    run_id,
                    sent,
                    send_key(),
                    delivery,
                    class,
                )
                .await;
                assert_eq!(new, old, "step {index}: replies differ");
                answers.push(old);
            }
            Step::Retract(submission_id) => {
                let old = today
                    .registry
                    .route_retract(
                        &today.key,
                        broca_protocol::SubmissionId(submission_id.into()),
                    )
                    .await
                    .expect("today's actor opens");
                let new = super::tests::retract(&commands, submission_id).await;
                assert_eq!(new, old, "step {index}: retraction replies differ");
            }
            Step::Release => {
                for gate in today_gate.iter().chain(store_gate.iter()) {
                    gate.notify_one();
                }
            }
            Step::Idle => {
                today.idle().await;
                super::tests::idle(&commands).await;
            }
        }
    }
    streams::assert_alike(
        &streams::store(&commands).await,
        &streams::routed(&today.registry, &today.key).await,
    );
    drop(commands);
    let session: Session<&'static MemoryBackend> = served.await.unwrap();
    assert_eq!(
        texts(&session),
        today.texts().await,
        "the conversations differ"
    );
    assert_eq!(
        store_starts(&session).await,
        today.starts(),
        "the run starts record the sends differently"
    );
    display_reads_alike(&today, &session, &parts).await;
    assert!(today.registry.stop_and_seal().await);
    answers
}

/// The read modes that add to a reply: display facts, tools, Rich UI stamps, each alone and all
/// together.
const OPTED: [(bool, bool, bool); 4] = [
    (true, false, false),
    (false, true, false),
    (false, false, true),
    (true, true, true),
];

/// The pages read in each mode: the newest, the first message alone, and three from the second.
const PAGES: [(Option<u64>, Option<u64>); 3] =
    [(None, None), (Some(1), Some(1)), (Some(2), Some(3))];

/// A read of `session` from `from`, `limit` messages, opted in as `opted` says.
fn opted_request(
    session: &BindIdentity,
    (from, limit): (Option<u64>, Option<u64>),
    (display, tools, rich_ui): (bool, bool, bool),
) -> crate::serve::SessionReadRequest {
    let mut request = crate::serve::SessionReadRequest::new(session.clone(), from, limit);
    request.include_display_facts = display;
    request.include_tools = tools;
    request.include_rich_ui = rich_ui;
    request
}

/// `session.read` with what each opt-in adds, on a few pages: today's actor's reply and the
/// store actor's session's, compared as [`comparable_read`] leaves them.
async fn display_reads_alike(
    today: &Today,
    session: &Session<&'static MemoryBackend>,
    parts: &broca_session_loop::parts::SharedParts,
) {
    use crate::serve::ServeHandler;
    for opted in OPTED {
        for page in PAGES {
            let request = opted_request(&today.key, page, opted);
            let old = today
                .registry
                .read(request.clone())
                .await
                .expect("today's session reads");
            let new = crate::store_read::serve_view(session, parts, &request)
                .await
                .expect("the store's session reads");
            assert_eq!(
                comparable_read(&new),
                comparable_read(&old),
                "display facts, tools, rich ui {opted:?}, page {page:?}"
            );
        }
    }
}

/// The same reads through each module's registry: a session on today's storage, and one the
/// new storage keeps.
async fn opted_reads_alike(today: &Module, store: &Module, label: &str) {
    use crate::serve::ServeHandler;
    for opted in OPTED {
        for page in PAGES {
            let old = today
                .registry
                .read(opted_request(&today.key, page, opted))
                .await
                .expect("today's session reads");
            let new = store
                .registry
                .read(opted_request(&store.key, page, opted))
                .await
                .expect("the store's session reads");
            assert_eq!(
                comparable_read(&new),
                comparable_read(&old),
                "{label}: display facts, tools, rich ui {opted:?}, page {page:?}"
            );
        }
    }
}

/// A `session.read` reply as JSON, with what each storage keeps for itself taken out: the log
/// identity (`incarnation`), the history version each counts for itself, the subscribe cursor
/// (`head`), each message's id, which each mints in its own form, and each call's key, which
/// names a record of its own log. Wall-clock times, which each run stamps for itself, are kept
/// as whether they're present, and so are call keys.
fn comparable_read(reply: &broca_wire::SessionReadResponse) -> serde_json::Value {
    fn presence(object: &mut serde_json::Map<String, serde_json::Value>, members: &[&str]) {
        for member in members {
            if let Some(value) = object.get_mut(*member) {
                *value = serde_json::json!(!value.is_null());
            }
        }
    }
    let mut value = serde_json::to_value(reply).unwrap();
    for member in ["incarnation", "history_version", "head"] {
        value.as_object_mut().unwrap().remove(member);
    }
    for message in value["messages"].as_array_mut().unwrap() {
        let message = message.as_object_mut().unwrap();
        message.remove("mid");
        presence(message, &["written_at", "created_at", "completed_at"]);
        for list in ["tool_calls", "tool_times", "notices"] {
            for item in message
                .get_mut(list)
                .and_then(serde_json::Value::as_array_mut)
                .into_iter()
                .flatten()
            {
                if let Some(item) = item.as_object_mut() {
                    presence(item, &["call_key", "started_at", "finished_at"]);
                }
            }
        }
        if let Some(delivered) = message
            .get_mut("delivered_as")
            .and_then(serde_json::Value::as_object_mut)
        {
            presence(delivered, &["call_key"]);
        }
    }
    value
}

/// What each run's start in `session` recorded about its send, in the shape [`Today::starts`]
/// reads from today's log.
async fn store_starts(session: &Session<&'static MemoryBackend>) -> Vec<serde_json::Value> {
    use broca_session_store::record::Record;
    session
        .open_log_records()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|(_, record)| match record {
            Record::RunStart(start) => Some(serde_json::json!({
                "run_id": start.run_id,
                "submission_id": start.submission_id,
                "send_id": start.send_id,
                "request_identity_digest": start.request_identity_digest,
                "title": start.title,
                "sent_by": start
                    .run_choices
                    .as_ref()
                    .and_then(|choices| choices.pointer("/record/sent_by"))
                    .cloned(),
                "principal": start.principal,
                "scope": start.scope,
                "session_class": start.session_class,
                "model": start.model,
            })),
            _ => None,
        })
        .collect()
}

fn hello() -> Vec<Vec<NormalizedEvent>> {
    vec![
        super::tests::text_step("a1", "hello"),
        super::tests::text_step("a2", "again"),
    ]
}

#[tokio::test]
async fn two_plain_sends_run_alike() {
    compare(
        hello(),
        false,
        vec![
            send("r1", "first"),
            Step::Idle,
            send("r2", "second"),
            Step::Idle,
        ],
    )
    .await;
}

#[tokio::test]
async fn a_finished_keyed_send_answers_its_retry_alike() {
    compare(
        hello(),
        false,
        vec![keyed("k1", "first"), Step::Idle, keyed("k1", "first")],
    )
    .await;
}

#[tokio::test]
async fn a_retry_with_another_payload_is_refused_alike() {
    compare(
        hello(),
        false,
        vec![keyed("k1", "first"), Step::Idle, keyed("k1", "changed")],
    )
    .await;
}

#[tokio::test]
async fn a_retry_of_the_running_run_is_answered_alike() {
    compare(
        hello(),
        true,
        vec![
            keyed("k1", "first"),
            keyed("k1", "first"),
            Step::Release,
            Step::Idle,
            keyed("k1", "first"),
        ],
    )
    .await;
}

#[tokio::test]
async fn a_retry_of_an_earlier_run_is_answered_alike() {
    compare(
        hello(),
        false,
        vec![
            keyed("k1", "first"),
            Step::Idle,
            send("r2", "second"),
            Step::Idle,
            keyed("k1", "first"),
        ],
    )
    .await;
}

#[tokio::test]
async fn a_running_runs_routing_decision_is_fixed_alike() {
    compare(
        hello(),
        true,
        vec![
            routed(keyed("k1", "first"), "decision-1"),
            routed(keyed("k1", "first"), "decision-2"),
            Step::Release,
            Step::Idle,
        ],
    )
    .await;
}

/// A run paused for sign-in owns the session: a new send is refused naming it, and a retry of
/// its own send resumes it as the same run, which answers on the step it couldn't send, alike.
#[tokio::test]
async fn a_sign_in_pause_resumes_on_its_retry_alike() {
    let mut steps = vec![auth_failure()];
    steps.extend(hello());
    compare(
        steps,
        false,
        vec![
            keyed("k1", "first"),
            Step::Idle,
            send("r2", "second"),
            keyed("k1", "first"),
            Step::Idle,
            keyed("k1", "first"),
        ],
    )
    .await;
}

/// A send while a run runs is queued, answered `pending`, and runs after that run, alike.
#[tokio::test]
async fn a_send_while_a_run_runs_is_queued_alike() {
    compare(
        hello(),
        true,
        vec![
            send("r1", "first"),
            send("r2", "second"),
            Step::Release,
            Step::Release,
            Step::Idle,
        ],
    )
    .await;
}

/// A queued send's retry is `pending`, and one with another payload is refused naming what
/// differs, alike; once it ran, its retry is answered from its run.
#[tokio::test]
async fn a_queued_sends_retry_is_answered_alike() {
    compare(
        hello(),
        true,
        vec![
            keyed("k1", "first"),
            keyed("k2", "second"),
            keyed("k2", "second"),
            keyed("k2", "changed"),
            Step::Release,
            Step::Release,
            Step::Idle,
            keyed("k2", "second"),
        ],
    )
    .await;
}

fn leaves_its_model(request: &mut SubmissionRequest) {
    request.model = broca_protocol::request::ModelHandle::new("stub", "another");
    request.inherit_frozen = [broca_protocol::request::InheritedParam::Model]
        .into_iter()
        .collect();
}

/// A plain send that leaves its model to the session is refused, before and after the session's
/// first run, alike: only a plan's model selection lets a send leave it out, and these sessions
/// have no plan.
#[tokio::test]
async fn a_send_leaving_its_model_to_the_session_runs_alike() {
    compare(
        vec![
            super::tests::text_step("a1", "hello"),
            super::tests::text_step("a2", "again"),
            super::tests::text_step("a3", "once more"),
        ],
        false,
        vec![
            tweaked(send("r0", "too early"), leaves_its_model),
            send("r1", "first"),
            Step::Idle,
            tweaked(send("r2", "second"), leaves_its_model),
            Step::Idle,
        ],
    )
    .await;
}

fn refused_at_admission(request: &mut SubmissionRequest) {
    request.stop_when = vec![broca_protocol::request::StopCondition::StepCount { n: 0 }];
}

/// A queued send whose run admission refuses ends, and the send behind it still runs, alike.
#[tokio::test]
async fn a_queued_send_refused_at_its_drain_ends_alike() {
    compare(
        hello(),
        true,
        vec![
            send("r1", "first"),
            tweaked(send("r2", "refused"), refused_at_admission),
            send("r3", "third"),
            Step::Release,
            Step::Release,
            Step::Idle,
        ],
    )
    .await;
}

fn from_core(request: &mut SubmissionRequest) {
    request.principal = Some(broca_protocol::BindPrincipal::Reserved {
        module_id: "prefrontal-core".into(),
    });
}

/// A send's principal is recorded as its run's sender and principal, for a run started directly
/// and one drained from the queue, alike.
#[tokio::test]
async fn a_sends_principal_is_recorded_alike() {
    compare(
        hello(),
        true,
        vec![
            tweaked(keyed("k1", "first"), from_core),
            tweaked(keyed("k2", "second"), from_core),
            Step::Release,
            Step::Release,
            Step::Idle,
        ],
    )
    .await;
}

/// A steer while a run runs joins it, answered `active` naming that run, and the run answers it
/// at its next step, alike.
#[tokio::test]
async fn a_steer_joins_the_running_run_alike() {
    compare(
        hello(),
        true,
        vec![
            send("r1", "first"),
            steered(send("s1", "steered")),
            Step::Release,
            Step::Release,
            Step::Idle,
        ],
    )
    .await;
}

fn another_model(request: &mut SubmissionRequest) {
    request.model = broca_protocol::request::ModelHandle::new("stub", "another");
}

/// A steer naming a model the running run didn't freeze is refused naming it, alike.
#[tokio::test]
async fn a_steer_with_another_model_is_refused_alike() {
    compare(
        hello(),
        true,
        vec![
            send("r1", "first"),
            tweaked(steered(send("s1", "steered")), another_model),
            Step::Release,
            Step::Idle,
        ],
    )
    .await;
}

/// A steer on an idle session starts a run of its own, and a keyed one records its key, alike.
#[tokio::test]
async fn a_steer_on_an_idle_session_starts_a_run_alike() {
    compare(
        hello(),
        false,
        vec![
            steered(send("s1", "steered")),
            Step::Idle,
            steered(keyed("k1", "again")),
            Step::Idle,
        ],
    )
    .await;
}

/// A steer behind a run paused for sign-in is refused naming that run, alike.
#[tokio::test]
async fn a_steer_behind_a_sign_in_pause_is_refused_alike() {
    compare(
        vec![auth_failure()],
        false,
        vec![
            keyed("k1", "first"),
            Step::Idle,
            steered(send("s1", "steered")),
        ],
    )
    .await;
}

/// A queued send is retracted before it runs, a second retraction finds nothing pending, and
/// retracting a send whose run started is too late, alike.
#[tokio::test]
async fn a_queued_send_is_retracted_alike() {
    compare(
        hello(),
        true,
        vec![
            send("r1", "first"),
            send("r2", "withdrawn"),
            send("r3", "third"),
            Step::Retract("sub-r2"),
            Step::Retract("sub-r2"),
            Step::Release,
            Step::Release,
            Step::Idle,
            Step::Retract("sub-r3"),
            Step::Retract("sub-r1"),
        ],
    )
    .await;
}

pub(super) fn core() -> broca_protocol::BindPrincipal {
    broca_protocol::BindPrincipal::Reserved {
        module_id: "prefrontal-core".into(),
    }
}

pub(super) fn head_scope(owner: broca_protocol::BindPrincipal) -> broca_protocol::SendScope {
    broca_protocol::SendScope::Stamped(broca_protocol::BindScope {
        scope: broca_protocol::SessionScope {
            owner,
            agent_id_frozen: false,
            scope_ref: "head-1".into(),
            scope_epoch: 1,
            flow_id: None,
            sender: None,
            agent_id: None,
            run_id: None,
        },
        owner_authorized: true,
        kind: broca_protocol::ScopeKind::Head,
        parent: None,
        parent_state: None,
    })
}

/// Core, sending under its own head scope. The send names no tools: on a scoped session tools
/// come only from a plan.
fn scoped_by_core(request: &mut SubmissionRequest) {
    request.principal = Some(core());
    request.scope = head_scope(core());
    request.tools = None;
}

/// Core under its own head scope, declaring the default request's tools.
fn scoped_with_tools(request: &mut SubmissionRequest) {
    request.principal = Some(core());
    request.scope = head_scope(core());
}

/// Another module, sending under core's head scope.
fn scoped_by_another(request: &mut SubmissionRequest) {
    scoped_by_core(request);
    request.principal = Some(broca_protocol::BindPrincipal::Reserved {
        module_id: "magic-context".into(),
    });
}

/// Core, sending with no scope stamp on a daemon that stamps scopes.
fn unstamped_by_core(request: &mut SubmissionRequest) {
    request.principal = Some(core());
    request.scope = broca_protocol::SendScope::Unstamped;
    request.tools = None;
}

/// A first scoped send freezes its scope, and later sends under it run with it, alike. A send
/// from another module under that scope, and an unstamped one, are refused by the session's
/// history, alike.
#[tokio::test]
async fn a_scoped_session_freezes_its_scope_alike() {
    compare(
        hello(),
        false,
        vec![
            tweaked(send("r1", "first"), scoped_by_core),
            Step::Idle,
            tweaked(send("r2", "second"), scoped_by_core),
            Step::Idle,
            tweaked(send("r3", "intruder"), scoped_by_another),
            tweaked(send("r4", "unstamped"), unstamped_by_core),
        ],
    )
    .await;
}

/// A scoped send can't freeze a scope while an unscoped run holds the session, and is refused
/// as busy naming that run, alike.
#[tokio::test]
async fn freezing_a_scope_while_a_run_runs_is_busy_alike() {
    compare(
        hello(),
        true,
        vec![
            tweaked(send("r1", "first"), from_core),
            tweaked(send("r2", "scoped"), scoped_by_core),
            Step::Release,
            Step::Idle,
        ],
    )
    .await;
}

/// A scoped send that declares tools is refused, since tools on a scoped session come only from
/// a plan, alike.
#[tokio::test]
async fn a_scoped_send_declaring_tools_is_refused_alike() {
    compare(
        hello(),
        false,
        vec![tweaked(send("r1", "first"), scoped_with_tools)],
    )
    .await;
}

/// A send queued behind a scoped run drains into a run that carries the session's scope, alike.
#[tokio::test]
async fn a_queued_send_carries_the_frozen_scope_alike() {
    compare(
        hello(),
        true,
        vec![
            tweaked(send("r1", "first"), scoped_by_core),
            tweaked(send("r2", "queued"), scoped_by_core),
            Step::Release,
            Step::Release,
            Step::Idle,
        ],
    )
    .await;
}

/// An interrupting send on an idle session just starts its message, on both actors alike.
#[tokio::test]
async fn an_interrupt_on_an_idle_session_starts_alike() {
    compare(
        hello(),
        false,
        vec![
            send("r1", "first"),
            Step::Idle,
            interrupting(send("r2", "interrupting")),
            Step::Idle,
        ],
    )
    .await;
}

#[tokio::test]
async fn a_sessions_label_freezes_at_its_first_send_alike() {
    compare(
        hello(),
        false,
        vec![
            labelled(send("r1", "first"), "sidekick"),
            Step::Idle,
            // A later send repeating the label runs; one naming another is refused.
            labelled(send("r2", "second"), "sidekick"),
            Step::Idle,
            labelled(send("r3", "third"), "mason"),
            Step::Idle,
        ],
    )
    .await;
}

#[tokio::test]
async fn a_later_send_without_its_label_keeps_the_sessions_alike() {
    compare(
        hello(),
        false,
        vec![
            labelled(send("r1", "first"), "sidekick"),
            Step::Idle,
            send("r2", "second"),
            Step::Idle,
        ],
    )
    .await;
}

#[tokio::test]
async fn a_label_on_an_unlabelled_session_is_ignored_alike() {
    compare(
        hello(),
        false,
        vec![
            send("r1", "first"),
            Step::Idle,
            labelled(send("r2", "second"), "sidekick"),
            Step::Idle,
        ],
    )
    .await;
}

/// A `final_text` run must offer no tools, and the test request carries some.
fn final_text_with_tools(request: &mut SubmissionRequest) {
    request.structured_output = Some(
        serde_json::from_value(serde_json::json!({
            "mode": "final_text",
            "schema": {"type": "object"},
        }))
        .unwrap(),
    );
}

/// A `final_text` request offering no tools, which a fresh run would admit: a steer naming it
/// must still match what the running run froze.
fn final_text_untooled(request: &mut SubmissionRequest) {
    final_text_with_tools(request);
    request.tools = None;
    request.policy_allowed = Default::default();
}

#[tokio::test]
async fn a_final_text_send_offering_tools_is_refused_alike() {
    compare(
        hello(),
        false,
        vec![
            tweaked(send("r1", "first"), final_text_with_tools),
            send("r2", "second"),
            Step::Idle,
        ],
    )
    .await;
}

#[tokio::test]
async fn a_steer_naming_structured_output_the_run_did_not_freeze_is_refused_alike() {
    compare(
        hello(),
        true,
        vec![
            send("r1", "first"),
            steered(tweaked(send("r2", "second"), final_text_untooled)),
            Step::Release,
            Step::Idle,
        ],
    )
    .await;
}

/// Four turns, each answering with text.
fn four_turns() -> Vec<Vec<NormalizedEvent>> {
    vec![
        super::tests::text_step("a1", "hello"),
        super::tests::text_step("a2", "again"),
        super::tests::text_step("a3", "more"),
        super::tests::text_step("a4", "done"),
    ]
}

/// Core under its own head scope, marking its message a notice.
fn noticed(request: &mut SubmissionRequest) {
    scoped_by_core(request);
    request.message_marks = [("notice".to_owned(), serde_json::json!(true))]
        .into_iter()
        .collect();
}

/// Core under its own head scope, saying its message speaks for the head's agent, which only
/// the scope's owner may say.
fn speaking_for(request: &mut SubmissionRequest) {
    scoped_by_core(request);
    request.message_marks = [(
        "speaks_for".to_owned(),
        serde_json::json!({"agent_id": "agent-1"}),
    )]
    .into_iter()
    .collect();
}

/// Another module under core's head scope, saying it speaks for the head's agent.
fn speaking_for_by_another(request: &mut SubmissionRequest) {
    speaking_for(request);
    request.principal = Some(broca_protocol::BindPrincipal::Reserved {
        module_id: "magic-context".into(),
    });
}

/// Core with no scope stamp, saying it speaks for an agent, which a send owning no scope can't.
fn speaking_for_unscoped(request: &mut SubmissionRequest) {
    speaking_for(request);
    request.scope = broca_protocol::SendScope::Unstamped;
}

/// Core under its own head scope, carrying both marks: an agent's automatic message, which a
/// runner declaring `notice_with_speaks_for` admits.
fn noticed_and_speaking_for(request: &mut SubmissionRequest) {
    speaking_for(request);
    request
        .message_marks
        .insert("notice".to_owned(), serde_json::json!(true));
}

/// [`noticed_and_speaking_for`], with the speaker's display name.
fn noticed_and_speaking_for_by_name(request: &mut SubmissionRequest) {
    noticed_and_speaking_for(request);
    request.message_marks.insert(
        "speaks_for".to_owned(),
        serde_json::json!({"agent_id": "agent-1", "name": "Ada"}),
    );
}

/// A send's display marks, a notice and the agent the scope's owner speaks for, are kept for a
/// run the send starts and for a run drained from the queue, and `session.read` serves them on
/// each run's message, alike.
#[tokio::test]
async fn a_sends_marks_are_kept_for_its_run_alike() {
    compare(
        four_turns(),
        true,
        vec![
            tweaked(send("r1", "first"), noticed),
            tweaked(send("r2", "queued"), speaking_for),
            Step::Release,
            Step::Release,
            Step::Idle,
            tweaked(send("r3", "third"), speaking_for),
            tweaked(send("r4", "queued too"), noticed),
            Step::Release,
            Step::Release,
            Step::Idle,
        ],
    )
    .await;
}

/// A steer's display marks are kept with the steer, and `session.read` serves them on the
/// message the running run places, alike.
#[tokio::test]
async fn a_steers_marks_are_kept_alike() {
    compare(
        four_turns(),
        true,
        vec![
            tweaked(send("r1", "first"), scoped_by_core),
            tweaked(steered(send("s1", "steered")), noticed),
            Step::Release,
            Step::Release,
            Step::Idle,
            tweaked(send("r2", "second"), scoped_by_core),
            tweaked(steered(send("s2", "steered again")), speaking_for),
            Step::Release,
            Step::Release,
            Step::Idle,
        ],
    )
    .await;
}

/// Marks a send may not carry are refused naming the member, before the send's owner is
/// checked, alike: another module speaking for the scope owner's agent, and a send with no
/// scope speaking for one. A send whose marks are admitted then runs.
#[tokio::test]
async fn marks_a_send_may_not_carry_are_refused_alike() {
    compare(
        hello(),
        false,
        vec![
            tweaked(send("r1", "another's"), speaking_for_by_another),
            tweaked(send("r2", "unscoped"), speaking_for_unscoped),
            tweaked(send("r4", "first"), noticed),
            Step::Idle,
        ],
    )
    .await;
}

/// An agent's automatic message, a notice speaking for the head's agent, is kept with both
/// marks and the speaker's display name for a send that starts a run, a queued send and a
/// steer, and `session.read` serves them, alike.
#[tokio::test]
async fn an_agents_named_notice_is_kept_alike() {
    compare(
        four_turns(),
        true,
        vec![
            tweaked(send("r1", "first"), noticed_and_speaking_for_by_name),
            tweaked(
                steered(send("s1", "steered")),
                noticed_and_speaking_for_by_name,
            ),
            Step::Release,
            Step::Release,
            Step::Idle,
            tweaked(send("r2", "second"), noticed_and_speaking_for),
            tweaked(send("r3", "queued"), noticed_and_speaking_for_by_name),
            Step::Release,
            Step::Release,
            Step::Idle,
        ],
    )
    .await;
}

/// Both loops behind one executor, as the module's executor will be: a session on today's
/// storage runs through today's loop, a session in the new storage through the new one.
pub(super) struct BothLoops {
    pub(super) today: TodayLoop,
    pub(super) store: super::tests::LoopExecutor,
}

#[async_trait]
impl RunExecutor for BothLoops {
    async fn execute(
        &self,
        job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        self.today.execute(job, out).await
    }

    async fn execute_on_store(
        &self,
        job: crate::executor::StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> crate::executor::StoreRunEnd {
        self.store.execute_on_store(job, out).await
    }
}

/// A module over a scratch folder whose opener can serve the new storage, with the registry's
/// new-session switch `on` or off.
struct Module {
    dir: std::path::PathBuf,
    registry: LineageRegistry,
    wal: Arc<FileWalStore>,
    key: BindIdentity,
    /// The caller whose sends reach the module's session.
    caller: super::tests::Caller,
}

impl Drop for Module {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

pub(super) const IDLE: Duration = Duration::from_millis(100);

async fn module(on: bool, steps: Vec<Vec<NormalizedEvent>>) -> Module {
    module_step_events(on, steps, None, None).await
}

async fn module_step_events(
    on: bool,
    steps: Vec<Vec<NormalizedEvent>>,
    cipher: Option<broca_session_store::keys::Cipher>,
    step_events: Option<Arc<crate::step_events::StepEvents>>,
) -> Module {
    module_options(on, steps, cipher, step_events, IDLE, None).await
}

async fn module_options(
    on: bool,
    steps: Vec<Vec<NormalizedEvent>>,
    cipher: Option<broca_session_store::keys::Cipher>,
    step_events: Option<Arc<crate::step_events::StepEvents>>,
    idle_ttl: Duration,
    gate: Option<Arc<Notify>>,
) -> Module {
    module_options_with_render(on, steps, cipher, step_events, idle_ttl, gate, None).await
}

async fn module_options_with_render(
    on: bool,
    steps: Vec<Vec<NormalizedEvent>>,
    cipher: Option<broca_session_store::keys::Cipher>,
    step_events: Option<Arc<crate::step_events::StepEvents>>,
    idle_ttl: Duration,
    gate: Option<Arc<Notify>>,
    render_config: Option<serde_json::Value>,
) -> Module {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "store-open-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    // Today's WAL store finds the new storage beside its own folder: its state root is `dir`.
    let wal = Arc::new(match cipher {
        Some(_) => FileWalStore::with_config(dir.join("wal"), broca_wal::WalConfig::OVERHAUL),
        None => FileWalStore::new(dir.join("wal")),
    });
    let mut store_loop = executor(steps.clone(), gate.clone());
    store_loop.provider.render_config = render_config.clone();
    let executor: Arc<dyn RunExecutor> = Arc::new(BothLoops {
        today: TodayLoop {
            provider: Gated {
                render_config,
                notes_sends: false,
                inner: broca_core::provider::StubProvider::new(steps.clone()),
                gate: gate.clone(),
            },
            wal: Arc::clone(&wal),
            planes: Default::default(),
        },
        store: store_loop,
    });
    let runtime = crate::store_open::StoreRuntime::open(
        &dir,
        broca_session_store::keys::DeploymentKey::new(1, &[7; 32])
            .creating(cipher.unwrap_or(broca_session_store::keys::Cipher::XChaCha20Poly1305)),
        Arc::clone(&executor),
        idle_ttl,
    )
    .await
    .unwrap();
    let mut opener = crate::opener::DefaultOpener::new(
        Arc::new(FileLeaseStore::new(dir.join("leases"))),
        wal.clone(),
        executor,
    )
    .with_idle_ttl(idle_ttl)
    .with_store_runtime(Arc::new(runtime));
    if let Some(events) = step_events {
        opener = opener.with_step_events(events);
    }
    let registry = LineageRegistry::new_with_reader(Arc::new(opener), wal.clone(), None);
    let registry = if on {
        registry.with_store_new_sessions()
    } else {
        registry
    };
    let key = BindIdentity::new(dir.join("project"), "store-actor", "s1");
    Module {
        dir,
        registry,
        wal,
        key,
        caller: Default::default(),
    }
}

impl Module {
    async fn send(&self, run_id: &str, text: &str, conversation_key: Option<&str>) -> SendOutcome {
        self.send_request(run_id, request(text), conversation_key)
            .await
    }

    async fn send_request(
        &self,
        run_id: &str,
        request: SubmissionRequest,
        conversation_key: Option<&str>,
    ) -> SendOutcome {
        self.registry
            .route_submission_with_order(
                &self.key,
                RunId(run_id.into()),
                self.caller.shape(run_id, &SendKey::Minted, request),
                RunKind::FreshOrResume,
                SendKey::Minted,
                false,
                broca_protocol::Delivery::Queue,
                None,
                conversation_key.map(str::to_owned),
                None,
                None,
                None,
                false,
                None,
            )
            .await
            .expect("the session opens")
    }

    /// Waits until the session is idle, then until its actor has closed and left the registry.
    async fn closed(&self) {
        for _ in 0..1000 {
            if self.registry.slot_state(&self.key).is_none() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("the session's actor never closed");
    }

    fn head(&self) -> std::path::PathBuf {
        // A session created without a class is ordinary.
        broca_wal::session_storage_head(
            &self.dir,
            broca_wal::StorageClass::Ordinary,
            &broca_core::session_addr(&self.key),
        )
    }

    fn floor(&self) -> String {
        std::fs::read_to_string(self.dir.join(broca_wal::FEATURE_FLOOR_FILENAME))
            .unwrap_or_default()
    }

    /// The id each message of the store session stored when it was placed, in order.
    async fn stored_mids(&self) -> Vec<String> {
        let session = self.load().await;
        let placed = usize::try_from(session.conversation.placed).unwrap();
        session
            .entries(1, placed)
            .await
            .unwrap()
            .into_iter()
            .map(|entry| entry.state.facts.expect("every message has facts").mid)
            .collect()
    }

    async fn load(&self) -> Session<broca_session_store::backend::LocalBackend> {
        let backend = broca_session_store::backend::LocalBackend::new(
            self.dir.join(broca_wal::SESSION_STORAGE_DIR),
        );
        let triple = broca_session_store::head::SessionTriple {
            project_root: self.key.project_root.display().to_string(),
            harness: self.key.harness.clone(),
            session: self.key.session.clone(),
        };
        Session::load(
            backend,
            broca_session_store::backend::SessionAddr(broca_core::session_addr(&self.key)),
            &triple,
            &broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
        )
        .await
        .unwrap()
    }

    /// The text of every message the store session holds, read once its actor is gone.
    async fn store_texts(&self) -> Vec<String> {
        let backend = broca_session_store::backend::LocalBackend::new(
            self.dir.join(broca_wal::SESSION_STORAGE_DIR),
        );
        let triple = broca_session_store::head::SessionTriple {
            project_root: self.key.project_root.display().to_string(),
            harness: self.key.harness.clone(),
            session: self.key.session.clone(),
        };
        let session = Session::load(
            backend,
            broca_session_store::backend::SessionAddr(broca_core::session_addr(&self.key)),
            &triple,
            &broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
        )
        .await
        .unwrap();
        session
            .conversation
            .messages
            .iter()
            .flat_map(|message| &message.content)
            .filter_map(|block| match block {
                ContentBlock::Text { text } => Some(text.clone()),
                _ => None,
            })
            .collect()
    }
}

/// With the switch on, a session that exists nowhere starts in the new storage: its head is
/// written, no WAL is, and the feature floor names the new storage before anything else. The
/// actor closes when idle, and the next send reopens the session from its head and runs after
/// the first run.
#[tokio::test]
async fn the_switch_starts_a_new_session_in_the_new_storage_and_reopens_it() {
    let module = module(
        true,
        vec![
            super::tests::text_step("a1", "hello"),
            super::tests::text_step("a2", "again"),
        ],
    )
    .await;
    assert!(matches!(
        module.send("r1", "first", None).await,
        SendOutcome::Active { .. }
    ));
    module.closed().await;
    assert!(
        module.head().exists(),
        "the session's head is in the new storage"
    );
    assert_eq!(module.wal.wal_present(&module.key), Some(false));
    assert!(
        module.floor().contains(broca_wal::SESSION_STORAGE_FEATURE),
        "{}",
        module.floor()
    );
    assert!(matches!(
        module.send("r2", "second", None).await,
        SendOutcome::Active { .. }
    ));
    module.closed().await;
    assert_eq!(module.wal.wal_present(&module.key), Some(false));
    assert_eq!(
        module.store_texts().await,
        ["first", "hello", "second", "again"]
    );
}

/// With the switch off, a session starts on today's storage and the new storage holds nothing.
#[tokio::test]
async fn sessions_stay_on_todays_storage_when_the_switch_is_off() {
    let module = module(false, vec![super::tests::text_step("a1", "hello")]).await;
    assert!(matches!(
        module.send("r1", "first", None).await,
        SendOutcome::Active { .. }
    ));
    module.closed().await;
    assert!(!module.head().exists());
    assert_eq!(module.wal.wal_present(&module.key), Some(true));
    assert!(!module.floor().contains(broca_wal::SESSION_STORAGE_FEATURE));
}

/// One `session.read` reply as JSON, with what legitimately differs between the two storages
/// taken out: the log identity (`incarnation`); the history version, which each storage counts
/// for itself (a written WAL serves 1, a new store session 0, and either only rises); the
/// subscribe cursor (`head`), a WAL position the new storage has none of; each message's id,
/// which each storage mints in its own form; and wall-clock times, which each run stamps for
/// itself. A time is kept as whether it's present.
async fn read_json(
    module: &Module,
    from_ordinal: Option<u64>,
    limit: Option<u64>,
    after_mid: Option<String>,
) -> (serde_json::Value, Vec<String>) {
    use crate::serve::{ServeHandler, SessionReadRequest};
    let mut request = SessionReadRequest::new(module.key.clone(), from_ordinal, limit);
    if after_mid.is_some() {
        request.history_version = Some(
            module
                .registry
                .read(SessionReadRequest::new(module.key.clone(), None, Some(1)))
                .await
                .expect("the session reads")
                .history_version,
        );
    }
    request.after_mid = after_mid;
    let reply = module
        .registry
        .read(request)
        .await
        .expect("the session reads");
    let mids = reply
        .messages
        .iter()
        .map(|message| message.mid.clone())
        .collect();
    let mut value = serde_json::to_value(&reply).unwrap();
    for member in ["incarnation", "history_version", "head"] {
        value.as_object_mut().unwrap().remove(member);
    }
    for message in value["messages"].as_array_mut().unwrap() {
        let message = message.as_object_mut().unwrap();
        message.remove("mid");
        for time in ["written_at", "created_at", "completed_at"] {
            if let Some(at) = message.get_mut(time) {
                *at = serde_json::json!(!at.is_null());
            }
        }
    }
    (value, mids)
}

/// A session in the new storage reads as the same history on today's storage does: the newest
/// page, a page from an ordinal, and a page after a message's id. Each storage's own message ids
/// continue a read in that storage.
#[tokio::test]
async fn a_store_session_reads_as_the_same_history_on_todays_storage() {
    let steps = || {
        vec![
            super::tests::text_step("a1", "hello"),
            super::tests::text_step("a2", "again"),
        ]
    };
    let today = module(false, steps()).await;
    let store = module(true, steps()).await;
    for module in [&today, &store] {
        assert!(matches!(
            module.send("r1", "first", None).await,
            SendOutcome::Active { .. }
        ));
        module.closed().await;
        assert!(matches!(
            module.send("r2", "second", None).await,
            SendOutcome::Active { .. }
        ));
        module.closed().await;
    }
    assert!(store.head().exists() && !today.head().exists());
    for (from, limit) in [(None, None), (Some(1), Some(2)), (Some(3), Some(1))] {
        let (old, _) = read_json(&today, from, limit, None).await;
        let (new, _) = read_json(&store, from, limit, None).await;
        assert_eq!(new, old, "page from {from:?} limit {limit:?}");
    }
    let (_, today_mids) = read_json(&today, Some(1), Some(1), None).await;
    let (_, store_mids) = read_json(&store, Some(1), Some(1), None).await;
    let (old, _) = read_json(&today, None, Some(2), Some(today_mids[0].clone())).await;
    let (new, _) = read_json(&store, None, Some(2), Some(store_mids[0].clone())).await;
    assert_eq!(new, old, "page after the first message");
    // The ids served are the ones the session stored when it placed each message.
    let (_, all) = read_json(&store, Some(1), None, None).await;
    assert_eq!(all, store.stored_mids().await);
    opted_reads_alike(&today, &store, "two finished runs").await;
    use cortexkit_role_llm_runner::read::MediaMode;
    for media in [
        MediaMode::Inline,
        MediaMode::Blocks,
        MediaMode::MediaRefs,
        MediaMode::Descriptors,
    ] {
        for opted in OPTED {
            for page in PAGES {
                let mut roles = Vec::new();
                for module in [&today, &store] {
                    let mut request = opted_request(&module.key, page, opted);
                    request.media = media;
                    let reply = module.registry.read(request).await.unwrap();
                    let mut envelope_roles = Vec::new();
                    for entry in &reply.messages {
                        let role = serde_json::to_value(entry.message.role).unwrap();
                        let wire = serde_json::to_string(entry).unwrap();
                        assert!(
                            wire.ends_with(&format!(",\"role\":{role}}}")),
                            "{media:?}, {opted:?}, {page:?}: role ends the read envelope: {wire}"
                        );
                        envelope_roles.push(role);
                    }
                    roles.push(envelope_roles);
                }
                assert_eq!(roles[1], roles[0], "{media:?}, {opted:?}, {page:?}");
            }
        }
    }
    let mut history_roles = Vec::new();
    for module in [&today, &store] {
        let request = serde_json::from_value(serde_json::json!({"form": "raw"})).unwrap();
        let reply = crate::history::read(&module.registry, &module.key, &request)
            .await
            .unwrap();
        let roles: Vec<_> = reply["messages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| {
                assert_eq!(entry["role"], entry["message"]["role"]);
                assert!(entry["role"].is_string(), "raw envelope role: {entry}");
                (
                    entry["role"].clone(),
                    entry
                        .as_object()
                        .unwrap()
                        .keys()
                        .cloned()
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        assert_eq!(roles.len(), 4);
        history_roles.push(roles);
    }
    assert_eq!(history_roles[1], history_roles[0]);
    // A cursor naming another history version is refused, never answered.
    use crate::serve::{ServeHandler, SessionReadRequest};
    let mut stale = SessionReadRequest::new(store.key.clone(), None, Some(1));
    stale.after_mid = Some(store_mids[0].clone());
    stale.history_version = Some(7);
    assert!(matches!(
        store.registry.read(stale).await,
        Err(crate::serve::ServeError::HistoryChanged)
    ));
}

mod admission;
mod controls;
mod head;
mod inline_refs;
mod keep_warm;
mod media;
mod media_ready;
mod media_render;
mod one_shot;
mod ordered;
mod pauses;
mod photo_sends;
mod refresh;
#[cfg(feature = "test-clock")]
mod retention;
mod scopes;
mod step_events;
mod streams;
mod warm;

mod held;

#[tokio::test]
async fn send_ids_equal_the_admitted_keys_and_unkeyed_submission_on_both_storages() {
    use crate::serve::ServeHandler;

    for delivery in [
        broca_protocol::Delivery::Queue,
        broca_protocol::Delivery::Steer,
    ] {
        let old_gate = Arc::new(Notify::new());
        let new_gate = Arc::new(Notify::new());
        let today = today(hello(), Some(old_gate.clone()));
        let old_calls = today.executor.provider.inner.clone();
        let parts = broca_session_loop::parts::SharedParts::new(Box::new(
            broca_session_loop::parts::MemoryParts::default(),
        ));
        let executor = executor(hello(), Some(new_gate.clone()));
        let new_calls = executor.provider.inner.clone();
        let (commands, served, _) = super::tests::actor_with(executor, parts.clone()).await;
        let caller = super::tests::Caller::default();
        let second_key = if delivery == broca_protocol::Delivery::Queue {
            SendKey::Minted
        } else {
            SendKey::SendId("steer-key".into())
        };
        let mut second_id = "steer-key".to_owned();
        for (run, text, key, mode) in [
            (
                "native-first",
                "native keyed send",
                SendKey::SendId("start-key".into()),
                broca_protocol::Delivery::Queue,
            ),
            ("native-second", "native second send", second_key, delivery),
        ] {
            let mut request = caller.shape(run, &key, request(text));
            from_core(&mut request);
            let old = today
                .send(run, request.clone(), key.clone(), mode, None)
                .await;
            let new = super::tests::send_exactly(&commands, run, request, key, mode, None).await;
            assert_eq!(new, old);
            if run == "native-second" && delivery == broca_protocol::Delivery::Queue {
                let SendOutcome::Pending { submission_id } = old else {
                    panic!("queue returned {old:?}")
                };
                second_id = submission_id.0;
            }
        }
        old_gate.notify_one();
        new_gate.notify_one();
        // Each scripted model call consumes one permit. Wait for the first permit to be
        // consumed before releasing the second, so Notify cannot coalesce the releases.
        broca_test_support::wait_until(
            "both first calls to consume their gate permits",
            || async {
                if !old_calls.calls().is_empty() && !new_calls.calls().is_empty() {
                    Ok(())
                } else {
                    Err("a first model call is still gated".to_owned())
                }
            },
        )
        .await;
        old_gate.notify_one();
        new_gate.notify_one();
        today.idle().await;
        super::tests::idle(&commands).await;
        drop(commands);
        let session = served.await.unwrap();
        let request = opted_request(&today.key, (Some(1), Some(100)), (true, false, false));
        let old = today.registry.read(request.clone()).await.unwrap();
        let new = crate::store_read::serve_view(&session, &parts, &request)
            .await
            .unwrap();
        for read in [&old, &new] {
            for (text, id) in [
                ("native keyed send", "start-key"),
                ("native second send", second_id.as_str()),
            ] {
                let entry = read.messages.iter().find(|entry| entry.message.content.iter().any(|part|
                    matches!(part, broca_protocol::ContentBlock::Text {text: actual} if actual == text)
                )).unwrap();
                assert_eq!(entry.sender.as_deref(), Some("prefrontal-core"));
                assert_eq!(entry.send_id.as_deref(), Some(id), "{text}");
            }
        }
        let mut entries = session.entries(1, 100).await.unwrap();
        for entry in &mut entries {
            if let Some(facts) = &mut entry.state.facts {
                facts.send_id = None;
            }
        }
        let recovered = session.older_entry_send_ids(&entries).await.unwrap();
        for entry in &new.messages {
            if let Some(id) = &entry.send_id {
                assert_eq!(recovered.get(&entry.ordinal), Some(id));
            }
        }
        assert!(today.registry.stop_and_seal().await);
    }
}
