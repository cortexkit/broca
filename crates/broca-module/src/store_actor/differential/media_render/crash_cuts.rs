//! Crashes of a session holding images, kept in the new storage: the process dies after any
//! record of the scenario reached the disk, and a new process opens what it left.
//!
//! The scenario runs on the module's real store path, on a session whose plan names a compaction
//! provider: an image a tool returns, then a photo the user sends in a run whose first step
//! applies the provider's view cutting the history, which seals the session and saves its image
//! index; then a run that pauses for a restart and resumes after it; then a run on a session
//! whose saved index was lost, which builds the index once from the session's records. The state
//! root is copied at three points: before the seal, after it, and during the run that built the
//! index. For each copy, every record boundary of its open log is a crash: the log is cut there,
//! as a crash leaves a log whose later appends never reached the disk, and a new module opens
//! the cut copy through the store's own open and the recovery a starting process runs.
//!
//! What each cut must do, compared as the bytes the provider was asked to send:
//! - A run paused for a restart resumes its next step, which renders the request the original
//!   sent for that step. A queued send starts its run with the request the original sent.
//!   Recovery sends nothing else: a run cut mid-step is sealed as a crash's orphan.
//! - The next run's first request carries every image the session holds.
//! - Where the cut log's checkpoint names a saved index, the same cut opened again without that
//!   index (so the open builds it from the records) sends the same bytes; where the run built the
//!   index because it was lost, the cut opened with the index put back sends the same bytes. A
//!   saved index that disagreed with the records it stands for shows here.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use broca_core::{FileLeaseStore, OutputEvent, RunError};
use broca_protocol::provider::NormalizedEvent;
use broca_protocol::{BindIdentity, PauseReason, SubmissionRequest};
use broca_session_store::backend::{LocalBackend, SessionAddr, SessionBackend};
use broca_session_store::frame::{FrameRead, FrameStream};
use broca_session_store::keys::{Cipher, DeploymentKey};
use broca_session_store::record::Record;
use broca_session_store::session::{SavedIndex, Session};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::super::controls::Compactor;
use super::super::media::{base64, bytes_of, image_profile, image_render_config, photo, Side, PNG};
use super::{install_store, photo_step, text, text_step, PhotoPlane, Rendering, Sent};
use crate::actor::SendOutcome;
use crate::executor::{RunExecutor, StoreRunEnd, StoreRunError, StoreRunJob};
use crate::registry::LineageRegistry;

cipher_tests!(every_record_boundary_cut_of_a_session_with_images_resumes_as_sent);

/// Long enough that no session closes, and so seals, by itself while the scenario runs: the
/// only seals are the ones the scenario asks for.
const IDLE: std::time::Duration = std::time::Duration::from_secs(600);

/// The runs the scenario sends, in order.
const RUNS: [&str; 4] = ["r1", "r2", "r3", "r4"];

/// The new storage's runs, rendering each request into `sent`, as the module's executor runs
/// them. Each run's stop request is `stop`, so cancelling it pauses a run that resumes after a
/// restart at its next boundary, as a shutdown does. The run loop records the wall clock, as in
/// production, because a restart pause is continued only while it is recent.
struct CutExecutor {
    script: Arc<super::super::super::tests::Gated>,
    sent: Sent,
    stop: CancellationToken,
    compactor: Arc<Compactor>,
}

#[async_trait]
impl RunExecutor for CutExecutor {
    async fn media_profile(
        &self,
        _request: &SubmissionRequest,
    ) -> Result<Option<Box<broca_provider::media_profile::MediaProfile>>, RunError> {
        Ok(Some(Box::new(image_profile())))
    }

    fn compaction_plane(&self) -> Option<Arc<dyn broca_core::CompactionPlane>> {
        Some(Arc::clone(&self.compactor) as Arc<dyn broca_core::CompactionPlane>)
    }

    async fn execute_on_store(
        &self,
        job: StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> StoreRunEnd {
        use broca_session_loop::writer::Writer as _;
        let StoreRunJob {
            mut req,
            mut control,
            mut writer,
            mut parts,
            mode,
            media,
            ..
        } = job;
        control.admitted_context_limit = control.admitted_context_limit.or(req.context_limit);
        let provider = Rendering::new(&self.script, &self.sent);
        let result = async {
            let holds_media = req
                .input
                .iter()
                .chain(
                    writer
                        .conversation()
                        .held_messages()
                        .unwrap()
                        .iter()
                        .map(|message| &**message),
                )
                .any(|message| !broca_session_store::session::message_media(message).is_empty());
            // A seal writes its records and calls no model, so, as the module's executor does,
            // it installs no media renderer and waits for no image index.
            let ready = match mode {
                crate::executor::StoreRunMode::Seal(_) => None,
                _ => install_store(media, holds_media, !req.tools.is_empty(), &provider)
                    .map_err(StoreRunError::Admission)?,
            };
            let tools = PhotoPlane::new();
            let now = || {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
            };
            let seams = broca_session_loop::Seams {
                person_steers: None,
                provider: &provider,
                tools: &tools,
                bind: req.session.clone(),
                out: Some(&out),
                cancel: control.cancel.clone().unwrap_or_default(),
                stop: self.stop.clone(),
                held: None,
                approval: &broca_core::AllowAll,
                hook_plane: control.hook_plane.clone(),
                now_ms: &now,
                session_address: None,
                compaction_plane: control.compaction_plane.as_deref(),
                compaction_session: control.compaction_session.clone(),
                compaction_changes: true,
                keep_warm: None,
                account_reports: None,
                max_same_account_wait: Default::default(),
                auth_selection: None,
                step_progress: control.step_progress.clone(),
                model_call_ready: ready,
                pause_for_auth_at_start: None,
                account_continue: None,
            };
            match mode {
                crate::executor::StoreRunMode::Fresh => {
                    let admission = broca_session_loop::admit::admission(
                        &mut req,
                        &control,
                        &provider,
                        writer.operations(),
                        Some(1),
                    )
                    .map_err(StoreRunError::Admission)?;
                    broca_session_loop::run(&mut writer, parts.as_mut(), admission, &seams).await
                }
                crate::executor::StoreRunMode::Seal(cause) => {
                    broca_session_loop::seal(&mut writer, parts.as_ref(), &seams, cause).await
                }
                crate::executor::StoreRunMode::Resume
                | crate::executor::StoreRunMode::RecheckSignIn
                | crate::executor::StoreRunMode::ProbeSignIn => {
                    broca_session_loop::run::resume(&mut writer, parts.as_ref(), &seams).await
                }
            }
            .map_err(StoreRunError::Loop)
        }
        .await;
        StoreRunEnd { parts, result }
    }
}

fn deployment(cipher: Cipher) -> DeploymentKey {
    DeploymentKey::new(1, &[7; 32]).creating(cipher)
}

/// A module over the state root `dir` whose sessions start in the new storage, scripted by
/// `steps`, rendering into `sent`, with the compaction provider it returns: a planned session's
/// history is sealed only when that provider's view cuts it.
async fn module(
    dir: &Path,
    key: &BindIdentity,
    steps: Vec<Vec<NormalizedEvent>>,
    sent: &Sent,
    stop: CancellationToken,
    cipher: Cipher,
) -> (LineageRegistry, Arc<Compactor>) {
    broca_core::media_store::create_media_directories(dir).unwrap();
    let compactor = Arc::new(Compactor::default());
    let executor: Arc<dyn RunExecutor> = Arc::new(CutExecutor {
        script: Arc::new(super::super::super::tests::Gated {
            inner: broca_core::provider::StubProvider::new(steps),
            gate: None,
            render_config: Some(image_render_config()),
            notes_sends: false,
            hold: None,
        }),
        sent: Arc::clone(sent),
        stop,
        compactor: Arc::clone(&compactor),
    });
    let runtime = Arc::new(
        crate::store_open::StoreRuntime::open(dir, deployment(cipher), Arc::clone(&executor), IDLE)
            .await
            .unwrap(),
    );
    let reader = Arc::downgrade(&runtime);
    let key = key.clone();
    compactor.reads(Arc::new(move || {
        let runtime = reader.upgrade().expect("the session runtime is open");
        let key = key.clone();
        Box::pin(async move {
            let view = runtime.current_view(&key).await.unwrap().unwrap();
            crate::store_entries::all_entries(view.as_ref())
                .await
                .unwrap()
        })
    }));
    let registry = LineageRegistry::new(Arc::new(
        crate::opener::DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            executor,
        )
        .with_idle_ttl(IDLE)
        .with_store_runtime(runtime),
    ));
    (registry, compactor)
}

/// The plan of the scenario's session: the compaction provider, and the `photo` tool its runs
/// may call.
fn plan() -> Arc<broca_wal::PlanManifest> {
    let mut plan = (*super::super::controls::compacting_plan()).clone();
    plan.tools = Arc::new(vec![serde_json::from_value(serde_json::json!({
        "name": "photo",
        "description": "shows the screen",
        "input_schema": {"type": "object"},
    }))
    .unwrap()]);
    Arc::new(plan)
}

/// A send to the scenario's session, as the side's caller sends it: the first
/// one names the session's `plan`, and the later ones carry none, as a planned session's sends
/// do.
async fn send(
    side: &Side,
    run_id: &str,
    request: SubmissionRequest,
    plan: Option<&Arc<broca_wal::PlanManifest>>,
) -> SendOutcome {
    let key = crate::actor::SendKey::Minted;
    side.registry
        .route_submission_with_order(
            &side.key,
            broca_protocol::RunId(run_id.into()),
            side.caller.shape(run_id, &key, super::planned(request)),
            crate::executor::RunKind::FreshOrResume,
            key,
            false,
            broca_protocol::Delivery::Queue,
            plan.map(|plan| crate::actor::SendPlan {
                identity: plan.plan_identity.clone(),
                manifest: Some(Arc::clone(plan)),
                model_check: None,
            }),
            None,
            None,
            None,
            None,
            false,
            None,
        )
        .await
        .expect("the session opens")
}

fn scratch(what: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "store-crash-cuts-{what}-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A copy of a state root, removed when dropped.
struct Snapshot(PathBuf);

impl Drop for Snapshot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Copies the state root `from` as a crash leaves it: every file but the leases, which name a
/// process that no longer runs.
fn snapshot(from: &Path) -> Snapshot {
    let to = scratch("snapshot");
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name() != "leases" {
            copy_tree(&entry.path(), &to.join(entry.file_name()));
        }
    }
    Snapshot(to)
}

fn copy_tree(from: &Path, to: &Path) {
    if std::fs::metadata(from).unwrap().is_dir() {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            copy_tree(&entry.path(), &to.join(entry.file_name()));
        }
    } else {
        std::fs::copy(from, to).unwrap();
    }
}

fn triple(key: &BindIdentity) -> broca_session_store::head::SessionTriple {
    broca_session_store::head::SessionTriple {
        project_root: key.project_root.to_string_lossy().into_owned(),
        harness: key.harness.clone(),
        session: key.session.clone(),
    }
}

fn addr(key: &BindIdentity) -> SessionAddr {
    SessionAddr(broca_core::session_addr(key))
}

/// The session as it lies in the state root `dir`, read without opening it for writing.
async fn view(
    dir: &Path,
    key: &BindIdentity,
    cipher: Cipher,
) -> broca_session_store::session::SessionView<LocalBackend> {
    Session::view(
        LocalBackend::new(dir.join(broca_wal::SESSION_STORAGE_DIR)),
        addr(key),
        &triple(key),
        &deployment(cipher),
    )
    .await
    .unwrap()
}

/// The session's open log in the state root `dir`: its file, relative to `dir`, and the offset
/// after each of its frames with what that frame holds, from the log's durable bound on. Each
/// offset is a record boundary a crash can leave the log at. The bytes a log was created with
/// (its checkpoint and the records a seal carries into it) were synced before the head named
/// the log, so no crash leaves it shorter than that.
async fn boundaries(
    dir: &Path,
    key: &BindIdentity,
    cipher: Cipher,
) -> (PathBuf, Vec<(u64, String)>) {
    let root = dir.join(broca_wal::SESSION_STORAGE_DIR);
    let addr = addr(key);
    let backend = LocalBackend::new(&root);
    let (stored, _) = backend.read_stored_head(&addr).unwrap().unwrap();
    let (head, session_key) =
        broca_session_store::head::open(&stored, &deployment(cipher)).unwrap();
    let frame_key = session_key.frame_key();
    let durable = head
        .open_log
        .as_ref()
        .expect("the head names its open log")
        .synced_len;
    let (log, end) = view(dir, key, cipher).await.open_log();
    let bytes = backend.read_log(&addr, &log, 0..end).await.unwrap();
    let mut bind = head.session_id.0.to_vec();
    bind.extend_from_slice(&log.0);
    let mut offset = 0;
    let mut cuts = Vec::new();
    while offset < bytes.len() {
        let FrameRead::Frame { header, body, len } =
            broca_session_store::frame::open(&bytes[offset..], |_| Some(&frame_key), &bind)
                .unwrap()
        else {
            panic!("the copied log is whole")
        };
        offset += len;
        let what = if header.stream == FrameStream::None {
            "a frame holding no record".to_owned()
        } else {
            let record = Record::decode(u32::from(header.kind), &body).unwrap();
            let name = format!("{record:?}");
            name.split(['(', ' ', '{'])
                .next()
                .unwrap_or_default()
                .to_owned()
        };
        if offset as u64 >= durable {
            cuts.push((offset as u64, what));
        }
    }
    let file = Path::new(broca_wal::SESSION_STORAGE_DIR)
        .join("logs")
        .join(&addr.0)
        .join(broca_session_store::codec::hex(&log.0));
    assert_eq!(
        std::fs::metadata(dir.join(&file)).unwrap().len(),
        end,
        "the open log's file holds the log"
    );
    (file, cuts)
}

/// The saved image indexes in the state root `dir`, relative to it, with their bytes.
fn index_objects(dir: &Path, key: &BindIdentity) -> Vec<(PathBuf, Vec<u8>)> {
    fn walk(base: &Path, at: &Path, out: &mut Vec<(PathBuf, Vec<u8>)>) {
        let Ok(entries) = std::fs::read_dir(at) else {
            return;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(base, &path, out);
            } else if path.file_name().is_some_and(|name| name == "mi") {
                let bytes = std::fs::read(&path).unwrap();
                out.push((path.strip_prefix(base).unwrap().to_owned(), bytes));
            }
        }
    }
    let mut out = Vec::new();
    let objects = dir
        .join(broca_wal::SESSION_STORAGE_DIR)
        .join("objects")
        .join(addr(key).0);
    walk(dir, &objects, &mut out);
    out
}

/// What a cut copy's saved image index is made before it opens.
#[derive(Clone, Copy)]
enum Index<'a> {
    /// As the crash left it.
    AsCut,
    /// Deleted, so the open builds the index from the session's records.
    Removed,
    /// Put back, so the open reads the index the checkpoint names.
    Restored(&'a [(PathBuf, Vec<u8>)]),
}

impl Index<'_> {
    fn name(self) -> &'static str {
        match self {
            Self::AsCut => "its index as cut",
            Self::Removed => "its index removed",
            Self::Restored(_) => "its index restored",
        }
    }
}

/// What a new module sent for a cut copy.
#[derive(Debug, PartialEq)]
struct Reopened {
    /// The requests recovery sent before any new send.
    recovered: Vec<Result<Vec<u8>, String>>,
    /// The requests of the next run sent after recovery.
    next: Vec<Result<Vec<u8>, String>>,
    /// The images the session's transcript holds after that run.
    held: usize,
    /// How the session's latest run ended, for failure messages.
    ended: String,
}

/// What recovery is expected to send for a cut session: the run and step whose request it
/// sends first, if any, and the decision that names it in failure messages.
struct Expected {
    step: Option<(String, u64)>,
    decision: String,
}

/// What recovery does with the cut session lying in `dir`: the decision the store actor makes
/// for its unfinished run (`store_actor::recovery_mode`), and the step it sends first: a run
/// paused for a restart resumes its next step, and a send still queued starts its run.
async fn expected(dir: &Path, key: &BindIdentity, cipher: Cipher) -> Expected {
    use broca_session_store::fold::operations::Resolution;
    let view = view(dir, key, cipher).await;
    let queued = !view.operations.queue.is_empty();
    let run = view.operations.run.as_ref();
    let next_run = || {
        let after = run.and_then(|run| RUNS.iter().position(|id| *id == run.run_id));
        RUNS.get(after.map_or(0, |at| at + 1))
            .map(|run| ((*run).to_owned(), 1))
    };
    match run.filter(|run| run.finished.is_none()) {
        Some(run) => {
            let resolution = run.resolution();
            let mode = crate::store_actor::recovery_mode(run);
            let decision = format!(
                "run {} after {} steps: {resolution:?}, recovered by {mode:?}{}",
                run.run_id,
                run.completed_steps,
                if queued { ", sends queued" } else { "" }
            );
            let step = match resolution {
                Resolution::Paused(PauseReason::Restart) => {
                    Some((run.run_id.clone(), run.completed_steps + 1))
                }
                _ if queued => next_run(),
                _ => None,
            };
            Expected { step, decision }
        }
        None => Expected {
            step: queued.then(next_run).flatten(),
            decision: format!(
                "no unfinished run{}",
                if queued { ", sends queued" } else { "" }
            ),
        },
    }
}

/// What a starting process does once its routes are registered: it sweeps the new storage for
/// sessions a crash or a shutdown left unfinished, sealing crash orphans and resuming runs that
/// paused for the restart, after enabling admission once dependency routes are ready.
async fn restart(side: &Side) {
    side.registry.enable_restart_resumes();
    side.registry.recover_store_crash_orphans().await.unwrap();
}

/// Cuts a copy of `snapshot` at `cut` bytes of its open log `file`, makes its saved index as
/// `index` says, starts a new module over it as a process starts after a crash, lets recovery
/// finish, then sends one more run. When `cuts_history`, the compaction provider answers its
/// first ride as it answered the original's: with the view cutting the history through the
/// session's first message.
async fn reopen(
    snapshot: &Path,
    file: &Path,
    cut: u64,
    index: Index<'_>,
    cuts_history: bool,
    key: &BindIdentity,
    cipher: Cipher,
) -> Reopened {
    let dir = scratch("cut");
    copy_tree(snapshot, &dir);
    std::fs::OpenOptions::new()
        .write(true)
        .open(dir.join(file))
        .unwrap()
        .set_len(cut)
        .unwrap();
    match index {
        Index::AsCut => {}
        Index::Removed => {
            for (path, _) in index_objects(&dir, key) {
                std::fs::remove_file(dir.join(path)).unwrap();
            }
        }
        Index::Restored(objects) => {
            for (path, bytes) in objects {
                std::fs::create_dir_all(dir.join(path).parent().unwrap()).unwrap();
                std::fs::write(dir.join(path), bytes).unwrap();
            }
        }
    }
    let sent = Sent::default();
    let steps = (1..=3)
        .map(|n| text_step(&format!("x{n}"), "noted"))
        .collect();
    let (registry, compactor) =
        module(&dir, key, steps, &sent, CancellationToken::new(), cipher).await;
    if cuts_history {
        compactor.cut_at_next_ride(1);
    }
    let side = Side {
        dir,
        registry,
        key: key.clone(),
        gate: None,
        cipher,
        caller: Default::default(),
    };
    restart(&side).await;
    side.idle().await;
    let recovered = sent.lock().unwrap().clone();
    let reply = send(&side, "rn", text("anything new?"), None).await;
    assert!(matches!(reply, SendOutcome::Active { .. }), "{reply:?}");
    side.idle().await;
    let next = sent.lock().unwrap()[recovered.len()..].to_vec();
    assert!(side.registry.stop_and_seal().await);
    let held = images_held(&side.dir, key, cipher).await;
    let ended = view(&side.dir, key, cipher)
        .await
        .operations
        .run
        .as_ref()
        .map(|run| {
            format!(
                "run {} after {} steps: {:?}, {:?}",
                run.run_id, run.completed_steps, run.finished, run.end_error
            )
        })
        .unwrap_or_default();
    Reopened {
        recovered,
        next,
        held,
        ended,
    }
}

/// The images the session in `dir` holds in its transcript.
async fn images_held(dir: &Path, key: &BindIdentity, cipher: Cipher) -> usize {
    view(dir, key, cipher)
        .await
        .conversation
        .held_messages()
        .unwrap()
        .iter()
        .map(|message| broca_session_store::session::message_media(message).len())
        .sum()
}

/// Every cut of `snapshot`'s open log after its first `from` bytes, each opened with every way
/// of `indexes`: each must recover as [`expected`] says, and all ways must send the same bytes.
/// Returns how many cuts it checked, how many of them resent a step, and what each cut that
/// failed was; each failure's reason is printed as it happens.
async fn check_cuts(
    phase: &str,
    snapshot: &Snapshot,
    from: u64,
    indexes: &[Index<'_>],
    first_sent: &HashMap<(String, u64), Vec<u8>>,
    key: &BindIdentity,
    cipher: Cipher,
) -> (usize, usize, Vec<String>) {
    use futures::FutureExt as _;
    let (file, cuts) = boundaries(&snapshot.0, key, cipher).await;
    let image = base64(&bytes_of(PNG));
    let mut checked = 0;
    let mut resent = 0;
    let mut failures = Vec::new();
    for (at, (cut, what)) in cuts.iter().enumerate().filter(|(_, (cut, _))| *cut > from) {
        let cut = *cut;
        checked += 1;
        let next = cuts
            .get(at + 1)
            .map_or("the log's end", |(_, next)| next.as_str());
        // The cut session as it lies on disk, to say what recovery must do with it.
        let probe = scratch("probe");
        copy_tree(&snapshot.0, &probe);
        std::fs::OpenOptions::new()
            .write(true)
            .open(probe.join(&file))
            .unwrap()
            .set_len(cut)
            .unwrap();
        let expected = expected(&probe, key, cipher).await;
        let _ = std::fs::remove_dir_all(&probe);
        // The original's first step of r2 was sent with the view that cut the history.
        let cuts_history = expected
            .step
            .as_ref()
            .is_some_and(|(run, step)| run == "r2" && *step == 1);
        let mut reference: Option<(Index<'_>, Reopened)> = None;
        for &index in indexes {
            let label = format!(
                "{phase}, cut at byte {cut} between {what} and {next}, {}: {}",
                index.name(),
                expected.decision
            );
            let checked = std::panic::AssertUnwindSafe(async {
                let reopened =
                    reopen(&snapshot.0, &file, cut, index, cuts_history, key, cipher).await;
                match &expected.step {
                    Some(step) => {
                        let first = first_sent.get(step).unwrap_or_else(|| {
                            panic!("{label}: the original sent no step {step:?}")
                        });
                        assert!(
                            reopened.recovered.first() == Some(&Ok(first.clone())),
                            "{label}: recovery must send step {step:?} as the original sent \
                             it; it sent {} requests",
                            reopened.recovered.len()
                        );
                        assert_eq!(
                            reopened.recovered.len(),
                            1,
                            "{label}: recovery sends that step only"
                        );
                    }
                    None => assert!(
                        reopened.recovered.is_empty(),
                        "{label}: recovery must send nothing, and sent {} requests",
                        reopened.recovered.len()
                    ),
                }
                let sent = reopened
                    .next
                    .first()
                    .unwrap_or_else(|| {
                        panic!(
                            "{label}: the next run sent nothing; the latest {}",
                            reopened.ended
                        )
                    })
                    .as_ref()
                    .unwrap_or_else(|code| panic!("{label}: the next run was refused: {code}"));
                assert_eq!(
                    String::from_utf8_lossy(sent).matches(&image).count(),
                    reopened.held,
                    "{label}: the next run's first request carries every image the session \
                     holds"
                );
                reopened
            })
            .catch_unwind()
            .await;
            match checked {
                Err(_) => failures.push(label),
                Ok(reopened) => match &reference {
                    None => reference = Some((index, reopened)),
                    Some((first, sent)) if *sent != reopened => failures.push(format!(
                        "{label}: sends other bytes than the same cut with {}",
                        first.name()
                    )),
                    Some(_) => {}
                },
            }
        }
        resent += usize::from(expected.step.is_some());
    }
    (checked, resent, failures)
}

/// Keeps the requests in `sent` as the ones the original sent for `steps`, in order.
fn first_sends(
    first_sent: &mut HashMap<(String, u64), Vec<u8>>,
    sent: &Sent,
    steps: &[(&str, u64)],
) {
    let sent = sent.lock().unwrap();
    assert_eq!(sent.len(), steps.len(), "the original sent {steps:?}");
    for ((run, step), request) in steps.iter().zip(sent.iter()) {
        let request = request.clone().expect("the original's request was sent");
        first_sent.insert(((*run).to_owned(), *step), request);
    }
}

async fn every_record_boundary_cut_of_a_session_with_images_resumes_as_sent(cipher: Cipher) {
    let dir = scratch("original");
    let key = BindIdentity::new(dir.join("project"), "store-actor", "s1");
    let plan = plan();
    let mut first_sent = HashMap::new();

    // r1 calls the tool, whose image the session captures.
    let sent = Sent::default();
    let stop = CancellationToken::new();
    let steps = vec![
        photo_step("a1"),
        text_step("a2", "I see it"),
        text_step("b1", "a photo"),
    ];
    let (registry, compactor) = module(&dir, &key, steps, &sent, stop.clone(), cipher).await;
    let mut original = Side {
        dir: dir.clone(),
        registry,
        key: key.clone(),
        gate: None,
        cipher,
        caller: Default::default(),
    };
    let reply = send(&original, "r1", text("show me"), Some(&plan)).await;
    assert!(matches!(reply, SendOutcome::Active { .. }), "{reply:?}");
    original.idle().await;
    let before_seal = snapshot(&dir);

    // r2 sends a photo, to another model, so its first step rides the compaction provider. The
    // provider's view cuts the history through the session's first message, and the session
    // seals what came before the cut, saving its index.
    compactor.cut_at_next_ride(1);
    let upload = original.uploaded(&bytes_of(PNG)).await;
    let mut look = photo("look", &[&upload]);
    look.model = broca_protocol::request::ModelHandle::new("stub", "another");
    let reply = send(&original, "r2", look, None).await;
    assert!(matches!(reply, SendOutcome::Active { .. }), "{reply:?}");
    original.idle().await;
    assert_eq!(compactor.views(), 1, "the provider handed its view over");
    let saved = view(&dir, &key, cipher)
        .await
        .read_media_index()
        .await
        .unwrap();
    assert!(
        matches!(saved, SavedIndex::Saved(_)),
        "the seal at the cut saved the session's index: {saved:?}"
    );

    // The process starts shutting down, and r3 pauses for the restart at its first boundary.
    stop.cancel();
    let mut again = text("and now?");
    again.on_restart = Some(broca_protocol::request::OnRestart::Resume);
    let reply = send(&original, "r3", again, None).await;
    assert!(
        matches!(
            reply,
            SendOutcome::Active { .. } | SendOutcome::Paused { .. }
        ),
        "{reply:?}"
    );
    original.idle().await;
    let paused = view(&dir, &key, cipher).await;
    let run = paused.operations.run.as_ref().unwrap();
    assert_eq!(
        (run.run_id.as_str(), run.paused, run.completed_steps),
        ("r3", Some(PauseReason::Restart), 0),
        "{run:?}"
    );
    drop(paused);
    first_sends(&mut first_sent, &sent, &[("r1", 1), ("r1", 2), ("r2", 1)]);
    let after_seal = snapshot(&dir);

    // After the restart r3 resumes: it calls the tool, then answers.
    assert!(original.registry.stop_and_seal().await);
    let sent = Sent::default();
    let steps = vec![photo_step("c1"), text_step("c2", "both seen")];
    (original.registry, _) =
        module(&dir, &key, steps, &sent, CancellationToken::new(), cipher).await;
    restart(&original).await;
    original.idle().await;
    first_sends(&mut first_sent, &sent, &[("r3", 1), ("r3", 2)]);

    // The saved index is lost. r4, with another photo and another image from the tool, builds
    // it once from the session's records.
    assert!(original.registry.stop_and_seal().await);
    let (_, cuts) = boundaries(&dir, &key, cipher).await;
    let built_from = cuts.last().unwrap().0;
    let lost = index_objects(&dir, &key);
    assert!(!lost.is_empty(), "the session's index is saved");
    for (path, _) in &lost {
        std::fs::remove_file(dir.join(path)).unwrap();
    }
    let sent = Sent::default();
    let steps = vec![photo_step("d1"), text_step("d2", "all seen")];
    (original.registry, _) =
        module(&dir, &key, steps, &sent, CancellationToken::new(), cipher).await;
    let saved = view(&dir, &key, cipher)
        .await
        .read_media_index()
        .await
        .unwrap();
    assert!(
        matches!(saved, SavedIndex::Missing(_)),
        "the session's index is lost: {saved:?}"
    );
    let upload = original.uploaded(&bytes_of(PNG)).await;
    let reply = send(&original, "r4", photo("and this", &[&upload]), None).await;
    assert!(matches!(reply, SendOutcome::Active { .. }), "{reply:?}");
    original.idle().await;
    first_sends(&mut first_sent, &sent, &[("r4", 1), ("r4", 2)]);
    let in_build = snapshot(&dir);
    assert!(original.registry.stop_and_seal().await);

    let mut resent = 0;
    let mut failures = Vec::new();
    for (phase, snapshot, from, indexes) in [
        ("before the seal", &before_seal, 0, &[Index::AsCut][..]),
        (
            "after the seal",
            &after_seal,
            0,
            &[Index::AsCut, Index::Removed][..],
        ),
        (
            "in the run building the lost index",
            &in_build,
            built_from,
            &[Index::AsCut, Index::Restored(&lost)][..],
        ),
    ] {
        let (checked, resumed, failed) =
            check_cuts(phase, snapshot, from, indexes, &first_sent, &key, cipher).await;
        assert!(checked > 0, "{phase}: no record boundary was cut");
        resent += resumed;
        failures.extend(failed);
    }
    assert!(
        failures.is_empty(),
        "{} cuts failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
    assert!(resent > 0, "some cut resumed a step");
    drop(original);
}
