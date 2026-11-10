//! The move against a real registry, store runtime and WAL on a scratch state root: a full
//! move, each refusal, a planted verification mismatch, and a crash at every step.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use broca_core::{FileLeaseStore, FileWalStore, RunIndexRow, RunIndexState};
use broca_core::{OutputEvent, RunError, RunSummary};
use broca_module::executor::{RunExecutor, RunJob};
use broca_module::opener::DefaultOpener;
use broca_module::LineageRegistry;
use broca_protocol::BindIdentity;
use broca_session_store::keys::Cipher;
use broca_wal::{FramedRecord, Wal as _, WalRecord};
use serde_json::json;
use tokio::sync::mpsc;

use super::*;

/// A change planted in the frozen records before the verifier reads them.
type Plant = Box<dyn Fn(&mut Vec<FramedRecord>) + Send>;

/// A test's faults: a crash at the Nth step point, and a change planted in the records the
/// verifier compares the staged session with.
#[derive(Default)]
pub(crate) struct Faults {
    crash_at: Mutex<Option<usize>>,
    seen: Mutex<Vec<&'static str>>,
    plant: Mutex<Option<Plant>>,
}

impl Faults {
    pub(crate) fn crash_here(&self, name: &'static str) -> bool {
        let mut seen = self.seen.lock().unwrap();
        seen.push(name);
        *self.crash_at.lock().unwrap() == Some(seen.len())
    }

    pub(crate) fn planted(&self, mut records: Vec<FramedRecord>) -> Vec<FramedRecord> {
        if let Some(plant) = self.plant.lock().unwrap().as_ref() {
            plant(&mut records);
        }
        records
    }
}

struct NoRuns;

#[async_trait]
impl RunExecutor for NoRuns {
    async fn execute(
        &self,
        _job: RunJob,
        _out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        unreachable!("these tests run no model step")
    }
}

const RUNS: [&str; 2] = ["r1", "r2"];

fn key() -> BindIdentity {
    BindIdentity::new("/project", "runner", "s")
}

fn deployment() -> broca_session_store::keys::DeploymentKey {
    broca_session_store::keys::DeploymentKey::new(1, &[7; 32])
}

struct Rig {
    dir: PathBuf,
    wal_store: Arc<FileWalStore>,
    registry: LineageRegistry,
    runtime: Arc<broca_module::store_open::StoreRuntime>,
    forgotten: Arc<AtomicUsize>,
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn runtime(dir: &Path) -> Arc<broca_module::store_open::StoreRuntime> {
    runtime_creating(dir, Cipher::XChaCha20Poly1305).await
}

/// A runtime with its deployment key loaded that creates sessions in mode `cipher`, as
/// `session_storage.encryption` configures it.
async fn runtime_creating(
    dir: &Path,
    cipher: Cipher,
) -> Arc<broca_module::store_open::StoreRuntime> {
    Arc::new(
        broca_module::store_open::StoreRuntime::open(
            dir,
            deployment().creating(cipher),
            Arc::new(NoRuns),
            Duration::from_millis(100),
        )
        .await
        .unwrap(),
    )
}

impl Rig {
    async fn start(tag: &str) -> Self {
        Self::start_creating(tag, Cipher::XChaCha20Poly1305).await
    }

    /// A rig whose new storage creates sessions in mode `cipher`.
    async fn start_creating(tag: &str, cipher: Cipher) -> Self {
        let dir = broca_test_support::scratch_root()
            .join(format!("broca-store-move-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let wal_store = Arc::new(FileWalStore::new(dir.join("wal")));
        let runtime = runtime_creating(&dir, cipher).await;
        let opener = DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal_store.clone(),
            Arc::new(NoRuns),
        )
        .with_store_runtime(runtime.clone());
        let registry = LineageRegistry::new_with_reader(Arc::new(opener), wal_store.clone(), None);
        Self {
            dir,
            wal_store,
            registry,
            runtime,
            forgotten: Arc::default(),
        }
    }

    fn wal(&self) -> PathBuf {
        self.wal_store.wal_path(&key())
    }

    /// A move with every operation served and a settled run index.
    fn mover(&self) -> StoreMove {
        let forgotten = self.forgotten.clone();
        StoreMove::new(
            self.registry.clone(),
            &self.dir,
            Arc::new(broca_module::store_open::StoreSlot::loaded(
                self.runtime.clone(),
            )),
            self.wal_store.clone(),
            Arc::new(FileLeaseStore::new(self.dir.join("leases"))),
            Arc::new(|_: &str| true),
        )
        .with_run_rows(Arc::new(|session: &BindIdentity| {
            Ok(RUNS.iter().map(|run| settled_row(session, run)).collect())
        }))
        .with_forget_projection(Arc::new(move |_: &BindIdentity| {
            forgotten.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }))
    }

    /// Writes `records` as the session's WAL, after a run start's frozen render config.
    fn write(&self, records: &[WalRecord]) {
        let mut wal = broca_wal::FileWal::open(self.wal())
            .unwrap()
            .with_feature_floor(self.dir.join(broca_wal::FEATURE_FLOOR_FILENAME));
        for record in records {
            wal.append(1, record, true).unwrap();
        }
    }

    fn write_session(&self) -> Vec<u8> {
        self.write_session_in(StorageClass::Ordinary)
    }

    /// Writes the session's WAL in the folder of `class`, as today's storage places it.
    fn write_session_in(&self, class: StorageClass) -> Vec<u8> {
        let path = self.wal_store.class_wal_path(class, &key());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let records = session_records(&path);
        let mut wal = broca_wal::FileWal::open(&path)
            .unwrap()
            .with_feature_floor(self.dir.join(broca_wal::FEATURE_FLOOR_FILENAME));
        for record in &records {
            wal.append(1, record, true).unwrap();
        }
        drop(wal);
        std::fs::read(path).unwrap()
    }

    /// The new storage's ordinary root, where a session from `wal/` moves.
    fn store(&self) -> PathBuf {
        self.dir.join(broca_wal::SESSION_STORAGE_DIR)
    }
}

fn settled_row(session: &BindIdentity, run: &str) -> RunIndexRow {
    RunIndexRow {
        run_id: run.into(),
        session: session.clone(),
        submission_id: None,
        request_identity_digest: None,
        state: RunIndexState::Completed,
        usage: Some(Default::default()),
        final_step_finish_reason: None,
        retries_used: 0,
        title: None,
        withdrawals_owed: false,
    }
}

fn record(value: serde_json::Value) -> WalRecord {
    serde_json::from_value(value).unwrap()
}

fn run_started(run: &str, at: u64) -> WalRecord {
    let frozen = broca_provider::families::anthropic_frozen_config("claude-test");
    record(json!({
        "type": "run_started", "run_id": run, "ts_ms": at,
        "session": {"project_root": "/project", "harness": "runner", "session": "s"},
        "config": {
            "model": {"provider_module_id": "anthropic", "model_id": "claude-test"},
            "tools": [], "tool_choice": {"type": "auto"}, "generation": {},
            "response_format": {"type": "text"}, "context_epoch": 0,
            "renderer_version": frozen.renderer_version,
            "frozen_render_config": serde_json::to_value(&frozen).unwrap(),
        },
        "input": [{"role": "user", "content": [{"type": "text", "text": format!("Hello from {run}.")}]}],
        "origin": {"kind": "fresh"},
    }))
}

/// Two runs, each one model step answering in text, whose attempts recorded the digest of the
/// request today's renderer builds for them. The WAL at `path` must not exist yet: its lineage
/// is read from the file the first append creates.
fn session_records(path: &Path) -> Vec<WalRecord> {
    session_records_with(path, false)
}

/// A run start of [`run_started`] on the current compaction contract, under a plan naming a
/// compaction provider.
fn compacting_run_started(run: &str, at: u64) -> WalRecord {
    let mut start = run_started(run, at);
    if let WalRecord::RunStarted {
        config,
        compaction_plan_version,
        manifest,
        ..
    } = &mut start
    {
        config.context_limit = Some(100_000);
        *compaction_plan_version = Some(broca_core::compaction_v2::PLAN_VERSION);
        *manifest = Some(Box::new(broca_wal::PlanManifest {
            rich_ui: None,
            plan: broca_wal::OpaqueJson::from_json_text(
                json!({"compaction_item": {"provider": "compactor", "preset": "summary",
                    "params": {}}, "composition": {}})
                .to_string(),
            )
            .unwrap(),
            plan_identity: "compacting-plan".into(),
            composition_digest: "composition".into(),
            fetches: Vec::new(),
            tools: Arc::new(Vec::new()),
            system_text: String::new(),
            system_digest: String::new(),
            join_version: 1,
            role_versions: [("compaction-provider".into(), "v2".into())].into(),
            declarations: Vec::new(),
        }));
    }
    start
}

/// [`session_records`], on the current compaction contract when `compacted`: then the second
/// run's step applies a view cutting through the session's first message before its request, as
/// today's loop records it (the view accepted, the step applying it, and the cut's marker), so
/// that request leaves the first message out.
fn session_records_with(path: &Path, compacted: bool) -> Vec<WalRecord> {
    use broca_core::compaction_v2::protocol::view::{Edit, EditOp, View};
    let lineage = {
        let wal = broca_wal::FileWal::open(path).unwrap();
        wal.lineage_id().map(|id| id.to_string())
    };
    let mid = |position| broca_core::message_mid(true, lineage.as_deref(), position).unwrap();
    let view =
        compacted.then(|| View::seal(1, vec![Edit::new(1, EditOp::Cut { through: mid(0) })]));
    let mut written: Vec<WalRecord> = Vec::new();
    for (index, run) in RUNS.iter().enumerate() {
        let step = index as u64 + 1;
        let at = 1_000 * step;
        let start = if compacted {
            compacting_run_started(run, at)
        } else {
            run_started(run, at)
        };
        let WalRecord::RunStarted { config, .. } = &start else {
            unreachable!()
        };
        let config = (**config).clone();
        written.push(start);
        written.push(record(json!({"type": "step_started", "step_id": step})));
        if let Some(view) = view.as_ref().filter(|_| step == 2) {
            let received = broca_core::compaction_v2::received::ReceivedView::from(view.clone());
            written.push(WalRecord::CompactionViewV2(broca_wal::CompactionViewV2 {
                version: view.version,
                checksum: view.checksum.clone(),
                edits: broca_core::compaction_v2::storage::encode(None, &received),
                built_through: view.built_through.clone(),
                new_signed: view.new_signed.clone(),
                applied_by: None,
            }));
            written.push(WalRecord::CompactionViewAppliedV2 {
                step_id: step,
                version: view.version,
                checksum: view.checksum.clone(),
            });
            written.push(WalRecord::CompactionMarker {
                harness: "runner".into(),
                session: "s".into(),
                view_version: view.version,
                first_kept_mid: mid(1),
                first_kept_ordinal: 2,
                checkpoint: 0,
            });
        }
        let (_, _, body) = broca_module::wal_render::render_step_with_lineage(
            lineage.as_deref(),
            &written,
            &config,
            run,
            step,
            None,
            None,
        )
        .unwrap();
        let sha = |bytes: &[u8]| broca_wal::migration::digest(bytes);
        written.push(record(json!({
            "type": "model_attempt_finished", "step_id": step, "attempt": 0,
            "outcome": "complete", "ts_ms": at + 1,
            "request_body_len": body.len(), "request_body_sha256": sha(&body),
            "response_body_len": 0, "response_body_sha256": sha(b""),
        })));
        written.push(record(json!({
            "type": "model_step_finished", "step_id": step,
            "assistant_message": {"message_id": format!("m{step}"),
                "content": [{"type": "text", "text": format!("Answer {step}.")}]},
            "usage": {"input_tokens": 3, "output_tokens": 1},
            "finish_reason": "stop", "retries_used": 0,
        })));
        written.push(record(json!({
            "type": "run_finished", "reason": "completed", "ts_ms": at + 3,
            "usage": {"input_tokens": 3, "output_tokens": 1}, "retries_used": 0,
        })));
    }
    written
}

/// A session whose log's records carry no time (one written before they did) moves, and its
/// head serves the same last update after the move as today's head served before it: the live
/// WAL file's write time, which the move records in the session (`Record::Moved`). Without it
/// the new storage could only refuse the head (`session_time_unknown`).
#[tokio::test]
async fn a_session_whose_records_carry_no_time_keeps_its_heads_last_update() {
    use broca_module::serve::ServeHandler as _;
    let rig = Rig::start("timeless").await;
    let mut records = Vec::new();
    for (index, run) in RUNS.iter().enumerate() {
        let step = index as u64 + 1;
        let mut start = run_started(run, 0);
        if let WalRecord::RunStarted { ts_ms, .. } = &mut start {
            *ts_ms = None;
        }
        records.push(start);
        records.push(record(json!({"type": "step_started", "step_id": step})));
        records.push(record(json!({
            "type": "model_step_finished", "step_id": step,
            "assistant_message": {"message_id": format!("m{step}"),
                "content": [{"type": "text", "text": format!("Answer {step}.")}]},
            "usage": {"input_tokens": 3, "output_tokens": 1},
            "finish_reason": "stop", "retries_used": 0,
        })));
        records.push(record(json!({
            "type": "run_finished", "reason": "completed",
            "usage": {"input_tokens": 3, "output_tokens": 1}, "retries_used": 0,
        })));
    }
    assert!(records
        .iter()
        .all(|record| broca_core::record_time_ms(record).is_none()));
    rig.write(&records);
    let today = rig.registry.head(&key()).await.unwrap().updated_at;
    assert!(today.is_some_and(|at| at > 0), "{today:?}");
    let moved = rig.mover().move_to_store(&key(), None).await.unwrap();
    assert_eq!(moved["moved"], true, "{moved}");
    let after = rig.registry.head(&key()).await.unwrap();
    assert!(
        after.incarnation != "unwritten",
        "served from the new storage"
    );
    assert_eq!(after.updated_at, today);
}

#[tokio::test]
async fn a_move_keeps_frozen_file_time_even_when_record_time_is_later() {
    use broca_module::serve::ServeHandler as _;
    let rig = Rig::start("record-ahead-of-file-time").await;
    rig.write_session();
    let (records, _) = read_wal(&rig.wal()).unwrap();
    let newest = broca_module::head_time::newest_record_ms(&records).unwrap();
    let written = newest - 1;
    std::fs::OpenOptions::new()
        .write(true)
        .open(rig.wal())
        .unwrap()
        .set_times(
            std::fs::FileTimes::new()
                .set_modified(std::time::UNIX_EPOCH + Duration::from_millis(written)),
        )
        .unwrap();
    let before = rig.registry.head(&key()).await.unwrap().updated_at.unwrap();
    assert_eq!(
        before, written,
        "today's head prioritizes the live file time"
    );
    eprintln!("head activity: live-file mtime={written}, last recorded time={newest}, today's selected={before}; the staged Moved record must preserve the selected file time, not the record maximum");
    let moved = rig.mover().move_to_store(&key(), None).await.unwrap();
    assert_eq!(moved["moved"], true);
    assert_eq!(
        rig.registry.head(&key()).await.unwrap().updated_at,
        Some(before)
    );
}

/// Every name under `path`, relative to it, for comparing what a folder holds.
fn tree(path: &Path) -> Vec<String> {
    fn walk(root: &Path, path: &Path, out: &mut Vec<String>) {
        let Ok(entries) = std::fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            out.push(path.strip_prefix(root).unwrap().display().to_string());
            if path.is_dir() {
                walk(root, &path, out);
            }
        }
    }
    let mut out = Vec::new();
    walk(path, path, &mut out);
    out.sort();
    out
}

/// The session is on today's storage, exactly as it was: its WAL holds `bytes`, no freeze or
/// marker sits beside it, and the new storage holds nothing under its address in any root, and
/// no staging folder or journal.
fn assert_today(rig: &Rig, bytes: &[u8]) {
    assert_today_at(rig, &rig.wal(), bytes);
}

/// [`assert_today`] for a session whose WAL is `wal`.
fn assert_today_at(rig: &Rig, wal: &Path, bytes: &[u8]) {
    let address = broca_core::session_addr(&key());
    assert_eq!(std::fs::read(wal).unwrap(), bytes, "the WAL is unchanged");
    assert!(!wal.with_extension("moving").exists(), "no freeze");
    assert!(!wal.with_extension("moved").exists(), "no marker");
    for class in StorageClass::ALL {
        let left: Vec<String> = tree(&class.store_root(&rig.dir))
            .into_iter()
            .filter(|path| {
                path.contains(&address)
                    || path.starts_with("staging/")
                    || path.starts_with("moves/")
            })
            .collect();
        assert!(
            left.is_empty(),
            "the new storage's {class} root holds {left:?}"
        );
    }
    assert_no_move_left(rig);
    assert_eq!(
        broca_wal::store_ownership(&rig.dir, &address).unwrap(),
        broca_wal::StoreOwnership::Today
    );
    rig.wal_store.check_move(&key(), true).unwrap();
}

/// No journal or staged session is left in the moves folder.
fn assert_no_move_left(rig: &Rig) {
    let left: Vec<String> = tree(&broca_wal::session_storage_moves(&rig.dir))
        .into_iter()
        .filter(|path| Path::new(path).components().count() > 1)
        .collect();
    assert!(left.is_empty(), "the moves folder holds {left:?}");
}

/// The session is the new storage's alone: its head and log exist in the ordinary root, today's
/// WAL is sealed with the `moved` marker naming the new storage, and no staging folder or
/// journal is left. A runtime opened afresh, as after a restart, reads it.
async fn assert_moved(rig: &Rig) {
    assert_moved_into(rig, &rig.wal(), StorageClass::Ordinary).await;
}

/// [`assert_moved`] for a session whose WAL was `wal`, into the root of `class`.
async fn assert_moved_into(rig: &Rig, wal: &Path, class: StorageClass) {
    let address = broca_core::session_addr(&key());
    assert_eq!(
        broca_wal::store_ownership(&rig.dir, &address).unwrap(),
        broca_wal::StoreOwnership::Held
    );
    assert_eq!(
        broca_wal::session_storage_class(&rig.dir, &address).unwrap(),
        Some(class)
    );
    assert!(class
        .store_root(&rig.dir)
        .join("logs")
        .join(&address)
        .exists());
    assert!(!wal.with_extension("moving").exists(), "no freeze");
    let moved: broca_wal::migration::Moved =
        serde_json::from_slice(&std::fs::read(wal.with_extension("moved")).unwrap()).unwrap();
    assert_eq!(moved.holder, HOLDER);
    assert_no_move_left(rig);
    let restarted = runtime(&rig.dir).await;
    assert_eq!(restarted.class_of(&key()).unwrap(), Some(class));
    let view = restarted
        .current_view(&key())
        .await
        .unwrap()
        .expect("the moved session reads");
    assert_eq!(broca_module::store_read::transcript_len(&view), 4);
}

#[tokio::test]
async fn a_moved_session_is_the_new_storages_alone() {
    let rig = Rig::start("moves").await;
    rig.write_session();
    let reply = rig.mover().move_to_store(&key(), None).await.unwrap();
    assert_eq!(reply["moved"], true);
    assert_eq!(reply["runs"], 2);
    assert_eq!(reply["messages"], 4);
    assert_eq!(reply["media"], 0);
    assert!(reply["checks"]
        .as_array()
        .unwrap()
        .iter()
        .any(|check| check == "next_request"));
    assert_moved(&rig).await;
    assert_eq!(
        rig.forgotten.load(Ordering::SeqCst),
        1,
        "its rows were dropped"
    );
    // Today's storage refuses it by name, and the registry routes it to the new storage.
    assert!(rig.wal_store.check_move(&key(), false).is_err());
    rig.registry.check_move(&key(), true).unwrap();
    // A second move refuses: the session is there already.
    let again = rig.mover().move_to_store(&key(), None).await.unwrap_err();
    assert_eq!(again.code, "not_movable");
    assert_eq!(again.detail.unwrap()["reason"], "in_new_storage");
}

/// A session that compacted on the current contract moves with its compaction views. The move
/// verifies before it publishes, so it moves only because the copy rendered the session's last
/// recorded request, whose step applied a view, and its next request as today's storage renders
/// them. The moved session's first seal, at its view's cut as the run loop seals, moves the stored
/// views out of the open log; carrying the loop's copies of them, it keeps them: the session
/// reloaded after the seal restores the same views.
#[tokio::test]
async fn a_compacted_session_moves_with_its_views() {
    use broca_session_store::backend::{LocalBackend, SessionAddr};
    use broca_session_store::session::{Carry, Session};
    let rig = Rig::start("compacted").await;
    let records = session_records_with(&rig.wal(), true);
    rig.write(&records);
    let reply = rig.mover().move_to_store(&key(), None).await.unwrap();
    assert_eq!(reply["moved"], true, "{reply}");
    let checks = reply["checks"].as_array().unwrap();
    for check in ["last_step", "next_request"] {
        assert!(checks.iter().any(|ran| ran == check), "{reply}");
    }
    assert_moved(&rig).await;

    let root = StorageClass::Ordinary.store_root(&rig.dir);
    let triple = broca_session_convert::stage::triple(&key());
    let deployment = deployment();
    let load = || {
        Session::load(
            LocalBackend::new(&root),
            SessionAddr(broca_core::session_addr(&key())),
            &triple,
            &deployment,
        )
    };
    let restored =
        |records: &[(u64, broca_session_store::record::Record)],
         operations: &broca_session_store::fold::operations::Operations| {
            broca_session_loop::compaction::restore(records, &key(), operations).unwrap()
        };
    let mut session = load().await.unwrap();
    let state = restored(
        &session.open_log_records().await.unwrap(),
        &session.operations,
    );
    let applied = state
        .applied()
        .cloned()
        .expect("the moved session holds the applied view");
    assert_eq!(applied.version, 1);
    let carry = broca_session_loop::compaction::carried_views(&state, &session.operations).unwrap();
    let cut = session
        .conversation
        .marker
        .as_ref()
        .expect("the moved session holds the cut's marker")
        .first_kept_ordinal;
    session
        .seal_at_entry(
            cut,
            &Carry {
                records: &carry,
                window_repetition: None,
            },
            1,
        )
        .await
        .unwrap();
    session.close().await.unwrap();
    let session = load().await.unwrap();
    assert!(
        session.conversation.window_start() > 1,
        "the history before the cut is sealed"
    );
    let after = restored(
        &session.open_log_records().await.unwrap(),
        &session.operations,
    );
    assert_eq!(after.applied(), Some(&applied));
    assert_eq!(after.latest(), Some(&applied));
}

/// Refuses with `reason` and writes nothing.
async fn assert_refused(rig: &Rig, mover: StoreMove, bytes: &[u8], reason: &str) {
    let refused = mover.move_to_store(&key(), None).await.unwrap_err();
    assert_eq!(refused.code, "not_movable", "{refused:?}");
    assert_eq!(refused.detail.unwrap()["reason"], reason);
    assert_today(rig, bytes);
}

#[tokio::test]
async fn each_precondition_refuses_by_name_and_writes_nothing() {
    // An operation the new storage doesn't serve, named.
    let rig = Rig::start("refuse-op").await;
    let bytes = rig.write_session();
    let mut mover = rig.mover();
    mover.served = Arc::new(|op: &str| op != "session.history");
    assert_refused(&rig, mover, &bytes, "session.history").await;
    drop(rig);

    // A run that hasn't ended.
    let rig = Rig::start("refuse-busy").await;
    let mut records = session_records(&rig.wal());
    records.pop();
    rig.write(&records);
    let bytes = std::fs::read(rig.wal()).unwrap();
    assert_refused(&rig, rig.mover(), &bytes, "session_busy").await;
    drop(rig);

    // A tool batch left open in a run that ended.
    let rig = Rig::start("refuse-batch").await;
    let mut records = session_records(&rig.wal());
    let end = records.len() - 1;
    records.insert(
        end,
        record(
            json!({"type": "tool_batch_started", "batch_id": "b1", "step_id": 2,
            "planned_calls": []}),
        ),
    );
    rig.write(&records);
    let bytes = std::fs::read(rig.wal()).unwrap();
    assert_refused(&rig, rig.mover(), &bytes, "open_tool_batch").await;
    drop(rig);

    // A run with an input-token budget, which the new run loop refuses.
    let rig = Rig::start("refuse-feature").await;
    let mut records = session_records(&rig.wal());
    if let WalRecord::RunStarted { config, .. } = &mut records[0] {
        config.budget = Some(serde_json::from_value(json!({"max_input_tokens": 5})).unwrap());
    }
    rig.write(&records);
    let bytes = std::fs::read(rig.wal()).unwrap();
    assert_refused(&rig, rig.mover(), &bytes, "unsupported_feature").await;
    drop(rig);

    // A run-index row still owed reconciliation.
    let rig = Rig::start("refuse-index").await;
    let bytes = rig.write_session();
    let mover = rig
        .mover()
        .with_run_rows(Arc::new(|session: &BindIdentity| {
            let mut row = settled_row(session, "r2");
            row.usage = None;
            Ok(vec![settled_row(session, "r1"), row])
        }));
    assert_refused(&rig, mover, &bytes, "reconcile_owed").await;
    drop(rig);

    // A registered late result.
    let rig = Rig::start("refuse-late").await;
    let bytes = rig.write_session();
    let late = Arc::new(
        broca_module::late_results_store::LateResultsStore::open(&rig.dir.join("late.db")).unwrap(),
    );
    late.register_calls(&[broca_core::tool_plane::LateResultRegistration {
        lineage: "lineage".into(),
        call_key: "call".into(),
        module: "provider".into(),
        bind: key(),
        scope: None,
    }])
    .unwrap();
    let mover = rig
        .mover()
        .with_late_results(late, broca_module::held_slots::HeldSlots::new());
    assert_refused(&rig, mover, &bytes, "session_busy").await;
    drop(rig);

    // A medium today's media store doesn't hold, then one whose bytes no longer match its
    // digest: `media_missing`, naming the medium, how it can't be read and how many can't.
    for (state, object) in [
        ("absent", None),
        ("corrupt", Some(b"other bytes".as_slice())),
    ] {
        let rig = Rig::start(&format!("refuse-media-{state}")).await;
        let digest = with_media(&rig, object);
        let bytes = std::fs::read(rig.wal()).unwrap();
        let media = tree(&rig.dir.join("media"));
        let refused = rig.mover().move_to_store(&key(), None).await.unwrap_err();
        assert_eq!(refused.code, "not_movable", "{refused:?}");
        assert_eq!(
            refused.detail.unwrap(),
            json!({"reason": "media_missing", "digest": digest, "state": state, "missing": 1})
        );
        assert_today(&rig, &bytes);
        assert_eq!(
            tree(&rig.dir.join("media")),
            media,
            "today's media store is unchanged"
        );
    }
}

/// The bytes of the image [`with_media`] names.
const IMAGE: &[u8] = b"image bytes";

/// Writes the session's WAL with its first run's input holding [`IMAGE`], in a state root whose
/// media stores exist, as a module's do. Today's media store holds `object` as the image's
/// bytes, or nothing when `None`. Returns the image's digest.
fn with_media(rig: &Rig, object: Option<&[u8]>) -> String {
    broca_core::media_store::create_media_directories(&rig.dir).unwrap();
    let digest = broca_wal::migration::digest(IMAGE);
    let mut records = session_records(&rig.wal());
    if let WalRecord::RunStarted { input, .. } = &mut records[0] {
        *input = std::sync::Arc::new(vec![serde_json::from_value(json!({
            "role": "user", "content": [{"type": "media", "kind": "image",
                "mime": "image/png", "byte_size": IMAGE.len(), "width": 1, "height": 1,
                "sha256": digest}],
        }))
        .unwrap()]);
    }
    if let Some(object) = object {
        std::fs::write(rig.dir.join("media/objects").join(&digest), object).unwrap();
    }
    rig.write(&records);
    digest
}

/// A session whose media today's store holds is staged with them, and the staged copy is checked
/// against the request each step sent. Here the WAL's recorded requests predate the image its
/// input was given, so the check of its last step differs: the move aborts naming that check,
/// and leaves today's storage as it was, with no staged object behind.
#[tokio::test]
async fn a_session_holding_media_is_staged_and_its_abort_leaves_nothing() {
    let rig = Rig::start("media-abort").await;
    with_media(&rig, Some(IMAGE));
    let bytes = std::fs::read(rig.wal()).unwrap();
    let refused = rig.mover().move_to_store(&key(), None).await.unwrap_err();
    assert_eq!(refused.code, "move_verification_failed", "{refused:?}");
    assert_eq!(refused.detail.unwrap()["check"], "last_step");
    assert_today(&rig, &bytes);
}

#[tokio::test]
async fn a_module_without_a_mover_refuses_store_unavailable() {
    let rig = Rig::start("unavailable").await;
    let bytes = rig.write_session();
    let refused = rig.registry.move_to_store(&key(), None).await.unwrap_err();
    assert_eq!(refused.code, "store_unavailable");
    assert_today(&rig, &bytes);
}

/// Until the deployment key has loaded, a move that would create an encrypted session refuses
/// as store sessions are refused then: `key_unavailable`, transient, with nothing written. Both
/// before the runtime opens and once it serves plain sessions without the key.
#[tokio::test]
async fn a_move_before_the_key_loads_refuses_key_unavailable() {
    let rig = Rig::start("awaiting-key").await;
    let bytes = rig.write_session();
    let mut mover = rig.mover();
    mover.slot = Arc::new(broca_module::store_open::StoreSlot::awaiting_key(
        &rig.dir, 1,
    ));
    let refused = mover.move_to_store(&key(), None).await.unwrap_err();
    assert_eq!(refused.code, "key_unavailable", "{refused:?}");
    assert_today(&rig, &bytes);

    let slot = Arc::new(broca_module::store_open::StoreSlot::awaiting_key(
        &rig.dir, 1,
    ));
    slot.open_without_key(&setup(&rig.dir)).await.unwrap();
    assert!(slot.runtime().is_some(), "plain sessions are served");
    let mut mover = rig.mover();
    mover.slot = slot;
    let refused = mover.move_to_store(&key(), None).await.unwrap_err();
    assert_eq!(refused.code, "key_unavailable", "{refused:?}");
    assert_today(&rig, &bytes);
}

/// What the slot opens its runtime with, besides the deployment key.
fn setup(dir: &Path) -> broca_module::store_open::StoreRuntimeSetup {
    broca_module::store_open::StoreRuntimeSetup {
        state_root: dir.to_path_buf(),
        executor: Arc::new(NoRuns),
        idle_ttl: Duration::from_millis(100),
        import_roots: None,
        billing: None,
    }
}

/// A move creates the session in the mode new sessions are created in
/// (`session_storage.encryption`). Plain or encrypted, the moved session verifies, and after a
/// restart it reloads in that mode and reads. A plain move needs no deployment key.
#[tokio::test]
async fn a_move_creates_the_session_in_the_configured_mode() {
    for cipher in Cipher::ALL {
        let rig = Rig::start_creating(&format!("mode-{}", cipher.label()), cipher).await;
        rig.write_session();
        let reply = rig.mover().move_to_store(&key(), None).await.unwrap();
        assert_eq!(reply["moved"], true, "{}", cipher.label());
        assert_moved(&rig).await;
        let restarted = runtime_creating(&rig.dir, cipher).await;
        let view = restarted.current_view(&key()).await.unwrap().unwrap();
        assert_eq!(view.head().cipher(), cipher);
    }

    // Plain, before the key has loaded: the runtime the slot opened without it moves the
    // session.
    let rig = Rig::start_creating("mode-plain-no-key", Cipher::None).await;
    rig.write_session();
    let slot = Arc::new(
        broca_module::store_open::StoreSlot::awaiting_key(&rig.dir, 1).creating(Cipher::None),
    );
    slot.open_without_key(&setup(&rig.dir)).await.unwrap();
    assert!(!slot.key_loaded());
    let mut mover = rig.mover();
    mover.slot = slot;
    mover.move_to_store(&key(), None).await.unwrap();
    assert_moved(&rig).await;
    let view = runtime(&rig.dir)
        .await
        .current_view(&key())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(view.head().cipher(), Cipher::None);
}

/// A difference planted in what the staged session is checked against aborts the move, naming
/// the check, and the session goes on working on today's storage.
#[tokio::test]
async fn a_planted_mismatch_aborts_naming_its_check() {
    let rig = Rig::start("planted").await;
    let bytes = rig.write_session();
    let mover = rig.mover();
    *mover.faults.plant.lock().unwrap() = Some(Box::new(|records: &mut Vec<FramedRecord>| {
        for frame in records.iter_mut() {
            if let WalRecord::ModelStepFinished {
                assistant_message, ..
            } = &mut frame.record
            {
                std::sync::Arc::make_mut(assistant_message).content =
                    vec![
                        serde_json::from_value(json!({"type": "text", "text": "Changed."}))
                            .unwrap(),
                    ];
            }
        }
    }));
    let refused = mover.move_to_store(&key(), None).await.unwrap_err();
    assert_eq!(refused.code, "move_verification_failed", "{refused:?}");
    assert_eq!(refused.detail.unwrap()["check"], "run_result");
    assert_today(&rig, &bytes);
    // The session opens on today's storage.
    rig.registry.actor_for(&key()).await.unwrap();
}

/// A crash at every step point: startup leaves exactly one owner and no orphaned bytes. Before
/// the head is written the session is today's, unchanged; from the head write on the move is
/// finished.
#[tokio::test]
async fn startup_after_a_crash_at_every_step_leaves_one_owner() {
    let steps = {
        let rig = Rig::start("crash-count").await;
        rig.write_session();
        let mover = rig.mover();
        mover.move_to_store(&key(), None).await.unwrap();
        let seen = mover.faults.seen.lock().unwrap().clone();
        seen
    };
    assert!(
        steps.contains(&"rename_logs") && steps.contains(&"head"),
        "{steps:?}"
    );
    let published = steps.iter().position(|step| *step == "head").unwrap() + 1;
    for at in 1..=steps.len() {
        let rig = Rig::start(&format!("crash-{at}")).await;
        let bytes = rig.write_session();
        let mover = rig.mover();
        *mover.faults.crash_at.lock().unwrap() = Some(at);
        let crashed = mover.move_to_store(&key(), None).await.unwrap_err();
        assert_eq!(crashed.code, "crashed", "step {at} ({})", steps[at - 1]);
        drop(mover);
        // Until startup settles the move, its journal or head keeps today's storage from
        // serving the session.
        assert!(
            rig.wal_store.store_owner_refusal(&key()).unwrap().is_some(),
            "step {at}"
        );
        let first = recover_at_startup(&rig.dir);
        assert!(first.refused.is_empty(), "step {at}: {first:?}");
        let forgotten = rig.forgotten.clone();
        let forget: ForgetProjection = Arc::new(move |_: &BindIdentity| {
            forgotten.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        let second = finish_at_startup(&rig.dir, Some(&forget), None);
        assert!(second.refused.is_empty(), "step {at}: {second:?}");
        if at >= published {
            assert_moved(&rig).await;
        } else {
            assert_today(&rig, &bytes);
        }
    }
}

/// Startup refuses to guess: a journal it can't read stays, and the session isn't served from
/// today's storage while it does.
#[tokio::test]
async fn an_unreadable_journal_is_left_and_refuses_the_session() {
    let rig = Rig::start("unreadable").await;
    rig.write_session();
    let address = broca_core::session_addr(&key());
    let journal = broca_wal::session_storage_move_journal(&rig.dir, &address);
    std::fs::create_dir_all(journal.parent().unwrap()).unwrap();
    std::fs::write(&journal, b"not json").unwrap();
    let recovered = recover_at_startup(&rig.dir);
    assert_eq!(recovered.refused, std::slice::from_ref(&address));
    assert!(journal.exists());
    rig.registry.check_move(&key(), true).unwrap();
    // The daemon hands startup's unsettled moves to its registry.
    rig.registry.refuse_unsettled_moves(recovered.refused);
    assert!(rig.registry.check_move(&key(), true).is_err());
    assert!(rig.registry.actor_for(&key()).await.is_err());
}

/// At the seal, today's late-results intake forgets the moved session's registrations and
/// keeps every other session's: a late or duplicate entry for one of the moved session's calls
/// then finds no registration, and the puller acks it as a no-op (it never writes a WAL or
/// holds the feed for it).
#[tokio::test]
async fn the_seal_hands_the_sessions_late_results_off() {
    let rig = Rig::start("late-handoff").await;
    let late =
        broca_module::late_results_store::LateResultsStore::open(&rig.dir.join("late.db")).unwrap();
    let other = BindIdentity::new("/project", "runner", "other");
    let registration =
        |bind: &BindIdentity, call_key: &str| broca_core::tool_plane::LateResultRegistration {
            lineage: format!("lineage-{call_key}"),
            call_key: call_key.into(),
            module: "provider".into(),
            bind: bind.clone(),
            scope: None,
        };
    late.register_calls(&[
        registration(&key(), "moved-call"),
        registration(&other, "kept-call"),
    ])
    .unwrap();
    retire(&key(), None, Some(&late)).unwrap();
    let left: Vec<String> = late
        .registrations()
        .unwrap()
        .into_iter()
        .map(|row| row.call_key)
        .collect();
    assert_eq!(left, ["kept-call"]);
}

/// While a move fences the session, a late result's delivery refuses and is retried later,
/// and a second move refuses as busy.
#[tokio::test]
async fn a_fenced_session_takes_no_delivery_and_no_second_move() {
    let rig = Rig::start("fenced").await;
    rig.write_session();
    let leases = FileLeaseStore::new(rig.dir.join("leases"));
    let fence = rig
        .registry
        .fence_store_move(&key(), &leases)
        .await
        .unwrap();
    assert!(fence.holds_lease());
    let delivery = rig.registry.resume_held_run(&key()).await.unwrap_err();
    assert!(delivery.starts_with("session_moving"), "{delivery}");
    assert_eq!(
        rig.registry
            .fence_store_move(&key(), &leases)
            .await
            .err()
            .unwrap(),
        broca_module::registry::store_move::FenceRefusal::Busy("session_moving".into())
    );
    drop(fence);
    rig.registry
        .fence_store_move(&key(), &leases)
        .await
        .unwrap();
}

/// What precondition 1 still refuses: every required operation the new storage doesn't serve
/// yet, read from the serve layer's allowlist and the compaction-runner route. The new storage
/// serves every one of them now, so a real move passes it; an operation added to the required
/// list before it is served there shows up here.
#[test]
fn precondition_one_names_what_the_new_storage_still_does_not_serve() {
    let served = served_by_serve_layer();
    let unserved: Vec<&str> = REQUIRED_OPERATIONS
        .iter()
        .copied()
        .filter(|op| !served(op))
        .collect();
    assert_eq!(unserved, Vec::<&str>::new());
}

/// A session moves into the root of the class its WAL's folder has: `wal-durable/` into the
/// durable root, `wal-ephemeral/` into the ephemeral root, `wal/` into the ordinary root. The
/// other roots get nothing of it.
#[tokio::test]
async fn each_wal_folder_moves_into_its_class_root() {
    let address = broca_core::session_addr(&key());
    for class in StorageClass::ALL {
        let rig = Rig::start(&format!("class-{class}")).await;
        rig.write_session_in(class);
        let reply = rig.mover().move_to_store(&key(), None).await.unwrap();
        assert_eq!(reply["class"], class.name(), "{reply}");
        let wal = rig.wal_store.class_wal_path(class, &key());
        assert_moved_into(&rig, &wal, class).await;
        for other in StorageClass::ALL
            .into_iter()
            .filter(|other| *other != class)
        {
            let left: Vec<String> = tree(&other.store_root(&rig.dir))
                .into_iter()
                .filter(|path| path.contains(&address))
                .collect();
            assert!(left.is_empty(), "{class}: the {other} root holds {left:?}");
        }
    }
}

/// The operator's `class: durable` moves a session from `wal/` into the durable root, with
/// the setup parts its runs name, so a restore of that root needs nothing outside it.
#[tokio::test]
async fn an_operator_moves_an_ordinary_session_into_the_durable_root() {
    // Not `class-durable`: `each_wal_folder_moves_into_its_class_root` names its durable rig
    // that, and the rig's folder is named by process and tag, so the two would share a folder.
    let rig = Rig::start("operator-durable").await;
    rig.write_session();
    let reply = rig
        .mover()
        .move_to_store(&key(), Some(StorageClass::Durable))
        .await
        .unwrap();
    assert_eq!(reply["class"], "durable", "{reply}");
    assert_moved_into(&rig, &rig.wal(), StorageClass::Durable).await;
    let durable = tree(&StorageClass::Durable.store_root(&rig.dir));
    assert!(
        durable.iter().any(|path| path.starts_with("setup")),
        "the durable root holds the session's setup parts: {durable:?}"
    );
    let ordinary = tree(&rig.store());
    assert!(
        ordinary
            .iter()
            .all(|path| !path.starts_with("setup")
                && !path.contains(&broca_core::session_addr(&key()))),
        "the ordinary root holds nothing of it: {ordinary:?}"
    );
}

/// The one class change a move makes is an ordinary session into the durable root. A session
/// with a retention policy never moves where backups capture it, and a durable session never
/// leaves the backed-up root: both refuse `class_refused`, writing nothing.
#[tokio::test]
async fn any_other_class_change_is_refused_and_writes_nothing() {
    for (tag, source, requested) in [
        (
            "ephemeral-to-durable",
            StorageClass::Ephemeral,
            StorageClass::Durable,
        ),
        (
            "durable-to-ordinary",
            StorageClass::Durable,
            StorageClass::Ordinary,
        ),
    ] {
        let rig = Rig::start(&format!("class-refused-{tag}")).await;
        let bytes = rig.write_session_in(source);
        let refused = rig
            .mover()
            .move_to_store(&key(), Some(requested))
            .await
            .unwrap_err();
        assert_eq!(refused.code, "not_movable", "{tag}: {refused:?}");
        let detail = refused.detail.unwrap();
        assert_eq!(detail["reason"], "class_refused", "{tag}");
        assert_eq!(detail["wal_class"], source.name(), "{tag}");
        assert_eq!(detail["class"], requested.name(), "{tag}");
        assert_today_at(&rig, &rig.wal_store.class_wal_path(source, &key()), &bytes);
    }
}

/// A move keeps its journal and its staged session in the moves folder, outside every class
/// root, and the journal names the class it moves into. Startup undoes a move that stopped
/// there, leaving the moves folder empty.
#[tokio::test]
async fn a_moves_journal_and_staging_live_in_the_moves_folder() {
    let staged_at = {
        let rig = Rig::start("moves-folder-count").await;
        rig.write_session();
        let mover = rig.mover();
        mover.move_to_store(&key(), None).await.unwrap();
        let seen = mover.faults.seen.lock().unwrap().clone();
        seen.iter().position(|step| *step == "stage").unwrap() + 1
    };
    let rig = Rig::start("moves-folder").await;
    let bytes = rig.write_session();
    let mover = rig.mover();
    *mover.faults.crash_at.lock().unwrap() = Some(staged_at);
    let crashed = mover
        .move_to_store(&key(), Some(StorageClass::Durable))
        .await
        .unwrap_err();
    assert_eq!(crashed.code, "crashed");
    drop(mover);
    let address = broca_core::session_addr(&key());
    let moves = broca_wal::session_storage_moves(&rig.dir);
    let journal_path = broca_wal::session_storage_move_journal(&rig.dir, &address);
    assert!(journal_path.starts_with(moves.join(broca_wal::MOVE_JOURNAL_DIR)));
    let journal: MoveJournal =
        serde_json::from_slice(&std::fs::read(&journal_path).unwrap()).unwrap();
    assert_eq!(journal.class, Some(StorageClass::Durable));
    let staging = moves.join(&journal.staging);
    assert!(staging.starts_with(moves.join(broca_wal::MOVE_STAGING_DIR)));
    assert!(!tree(&staging).is_empty(), "the staged session is there");
    for class in StorageClass::ALL {
        let inside: Vec<String> = tree(&class.store_root(&rig.dir))
            .into_iter()
            .filter(|path| path.starts_with("staging") || path.starts_with("moves"))
            .collect();
        assert!(inside.is_empty(), "the {class} root holds {inside:?}");
    }
    let recovered = recover_at_startup(&rig.dir);
    assert_eq!(recovered.rolled_back, std::slice::from_ref(&address));
    assert_today(&rig, &bytes);
}
