//! A session that compacted on the current compaction contract moves with its compaction views:
//! the staged session holds the views it accepted and the steps that applied them, and renders
//! every recorded request and its next one byte for byte as today's storage renders them.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use broca_core::compaction_v2::protocol::view::{Edit, EditOp, View};
use broca_core::compaction_v2::received::ReceivedView;
use broca_protocol::BindIdentity;
use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_session_store::keys::DeploymentKey;
use broca_session_store::session::{Carry, Session};
use broca_wal::{FramedRecord, Wal, WalLineageId, WalRecord};
use futures::executor::block_on;
use serde_json::json;

use crate::stage::Stage;
use crate::verify::{Mode, Verify, ALL_STEPS, LAST_STEP, NEXT_REQUEST};

fn session() -> BindIdentity {
    BindIdentity::new("/project", "runner", "s")
}

fn deployment() -> DeploymentKey {
    DeploymentKey::new(1, &[0x5a; 32])
}

fn scratch(tag: &str) -> PathBuf {
    let root = broca_test_support::scratch_root().join(format!(
        "session-convert-views-{}-{tag}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    root
}

/// The compaction provider the plan names.
const PROVIDER: &str = "compactor";

/// The plan a compacting session froze: a compaction item naming [`PROVIDER`], on the provider
/// role's current version.
fn manifest() -> broca_wal::PlanManifest {
    use sha2::Digest as _;
    let plan = json!({
        "compaction_item": {"provider": PROVIDER, "preset": "summary", "params": {}},
        "composition": {},
    });
    let identity =
        broca_session_store::codec::hex(&sha2::Sha256::digest(serde_jcs::to_vec(&plan).unwrap()));
    broca_wal::PlanManifest {
        rich_ui: None,
        plan: broca_wal::OpaqueJson::from_json_text(plan.to_string()).unwrap(),
        plan_identity: identity,
        composition_digest: "0".repeat(64),
        fetches: Vec::new(),
        tools: Arc::new(Vec::new()),
        system_text: String::new(),
        system_digest: String::new(),
        join_version: 1,
        role_versions: [("compaction-provider".into(), "v2".into())].into(),
        declarations: Vec::new(),
    }
}

/// A run start on the current compaction contract, under the compacting plan, whose frozen render
/// config renders through today's Anthropic family.
fn run_started(run: &str, at: u64, input: &str) -> WalRecord {
    let frozen = broca_provider::families::anthropic_frozen_config("claude-test");
    let mut record: WalRecord = serde_json::from_value(json!({
        "type": "run_started", "run_id": run, "ts_ms": at,
        "session": {"project_root": "/project", "harness": "runner", "session": "s"},
        "config": {
            "model": {"provider_module_id": "anthropic", "model_id": "claude-test"},
            "tools": [], "tool_choice": {"type": "auto"}, "generation": {},
            "response_format": {"type": "text"}, "context_epoch": 0, "context_limit": 100_000,
            "renderer_version": frozen.renderer_version,
            "frozen_render_config": serde_json::to_value(&frozen).unwrap(),
        },
        "input": [{"role": "user", "content": [{"type": "text", "text": input}]}],
        "origin": {"kind": "fresh"},
    }))
    .unwrap();
    if let WalRecord::RunStarted {
        config,
        compaction_plan_version,
        manifest: frozen_manifest,
        ..
    } = &mut record
    {
        config.frozen_render_config = serde_json::to_value(&frozen).unwrap();
        *compaction_plan_version = Some(broca_core::compaction_v2::PLAN_VERSION);
        *frozen_manifest = Some(Box::new(manifest()));
    }
    record
}

fn record(value: serde_json::Value) -> WalRecord {
    serde_json::from_value(value).unwrap()
}

/// A WAL being written, with the records written so far.
struct Log {
    path: PathBuf,
    wal: broca_wal::FileWal,
    lineage: Option<WalLineageId>,
    written: Vec<WalRecord>,
}

impl Log {
    fn open(root: &Path) -> Self {
        let path = root.join("session.wal");
        // The run starts record the compaction plan version and the plan, each a WAL feature
        // the log's floor must name before the record is written.
        let wal = broca_wal::FileWal::open(&path)
            .unwrap()
            .with_feature_floor(root.join(broca_wal::FEATURE_FLOOR_FILENAME));
        let lineage = wal.lineage_id();
        Self {
            path,
            wal,
            lineage,
            written: Vec::new(),
        }
    }

    fn append(&mut self, record: WalRecord) {
        self.wal.append(1, &record, true).unwrap();
        self.written.push(record);
    }

    /// The id today's storage gives the message at 0-based `position` on this log.
    fn mid(&self, position: u64) -> String {
        broca_core::message_mid(true, self.lineage().as_deref(), position).unwrap()
    }

    fn lineage(&self) -> Option<String> {
        self.lineage.map(|id| id.to_string())
    }

    /// Today's render of step `step` of run `run` from what is written so far.
    fn render(&self, run: &str, step: u64) -> Vec<u8> {
        let config = self
            .written
            .iter()
            .rev()
            .find_map(|record| match record {
                WalRecord::RunStarted { config, .. } => Some((**config).clone()),
                _ => None,
            })
            .unwrap();
        let (_, _, body) = broca_module::wal_render::render_step_with_lineage(
            self.lineage().as_deref(),
            &self.written,
            &config,
            run,
            step,
            None,
            None,
        )
        .unwrap();
        body
    }

    /// One model step of run `run` answering `answer`, its attempt recording the digest of the
    /// request today's renderer builds for it, after the records `before` that today's loop
    /// writes between the step's start and its request (a view accepted, applied and marked).
    /// Returns the request's body.
    fn step(
        &mut self,
        run: &str,
        step: u64,
        at: u64,
        before: Vec<WalRecord>,
        answer: &str,
    ) -> Vec<u8> {
        self.append(record(json!({"type": "step_started", "step_id": step})));
        for record in before {
            self.append(record);
        }
        let body = self.render(run, step);
        self.append(record(json!({
            "type": "model_attempt_finished", "step_id": step, "attempt": 0,
            "outcome": "complete", "ts_ms": at + 1,
            "request_body_len": body.len(),
            "request_body_sha256": crate::verify::sha256_hex(&body),
            "response_body_len": 0,
            "response_body_sha256": crate::verify::sha256_hex(b""),
        })));
        self.append(record(json!({
            "type": "model_step_finished", "step_id": step,
            "assistant_message": {"message_id": format!("a{step}"),
                "content": [{"type": "text", "text": answer}]},
            "usage": {"input_tokens": 3, "output_tokens": 1},
            "finish_reason": "stop", "retries_used": 0,
        })));
        body
    }

    fn finish(&mut self, at: u64) {
        self.append(record(json!({
            "type": "run_finished", "reason": "completed", "ts_ms": at + 3,
            "usage": {"input_tokens": 3, "output_tokens": 1}, "retries_used": 0,
        })));
    }

    /// The WAL as read back from its file.
    fn read(self) -> (Vec<FramedRecord>, Option<WalLineageId>) {
        drop(self.wal);
        let read = broca_wal::read_records_range(
            &self.path,
            broca_wal::DEFAULT_MAX_READ_BYTES,
            0,
            None,
            None,
        )
        .unwrap();
        let broca_wal::WalRangeRecords::Complete(records) = read.records() else {
            panic!("the WAL reads back whole")
        };
        (records.to_vec(), read.lineage_id)
    }
}

/// The record today's journal writes for `view`, accepted after `previous`.
fn accepted(previous: Option<&View>, view: &View, applied_by: Option<&str>) -> WalRecord {
    let previous = previous.cloned().map(ReceivedView::from);
    let received = ReceivedView::from(view.clone());
    WalRecord::CompactionViewV2(broca_wal::CompactionViewV2 {
        version: view.version,
        checksum: view.checksum.to_string(),
        edits: broca_core::compaction_v2::storage::encode(previous.as_ref(), &received),
        built_through: view.built_through.clone(),
        new_signed: view.new_signed.clone(),
        applied_by: applied_by.map(str::to_owned),
    })
}

fn applied(step: u64, view: &View) -> WalRecord {
    WalRecord::CompactionViewAppliedV2 {
        step_id: step,
        version: view.version,
        checksum: view.checksum.to_string(),
    }
}

/// A compacted session's WAL: a first run; a second whose step applies view 1, which cuts
/// through the session's first message, and marks the cut; then view 2, handed over by the
/// provider after that run and accepted without a step applying it yet. Returns the WAL and the
/// body of the second run's request.
fn compacted(
    root: &Path,
    applied_by: Option<&str>,
) -> (Vec<FramedRecord>, Option<WalLineageId>, Vec<u8>) {
    let mut log = Log::open(root);
    log.append(run_started("r1", 1_000, "Hello."));
    log.step("r1", 1, 1_000, Vec::new(), "One.");
    log.finish(1_000);
    log.append(run_started("r2", 2_000, "Again."));
    let first = View::seal_with_policy(
        1,
        vec![Edit::new(
            1,
            EditOp::Cut {
                through: log.mid(0),
            },
        )],
        Some(log.mid(1)),
        Some("strip".into()),
    );
    let marker = WalRecord::CompactionMarker {
        harness: "runner".into(),
        session: "s".into(),
        view_version: 1,
        first_kept_mid: log.mid(1),
        first_kept_ordinal: 2,
        checkpoint: 0,
    };
    let body = log.step(
        "r2",
        2,
        2_000,
        vec![
            accepted(None, &first, applied_by),
            applied(2, &first),
            marker,
        ],
        "Two.",
    );
    log.finish(2_000);
    let second = View::seal(
        2,
        vec![Edit::new(
            1,
            EditOp::Cut {
                through: log.mid(1),
            },
        )],
    );
    log.append(accepted(Some(&first), &second, Some(PROVIDER)));
    let (records, lineage) = log.read();
    (records, lineage, body)
}

fn moved(records: &[FramedRecord]) -> broca_session_store::record::Moved {
    broca_session_store::record::Moved {
        source_lineage: None,
        move_id: "store-move:test".into(),
        source_activity_ms: broca_module::head_time::updated_at_of_wal(records, None),
    }
}

/// The compacted session of [`compacted`], converted with no refusal and written to its staging
/// folder under `root`.
struct Compacted {
    records: Vec<FramedRecord>,
    lineage: Option<WalLineageId>,
    /// The body of the request the step that applied view 1 sent.
    body: Vec<u8>,
    staged: crate::stage::Staged,
    staging: PathBuf,
}

fn staged(root: &Path) -> Compacted {
    staged_with_applier(root, None)
}

fn staged_with_applier(root: &Path, applied_by: Option<&str>) -> Compacted {
    let (records, lineage, body) = compacted(root, applied_by);
    assert_eq!(crate::preconditions::wal_refusal(&records, lineage), None);
    let lineage_text = lineage.map(|id| id.to_string());
    let prepared = crate::stage::prepare(&records, lineage_text.as_deref(), None).unwrap();
    assert_eq!(
        block_on(crate::preconditions::converted_refusal(&prepared)),
        Ok(None),
        "a session holding compaction views converts"
    );
    let staging = root.join("session-store/staging/store-move-test");
    let stage = Stage {
        source_history_version: 1,
        records: &records,
        lineage: lineage_text.as_deref(),
        session: &session(),
        deployment: &deployment(),
        store_root: &root.join("session-store"),
        staging: &staging,
        media: None,
        moved: &moved(&records),
    };
    let staged = block_on(crate::stage::write(&stage, prepared)).unwrap();
    Compacted {
        records,
        lineage,
        body,
        staged,
        staging,
    }
}

/// Every check of the verifier's offline mode over the staged session, every recorded request
/// and the session's next one included.
fn verify_offline(root: &Path, compacted: &Compacted) -> crate::verify::Verified {
    let verified = block_on(crate::verify::verify(Verify {
        records: &compacted.records,
        lineage: compacted.lineage,
        session: &session(),
        deployment: &deployment(),
        store_root: &root.join("session-store"),
        staging: &compacted.staging,
        converted: &compacted.staged.converted,
        mode: Mode::Offline,
        media: None,
        media_root: None,
        todays_updated_at: broca_module::head_time::updated_at_of_wal(&compacted.records, None),
    }))
    .unwrap_or_else(|mismatch| panic!("{mismatch}"));
    for check in [LAST_STEP, NEXT_REQUEST, ALL_STEPS] {
        assert!(verified.checks.contains(&check), "{check}");
    }
    verified
}

/// The staged session as its writer opens it.
fn load(staging: &Path) -> Session<LocalBackend> {
    block_on(Session::load(
        LocalBackend::new(staging),
        SessionAddr(broca_core::session_addr(&session())),
        &crate::stage::triple(&session()),
        &deployment(),
    ))
    .unwrap()
}

/// The applied and accepted views the run loop restores from the session's open log, by version
/// and checksum.
fn restored_views(store: &Session<LocalBackend>) -> [Option<(u64, String)>; 2] {
    let records = block_on(store.open_log_records()).unwrap();
    let state =
        broca_session_loop::compaction::restore(&records, &session(), &store.operations).unwrap();
    [state.applied(), state.latest()]
        .map(|view| view.map(|view| (view.version, view.checksum.to_string())))
}

/// A compacted session converts with no refusal, and its staged copy agrees with its WAL on every
/// check, every recorded request and its next one included. The view the second run applied hides
/// the session's first message from that run's request, so the requests compared are compacted.
#[test]
fn a_compacted_session_moves_with_its_views_and_renders_alike() {
    let root = scratch("compacted");
    let compacted = staged(&root);
    let request = String::from_utf8_lossy(&compacted.body);
    assert!(request.contains("Again."), "{request}");
    assert!(
        !request.contains("Hello."),
        "the view's cut hides the first message: {request}"
    );
    verify_offline(&root, &compacted);
    // The staged session names both views, and holds what restores them.
    let store = load(&compacted.staging);
    assert_eq!(
        restored_views(&store),
        [
            Some((1, view_checksum(&compacted, 1))),
            Some((2, view_checksum(&compacted, 2)))
        ]
    );
    drop(store);
    let _ = std::fs::remove_dir_all(&root);
}

/// Verification compares the complete applied-view reply, including the recorded provider,
/// rather than the provider of a newer accepted view. An absent provider remains absent.
#[test]
fn a_converted_applied_view_matches_todays_read_with_build_metadata_and_applier() {
    for applied_by in [Some("original-compactor"), None] {
        let root = scratch(if applied_by.is_some() {
            "applier"
        } else {
            "no-applier"
        });
        let compacted = staged_with_applier(&root, applied_by);
        let store = load(&compacted.staging);
        let records = block_on(store.open_log_records()).unwrap();
        let state =
            broca_session_loop::compaction::restore(&records, &session(), &store.operations)
                .unwrap();
        let view = state.applied().unwrap();
        let through = broca_core::message_mid(
            true,
            compacted.lineage.map(|id| id.to_string()).as_deref(),
            1,
        )
        .unwrap();
        assert_eq!(view.built_through.as_deref(), Some(through.as_str()));
        assert_eq!(view.new_signed.as_deref(), Some("strip"));
        assert_eq!(state.applied_by(), applied_by);
        assert_eq!(state.accepted_by(), Some(PROVIDER));
        drop(store);
        verify_offline(&root, &compacted);
        let _ = std::fs::remove_dir_all(&root);
    }
}

/// The new storage's first seal of a converted compacted session moves the stored views out of
/// its open log, so the run loop must carry copies of them into the new one, as it does for a
/// session that compacted in the new storage. Sealed at the view's cut, as the loop seals when a
/// step applies a cut, the session restores the same views from its new open log, and still
/// agrees with its WAL on every check, its next request included.
#[test]
fn a_converted_sessions_first_seal_carries_its_views() {
    let root = scratch("sealed");
    let compacted = staged(&root);
    let mut store = load(&compacted.staging);
    let before = restored_views(&store);
    let records = block_on(store.open_log_records()).unwrap();
    let state =
        broca_session_loop::compaction::restore(&records, &session(), &store.operations).unwrap();
    let carry = broca_session_loop::compaction::carried_views(&state, &store.operations).unwrap();
    let marker = store
        .conversation
        .marker
        .clone()
        .expect("the converted session holds the cut's marker");
    // Without the copies the store refuses the seal, which would lose the views.
    assert!(matches!(
        block_on(store.seal_at_entry(marker.first_kept_ordinal, &Carry::default(), 1)),
        Err(broca_session_store::session::SessionError::ViewNotCarried)
    ));
    let carry = Carry {
        records: &carry,
        window_repetition: None,
    };
    block_on(store.seal_at_entry(marker.first_kept_ordinal, &carry, 1)).unwrap();
    block_on(store.close()).unwrap();
    let store = load(&compacted.staging);
    assert!(
        store.conversation.window_start() > 1,
        "the history before the cut is sealed"
    );
    assert_eq!(restored_views(&store), before);
    drop(store);
    verify_offline(&root, &compacted);
    let _ = std::fs::remove_dir_all(&root);
}

/// The checksum of the view with `version` that the WAL accepted.
fn view_checksum(compacted: &Compacted, version: u64) -> String {
    compacted
        .records
        .iter()
        .find_map(|frame| match &frame.record {
            WalRecord::CompactionViewV2(view) if view.version == version => {
                Some(view.checksum.clone())
            }
            _ => None,
        })
        .unwrap()
}

/// A session on the current compaction contract whose replies carry part-number notation is
/// moved and verified: the staged copy keeps the replies as the model wrote them, serves them
/// cleaned (its entry facts say its runs clean replies), and serves the source's history
/// version raised by the exposure epoch, and the verifier, which compares today's reads raised
/// and cleaned the same way, finds no mismatch.
#[test]
fn a_session_whose_replies_carry_part_numbers_moves_and_verifies() {
    let root = scratch("tagged-replies");
    let written = [
        "\u{a7}2\u{a7} One.",
        "As \u{a7}1\u{a7} said, \u{a7}3\u{a7} two.",
    ];
    let mut log = Log::open(&root);
    log.append(run_started("r1", 1_000, "Hello."));
    log.step("r1", 1, 1_000, Vec::new(), written[0]);
    log.finish(1_000);
    log.append(run_started("r2", 2_000, "Again."));
    log.step("r2", 2, 2_000, Vec::new(), written[1]);
    log.finish(2_000);
    let (records, lineage) = log.read();
    let lineage_text = lineage.map(|id| id.to_string());
    let prepared = crate::stage::prepare(&records, lineage_text.as_deref(), None).unwrap();
    let staging = root.join("session-store/staging/store-move-test");
    let source_history_version = 3;
    let staged = block_on(crate::stage::write(
        &Stage {
            source_history_version,
            records: &records,
            lineage: lineage_text.as_deref(),
            session: &session(),
            deployment: &deployment(),
            store_root: &root.join("session-store"),
            staging: &staging,
            media: None,
            moved: &moved(&records),
        },
        prepared,
    ))
    .unwrap();
    let compacted = Compacted {
        records,
        lineage,
        body: Vec::new(),
        staged,
        staging,
    };
    verify_offline(&root, &compacted);

    let store = load(&compacted.staging);
    assert_eq!(store.head().history_version, source_history_version);
    let stored: Vec<String> = block_on(store.entries(1, 16))
        .unwrap()
        .into_iter()
        .filter(|entry| entry.message.role == broca_protocol::Role::Assistant)
        .inspect(|entry| assert!(entry.state.facts.as_ref().unwrap().clean_reply))
        .flat_map(|entry| entry.message.content)
        .filter_map(|block| match block {
            broca_protocol::ContentBlock::Text { text } => Some(text),
            _ => None,
        })
        .collect();
    assert_eq!(stored, written, "the staged copy keeps the written replies");
    let parts = broca_session_loop::parts::MemoryParts {
        parts: compacted.staged.converted.parts.clone(),
    };
    let request = broca_module::serve::SessionReadRequest::new(session(), Some(1), None);
    let read = block_on(broca_module::store_read::serve_view(
        &store, &parts, &request,
    ))
    .unwrap();
    assert_eq!(
        read.history_version,
        broca_module::store_exposure::served_history_version(source_history_version)
    );
    let served: Vec<String> = read
        .messages
        .iter()
        .filter(|entry| entry.message.role == broca_protocol::Role::Assistant)
        .flat_map(|entry| entry.message.content.clone())
        .filter_map(|block| match block {
            broca_protocol::ContentBlock::Text { text } => Some(text),
            _ => None,
        })
        .collect();
    let cleaned: Vec<String> = written
        .iter()
        .map(|text| broca_protocol::tags::exposed_model_text(true, text).into_owned())
        .collect();
    assert_eq!(served, cleaned);
    assert_ne!(served, written);
    drop(store);
    let _ = std::fs::remove_dir_all(&root);
}
