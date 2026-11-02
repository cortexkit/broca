//! The installed format-2 bundle without transport, using the real service handlers.

// Compile the daemon's boot implementation itself, not a second implementation of its order.
// These files are compiled whole, and this test calls only the handles it measures, so the
// rest is dead code in this test target.
#[allow(dead_code)]
#[path = "../src/composition.rs"]
mod composition;
#[allow(dead_code)]
#[path = "../src/overhaul_daemon.rs"]
mod overhaul_daemon;
#[allow(dead_code)]
#[path = "../src/state_format.rs"]
mod state_format;
use composition::opener_for_state_root;

use broca_wal::access_counter::{OperationAccess, ReadClass, ValidationRead, WalAccessCounter};
use std::collections::BTreeMap;

fn fallbacks(access: &OperationAccess) -> BTreeMap<&'static str, usize> {
    let mut counts = BTreeMap::new();
    for reason in &access.seed_fallbacks {
        assert!(!reason.is_empty(), "{}: unnamed fallback", access.name);
        *counts.entry(*reason).or_default() += 1;
    }
    counts
}

fn check_operation(access: &OperationAccess, expected: &[(&'static str, usize)], resume: bool) {
    check_budget(access, expected, resume, 0);
}

fn check_budget(
    access: &OperationAccess,
    expected: &[(&'static str, usize)],
    resume: bool,
    origin_budget: usize,
) {
    assert_eq!(
        fallbacks(access),
        expected.iter().copied().collect(),
        "{}: fallback budget",
        access.name
    );
    let origins = access
        .reads
        .iter()
        .filter(|read| read.class == ReadClass::Origin)
        .count();
    let writer_decodes: u64 = access
        .writer_opens
        .iter()
        .map(|open| open.frames_decoded)
        .sum();
    let service_bytes: u64 = access
        .reads
        .iter()
        .filter(|read| read.class != ReadClass::WriterOpen)
        .map(|read| read.length)
        .sum();
    println!(
        "{}: gated reads={} bytes={} decodes={} origins={} fallbacks={:?}",
        access.name,
        access
            .reads
            .iter()
            .filter(|read| read.class != ReadClass::WriterOpen)
            .count(),
        service_bytes,
        access.frames_decoded - writer_decodes,
        origins,
        fallbacks(access)
    );
    assert_eq!(
        origins, origin_budget,
        "{}: service/resume origin read",
        access.name
    );
    for open in &access.writer_opens {
        println!(
            "{}: writer-open EXCLUDED session_length={} opens={} bytes={} decodes={}",
            access.name, open.session_length, open.opens, open.bytes, open.frames_decoded
        );
        assert_eq!(
            open.opens, 1,
            "each writer-open bucket must name one recovery open"
        );
        assert!(open.bytes >= open.session_length);
    }
    if resume {
        assert!(
            access
                .reads
                .iter()
                .any(|read| read.class == ReadClass::Validation(ValidationRead::BoundaryFrame)),
            "{}: resume did not authenticate a stored seed",
            access.name
        );
    }
}

fn fixture_startup(name: &str) -> bool {
    name.starts_with("writer ")
        || name == "zero-step ordinary episode"
        || name == "sparse window setup"
}

fn report_fixture_writer(startup: &OperationAccess, recovery: &OperationAccess) {
    assert!(recovery.name.starts_with("writer-open fixture "));
    assert!(fallbacks(startup).is_empty());
    assert!(fallbacks(recovery).is_empty());
    let opens: Vec<_> = startup
        .writer_opens
        .iter()
        .chain(&recovery.writer_opens)
        .collect();
    assert_eq!(opens.iter().map(|open| open.opens).sum::<u64>(), 1);
    let open = opens.iter().find(|open| open.opens == 1).unwrap();
    assert!(opens.iter().all(|other| other.path == open.path));
    // The startup continuation and the blocking open use different operation
    // contexts. Sum each recorded range once, including writer-state replays
    // after the descriptor opens; do not make that cost disappear at the handoff.
    println!(
        "writer-open class EXCLUDED: {} session_length={} opens=1 bytes={} decodes={}",
        startup.name,
        open.session_length,
        startup
            .reads
            .iter()
            .chain(&recovery.reads)
            .map(|read| read.length)
            .sum::<u64>(),
        startup.frames_decoded + recovery.frames_decoded
    );
}

#[test]
fn read_gate_rejects_an_origin_read_and_an_unexpected_named_fallback() {
    let counter = WalAccessCounter::default();
    counter
        .operation("bad service")
        .run(|| broca_wal::access_counter::record_read(0, 70));
    let access = counter.snapshot().remove(0);
    assert!(std::panic::catch_unwind(|| check_operation(&access, &[], false)).is_err());
    let counter = WalAccessCounter::default();
    counter
        .operation("bad fallback")
        .run(|| broca_wal::access_counter::seed_fallback("fixture fallback"));
    let access = counter.snapshot().remove(0);
    assert!(std::panic::catch_unwind(|| check_operation(&access, &[], false)).is_err());
    check_operation(&access, &[("fixture fallback", 1)], false);
}

#[test]
fn read_gate_rejects_a_resume_that_never_validated_a_seed() {
    let access = OperationAccess {
        name: "unvalidated resume".into(),
        ..Default::default()
    };
    assert!(std::panic::catch_unwind(|| check_operation(&access, &[], true)).is_err());
}

use broca_core::WalStore;
use broca_module::serve::{SendRequest, ServeHandler, SessionReadRequest, SubscribeFrom};
use broca_protocol::{BindIdentity, ContentBlock, RunId};
use broca_wal::WalRecord;
use overhaul_daemon::OverhaulDaemon;
use serde_json::json;
use std::path::{Path, PathBuf};

fn root(label: &str) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let root = broca_test_support::scratch_root().join(format!(
        "installed-{label}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&root).unwrap();
    state_format::prepare(&root, true).unwrap();
    root
}

fn no_legacy_index(root: &Path) {
    for name in ["run-index.db", "run-index.db-wal", "run-index.db-shm"] {
        assert!(
            !root.join(name).exists(),
            "{} appeared",
            root.join(name).display()
        );
    }
}

fn request() -> broca_core::RunRequest {
    let mut request = broca_core::RunRequest::from_submission(
        serde_json::from_value(json!({
            "model":{"provider_module_id":"stub","model_id":"m"},
            "input":[{"role":"user","content":[{"type":"text","text":"history question"}]}],
            "tools":[{"name":"work","description":"fixture work","input_schema":{"type":"object"}}]
        }))
        .unwrap(),
        RunId("fixture".into()),
        BindIdentity::new("/installed", "runner", "fixture"),
        broca_protocol::SubmissionId("fixture".into()),
    );
    // The daemon timestamps admitted input. Legacy fixture records without this
    // field deliberately use the separately tested historical-timestamp fallback.
    request.input_written_at_ms = Some(1_000);
    request
}

fn scripts() -> Vec<Vec<broca_protocol::NormalizedEvent>> {
    use broca_protocol::*;
    let message = AssistantMessage {
        message_id: MessageId("tool-message".into()),
        content: vec![ContentBlock::ToolCall(ToolCall {
            tool_call_id: ToolCallId("call".into()),
            tool_name: "work".into(),
            input: json!({}),
            thought_signature: None,
        })],
    };
    let mut steps = vec![vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage { message }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ]];
    steps.extend((broca_crash_harness::process_kill_probe().scripts)());
    steps
}

async fn partial_turn() -> Vec<WalRecord> {
    let scenario = broca_crash_harness::Scenario {
        name: "installed-fixture",
        request,
        control: broca_core::RunControl::default,
        scripts,
        render: broca_crash_harness::Render::Canonical,
        plane: broca_core::StubToolPlane::new,
        expected: broca_protocol::RunFinishReason::Completed,
        extension: None,
    };
    let reference = broca_crash_harness::RunnerHandle::new(scenario).await;
    let records = reference.reference.log.records();
    let end = records
        .iter()
        .position(|record| matches!(record, WalRecord::TurnFinished { .. }))
        .unwrap();
    records[..=end].to_vec()
}

async fn write_turn(
    daemon: &OverhaulDaemon,
    access: &WalAccessCounter,
    bind: &BindIdentity,
    id: &str,
    template: &[WalRecord],
    media: bool,
) -> (u64, broca_core::WalHandle) {
    let path = daemon.wal.wal_path(bind);
    let root = path.parent().unwrap().parent().unwrap();
    let tenure = broca_core::lease::WalLeaseDomain::new(root.join("leases"))
        .acquire(bind)
        .unwrap();
    let store = std::sync::Arc::new(overhaul_daemon::CountedWriterStore {
        inner: daemon.wal.clone(),
        operation: access.operation(format!("writer-open fixture {id}")),
    });
    let writer = broca_core::WalHandle::open_and_spawn_owned_with_class(
        store.clone(),
        bind.clone(),
        tenure,
        None,
        None,
        None,
    )
    .await
    .unwrap();
    // Pin the separate writer-lifecycle publication instead of letting its
    // coalescing with the first run change the fixture's fallback count.
    let opened = writer.sync().await.unwrap();
    published(daemon, bind, opened).await;
    let frames = store.read_records(bind).unwrap();
    let checkpoint = broca_core::AdmissionCheckpointV1::from_frames(&frames).unwrap();
    writer.seed_lineage_tags(checkpoint.tags);
    for record in template {
        let mut record = record.clone();
        if let WalRecord::RunStarted {
            session,
            run_id,
            submission_id,
            input,
            ..
        } = &mut record
        {
            *session = bind.clone();
            *run_id = RunId(id.into());
            *submission_id = Some(broca_protocol::SubmissionId(format!("sub-{id}")));
            let mut messages = input.to_vec();
            if media {
                messages
                    .last_mut()
                    .unwrap()
                    .content
                    .push(ContentBlock::Media(broca_protocol::MediaRef {
                        kind: broca_protocol::MediaKind::Image,
                        mime: "image/png".into(),
                        byte_size: 69,
                        sha256: "a".repeat(64),
                        width: Some(1),
                        height: Some(1),
                    }));
            }
            *input = messages.into();
        }
        writer.record(record).into_result().unwrap();
    }
    let seq = writer.sync().await.unwrap();
    (seq, writer)
}

async fn published(daemon: &OverhaulDaemon, bind: &BindIdentity, seq: u64) {
    let db = daemon.stores.overhaul.as_ref().unwrap().db.clone();
    let bind = bind.clone();
    // The publisher uses a real OS thread. Wait off the Tokio clock: paused test time
    // must neither starve its feed nor turn a publication assertion into a timer race.
    tokio::task::spawn_blocking(move || broca_test_support::wait_until_sync("installed publication feed", || {
        db.read_snapshot(|tx| {
            Ok(tx.query_row("SELECT COALESCE(indexed_seq,0) FROM session WHERE project_root=?1 AND harness=?2 AND session_key=?3",
                rusqlite::params![bind.project_root.to_string_lossy(), bind.harness, bind.session], |r| r.get::<_,u64>(0)).ok())
        }).map_err(|e| e.to_string()).and_then(|head| if head.is_some_and(|head| head >= seq) { Ok(()) } else { Err(format!("head={head:?}, expected {seq}")) })
    })).await.unwrap();
}

async fn resume(daemon: &OverhaulDaemon, bind: &BindIdentity, id: &str) {
    submit(daemon, bind, id, json!([]), "completed").await;
}

async fn submit(
    daemon: &OverhaulDaemon,
    bind: &BindIdentity,
    id: &str,
    input: serde_json::Value,
    expected: &str,
) {
    let response = daemon
        .registry
        .send(SendRequest {
            session_class: None,
            retention: None,
            conversation_key: None,
            session: bind.clone(),
            run_id: RunId(id.into()),
            send_id: None,
            title: None,
            request: Box::new(
                serde_json::from_value(json!({
                    "model":{"provider_module_id":"stub","model_id":"m"}, "input":input
                }))
                .unwrap(),
            ),
            skip_materialization: true,
            kind: broca_module::executor::RunKind::FreshOrResume,
            delivery: broca_protocol::Delivery::Queue,
            stop_sequences_named: false,
            plan: None,
            order: None,
        })
        .await
        .unwrap();
    match response {
        broca_module::serve::SendResponse::Active { run_id, .. } => assert_eq!(run_id, id),
        broca_module::serve::SendResponse::Finished { run_id, reason, .. } => {
            assert_eq!(run_id, id);
            assert_eq!(serde_json::to_value(reason).unwrap(), expected);
        }
        other => panic!("unexpected send response: {other:?}"),
    }
    broca_test_support::event_timeout(
        "installed executor completion",
        daemon.execution_done.notified(),
        || id.to_owned(),
    )
    .await
    .unwrap();
    let status = broca_test_support::wait_until("resumed run completed", || async {
        let status = daemon
            .registry
            .run_status(bind, RunId(id.into()))
            .await
            .map_err(|e| e.to_string())?;
        let status = serde_json::to_value(status).unwrap();
        if matches!(
            status["state"].as_str(),
            Some("completed" | "error" | "interrupted" | "cancelled")
        ) {
            Ok(status)
        } else {
            Err(status.to_string())
        }
    })
    .await;
    assert_eq!(status["state"], expected, "{status}");
}

async fn service_reads(
    daemon: &OverhaulDaemon,
    access: &WalAccessCounter,
    bind: &BindIdentity,
    id: &str,
    raw: bool,
) {
    for from in [None, Some(0)].into_iter().filter(|_| raw) {
        access
            .operation(format!("pages {id} {from:?}"))
            .run_future(async {
                let mut request = SessionReadRequest::new(bind.clone(), from, Some(2));
                request.media = cortexkit_role_llm_runner::read::MediaMode::Descriptors;
                let page = daemon.registry.read(request).await.unwrap();
                assert!(!page.messages.is_empty());
            })
            .await;
    }
    access
        .operation(format!("model page {id}"))
        .run_future(async {
            daemon
                .registry
                .read_model(SessionReadRequest::new(bind.clone(), None, Some(2)))
                .await
                .unwrap();
        })
        .await;
    if raw {
        access
            .operation(format!("head {id}"))
            .run_future(async {
                daemon.registry.head(bind).await.unwrap();
            })
            .await;
    }
    access
        .operation(format!("status {id}"))
        .run_future(async {
            daemon
                .registry
                .run_status(bind, RunId(id.into()))
                .await
                .unwrap();
            daemon.registry.status(bind).await.unwrap();
        })
        .await;
    access
        .operation(format!("result {id}"))
        .run_future(async {
            let result = daemon
                .registry
                .run_result(bind, RunId(id.into()))
                .await
                .unwrap();
            assert!(serde_json::to_string(&result).unwrap().contains("done"));
        })
        .await;
    access
        .operation(format!("attach {id}"))
        .run_future(async {
            let (tx, mut rx) = tokio::sync::mpsc::channel(256);
            daemon
                .registry
                .subscribe(bind, SubscribeFrom::Start, tx)
                .await
                .unwrap();
            assert!(
                broca_test_support::event_timeout("attach replay", rx.recv(), || {
                    "installed attach has not replayed".into()
                })
                .await
                .unwrap()
                .is_some()
            );
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_clean_multi_run_read_gate() {
    let dense = partial_turn().await;
    for (media, sparse) in [(false, false), (true, false), (false, true), (true, true)] {
        println!("lineage: media={media} window_sparse={sparse}");
        let label = format!("media-{media}-sparse-{sparse}");
        let root = root(&label);
        let bind = BindIdentity::new("/installed", "runner", &label);
        let mut template = dense.clone();
        if sparse {
            if let WalRecord::RunStarted {
                manifest, input, ..
            } = &mut template[0]
            {
                *manifest = Some(compaction_plan());
                let messages: Vec<broca_protocol::Message> = serde_json::from_value(json!([
                    {"role":"user","content":[{"type":"text","text":"old question"}]},
                    {"role":"assistant","origin":{"provider_module_id":"stub","model_id":"m"},"content":[{"type":"text","text":"old answer"}]},
                    {"role":"user","content":[{"type":"text","text":"new question"}]}
                ]))
                .unwrap();
                *input = messages.into();
            }
        }
        for number in 1..=3 {
            let access = WalAccessCounter::default();
            let id = format!("run-{number}");
            let daemon = OverhaulDaemon::boot(&root, access.clone()).await;
            let start = access.snapshot().len();
            if number == 2 {
                let zero = vec![
                    template[0].clone(),
                    WalRecord::RunFinished {
                        interrupt_reason: None,
                        reason: broca_protocol::RunFinishReason::Interrupted,
                        ts_ms: Some(10),
                        usage: Default::default(),
                        error: None,
                        retries_used: 0,
                    },
                ];
                let (seq, writer) = access
                    .operation("zero-step ordinary episode")
                    .run_future(write_turn(
                        &daemon,
                        &access,
                        &bind,
                        "zero-step",
                        &zero,
                        false,
                    ))
                    .await;
                published(&daemon, &bind, seq).await;
                writer.shutdown().await.unwrap();
                published(&daemon, &bind, seq + 1).await;
                access
                    .operation("zero-step status and result")
                    .run_future(async {
                        let status = daemon
                            .registry
                            .run_status(&bind, RunId("zero-step".into()))
                            .await
                            .unwrap();
                        assert_eq!(
                            serde_json::to_value(status).unwrap()["state"],
                            "interrupted"
                        );
                        daemon
                            .registry
                            .run_result(&bind, RunId("zero-step".into()))
                            .await
                            .unwrap();
                    })
                    .await;
            }
            let mut records = template.clone();
            if sparse && number == 1 {
                let replacement = std::sync::Arc::new(vec![broca_protocol::Message::new(
                    broca_protocol::Role::User,
                    vec![ContentBlock::Text {
                        text: "earlier history summary".into(),
                    }],
                )]);
                let setup = WalRecord::CompactionMessage {
                    provider: "compactor".into(),
                    kind: broca_wal::CompactionMessageKind::Setup,
                    compaction_id: "installed-window".into(),
                    version: 1,
                    range: broca_wal::CompactionRange { from: 1, to: 2 },
                    sha256: broca_wal::compaction_replacement_sha256(&replacement).unwrap(),
                    replacement,
                    request_id: None,
                    cursor: None,
                    call_when: None,
                    stability: None,
                    ceiling: None,
                    tag_checkpoint: None,
                    ride_wait_ms: None,
                    view_features: None,
                    view_due: None,
                };
                records.insert(1, setup);
            }
            let (seq, writer) = access
                .operation(format!("writer {id}"))
                .run_future(write_turn(
                    &daemon,
                    &access,
                    &bind,
                    &id,
                    &records,
                    media && number == 1,
                ))
                .await;
            published(&daemon, &bind, seq).await;
            writer.shutdown().await.unwrap();
            published(&daemon, &bind, seq + 1).await;
            no_legacy_index(&root);
            let full = full_replay_prompt(&daemon, &bind, &id).await;
            access
                .operation(format!("send admission {id}"))
                .run_future(resume(&daemon, &bind, &id))
                .await;
            {
                let prompts = daemon.resumed_prompts.lock().unwrap();
                assert_eq!(
                    prompts.len(),
                    1,
                    "the unfinished tool turn must ask for its next step"
                );
                assert_eq!(
                    serde_json::to_value(&prompts[0]).unwrap(),
                    serde_json::to_value(&full).unwrap(),
                    "seeded resume differs from full replay"
                );
            }
            // Compacted raw pages and head have a separate, explicitly budgeted
            // fallback fixture. Other sparse service and resume reads must not
            // scan from the start of the WAL.
            service_reads(&daemon, &access, &bind, &id, !sparse).await;
            assert_eq!(
                *daemon.resume_windows.lock().unwrap(),
                vec![sparse],
                "dense/window-sparse route was not exercised"
            );
            no_legacy_index(&root);
            // Join the actor and publisher before auditing the complete operation
            // ledger, including post-run admission and the writer's closing sync.
            daemon.shutdown().await;
            let operations = access.snapshot();
            let resumes: Vec<_> = operations[start..]
                .iter()
                .filter(|op| op.name.starts_with("resume seed"))
                .collect();
            assert_eq!(
                resumes.len(),
                1,
                "every run executes one stored-seed resume"
            );
            for name in ["admission prior", "model page", "model attach"] {
                assert!(
                    operations
                        .iter()
                        .any(|op| op.name == name && !op.reads.is_empty()),
                    "{name}: no measured WAL work"
                );
            }
            for (index, op) in operations.iter().enumerate().skip(1) {
                if fixture_startup(&op.name) {
                    report_fixture_writer(op, &operations[index + 1]);
                } else if !op.name.starts_with("writer-open fixture ") {
                    check_operation(op, &[], op.name.starts_with("resume seed"));
                }
            }
            no_legacy_index(&root);
            let feed = operations
                .iter()
                .find(|op| op.name == "boot and publication feed")
                .unwrap();
            if number == 1 {
                check_budget(
                    feed,
                    &[
                        ("publication index missing", 1),
                        ("publication continuation rebuild", 1),
                    ],
                    false,
                    1,
                );
            } else {
                check_operation(feed, &[], false);
            }
            assert!(
                operations
                    .iter()
                    .flat_map(|op| &op.writer_opens)
                    .any(|open| open.session_length > 0),
                "writer-open accounting was vacuous"
            );
        }
        no_legacy_index(&root);
        std::fs::remove_dir_all(root).unwrap();
    }
    println!("writer-open class was EXCLUDED from the service/resume zero-origin gate");
}

const ROOT_ENV: &str = "BROCA_INSTALLED_TEST_ROOT";
const CHILD: &str = "installed_publication_child";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_publication_child() {
    let Some(root) = std::env::var_os(ROOT_ENV) else {
        return;
    };
    let root = PathBuf::from(root);
    let bind = BindIdentity::new("/installed", "runner", "crash");
    let access = WalAccessCounter::default();
    let daemon = OverhaulDaemon::boot(&root, access.clone()).await;
    // A boot cut must kill before any handler or restart discovery is constructed.
    std::fs::write(root.join("boot-returned"), b"reconciled").unwrap();
    if std::env::var_os("BROCA_INSTALLED_RECOVER").is_some() {
        let boot = access.snapshot();
        check_budget(
            &boot[0],
            &[("publication continuation rebuild", 1)],
            false,
            1,
        );
        for operation in &boot[1..] {
            check_operation(operation, &[], false);
        }
        let active = daemon
            .stores
            .overhaul
            .as_ref()
            .unwrap()
            .recovery_discovery()
            .active_runs()
            .unwrap();
        assert!(
            active
                .iter()
                .any(|(id, key)| id == "crash-run" && key == &bind),
            "boot did not reconcile the active run before recovery discovery"
        );
        let full = full_replay_prompt(&daemon, &bind, "crash-run").await;
        let start = access.snapshot().len();
        access
            .operation("crash resume admission")
            .run_future(resume(&daemon, &bind, "crash-run"))
            .await;
        assert_eq!(
            serde_json::to_value(&daemon.resumed_prompts.lock().unwrap()[0]).unwrap(),
            serde_json::to_value(&full).unwrap()
        );
        service_reads(&daemon, &access, &bind, "crash-run", true).await;
        no_legacy_index(&root);
        daemon.shutdown().await;
        for op in &access.snapshot()[start..] {
            check_operation(op, &[], op.name.starts_with("resume seed"));
        }
        std::fs::write(
            root.join("recovered"),
            b"seeded resume and service reads equal full replay",
        )
        .unwrap();
    } else {
        let template = partial_turn().await;
        let (seq, writer) = access
            .operation("crash writer")
            .run_future(write_turn(
                &daemon,
                &access,
                &bind,
                "crash-run",
                &template,
                false,
            ))
            .await;
        published(&daemon, &bind, seq).await;
        writer.shutdown().await.unwrap();
        panic!("publication cut was not reached");
    }
}

fn derived_rows(root: &Path) -> Vec<(String, Option<u64>, u64)> {
    let connection = rusqlite::Connection::open_with_flags(
        root.join("broca.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let rows = connection
        .prepare("SELECT sid,indexed_seq,indexed_end FROM session ORDER BY sid")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    rows
}

#[test]
#[ignore = "installed real-process crash lane; integrating agent runs once per merged batch"]
fn installed_writer_sync_and_boot_reconciliation_crash_cuts() {
    let root = root("crashes");
    let template_len = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(partial_turn())
        .len() as u64;
    // The owned writer syncs its WriterOpened record before the fixture turn.
    let minimum = template_len + 1;
    let minimum_text = minimum.to_string();
    let marker = root.join("publication-cut");
    let cut_env = [
        (ROOT_ENV, root.as_os_str()),
        ("BROCA_TEST_PUBLICATION_CUT", marker.as_os_str()),
        (
            "BROCA_TEST_PUBLICATION_CUT_MIN_SEQ",
            std::ffi::OsStr::new(&minimum_text),
        ),
    ];
    assert_eq!(
        broca_crash_harness::kill_child_test_at_marker(
            "installed writer descriptor synced before broca.db commit",
            CHILD,
            &cut_env,
            &marker
        ),
        broca_crash_harness::KillMethod::Sigkill
    );
    no_legacy_index(&root);
    let before = derived_rows(&root);
    assert!(
        before.iter().all(|(_, seq, _)| seq.unwrap_or(0) < minimum),
        "the synced turn publication must not have committed"
    );
    std::fs::remove_file(&marker).unwrap();
    std::fs::remove_file(root.join("boot-returned")).unwrap();
    assert_eq!(
        broca_crash_harness::kill_child_test_at_marker(
            "installed boot reconciliation before its transaction commits",
            CHILD,
            &cut_env,
            &marker
        ),
        broca_crash_harness::KillMethod::Sigkill
    );
    assert!(
        !root.join("boot-returned").exists(),
        "boot exposed readers before reconciliation finished"
    );
    assert_eq!(
        derived_rows(&root),
        before,
        "killed boot publication was not atomic"
    );
    no_legacy_index(&root);
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", CHILD, "--nocapture"])
        .env(ROOT_ENV, &root)
        .env("BROCA_INSTALLED_RECOVER", "1")
        .env_remove("BROCA_TEST_PUBLICATION_CUT")
        .env_remove("BROCA_TEST_PUBLICATION_CUT_MIN_SEQ")
        .status()
        .unwrap();
    assert!(status.success(), "fresh-process recovery failed: {status}");
    assert!(root.join("recovered").exists());
    assert!(!derived_rows(&root).is_empty());
    no_legacy_index(&root);
    std::fs::remove_dir_all(root).unwrap();
}

async fn full_replay_prompt(
    daemon: &OverhaulDaemon,
    bind: &BindIdentity,
    id: &str,
) -> Vec<broca_protocol::Message> {
    let root = root("full-replay-oracle");
    let wal =
        broca_core::FileWalStore::with_config(root.join("wal"), broca_wal::WalConfig::OVERHAUL);
    std::fs::create_dir_all(root.join("wal")).unwrap();
    std::fs::copy(daemon.wal.wal_path(bind), wal.wal_path(bind)).unwrap();
    let mut req = request();
    req.session = bind.clone();
    req.run_id = RunId(id.into());
    let provider =
        broca_core::StubProvider::new((broca_crash_harness::process_kill_probe().scripts)());
    let (tx, mut rx) = tokio::sync::mpsc::channel(64);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let counter = WalAccessCounter::default();
    let summary = counter
        .operation("explicit full-replay oracle, not a service request")
        .run_future(broca_core::run_loop_with_control(
            req,
            &provider,
            &broca_core::StubToolPlane::new(),
            &broca_core::FileLeaseStore::new(root.join("leases")),
            &broca_core::AllowAll,
            &wal,
            None,
            broca_core::RunControl::default(),
            &tx,
        ))
        .await
        .unwrap();
    assert_eq!(summary.reason, broca_protocol::RunFinishReason::Completed);
    drop(tx);
    collector.await.unwrap();
    assert!(
        counter.snapshot()[0]
            .reads
            .iter()
            .any(|read| read.class == ReadClass::Origin),
        "the independent oracle must exercise full replay"
    );
    let mut calls = provider.calls();
    assert_eq!(calls.len(), 1);
    std::fs::remove_dir_all(root).unwrap();
    calls.remove(0).prompt
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_handler_preserves_dense_seed_after_zero_step_interruption() {
    let root = root("handler-zero-step-regression");
    let bind = BindIdentity::new("/installed", "runner", "handler-zero-step");
    let access = WalAccessCounter::default();
    let daemon = OverhaulDaemon::boot(&root, access.clone()).await;
    let input = |text: &str| json!([{"role":"user","content":[{"type":"text","text":text}]}]);
    submit(
        &daemon,
        &bind,
        "first",
        input("first question"),
        "completed",
    )
    .await;
    submit(
        &daemon,
        &bind,
        "zero-step",
        input("interrupted before model"),
        "interrupted",
    )
    .await;
    let before = access.snapshot().len();
    submit(
        &daemon,
        &bind,
        "second",
        input("second question"),
        "completed",
    )
    .await;
    let operations = access.snapshot();
    let resumed: Vec<_> = operations[before..]
        .iter()
        .filter(|op| op.name == "resume seed second")
        .collect();
    assert_eq!(resumed.len(), 1);
    check_operation(resumed[0], &[], true);
    daemon.shutdown().await;
    no_legacy_index(&root);
    std::fs::remove_dir_all(root).unwrap();
}

fn compaction_plan() -> Box<broca_wal::PlanManifest> {
    Box::new(broca_wal::PlanManifest {
        rich_ui: None,
        plan: broca_wal::OpaqueJson::from_json_text(
            json!({
                "compaction_item":{"provider":"compactor","params":{}}, "composition":{}
            })
            .to_string(),
        )
        .unwrap(),
        plan_identity: "installed-plan".into(),
        composition_digest: "installed-composition".into(),
        tools: Default::default(),
        system_text: String::new(),
        system_digest: String::new(),
        join_version: 1,
        role_versions: Default::default(),
        fetches: vec![],
        declarations: vec![],
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_compacted_raw_pages_and_head_have_named_fallback_budgets() {
    let template = partial_turn().await;
    for messages in [1_000, 150_000] {
        let root = root(&format!("compacted-raw-{messages}"));
        let bind = BindIdentity::new("/installed", "runner", format!("raw-{messages}"));
        let access = WalAccessCounter::default();
        let daemon = OverhaulDaemon::boot(&root, access.clone()).await;
        let mut records = template.clone();
        if let WalRecord::RunStarted {
            manifest, input, ..
        } = &mut records[0]
        {
            *manifest = Some(compaction_plan());
            *input = vec![
                broca_protocol::Message::new(
                    broca_protocol::Role::User,
                    vec![ContentBlock::Text {
                        text: "prefix".into()
                    }]
                );
                messages
            ]
            .into();
        }
        let replacement = std::sync::Arc::new(vec![broca_protocol::Message::new(
            broca_protocol::Role::User,
            vec![ContentBlock::Text {
                text: "prefix summary".into(),
            }],
        )]);
        records.insert(
            1,
            WalRecord::CompactionMessage {
                provider: "compactor".into(),
                kind: broca_wal::CompactionMessageKind::Setup,
                compaction_id: "raw-window".into(),
                version: 1,
                range: broca_wal::CompactionRange {
                    from: 1,
                    to: messages as u64 - 1,
                },
                sha256: broca_wal::compaction_replacement_sha256(&replacement).unwrap(),
                replacement,
                request_id: None,
                cursor: None,
                call_when: None,
                stability: None,
                ceiling: None,
                tag_checkpoint: None,
                ride_wait_ms: None,
                view_features: None,
                view_due: None,
            },
        );
        let (seq, writer) = access
            .operation("writer raw cost fixture")
            .run_future(write_turn(
                &daemon, &access, &bind, "raw-run", &records, false,
            ))
            .await;
        published(&daemon, &bind, seq).await;
        writer.shutdown().await.unwrap();
        published(&daemon, &bind, seq + 1).await;
        for kind in ["raw page", "cold head"] {
            let counter = WalAccessCounter::default();
            let started = std::time::Instant::now();
            counter
                .operation(format!("compacted {kind} exception"))
                .run_future(async {
                    if kind == "raw page" {
                        let page = daemon
                            .registry
                            .read(SessionReadRequest::new(bind.clone(), None, Some(2)))
                            .await
                            .unwrap();
                        assert_eq!(page.messages.len(), 2);
                    } else {
                        daemon.registry.head(&bind).await.unwrap();
                    }
                })
                .await;
            let elapsed = started.elapsed();
            let measured = counter.snapshot().remove(0);
            let origins = if kind == "raw page" { 1 } else { 3 };
            check_budget(
                &measured,
                &[("raw page requires compaction source hydration", 1)],
                false,
                origins,
            );
            println!("compacted {kind} exception: session_messages={messages} wall_ms={:.3} bytes_read={} origin_reads={origins} named_fallbacks=1",
                elapsed.as_secs_f64() * 1_000.0, measured.reads.iter().map(|read| read.length).sum::<u64>());
        }
        resume(&daemon, &bind, "raw-run").await;
        let counter = WalAccessCounter::default();
        let started = std::time::Instant::now();
        counter
            .operation("compacted warm head exception")
            .run_future(daemon.registry.head(&bind))
            .await
            .unwrap();
        let elapsed = started.elapsed();
        let measured = counter.snapshot().remove(0);
        check_budget(
            &measured,
            &[("raw page requires compaction source hydration", 1)],
            false,
            1,
        );
        println!("compacted warm head exception: session_messages={messages} wall_ms={:.3} bytes_read={} origin_reads=1 named_fallbacks=1",
            elapsed.as_secs_f64() * 1_000.0, measured.reads.iter().map(|read| read.length).sum::<u64>());
        no_legacy_index(&root);
        daemon.shutdown().await;
        std::fs::remove_dir_all(root).unwrap();
    }
}

fn assert_locator_roundtrip(daemon: &OverhaulDaemon, bind: &BindIdentity) {
    let encoded = daemon.stores.overhaul.as_ref().unwrap().db.read_snapshot(|tx| {
        tx.query_row("SELECT locator_seed FROM session WHERE project_root=?1 AND harness=?2 AND session_key=?3",
            rusqlite::params![bind.project_root.to_string_lossy(), bind.harness, bind.session], |row| row.get::<_, String>(0))
            .map_err(Into::into)
    }).unwrap();
    assert!(
        broca_core::projection_plan::OffsetSeed::decode(&encoded, None).is_some(),
        "publisher wrote a locator its decoder refuses: {encoded}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_header_only_publication_roundtrips_before_the_first_run() {
    let root = root("header-only-publication");
    let bind = BindIdentity::new("/installed", "runner", "header-only");
    let access = WalAccessCounter::default();
    let daemon = OverhaulDaemon::boot(&root, access.clone()).await;
    let header = daemon.wal.open(&bind).unwrap();
    header.sync().unwrap();
    published(&daemon, &bind, 0).await;
    assert_locator_roundtrip(&daemon, &bind);
    drop(header);
    let template = partial_turn().await;
    let (seq, writer) = access
        .operation("writer first turn")
        .run_future(write_turn(
            &daemon, &access, &bind, "first", &template, false,
        ))
        .await;
    published(&daemon, &bind, seq).await;
    assert_locator_roundtrip(&daemon, &bind);
    writer.shutdown().await.unwrap();
    published(&daemon, &bind, seq + 1).await;
    daemon.shutdown().await;
    let operations = access.snapshot();
    let feed = operations
        .iter()
        .find(|op| op.name == "boot and publication feed")
        .unwrap();
    check_budget(
        feed,
        &[
            ("publication index missing", 1),
            ("publication continuation rebuild", 1),
        ],
        false,
        1,
    );
    no_legacy_index(&root);
    std::fs::remove_dir_all(root).unwrap();
}
