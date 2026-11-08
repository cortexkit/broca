//! Exports are checked against the HTTP server's own request capture, not WAL digests alone.
use broca_core::{
    run_loop, AllowAll, FileLeaseStore, FileWalStore, RunRequest, StubToolPlane, WalStore,
};
use broca_module_serve::cache_dump::{export, export_records, wire_usage, Summary};
use broca_protocol::{
    request::{ModelHandle, StopCondition, Tool, ToolChoice},
    BindIdentity, ContentBlock, GenerationConfig, Message, Role, RunId, Usage,
};
use broca_provider::{
    families::{
        anthropic_frozen_config, gemini_frozen_config, google_codeassist_frozen_config,
        openai_responses_frozen_config, AnthropicMessages, Gemini, GoogleCodeAssist,
        OpenAiResponses,
    },
    spec::WireFamilyId,
    wire::{AuthHandle, FrozenRenderConfig, WireFamily},
    LiveProvider,
};
use broca_wal::WalRecord;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};

fn scratch(tag: &str) -> PathBuf {
    let root =
        broca_test_support::scratch_root().join(format!("cache-dump-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("proj")).unwrap();
    root
}
fn index(root: &Path) -> rusqlite::Connection {
    drop(broca_store::SqliteStore::open(root.join("run-index.db")).unwrap());
    let connection = rusqlite::Connection::open(root.join("run-index.db")).unwrap();
    complete_inventory(&connection);
    connection
}
fn complete_inventory(connection: &rusqlite::Connection) {
    // These fixtures supply every synthetic bind directly, rather than relying on
    // a background census to discover the WALs before a name lookup can run.
    connection
        .execute_batch(
            "INSERT OR REPLACE INTO agent_data_projection VALUES(1,1);
        INSERT OR REPLACE INTO agent_data_census_done VALUES(1);
        DELETE FROM agent_data_pending_session;
        UPDATE agent_data_census SET covered=1;",
        )
        .unwrap();
}
fn index_bind(
    connection: &rusqlite::Connection,
    bind: &BindIdentity,
    state: &str,
    timestamp: Option<i64>,
) {
    connection
        .execute(
            "INSERT OR REPLACE INTO run_index(run_id,session,state,state_changed_ms) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![
                broca_core::wal_writer::session_addr(bind),
                serde_json::to_string(bind).unwrap(),
                state,
                timestamp
            ],
        )
        .unwrap();
    connection
        .execute(
            "INSERT OR REPLACE INTO agent_data_session_ref VALUES(?1,?2,?3,?4,?5)",
            rusqlite::params![
                broca_core::agent_session_ref(bind),
                broca_core::session_addr(bind),
                bind.project_root.to_str().unwrap(),
                bind.harness,
                bind.session
            ],
        )
        .unwrap();
    complete_inventory(connection);
}

fn retained_wal(root: &Path, key: &BindIdentity, activity_ms: u64, retention_s: u64) -> String {
    let store = FileWalStore::new(root.join("wal"));
    let mut wal = store.open_with_retention(key, Some(retention_s)).unwrap();
    let mut start: WalRecord = serde_json::from_str(include_str!(
        "../../broca-wal/tests/golden/pre_step_transforms/run_started.json"
    ))
    .unwrap();
    if let WalRecord::RunStarted {
        retention,
        ts_ms,
        session,
        ..
    } = &mut start
    {
        *retention = Some(retention_s);
        *ts_ms = Some(activity_ms);
        *session = key.clone();
    }
    wal.append(1, &start, true).unwrap();
    wal.append(
        1,
        &WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: Some(activity_ms),
            reason: broca_protocol::RunFinishReason::Completed,
            usage: Default::default(),
            error: None,
            retries_used: 0,
        },
        true,
    )
    .unwrap();
    wal.lineage_id().unwrap().to_string()
}

#[tokio::test]
async fn expired_retention_refuses_all_selected_exports_without_touching_out() {
    let (root, _, _) = fixture(
        "live-before-expired",
        AnthropicMessages,
        anthropic_frozen_config("m"),
        vec![anthropic(false)],
        false,
    )
    .await;
    let original = BindIdentity::new(root.join("proj"), "test", "live-before-expired");
    let mut keys = [
        BindIdentity::new(root.join("proj"), "test", "selection-a"),
        BindIdentity::new(root.join("proj"), "test", "selection-b"),
    ];
    keys.sort_by_key(broca_core::wal_writer::session_addr);
    let [live, expired] = keys;
    let store = FileWalStore::new(root.join("wal"));
    std::fs::copy(store.wal_path(&original), store.wal_path(&live)).unwrap();
    index_bind(&index(&root), &live, "completed", Some(200));
    retained_wal(&root, &expired, 200, 1);
    index_bind(&index(&root), &expired, "completed", Some(200));
    let sessions = [live.session.clone(), expired.session.clone()];
    let out = root.join("expired-out");
    let error = export(&root, &sessions, None, &out).err().unwrap();
    assert_eq!(error, "expired {expired_at_ms: 1200}");
    assert!(!out.exists(), "a refused selection must not create --out");
    std::fs::create_dir(&out).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&out, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    std::fs::write(out.join("sentinel"), "untouched").unwrap();
    assert!(export(&root, &sessions, None, &out).is_err());
    assert_eq!(std::fs::read_dir(&out).unwrap().count(), 1);
    assert_eq!(std::fs::read(out.join("sentinel")).unwrap(), b"untouched");
}

#[test]
fn cache_dump_honors_matching_tombstone_before_and_after_deletion() {
    let root = scratch("tombstoned-dump");
    let key = BindIdentity::new(root.join("proj"), "test", "tombstoned");
    let lineage = retained_wal(&root, &key, u64::MAX - 1000, 1);
    index_bind(&index(&root), &key, "completed", Some(200));
    let store = FileWalStore::new(root.join("wal"));
    let path = store.tombstone_path(&key, &lineage);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        &path,
        serde_json::to_vec(&json!({
            "lineage_id": lineage, "expired_at_ms": 1200, "seq": 1
        }))
        .unwrap(),
    )
    .unwrap();
    let out = root.join("out");
    for deleted in [false, true] {
        if deleted {
            std::fs::remove_file(store.ephemeral_wal_path(&key)).unwrap();
        }
        assert_eq!(
            export(&root, std::slice::from_ref(&key.session), None, &out)
                .err()
                .unwrap(),
            "expired {expired_at_ms: 1200}"
        );
        assert!(!out.exists());
    }
    let mut fresh = store.open(&key).unwrap();
    let mut start: WalRecord = serde_json::from_str(include_str!(
        "../../broca-wal/tests/golden/pre_step_transforms/run_started.json"
    ))
    .unwrap();
    if let WalRecord::RunStarted { session, .. } = &mut start {
        *session = key.clone();
    }
    fresh.append(1, &start, true).unwrap();
    drop(fresh);
    assert!(
        export(&root, &[key.session], None, &out).is_ok(),
        "a fresh forever lineage supersedes the old tombstone"
    );
}

#[cfg(feature = "test-clock")]
#[test]
fn cache_dump_checks_the_feature_clock_before_creating_output() {
    use broca_module_serve::cache_dump::{export_with_bind, DirectBind};
    let root = scratch("clock-dump");
    let key = BindIdentity::new(root.join("proj"), "test", "clock-controlled");
    retained_wal(&root, &key, broca_core::Clock::default().now_ms(), 3600);
    let direct = DirectBind {
        project_root: root.join("proj"),
        harness: "test".into(),
    };
    assert!(export_with_bind(
        &root,
        std::slice::from_ref(&key.session),
        None,
        &root.join("live-out"),
        Some(&direct)
    )
    .is_ok());
    std::fs::write(root.join("test-clock"), r#"{"offset_ms": 3601000}"#).unwrap();
    let out = root.join("expired-out");
    let error = export_with_bind(&root, &[key.session], None, &out, Some(&direct))
        .err()
        .unwrap();
    assert!(error.starts_with("expired {expired_at_ms:"));
    assert!(!out.exists());
    assert!(!root.join("run-index.db").exists());
}
fn sse(events: Vec<Value>) -> Vec<u8> {
    events
        .into_iter()
        .map(|v| format!("event: {}\ndata: {v}\n\n", v["type"].as_str().unwrap()))
        .collect::<String>()
        .into_bytes()
}

fn mode3_manifest() -> Arc<broca_wal::PlanManifest> {
    Arc::new(broca_wal::PlanManifest {
        rich_ui: None,
        plan: broca_wal::OpaqueJson::from_json_text(
            json!({
                "compaction_item":{"provider":"compactor","preset":"head","params":{}},
                "composition":{},"render_tags":true
            })
            .to_string(),
        )
        .unwrap(),
        plan_identity: "mode3".into(),
        composition_digest: "digest".into(),
        tools: Arc::new(vec![]),
        system_text: "Frozen plan system".into(),
        system_digest: "system".into(),
        join_version: 1,
        role_versions: Default::default(),
        fetches: vec![],
        declarations: vec![],
    })
}

struct DumpCompactionPlane(Arc<broca_core::wal_writer::WalHandle>, bool);

#[async_trait::async_trait]
impl broca_core::compaction::CompactionPlane for DumpCompactionPlane {
    async fn setup(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        _: broca_core::compaction_provider::setup::SetupRequest,
    ) -> Result<
        broca_core::compaction_provider::setup::SetupAnswer,
        broca_core::compaction::CompactionUnavailable,
    > {
        panic!("mode-3 must not call the retired compaction protocol")
    }

    async fn call_v2(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        method: &str,
        _: Value,
        _: u64,
    ) -> Result<Box<serde_json::value::RawValue>, broca_core::compaction_v2::runtime::CallError>
    {
        use broca_core::compaction_v2::protocol::view::{Edit, EditOp, View};
        let answer = match method {
            "compaction.setup" => json!({"answer":"ready"}),
            "compaction.notice" => json!({"answer":"received"}),
            "compaction.ride" => {
                let lineage = self.0.lineage_id().unwrap().to_string();
                let through = broca_core::message_mid(true, Some(&lineage), 0).unwrap();
                let edits = vec![Edit::new(1, EditOp::Cut { through })];
                let view = if self.1 {
                    View::seal_with_policy(
                        1,
                        edits,
                        Some(broca_core::message_mid(true, Some(&lineage), 1).unwrap()),
                        Some("strip".into()),
                    )
                } else {
                    View::seal(1, edits)
                };
                json!({"answer":"view","view":view})
            }
            _ => panic!("unexpected compaction method {method}"),
        };
        Ok(serde_json::value::to_raw_value(&answer).unwrap())
    }
}
fn anthropic(tool: bool) -> Vec<u8> {
    let mut events = vec![
        json!({"type":"message_start", "message":{"id":"msg", "type":"message", "role":"assistant", "model":"m", "content":[], "usage":{"input_tokens":11,"cache_read_input_tokens":23,"cache_creation_input_tokens":7,"output_tokens":0}}}),
    ];
    if tool {
        events.push(json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call","name":"read","input":{}}}));
        events.push(json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"path\":\"a\"}"}}));
    } else {
        events.push(json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}));
        events.push(json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"done"}}));
    }
    events.extend([json!({"type":"content_block_stop","index":0}), json!({"type":"message_delta","delta":{"stop_reason":if tool {"tool_use"} else {"end_turn"}},"usage":{"output_tokens":5}}),json!({"type":"message_stop"})]);
    sse(events)
}
fn responses() -> Vec<u8> {
    sse(vec![
        json!({"type":"response.created","response":{"id":"r","status":"in_progress","output":[]}}),
        json!({"type":"response.output_text.delta","item_id":"m","output_index":0,"content_index":0,"delta":"done"}),
        json!({"type":"response.completed","response":{"id":"r","status":"completed","output":[{"id":"m","type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}],"usage":{"input_tokens":41,"input_tokens_details":{"cached_tokens":23,"cache_write_tokens":7},"output_tokens":5}}}),
    ])
}
/// Gemini streams bare `data:` lines with no SSE event names. The prompt count includes the
/// 23 cached tokens and the 17 thoughts are reported beside, not inside, the 5 candidates.
fn gemini_chunk() -> Value {
    json!({"candidates":[{"content":{"parts":[{"text":"done"}],"role":"model"},"finishReason":"STOP","index":0}],"usageMetadata":{"promptTokenCount":34,"cachedContentTokenCount":23,"candidatesTokenCount":5,"thoughtsTokenCount":17}})
}
fn data_sse(chunk: Value) -> Vec<u8> {
    format!("data: {chunk}\n\n").into_bytes()
}
const GEMINI_WIRE_USAGE: &str = r#"{"usage":{"promptTokenCount":34,"cachedContentTokenCount":23,"candidatesTokenCount":5,"thoughtsTokenCount":17},"broca_usage":{"input_tokens":11,"cached_input_tokens":23,"output_tokens":22,"reasoning_tokens":17}}"#;
async fn read_body(socket: &mut TcpStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut chunk = [0; 8192];
    let end = loop {
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0, "request incomplete");
        bytes.extend_from_slice(&chunk[..n]);
        if let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let headers = String::from_utf8_lossy(&bytes[..end]).to_ascii_lowercase();
    let len: usize = headers
        .lines()
        .find_map(|s| s.strip_prefix("content-length:"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    while bytes.len() < end + len {
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0, "request incomplete");
        bytes.extend_from_slice(&chunk[..n]);
    }
    bytes[end..end + len].to_vec()
}
async fn fixture<F: WireFamily + 'static>(
    tag: &str,
    family: F,
    frozen: FrozenRenderConfig,
    replies: Vec<Vec<u8>>,
    tool: bool,
) -> (PathBuf, Vec<WalRecord>, Vec<Vec<u8>>) {
    fixture_with_mode(tag, family, frozen, replies, tool, FixtureMode::Legacy).await
}

#[derive(Clone, Copy)]
enum FixtureMode {
    Legacy,
    Compaction,
    StripSigned,
    GlobalSystem,
}

fn fixture_request(session: BindIdentity, tools: Vec<Tool>) -> RunRequest {
    RunRequest {
        account_route: None,
        route_decision_id: None,
        input_written_at_ms: None,
        directives: None,
        session_class: None,
        retention: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        run_id: RunId("fixture/run".into()),
        session,
        model: ModelHandle::new("test", "m"),
        input: vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "fixture".into(),
            }],
        )],
        policy_allowed: tools.iter().map(|t| t.name.clone()).collect(),
        tools,
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![StopCondition::StepCount { n: 5 }],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
        structured_output: None,
    }
}

async fn fixture_with_mode<F: WireFamily + 'static>(
    tag: &str,
    family: F,
    mut frozen: FrozenRenderConfig,
    replies: Vec<Vec<u8>>,
    tool: bool,
    mode: FixtureMode,
) -> (PathBuf, Vec<WalRecord>, Vec<Vec<u8>>) {
    let root = scratch(tag);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    frozen.base_url = Some(format!("http://{}", listener.local_addr().unwrap()));
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let capture = Arc::clone(&bodies);
    let server = tokio::spawn(async move {
        for reply in replies {
            let (mut socket, _) = listener.accept().await.unwrap();
            let body = read_body(&mut socket).await;
            capture.lock().unwrap().push(body);
            let header = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n", reply.len());
            socket.write_all(header.as_bytes()).await.unwrap();
            socket.write_all(&reply).await.unwrap();
        }
    });
    let session = BindIdentity::new(root.join("proj"), "test", tag);
    let tools = if tool {
        vec![Tool {
            name: "read".into(),
            description: "Read".into(),
            input_schema: json!({"type":"object","properties":{"path":{"type":"string"}}}),
            module: None,
            execution_mode: Default::default(),
            reply: None,
        }]
    } else {
        vec![]
    };
    let mut request = fixture_request(session.clone(), tools);
    let wal = FileWalStore::new(root.join("wal"));
    let (tx, mut rx) = mpsc::channel(4096);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let provider = LiveProvider::new(family, frozen, AuthHandle::Bearer("fixture".into()));
    let run = async {
        if !matches!(mode, FixtureMode::Legacy) {
            request.context_limit = Some(4000);
            if matches!(mode, FixtureMode::Compaction | FixtureMode::StripSigned) {
                request.input.insert(
                    0,
                    Message::new(
                        Role::User,
                        vec![ContentBlock::Text {
                            text: "old context ".repeat(10_000),
                        }],
                    ),
                );
                if matches!(mode, FixtureMode::StripSigned) {
                    request.model = ModelHandle::new("anthropic", "claude-opus-5-5");
                    request.input.push(Message::new(
                        Role::Assistant,
                        vec![
                            ContentBlock::Reasoning {
                                text: "private".into(),
                                signature: Some("signature".into()),
                            },
                            ContentBlock::ToolCall(broca_protocol::ToolCall {
                                tool_call_id: broca_protocol::ToolCallId("prior-call".into()),
                                tool_name: "read".into(),
                                input: json!({"path":"a"}),
                                thought_signature: None,
                            }),
                        ],
                    ));
                    request.input.push(Message::new(
                        Role::Tool,
                        vec![ContentBlock::ToolResult(broca_protocol::ToolResult {
                            tool_call_id: broca_protocol::ToolCallId("prior-call".into()),
                            output: broca_protocol::ToolOutput::Text {
                                text: "prior result".into(),
                            },
                            is_error: false,
                        })],
                    ));
                }
            } else {
                request.input.insert(
                    0,
                    Message::new(
                        Role::System,
                        vec![ContentBlock::Text {
                            text: "Frozen unplanned system".into(),
                        }],
                    ),
                );
            }
            let writer = Arc::new(
                broca_core::wal_writer::WalHandle::spawn(wal.open(&session).unwrap(), 1).unwrap(),
            );
            let control = broca_core::RunControl {
                new_log_compaction_plan_version: Some(broca_core::compaction_v2::PLAN_VERSION),
                admitted_context_limit: request.context_limit,
                manifest: matches!(mode, FixtureMode::Compaction | FixtureMode::StripSigned)
                    .then(mode3_manifest),
                compaction_plane: if matches!(
                    mode,
                    FixtureMode::Compaction | FixtureMode::StripSigned
                ) {
                    Some(Arc::new(DumpCompactionPlane(
                        writer.clone(),
                        matches!(mode, FixtureMode::StripSigned),
                    )))
                } else {
                    None
                },
                ..Default::default()
            };
            let result = broca_core::run_loop_on_writer(
                request,
                &writer,
                broca_core::StartMode::FreshOrResume,
                &provider,
                &StubToolPlane::new(),
                &AllowAll,
                &wal,
                None,
                control,
                &tx,
            )
            .await;
            Arc::try_unwrap(writer)
                .ok()
                .unwrap()
                .shutdown()
                .await
                .unwrap();
            result
        } else {
            run_loop(
                request,
                &provider,
                &StubToolPlane::new(),
                &FileLeaseStore::new(root.join("leases")),
                &AllowAll,
                &wal,
                &tx,
            )
            .await
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(20), run)
        .await
        .unwrap()
        .unwrap();
    drop(tx);
    drain.await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    let records = wal
        .read_records(&session)
        .unwrap()
        .into_iter()
        .map(|f| f.record)
        .collect::<Vec<_>>();
    let terminal_ms = records
        .iter()
        .filter_map(|record| match record {
            WalRecord::ModelAttemptFinished { ts_ms, .. } => Some(*ts_ms),
            _ => None,
        })
        .max()
        .unwrap() as i64
        + 1;
    index_bind(&index(&root), &session, "completed", Some(terminal_ms));
    let received = bodies.lock().unwrap().clone();
    (root, records, received)
}
fn files(out: &Path, suffix: &str) -> Vec<PathBuf> {
    let mut files: Vec<_> = std::fs::read_dir(out)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.file_name().unwrap().to_str().unwrap().ends_with(suffix))
        .collect();
    files.sort();
    files
}
fn check_bodies(out: &Path, captured: &[Vec<u8>]) {
    let bodies = files(out, ".body.json");
    assert_eq!(bodies.len(), captured.len());
    for (file, received) in bodies.iter().zip(captured) {
        assert!(
            Sha256::digest(std::fs::read(file).unwrap()) == Sha256::digest(received),
            "export must equal independently captured request bytes"
        );
    }
}

const TOOL_PNG: &[u8] =
    include_bytes!("../../broca-provider/examples/media-live/fixtures/tool-image.png");

struct NativeImageTool;

#[async_trait::async_trait]
impl broca_core::tool_plane::ToolPlane for NativeImageTool {
    async fn batch_started(
        &self,
        _: &broca_core::tool_plane::BatchCtx,
        _: &[broca_protocol::ToolCall],
        _: &broca_core::ToolManifest,
    ) -> Result<(), broca_core::ToolPlaneError> {
        Ok(())
    }
    async fn batch_finished(
        &self,
        _: &broca_core::tool_plane::BatchCtx,
    ) -> Result<(), broca_core::ToolPlaneError> {
        Ok(())
    }
    async fn dispatch(
        &self,
        ctx: &broca_core::tool_plane::BatchCtx,
        call: &broca_protocol::ToolCall,
    ) -> broca_core::tool_plane::DispatchResult {
        let media = ctx
            .media_capture
            .as_ref()
            .expect("live tool capture installed")
            .capture(
                &ctx.batch_id,
                &call.tool_call_id,
                TOOL_PNG.to_vec(),
                "image/png".into(),
            )
            .await
            .unwrap();
        broca_core::tool_plane::DispatchResult::Completed(broca_protocol::ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: broca_protocol::ToolOutput::Parts {
                parts: vec![broca_protocol::ToolOutputPart::Media { media }],
            },
            is_error: false,
        })
    }
}

fn native_provider(
    wal: Arc<FileWalStore>,
    session: &BindIdentity,
    frozen: FrozenRenderConfig,
    captured: Arc<Mutex<Vec<Vec<u8>>>>,
) -> LiveProvider<broca_provider::families::OpenAiResponsesChatgpt> {
    use broca_core::media_request::MediaCheckpoint;
    use broca_core::provider::Provider;
    let mut checkpoint = MediaCheckpoint::empty();
    for frame in wal.read_records(session).unwrap() {
        checkpoint.fold(frame.seq, &frame.record).unwrap();
    }
    let checkpoint = MediaCheckpoint::decode(&checkpoint.encode()).unwrap();
    let store = wal.uploads().unwrap().session_media_store(session).unwrap();
    let key = session.clone();
    let media = broca_provider::media_request::LiveMedia::from_checkpoint(
        store,
        checkpoint,
        None,
        frozen.media.clone(),
        Box::new(move |after| {
            wal.read_records_range(&key, after, None, None)
                .map_err(|error| error.to_string())
                .and_then(|range| match range.into_records() {
                    broca_wal::WalRangeRecords::Complete(records) => Ok(records),
                    _ => Err("native image fixture requires complete WAL tail".into()),
                })
        }),
    )
    .unwrap();
    let provider = LiveProvider::new(
        broca_provider::families::OpenAiResponsesChatgpt,
        frozen,
        AuthHandle::Bearer("fixture".into()),
    );
    provider.set_media_request_renderer(Arc::new(move |config, opts, tags, commit, provenance| {
        let body = media.render_with_provenance(config, opts, tags, commit, provenance)?;
        // This is the transport's render hook, before endpoint construction and WebSocket framing.
        // Admission estimates are not sent requests and must not enter the byte oracle.
        if commit {
            captured.lock().unwrap().push(body.clone());
        }
        Ok(body)
    }));
    provider
}

async fn chatgpt_media_fixture(
    tag: &str,
    mode3: bool,
) -> (PathBuf, BindIdentity, Vec<WalRecord>, Vec<Vec<u8>>) {
    use futures::{SinkExt, StreamExt};
    let root = scratch(tag);
    let session = BindIdentity::new(root.join("proj"), "test", tag);
    let wal = Arc::new(FileWalStore::new(root.join("wal")));
    drop(wal.open(&session).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut frozen =
        broca_provider::families::openai_responses_chatgpt_frozen_config("gpt-5.3-codex");
    frozen.base_url = Some(format!("http://{}", listener.local_addr().unwrap()));
    frozen.media = Some(broca_provider::media_profile::select_media_profile(
        WireFamilyId::OpenAiResponsesChatgpt,
        None,
        "gpt-5.3-codex",
        broca_provider::spec::InputModalities::from_value(&json!(["text", "image"])),
        Some(128000),
    ));
    let server = tokio::spawn(async move {
        let mut call = 0;
        for count in [2, 1, 1] {
            let (socket, _) = listener.accept().await.unwrap();
            let mut config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default();
            config.extensions.permessage_deflate = Some(tokio_tungstenite::tungstenite::extensions::compression::deflate::DeflateConfig::default());
            let mut socket = tokio_tungstenite::accept_async_with_config(socket, Some(config))
                .await
                .unwrap();
            for _ in 0..count {
                let request = socket.next().await.unwrap().unwrap();
                let request: Value = serde_json::from_str(request.to_text().unwrap()).unwrap();
                assert_eq!(request["type"], "response.create");
                let mut events = if call == 0 {
                    vec![
                        json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc","call_id":"image-call","name":"read","arguments":""}}),
                        json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":"{}"}),
                    ]
                } else {
                    vec![json!({"type":"response.output_text.delta","delta":"done"})]
                };
                events.push(json!({"type":"response.completed","response":{"id":format!("r{call}"),"status":"completed","usage":{"input_tokens":12,"output_tokens":1}}}));
                for event in events {
                    socket
                        .send(tokio_tungstenite::tungstenite::Message::Text(
                            event.to_string().into(),
                        ))
                        .await
                        .unwrap();
                }
                call += 1;
            }
        }
        assert_eq!(call, 4);
    });
    let tools = vec![Tool {
        name: "read".into(),
        description: "Screenshot".into(),
        input_schema: json!({"type":"object"}),
        module: None,
        execution_mode: Default::default(),
        reply: None,
    }];
    let captured = Arc::new(Mutex::new(Vec::new()));
    let (tx, mut rx) = mpsc::channel(4096);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let mut request = fixture_request(session.clone(), tools);
    request.run_id = RunId("media-1".into());
    request.model = ModelHandle::new("openai", "gpt-5.3-codex");
    request.context_limit = Some(128000);
    let provider = native_provider(wal.clone(), &session, frozen.clone(), captured.clone());
    let mut manifest = (*mode3_manifest()).clone();
    manifest.plan =
        broca_wal::OpaqueJson::from_json_text(json!({"render_tags":true}).to_string()).unwrap();
    manifest.tools = Arc::new(request.tools.clone());
    let control = broca_core::RunControl {
        new_log_compaction_plan_version: mode3.then_some(broca_core::compaction_v2::PLAN_VERSION),
        admitted_context_limit: request.context_limit,
        manifest: Some(Arc::new(manifest)),
        ..Default::default()
    };
    broca_core::run::run_loop_with_control(
        request.clone(),
        &provider,
        &NativeImageTool,
        &FileLeaseStore::new(root.join("leases")),
        &AllowAll,
        &*wal,
        None,
        control,
        &tx,
    )
    .await
    .unwrap();
    drop(provider);
    for (id, mode, pause) in [
        ("media-2", broca_core::StartMode::AppendEpisode, false),
        ("media-3", broca_core::StartMode::AppendEpisode, true),
        ("media-3", broca_core::StartMode::ResumeAfterAuth, false),
    ] {
        request.run_id = RunId(id.into());
        request.input = vec![Message::new(
            Role::User,
            vec![ContentBlock::Text { text: id.into() }],
        )];
        let provider = native_provider(wal.clone(), &session, frozen.clone(), captured.clone());
        let writer =
            broca_core::wal_writer::WalHandle::spawn(wal.open(&session).unwrap(), 1).unwrap();
        let control = broca_core::RunControl {
            pause_for_auth_at_start: pause.then(|| {
                serde_json::from_value(json!({"class":"auth_required","message":"fixture pause"}))
                    .unwrap()
            }),
            ..Default::default()
        };
        let result = broca_core::run_loop_on_writer(
            request.clone(),
            &writer,
            mode,
            &provider,
            &NativeImageTool,
            &AllowAll,
            &*wal,
            None,
            control,
            &tx,
        )
        .await
        .unwrap();
        assert_eq!(result.paused.is_some(), pause);
        writer.shutdown().await.unwrap();
    }
    drop(tx);
    drain.await.unwrap();
    server.await.unwrap();
    let records: Vec<_> = wal
        .read_records(&session)
        .unwrap()
        .into_iter()
        .map(|frame| frame.record)
        .collect();
    let terminal_ms = records
        .iter()
        .filter_map(|record| match record {
            WalRecord::ModelAttemptFinished { ts_ms, .. } => Some(*ts_ms as i64 + 1),
            _ => None,
        })
        .max()
        .unwrap();
    index_bind(&index(&root), &session, "completed", Some(terminal_ms));
    let bodies = captured.lock().unwrap().clone();
    assert_eq!(bodies.len(), 4);
    assert!(records
        .iter()
        .any(|record| matches!(record, WalRecord::RunPaused { .. })));
    for body in &bodies[1..] {
        use base64::Engine;
        let text = std::str::from_utf8(body).unwrap();
        assert!(text.contains("\"type\":\"input_image\""));
        assert!(text.contains(&format!(
            "data:image/png;base64,{}",
            base64::engine::general_purpose::STANDARD.encode(TOOL_PNG)
        )));
    }
    (root, session, records, bodies)
}

#[tokio::test]
async fn chatgpt_media_cache_dump_matches_live_first_later_and_resumed_requests() {
    use broca_core::media_store::MediaStoreClass;
    for class in [
        MediaStoreClass::NonDurable,
        MediaStoreClass::Durable,
        MediaStoreClass::Ephemeral,
    ] {
        let tag = format!("native-{}", class.directory());
        let (root, session, records, captured) = chatgpt_media_fixture(&tag, true).await;
        let wal = FileWalStore::new(root.join("wal"));
        let path = match class {
            MediaStoreClass::NonDurable => wal.wal_path(&session),
            MediaStoreClass::Durable => wal.durable_wal_path(&session),
            MediaStoreClass::Ephemeral => wal.ephemeral_wal_path(&session),
        };
        if class != MediaStoreClass::NonDurable {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::rename(wal.wal_path(&session), path).unwrap();
            std::fs::rename(root.join("media"), root.join(class.directory())).unwrap();
        }
        let media = root.join(class.directory());
        for entry in std::fs::read_dir(&media).unwrap() {
            let entry = entry.unwrap().path();
            if entry.file_name().unwrap() != "objects" {
                if entry.is_dir() {
                    std::fs::remove_dir_all(entry).unwrap();
                } else {
                    std::fs::remove_file(entry).unwrap();
                }
            }
        }
        let out = root.join("out");
        let summary = export(&root, &[tag], None, &out).unwrap();
        assert_eq!(summary.calls_exported, 4);
        assert_eq!(summary.calls_verified, 4);
        assert_eq!(summary.digest_mismatches, 0);
        assert!(summary.calls_skipped.is_empty());
        let mut run = None;
        let mut calls = 0;
        for record in &records {
            match record {
                WalRecord::RunStarted { run_id, .. } => run = Some(&run_id.0),
                WalRecord::ModelAttemptFinished {
                    step_id,
                    request_body_sha256,
                    transport,
                    ..
                } => {
                    assert_eq!(transport.as_deref(), Some("websocket"));
                    let filename = broca_provider::request_dump::request_dump_filename(
                        None,
                        run.unwrap(),
                        step_id.0,
                    );
                    let body =
                        std::fs::read(out.join(filename.replace(".json", ".body.json"))).unwrap();
                    assert_eq!(
                        body, captured[calls],
                        "export differs from live canonical body"
                    );
                    assert_eq!(format!("{:x}", Sha256::digest(&body)), *request_body_sha256);
                    calls += 1;
                }
                _ => {}
            }
        }
        assert_eq!(calls, 4);
        assert_eq!(
            std::fs::read_dir(&media).unwrap().count(),
            1,
            "export must not initialize refs, locks or identities"
        );
        if class == MediaStoreClass::Durable {
            let options = broca_core::wal_fold::FoldOptions {
                write: true,
                ..Default::default()
            };
            let report = broca_core::wal_fold::fold(
                &root,
                broca_core::wal_fold::FoldSettings {
                    options: &options,
                    cold_ms: 0,
                },
            )
            .unwrap();
            assert_eq!(report.removed, 1);
            let archived = root.join("out-archived");
            let summary = export(&root, &[session.session], None, &archived).unwrap();
            assert_eq!(summary.calls_verified, 4);
            assert_eq!(summary.digest_mismatches, 0);
            check_bodies(&archived, &captured);
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

fn assert_native_refusal(root: &Path, session: &BindIdentity, code: &str) -> String {
    let out = root.join("refused-out");
    let result = std::process::Command::new(env!("CARGO_BIN_EXE_broca-session"))
        .args(["cache-dump", "--state-root"])
        .arg(root)
        .args(["--session", &session.session, "--out"])
        .arg(&out)
        .output()
        .unwrap();
    assert!(
        !result.status.success(),
        "missing native inputs must refuse"
    );
    assert!(
        result.stdout.is_empty(),
        "refused command must not print a success summary"
    );
    let error = String::from_utf8(result.stderr).unwrap();
    assert!(error.contains(code), "{error}");
    assert!(error.contains(&session.session), "{error}");
    assert!(
        error.contains("media-1") && error.contains("step=2"),
        "{error}"
    );
    assert!(
        !error.contains("digest_mismatch") && !error.contains("render_unavailable"),
        "{error}"
    );
    for suffix in [".body.json", ".response.json", ".meta.json"] {
        let written = files(&out, suffix);
        assert_eq!(
            written.len(),
            1,
            "only the earlier text-only call may remain"
        );
        assert!(written[0]
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .contains("media-1-s1"));
    }
    error
}

#[tokio::test]
async fn chatgpt_media_cache_dump_missing_store_refuses_without_creating_directories() {
    let (root, session, _, _) = chatgpt_media_fixture("native-missing-store", true).await;
    std::fs::remove_dir_all(root.join("media")).unwrap();
    let error = assert_native_refusal(&root, &session, "media_store_missing");
    assert!(
        error.contains(root.join("media").to_str().unwrap()),
        "{error}"
    );
    for directory in ["media", "media-durable", "media-ephemeral"] {
        assert!(!root.join(directory).exists());
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn chatgpt_media_cache_dump_missing_object_refuses_without_cross_class_fallback() {
    let (root, session, _, _) = chatgpt_media_fixture("native-missing-object", true).await;
    let digest = format!("{:x}", Sha256::digest(TOOL_PNG));
    let wrong = root.join("media-durable/objects");
    std::fs::create_dir_all(&wrong).unwrap();
    std::fs::rename(
        root.join("media/objects").join(&digest),
        wrong.join(&digest),
    )
    .unwrap();
    let error = assert_native_refusal(&root, &session, "media_object_missing");
    assert!(error.contains(&digest), "{error}");
    assert!(!root.join("media/objects").join(&digest).exists());
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn cache_dump_text_only_needs_no_media_directory() {
    let (root, _, captured) = fixture(
        "text-no-media",
        OpenAiResponses,
        openai_responses_frozen_config("m"),
        vec![responses()],
        false,
    )
    .await;
    let out = root.join("out");
    let summary = export(&root, &["text-no-media".into()], None, &out).unwrap();
    assert_eq!(summary.calls_verified, 1);
    assert_eq!(summary.digest_mismatches, 0);
    check_bodies(&out, &captured);
    for directory in ["media", "media-durable", "media-ephemeral"] {
        assert!(!root.join(directory).exists());
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn chatgpt_media_cache_dump_defers_legacy_media_only_tags() {
    let (root, session, _, captured) = chatgpt_media_fixture("native-legacy-tags", false).await;
    let out = root.join("out");
    let summary = export(&root, &[session.session], None, &out).unwrap();
    assert_eq!(summary.calls_verified, 4);
    assert_eq!(summary.digest_mismatches, 0);
    check_bodies(&out, &captured);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn mode3_offline_rebuild_matches_every_received_step_after_a_ride() {
    let (root, mut records, bodies) = fixture_with_mode(
        "mode3-rebuild",
        AnthropicMessages,
        anthropic_frozen_config("m"),
        vec![anthropic(true), anthropic(false)],
        true,
        FixtureMode::Compaction,
    )
    .await;
    assert_eq!(bodies.len(), 2);
    assert!(records
        .iter()
        .any(|r| matches!(r, WalRecord::CompactionViewAppliedV2 { .. })));
    // An accepted replacement is not the view the request sent until an application is recorded.
    let at = records
        .iter()
        .position(|record| matches!(record, WalRecord::ModelAttemptFinished { .. }))
        .unwrap();
    let newer = broca_core::compaction_v2::protocol::view::View::seal(2, vec![]);
    records.insert(
        at,
        WalRecord::CompactionViewV2(broca_wal::CompactionViewV2 {
            version: 2,
            checksum: newer.checksum.clone(),
            edits: broca_core::compaction_v2::storage::encode(None, &newer.into()),
            built_through: None,
            new_signed: None,
            applied_by: None,
        }),
    );
    let (run_id, config) = records
        .iter()
        .find_map(|record| match record {
            WalRecord::RunStarted { run_id, config, .. } => Some((run_id, config)),
            _ => None,
        })
        .unwrap();
    let bind = BindIdentity::new(root.join("proj"), "test", "mode3-rebuild");
    let lineage = FileWalStore::new(root.join("wal"))
        .read_records_range(&bind, 0, None, None)
        .unwrap()
        .lineage_id
        .unwrap()
        .to_string();
    for (index, record) in records.iter().enumerate() {
        if let WalRecord::ModelAttemptFinished { step_id, .. } = record {
            let (_, _, rebuilt) = broca_module_serve::wal_render::render_step_with_lineage(
                Some(&lineage),
                &records[..index],
                config,
                &run_id.0,
                step_id.0,
                None,
                None,
            )
            .unwrap();
            assert!(
                rebuilt == bodies[step_id.0 as usize - 1],
                "step {} body differs",
                step_id.0
            );
        }
    }
}

#[tokio::test]
async fn mode3_global_system_next_request_matches_cache_dump_rebuild() {
    let (root, records, bodies) = fixture_with_mode(
        "mode3-global-system",
        AnthropicMessages,
        anthropic_frozen_config("m"),
        vec![anthropic(true), anthropic(false)],
        true,
        FixtureMode::GlobalSystem,
    )
    .await;
    assert_eq!(bodies.len(), 2);
    let (run_id, config, bind) = records
        .iter()
        .find_map(|record| match record {
            WalRecord::RunStarted {
                run_id,
                config,
                session,
                input,
                manifest,
                ..
            } => {
                assert!(manifest.is_none());
                assert!(config.global_system.is_some());
                assert!(input.iter().all(|message| message.role != Role::System));
                Some((run_id, config, session))
            }
            _ => None,
        })
        .unwrap();
    let next = records
        .iter()
        .position(|record| {
            matches!(record,
                WalRecord::ModelAttemptFinished { step_id, .. } if step_id.0 == 2
            )
        })
        .unwrap();
    let lineage = FileWalStore::new(root.join("wal"))
        .read_records_range(bind, 0, None, None)
        .unwrap()
        .lineage_id
        .unwrap()
        .to_string();
    let (_, _, rebuilt) = broca_module_serve::wal_render::render_step_with_lineage(
        Some(&lineage),
        &records[..next],
        config,
        &run_id.0,
        2,
        None,
        None,
    )
    .unwrap();
    let live: Value = serde_json::from_slice(&bodies[1]).unwrap();
    assert!(live["system"].as_array().unwrap().iter().any(|part| {
        part["text"]
            .as_str()
            .is_some_and(|text| text.contains("Frozen unplanned system"))
    }));
    assert!(
        rebuilt == bodies[1],
        "the next request must include the same frozen system bytes"
    );
}

#[tokio::test]
async fn legacy_view_applied_after_step_started_exports_the_first_request() {
    use broca_core::compaction_provider::{
        lineage::MessageRef,
        view::{CompactionView, ViewRange},
    };
    let (root, mut records, _) = fixture(
        "legacy-applied",
        AnthropicMessages,
        anthropic_frozen_config("m"),
        vec![anthropic(false)],
        false,
    )
    .await;
    let boundary = records
        .iter()
        .position(|r| matches!(r, WalRecord::StepStarted { .. }))
        .unwrap();
    let replacement = vec![Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "summary".into(),
        }],
    )];
    let view = CompactionView::new(
        "session",
        1,
        "lineage",
        MessageRef {
            ordinal: 0,
            mid: "m0".into(),
        },
        vec![ViewRange::replace(
            0,
            1,
            serde_json::to_string(&replacement).unwrap(),
        )],
    );
    records.insert(
        boundary,
        serde_json::from_value(json!({
            "type":"compaction_message", "provider":"compactor", "kind":"setup",
            "compaction_id":"initial", "version":1, "range":{"from":0,"to":0},
            "sha256":"fixture", "replacement":[]
        }))
        .unwrap(),
    );
    records.insert(
        boundary + 2,
        WalRecord::CompactionViewApplied {
            provider: "compactor".into(),
            view_version: 1,
            view: broca_wal::OpaqueJson::from_json_text(serde_json::to_string(&view).unwrap())
                .unwrap(),
            choice: broca_wal::CompactionViewChoice::Applied,
        },
    );
    let expected = AnthropicMessages.render(
        &anthropic_frozen_config("m"),
        &broca_protocol::CallOptions::new(replacement),
    );
    for record in &mut records {
        if let WalRecord::ModelAttemptFinished {
            request_body_sha256,
            ..
        } = record
        {
            *request_body_sha256 = format!("{:x}", Sha256::digest(&expected));
        }
    }
    let out = root.join("legacy-out");
    std::fs::create_dir(&out).unwrap();
    let mut summary = Summary::default();
    export_records("legacy", &records, &out, &mut summary).unwrap();
    assert_eq!(summary.calls_verified, 1);
    assert_eq!(summary.calls_exported, 1);
    assert!(summary.calls_skipped.is_empty());
    check_bodies(&out, &[expected]);
}

#[tokio::test]
async fn mode3_cache_dump_exports_every_call_verified_without_skips() {
    let (root, _, bodies) = fixture_with_mode(
        "mode3-export",
        AnthropicMessages,
        anthropic_frozen_config("m"),
        vec![anthropic(true), anthropic(false)],
        true,
        FixtureMode::Compaction,
    )
    .await;
    let out = root.join("out");
    let summary = export(&root, &["mode3-export".into()], None, &out).unwrap();
    assert_eq!(summary.calls_examined, 2);
    assert_eq!(summary.calls_verified, 2);
    assert_eq!(summary.calls_exported, 2);
    assert_eq!(summary.digest_mismatches, 0);
    assert!(
        summary.calls_skipped.is_empty(),
        "{:?}",
        summary.calls_skipped
    );
    check_bodies(&out, &bodies);
}

#[tokio::test]
async fn mode3_cache_dump_matches_live_newest_signed_strip() {
    let (root, records, bodies) = fixture_with_mode(
        "mode3-strip",
        AnthropicMessages,
        anthropic_frozen_config("claude-opus-5-5"),
        vec![anthropic(false)],
        true,
        FixtureMode::StripSigned,
    )
    .await;
    let stored = records
        .iter()
        .find_map(|record| match record {
            WalRecord::CompactionViewV2(view) => Some(view),
            _ => None,
        })
        .unwrap();
    assert!(stored.built_through.is_some());
    assert_eq!(stored.new_signed.as_deref(), Some("strip"));
    assert!(
        stored.applied_by.is_none(),
        "ride responses are not attested apply_view calls"
    );
    let rebuilt = broca_core::compaction_v2::storage::rebuild(
        None,
        stored.version,
        stored.checksum.clone(),
        &stored.edits,
        stored.built_through.clone(),
        stored.new_signed.clone(),
    )
    .unwrap();
    assert!(rebuilt.view().verify());
    assert_eq!(bodies.len(), 1);
    let body: Value = serde_json::from_slice(&bodies[0]).unwrap();
    let content: Vec<_> = body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|message| message["content"].as_array().unwrap())
        .collect();
    assert!(content
        .iter()
        .any(|part| part["type"] == "tool_use" && part["id"] == "prior-call"));
    assert!(content
        .iter()
        .any(|part| part["type"] == "tool_result" && part["tool_use_id"] == "prior-call"));
    assert!(!content
        .iter()
        .any(|part| part["type"] == "thinking" || part["type"] == "redacted_thinking"));
    assert!(!String::from_utf8_lossy(&bodies[0]).contains("old context"));
    let out = root.join("out");
    let summary = export(&root, &["mode3-strip".into()], None, &out).unwrap();
    assert_eq!(summary.calls_examined, 1);
    assert_eq!(summary.calls_verified, 1);
    assert_eq!(summary.calls_exported, 1);
    assert_eq!(summary.digest_mismatches, 0);
    assert!(
        summary.calls_skipped.is_empty(),
        "{:?}",
        summary.calls_skipped
    );
    check_bodies(&out, &bodies);
}

#[tokio::test]
async fn anthropic_tool_run_exports_server_bytes_and_wire_usage() {
    let (root, _, captured) = fixture(
        "anthropic",
        AnthropicMessages,
        anthropic_frozen_config("m"),
        vec![
            anthropic(true),
            String::from_utf8(anthropic(true))
                .unwrap()
                .replace("\"call\"", "\"call2\"")
                .into_bytes(),
            anthropic(false),
        ],
        true,
    )
    .await;
    let out = root.join("out");
    let summary = export(&root, &["anthropic".into()], None, &out).unwrap();
    assert_eq!(summary.calls_exported, 3);
    check_bodies(&out, &captured);
    for file in files(&out, ".response.json") {
        let value: Value = serde_json::from_slice(&std::fs::read(file).unwrap()).unwrap();
        assert_eq!(
            value,
            json!({"usage":{"input_tokens":11,"cache_read_input_tokens":23,"cache_creation_input_tokens":7,"output_tokens":5},"broca_usage":{"input_tokens":11,"cached_input_tokens":23,"cache_write_tokens":7,"output_tokens":5}})
        );
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&out).unwrap().permissions().mode() & 0o777,
            0o700
        );
        for entry in std::fs::read_dir(&out).unwrap() {
            assert_eq!(
                entry.unwrap().metadata().unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }
    let wal = FileWalStore::new(root.join("wal"));
    let address = wal.addresses().unwrap().into_iter().next().unwrap();
    let live = root.join("wal").join(format!("{address}.wal"));
    std::fs::create_dir(root.join("wal-archive")).unwrap();
    let mut writer = broca_wal::archive::ArchiveWriter::create(root.join("wal-archive/fold-1.ark"))
        .unwrap()
        .without_feature_floor();
    writer
        .append(&address, &std::fs::read(&live).unwrap())
        .unwrap();
    writer.seal().unwrap();
    std::fs::remove_file(live).unwrap();
    let archived = root.join("archived");
    let summary = export(&root, &["anthropic".into()], None, &archived).unwrap();
    assert_eq!(summary.calls_exported, 3);
    for original in files(&out, ".json") {
        assert!(
            std::fs::read(&original).unwrap()
                == std::fs::read(archived.join(original.file_name().unwrap())).unwrap(),
            "archive and live export bytes must agree"
        );
    }
}
#[tokio::test]
async fn responses_run_exports_server_bytes_and_inclusive_input() {
    let (root, _, captured) = fixture(
        "responses",
        OpenAiResponses,
        openai_responses_frozen_config("m"),
        vec![responses()],
        false,
    )
    .await;
    let out = root.join("out");
    let summary = export(&root, &["responses".into()], None, &out).unwrap();
    assert_eq!(summary.calls_exported, 1);
    check_bodies(&out, &captured);
    let value: Value =
        serde_json::from_slice(&std::fs::read(&files(&out, ".response.json")[0]).unwrap()).unwrap();
    assert_eq!(
        value,
        json!({"usage":{"input_tokens":41,"input_tokens_details":{"cached_tokens":23},"output_tokens":5},"broca_usage":{"input_tokens":11,"cached_input_tokens":23,"cache_write_tokens":7,"output_tokens":5}})
    );
}
fn assert_gemini_export(tag: &str, captured: &[Vec<u8>], root: &Path) {
    let out = root.join("out");
    let summary = export(root, &[tag.into()], None, &out).unwrap();
    assert_eq!(summary.calls_exported, 1);
    assert!(summary.calls_skipped.is_empty());
    check_bodies(&out, captured);
    let value: Value =
        serde_json::from_slice(&std::fs::read(&files(&out, ".response.json")[0]).unwrap()).unwrap();
    assert_eq!(
        value,
        serde_json::from_str::<Value>(GEMINI_WIRE_USAGE).unwrap()
    );
}
#[tokio::test]
async fn gemini_run_exports_server_bytes_and_usage_metadata() {
    let (root, _, captured) = fixture(
        "gemini",
        Gemini,
        gemini_frozen_config("m"),
        vec![data_sse(gemini_chunk())],
        false,
    )
    .await;
    assert_gemini_export("gemini", &captured, &root);
}

#[tokio::test]
async fn durable_live_and_archived_exports_match_server_bytes() {
    let tag = "durable-export";
    let (root, _, captured) = fixture(
        tag,
        Gemini,
        gemini_frozen_config("m"),
        vec![data_sse(gemini_chunk())],
        false,
    )
    .await;
    let store = FileWalStore::new(root.join("wal"));
    let bind = BindIdentity::new(root.join("proj"), "test", tag);
    std::fs::create_dir_all(store.durable_root()).unwrap();
    std::fs::rename(store.wal_path(&bind), store.durable_wal_path(&bind)).unwrap();
    assert_gemini_export(tag, &captured, &root);
    let options = broca_core::wal_fold::FoldOptions {
        write: true,
        ..Default::default()
    };
    let report = broca_core::wal_fold::fold(
        &root,
        broca_core::wal_fold::FoldSettings {
            options: &options,
            cold_ms: 0,
        },
    )
    .unwrap();
    assert_eq!(report.removed, 1);
    assert!(!store.durable_wal_path(&bind).exists());
    let out = root.join("out-archived");
    let summary = export(&root, &[tag.into()], None, &out).unwrap();
    assert_eq!(summary.calls_exported, 1);
    assert!(summary.calls_skipped.is_empty());
    check_bodies(&out, &captured);
}
#[tokio::test]
async fn codeassist_run_exports_server_bytes_and_usage_metadata() {
    let mut frozen = google_codeassist_frozen_config("m");
    frozen.project_id = Some("fixture-project".into());
    let (root, _, captured) = fixture(
        "codeassist",
        GoogleCodeAssist,
        frozen,
        vec![data_sse(json!({"response": gemini_chunk()}))],
        false,
    )
    .await;
    // The captured body is the cloudcode-pa envelope, so a match proves the export is the
    // wrapped request rather than the bare Gemini body nested inside it.
    let envelope: Value = serde_json::from_slice(&captured[0]).unwrap();
    assert_eq!(envelope["project"], "fixture-project");
    assert_gemini_export("codeassist", &captured, &root);
}
#[test]
fn gemini_unreported_counts_are_omitted_not_zero() {
    for family in [WireFamilyId::Gemini, WireFamilyId::GoogleCodeAssist] {
        assert_eq!(
            wire_usage(family, Usage::default()).unwrap(),
            json!({"usage":{},"broca_usage":{}})
        );
        // Reported zeros survive as zeros.
        assert_eq!(
            wire_usage(
                family,
                Usage {
                    input_tokens: Some(0),
                    output_tokens: Some(0),
                    ..Default::default()
                }
            )
            .unwrap()["usage"],
            json!({"promptTokenCount":0,"candidatesTokenCount":0})
        );
        // A cache count without an input count cannot rebuild the prompt total.
        assert_eq!(
            wire_usage(
                family,
                Usage {
                    cached_input_tokens: Some(23),
                    ..Default::default()
                }
            )
            .unwrap()["usage"],
            json!({"cachedContentTokenCount":23})
        );
        // A lone thoughts count normalizes to output == thoughts; candidates were not reported.
        assert_eq!(
            wire_usage(
                family,
                Usage {
                    output_tokens: Some(17),
                    reasoning_tokens: Some(17),
                    ..Default::default()
                }
            )
            .unwrap()["usage"],
            json!({"thoughtsTokenCount":17})
        );
    }
}
#[test]
fn gemini_prompt_count_includes_cached_content() {
    for family in [WireFamilyId::Gemini, WireFamilyId::GoogleCodeAssist] {
        let usage = wire_usage(
            family,
            Usage {
                input_tokens: Some(11),
                cached_input_tokens: Some(23),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(usage["usage"]["promptTokenCount"], 34);
        assert_eq!(usage["usage"]["cachedContentTokenCount"], 23);
    }
}
#[tokio::test]
async fn tampered_frozen_config_never_exports_body() {
    let (root, mut records, _) = fixture(
        "tamper",
        AnthropicMessages,
        anthropic_frozen_config("m"),
        vec![anthropic(false)],
        false,
    )
    .await;
    for record in &mut records {
        if let WalRecord::RunStarted { config, .. } = record {
            config.frozen_render_config["wire_model_id"] = json!("changed");
        }
    }
    let session = records
        .iter()
        .find_map(|r| match r {
            WalRecord::RunStarted { session, .. } => Some(session.clone()),
            _ => None,
        })
        .unwrap();
    let tampered_root = root.join("tampered");
    let wal = FileWalStore::new(tampered_root.join("wal"));
    let mut writer = wal.open(&session).unwrap();
    for record in &records {
        writer.append(1, record, true).unwrap();
    }
    drop(writer);
    index_bind(&index(&tampered_root), &session, "completed", Some(1));
    let out = root.join("out");
    let summary = export(&tampered_root, &["tamper".into()], None, &out).unwrap();
    assert_eq!(summary.digest_mismatches, 1);
    assert!(
        files(&out, ".body.json").is_empty(),
        "unverified requests must not be written"
    );
    assert!(files(&out, ".response.json").is_empty());
    let meta: Value =
        serde_json::from_slice(&std::fs::read(&files(&out, ".meta.json")[0]).unwrap()).unwrap();
    assert_eq!(meta["verified"], false);
    assert_eq!(meta["reason"], "digest_mismatch");
}
#[test]
fn unreported_counts_are_omitted_not_zero() {
    for family in [
        WireFamilyId::AnthropicMessages,
        WireFamilyId::OpenAiResponses,
        WireFamilyId::OpenAiResponsesChatgpt,
    ] {
        assert_eq!(
            wire_usage(family, Usage::default()).unwrap(),
            json!({"usage":{},"broca_usage":{}})
        );
        assert_eq!(
            wire_usage(
                family,
                Usage {
                    input_tokens: Some(0),
                    output_tokens: Some(0),
                    ..Default::default()
                }
            )
            .unwrap(),
            json!({"usage":{"input_tokens":0,"output_tokens":0},"broca_usage":{"input_tokens":0,"output_tokens":0}})
        );
    }
}
#[test]
fn anthropic_input_is_already_fresh() {
    assert_eq!(
        wire_usage(
            WireFamilyId::AnthropicMessages,
            Usage {
                input_tokens: Some(11),
                cached_input_tokens: Some(23),
                cache_write_tokens: Some(7),
                ..Default::default()
            }
        )
        .unwrap()["usage"]["input_tokens"],
        11
    );
}
#[test]
fn responses_and_chatgpt_restore_read_and_write_input() {
    for family in [
        WireFamilyId::OpenAiResponses,
        WireFamilyId::OpenAiResponsesChatgpt,
    ] {
        assert_eq!(
            wire_usage(
                family,
                Usage {
                    input_tokens: Some(11),
                    cached_input_tokens: Some(23),
                    cache_write_tokens: Some(7),
                    ..Default::default()
                }
            )
            .unwrap()["usage"]["input_tokens"],
            41
        );
    }
}
#[cfg(unix)]
#[test]
fn refuses_group_readable_output() {
    use std::os::unix::fs::PermissionsExt;
    let root = scratch("permissions");
    let out = root.join("out");
    std::fs::create_dir(&out).unwrap();
    std::fs::set_permissions(&out, std::fs::Permissions::from_mode(0o750)).unwrap();
    assert!(export(&root, &["absent".into()], None, &out).is_err());
    assert!(files(&out, ".json").is_empty());
}
#[test]
fn since_accepts_rfc3339_and_epoch_ms() {
    use broca_module_serve::cache_dump::parse_since;
    assert_eq!(parse_since("2026-01-01T00:00:00Z").unwrap(), 1767225600000);
    assert_eq!(parse_since("1767225600000").unwrap(), 1767225600000);
    assert!(parse_since("not a date").is_err());
}

#[tokio::test]
async fn last_completed_attempt_owns_usage_and_transformed_mismatches_are_named() {
    let (root, mut records, _) = fixture(
        "attempts",
        AnthropicMessages,
        anthropic_frozen_config("m"),
        vec![anthropic(false)],
        false,
    )
    .await;
    let index = records
        .iter()
        .position(|r| matches!(r, WalRecord::ModelAttemptFinished { .. }))
        .unwrap();
    let mut earlier = records[index].clone();
    if let WalRecord::ModelAttemptFinished {
        request_body_sha256,
        ..
    } = &mut earlier
    {
        *request_body_sha256 = "bad".into();
    }
    if let WalRecord::ModelAttemptFinished { attempt, .. } = &mut records[index] {
        *attempt = 1;
    }
    records.insert(index, earlier);
    let out = root.join("out");
    std::fs::create_dir(&out).unwrap();
    let mut summary = Summary::default();
    export_records("0123456789abcdef", &records, &out, &mut summary).unwrap();
    assert_eq!(summary.calls_exported, 1);
    assert_eq!(summary.digest_mismatches, 0);
    assert!(files(&out, ".meta.json")[0]
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .ends_with("fixture_run-s1.meta.json"));
    let meta: Value =
        serde_json::from_slice(&std::fs::read(&files(&out, ".meta.json")[0]).unwrap()).unwrap();
    assert_eq!(meta["attempt"], 1);
    assert_eq!(meta["files"]["body"], "fixture_run-s1.body.json");
    assert_eq!(meta["files"]["response"], "fixture_run-s1.response.json");
    for record in &mut records {
        if let WalRecord::ModelAttemptFinished {
            attempt: 1,
            request_body_sha256,
            input_transformations,
            ..
        } = record
        {
            *request_body_sha256 = "bad".into();
            *input_transformations = Some(vec![]);
        }
    }
    let mismatch = root.join("mismatch");
    std::fs::create_dir(&mismatch).unwrap();
    let mut summary = Summary::default();
    export_records("0123456789abcdef", &records, &mismatch, &mut summary).unwrap();
    assert_eq!(summary.calls_skipped.get("transformed_request"), Some(&1));
    let meta: Value =
        serde_json::from_slice(&std::fs::read(&files(&mismatch, ".meta.json")[0]).unwrap())
            .unwrap();
    assert_eq!(meta["reason"], "transformed_request");
    assert_eq!(meta["verified"], false);
    assert!(files(&mismatch, ".body.json").is_empty());
}

#[test]
fn empty_selection_exits_nonzero_without_daemon() {
    let root = scratch("empty-cli");
    index(&root);
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_broca-session"))
        .args(["cache-dump", "--session", "absent", "--out"])
        .arg(root.join("out"))
        .arg("--state-root")
        .arg(&root)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("unknown session") && error.contains("alfonso:absent"));
    assert!(!root.join("out").exists());
}

#[tokio::test]
async fn response_without_durable_usage_is_examined_and_body_only() {
    let (root, mut records, _) = fixture(
        "missing-usage",
        AnthropicMessages,
        anthropic_frozen_config("m"),
        vec![anthropic(false)],
        false,
    )
    .await;
    records.retain(|r| !matches!(r, WalRecord::ModelStepFinished { .. }));
    let out = root.join("out");
    std::fs::create_dir(&out).unwrap();
    let mut summary = Summary::default();
    export_records("0123456789abcdef", &records, &out, &mut summary).unwrap();
    assert_eq!(summary.calls_examined, 1);
    assert_eq!(summary.calls_exported, 0);
    assert_eq!(summary.calls_verified, 1);
    assert_eq!(summary.calls_skipped.get("missing_step_usage"), Some(&1));
    assert_eq!(files(&out, ".body.json").len(), 1);
    assert!(files(&out, ".response.json").is_empty());
    let meta: Value =
        serde_json::from_slice(&std::fs::read(&files(&out, ".meta.json")[0]).unwrap()).unwrap();
    assert_eq!(meta["reason"], "missing_step_usage");
    assert_eq!(meta["verified"], true);
}

#[tokio::test]
async fn session_selection_reads_only_inventory_matching_binds() {
    let (root, _, captured) = fixture(
        "indexed-target",
        AnthropicMessages,
        anthropic_frozen_config("m"),
        vec![anthropic(false)],
        false,
    )
    .await;
    let unrelated = BindIdentity::new(root.join("proj"), "test", "unrelated");
    index_bind(&index(&root), &unrelated, "completed", Some(1));
    let wal = FileWalStore::new(root.join("wal"));
    // The unrelated WAL needs no valid records: the read count exposes any broad selection.
    std::fs::write(wal.wal_path(&unrelated), b"not a WAL").unwrap();
    let out = root.join("selected");
    let summary = export(&root, &["indexed-target".into()], None, &out).unwrap();
    assert_eq!(summary.sessions_read, 1);
    assert_eq!(summary.calls_exported, 1);
    assert!(summary.sessions_not_found.is_empty());
    check_bodies(&out, &captured);
}

#[tokio::test]
async fn since_preselects_activity_bounds_and_checks_wal_attempt_time() {
    let (root, records, _) = fixture(
        "active",
        AnthropicMessages,
        anthropic_frozen_config("m"),
        vec![anthropic(false)],
        false,
    )
    .await;
    let cutoff = records
        .iter()
        .find_map(|r| match r {
            WalRecord::ModelAttemptFinished { ts_ms, .. } => Some(*ts_ms),
            _ => None,
        })
        .unwrap();
    let wal = FileWalStore::new(root.join("wal"));
    let connection = index(&root);
    let active = BindIdentity::new(root.join("proj"), "test", "active");
    index_bind(&connection, &active, "active", Some(1));
    for (name, state, time) in [
        ("paused", "paused", Some(1)),
        ("recent-terminal", "completed", Some(cutoff as i64 + 1)),
    ] {
        let bind = BindIdentity::new(root.join("proj"), "test", name);
        index_bind(&connection, &bind, state, time);
        let mut writer = wal.open(&bind).unwrap();
        for mut record in records.clone() {
            if let WalRecord::ModelAttemptFinished { ts_ms, .. } = &mut record {
                *ts_ms = 1;
            }
            writer.append(1, &record, true).unwrap();
        }
    }
    for (name, time) in [("old-terminal", Some(1)), ("legacy-undated", None)] {
        let bind = BindIdentity::new(root.join("proj"), "test", name);
        index_bind(&connection, &bind, "completed", time);
        std::fs::write(wal.wal_path(&bind), b"not a WAL").unwrap();
    }
    let summary = export(&root, &[], Some(cutoff), &root.join("since")).unwrap();
    assert_eq!(summary.sessions_read, 3);
    assert_eq!(summary.calls_examined, 1);
    assert_eq!(summary.calls_exported, 1);
    assert_eq!(summary.undated_runs_not_selected, 1);
    assert_eq!(summary.calls_before_since, 2);
}

#[tokio::test]
async fn direct_bind_selection_needs_no_index() {
    let (root, _, _) = fixture(
        "direct",
        AnthropicMessages,
        anthropic_frozen_config("m"),
        vec![anthropic(false)],
        false,
    )
    .await;
    std::fs::remove_file(root.join("run-index.db")).unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_broca-session"))
        .args([
            "cache-dump",
            "--session",
            "direct",
            "--harness",
            "test",
            "--project-root",
        ])
        .arg(root.join("proj"))
        .arg("--out")
        .arg(root.join("direct-out"))
        .arg("--state-root")
        .arg(&root)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "direct selection must not open the index"
    );
    let summary: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(summary["calls_exported"], 1);
    assert!(!root.join("run-index.db").exists());
}

#[tokio::test]
async fn read_only_index_sees_uncheckpointed_committed_binds() {
    let (root, _, _) = fixture(
        "original-name",
        AnthropicMessages,
        anthropic_frozen_config("m"),
        vec![anthropic(false)],
        false,
    )
    .await;
    let old = BindIdentity::new(root.join("proj"), "test", "original-name");
    let new = BindIdentity::new(root.join("proj"), "test", "new-name");
    let wal = FileWalStore::new(root.join("wal"));
    std::fs::copy(wal.wal_path(&old), wal.wal_path(&new)).unwrap();
    let connection = index(&root);
    connection
        .execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")
        .unwrap();
    index_bind(&connection, &new, "completed", Some(1));
    assert!(root.join("run-index.db-wal").metadata().unwrap().len() > 0);
    let summary = export(&root, &["new-name".into()], None, &root.join("sidecar")).unwrap();
    assert_eq!(summary.calls_exported, 1);
    assert!(summary.sessions_not_found.is_empty());
    drop(connection);
}

#[test]
fn unreadable_index_names_path_and_does_not_create_database() {
    let root = scratch("missing-index");
    let error = match export(&root, &["unknown".into()], None, &root.join("out")) {
        Ok(_) => panic!("missing index must fail"),
        Err(error) => error,
    };
    assert!(error.contains(root.join("run-index.db").to_str().unwrap()));
    assert!(!root.join("run-index.db").exists());
    let corrupted = root.join("run-index.db");
    std::fs::write(&corrupted, b"not SQLite").unwrap();
    assert!(export(&root, &["unknown".into()], None, &root.join("bad-out")).is_err());
}

#[tokio::test]
async fn digest_mismatch_outranks_missing_usage_with_one_skip_reason() {
    let (root, mut records, _) = fixture(
        "mismatch-no-usage",
        AnthropicMessages,
        anthropic_frozen_config("m"),
        vec![anthropic(false)],
        false,
    )
    .await;
    records.retain(|r| !matches!(r, WalRecord::ModelStepFinished { .. }));
    for record in &mut records {
        if let WalRecord::ModelAttemptFinished {
            request_body_sha256,
            ..
        } = record
        {
            *request_body_sha256 = "changed".into();
        }
    }
    let out = root.join("both");
    std::fs::create_dir(&out).unwrap();
    let mut summary = Summary::default();
    export_records("0123456789abcdef", &records, &out, &mut summary).unwrap();
    assert_eq!(summary.calls_examined, 1);
    assert_eq!(summary.digest_mismatches, 1);
    assert_eq!(summary.calls_skipped.len(), 1);
    assert_eq!(summary.calls_skipped.get("digest_mismatch"), Some(&1));
    assert!(files(&out, ".body.json").is_empty());
    assert!(files(&out, ".response.json").is_empty());
    let meta: Value =
        serde_json::from_slice(&std::fs::read(&files(&out, ".meta.json")[0]).unwrap()).unwrap();
    assert_eq!(meta["reason"], "digest_mismatch");
    assert_eq!(meta["verified"], false);
}

#[tokio::test]
async fn session_absent_from_inventory_refuses_before_any_valid_export() {
    let (root, _, _) = fixture(
        "known",
        AnthropicMessages,
        anthropic_frozen_config("m"),
        vec![anthropic(false)],
        false,
    )
    .await;
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_broca-session"))
        .args([
            "cache-dump",
            "--session",
            "known",
            "--session",
            "missing-name",
            "--out",
        ])
        .arg(root.join("partial"))
        .arg("--state-root")
        .arg(&root)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("missing-name") && error.contains("alfonso:missing-name"));
    assert!(!root.join("partial").exists());
}

#[tokio::test]
async fn since_filters_winning_attempt_and_replays_prior_history() {
    let (root, mut records, captured) = fixture(
        "since-history",
        AnthropicMessages,
        anthropic_frozen_config("m"),
        vec![anthropic(true), anthropic(false)],
        true,
    )
    .await;
    let bind = BindIdentity::new(root.join("proj"), "test", "since-history");
    for record in &mut records {
        if let WalRecord::ModelAttemptFinished { step_id, ts_ms, .. } = record {
            *ts_ms = if step_id.0 == 1 { 1 } else { 1001 };
        }
    }
    let first = records
        .iter()
        .position(|r| matches!(r, WalRecord::ModelAttemptFinished { .. }))
        .unwrap();
    let mut failed_retry = records[first].clone();
    if let WalRecord::ModelAttemptFinished { ts_ms, outcome, .. } = &mut failed_retry {
        *ts_ms = 2000;
        *outcome = broca_protocol::provider::AttemptOutcome::HttpError;
    }
    records.insert(first, failed_retry);
    let wal = FileWalStore::new(root.join("wal"));
    std::fs::remove_file(wal.wal_path(&bind)).unwrap();
    let mut writer = wal.open(&bind).unwrap();
    for record in &records {
        writer.append(1, record, true).unwrap();
    }
    drop(writer);
    index_bind(&index(&root), &bind, "completed", Some(2001));
    let out = root.join("recent-calls");
    let summary = export(&root, &[], Some(1000), &out).unwrap();
    assert_eq!(summary.calls_examined, 1);
    assert_eq!(summary.calls_exported, 1);
    assert_eq!(summary.calls_before_since, 1);
    check_bodies(&out, &captured[1..]);
    assert!(files(&out, ".body.json")[0]
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .ends_with("-s2.body.json"));
}

#[test]
fn move_sidecars_and_pending_activation_cannot_leave_a_cache_dump() {
    for kind in ["moving", "moved", "activate"] {
        let root = scratch(&format!("move-cache-{kind}"));
        let key = BindIdentity::new(root.join("proj"), "runner", "moved");
        let store = broca_core::FileWalStore::new(root.join("wal"));
        {
            let mut writer = broca_core::WalStore::open(&store, &key).unwrap();
            writer
                .append(
                    1,
                    &broca_wal::WalRecord::StepStarted {
                        step_id: broca_protocol::StepId(1),
                    },
                    true,
                )
                .unwrap();
        }
        let path = store.wal_path(&key);
        let before = std::fs::read(&path).unwrap();
        match kind {
            "moving" => {
                broca_wal::migration::freeze(
                    &root,
                    &path,
                    &broca_wal::migration::Freeze {
                        cut_id: "cut".into(),
                        end_offset: before.len() as u64,
                        lineage_id: "lineage".into(),
                    },
                )
                .unwrap()
                .unwrap();
            }
            "moved" => {
                broca_wal::migration::atomic_json(
                    &root,
                    &path.with_extension("moved"),
                    &broca_wal::migration::Moved {
                        cut_id: "cut".into(),
                        holder: "remote".into(),
                        lineage_id: "lineage".into(),
                    },
                )
                .unwrap();
            }
            "activate" => {
                let reference = cortexkit_role_llm_runner::session_ref(
                    &cortexkit_role_llm_runner::SessionIdentity {
                        runner: "broca",
                        harness: "runner",
                        session: "moved",
                    },
                );
                broca_wal::migration::atomic_json(&root, &root.join("agent-data/cuts/cut.json"),&serde_json::json!({"refs":[{"ref_kind":"broca-session","ref":reference}],"intent":{"op":"activate","request":{}}})).unwrap();
            }
            _ => unreachable!(),
        }
        let out = root.join("export");
        let direct = broca_module_serve::cache_dump::DirectBind {
            project_root: key.project_root.clone(),
            harness: key.harness.clone(),
        };
        let error = broca_module_serve::cache_dump::export_with_bind(
            &root,
            &["moved".into()],
            None,
            &out,
            Some(&direct),
        )
        .err()
        .expect("move must refuse cache dump");
        assert!(error.contains("session_mov"), "{error}");
        assert!(!out.exists());
        assert_eq!(std::fs::read(path).unwrap(), before);
    }
}
