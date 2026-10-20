//! End-to-end tests proving that WAL corruption fails loud through the run loop,
//! while a torn tail is safely discarded and resumes cleanly.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_core::{
    provider::Provider, run_loop, AllowAll, FileLeaseStore, FileWalStore, RunError, RunRequest,
    StubReply, StubToolPlane,
};
use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput},
    provider::{NormalizedEvent, ProviderControl},
    request::{CallOptions, ExecutionMode, ModelHandle, StopCondition, Tool, ToolChoice},
    FinishReason, GenerationConfig, MessageId, RunId, StepId, ToolCallId, Usage,
};
use serde_json::json;
use tokio::sync::mpsc;

#[derive(Clone)]
struct RecordingProvider {
    scripts: Arc<Vec<Vec<NormalizedEvent>>>,
    sent: Arc<Mutex<HashMap<usize, Vec<u8>>>>,
}

impl RecordingProvider {
    fn new(scripts: Vec<Vec<NormalizedEvent>>) -> Self {
        Self {
            scripts: Arc::new(scripts),
            sent: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl Provider for RecordingProvider {
    async fn step(&self, step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        let bytes = self.render_request(opts);
        let idx = (step_id.0 - 1) as usize;
        self.sent.lock().unwrap().insert(idx, bytes);

        let (tx, rx) = mpsc::channel(64);
        let script = self.scripts.get(idx).cloned().unwrap_or_default();
        tokio::spawn(async move {
            for ev in script {
                let _ = tx.send(ev).await;
            }
        });
        rx
    }
}

fn scripts() -> Vec<Vec<NormalizedEvent>> {
    vec![
        vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: MessageId("m1".into()),
                    content: vec![
                        ContentBlock::ToolCall(ToolCall {
                            thought_signature: None,
                            tool_call_id: ToolCallId("t1".into()),
                            tool_name: "edit".into(),
                            input: json!({"f": "a"}),
                        }),
                        ContentBlock::ToolCall(ToolCall {
                            thought_signature: None,
                            tool_call_id: ToolCallId("t2".into()),
                            tool_name: "edit".into(),
                            input: json!({"f": "b"}),
                        }),
                    ],
                },
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage {
                    input_tokens: Some(100),
                    output_tokens: Some(10),
                    ..Default::default()
                },
                finish_reason: FinishReason::ToolCalls,
                retries_used: 0,
            }),
        ],
        vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: MessageId("m2".into()),
                    content: vec![ContentBlock::Text {
                        text: "done".into(),
                    }],
                },
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage {
                    input_tokens: Some(50),
                    output_tokens: Some(4),
                    ..Default::default()
                },
                finish_reason: FinishReason::Stop,
                retries_used: 0,
            }),
        ],
    ]
}

fn scripts_with_nondispatched_second_call() -> Vec<Vec<NormalizedEvent>> {
    let mut scripts = scripts();
    let NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) =
        &mut scripts[0][0]
    else {
        panic!("first script event must be the assistant tool-call message");
    };
    let ContentBlock::ToolCall(call) = &mut message.content[1] else {
        panic!("second assistant block must be a tool call");
    };
    // The request manifest declares only `edit`, so this call becomes a durable NoDispatch
    // after t1's dispatch intent and gives the corruption fixture a valid later frame.
    call.tool_name = "not-declared".into();
    scripts
}

fn request(run: &str, dir: &Path) -> RunRequest {
    RunRequest {
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
        run_id: RunId(run.into()),
        session: broca_protocol::BindIdentity::new(dir, "m9", "s1"),
        model: ModelHandle::new("stub", "m"),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "edit a and b".into(),
            }],
        }],
        tools: vec![Tool {
            name: "edit".into(),
            description: "edit a file".into(),
            input_schema: json!({"type": "object"}),
            module: None,
            execution_mode: ExecutionMode::Parallel,
        }],
        policy_allowed: ["edit".to_string()].into_iter().collect(),
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

fn make_plane() -> StubToolPlane {
    StubToolPlane::new().with_tool("edit", |c| {
        StubReply::Output(ToolOutput::Text {
            text: format!("edited-{}", c.tool_call_id.0),
        })
    })
}

async fn run_once(
    dir: &Path,
    provider: &RecordingProvider,
    plane: &StubToolPlane,
) -> Result<RunFinishReason, RunError> {
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let (tx, mut rx) = mpsc::channel(512);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let summary = run_loop(
        request("r", dir),
        provider,
        plane,
        &lease,
        &AllowAll,
        &wal,
        &tx,
    )
    .await?;
    drop(tx);
    collector.await.unwrap();
    Ok(summary.reason)
}

#[allow(dead_code)]
struct RawFrame {
    offset: usize,
    len: u32,
    ver: u8,
    seq: u64,
    fence: u64,
    digest: [u8; 32],
    payload_offset: usize,
    payload_len: usize,
    total_len: usize,
}

fn parse_raw_frames(bytes: &[u8]) -> Vec<RawFrame> {
    let mut frames = Vec::new();
    let mut offset = 0;
    while offset < bytes.len() {
        if bytes.len() - offset < 53 {
            break;
        }
        let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        let ver = bytes[offset + 4];
        let seq = u64::from_le_bytes(bytes[offset + 5..offset + 13].try_into().unwrap());
        let fence = u64::from_le_bytes(bytes[offset + 13..offset + 21].try_into().unwrap());
        let mut digest = [0u8; 32];
        digest.copy_from_slice(&bytes[offset + 21..offset + 53]);
        let total_len = 53 + len as usize;
        // Record frames: ordinary ones, and gated ones (a tagged session's records require
        // `runner-tags/v1`). Lineage metadata frames are skipped.
        if ver == broca_wal::FRAMING_VERSION || ver == broca_wal::GATED_FRAMING_VERSION {
            frames.push(RawFrame {
                offset,
                len,
                ver,
                seq,
                fence,
                digest,
                payload_offset: offset + 53,
                payload_len: len as usize,
                total_len,
            });
        }
        offset += total_len;
    }
    frames
}

#[tokio::test]
async fn test_flipped_payload_byte_fails_loud() {
    let base = broca_test_support::scratch_root()
        .join(format!("broca-wal-corrupt-1-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    // 1. Run reference run to completion.
    let provider = RecordingProvider::new(scripts());
    let plane = make_plane();
    let reason = run_once(&base, &provider, &plane).await.expect("run ok");
    assert_eq!(reason, RunFinishReason::Completed);
    assert_eq!(plane.exec_count(&ToolCallId("t1".into())), 1);
    assert_eq!(plane.exec_count(&ToolCallId("t2".into())), 1);

    // 2. Read WAL file.
    let wal_store = FileWalStore::new(base.join("wal"));
    let wal_path = wal_store.wal_path(&request("r", &base).session);
    let mut bytes = std::fs::read(&wal_path).unwrap();

    // 3. Parse frames and corrupt a committed (non-tail) record.
    let frames = parse_raw_frames(&bytes);
    assert!(
        frames.len() >= 2,
        "expected at least 2 frames, got {}",
        frames.len()
    );

    // Let's corrupt the first frame (RunStarted).
    let target_frame = &frames[0];
    // Flip a byte in the payload.
    let corrupt_idx = target_frame.payload_offset;
    bytes[corrupt_idx] ^= 0xff;

    // Write back.
    std::fs::write(&wal_path, &bytes).unwrap();

    // 4. Attempt to resume.
    let resume_provider = RecordingProvider::new(scripts());
    let resume_plane = make_plane();
    let res = run_once(&base, &resume_provider, &resume_plane).await;

    // Assert it fails loud with a WAL corruption error.
    match res {
        Err(RunError::Wal(msg)) => {
            assert!(
                msg.contains("wal corruption") || msg.contains("failed its digest"),
                "unexpected error message: {}",
                msg
            );
        }
        other => panic!("expected RunError::Wal, got {:?}", other),
    }

    // Assert no tools were executed during resume.
    assert_eq!(resume_plane.exec_count(&ToolCallId("t1".into())), 0);
    assert_eq!(resume_plane.exec_count(&ToolCallId("t2".into())), 0);

    let _ = std::fs::remove_dir_all(&base);
}

#[tokio::test]
async fn test_torn_tail_resumes_cleanly() {
    let base = broca_test_support::scratch_root()
        .join(format!("broca-wal-corrupt-2-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    // 1. Run reference run to completion.
    let provider = RecordingProvider::new(scripts());
    let plane = make_plane();
    let reason = run_once(&base, &provider, &plane).await.expect("run ok");
    assert_eq!(reason, RunFinishReason::Completed);

    // 2. Read WAL file.
    let wal_store = FileWalStore::new(base.join("wal"));
    let wal_path = wal_store.wal_path(&request("r", &base).session);
    let bytes = std::fs::read(&wal_path).unwrap();

    // 3. Parse frames and truncate the last frame mid-frame.
    let frames = parse_raw_frames(&bytes);
    assert!(!frames.is_empty());
    let last_frame = &frames[frames.len() - 1];

    // Truncate mid-frame: keep all previous frames, plus only a part of the last frame.
    // Let's keep only the first 10 bytes of the last frame.
    let truncate_len = last_frame.offset + 10;
    let truncated_bytes = &bytes[..truncate_len];

    // Write back.
    std::fs::write(&wal_path, truncated_bytes).unwrap();

    // 4. Attempt to resume.
    let resume_provider = RecordingProvider::new(scripts());
    let resume_plane = make_plane();
    let res = run_once(&base, &resume_provider, &resume_plane).await;

    // Assert it resumes cleanly (returns Ok).
    assert!(res.is_ok(), "expected clean resume, got {:?}", res);

    let _ = std::fs::remove_dir_all(&base);
}

#[tokio::test]
async fn test_missing_intent_gap_fails_closed_before_redispatch() {
    let base = broca_test_support::scratch_root().join(format!(
        "broca-wal-corrupt-missing-intent-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    // Produce a real barrier containing Intent(t1), then NoDispatch(t2). The completed reference
    // run proves t1 executed once; cutting the WAL back to that barrier simulates a crash before
    // its result became durable.
    let provider = RecordingProvider::new(scripts_with_nondispatched_second_call());
    let plane = make_plane();
    let reason = run_once(&base, &provider, &plane).await.expect("run ok");
    assert_eq!(reason, RunFinishReason::Completed);
    assert_eq!(plane.exec_count(&ToolCallId("t1".into())), 1);
    assert_eq!(plane.exec_count(&ToolCallId("t2".into())), 0);

    let wal_store = FileWalStore::new(base.join("wal"));
    let wal_path = wal_store.wal_path(&request("r", &base).session);
    let bytes = std::fs::read(&wal_path).unwrap();
    let frames = parse_raw_frames(&bytes);
    let records = broca_wal::replay_file(&wal_path, broca_wal::DEFAULT_MAX_READ_BYTES).unwrap();
    assert_eq!(frames.len(), records.len());
    let intent_index = records
        .iter()
        .position(|frame| {
            matches!(
                &frame.record,
                broca_wal::WalRecord::ToolDispatchIntent { tool_call_id, .. }
                    if tool_call_id.0 == "t1"
            )
        })
        .expect("t1 dispatch intent");
    let no_dispatch_index = records
        .iter()
        .position(|frame| {
            matches!(
                &frame.record,
                broca_wal::WalRecord::ToolNoDispatch { tool_call_id, .. }
                    if tool_call_id.0 == "t2"
            )
        })
        .expect("t2 no-dispatch terminal");
    assert_eq!(no_dispatch_index, intent_index + 1);

    let mut gapped = Vec::new();
    for (index, frame) in frames.iter().enumerate().take(no_dispatch_index + 1) {
        if index != intent_index {
            gapped.extend_from_slice(&bytes[frame.offset..frame.offset + frame.total_len]);
        }
    }
    std::fs::write(&wal_path, gapped).unwrap();

    let resume_provider = RecordingProvider::new(scripts_with_nondispatched_second_call());
    let result = run_once(&base, &resume_provider, &plane).await;
    match result {
        Err(RunError::Wal(message)) => assert!(
            message.contains("wal sequence gap"),
            "unexpected error message: {message}"
        ),
        other => panic!("expected sequence-gap RunError::Wal, got {other:?}"),
    }
    assert_eq!(
        plane.exec_count(&ToolCallId("t1".into())),
        1,
        "recovery must fail before a missing intent can redispatch t1"
    );

    let _ = std::fs::remove_dir_all(&base);
}

#[tokio::test]
async fn test_seq_regression_fails_loud() {
    let base = broca_test_support::scratch_root()
        .join(format!("broca-wal-corrupt-3-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    // 1. Run reference run to completion.
    let provider = RecordingProvider::new(scripts());
    let plane = make_plane();
    let reason = run_once(&base, &provider, &plane).await.expect("run ok");
    assert_eq!(reason, RunFinishReason::Completed);

    // 2. Read WAL file.
    let wal_store = FileWalStore::new(base.join("wal"));
    let wal_path = wal_store.wal_path(&request("r", &base).session);
    let bytes = std::fs::read(&wal_path).unwrap();

    // 3. Parse frames.
    let frames = parse_raw_frames(&bytes);
    assert!(
        frames.len() >= 3,
        "expected at least 3 frames, got {}",
        frames.len()
    );

    // Duplicate frame 2 in place. The prefix remains contiguous through seq 2, so the repeated
    // seq 2 reaches the existing NonMonotonic classification rather than the new gap check.
    let mut spliced_bytes = Vec::new();
    spliced_bytes
        .extend_from_slice(&bytes[frames[0].offset..frames[0].offset + frames[0].total_len]);
    for _ in 0..2 {
        spliced_bytes
            .extend_from_slice(&bytes[frames[1].offset..frames[1].offset + frames[1].total_len]);
    }
    spliced_bytes.extend_from_slice(&bytes[frames[2].offset..]);

    // Write back.
    std::fs::write(&wal_path, &spliced_bytes).unwrap();

    // 4. Attempt to resume.
    let resume_provider = RecordingProvider::new(scripts());
    let resume_plane = make_plane();
    let res = run_once(&base, &resume_provider, &resume_plane).await;

    // Assert it fails loud with a seq regression error.
    match res {
        Err(RunError::Wal(msg)) => {
            assert!(
                msg.contains("wal seq regressed"),
                "unexpected error message: {}",
                msg
            );
        }
        other => panic!("expected RunError::Wal, got {:?}", other),
    }

    let _ = std::fs::remove_dir_all(&base);
}

#[tokio::test]
async fn test_corrupted_header_fails_loud() {
    let base = broca_test_support::scratch_root()
        .join(format!("broca-wal-corrupt-4-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    // 1. Run reference run to completion.
    let provider = RecordingProvider::new(scripts());
    let plane = make_plane();
    let reason = run_once(&base, &provider, &plane).await.expect("run ok");
    assert_eq!(reason, RunFinishReason::Completed);

    // 2. Read WAL file.
    let wal_store = FileWalStore::new(base.join("wal"));
    let wal_path = wal_store.wal_path(&request("r", &base).session);
    let mut bytes = std::fs::read(&wal_path).unwrap();

    // 3. Parse frames.
    let frames = parse_raw_frames(&bytes);
    assert!(!frames.is_empty());

    // Let's corrupt the version byte of the first frame.
    // The version byte is at offset 4 of the frame.
    let target_frame = &frames[0];
    let ver_idx = target_frame.offset + 4;
    bytes[ver_idx] = 99; // invalid version

    // Write back.
    std::fs::write(&wal_path, &bytes).unwrap();

    // 4. Attempt to resume.
    let resume_provider = RecordingProvider::new(scripts());
    let resume_plane = make_plane();
    let res = run_once(&base, &resume_provider, &resume_plane).await;

    // Assert it fails loud with an unsupported version error.
    match res {
        Err(error @ RunError::Wal(_)) => {
            let RunError::Wal(msg) = &error else {
                unreachable!()
            };
            assert!(
                msg.contains("unsupported WAL framing version"),
                "unexpected error message: {}",
                msg
            );
            // Corruption keeps its transient admission class: only a newer writer, whose
            // bytes are intact, is permanent.
            assert_eq!(
                error.admission_error_class(),
                broca_protocol::ErrorClass::Transient
            );
        }
        other => panic!("expected RunError::Wal, got {:?}", other),
    }

    let _ = std::fs::remove_dir_all(&base);
}

/// Resuming a session whose WAL a newer build appended to refuses by the NewerWriter name, not
/// as corruption, and leaves the WAL exactly as it was.
///
/// The refusal is typed and classed permanent at admission: retrying the same send changes
/// nothing, because only placing a newer build can read the WAL. Classed transient, it would be
/// retried forever.
#[tokio::test]
async fn test_newer_writer_frame_refuses_resume_by_name() {
    let base = broca_test_support::scratch_root()
        .join(format!("broca-wal-newer-writer-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let provider = RecordingProvider::new(scripts());
    let plane = make_plane();
    let reason = run_once(&base, &provider, &plane).await.expect("run ok");
    assert_eq!(reason, RunFinishReason::Completed);

    let wal_store = FileWalStore::new(base.join("wal"));
    let wal_path = wal_store.wal_path(&request("r", &base).session);
    let last = broca_wal::replay_file(&wal_path, broca_wal::DEFAULT_MAX_READ_BYTES)
        .unwrap()
        .last()
        .unwrap()
        .clone();
    let frame = broca_wal::framing::encode_with_requires(
        last.seq + 1,
        last.fence,
        &["from-the-future/v1"],
        &last.record,
    )
    .unwrap();
    let mut bytes = std::fs::read(&wal_path).unwrap();
    bytes.extend_from_slice(&frame);
    std::fs::write(&wal_path, &bytes).unwrap();

    let resume_provider = RecordingProvider::new(scripts());
    let resume_plane = make_plane();
    match run_once(&base, &resume_provider, &resume_plane).await {
        Err(error @ RunError::NewerWriter { .. }) => {
            let RunError::NewerWriter { missing } = &error else {
                unreachable!()
            };
            assert_eq!(missing, &vec!["from-the-future/v1".to_string()]);
            assert_eq!(
                error.admission_error_class(),
                broca_protocol::ErrorClass::Permanent
            );
            let msg = error.to_string();
            assert!(msg.contains("from-the-future/v1"), "{msg}");
            assert!(
                msg.contains("a newer broca build wrote this session"),
                "{msg}"
            );
            assert!(!msg.contains("corruption"), "{msg}");
        }
        other => panic!("expected RunError::NewerWriter, got {:?}", other),
    }
    assert_eq!(std::fs::read(&wal_path).unwrap(), bytes);

    let _ = std::fs::remove_dir_all(&base);
}

#[tokio::test]
async fn test_oversized_length_fails_loud() {
    let base = broca_test_support::scratch_root()
        .join(format!("broca-wal-corrupt-5-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    // 1. Run reference run to completion.
    let provider = RecordingProvider::new(scripts());
    let plane = make_plane();
    let reason = run_once(&base, &provider, &plane).await.expect("run ok");
    assert_eq!(reason, RunFinishReason::Completed);

    // 2. Read WAL file.
    let wal_store = FileWalStore::new(base.join("wal"));
    let wal_path = wal_store.wal_path(&request("r", &base).session);
    let mut bytes = std::fs::read(&wal_path).unwrap();

    // 3. Parse frames.
    let frames = parse_raw_frames(&bytes);
    assert!(!frames.is_empty());

    // Let's corrupt the length of the first frame to be oversized.
    // The length is at offset 0..4 of the frame.
    let target_frame = &frames[0];
    let len_idx = target_frame.offset;
    // Set length to MAX_PAYLOAD_LEN + 1 (64 MiB + 1)
    let oversized_len = 64 * 1024 * 1024 + 1u32;
    bytes[len_idx..len_idx + 4].copy_from_slice(&oversized_len.to_le_bytes());

    // Write back.
    std::fs::write(&wal_path, &bytes).unwrap();

    // 4. Attempt to resume.
    let resume_provider = RecordingProvider::new(scripts());
    let resume_plane = make_plane();
    let res = run_once(&base, &resume_provider, &resume_plane).await;

    // Assert it fails loud with a payload too large error.
    match res {
        Err(RunError::Wal(msg)) => {
            assert!(
                msg.contains("exceeds the") && msg.contains("byte cap"),
                "unexpected error message: {}",
                msg
            );
        }
        other => panic!("expected RunError::Wal, got {:?}", other),
    }

    let _ = std::fs::remove_dir_all(&base);
}
