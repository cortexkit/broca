//! Native media in an ordered steer must agree between a real run and WAL reconstruction.
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use base64::Engine;
use broca_core::{
    delivery_order::OrderedSend, media_request::MediaCheckpoint, provider::Provider,
    run::run_loop_on_writer, wal_writer::WalHandle, AllowAll, FileWalStore, RunControl, RunRequest,
    StartMode, SteerInbox, StubToolPlane, WalStore,
};
use broca_module_serve::wal_render::{render_step_with_media, OfflineMediaContext};
use broca_protocol::{
    provider::ProviderControl, BindIdentity, CallOptions, ContentBlock, DeliveryMode,
    DeliveryOrder, Message, NormalizedEvent, Role, RunId, StepId, SubmissionId, Usage,
};
use broca_provider::{media_request::LiveMedia, spec::WireFamilyId, wire::FrozenRenderConfig};
use broca_wal::WalRecord;
use serde_json::json;

use tokio::sync::{mpsc, Notify};

const PNG: &[u8] =
    include_bytes!("../../broca-provider/examples/media-live/fixtures/tool-image.png");

struct Model {
    frozen: FrozenRenderConfig,
    media: LiveMedia,
    sent: Mutex<Vec<Vec<u8>>>,
    entered: Notify,
    release: Notify,
    hold: bool,
}

impl Model {
    fn render(&self, opts: &CallOptions, commit: bool) -> Vec<u8> {
        self.media
            .render(
                &serde_json::to_value(self.frozen.for_step(opts)).unwrap(),
                opts,
                false,
                commit,
            )
            .unwrap()
    }
}

#[async_trait]
impl Provider for Model {
    fn render_config_blob(&self) -> serde_json::Value {
        serde_json::to_value(&self.frozen).unwrap()
    }

    fn renderer_version(&self) -> u32 {
        self.frozen.renderer_version
    }

    fn defer_media_tags(&self, _: bool) -> bool {
        true
    }

    fn render_request(&self, opts: &CallOptions) -> Vec<u8> {
        self.render(opts, false)
    }

    async fn step(&self, _: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.sent.lock().unwrap().push(self.render(opts, true));
        self.entered.notify_one();
        let first = self.sent.lock().unwrap().len() == 1;
        if self.hold && first {
            self.release.notified().await;
        }
        let (tx, rx) = mpsc::channel(2);
        if self.hold && first {
            tx.send(NormalizedEvent::Control(
                ProviderControl::AssistantMessage {
                    message: broca_protocol::AssistantMessage {
                        message_id: broca_protocol::MessageId("tool-call".into()),
                        content: vec![ContentBlock::ToolCall(broca_protocol::ToolCall {
                            tool_call_id: broca_protocol::ToolCallId("work-call".into()),
                            tool_name: "work".into(),
                            input: json!({}),
                            thought_signature: None,
                        })],
                    },
                },
            ))
            .await
            .unwrap();
        }
        tx.send(NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: if self.hold && first {
                broca_protocol::FinishReason::ToolCalls
            } else {
                broca_protocol::FinishReason::Stop
            },
            retries_used: 0,
        }))
        .await
        .unwrap();
        rx
    }
}

async fn ordered_photo_fixture(tag: &str, busy: bool) {
    let root = broca_test_support::scratch_root()
        .join(format!("ordered-media-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let wal = Arc::new(FileWalStore::new(root.join("wal")));
    let session = BindIdentity::new(root.join("project"), "test", "ordered-photo");
    let req = RunRequest::from_submission(
        serde_json::from_value(json!({
            "model":{"provider_module_id":"test","model_id":"gpt-5.3-codex"},
            "input":[{"role":"user","content":[{"type":"text","text":"work"}]}],
            "policy_allowed":["work"],
            "tools":[{"name":"work","description":"work","input_schema":{"type":"object"}}]
        }))
        .unwrap(),
        RunId("ordered-media".into()),
        session.clone(),
        SubmissionId("initial".into()),
    );
    let writer = WalHandle::spawn(wal.open(&session).unwrap(), 1).unwrap();
    let store = wal
        .uploads()
        .unwrap()
        .session_media_store(&session)
        .unwrap();
    let reference = broca_core::media_validation::validate_media(PNG, "image/png").unwrap();
    store
        .session(&session)
        .unwrap()
        .register_and_append(&reference, PNG, || Ok(()))
        .unwrap();
    let mut frozen =
        broca_provider::families::openai_responses_chatgpt_frozen_config("gpt-5.3-codex");
    frozen.media = Some(broca_provider::media_profile::select_media_profile(
        WireFamilyId::OpenAiResponsesChatgpt,
        None,
        "gpt-5.3-codex",
        broca_provider::spec::InputModalities::from_value(&json!(["text", "image"])),
        Some(128000),
    ));
    let tail_wal = wal.clone();
    let tail_session = session.clone();
    let head = writer.durable_head();
    let model = Model {
        media: LiveMedia::from_checkpoint(
            store,
            MediaCheckpoint::empty(),
            None,
            frozen.media.clone(),
            Box::new(move |after| {
                let through = head.load(std::sync::atomic::Ordering::Acquire);
                let range = tail_wal
                    .read_records_range(&tail_session, after, Some(through), None)
                    .map_err(|e| e.to_string())?;
                match range.into_records() {
                    broca_wal::WalRangeRecords::Complete(records) => Ok(records),
                    _ => Err("incomplete media tail".into()),
                }
            }),
        )
        .unwrap(),
        frozen,
        sent: Mutex::new(Vec::new()),
        entered: Notify::new(),
        release: Notify::new(),
        hold: busy,
    };
    let inbox = SteerInbox::new();
    let send = OrderedSend {
        admitted_at_ms: None,
        submission_id: SubmissionId("photo".into()),
        principal: None,
        order: DeliveryOrder {
            modes: vec![DeliveryMode::ToolResult, DeliveryMode::Steer],
            dedupe_key: None,
            expires_at_ms: None,
            template: None,
        },
        input: vec![Message::new(
            Role::User,
            vec![ContentBlock::Media(reference)],
        )],
    };
    let (tx, _rx) = mpsc::channel(256);
    let plane = StubToolPlane::new().with_canned_text("work", "done");
    let mut running = Box::pin(run_loop_on_writer(
        req,
        &writer,
        StartMode::FreshOrResume,
        &model,
        &plane,
        &AllowAll,
        &*wal,
        None,
        RunControl {
            steer: Some(inbox.clone()),
            ..Default::default()
        },
        &tx,
    ));
    if busy {
        tokio::select! {
            biased;
            result = &mut running => panic!("first request was not held: {result:?}"),
            _ = model.entered.notified() => {}
        }
    }
    assert!(matches!(
        inbox.accept_ordered(send, |_| {}),
        broca_core::OrderedAcceptance::Queued { superseded: None }
    ));
    model.release.notify_one();
    running.await.unwrap();
    writer.shutdown().await.unwrap();
    let records: Vec<_> = wal
        .read_records(&session)
        .unwrap()
        .into_iter()
        .map(|f| f.record)
        .collect();
    let sent = model.sent.lock().unwrap();
    assert_eq!(sent.len(), if busy { 2 } else { 1 });
    let photo_request = sent.last().unwrap();
    let body = std::str::from_utf8(photo_request).unwrap();
    assert!(
        body.contains("\"type\":\"input_image\""),
        "live request must carry the steered image: {body}"
    );
    assert!(body.contains(&format!(
        "data:image/png;base64,{}",
        base64::engine::general_purpose::STANDARD.encode(PNG)
    )));
    assert!(!body.contains("reason=provenance"));
    let step_at = records
        .iter()
        .rposition(|r| matches!(r, WalRecord::StepStarted { .. }))
        .unwrap();
    assert!(matches!(
        &records[step_at - 1],
        WalRecord::SendDelivered {
            mode: DeliveryMode::Steer,
            ..
        }
    ));
    let config = records
        .iter()
        .find_map(|r| match r {
            WalRecord::RunStarted { config, .. } => Some(config),
            _ => None,
        })
        .unwrap();
    let step = match &records[step_at] {
        WalRecord::StepStarted { step_id, .. } => step_id.0,
        _ => unreachable!(),
    };
    let (_, _, offline) = render_step_with_media(
        &OfflineMediaContext {
            state_root: &root,
            identity: &session,
            class: broca_core::media_store::MediaStoreClass::NonDurable,
            lineage: None,
        },
        &records[..=step_at],
        config,
        "ordered-media",
        step,
        None,
    )
    .unwrap();
    assert_eq!(*photo_request, offline);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn ordered_steer_photo_is_native_live_and_offline() {
    ordered_photo_fixture("first", false).await;
}

#[tokio::test]
async fn ordered_steer_photo_into_busy_run_is_native_live_and_offline() {
    ordered_photo_fixture("busy", true).await;
}
