//! The session's image index: what a run's writes hand over is applied before the run hears its
//! write went through, an open starts from the saved index and reads no record twice, a session
//! without images keeps no index, and the records only the new storage has (an undo, a recovery
//! cut) change the rendered request as their rules say.

use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD, Engine as _};
use broca_protocol::caller::RunFinishReason;
use broca_protocol::content::{ContentBlock, MediaRef, Message, Role};
use broca_protocol::ids::MessageId;
use broca_protocol::request::ModelHandle;
use broca_session_loop::parts::{MemoryParts, Parts};
use broca_session_loop::Writer as _;
use broca_session_store::backend::{MemoryBackend, SessionAddr, SessionBackend as _};
use broca_session_store::codec::Digest;
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::record::{
    Assistant, Attempt, EntryFacts, Input, PartKind, PartRef, Placement, Record, RunEnd, RunStart,
    Steer, StepEnd, StepStart,
};
use broca_session_store::session::{NewInputMedia, Session};
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};

use super::{RunAppends, SessionIndex};

const ADDR: &str = "0123456789abcdef";

pub(crate) fn addr() -> SessionAddr {
    SessionAddr(ADDR.into())
}

pub(crate) fn triple() -> SessionTriple {
    SessionTriple {
        project_root: "/project".into(),
        harness: "store-actor".into(),
        session: "s1".into(),
    }
}

pub(crate) fn deployment() -> DeploymentKey {
    DeploymentKey::new(1, &[7; 32])
}

/// The media profile of a model that takes images.
fn image_profile() -> broca_provider::media_profile::MediaProfile {
    broca_provider::media_profile::select_media_profile(
        broca_provider::spec::WireFamilyId::AnthropicMessages,
        Some(broca_provider::media_profile::MediaQuirk::Anthropic),
        "m",
        broca_provider::spec::InputModalities::from_value(&json!(["text", "image"])),
        Some(200_000),
    )
}

/// The frozen render config of a model that takes images, as its provider freezes it.
fn image_render_config() -> Value {
    let mut config = broca_provider::families::frozen_config_for_family(
        broca_provider::spec::WireFamilyId::AnthropicMessages,
        "m",
    );
    config.media = Some(image_profile());
    serde_json::to_value(config).unwrap()
}

/// A 1x1 PNG.
const PNG: &str = "89504e470d0a1a0a0000000d4948445200000001000000010802000000907753de0000000c49444154789c63f8cfc0000003010100c9fe92ef0000000049454e44ae426082";

/// The bytes of image `n`: the 1x1 PNG with a text chunk naming `n` after its header, so each
/// `n` has its own digest and every one reads back as a PNG.
fn bytes(n: u32) -> Vec<u8> {
    let png: Vec<u8> = (0..PNG.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&PNG[at..at + 2], 16).unwrap())
        .collect();
    // The signature and the header chunk.
    let (head, rest) = png.split_at(8 + 25);
    let text = format!("n\0{n}");
    let mut chunk = b"tEXt".to_vec();
    chunk.extend(text.as_bytes());
    let mut out = head.to_vec();
    out.extend((text.len() as u32).to_be_bytes());
    out.extend(&chunk);
    out.extend(crc(&chunk).to_be_bytes());
    out.extend(rest);
    out
}

/// A PNG chunk's checksum.
fn crc(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xedb8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

fn reference(n: u32) -> MediaRef {
    let bytes = bytes(n);
    serde_json::from_value(json!({
        "kind": "image", "mime": "image/png", "byte_size": bytes.len(),
        "sha256": format!("{:x}", Sha256::digest(&bytes)), "width": 1, "height": 1,
    }))
    .unwrap()
}

pub(crate) fn said(words: &str) -> Message {
    Message::new(Role::User, vec![ContentBlock::Text { text: words.into() }])
}

/// A person's message with image `n` after its words, at block 1.
pub(crate) fn photo(words: &str, n: u32) -> Message {
    Message::new(
        Role::User,
        vec![
            ContentBlock::Text { text: words.into() },
            ContentBlock::Media(reference(n)),
        ],
    )
}

/// A session being written the way the actor serves a run's writes, with its image index.
pub(crate) struct Store {
    pub(crate) backend: &'static MemoryBackend,
    pub(crate) session: Session<&'static MemoryBackend>,
    pub(crate) index: SessionIndex,
    pub(crate) parts: MemoryParts,
    pub(crate) config: PartRef,
    tool_media: broca_session_loop::PendingMedia,
}

impl Store {
    pub(crate) async fn new() -> Self {
        let backend: &'static MemoryBackend = Box::leak(Box::default());
        let session = Session::create(backend, addr(), triple(), &deployment(), false)
            .await
            .unwrap();
        Self::over(backend, session).await
    }

    /// The store over `session`, its index opened as an actor opens it.
    pub(crate) async fn over(
        backend: &'static MemoryBackend,
        mut session: Session<&'static MemoryBackend>,
    ) -> Self {
        let mut parts = MemoryParts::default();
        let config = parts
            .put(PartKind::RenderConfig, &image_render_config())
            .unwrap();
        let mut index = SessionIndex::new(&session, Some(backend));
        index.open(&mut session, &parts).await;
        Self {
            backend,
            session,
            index,
            parts,
            config,
            tool_media: broca_session_loop::PendingMedia::default(),
        }
    }

    fn appends(&mut self) -> RunAppends<'_, &'static MemoryBackend> {
        RunAppends {
            session: &mut self.session,
            tool_media: &self.tool_media,
            index: &mut self.index,
            parts: &self.parts,
        }
    }

    pub(crate) async fn append(&mut self, records: Vec<Record>, barrier: bool) {
        self.appends().append(&records, barrier).await.unwrap();
    }

    /// A run whose input is `message`, with its image's bytes stored, and one step answering
    /// it. Returns the id of the input message.
    pub(crate) async fn run(&mut self, run: &str, message: Message, image: Option<u32>) -> String {
        let mid = self.session.message_id(2);
        let ordinal = self.session.conversation.placed + 1;
        let records = vec![
            start(run, vec![self.config.clone()]),
            input(run, Placement::RunInput, None, message),
            facts(ordinal, mid.clone()),
        ];
        let media = image
            .map(|n| NewInputMedia {
                record: 1,
                message: 0,
                block: 1,
                part: None,
                bytes: bytes(n),
            })
            .into_iter()
            .collect();
        self.appends()
            .append_input_media(&records, media, true)
            .await
            .unwrap();
        self.step(run, 1).await;
        self.append(vec![end(run)], true).await;
        mid
    }

    /// Model step `step` of `run`, answering in text.
    pub(crate) async fn step(&mut self, run: &str, step: u64) {
        self.append(vec![step_start(run, step)], true).await;
        self.append(vec![attempt(run, step)], true).await;
        let ordinal = self.session.conversation.placed + 1;
        let mid = self.session.message_id(2);
        self.append(
            vec![
                assistant(run, step),
                step_end(run, step, 0),
                facts(ordinal, mid),
            ],
            true,
        )
        .await;
    }

    /// The images the index holds, by image number, in capture order.
    fn captured(&self) -> Vec<(u32, Option<u64>, bool)> {
        let Some(index) = self.index.index() else {
            return Vec::new();
        };
        index
            .session()
            .facts()
            .captures
            .into_iter()
            .map(|capture| {
                let n = (0..10_000)
                    .find(|n| reference(*n).sha256 == capture.reference.sha256)
                    .expect("a test image");
                (n, capture.ordinal, capture.person)
            })
            .collect()
    }

    /// The request a run renders now for the session's transcript, under a model that takes
    /// images.
    fn render(&self) -> Result<String, String> {
        let opts: broca_protocol::request::CallOptions = serde_json::from_value(json!({
            "prompt": self.session.conversation.held_messages().unwrap(),
        }))
        .unwrap();
        self.index
            .for_run()
            .render(&image_render_config(), &opts, false, None)
            .map(|body| String::from_utf8(body).unwrap())
            .map_err(|error| {
                format!(
                    "{}: {}",
                    error.provider_code.unwrap_or_default(),
                    error.message
                )
            })
    }

    /// The names of the saved index objects the session holds.
    pub(crate) fn index_objects(&self) -> Vec<String> {
        self.backend
            .object_names(&addr())
            .into_iter()
            .filter(|name| name.ends_with("/mi"))
            .collect()
    }

    /// Seals the open log, as the run's writer does.
    pub(crate) async fn seal(&mut self) {
        self.index.absorb(&mut self.session, &self.parts);
        self.session.seal(None, 1_000).await.unwrap();
        self.index.absorb(&mut self.session, &self.parts);
    }
}

fn shows(body: &str, n: u32) -> bool {
    body.contains(&STANDARD.encode(bytes(n)))
}

/// A run's write is applied to the index before the run hears it went through, from what the
/// write handed over: no record is read back, so a write costs the same however long the log.
#[tokio::test]
async fn a_runs_writes_reach_the_index_before_it_hears_and_read_no_log() {
    let mut store = Store::new().await;
    for n in 0..20 {
        store
            .run(&format!("r{n}"), said("no image yet"), None)
            .await;
    }
    assert!(store.index.index().is_none(), "no capture, no index");
    let reads = store.backend.log_reads();
    store.run("photo", photo("look", 1), Some(1)).await;
    assert_eq!(store.backend.log_reads(), reads, "a write read the log");
    assert_eq!(store.captured(), vec![(1, Some(40), true)]);
    let body = store.render().unwrap();
    assert!(shows(&body, 1), "{body}");
}

/// A session that never captured an image keeps no index after 1,000 steps: none in memory,
/// none saved by a seal, and the store's fold keeps its scalars only.
#[tokio::test]
async fn a_session_without_images_holds_no_index_after_a_thousand_steps() {
    let mut store = Store::new().await;
    let mid = store.session.message_id(2);
    store
        .append(
            vec![
                start("r1", vec![store.config.clone()]),
                input("r1", Placement::RunInput, None, said("go")),
                facts(1, mid),
            ],
            true,
        )
        .await;
    for step in 1..=1_000 {
        store.step("r1", step).await;
    }
    assert!(store.index.index().is_none());
    assert_eq!(store.index.stored(), None, "0 index bytes");
    assert!(!store.session.media_fold().has_captured());
    store.seal().await;
    assert_eq!(store.index_objects(), Vec::<String>::new());
    assert!(store.index.broken().is_none(), "{:?}", store.index.broken());
}

/// A session with 500 images saves an index under 500 KB, with where each image's bytes are.
#[tokio::test]
async fn five_hundred_images_save_an_index_under_500_kb() {
    let mut store = Store::new().await;
    for n in 0..500 {
        store.run(&format!("r{n}"), photo("look", n), Some(n)).await;
    }
    store.seal().await;
    let names = store.index_objects();
    assert_eq!(names.len(), 1, "{names:?}");
    let saved = store
        .backend
        .get_object(&addr(), &names[0])
        .await
        .unwrap()
        .unwrap();
    assert!(saved.len() < 500 * 1024, "{} bytes", saved.len());
    let stored = store.index.stored().unwrap();
    assert_eq!(stored.captures.len(), 500);
    assert_eq!(stored.objects.len(), 500);
}

/// An open of a sealed session starts from its saved index and applies only what the open log's
/// records after it handed over: the index reads no record, and holds what it held at the close.
#[tokio::test]
async fn an_open_of_a_sealed_session_reads_no_record_twice() {
    let mut store = Store::new().await;
    store.run("r1", photo("look", 1), Some(1)).await;
    store.seal().await;
    store.run("r2", photo("and this", 2), Some(2)).await;
    let held = store.captured();
    let body = store.render().unwrap();
    let backend = store.backend;
    store.session.close().await.unwrap();

    let mut session = Session::load(backend, addr(), &triple(), &deployment())
        .await
        .unwrap();
    let reads = backend.log_reads();
    let mut index = SessionIndex::new(&session, Some(backend));
    index.open(&mut session, &store.parts).await;
    assert_eq!(backend.log_reads(), reads, "the open read records again");
    assert!(index.ready(), "the saved index carried the session");
    let reopened = Store {
        backend,
        session,
        index,
        parts: store.parts,
        config: store.config,
        tool_media: Default::default(),
    };
    assert_eq!(reopened.captured(), held);
    assert_eq!(reopened.render().unwrap(), body);
}

/// An undo removes the images of the messages from the one it returns to, and a request after it
/// renders the images before that message only.
#[tokio::test]
async fn an_undo_removes_the_images_from_the_message_it_returns_to() {
    let mut store = Store::new().await;
    store.run("r1", photo("look", 1), Some(1)).await;
    let second = store.run("r2", photo("and this", 2), Some(2)).await;
    assert_eq!(
        store.captured(),
        vec![(1, Some(0), true), (2, Some(2), true)]
    );
    store
        .session
        .revert(
            &second,
            vec![
                resend("r3", vec![store.config.clone()]),
                input("r3", Placement::RunInput, None, said("instead")),
                facts(3, "resent".into()),
            ],
            None,
            2_000,
        )
        .await
        .unwrap();
    store.index.absorb(&mut store.session, &store.parts);
    assert_eq!(store.captured(), vec![(1, Some(0), true)]);
    let body = store.render().unwrap();
    assert!(shows(&body, 1), "{body}");
    assert!(!shows(&body, 2), "the undone image is still sent: {body}");
}

/// A recovery cut removes what it may have lost: here an accepted steer's image, whose placing
/// boundary was lost. The index the session builds again after the cut doesn't hold it, and the
/// next request renders the images the session kept.
#[tokio::test]
async fn a_recovery_cut_removes_the_images_it_may_have_lost() {
    let mut store = Store::new().await;
    store.run("r1", photo("look", 1), Some(1)).await;
    let mid = store.session.message_id(2);
    store
        .append(
            vec![
                start("r2", vec![store.config.clone()]),
                input("r2", Placement::RunInput, None, said("go on")),
                facts(3, mid),
            ],
            true,
        )
        .await;
    store.append(vec![step_start("r2", 1)], true).await;
    let steered = store.session.next_seq();
    store
        .append(vec![steer(vec![photo("and this", 2)])], true)
        .await;
    store.append(vec![attempt("r2", 1)], true).await;
    let ordinal = store.session.conversation.placed + 1;
    let mid = store.session.message_id(2);
    store
        .append(
            vec![
                assistant("r2", 1),
                step_end("r2", 1, 0),
                facts(ordinal, mid),
            ],
            true,
        )
        .await;
    assert!(
        store.captured().contains(&(2, None, true)),
        "{:?}",
        store.captured()
    );
    // The boundary that places the steer, not yet acknowledged, is damaged.
    let from = store.session.open_log().1;
    let mid = store.session.message_id(1);
    store
        .append(
            vec![
                input("r2", Placement::Steer, Some(steered), photo("and this", 2)),
                facts(ordinal + 1, mid),
                step_start("r2", 2),
            ],
            false,
        )
        .await;
    let backend = store.backend;
    drop(store.session);
    damage(backend, from + 60).await;
    let recovered = Session::recover(backend, addr(), &triple(), &deployment(), 3_000)
        .await
        .unwrap();
    assert!(matches!(
        recovered,
        broca_session_store::session::Recovery::Recovered(_)
    ));

    let session = Session::load(backend, addr(), &triple(), &deployment())
        .await
        .unwrap();
    let mut store = Store::over(backend, session).await;
    // The recovery saved no index: the session's index is built once from its records.
    let through = store.index.start_build().expect("a build is due");
    let history = store
        .session
        .stored_records(backend)
        .media_history(through)
        .await
        .map_err(|error| format!("{error:?}"));
    let last = store.session.next_seq() - 1;
    store.index.built(history, last, &store.parts);
    store.index.absorb(&mut store.session, &store.parts);
    assert!(store.index.broken().is_none(), "{:?}", store.index.broken());
    assert_eq!(store.captured(), vec![(1, Some(0), true)]);
    let body = store.render().unwrap();
    assert!(shows(&body, 1), "{body}");
    assert!(!shows(&body, 2), "{body}");
}

/// Flips a byte of the open log at `at`, as storage damage would.
async fn damage(backend: &'static MemoryBackend, at: u64) {
    let (stored, _) = backend.read_head(&addr()).await.unwrap().unwrap();
    let (head, _) = broca_session_store::head::open(&stored, &deployment()).unwrap();
    let log = head.open_log.unwrap().change;
    let end = backend.log_end(&addr(), &log).await.unwrap();
    let mut bytes = backend.read_log(&addr(), &log, 0..end).await.unwrap();
    bytes[at as usize] ^= 0x40;
    backend.set_log(&addr(), &log, bytes);
}

/// A record the index can't take breaks it for the actor's life: a request holding an image is
/// refused, naming why, and one without renders as it always does.
#[tokio::test]
async fn a_record_the_index_cant_take_breaks_it_and_refuses_media_by_name() {
    let mut store = Store::new().await;
    store.run("r1", photo("look", 1), Some(1)).await;
    store
        .append(
            vec![Record::Event(broca_session_store::record::Event {
                name: "compaction_view_applied".into(),
                run_id: None,
                body: Some(json!({})),
            })],
            true,
        )
        .await;
    let reason = store.index.broken().expect("the index broke");
    assert!(reason.contains("first compaction protocol"), "{reason}");
    let refused = store.render().unwrap_err();
    assert!(refused.starts_with("store_media_unavailable"), "{refused}");
}

/// The start of the run an undo sends again, which names the send it repeats.
fn resend(run: &str, setup: Vec<PartRef>) -> Record {
    let mut start = start(run, setup);
    if let Record::RunStart(start) = &mut start {
        start.send_id = Some("resend".into());
    }
    start
}

fn model() -> ModelHandle {
    ModelHandle::new("anthropic", "claude")
}

fn steer(messages: Vec<Message>) -> Record {
    Record::Steer(Steer {
        input: messages,
        written_at_ms: None,
        tags_v1: None,
        receipt: None,
        media: Vec::new(),
        message_marks: None,
        named_speaker: None,
    })
}

pub(crate) fn start(run: &str, setup: Vec<PartRef>) -> Record {
    Record::RunStart(Box::new(RunStart {
        named_speaker: None,
        run_id: run.into(),
        model: model(),
        setup,
        renderer_version: 1,
        submission_id: None,
        send_id: None,
        request_identity_digest: None,
        ts_ms: None,
        written_at_ms: None,
        session_class: None,
        retention_secs: None,
        title: None,
        context_limit: None,
        service_tier: None,
        route_decision_id: None,
        compaction_plan_version: None,
        auth_selection: None,
        run_choices: None,
        origin: None,
        principal: None,
        scope: None,
        hooks: None,
        keep_warm: None,
        on_restart: None,
        seeded: None,
        refusal_schedule: None,
        imported_ids: None,
        message_marks: None,
    }))
}

fn end(run: &str) -> Record {
    Record::RunEnd(RunEnd {
        run_id: run.into(),
        reason: RunFinishReason::Completed,
        usage: None,
        ts_ms: None,
        retries_used: 0,
        interrupt_reason: None,
        error: None,
    })
}

pub(crate) fn input(
    run: &str,
    placement: Placement,
    receipt: Option<u64>,
    message: Message,
) -> Record {
    Record::Input(Input {
        run_id: run.into(),
        placement,
        message: Arc::new(message),
        written_at_ms: None,
        submission_id: None,
        notice: None,
        tags_v1: None,
        receipt_seq: receipt,
        pruned: Vec::new(),
        media: Vec::new(),
    })
}

pub(crate) fn facts(ordinal: u64, mid: String) -> Record {
    Record::EntryFacts(Arc::new(EntryFacts {
        send_id: None,
        named_speaker: None,
        ordinal,
        written_at_ms: Some(ordinal),
        tag_base: 0,
        notice: None,
        usage: None,
        generated_under: None,
        native: false,
        render_tags: false,
        clean_reply: false,
        mid,
        run_id: None,
        created_at_ms: None,
        delivery_mode: None,
        delivery_tool_call_id: None,
        client_message_id: None,
        sender: None,
        marks: None,
        no_display_facts: None,
        reasoning_summaries: Vec::new(),
        rich_ui: None,
        notices: Vec::new(),
        runner_tag_base: None,
        legacy_view_seq: None,
    }))
}

pub(crate) fn step_start(run: &str, step: u64) -> Record {
    Record::StepStart(StepStart {
        run_id: run.into(),
        step_id: step,
        mid: None,
    })
}

pub(crate) fn attempt(run: &str, step: u64) -> Record {
    Record::Attempt(Attempt {
        run_id: run.into(),
        step_id: step,
        attempt: 1,
        attempt_id: None,
        outcome: "complete".into(),
        http_status: Some(200),
        request_body_len: 0,
        request_body_sha256: Digest([0; 32]),
        response_body_len: 0,
        response_body_sha256: Digest([0; 32]),
        ts_ms: 1,
        sent_at_ms: None,
        first_content_at_ms: None,
        last_content_at_ms: None,
        transport: None,
        served_account_ref: None,
        billing: None,
        telemetry: None,
        reached_cache: None,
        requested_cache_ttl_ms: None,
    })
}

pub(crate) fn assistant(run: &str, step: u64) -> Record {
    let content = vec![ContentBlock::Text {
        text: "seen".into(),
    }];
    Record::Assistant(Assistant {
        run_id: run.into(),
        step_id: step,
        provider_message_id: MessageId(format!("answer{step}")),
        message: Arc::new(Message::assistant_from(content, model())),
        usage: None,
        written_at_ms: None,
        last_content_at_ms: None,
        budget_finalize: None,
        tags_v1: None,
        pruned: Vec::new(),
    })
}

pub(crate) fn step_end(run: &str, step: u64, calls: u32) -> Record {
    Record::StepEnd(StepEnd {
        run_id: run.into(),
        step_id: step,
        usage: None,
        finish_reason: None,
        retries_used: 0,
        service_tier_used: None,
        speed_used: None,
        call_count: calls,
        fallback: None,
        hooks: None,
        budget_finalize: None,
        total_usage: None,
        stop_met: None,
        limiting_stop_met: None,
        by_model: Vec::new(),
    })
}

/// A digest keys the objects map in a quarter of its text and gives that text back: a
/// lowercase hex SHA-256 as its bytes, any other text as it is. Keys order as their texts do,
/// which is the order the saved index lists its objects in.
#[test]
fn a_digest_key_gives_its_text_back_and_orders_as_its_text() {
    let texts: Vec<String> = vec![
        "00".repeat(32),
        "ff".repeat(32),
        "0a".repeat(32),
        "AB".repeat(32),
        "g".repeat(64),
        "short".into(),
    ];
    for text in &texts {
        assert_eq!(super::Digest::of(text).text(), *text);
    }
    assert!(matches!(
        super::Digest::of(&"0a".repeat(32)),
        super::Digest::Sha256(_)
    ));
    assert!(matches!(
        super::Digest::of(&"AB".repeat(32)),
        super::Digest::Text(_)
    ));
    let mut keys: Vec<_> = texts.iter().map(|text| super::Digest::of(text)).collect();
    keys.sort();
    let mut sorted = texts.clone();
    sorted.sort();
    assert_eq!(
        keys.iter().map(super::Digest::text).collect::<Vec<_>>(),
        sorted
    );
}
