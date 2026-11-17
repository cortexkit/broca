//! Requests holding media, rendered for a session kept in the new storage: every request is
//! pinned byte for byte.
//!
//! The executor installs a media renderer as the module's executor does, from the session's
//! media fold and its own media objects (see `store_media`). The scripted model renders each
//! request through the renderer it was given, as the transport does, and keeps the bytes, or the
//! refusal when a request couldn't be rendered and so was never sent.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use broca_core::provider::{MediaRequestRenderer, Provider};
use broca_core::{
    AllowAll, BatchCtx, DispatchResult, FileLeaseStore, OutputEvent, RunError, StubToolPlane,
    ToolManifest, ToolPlane, ToolPlaneError,
};
use broca_protocol::content::{AssistantMessage, ContentBlock, ToolOutput, ToolOutputPart};
use broca_protocol::provider::{NormalizedEvent, ProviderControl};
use broca_protocol::{BindIdentity, FinishReason, MessageId, SubmissionRequest, ToolCall, Usage};
use serde_json::{json, Value};
use tokio::sync::mpsc;

use super::media::{base64, bytes_of, image_profile, image_render_config, photo, Side, PNG, WIDE};
use crate::actor::SendOutcome;
use crate::executor::{RunExecutor, RunKind, StoreRunEnd, StoreRunError, StoreRunJob};
use crate::registry::LineageRegistry;
use broca_session_store::keys::Cipher;

cipher_tests!(
    a_photo_in_the_input_renders_alike,
    a_photo_in_a_tool_result_renders_alike,
    a_queued_photo_renders_alike,
    a_steered_photo_renders_alike,
    a_run_after_a_restart_renders_an_earlier_photo_alike,
    a_tool_image_renders_alike_after_a_restart,
    an_input_photo_and_a_tool_image_render_offline_as_sent,
    a_photo_whose_bytes_cant_be_read_refuses_alike_and_nothing_is_sent,
    a_run_after_a_view_drops_a_photo_renders_alike,
    a_view_inserting_an_image_renders_alike,
    a_setup_covering_a_photo_teaches_its_id_without_a_notice,
    a_photo_between_numbered_messages_renders_alike,
    an_ordered_steer_photo_lands_with_its_bytes_alike,
    an_idle_start_photo_starts_its_run_and_renders_alike,
    an_ordered_photo_no_mode_can_carry_is_refused_alike,
);

/// What a side's model was asked to send, in order: each request's body, or the code of the
/// refusal that kept it from being sent.
type Sent = Arc<Mutex<Vec<Result<Vec<u8>, String>>>>;

/// The scripted model of one run, rendering each request as the transport renders it.
struct Rendering {
    script: Arc<super::super::tests::Gated>,
    renderer: RwLock<Option<Arc<MediaRequestRenderer>>>,
    render_tags: AtomicBool,
    sent: Sent,
}

impl Rendering {
    fn new(script: &Arc<super::super::tests::Gated>, sent: &Sent) -> Self {
        Self {
            script: Arc::clone(script),
            renderer: RwLock::new(None),
            render_tags: AtomicBool::new(false),
            sent: Arc::clone(sent),
        }
    }
}

fn refusal(code: String) -> broca_protocol::ProviderError {
    broca_protocol::ProviderError {
        class: broca_protocol::ErrorClass::Permanent,
        message: code.clone(),
        provider_code: Some(code),
        provider_details: None,
        cause: None,
        provider_subcode: None,
        refusal: None,
        status: None,
        retry_after_secs: None,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
        account_continue: None,
    }
}

#[async_trait]
impl Provider for Rendering {
    fn render_config_blob(&self) -> Value {
        image_render_config()
    }

    fn render_config_blob_for_version(&self, _renderer_version: u32) -> Value {
        image_render_config()
    }

    fn supports_media_renderer(&self) -> bool {
        true
    }

    fn set_media_request_renderer(&self, renderer: Arc<MediaRequestRenderer>) {
        *self.renderer.write().unwrap() = Some(renderer);
    }

    fn defer_media_tags(&self, render_tags: bool) -> bool {
        self.render_tags.store(render_tags, Ordering::Relaxed);
        self.renderer.read().unwrap().is_some()
    }

    async fn step(
        &self,
        step_id: broca_protocol::StepId,
        opts: &broca_protocol::request::CallOptions,
    ) -> mpsc::Receiver<NormalizedEvent> {
        let config = image_render_config();
        let renderer = self.renderer.read().unwrap().clone();
        let rendered = match renderer {
            Some(renderer) => renderer(
                &config,
                opts,
                self.render_tags.load(Ordering::Relaxed),
                true,
                None,
            )
            .map_err(|error| error.provider_code.unwrap_or(error.message)),
            None => {
                let frozen: broca_provider::wire::FrozenRenderConfig =
                    serde_json::from_value(config).unwrap();
                Ok(broca_provider::registry::wire_family(frozen.family_id)
                    .unwrap()
                    .render(&frozen, opts))
            }
        };
        let refused = rendered.as_ref().err().cloned();
        self.sent.lock().unwrap().push(rendered);
        if let Some(code) = refused {
            let (events, receiver) = mpsc::channel(1);
            let _ = events
                .send(NormalizedEvent::Control(ProviderControl::Error {
                    error: refusal(code),
                    retries_used: 0,
                }))
                .await;
            return receiver;
        }
        self.script.step(step_id, opts).await
    }
}

/// The tool plane of both sides: `photo` answers with an image captured for its call through
/// the batch's capture, as a tool module's image reaches the session; `read` answers text;
/// `wait` answers once the test releases the hold its input names (see [`ToolHold`]).
struct PhotoPlane(StubToolPlane);

/// A `wait` call the test holds, so it can take the state root while the call runs, as a crash
/// finds it. A call names its hold by its input's `hold` member.
#[derive(Default)]
struct ToolHold {
    /// How many calls reached the hold.
    reached: std::sync::atomic::AtomicUsize,
    /// Lets the waiting call answer.
    released: tokio::sync::Notify,
}

/// The holds `wait` calls name, by name.
static HOLDS: Mutex<std::collections::BTreeMap<String, Arc<ToolHold>>> =
    Mutex::new(std::collections::BTreeMap::new());

/// A hold named `name`, which a `wait` call naming it waits on.
fn tool_hold(name: &str) -> Arc<ToolHold> {
    Arc::clone(HOLDS.lock().unwrap().entry(name.to_owned()).or_default())
}

impl PhotoPlane {
    fn new() -> Self {
        Self(StubToolPlane::new().with_canned_text("read", "file contents"))
    }
}

#[async_trait]
impl ToolPlane for PhotoPlane {
    async fn batch_started(
        &self,
        ctx: &BatchCtx,
        calls: &[ToolCall],
        manifest: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        self.0.batch_started(ctx, calls, manifest).await
    }

    async fn dispatch(&self, ctx: &BatchCtx, call: &ToolCall) -> DispatchResult {
        if call.tool_name == "wait" {
            let input = call.input.parse();
            let name = input["hold"].as_str().expect("a wait call names its hold");
            let hold = tool_hold(name);
            hold.reached.fetch_add(1, Ordering::SeqCst);
            hold.released.notified().await;
            return DispatchResult::Completed(broca_protocol::ToolResult {
                tool_call_id: call.tool_call_id.clone(),
                output: ToolOutput::Text {
                    text: "waited".into(),
                },
                is_error: false,
            });
        }
        if call.tool_name != "photo" {
            return self.0.dispatch(ctx, call).await;
        }
        let capture = ctx
            .media_capture
            .as_ref()
            .expect("the run's writer captures tool media");
        let media = capture
            .capture(
                &ctx.batch_id,
                &call.tool_call_id,
                bytes_of(PNG),
                "image/png".into(),
            )
            .await
            .expect("the image is captured");
        DispatchResult::Completed(broca_protocol::ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: ToolOutput::Parts {
                parts: vec![
                    ToolOutputPart::Text {
                        text: "the screen".into(),
                    },
                    ToolOutputPart::Media { media },
                ],
            },
            is_error: false,
        })
    }

    async fn batch_finished(&self, ctx: &BatchCtx) -> Result<(), ToolPlaneError> {
        self.0.batch_finished(ctx).await
    }
}

/// Runs both sides' runs, each installing its media renderer as the module's executor does.
struct RenderExecutor {
    script: Arc<super::super::tests::Gated>,
    sent: Sent,
    /// The compaction provider the side's runs are offered, when its sessions are planned.
    compactor: Option<Arc<super::controls::Compactor>>,
}

/// The new storage's renderer, installed as the module's executor installs it for a store run
/// that may carry media: ready once the session's image index is, each model call waiting for it.
fn install_store(
    job_media: Option<crate::store_media::StoreRunMediaWait>,
    holds_media: bool,
    tools: bool,
    provider: &Rendering,
) -> Result<Option<Arc<dyn broca_session_loop::run::ModelCallReady>>, RunError> {
    if !(holds_media || tools) {
        return Ok(None);
    }
    let media = job_media.ok_or_else(|| RunError::Wal("store_media_unavailable".into()))?;
    let lazy = crate::store_media::LazyRenderer::new(media, Box::new(|media| Ok(media.renderer())));
    provider.set_media_request_renderer(lazy.renderer());
    Ok(Some(lazy))
}

#[async_trait]
impl RunExecutor for RenderExecutor {
    async fn media_profile(
        &self,
        _request: &SubmissionRequest,
    ) -> Result<Option<Box<broca_provider::media_profile::MediaProfile>>, RunError> {
        Ok(Some(Box::new(image_profile())))
    }

    fn compaction_plane(&self) -> Option<Arc<dyn broca_core::CompactionPlane>> {
        self.compactor
            .clone()
            .map(|compactor| compactor as Arc<dyn broca_core::CompactionPlane>)
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
            // it installs no media renderer and waits for none of the session's media state.
            let ready = match mode {
                crate::executor::StoreRunMode::Seal(_) => None,
                _ => install_store(media, holds_media, !req.tools.is_empty(), &provider)
                    .map_err(StoreRunError::Admission)?,
            };
            let tools = PhotoPlane::new();
            let now = || 1;
            let seams = broca_session_loop::Seams {
                person_steers: None,
                provider: &provider,
                tools: &tools,
                bind: req.session.clone(),
                out: Some(&out),
                cancel: control.cancel.clone().unwrap_or_default(),
                stop: Default::default(),
                held: None,
                approval: &AllowAll,
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

/// A module over the state root `dir` whose runs render their requests into `sent`. Sessions
/// start in the new storage; `store` must say so.
async fn registry(
    dir: &std::path::Path,
    store: bool,
    steps: Vec<Vec<NormalizedEvent>>,
    gate: Option<Arc<tokio::sync::Notify>>,
    sent: &Sent,
    compactor: Option<Arc<super::controls::Compactor>>,
    cipher: Cipher,
) -> LineageRegistry {
    assert!(store, "sessions start in the new storage");
    broca_core::media_store::create_media_directories(dir).unwrap();
    let executor: Arc<dyn RunExecutor> = Arc::new(RenderExecutor {
        script: Arc::new(super::super::tests::Gated {
            inner: broca_core::provider::StubProvider::new(steps),
            gate,
            render_config: Some(image_render_config()),
            notes_sends: false,
            hold: None,
        }),
        sent: Arc::clone(sent),
        compactor: compactor.clone(),
    });
    let runtime = crate::store_open::StoreRuntime::open(
        dir,
        broca_session_store::keys::DeploymentKey::new(1, &[7; 32]).creating(cipher),
        Arc::clone(&executor),
        super::IDLE,
    )
    .await
    .unwrap();
    let runtime = Arc::new(runtime);
    if let Some(compactor) = compactor {
        let key = BindIdentity::new(dir.join("project"), "store-actor", "s1");
        let runtime = Arc::downgrade(&runtime);
        compactor.reads(Arc::new(move || {
            let runtime = runtime.upgrade().expect("the session runtime is open");
            let key = key.clone();
            Box::pin(async move {
                let view = runtime.current_view(&key).await.unwrap().unwrap();
                crate::store_entries::all_entries(view.as_ref())
                    .await
                    .unwrap()
            })
        }));
    }
    LineageRegistry::new(Arc::new(
        crate::opener::DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            executor,
        )
        .with_idle_ttl(super::IDLE)
        .with_store_runtime(runtime),
    ))
}

/// One side: a module and what its model was asked to send.
struct Rendered {
    side: Side,
    sent: Sent,
}

impl Rendered {
    async fn new(
        store: bool,
        steps: Vec<Vec<NormalizedEvent>>,
        gated: bool,
        cipher: Cipher,
    ) -> Self {
        Self::with_compactor(store, steps, gated, None, cipher).await
    }

    /// [`Rendered::new`], its runs offered `compactor` as their compaction provider.
    async fn with_compactor(
        store: bool,
        steps: Vec<Vec<NormalizedEvent>>,
        gated: bool,
        compactor: Option<Arc<super::controls::Compactor>>,
        cipher: Cipher,
    ) -> Self {
        use std::sync::atomic::AtomicU64;
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = broca_test_support::scratch_root().join(format!(
            "store-media-render-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let gate = gated.then(|| Arc::new(tokio::sync::Notify::new()));
        let sent = Sent::default();
        let registry = registry(&dir, store, steps, gate.clone(), &sent, compactor, cipher).await;
        let key = BindIdentity::new(dir.join("project"), "store-actor", "s1");
        Self {
            side: Side {
                dir,
                registry,
                key,
                gate,
                cipher,
                caller: Default::default(),
            },
            sent,
        }
    }

    /// The side restarted on its state root, as a process starting there opens it, scripted by
    /// `steps`.
    async fn restarted(mut self, store: bool, steps: Vec<Vec<NormalizedEvent>>) -> Self {
        assert!(self.side.registry.stop_and_seal().await);
        let cipher = self.side.cipher;
        self.side.registry =
            registry(&self.side.dir, store, steps, None, &self.sent, None, cipher).await;
        self
    }

    fn sent(&self) -> Vec<Result<Vec<u8>, String>> {
        self.sent.lock().unwrap().clone()
    }
}

fn text_step(id: &str, text: &str) -> Vec<NormalizedEvent> {
    super::super::tests::text_step(id, text)
}

/// A step that calls the `photo` tool.
fn photo_step(id: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(id.into()),
                content: vec![ContentBlock::ToolCall(broca_protocol::content::ToolCall {
                    thought_signature: None,
                    tool_call_id: broca_protocol::ToolCallId("c1".into()),
                    tool_name: "photo".into(),
                    input: json!({}).into(),
                })],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(100),
                output_tokens: Some(20),
                ..Usage::default()
            },
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ]
}

/// A text send by core under its head scope, as a photo send is, declaring no tools: a scoped
/// session declares tools only through a plan.
fn text(text: &str) -> SubmissionRequest {
    let mut sent = super::super::tests::request(text);
    super::scoped_by_core(&mut sent);
    sent
}

/// An unscoped send declaring the `photo` tool. A run that may call a tool may get an image
/// back, so both sides install their media renderer for it.
fn with_photo_tool(text: &str) -> SubmissionRequest {
    serde_json::from_value(json!({
        "model": {"provider_module_id": "stub", "model_id": "m"},
        "input": [{"role": "user", "content": [{"type": "text", "text": text}]}],
        "tools": [{
            "name": "photo",
            "description": "shows the screen",
            "input_schema": {"type": "object"},
        }],
        "policy_allowed": ["photo"],
        "stop_when": [{"type": "step_count", "n": 3}],
    }))
    .unwrap()
}

/// A side started by a text send, its run finished, or still in its first model step when
/// `gated`. Its session is in mode `cipher`.
async fn both(steps: Vec<Vec<NormalizedEvent>>, gated: bool, cipher: Cipher) -> Rendered {
    both_from(steps, gated, text("first"), cipher).await
}

/// [`both`], started by the send `first`.
async fn both_from(
    steps: Vec<Vec<NormalizedEvent>>,
    gated: bool,
    first: SubmissionRequest,
    cipher: Cipher,
) -> Rendered {
    let side = Rendered::new(true, steps, gated, cipher).await;
    let sent = side.side.send("r1", first).await;
    assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
    assert_eq!(side.side.stored_mode(), cipher);
    if !gated {
        side.side.idle().await;
    }
    side
}

/// Pins every request the side's model was asked to send, and checks that the last one sent
/// the image's bytes.
fn sent_alike(store: &Rendered, when: &str) {
    let new = store.sent();
    sent_pinned(&new, when);
    let last = new
        .last()
        .expect("a request")
        .as_ref()
        .expect("it was sent");
    let image = base64(&bytes_of(PNG));
    assert!(
        String::from_utf8_lossy(last).contains(&image),
        "{when}: the last request carries the image's bytes"
    );
}

/// Pins how many requests were sent and each one: its body as text, or why it wasn't sent.
fn sent_pinned(sent: &[Result<Vec<u8>, String>], when: &str) {
    broca_test_support::pin!(format!("{when}: how many requests"), sent.len());
    for (index, request) in sent.iter().enumerate() {
        match request {
            Ok(body) => broca_test_support::pin!(
                format!("{when}: request {index}"),
                broca_test_support::pins::Text(String::from_utf8_lossy(body).into_owned())
            ),
            Err(refused) => {
                broca_test_support::pin!(format!("{when}: request {index}"), refused)
            }
        }
    }
}

/// Uploads the image on `side` and sends it as `delivery`.
async fn send_photo(
    side: &Rendered,
    run_id: &str,
    delivery: broca_protocol::Delivery,
) -> SendOutcome {
    send_image(side, run_id, delivery, PNG).await
}

/// Uploads the image `hex` names on `side` and sends it as `delivery`.
async fn send_image(
    side: &Rendered,
    run_id: &str,
    delivery: broca_protocol::Delivery,
    hex: &str,
) -> SendOutcome {
    let upload = side.side.uploaded(&bytes_of(hex)).await;
    side.side
        .deliver(run_id, photo("look", &[&upload]), delivery)
        .await
}

fn three_texts() -> Vec<Vec<NormalizedEvent>> {
    vec![
        text_step("a1", "hello"),
        text_step("a2", "seen"),
        text_step("a3", "noted"),
    ]
}

/// A photo in the user's input renders alike, and so does the next run's request, whose history
/// holds it.
async fn a_photo_in_the_input_renders_alike(cipher: Cipher) {
    let store = both(three_texts(), false, cipher).await;
    for side in [&store] {
        let sent = send_photo(side, "r2", broca_protocol::Delivery::Queue).await;
        assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
        side.side.idle().await;
    }
    sent_alike(&store, "the photo send's request");
    for side in [&store] {
        side.side.send("r3", text("and now?")).await;
        side.side.idle().await;
    }
    sent_alike(&store, "a later run holding the photo");
}

/// A photo a tool returned renders alike in the request after its result.
async fn a_photo_in_a_tool_result_renders_alike(cipher: Cipher) {
    let steps = vec![
        text_step("a1", "hello"),
        photo_step("a2"),
        text_step("a3", "I see it"),
    ];
    let store = both_from(steps, false, with_photo_tool("first"), cipher).await;
    for side in [&store] {
        side.side.send("r2", with_photo_tool("show me")).await;
        side.side.idle().await;
    }
    sent_alike(&store, "the request after the tool's image");
}

/// A queued photo renders alike when its run starts.
async fn a_queued_photo_renders_alike(cipher: Cipher) {
    let store = both(three_texts(), true, cipher).await;
    for side in [&store] {
        let sent = send_photo(side, "r2", broca_protocol::Delivery::Queue).await;
        assert!(matches!(sent, SendOutcome::Pending { .. }), "{sent:?}");
        side.side.release_to_idle().await;
    }
    sent_alike(&store, "the queued photo's run");
}

/// A photo steered into a running run renders alike at the step that places it. The run steered
/// into started from a photo send, so both sides render its requests with their media renderer.
async fn a_steered_photo_renders_alike(cipher: Cipher) {
    let store = both(three_texts(), true, cipher).await;
    for side in [&store] {
        side.side.release_to_idle().await;
        let sent = send_photo(side, "r2", broca_protocol::Delivery::Queue).await;
        assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
        let sent = send_image(side, "r3", broca_protocol::Delivery::Steer, WIDE).await;
        assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
        side.side.release_to_idle().await;
    }
    let last = store.sent().last().cloned().unwrap().unwrap();
    assert!(
        String::from_utf8_lossy(&last).contains(&base64(&bytes_of(WIDE))),
        "the step after the steer carries the steered image"
    );
    sent_alike(&store, "the step that places the steered photo");
}

/// Uploads the image `hex` names on `side` and sends it with the ordered delivery list `modes`,
/// as the side's caller sends it.
async fn send_ordered_image(
    side: &Rendered,
    run_id: &str,
    modes: &[&str],
    hex: &str,
) -> SendOutcome {
    let upload = side.side.uploaded(&bytes_of(hex)).await;
    let key = crate::actor::SendKey::Minted;
    side.side
        .registry
        .route_submission_with_order(
            &side.side.key,
            broca_protocol::RunId(run_id.into()),
            side.side
                .caller
                .shape(run_id, &key, photo("look", &[&upload])),
            RunKind::FreshOrResume,
            key,
            false,
            broca_protocol::Delivery::Queue,
            None,
            None,
            None,
            None,
            None,
            false,
            Some(serde_json::from_value(json!({ "modes": modes })).unwrap()),
        )
        .await
        .expect("the session opens")
}

/// A photo sent with the ordered list `[tool_result, steer]` while a run's model step is in
/// flight is queued alike. A tool result can't carry a photo, so it lands as a steer at the
/// run's next step boundary (the one a plain steer sent alongside makes the run take). Both
/// storages write that boundary in the same order: the run's start, the send's delivery, then
/// the step's start.
///
/// That step sends the delivered photo's bytes, never a `provenance` marker in their place: the
/// delivered copy points at the bytes the queued record stored.
async fn an_ordered_steer_photo_lands_with_its_bytes_alike(cipher: Cipher) {
    let store = both(three_texts(), true, cipher).await;
    for side in [&store] {
        side.side.release_to_idle().await;
        let sent = send_photo(side, "r2", broca_protocol::Delivery::Queue).await;
        assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
        let sent = send_ordered_image(side, "o1", &["tool_result", "steer"], WIDE).await;
        assert!(matches!(sent, SendOutcome::Pending { .. }), "{sent:?}");
        let sent = side
            .side
            .deliver("r3", text("and this"), broca_protocol::Delivery::Steer)
            .await;
        assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
        side.side.release_to_idle().await;
    }
    sent_alike(&store, "the step that delivers the ordered photo");
    let last = store.sent().last().cloned().unwrap().unwrap();
    let last = String::from_utf8_lossy(&last);
    assert!(
        last.contains(&base64(&bytes_of(WIDE))) && !last.contains("reason=provenance"),
        "the delivering step sends the delivered photo's bytes: {last}"
    );
}

/// A photo sent with the ordered list `[idle_start]` to an idle session starts its run alike,
/// and the run's request carries its image alike.
async fn an_idle_start_photo_starts_its_run_and_renders_alike(cipher: Cipher) {
    let store = both(three_texts(), false, cipher).await;
    for side in [&store] {
        let sent = send_ordered_image(side, "o1", &["idle_start"], PNG).await;
        assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
        side.side.idle().await;
    }
    sent_alike(&store, "the run the ordered photo started");
}

/// A photo sent with an ordered list naming neither `steer` nor `idle_start` has no mode that
/// can carry it: a tool result carries text only, and turn-scoped system text is text. Both
/// refuse it alike, as media the carrier can't take, and neither side sends anything for it.
async fn an_ordered_photo_no_mode_can_carry_is_refused_alike(cipher: Cipher) {
    let store = both(three_texts(), false, cipher).await;
    for modes in [&["tool_result"][..], &["turn_system"][..]] {
        let mut replies = Vec::new();
        for side in [&store] {
            let before = side.sent().len();
            replies.push(send_ordered_image(side, "o1", modes, PNG).await);
            side.side.idle().await;
            assert_eq!(side.sent().len(), before, "{modes:?}: nothing is sent");
        }
        broca_test_support::pin!(format!("{modes:?}: the answer"), replies[0]);
        assert!(
            matches!(
                replies[0],
                SendOutcome::DeliveryRefused {
                    code: "media_unsupported",
                    ..
                }
            ),
            "{modes:?}: {:?}",
            replies[0]
        );
    }
}

/// After a restart, a run's request still carries the photo an earlier run's input held: the
/// new storage's fold is rebuilt from the session's first record, and must hold its capture.
async fn a_run_after_a_restart_renders_an_earlier_photo_alike(cipher: Cipher) {
    let store = both(three_texts(), false, cipher).await;
    for side in [&store] {
        send_photo(side, "r2", broca_protocol::Delivery::Queue).await;
        side.side.idle().await;
    }
    let store = store
        .restarted(true, vec![text_step("a4", "still here")])
        .await;
    for side in [&store] {
        let sent = side.side.send("r3", text("still there?")).await;
        assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
        side.side.idle().await;
    }
    sent_alike(&store, "the first run after the restart");
}

/// A tool's image survives a restart: the next run's request carries its bytes, read from the
/// object stored with the tool's result.
async fn a_tool_image_renders_alike_after_a_restart(cipher: Cipher) {
    let steps = vec![
        text_step("a1", "hello"),
        photo_step("a2"),
        text_step("a3", "I see it"),
    ];
    let store = both_from(steps, false, with_photo_tool("first"), cipher).await;
    for side in [&store] {
        side.side.send("r2", with_photo_tool("show me")).await;
        side.side.idle().await;
    }
    let store = store
        .restarted(true, vec![text_step("a4", "still here")])
        .await;
    for side in [&store] {
        side.side.send("r3", with_photo_tool("still there?")).await;
        side.side.idle().await;
    }
    sent_alike(&store, "the first run after the restart");
}

/// [`with_photo_tool`], sent by core: a session whose runs may get a tool's image and whose user
/// sends a photo core uploaded.
fn photo_tool_by_core(text: &str) -> SubmissionRequest {
    let mut sent = with_photo_tool(text);
    sent.principal = Some(super::core());
    sent
}

/// The first request of a session's latest run, rendered offline from the new storage: the
/// session as the run holds it and its own media objects (`store_media::offline_render`). For a
/// session holding a tool's image and a photo the user sent, that must be the bytes the model was
/// asked to send. So the offline render, which a session move uses to check the copy it writes,
/// is checked against the request the session's run really renders.
async fn an_input_photo_and_a_tool_image_render_offline_as_sent(cipher: Cipher) {
    use futures::FutureExt as _;
    let steps = vec![
        text_step("a1", "hello"),
        photo_step("a2"),
        text_step("a3", "I see it"),
        text_step("a4", "a photo"),
    ];
    let store = both_from(steps, true, photo_tool_by_core("first"), cipher).await;
    for side in [&store] {
        side.side.release_to_idle().await;
        let sent = side.side.send("r2", photo_tool_by_core("show me")).await;
        assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
        side.side.release_to_idle().await;
        // A release that found no step waiting leaves a permit; the photo's run must wait.
        let gate = side.side.gate.as_ref().unwrap();
        let _ = gate.notified().now_or_never();
        let before = side.sent().len();
        let upload = side.side.uploaded(&bytes_of(PNG)).await;
        let mut photo = photo_tool_by_core("look");
        photo.content = super::media::photo("look", &[&upload]).content;
        let sent = side
            .side
            .deliver("r3", photo, broca_protocol::Delivery::Queue)
            .await;
        assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
        broca_test_support::wait_until("the photo run's request is rendered", || async {
            if side.sent().len() > before {
                Ok(())
            } else {
                Err("not yet".to_owned())
            }
        })
        .await;
    }
    let image = base64(&bytes_of(PNG));
    let last = |side: &Rendered| side.sent().last().unwrap().clone().unwrap();
    assert!(String::from_utf8_lossy(&last(&store)).contains(&image));
    let text = String::from_utf8_lossy(&last(&store)).into_owned();
    assert_eq!(
        text.matches(&image).count(),
        2,
        "the request carries the tool's image and the user's photo"
    );

    // The new storage's: the session as its run holds it now, and its own media objects.
    use broca_session_store::backend::{LocalBackend, SessionAddr};
    let root = store.side.dir.join(broca_wal::SESSION_STORAGE_DIR);
    let deployment = broca_session_store::keys::DeploymentKey::new(1, &[7; 32]).creating(cipher);
    let key = &store.side.key;
    let view = broca_session_store::session::Session::view(
        LocalBackend::new(&root),
        SessionAddr(broca_core::session_addr(key)),
        &broca_session_store::head::SessionTriple {
            project_root: key.project_root.to_string_lossy().into_owned(),
            harness: key.harness.clone(),
            session: key.session.clone(),
        },
        &deployment,
    )
    .await
    .unwrap();
    let setup = LocalBackend::new(&root);
    let parts = broca_session_loop::parts::DeploymentParts::loaded(
        broca_session_store::setup::SetupStore::new(
            &setup,
            &deployment.setup_key_for(cipher).unwrap(),
        )
        .load_all()
        .await
        .unwrap(),
    );
    // The render config and tag choice the run froze.
    let start = view
        .operations
        .run
        .as_ref()
        .and_then(|run| run.start.as_ref())
        .expect("the photo's run is under way");
    let config = broca_session_loop::run::run_config_of_start(start, &parts).unwrap();
    let frozen: broca_provider::wire::FrozenRenderConfig =
        serde_json::from_value(config.frozen_render_config.clone()).unwrap();
    let body = crate::store_media::offline_render(
        &view,
        LocalBackend::new(&root),
        &parts,
        key,
        1,
        &frozen,
        config.render_tags,
        None,
    )
    .await
    .unwrap();
    assert!(
        body == last(&store),
        "the new storage's offline render is the request sent"
    );
    drop(view);
    sent_alike(&store, "the photo run's request");
    for side in [&store] {
        side.side.release_to_idle().await;
    }
}

/// Media files under `dir` holding the image: the new storage's media object, and a copy at the
/// media store's object path.
fn media_files(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if path.is_dir() {
            media_files(&path, out);
        } else if name.contains("-media-") || name == super::media::digest(&bytes_of(PNG)) {
            out.push(path);
        }
    }
}

/// A photo whose bytes can no longer be read refuses the request by name, and nothing is sent:
/// never a request without its image. The bytes are read from the session's own object, so
/// removing that object is what refuses it, even with the same bytes waiting at the media store's
/// object path: a run of a session in the new storage never reads those.
async fn a_photo_whose_bytes_cant_be_read_refuses_alike_and_nothing_is_sent(cipher: Cipher) {
    let store = both(three_texts(), false, cipher).await;
    for side in [&store] {
        send_photo(side, "r2", broca_protocol::Delivery::Queue).await;
        side.side.idle().await;
    }
    for side in [&store] {
        let mut files = Vec::new();
        media_files(&side.side.dir, &mut files);
        assert!(!files.is_empty(), "the image is stored");
        for file in files {
            std::fs::remove_file(file).unwrap();
        }
    }
    for class in [
        broca_core::media_store::MediaStoreClass::Durable,
        broca_core::media_store::MediaStoreClass::NonDurable,
        broca_core::media_store::MediaStoreClass::Ephemeral,
    ] {
        let objects = store.side.dir.join(class.directory()).join("objects");
        std::fs::create_dir_all(&objects).unwrap();
        std::fs::write(
            objects.join(super::media::digest(&bytes_of(PNG))),
            bytes_of(PNG),
        )
        .unwrap();
    }
    for side in [&store] {
        side.side.send("r3", text("and now?")).await;
        side.side.idle().await;
    }
    let new = store.sent();
    assert_eq!(new.last(), Some(&Err("media_object_missing".to_owned())));
    broca_test_support::pin!("both refuse alike", new);
}

/// Sends `request` on `side` as run `run_id` under the fetch plan `plan`, as a planned
/// session's sends arrive from the side's caller.
async fn send_planned(
    side: &Rendered,
    run_id: &str,
    request: SubmissionRequest,
    plan: &Arc<broca_wal::PlanManifest>,
) -> SendOutcome {
    let key = crate::actor::SendKey::Minted;
    side.side
        .registry
        .route_submission_with_order(
            &side.side.key,
            broca_protocol::RunId(run_id.into()),
            side.side.caller.shape(run_id, &key, request),
            RunKind::FreshOrResume,
            key,
            false,
            broca_protocol::Delivery::Queue,
            Some(crate::actor::SendPlan {
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

/// A send by core under its head scope on a planned session, which names no tools of its own,
/// with the context limit the compaction contract needs.
fn planned(mut request: SubmissionRequest) -> SubmissionRequest {
    request.tools = None;
    request.policy_allowed = Default::default();
    request.context_limit = Some(200_000);
    request
}

/// A run after a compaction view that drops an earlier photo renders alike: the view is applied
/// at the run's first step on both storages, and the photo, no longer in the model's view, is
/// left out of the request the same way.
async fn a_run_after_a_view_drops_a_photo_renders_alike(cipher: Cipher) {
    let plan = super::controls::compacting_plan();
    let steps = three_texts();
    let mut sides = Vec::new();
    let mut compactors = Vec::new();
    for store in [true] {
        let compactor = Arc::new(super::controls::Compactor::default());
        let side = Rendered::with_compactor(
            store,
            steps.clone(),
            false,
            Some(Arc::clone(&compactor)),
            cipher,
        )
        .await;
        let sent = send_planned(&side, "r1", planned(text("first")), &plan).await;
        assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
        side.side.idle().await;
        if store {
            assert_eq!(side.side.stored_mode(), cipher);
        }
        let upload = side.side.uploaded(&bytes_of(PNG)).await;
        let sent = send_planned(&side, "r2", planned(photo("look", &[&upload])), &plan).await;
        assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
        side.side.idle().await;
        // The view drops the photo's message: the session's third, after the first run's two.
        compactor.drop_at_next_ride(3);
        let mut next = planned(text("next"));
        next.model = broca_protocol::request::ModelHandle::new("stub", "another");
        let sent = send_planned(&side, "r3", next, &plan).await;
        assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
        side.side.idle().await;
        sides.push(side);
        compactors.push(compactor);
    }
    assert_eq!(
        compactors
            .iter()
            .map(|compactor| compactor.views())
            .collect::<Vec<_>>(),
        [1],
        "a ride handed the side its view"
    );
    let store = sides.pop().unwrap();
    let new = store.sent();
    sent_pinned(&new, "");
    let image = base64(&bytes_of(PNG));
    let carried: Vec<bool> = new
        .iter()
        .map(|sent| String::from_utf8_lossy(sent.as_ref().unwrap()).contains(&image))
        .collect();
    assert_eq!(
        carried,
        [false, true, false],
        "only the photo's own run sent its bytes; the view dropped it from the next"
    );
}

/// A view that inserts a user message carrying an image, as Magic Context inserts the image it
/// draws of its memories, renders alike. The image isn't a medium the session captured, so on
/// both storages the request shows its provenance marker in its place, never its bytes. Until
/// the new storage rendered it, such a view applied there and then failed every request. The
/// new storage tells its provider it takes no images, whatever the model takes, yet accepts the
/// view: a provider repeats an insert it made before, and refusing it would hold an older view.
async fn a_view_inserting_an_image_renders_alike(cipher: Cipher) {
    let plan = super::controls::compacting_plan();
    let png = base64(&bytes_of(PNG));
    let mut sides = Vec::new();
    let mut stated = Vec::new();
    for store in [true] {
        let compactor = Arc::new(super::controls::Compactor::default());
        let side = Rendered::with_compactor(
            store,
            three_texts(),
            false,
            Some(Arc::clone(&compactor)),
            cipher,
        )
        .await;
        let sent = send_planned(&side, "r1", planned(text("first")), &plan).await;
        assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
        side.side.idle().await;
        compactor.insert_image_at_next_ride(png.clone());
        // Another model loses the prompt cache, so the run's first request rides.
        let mut next = planned(text("next"));
        next.model = broca_protocol::request::ModelHandle::new("stub", "another");
        let sent = send_planned(&side, "r2", next, &plan).await;
        assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
        side.side.idle().await;
        assert_eq!(compactor.views(), 1, "a ride handed the view");
        stated.push(compactor.stated_accepts_images());
        sides.push(side);
    }
    let store_stated = stated.pop().unwrap();
    assert!(
        !store_stated.is_empty(),
        "the store's calls state their conditions"
    );
    assert!(
        store_stated.iter().all(|value| *value == json!(false)),
        "the store states it takes no images: {store_stated:?}"
    );
    let store = sides.pop().unwrap();
    let new = store.sent();
    sent_pinned(&new, "");
    let last = String::from_utf8_lossy(new.last().unwrap().as_ref().unwrap()).into_owned();
    assert!(last.contains("Memories."), "the view applied: {last}");
    assert!(!last.contains(&png), "the image's bytes were sent: {last}");
    assert!(
        last.contains("[media not shown: kind=image mime=image/png")
            && last.contains("reason=provenance]"),
        "its marker stands in its place: {last}"
    );
}

/// Setup may run after the photo is durable and cover it before any notice. A restarted
/// provider must read the session's entries before answering a ride that drops that photo.
async fn a_setup_covering_a_photo_teaches_its_id_without_a_notice(cipher: Cipher) {
    let plan = super::controls::compacting_plan();
    for store in [true] {
        let compactor = Arc::new(super::controls::Compactor::default());
        let side = Rendered::with_compactor(
            store,
            three_texts(),
            false,
            Some(Arc::clone(&compactor)),
            cipher,
        )
        .await;
        let sent = send_planned(&side, "r1", planned(text("first")), &plan).await;
        assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
        side.side.idle().await;
        let upload = side.side.uploaded(&bytes_of(PNG)).await;
        let sent = send_planned(&side, "r2", planned(photo("look", &[&upload])), &plan).await;
        assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
        side.side.idle().await;
        compactor
            .assert_drop_after_setup_without_notice(3, &side.side.key)
            .await;
    }
}

/// On a session whose plan shows part numbers, a request whose photo message sits between
/// numbered messages: the loop leaves the photo's message for the media renderer to number, and
/// numbers the others itself.
async fn a_photo_between_numbered_messages_renders_alike(cipher: Cipher) {
    let plan = super::controls::manifest(json!({"render_tags": true}), vec![]);
    let mut sides = Vec::new();
    for store in [true] {
        let side = Rendered::new(store, three_texts(), false, cipher).await;
        let sent = send_planned(&side, "r1", planned(text("first")), &plan).await;
        assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
        side.side.idle().await;
        if store {
            assert_eq!(side.side.stored_mode(), cipher);
        }
        let upload = side.side.uploaded(&bytes_of(PNG)).await;
        let sent = send_planned(&side, "r2", planned(photo("look", &[&upload])), &plan).await;
        assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
        side.side.idle().await;
        let sent = send_planned(&side, "r3", planned(text("and now?")), &plan).await;
        assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
        side.side.idle().await;
        sides.push(side);
    }
    let store = sides.pop().unwrap();
    sent_alike(&store, "a request with a photo between numbered messages");
    let last = store.sent().last().cloned().unwrap().unwrap();
    assert!(
        String::from_utf8_lossy(&last).contains('§'),
        "the plan's numbers are shown: {}",
        String::from_utf8_lossy(&last)
    );
}

mod crash_cuts;
mod import_media;
mod tool_crash;
