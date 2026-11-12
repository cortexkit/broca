//! What each model step of a compacted head costs broca, at 1,000, 150,000 and 500,000 messages.
//!
//! A head's run takes many steps on a session whose compaction view summarises most of its
//! history. This bench builds such a session in the new session storage (local files, encrypted),
//! opens a store actor over it as the module opens one, and sends a run of tool steps through the
//! new run loop with a scripted model and a scripted compaction provider. The provider hands over
//! a view at the run's first step: a summary inserted at the start and a cut through all but the
//! newest messages. Every later step renders through that view.
//!
//! Nothing on the step path is changed or stubbed out except the model, the tools and the
//! compaction provider at the far end of their seams. The run's writer is wrapped, so every call
//! the loop makes into the session (each append and its barrier, each read) is timed where the
//! loop makes it; the seams time when the loop reaches the model, the tools and the compaction
//! provider. The time between two timed calls is the loop's own work, labelled by the calls on
//! either side. The scripted compaction provider reads `session.entries` from its cursor through
//! the actor after each notice, as Magic Context (the compaction provider agent heads run with)
//! reads the entries each notice announces.
//!
//! Each size is measured in three cases, which differ in how the history behind the view's cut
//! is kept:
//! - `sealed-once`: the history is written unsealed, and the run's first step, applying the
//!   view, seals everything before its cut in one seal, as the first view on a long unsealed
//!   head does;
//! - `sealed-often`: while the history is written, a run applies a newer view about every
//!   `--seal-every` messages, each sealing the history before its cut, as a head's history is
//!   sealed over its life; the measured run's first step seals the rest up to its cut;
//! - `open-log`: the session never seals, so the whole history stays in the open log behind the
//!   cut (the run's writer skips the loop's seal);
//! - `no-cut`, a worker's shape: the view cuts nothing, so every request carries the whole
//!   session, but replaces the output of every tool call before the newest `--keep` messages
//!   with a short marker, as a worker's view drops old tool results. A worker's session stays
//!   within its context window, so this case is run at smaller sizes (`--sizes
//!   1000,5000,20000 --cases no-cut`). The summary splits its request build into the renders of
//!   the request's bytes and the rest, with each one's slope per message, and times the view's
//!   planning over the window offline.
//!
//! Run it on a release build (it writes a few gigabytes under the store directory and takes
//! minutes; `open-log` takes tens of seconds a step at the larger sizes):
//!
//! ```text
//! cargo run --release -p broca-session-convert --example compacted_step_cost -- \
//!     --out <results dir> [--store <scratch dir>] [--sizes 1000,150000,500000] \
//!     [--cases sealed-once,sealed-often,open-log,no-cut] [--warmup 10] [--steps 60] [--keep 300] \
//!     [--seal-every 2000] [--opens 3] [--open-log-warmup N] [--open-log-steps N] \
//!     [--unbilled true] [--interleave true]
//! ```
//! `--token-counts true` instead measures cold 100-entry full-detail pages, incremental notices,
//! and a 150k-message paged indexing walk, reporting time spent inside the tokenizer separately.
//!
//! `--open-log-warmup` and `--open-log-steps` set the warm-up and measured steps of the
//! `open-log` case at every size but the smallest. The ratios compare each size with the
//! smallest size given. By default every history run is billed, as on a module that keeps the
//! usage export; `--unbilled true` leaves them all owed to billing, as on a module without one.
//!
//! By default each size and case is measured alone, one after another. `--interleave true`
//! measures the sizes of each case side by side instead, so whatever else loads the machine (the
//! disk's flushes above all) falls on every size alike: every size's session is built first, and
//! then their measured runs run at the same time, their model calls taking turns round-robin in
//! the order the sizes are given (the first size's step, the second's, the first's again, and
//! so on). The turn is kept in the scripted model's seam: a run waiting for its turn is timed as
//! the `model: turnstile wait` item, which counts with the test doubles, outside every part and
//! the total. The actors' opening costs are measured before any run starts and after all end.
//!
//! It writes `summary.md` (tables per part and size, with ratios to the smallest size),
//! `steps-<size>-<case>.jsonl` (every step's timed calls and gaps, in order, warm-up included)
//! and `labels-<size>-<case>.tsv` (each label's samples over the measured steps) under `--out`.
//! Each step's line has `model_call_at_ns`, when the model call ending its cycle began, in
//! nanoseconds from one origin the whole bench shares, so the step files of sizes measured side
//! by side can be merged in time order.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use broca_core::compaction_v2::protocol::view::{Edit, EditOp, InsertAt, InsertedMessage, View};
use broca_core::provider::StubProvider;
use broca_core::stub_plane::StubToolPlane;
use broca_core::{OutputEvent, RunError, RunSummary};
use broca_module::actor::{LineageCmd, LineageStatus, SendKey, SendOutcome, SendPlan};
use broca_module::executor::{
    RunExecutor, RunJob, RunKind, StoreRunEnd, StoreRunError, StoreRunJob, StoreRunMode,
};
use broca_module::store_actor::StoreActor;
use broca_module::store_reads::{StoreReadOutcome, StoreReadReply, StoreReadRequest};
use broca_protocol::content::{AssistantMessage, ContentBlock, FinishReason, ToolCall};
use broca_protocol::{
    BindIdentity, MessageId, NormalizedEvent, ProviderControl, RunId, SubmissionRequest,
    ToolCallId, Usage,
};
use broca_session_loop::parts::{MemoryParts, SharedParts};
use broca_session_loop::run::Seams;
use broca_session_loop::writer::Writer;
use broca_session_store::backend::{LocalBackend, MemoryBackend, SessionAddr};
use broca_session_store::fold::conversation::Conversation;
use broca_session_store::fold::operations::Operations;
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::{Cipher, DeploymentKey};
use broca_session_store::record::Record;
use broca_session_store::session::{Session, SessionError};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};

#[path = "compacted_step_cost/token_counts.rs"]
mod token_counts;

/// The address every session of the bench is written at.
const ADDR: &str = "0123456789abcdef";

/// The id of the compaction provider the plan names, which `session.entries` admits.
const COMPACTOR: &str = "compactor";

/// Every run id has this many characters, so a template run's id can be replaced in a record's
/// encoded bytes by the id of the run built from it.
const RUN_ID_LEN: usize = 10;

fn bind() -> BindIdentity {
    BindIdentity::new("/project", "step-cost", "s1")
}

fn triple() -> SessionTriple {
    SessionTriple {
        project_root: "/project".into(),
        harness: "step-cost".into(),
        session: "s1".into(),
    }
}

/// The deployment key, creating encrypted sessions.
fn deployment() -> DeploymentKey {
    DeploymentKey::new(1, &[7; 32]).creating(Cipher::XChaCha20Poly1305)
}

/// A fetch plan naming the compaction provider, with the `read` tool and a system text.
fn manifest() -> Arc<broca_wal::PlanManifest> {
    Arc::new(broca_wal::PlanManifest {
        rich_ui: None,
        plan: broca_wal::OpaqueJson::from_json_text(
            json!({"compaction_item": {"provider": COMPACTOR, "preset": "head", "params": {}}})
                .to_string(),
        )
        .unwrap(),
        plan_identity: "plan-1".into(),
        composition_digest: "composition".into(),
        tools: Arc::new(vec![serde_json::from_value(json!({
            "name": "read",
            "description": "Reads a file of the project and answers its text.",
            "input_schema": {
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"],
            },
        }))
        .unwrap()]),
        system_text: "You are the head of a team of coding agents. Plan, delegate and review."
            .into(),
        system_digest: "system".into(),
        join_version: 1,
        role_versions: Default::default(),
        fetches: vec![],
        declarations: vec![],
    })
}

/// The context window the `no-cut` case's measured run is admitted with (see `run_measured`).
const NO_CUT_CONTEXT_LIMIT: u64 = 20_000_000;

/// A send naming no tools of its own (the plan's apply), on model `model`, with a context window
/// a head's model has.
fn request(text: &str, model: &str, max_steps: u32) -> SubmissionRequest {
    serde_json::from_value(json!({
        "model": {"provider_module_id": "stub", "model_id": model},
        "input": [{"role": "user", "content": [{"type": "text", "text": text}]}],
        "stop_when": [{"type": "step_count", "n": max_steps}],
        "context_limit": 1_000_000,
    }))
    .unwrap()
}

/// A model step that writes a sentence, reasons, and calls `read` once, with call id `call`.
fn tool_step(call: &str) -> Vec<NormalizedEvent> {
    let reasoning = "The reader reloads the session on every page. Before changing it I need to \
                     see where the view is built and who else holds it, so I read the module \
                     that defines it and then the caller. "
        .repeat(3);
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(format!("m-{call}")),
                content: vec![
                    ContentBlock::Reasoning {
                        text: reasoning,
                        signature: Some("c2lnbmF0dXJl".repeat(16)),
                    },
                    ContentBlock::Text {
                        text: "Let me look at the file that defines this before changing it."
                            .into(),
                    },
                    ContentBlock::ToolCall(ToolCall {
                        thought_signature: None,
                        tool_call_id: ToolCallId(call.into()),
                        tool_name: "read".into(),
                        input: json!({"path": format!("crates/example/src/{call}.rs")}),
                    }),
                ],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(60_000),
                output_tokens: Some(180),
                ..Usage::default()
            },
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ]
}

/// A model step that answers in text and ends the run.
fn answer_step(id: &str) -> Vec<NormalizedEvent> {
    let text = "The change is in place: the reader now keeps the view it loaded and serves it \
                again until the session moves on. "
        .repeat(4);
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(id.into()),
                content: vec![ContentBlock::Text { text }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(60_000),
                output_tokens: Some(120),
                ..Usage::default()
            },
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ]
}

/// What a `read` call answers: about `bytes` bytes of source.
fn tool_text(bytes: usize) -> String {
    "fn example(reader: &Reader) -> u64 { reader.view().len() }\n".repeat(bytes / 59 + 1)
}

/// The instant every timeline counts from, so the events of runs measured side by side compare.
static ORIGIN: OnceLock<Instant> = OnceLock::new();

/// One timed call, or the loop's own work between two of them.
#[derive(Clone, Debug)]
struct Event {
    label: String,
    /// Nanoseconds from the bench's [`ORIGIN`].
    start: u64,
    end: u64,
    /// Nested inside another event (an entries read inside the notice that prompted it), so
    /// not part of the step's sequence.
    nested: bool,
}

/// Every timed event of one run, in the order they started.
struct Timeline {
    origin: Instant,
    events: Mutex<Vec<Event>>,
}

impl Timeline {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            origin: *ORIGIN.get_or_init(Instant::now),
            events: Mutex::new(Vec::new()),
        })
    }

    fn now(&self) -> u64 {
        self.origin.elapsed().as_nanos() as u64
    }

    fn push(&self, label: String, start: u64, end: u64, nested: bool) {
        self.events.lock().unwrap().push(Event {
            label,
            start,
            end,
            nested,
        });
    }

    fn take(&self) -> Vec<Event> {
        std::mem::take(&mut *self.events.lock().unwrap())
    }
}

/// The names of `records`' kinds, joined, for a label.
fn kinds(records: &[Record]) -> String {
    let mut names: Vec<&str> = records.iter().map(Record::name).collect();
    names.dedup();
    names.join(",")
}

/// The run's writer, timing each call the loop makes into the session. With `seals` off, a seal
/// at a view's cut is skipped, so the history behind the cut stays in the open log.
struct TimedWriter<W> {
    inner: W,
    timeline: Arc<Timeline>,
    seals: bool,
}

impl<W: Writer> TimedWriter<W> {
    fn mark(&self, label: String, start: u64) {
        let end = self.timeline.now();
        self.timeline.push(label, start, end, false);
    }
}

impl<W: Writer> Writer for TimedWriter<W> {
    async fn append(&mut self, records: &[Record], barrier: bool) -> Result<u64, SessionError> {
        let start = self.timeline.now();
        let result = self.inner.append(records, barrier).await;
        self.mark(
            format!(
                "append{} [{}]",
                if barrier { "+sync" } else { "" },
                kinds(records)
            ),
            start,
        );
        result
    }

    async fn append_input_media(
        &mut self,
        records: &[Record],
        media: Vec<broca_session_store::session::NewInputMedia>,
        barrier: bool,
    ) -> Result<(u64, Vec<Record>), SessionError> {
        let start = self.timeline.now();
        let result = self.inner.append_input_media(records, media, barrier).await;
        self.mark(format!("append_input_media [{}]", kinds(records)), start);
        result
    }

    async fn send_records(&mut self, submission_id: &str) -> Result<Vec<Record>, SessionError> {
        let start = self.timeline.now();
        let result = self.inner.send_records(submission_id).await;
        self.mark("send_records".into(), start);
        result
    }

    async fn append_with(
        &mut self,
        build: &mut (dyn for<'s> FnMut(&'s Conversation, &'s Operations, u64) -> Vec<Record>
                  + Send),
        barrier: bool,
    ) -> Result<Vec<Record>, SessionError> {
        let start = self.timeline.now();
        let result = self.inner.append_with(build, barrier).await;
        let label = match &result {
            Ok(records) if records.is_empty() => "append_with (nothing)".to_owned(),
            Ok(records) => format!(
                "append_with{} [{}]",
                if barrier || broca_session_loop::writer::ends_run(records) {
                    "+sync"
                } else {
                    ""
                },
                kinds(records)
            ),
            Err(_) => "append_with (failed)".to_owned(),
        };
        self.mark(label, start);
        result
    }

    fn conversation(&self) -> &Conversation {
        self.inner.conversation()
    }

    fn operations(&self) -> &Operations {
        self.inner.operations()
    }

    fn session_id(&self) -> String {
        self.inner.session_id()
    }

    fn next_seq(&self) -> u64 {
        self.inner.next_seq()
    }

    fn retired_tag_floor(&self) -> u64 {
        self.inner.retired_tag_floor()
    }

    fn message_id_prefix(&self) -> String {
        self.inner.message_id_prefix()
    }

    async fn open_log_records(&self) -> Result<Vec<(u64, Record)>, SessionError> {
        let start = self.timeline.now();
        let result = self.inner.open_log_records().await;
        self.mark("open_log_records".into(), start);
        result
    }

    async fn run_start(
        &self,
        run_id: &str,
    ) -> Result<Option<broca_session_store::record::RunStart>, SessionError> {
        let start = self.timeline.now();
        let result = self.inner.run_start(run_id).await;
        self.mark("run_start".into(), start);
        result
    }

    async fn tool_intent(
        &self,
        call_key: &str,
    ) -> Result<Option<broca_session_store::record::ToolIntent>, SessionError> {
        let start = self.timeline.now();
        let result = self.inner.tool_intent(call_key).await;
        self.mark("tool_intent".into(), start);
        result
    }

    async fn enrollment(
        &self,
    ) -> Result<Option<broca_session_store::session::EnrollmentOverlay>, SessionError> {
        let start = self.timeline.now();
        let result = self.inner.enrollment().await;
        self.mark("enrollment".into(), start);
        result
    }

    async fn put_enrollment(
        &mut self,
        entries: &broca_session_store::record::EnrolledEntries,
    ) -> Result<broca_session_store::record::EnrollmentOverlayRef, SessionError> {
        let start = self.timeline.now();
        let result = self.inner.put_enrollment(entries).await;
        self.mark("put_enrollment".into(), start);
        result
    }

    async fn seal_at_entry(
        &mut self,
        ordinal: u64,
        carry: &broca_session_store::session::Carry<'_>,
        now_ms: u64,
    ) -> Result<(), SessionError> {
        let start = self.timeline.now();
        let result = if self.seals {
            self.inner.seal_at_entry(ordinal, carry, now_ms).await
        } else {
            Ok(())
        };
        self.mark(
            if self.seals {
                "seal_at_entry"
            } else {
                "seal_at_entry (skipped)"
            }
            .into(),
            start,
        );
        result
    }

    async fn seal_ledger(
        &mut self,
        carry: &broca_session_store::session::Carry<'_>,
        now_ms: u64,
    ) -> Result<(), SessionError> {
        let start = self.timeline.now();
        let result = self.inner.seal_ledger(carry, now_ms).await;
        self.mark("seal_ledger".into(), start);
        result
    }

    async fn entry_by_mid(
        &self,
        mid: &str,
    ) -> Result<Option<broca_session_store::session::StoredEntry>, SessionError> {
        let start = self.timeline.now();
        let result = self.inner.entry_by_mid(mid).await;
        self.mark("entry_by_mid".into(), start);
        result
    }

    async fn entries_by_mid(
        &self,
        mids: &[String],
    ) -> Result<Vec<broca_session_store::session::StoredEntry>, SessionError> {
        let start = self.timeline.now();
        let result = self.inner.entries_by_mid(mids).await;
        self.mark(format!("entries_by_mid ({})", mids.len()), start);
        result
    }

    async fn sealed_derived(
        &self,
    ) -> Result<Option<Arc<dyn std::any::Any + Send + Sync>>, SessionError> {
        let start = self.timeline.now();
        let result = self.inner.sealed_derived().await;
        self.mark("sealed_derived".into(), start);
        result
    }

    async fn keep_sealed_derived(
        &self,
        value: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<(), SessionError> {
        let start = self.timeline.now();
        let result = self.inner.keep_sealed_derived(value).await;
        self.mark("keep_sealed_derived".into(), start);
        result
    }

    fn media(&self) -> Option<Arc<dyn broca_core::tool_media::MediaCapture>> {
        self.inner.media()
    }
}

/// Keeps the measured runs of an interleaved bench (`--interleave true`) taking their model
/// calls in turn. Each run has a seat; the seats make one model call each, round-robin in seat
/// order, and a seat leaves once its run makes its last call, or ends.
struct Turnstile {
    turns: Mutex<Turns>,
    turned: tokio::sync::Notify,
}

struct Turns {
    /// Whether each seat's run still makes model calls.
    seated: Vec<bool>,
    /// The seat whose call is next.
    next: usize,
}

impl Turns {
    /// Gives the turn to the next seat after `from`, in seat order, whose run still makes calls.
    fn pass(&mut self, from: usize) {
        let count = self.seated.len();
        if let Some(next) = (1..=count)
            .map(|step| (from + step) % count)
            .find(|seat| self.seated[*seat])
        {
            self.next = next;
        }
    }
}

impl Turnstile {
    fn new(seats: usize) -> Arc<Self> {
        Arc::new(Self {
            turns: Mutex::new(Turns {
                seated: vec![true; seats],
                next: 0,
            }),
            turned: tokio::sync::Notify::new(),
        })
    }

    /// Waits for `seat`'s turn, then reads `clock` and passes the turn on, in that order, so the
    /// calls' clock readings follow their turns. With `last`, the seat leaves as it passes.
    async fn enter(&self, seat: usize, last: bool, clock: impl Fn() -> u64) -> u64 {
        loop {
            let turned = self.turned.notified();
            tokio::pin!(turned);
            // Registered before the turn is read, so a pass after the read still wakes it.
            turned.as_mut().enable();
            {
                let mut turns = self.turns.lock().unwrap();
                if turns.next == seat {
                    let at = clock();
                    if last {
                        turns.seated[seat] = false;
                    }
                    turns.pass(seat);
                    drop(turns);
                    self.turned.notify_waiters();
                    return at;
                }
            }
            turned.await;
        }
    }

    /// `seat`'s run ended: it makes no more calls, so the others never wait for it.
    fn leave(&self, seat: usize) {
        let mut turns = self.turns.lock().unwrap();
        turns.seated[seat] = false;
        if turns.next == seat {
            turns.pass(seat);
        }
        drop(turns);
        self.turned.notify_waiters();
    }
}

/// A run's seat at a [`Turnstile`].
#[derive(Clone)]
struct Seat {
    turnstile: Arc<Turnstile>,
    index: usize,
}

/// The scripted model, timing when the loop reaches it and each render the loop asks of it.
/// At each step it also renders the request as the Anthropic Messages provider would send it,
/// timed apart: the scripted model's own rendering is the canonical one, which the loop uses for
/// its size estimates. With a seat at a [`Turnstile`], each call waits for its turn after that
/// render, timed as the `model: turnstile wait` item.
struct BenchProvider {
    inner: StubProvider,
    wire: broca_provider::LiveProvider<broca_provider::families::AnthropicMessages>,
    timeline: Arc<Timeline>,
    /// The size of the newest request the Anthropic renderer gave, in bytes.
    wire_bytes: Mutex<Vec<usize>>,
    seat: Option<Seat>,
    /// How many steps the model is scripted with, and how many calls it had so far.
    scripted: usize,
    called: AtomicUsize,
}

impl BenchProvider {
    fn new(steps: Vec<Vec<NormalizedEvent>>, timeline: Arc<Timeline>) -> Self {
        Self {
            scripted: steps.len(),
            inner: StubProvider::new(steps),
            wire: broca_provider::LiveProvider::new(
                broca_provider::families::AnthropicMessages,
                broca_provider::families::anthropic_frozen_config("claude-sonnet-4-5"),
                broca_provider::wire::AuthHandle::None,
            ),
            timeline,
            wire_bytes: Mutex::new(Vec::new()),
            seat: None,
            called: AtomicUsize::new(0),
        }
    }

    /// This model, taking its calls in turn at `seat`.
    fn seated(mut self, seat: Option<Seat>) -> Self {
        self.seat = seat;
        self
    }
}

#[async_trait]
impl broca_core::provider::Provider for BenchProvider {
    fn render_config_blob(&self) -> Value {
        broca_core::provider::Provider::render_config_blob(&self.inner)
    }

    fn render_config_blob_for_version(&self, renderer_version: u32) -> Value {
        broca_core::provider::Provider::render_config_blob_for_version(
            &self.inner,
            renderer_version,
        )
    }

    fn render_request(&self, opts: &broca_protocol::request::CallOptions) -> Vec<u8> {
        let start = self.timeline.now();
        let bytes = broca_core::provider::Provider::render_request(&self.inner, opts);
        let end = self.timeline.now();
        self.timeline
            .push("render (canonical, loop)".into(), start, end, false);
        bytes
    }

    fn try_render_request(
        &self,
        opts: &broca_protocol::request::CallOptions,
    ) -> Result<Vec<u8>, Box<broca_protocol::ProviderError>> {
        let start = self.timeline.now();
        let bytes = broca_core::provider::Provider::try_render_request(&self.inner, opts);
        let end = self.timeline.now();
        self.timeline
            .push("render (canonical, loop)".into(), start, end, false);
        bytes
    }

    async fn step(
        &self,
        step_id: broca_protocol::StepId,
        opts: &broca_protocol::request::CallOptions,
    ) -> mpsc::Receiver<NormalizedEvent> {
        // The live provider renders the request it sends before its call goes out.
        let start = self.timeline.now();
        let rendered = broca_core::provider::Provider::try_render_request(&self.wire, opts);
        let end = self.timeline.now();
        match rendered {
            Ok(bytes) => {
                self.wire_bytes.lock().unwrap().push(bytes.len());
                self.timeline
                    .push("render (Anthropic wire bytes)".into(), start, end, false);
            }
            Err(error) => {
                eprintln!(
                    "the Anthropic renderer refused the request: {}",
                    error.message
                );
                self.timeline.push(
                    "render (Anthropic wire bytes, failed)".into(),
                    start,
                    end,
                    false,
                );
            }
        }
        let at = match &self.seat {
            Some(seat) => {
                let call = self.called.fetch_add(1, Ordering::SeqCst);
                let waiting = self.timeline.now();
                let at = seat
                    .turnstile
                    .enter(seat.index, call + 1 >= self.scripted, || {
                        self.timeline.now()
                    })
                    .await;
                self.timeline
                    .push("model: turnstile wait".into(), waiting, at, false);
                at
            }
            None => self.timeline.now(),
        };
        self.timeline.push("model step".into(), at, at, false);
        self.inner.step(step_id, opts).await
    }
}

/// The tools: the stub plane answering `read`, timing each call the loop makes.
struct BenchTools {
    inner: StubToolPlane,
    timeline: Arc<Timeline>,
}

#[async_trait]
impl broca_core::tool_plane::ToolPlane for BenchTools {
    async fn batch_started(
        &self,
        ctx: &broca_core::tool_plane::BatchCtx,
        calls: &[broca_protocol::ToolCall],
        manifest: &broca_core::ToolManifest,
    ) -> Result<(), broca_core::tool_plane::ToolPlaneError> {
        let start = self.timeline.now();
        let result = self.inner.batch_started(ctx, calls, manifest).await;
        let end = self.timeline.now();
        self.timeline
            .push("tools: batch started".into(), start, end, false);
        result
    }

    async fn dispatch(
        &self,
        ctx: &broca_core::tool_plane::BatchCtx,
        call: &broca_protocol::ToolCall,
    ) -> broca_core::tool_plane::DispatchResult {
        let start = self.timeline.now();
        let result = self.inner.dispatch(ctx, call).await;
        let end = self.timeline.now();
        self.timeline
            .push("tools: dispatch (scripted)".into(), start, end, false);
        result
    }

    async fn batch_finished(
        &self,
        ctx: &broca_core::tool_plane::BatchCtx,
    ) -> Result<(), broca_core::tool_plane::ToolPlaneError> {
        let start = self.timeline.now();
        let result = self.inner.batch_finished(ctx).await;
        let end = self.timeline.now();
        self.timeline
            .push("tools: batch finished".into(), start, end, false);
        result
    }
}

/// Where the compaction provider reads `session.entries`: the actor serving the session, and
/// for a read the actor hands back, the session's files.
#[derive(Clone)]
struct EntriesReader {
    commands: mpsc::UnboundedSender<LineageCmd>,
    root: PathBuf,
    parts: SharedParts,
}

/// The scripted compaction provider. It answers Setup ready and takes every notice; it answers
/// its first ride with `view`, if set, and every later ride with nothing. After each notice it
/// reads `session.entries` from its cursor through the session's actor, as Magic Context (the
/// compaction provider agent heads run with) reads the entries a notice announces, and moves its
/// cursor to the newest entry it read.
struct Compactor {
    timeline: Arc<Timeline>,
    view: Mutex<Option<View>>,
    reader: Mutex<Option<EntriesReader>>,
    /// The ordinal of the newest entry read so far.
    cursor: Mutex<u64>,
    /// How the actor answered each entries read: served, or handed back.
    outcomes: Mutex<BTreeMap<&'static str, usize>>,
    /// Each method the provider was called with, in order.
    methods: Mutex<Vec<String>>,
    /// The provider rules the newest call stated, as the loop stated them.
    rules: Mutex<Option<Value>>,
}

impl Compactor {
    fn new(timeline: Arc<Timeline>) -> Arc<Self> {
        Arc::new(Self {
            timeline,
            view: Mutex::new(None),
            reader: Mutex::new(None),
            cursor: Mutex::new(0),
            outcomes: Mutex::new(BTreeMap::new()),
            methods: Mutex::new(Vec::new()),
            rules: Mutex::new(None),
        })
    }

    /// Reads the entries after the cursor through the actor, timed as one nested event, and
    /// moves the cursor to the newest entry read.
    async fn read_entries(&self) {
        let Some(reader) = self.reader.lock().unwrap().clone() else {
            return;
        };
        let after = *self.cursor.lock().unwrap();
        let params = serde_json::value::to_raw_value(&json!({
            "select": {"by": "after", "after_ordinal": after, "limit": 100},
            "detail": "full",
        }))
        .unwrap();
        let start = self.timeline.now();
        let request = StoreReadRequest::Compaction {
            key: bind(),
            caller: Some(COMPACTOR.into()),
            method: "session.entries".into(),
            params: params.clone(),
        };
        let (reply, answer) = oneshot::channel();
        reader
            .commands
            .send(LineageCmd::StoreRead {
                request: Box::new(request),
                reply,
            })
            .expect("the actor serves reads");
        let (page, outcome) = match answer.await.expect("the actor answers reads") {
            StoreReadOutcome::Served(reply) => match *reply {
                StoreReadReply::Compaction(page) => (page, "served by the actor"),
                _ => panic!("an entries read answered with another read's reply"),
            },
            StoreReadOutcome::Stored {
                position,
                entry_speaks_for,
                ..
            } => {
                // The actor needed storage: the module reads the session as it stood there.
                let view = Session::view_at(
                    LocalBackend::new(reader.root.clone()),
                    SessionAddr(ADDR.into()),
                    &triple(),
                    &deployment(),
                    &position,
                )
                .await
                .expect("a view at the actor's position");
                let page = broca_module::store_entries::serve_compaction(
                    &reader.parts,
                    Some(&*view),
                    &bind(),
                    Some(COMPACTOR),
                    "session.entries",
                    &params,
                    &entry_speaks_for,
                    None,
                )
                .await;
                (page, "handed back, read from a view")
            }
            StoreReadOutcome::Refused(error) => panic!("an entries read was refused: {error:?}"),
            StoreReadOutcome::Reopen => panic!("the actor closed during an entries read"),
        };
        let end = self.timeline.now();
        self.timeline
            .push(format!("entries read ({outcome})"), start, end, true);
        *self.outcomes.lock().unwrap().entry(outcome).or_default() += 1;
        let page: Value = match page {
            Ok(page) => serde_json::from_str(page.get()).unwrap(),
            Err(refusal) => panic!("session.entries refused: {refusal:?}"),
        };
        if let Some(newest) = page["entries"]
            .as_array()
            .and_then(|entries| entries.iter().filter_map(|e| e["ordinal"].as_u64()).max())
        {
            *self.cursor.lock().unwrap() = newest;
        }
    }
}

#[async_trait]
impl broca_core::CompactionPlane for Compactor {
    async fn setup(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        _: broca_core::compaction_provider::setup::SetupRequest,
    ) -> Result<
        broca_core::compaction_provider::setup::SetupAnswer,
        broca_core::CompactionUnavailable,
    > {
        panic!("a session on the current compaction contract never calls the first protocol")
    }

    async fn call_v2(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        method: &str,
        params: Value,
        _: u64,
    ) -> Result<Box<serde_json::value::RawValue>, broca_core::compaction_v2::runtime::CallError>
    {
        let start = self.timeline.now();
        self.methods.lock().unwrap().push(method.to_owned());
        if let Some(rules) = params.pointer("/conditions/rules") {
            *self.rules.lock().unwrap() = Some(rules.clone());
        }
        // A real plane sends the call's parameters over the daemon's bus as JSON.
        let encoded = serde_json::to_vec(&params).unwrap();
        let entries = params
            .get("entries")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        let answer = match method {
            "compaction.setup" => json!({"answer": "ready"}),
            "compaction.notice" => json!({"answer": "received"}),
            "compaction.ride" => match self.view.lock().unwrap().take() {
                None => json!({"answer": "nothing"}),
                Some(view) => json!({"answer": "view", "view": view}),
            },
            other => panic!("unexpected compaction method {other}"),
        };
        let end = self.timeline.now();
        self.timeline.push(
            format!(
                "{method} (encode {} entries, {} KB)",
                bucket(entries),
                encoded.len() / 1024
            ),
            start,
            end,
            false,
        );
        if method == "compaction.notice" {
            self.read_entries().await;
        }
        Ok(serde_json::value::to_raw_value(&answer).unwrap())
    }
}

/// `count` as a coarse bucket, so a label stays the same from step to step.
fn bucket(count: usize) -> String {
    match count {
        0 => "0".into(),
        1..=9 => "1-9".into(),
        10..=99 => "10-99".into(),
        100..=999 => "100-999".into(),
        _ => format!("{}k+", count / 1000),
    }
}

/// The summarising view of version `version`: a summary inserted at the start, and a cut through
/// `through`.
fn summary_view(version: u64, through: &str) -> View {
    View::seal(
        version,
        vec![
            summary_insert(version),
            Edit::new(
                version,
                EditOp::Cut {
                    through: through.to_owned(),
                },
            ),
        ],
    )
}

/// The summary a view of version `version` inserts at the start.
fn summary_insert(version: u64) -> Edit {
    let summary = "Summary of the session so far: the team moved the reader onto kept views, \
                   fixed the seal at a cut, and reviewed three deliveries. Open threads: the \
                   entries page cost, the actor's open cost, and the view cache bound. "
        .repeat(40);
    Edit::new(
        version,
        EditOp::Insert {
            at: InsertAt::Start,
            message: InsertedMessage::new(
                format!("summary-{version}"),
                broca_core::compaction_v2::protocol::read::Role::User,
                vec![summary],
            ),
        },
    )
}

/// The window entries of `conversation`, projected as the compaction provider reads them.
fn window_entries(
    conversation: &Conversation,
) -> Vec<broca_core::compaction_v2::protocol::read::Entry> {
    broca_session_store::session::window_entries(conversation, 1, usize::MAX)
        .iter()
        .map(|stored| broca_session_loop::entries::project(stored).unwrap())
        .collect()
}

/// The worker's view of version `version` (`no-cut`): a summary inserted at the start, no cut,
/// and the output of every finished tool call in an entry up to ordinal `through` replaced by a
/// short marker, as a worker's view drops old tool results.
fn dropping_view(version: u64, conversation: &Conversation, through: u64) -> View {
    use broca_core::compaction_v2::protocol::part::PartBody;
    let mut edits = vec![summary_insert(version)];
    for entry in window_entries(conversation) {
        if entry.ordinal > through {
            break;
        }
        for part in &entry.parts {
            if let PartBody::Tool(tool) = &part.body {
                if tool.output.is_some() && !tool.provider_executed {
                    edits.push(Edit::new(
                        version,
                        EditOp::Replace {
                            mid: entry.mid.clone(),
                            part: part.id.clone(),
                            field: "output".into(),
                            value: json!("[output dropped]"),
                            expect: part.digest.clone(),
                        },
                    ));
                }
            }
        }
    }
    View::seal(version, edits)
}

/// Runs each store job through the new loop's admission and run, as the module's executor does,
/// over the scripted model, tools and compaction provider, with the run's writer timed.
struct BenchExecutor {
    provider: BenchProvider,
    tool_text: String,
    compactor: Arc<Compactor>,
    timeline: Arc<Timeline>,
    seals: bool,
}

#[async_trait]
impl RunExecutor for BenchExecutor {
    async fn execute(
        &self,
        _job: RunJob,
        _out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        unreachable!("a store session's runs come through execute_on_store")
    }

    fn compaction_plane(&self) -> Option<Arc<dyn broca_core::CompactionPlane>> {
        Some(Arc::clone(&self.compactor) as Arc<dyn broca_core::CompactionPlane>)
    }

    async fn execute_on_store(
        &self,
        job: StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> StoreRunEnd {
        let StoreRunJob {
            mut req,
            mut control,
            writer,
            mut parts,
            mode,
            ..
        } = job;
        // The module's executor admits a run's context limit from the caller's override when
        // the send names one.
        control.admitted_context_limit = control.admitted_context_limit.or(req.context_limit);
        let now = || broca_core::run::epoch_millis().unwrap_or(0);
        let mut writer = TimedWriter {
            inner: writer,
            timeline: Arc::clone(&self.timeline),
            seals: self.seals,
        };
        let result = async {
            if mode != StoreRunMode::Fresh {
                return Err(StoreRunError::Loop(
                    broca_session_loop::run::LoopError::Unsupported("the bench only starts runs"),
                ));
            }
            let admission = broca_session_loop::admit::admission(
                &mut req,
                &control,
                &self.provider,
                writer.operations(),
                Some(now()),
            )
            .map_err(StoreRunError::Admission)?;
            let tools = BenchTools {
                inner: StubToolPlane::new().with_canned_text("read", &self.tool_text),
                timeline: Arc::clone(&self.timeline),
            };
            let seams = Seams {
                provider: &self.provider,
                tools: &tools,
                bind: req.session.clone(),
                out: Some(&out),
                cancel: control.cancel.clone().unwrap_or_default(),
                stop: Default::default(),
                held: None,
                approval: &broca_core::capability::AllowAll,
                hook_plane: control.hook_plane.clone(),
                now_ms: &now,
                session_address: None,
                compaction_plane: control.compaction_plane.as_deref(),
                compaction_session: control.compaction_session.clone(),
                // The module's executor has every store run tell the compaction provider of the
                // session's changes outside its steps.
                compaction_changes: true,
                keep_warm: None,
                account_reports: None,
                max_same_account_wait: Default::default(),
                auth_selection: None,
                step_progress: control.step_progress.clone(),
                model_call_ready: None,
                pause_for_auth_at_start: None,
            };
            broca_session_loop::run::run(&mut writer, parts.as_mut(), admission, &seams)
                .await
                .map_err(StoreRunError::Loop)
        }
        .await;
        StoreRunEnd { parts, result }
    }
}

/// Sends `request` as run `run_id` to the actor behind `commands`, with `plan` when given.
async fn send(
    commands: &mpsc::UnboundedSender<LineageCmd>,
    run_id: &str,
    request: SubmissionRequest,
    plan: Option<&Arc<broca_wal::PlanManifest>>,
) -> SendOutcome {
    let (reply, answer) = oneshot::channel();
    commands
        .send(LineageCmd::Send {
            session_class: None,
            retention: None,
            stop_sequences_named: false,
            conversation_key: None,
            title: None,
            run_id: RunId(run_id.into()),
            request: Box::new(request),
            kind: RunKind::FreshOrResume,
            key: SendKey::Minted,
            skip_materialization: false,
            delivery: broca_protocol::Delivery::Queue,
            plan: plan.map(|manifest| {
                Box::new(SendPlan {
                    identity: manifest.plan_identity.clone(),
                    manifest: Some(Arc::clone(manifest)),
                    model_check: None,
                })
            }),
            order: None,
            reply,
        })
        .expect("the actor takes sends");
    answer.await.expect("the actor answers sends")
}

/// Waits until the actor behind `commands` has no run.
async fn idle(commands: &mpsc::UnboundedSender<LineageCmd>) {
    loop {
        let (reply, answer) = oneshot::channel();
        commands
            .send(LineageCmd::Status { reply })
            .expect("the actor answers its status");
        if answer.await.unwrap() == LineageStatus::Idle {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// One run the new loop wrote on the compaction contract: its records from its start on, the
/// ordinal of its first message, and how many messages it placed.
struct Template {
    records: Vec<Record>,
    first_ordinal: u64,
    messages: u64,
}

/// The run id template runs are written under.
const TEMPLATE_RUN: &str = "tplrun0000";

/// A run of `turns` tool steps whose results are `result_bytes` long and a final answer,
/// written by the new loop through a store actor on a session in memory, under the compacting
/// plan, with its setup parts kept in `parts`.
async fn template(turns: usize, result_bytes: usize, parts: &SharedParts) -> Template {
    let timeline = Timeline::new();
    let mut steps: Vec<_> = (1..=turns).map(|n| tool_step(&format!("c{n}"))).collect();
    steps.push(answer_step("final"));
    let compactor = Compactor::new(Arc::clone(&timeline));
    let executor = BenchExecutor {
        provider: BenchProvider::new(steps, Arc::clone(&timeline)),
        tool_text: tool_text(result_bytes),
        compactor,
        timeline,
        seals: true,
    };
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let session = Session::create(
        backend,
        SessionAddr(ADDR.into()),
        triple(),
        &deployment(),
        false,
    )
    .await
    .unwrap();
    let actor = StoreActor::new(bind(), session, Arc::new(executor), Box::new(parts.clone()))
        .with_readers(Arc::new(move || backend));
    let (commands, receiver) = mpsc::unbounded_channel();
    let served = tokio::spawn(actor.serve(receiver));
    let outcome = send(
        &commands,
        TEMPLATE_RUN,
        request(
            "Please find where the reader reloads the session and fix it.",
            "m",
            50,
        ),
        Some(&manifest()),
    )
    .await;
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    let all = session.records_from(1).await.unwrap();
    let mut start = all
        .iter()
        .position(|(_, record)| matches!(record, Record::RunStart(_)))
        .expect("the run's start");
    // The setup the run froze, written before its start because the session had none yet. Only
    // a session's first run carries it when every later run freezes the same setup.
    if start > 0 && matches!(all[start - 1].1, Record::SetupStub(_)) {
        start -= 1;
    }
    let records: Vec<Record> = all[start..]
        .iter()
        .map(|(_, record)| record.clone())
        .filter(|record| !matches!(record, Record::WriterOpened(_) | Record::ChangeAborted(_)))
        .collect();
    let ordinals: Vec<u64> = records
        .iter()
        .filter_map(|record| match record {
            Record::EntryFacts(facts) => Some(facts.ordinal),
            _ => None,
        })
        .collect();
    let first_ordinal = *ordinals.iter().min().expect("the run placed messages");
    let messages = ordinals.iter().max().unwrap() - first_ordinal + 1;
    Template {
        records,
        first_ordinal,
        messages,
    }
}

fn run_id(index: usize) -> String {
    let id = format!("r{index:09}");
    assert_eq!(id.len(), RUN_ID_LEN);
    id
}

/// `record` from a template run, as run `run` writes it when its messages' ordinals move by
/// `ordinal_shift` and the record itself takes the seq whose message id is `mid`.
fn moved(record: &Record, run: &str, ordinal_shift: i64, mid: &str) -> Record {
    let mut body = record.encode_body();
    let (from, to) = (TEMPLATE_RUN.as_bytes(), run.as_bytes());
    let mut at = 0;
    while let Some(found) = body[at..]
        .windows(from.len())
        .position(|window| window == from)
    {
        body[at + found..at + found + to.len()].copy_from_slice(to);
        at += found + to.len();
    }
    let mut record = Record::decode(record.kind(), &body).expect("a moved record decodes");
    let shift = |ordinal: u64| u64::try_from(ordinal as i64 + ordinal_shift).unwrap();
    match &mut record {
        Record::EntryFacts(facts) => {
            facts.ordinal = shift(facts.ordinal);
            facts.mid = mid.to_owned();
        }
        Record::TurnFacts(turn) => turn.ordinal = shift(turn.ordinal),
        // Each message is named by the seq of its own facts, so no step or batch start
        // announces an id.
        Record::StepStart(start) => start.mid = None,
        Record::BatchStart(batch) => batch.mid = None,
        _ => {}
    }
    record
}

/// A session built for the bench: where it is, how many messages it holds, and the message the
/// view cuts through.
#[derive(Clone)]
struct Built {
    root: PathBuf,
    messages: u64,
    /// The id and ordinal of the last message of the newest run that ends at or before
    /// `messages - keep`: the view's cut goes through it, so the window after the cut starts at
    /// a run's input.
    cut: (String, u64),
    written: Duration,
    bytes: u64,
    /// How many views with a cut applied while the history was written, each sealing it.
    views: u64,
}

/// Writes a session of at least `messages` messages under `root`, from `templates` in turn,
/// every run complete. With `seal_every`, each time about that many more messages were written
/// (short of the newest `keep` and the stretch before them), a run applies a newer compaction
/// view cutting through the newest message, which seals the history before it, as a head's
/// history is sealed at each new cut its views make; without, all of it stays in the open log.
/// With `billed`, each run is followed by the `Billed` record the store actor writes once the
/// usage export has its spend facts, as on a module that keeps that export; without, every
/// finished run stays in the session's list of runs owed to billing.
async fn build(
    root: &Path,
    messages: u64,
    keep: u64,
    seal_every: Option<u64>,
    billed: bool,
    templates: &[Template],
    parts: &SharedParts,
) -> Built {
    let started = Instant::now();
    let _ = std::fs::remove_dir_all(root);
    let mut session = Session::create(
        LocalBackend::new(root.to_path_buf()),
        SessionAddr(ADDR.into()),
        triple(),
        &deployment(),
        false,
    )
    .await
    .unwrap();
    let cut_at = messages.saturating_sub(keep);
    let mut cut = None;
    let mut index = 0;
    let mut views = 0;
    let mut next_seal = seal_every;
    loop {
        let template = &templates[index % templates.len()];
        let run = run_id(index);
        let shift = session.conversation.placed as i64 + 1 - template.first_ordinal as i64;
        let mut batch: Vec<Record> = template
            .records
            .iter()
            .filter(|record| index == 0 || !matches!(record, Record::SetupStub(_)))
            .enumerate()
            .map(|(offset, record)| moved(record, &run, shift, &session.message_id(offset as u64)))
            .collect();
        if billed {
            batch.push(Record::Billed(broca_session_store::record::Billed {
                run_id: run.clone(),
            }));
        }
        let last = session.conversation.placed + template.messages >= messages;
        session
            .append(&batch, index % 64 == 0 || last)
            .await
            .unwrap();
        let newest = batch
            .iter()
            .filter_map(|record| match record {
                Record::EntryFacts(facts) => Some((facts.mid.clone(), facts.ordinal)),
                _ => None,
            })
            .max_by_key(|(_, ordinal)| *ordinal)
            .expect("a run places messages");
        let placed = session.conversation.placed;
        if next_seal.is_some_and(|at| placed >= at)
            && placed + seal_every.unwrap_or(0) <= cut_at
            && !last
        {
            // A run whose first step applies a newer view, cutting through the newest
            // message, and seals the history before it.
            views += 1;
            cut_run(&mut session, parts, views, &newest.0, index, billed).await;
            next_seal = seal_every.map(|every| placed + every);
        }
        if newest.1 <= cut_at.max(1) || cut.is_none() {
            cut = Some(newest);
        }
        if last {
            break;
        }
        index += 1;
    }
    let built = session.conversation.placed;
    session.close().await.unwrap();
    Built {
        root: root.to_path_buf(),
        messages: built,
        cut: cut.unwrap(),
        written: started.elapsed(),
        bytes: dir_bytes(root),
        views,
    }
}

/// The history of `size` messages a case measures, written under the store directory: sealed
/// at a new cut every `--seal-every` messages when `often` (`sealed-often`), else kept in the
/// open log.
async fn build_base(
    args: &Args,
    size: u64,
    often: bool,
    templates: &[Template],
    parts: &SharedParts,
) -> Built {
    let base = args.store.join(format!(
        "{size}-{}",
        if often { "sealed-often-base" } else { "base" }
    ));
    let built = build(
        &base,
        size,
        args.keep,
        often.then_some(args.seal_every),
        !args.unbilled,
        templates,
        parts,
    )
    .await;
    println!(
        "built {} messages in {:.1} s ({:.1} MB, {} cuts sealed)",
        built.messages,
        built.written.as_secs_f64(),
        built.bytes as f64 / 1e6,
        built.views
    );
    built
}

/// Runs one run of one answering step on `session` under the compacting plan, on another model
/// than the history's runs, so its step rides the compaction provider, which hands over the
/// summarising view of version `version` cutting through the message `through`. The step
/// applies the view and seals the history before the cut, as a head's run does when its
/// provider hands a newer view over. Its run id is made from `index`; with `billed`, the run is
/// billed once it ends, as [`build`] bills its runs.
async fn cut_run(
    session: &mut Session<LocalBackend>,
    parts: &SharedParts,
    version: u64,
    through: &str,
    index: usize,
    billed: bool,
) {
    let timeline = Timeline::new();
    let compactor = Compactor::new(Arc::clone(&timeline));
    *compactor.view.lock().unwrap() = Some(summary_view(version, through));
    let provider = BenchProvider::new(vec![answer_step(&format!("cut-{version}"))], timeline);
    let run_id = format!("c{index:09}");
    let mut req = broca_core::RunRequest::from_submission(
        request("Carry on from the summary.", "m2", 5),
        RunId(run_id.clone()),
        bind(),
        broca_protocol::SubmissionId(String::new()),
    );
    let control = broca_core::RunControl {
        cancel: Some(Default::default()),
        manifest: Some(manifest()),
        admitted_context_limit: Some(1_000_000),
        new_log_compaction_plan_version: Some(broca_core::compaction_v2::PLAN_VERSION),
        ..Default::default()
    };
    let now = || broca_core::run::epoch_millis().unwrap_or(0);
    let admission = broca_session_loop::admit::admission(
        &mut req,
        &control,
        &provider,
        Writer::operations(session),
        Some(now()),
    )
    .unwrap();
    let tools = StubToolPlane::new();
    let seams = Seams {
        provider: &provider,
        tools: &tools,
        bind: bind(),
        out: None,
        cancel: Default::default(),
        stop: Default::default(),
        held: None,
        approval: &broca_core::capability::AllowAll,
        hook_plane: None,
        now_ms: &now,
        session_address: None,
        compaction_plane: Some(&*compactor),
        compaction_session: None,
        compaction_changes: false,
        keep_warm: None,
        account_reports: None,
        max_same_account_wait: Default::default(),
        auth_selection: None,
        step_progress: None,
        model_call_ready: None,
        pause_for_auth_at_start: None,
    };
    let window_start = Writer::conversation(session).window_start();
    let mut parts = parts.clone();
    broca_session_loop::run::run(session, &mut parts, admission, &seams)
        .await
        .unwrap();
    assert!(
        Writer::conversation(session).window_start() > window_start,
        "the view of version {version} sealed nothing; compaction calls: {:?}",
        compactor.methods.lock().unwrap()
    );
    if billed {
        session
            .append(
                &[Record::Billed(broca_session_store::record::Billed {
                    run_id,
                })],
                false,
            )
            .await
            .unwrap();
    }
}

/// The bytes of every file under `root`.
fn dir_bytes(root: &Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(root) {
        for entry in entries.flatten() {
            let path = entry.path();
            match entry.metadata() {
                Ok(meta) if meta.is_dir() => total += dir_bytes(&path),
                Ok(meta) => total += meta.len(),
                Err(_) => {}
            }
        }
    }
    total
}

/// Copies the directory `from` to `to`, replacing what `to` held.
fn copy_dir(from: &Path, to: &Path) {
    let _ = std::fs::remove_dir_all(to);
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap().flatten() {
        let (source, target) = (entry.path(), to.join(entry.file_name()));
        if entry.metadata().unwrap().is_dir() {
            copy_dir(&source, &target);
        } else {
            std::fs::copy(&source, &target).unwrap();
        }
    }
}

/// The parts of a step cycle the bench reports, in order. A cycle runs from one model call to
/// the next: the step's answer, its notice, its tool batch, the notice of the batch's results,
/// and the next step's boundary and request.
const PARTS: [&str; 7] = [
    "1 request build",
    "2 step durability",
    "3 compaction notices",
    "4 entries reads",
    "5a answer handling",
    "5b tool batch bookkeeping",
    "total",
];

/// Where in a cycle an item falls, which decides its part unless it is an append.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Phase {
    Answer,
    Notice,
    Batch,
    Request,
}

impl Phase {
    fn part(self) -> &'static str {
        match self {
            Phase::Answer => PARTS[4],
            Phase::Notice => PARTS[2],
            Phase::Batch => PARTS[5],
            Phase::Request => PARTS[0],
        }
    }
}

/// One measured step cycle: each timed call and each gap between two, in order, with its part,
/// and each part's sum.
struct Cycle {
    step: usize,
    items: Vec<(String, u64, &'static str)>,
    parts: BTreeMap<&'static str, u64>,
    /// When the model call that ends the cycle began, in nanoseconds from the bench's origin.
    model_call_at: u64,
}

/// Splits a run's events into step cycles, one per model call that has a next one.
fn cycles(events: &[Event]) -> Vec<Cycle> {
    let mut top: Vec<&Event> = events.iter().filter(|event| !event.nested).collect();
    top.sort_by_key(|event| (event.start, event.end));
    let nested: Vec<&Event> = events.iter().filter(|event| event.nested).collect();
    let marks: Vec<usize> = top
        .iter()
        .enumerate()
        .filter(|(_, event)| event.label == "model step")
        .map(|(index, _)| index)
        .collect();
    // The run's start up to its first model call (step 0), then one cycle per model call that
    // has a next one.
    let mut bounds = Vec::new();
    if let Some(&first) = marks.first().filter(|first| **first > 0) {
        bounds.push((0, 0, first));
    }
    for (step, pair) in marks.windows(2).enumerate() {
        bounds.push((step + 1, pair[0], pair[1]));
    }
    let mut out = Vec::new();
    for (step, from, to) in bounds {
        // The cycle's events after its model call, then the next model call, which ends it.
        let span = &top[from..=to];
        let find = |after: usize, test: &dyn Fn(&str) -> bool| {
            span.iter()
                .enumerate()
                .skip(after + 1)
                .find(|(_, event)| test(&event.label))
                .map(|(index, _)| index)
        };
        let is_append = |label: &str| label.starts_with("append") && !label.contains("(nothing)");
        let step_end = find(0, &|label| is_append(label) && label.contains("StepEnd"));
        let notice = |after: Option<usize>| {
            after.and_then(|after| find(after, &|label| label.starts_with("compaction.notice")))
        };
        let first_notice = notice(step_end);
        let results = find(first_notice.or(step_end).unwrap_or(0), &|label| {
            is_append(label) && label.contains("BatchEnd")
        });
        let second_notice = notice(results);
        let phase = |index: usize| {
            let at_or_before = |anchor: Option<usize>| anchor.is_some_and(|anchor| index <= anchor);
            if at_or_before(step_end) {
                Phase::Answer
            } else if at_or_before(first_notice) {
                Phase::Notice
            } else if at_or_before(results) {
                Phase::Batch
            } else if at_or_before(second_notice) {
                Phase::Notice
            } else {
                Phase::Request
            }
        };
        let mut items = Vec::new();
        if step == 0 {
            let part = if is_append(&span[0].label) {
                PARTS[1]
            } else {
                Phase::Request.part()
            };
            items.push((span[0].label.clone(), span[0].end - span[0].start, part));
        }
        for (index, event) in span.iter().enumerate().skip(1) {
            let previous = span[index - 1];
            // The loop's own work since the previous call, less any entries read the
            // compaction provider made inside it.
            let (gap_start, gap_end) = (previous.end.max(previous.start), event.start);
            let inside: u64 = nested
                .iter()
                .filter(|read| read.start >= gap_start && read.end <= gap_end)
                .map(|read| read.end - read.start)
                .sum();
            let gap = gap_end.saturating_sub(gap_start).saturating_sub(inside);
            items.push((
                format!("gap: {} -> {}", previous.label, event.label),
                gap,
                phase(index).part(),
            ));
            if index == span.len() - 1 {
                break;
            }
            let part = if is_append(&event.label) {
                PARTS[1]
            } else if event.label.starts_with("tools:") || event.label.starts_with("model:") {
                "test doubles"
            } else {
                phase(index).part()
            };
            items.push((event.label.clone(), event.end - event.start, part));
        }
        let (start, end) = (span[0].start, span[span.len() - 1].start);
        for read in nested
            .iter()
            .filter(|read| read.start >= start && read.end <= end)
        {
            items.push((read.label.clone(), read.end - read.start, PARTS[3]));
        }
        let mut parts = BTreeMap::new();
        for (_, ns, part) in &items {
            *parts.entry(*part).or_insert(0) += ns;
            if *part != "test doubles" {
                *parts.entry(PARTS[6]).or_insert(0) += ns;
            }
        }
        out.push(Cycle {
            step,
            items,
            parts,
            model_call_at: span[span.len() - 1].start,
        });
    }
    out
}

/// p50, p95 and max of `samples`, in milliseconds.
fn stats(samples: &[u64]) -> (f64, f64, f64) {
    if samples.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let at = |q: f64| {
        let index = ((sorted.len() as f64 - 1.0) * q).round() as usize;
        sorted[index] as f64 / 1e6
    };
    (at(0.5), at(0.95), *sorted.last().unwrap() as f64 / 1e6)
}

fn durations_ns(samples: &[Duration]) -> Vec<u64> {
    samples.iter().map(|d| d.as_nanos() as u64).collect()
}

/// What the bench was asked to measure.
struct Args {
    token_counts: bool,
    out: PathBuf,
    store: PathBuf,
    sizes: Vec<u64>,
    cases: Vec<String>,
    warmup: usize,
    steps: usize,
    /// Measured steps in the `open-log` case at sizes above the smallest, where one step takes
    /// tens of seconds; `steps` when not given.
    open_log_steps: Option<usize>,
    /// Warm-up steps in the `open-log` case at sizes above the smallest; `warmup` when not given.
    open_log_warmup: Option<usize>,
    keep: u64,
    seal_every: u64,
    /// Whether the history's runs stay owed to billing, as on a module with no usage export.
    unbilled: bool,
    /// Whether each case's sizes are measured side by side, their model calls taking turns.
    interleave: bool,
    opens: usize,
}

/// The cases the bench knows: how the history behind the view's cut is kept, or, in `no-cut`,
/// a view that never cuts but leaves out the output of every tool call but the newest, as a
/// worker's view does.
const CASES: [&str; 4] = ["sealed-once", "sealed-often", "open-log", "no-cut"];

impl Args {
    fn parse() -> Self {
        let mut args = Self {
            token_counts: false,
            out: PathBuf::from("target/scratch/compacted-step-cost/results"),
            store: PathBuf::from("target/scratch/compacted-step-cost/store"),
            sizes: vec![1_000, 150_000, 500_000],
            cases: CASES.iter().map(|case| (*case).to_owned()).collect(),
            warmup: 10,
            steps: 60,
            open_log_steps: None,
            open_log_warmup: None,
            keep: 300,
            seal_every: 2_000,
            unbilled: false,
            interleave: false,
            opens: 3,
        };
        let list = |value: &str| -> Vec<String> {
            value
                .split(',')
                .map(|item| item.trim().to_owned())
                .collect()
        };
        let mut given = std::env::args().skip(1);
        while let Some(flag) = given.next() {
            let value = given
                .next()
                .unwrap_or_else(|| panic!("{flag} needs a value"));
            match flag.as_str() {
                "--token-counts" => args.token_counts = value.parse().unwrap(),
                "--out" => args.out = PathBuf::from(value),
                "--store" => args.store = PathBuf::from(value),
                "--sizes" => args.sizes = list(&value).iter().map(|s| s.parse().unwrap()).collect(),
                "--cases" => args.cases = list(&value),
                "--warmup" => args.warmup = value.parse().unwrap(),
                "--steps" => args.steps = value.parse().unwrap(),
                "--open-log-steps" => args.open_log_steps = Some(value.parse().unwrap()),
                "--open-log-warmup" => args.open_log_warmup = Some(value.parse().unwrap()),
                "--keep" => args.keep = value.parse().unwrap(),
                "--seal-every" => args.seal_every = value.parse().unwrap(),
                "--unbilled" => args.unbilled = value.parse().unwrap(),
                "--interleave" => args.interleave = value.parse().unwrap(),
                "--opens" => args.opens = value.parse().unwrap(),
                other => panic!("unknown flag {other}"),
            }
        }
        for case in &args.cases {
            assert!(
                CASES.contains(&case.as_str()),
                "a case is one of {CASES:?}, not {case}"
            );
        }
        args
    }
}

/// What opening the session's actor costs.
#[derive(Default)]
struct OpenCosts {
    /// `Session::load` from the session's files.
    load: Vec<Duration>,
    /// Seeding the live projection subscriptions read from (`store_fanout::measure_seed`).
    seed: Vec<Duration>,
    /// The actor built over the loaded session, serving, until it answers its first command
    /// (its recovery included).
    actor: Vec<Duration>,
    /// The open log's records and the window's messages when it opened.
    open_log_records: usize,
    window_start: u64,
}

/// Opens the session under `root` `samples` times, as the module opens a session's actor.
async fn open_costs(root: &Path, parts: &SharedParts, samples: usize) -> OpenCosts {
    let mut costs = OpenCosts::default();
    for _ in 0..samples {
        let started = Instant::now();
        let session = Session::load(
            LocalBackend::new(root.to_path_buf()),
            SessionAddr(ADDR.into()),
            &triple(),
            &deployment(),
        )
        .await
        .unwrap();
        costs.load.push(started.elapsed());
        costs.window_start = session.conversation.window_start();
        costs.open_log_records = session.open_log_records().await.unwrap().len();
        let started = Instant::now();
        broca_module::store_fanout::measure_seed(&session)
            .await
            .unwrap();
        costs.seed.push(started.elapsed());
        let timeline = Timeline::new();
        let executor = BenchExecutor {
            provider: BenchProvider::new(Vec::new(), Arc::clone(&timeline)),
            tool_text: String::new(),
            compactor: Compactor::new(Arc::clone(&timeline)),
            timeline,
            seals: true,
        };
        let started = Instant::now();
        let (commands, served) = serve(root, session, Arc::new(executor), parts);
        idle(&commands).await;
        costs.actor.push(started.elapsed());
        drop(commands);
        served.await.unwrap().close().await.unwrap();
    }
    costs
}

/// A store actor over `session`, kept under `root`, as the module opens one: with finished
/// runs' facts kept, and serving subscriptions over read-only handles on the session's files.
fn serve(
    root: &Path,
    session: Session<LocalBackend>,
    executor: Arc<BenchExecutor>,
    parts: &SharedParts,
) -> (
    mpsc::UnboundedSender<LineageCmd>,
    tokio::task::JoinHandle<Session<LocalBackend>>,
) {
    let readers_root = root.to_path_buf();
    let actor = StoreActor::new(bind(), session, executor, Box::new(parts.clone()))
        .with_settled(Arc::new(broca_module::store_status::SettledRuns::default()))
        .with_readers(Arc::new(move || LocalBackend::new(readers_root.clone())));
    let (commands, receiver) = mpsc::unbounded_channel();
    (commands, tokio::spawn(actor.serve(receiver)))
}

/// One size and case, measured.
struct Measured {
    size: u64,
    case: String,
    built: Built,
    cycles: Vec<Cycle>,
    warmup_cycles: Vec<Cycle>,
    before: OpenCosts,
    after: OpenCosts,
    notes: Vec<String>,
    wire_bytes: Vec<usize>,
    /// The view the run applied, and the provider rules its calls stated.
    view: View,
    rules: Option<Value>,
    /// What planning that view over the session's window costs (see [`planner_costs`]).
    planner: Vec<Duration>,
}

/// Sends a run of `warmup + steps` tool steps and an answer to the session under `root` and
/// measures each step cycle: `warmup` steps, then `steps` steps measured. The actor's opening
/// costs are measured before the run and after it.
async fn measure(
    args: &Args,
    built: &Built,
    case: &str,
    steps: (usize, usize),
    parts: &SharedParts,
) -> Measured {
    let before = open_costs(&built.root, parts, args.opens).await;
    let mut measured = run_measured(built, case, steps, parts, None).await;
    measured.before = before;
    measured.after = open_costs(&built.root, parts, args.opens).await;
    measured.planner = planner_costs(&built.root, &measured, args.opens).await;
    measured
}

/// [`measure`] for each of `runs` (a session and its warm-up and measured steps) side by side:
/// every actor's opening costs first, then the runs at the same time, their model calls taking
/// turns in the order given, then every actor's opening costs again.
async fn measure_interleaved(
    args: &Args,
    runs: &[(Built, (usize, usize))],
    case: &str,
    parts: &SharedParts,
) -> Vec<Measured> {
    let mut before = Vec::new();
    for (built, _) in runs {
        before.push(open_costs(&built.root, parts, args.opens).await);
    }
    let turnstile = Turnstile::new(runs.len());
    let mut measured =
        futures::future::join_all(runs.iter().enumerate().map(|(index, (built, steps))| {
            let seat = Seat {
                turnstile: Arc::clone(&turnstile),
                index,
            };
            run_measured(built, case, *steps, parts, Some(seat))
        }))
        .await;
    for ((one, before), (built, _)) in measured.iter_mut().zip(before).zip(runs) {
        one.before = before;
        one.after = open_costs(&built.root, parts, args.opens).await;
        one.planner = planner_costs(&built.root, one, args.opens).await;
    }
    let check = turn_check(&measured);
    println!("{case}: {check}");
    for one in &mut measured {
        one.notes.push(check.clone());
    }
    measured
}

/// Whether the model calls of runs measured side by side took turns: merged in time order from
/// their step cycles, as the step files record them, each call should be the next run's in
/// round-robin order among the runs that still make calls.
fn turn_check(runs: &[Measured]) -> String {
    let calls: Vec<Vec<u64>> = runs
        .iter()
        .map(|run| {
            run.warmup_cycles
                .iter()
                .chain(&run.cycles)
                .map(|cycle| cycle.model_call_at)
                .collect()
        })
        .collect();
    let sizes: Vec<String> = runs.iter().map(|run| run.size.to_string()).collect();
    format!(
        "Interleaved with the {} messages runs: of the {} model calls their step cycles end at, \
         merged in time order, {} fell out of round-robin turn.",
        sizes.join(" and "),
        calls.iter().map(Vec::len).sum::<usize>(),
        out_of_turn(&calls)
    )
}

/// How many of the calls in `runs` (each run's call times), merged in time order, aren't the
/// call of the run whose turn it is, round-robin in run order among the runs with calls left.
fn out_of_turn(runs: &[Vec<u64>]) -> usize {
    let mut calls: Vec<(u64, usize)> = runs
        .iter()
        .enumerate()
        .flat_map(|(seat, times)| times.iter().map(move |at| (*at, seat)))
        .collect();
    calls.sort_unstable();
    let mut left: Vec<usize> = runs.iter().map(Vec::len).collect();
    let (mut expected, mut out_of_turn) = (0, 0);
    for &(_, seat) in &calls {
        if seat != expected {
            out_of_turn += 1;
        }
        left[seat] -= 1;
        if let Some(next) = (1..=left.len())
            .map(|step| (seat + step) % left.len())
            .find(|next| left[*next] > 0)
        {
            expected = next;
        }
    }
    out_of_turn
}

/// Sends a run of `warmup + steps` tool steps and an answer to the session under `root` and
/// measures each step cycle: `warmup` steps, then `steps` steps measured. With `seat`, the run's
/// model calls take their turns at its turnstile. The opening costs are left for the caller.
async fn run_measured(
    built: &Built,
    case: &str,
    (warmup, steps): (usize, usize),
    parts: &SharedParts,
    seat: Option<Seat>,
) -> Measured {
    let root = &built.root;
    let timeline = Timeline::new();
    let compactor = Compactor::new(Arc::clone(&timeline));
    let session = Session::load(
        LocalBackend::new(root.clone()),
        SessionAddr(ADDR.into()),
        &triple(),
        &deployment(),
    )
    .await
    .unwrap();
    let view = if case == "no-cut" {
        dropping_view(built.views + 1, &session.conversation, built.cut.1)
    } else {
        summary_view(built.views + 1, &built.cut.0)
    };
    *compactor.view.lock().unwrap() = Some(view.clone());
    // The provider starts with every entry the session held before the run already read, so
    // each read after a notice asks only for the run's new entries.
    *compactor.cursor.lock().unwrap() = built.messages;
    let total = warmup + steps;
    let mut script: Vec<_> = (1..=total)
        .map(|n| tool_step(&format!("t{n:05}")))
        .collect();
    script.push(answer_step("done"));
    let executor = Arc::new(BenchExecutor {
        provider: BenchProvider::new(script, Arc::clone(&timeline)).seated(seat.clone()),
        tool_text: tool_text(2_048),
        compactor: Arc::clone(&compactor),
        timeline: Arc::clone(&timeline),
        seals: case.starts_with("sealed"),
    });
    let (commands, served) = serve(root, session, Arc::clone(&executor), parts);
    *compactor.reader.lock().unwrap() = Some(EntriesReader {
        commands: commands.clone(),
        root: root.clone(),
        parts: parts.clone(),
    });
    let mut submission = request(
        "Carry on with the review: read each file the plan names and report what changed.",
        // Another model than the history's runs, so the first step rides the compaction
        // provider, which hands the view over there.
        "m2",
        (total + 5) as u32,
    );
    if case == "no-cut" {
        // The history's runs are copies of small template runs, so the first request's size
        // estimate counts the whole uncut request as growth since the last request measured;
        // past a few thousand messages that passes a 1,000,000-token window and the run would
        // end at the context ceiling before its first model call.
        submission.context_limit = Some(NO_CUT_CONTEXT_LIMIT);
    }
    idle(&commands).await;
    timeline.take();
    let outcome = send(&commands, "measure001", submission, None).await;
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
    idle(&commands).await;
    if let Some(seat) = &seat {
        seat.turnstile.leave(seat.index);
    }
    let events = timeline.take();
    *compactor.reader.lock().unwrap() = None;
    drop(commands);
    let session = served.await.unwrap();

    let mut notes = Vec::new();
    let calls = executor.provider.inner.calls();
    assert_eq!(
        calls.len(),
        total + 1,
        "every scripted step was called; compaction calls: {:?}",
        compactor.methods.lock().unwrap()
    );
    let summarised = |call: &broca_protocol::request::CallOptions| {
        call.prompt.iter().any(|message| {
            message.content.iter().any(|block| {
                matches!(block, ContentBlock::Text { text }
                    if text.starts_with("Summary of the session so far"))
            })
        })
    };
    for (index, call) in calls.iter().enumerate() {
        assert!(
            summarised(call),
            "step {} rendered no summary: the view didn't apply",
            index + 1
        );
    }
    notes.push(format!(
        "Every step's request rendered the view: the summary and {}..{} messages (first step, \
         last step).",
        calls.first().unwrap().prompt.len(),
        calls.last().unwrap().prompt.len()
    ));
    let count = |label: &str| {
        events
            .iter()
            .filter(|event| event.label.starts_with(label))
            .count()
    };
    notes.push(format!(
        "Compaction calls: {} rides, {} notices, {} setups; seals at a cut: {} made, {} skipped; \
         ledger seals: {}.",
        count("compaction.ride"),
        count("compaction.notice"),
        count("compaction.setup"),
        count("seal_at_entry") - count("seal_at_entry (skipped)"),
        count("seal_at_entry (skipped)"),
        count("seal_ledger"),
    ));
    notes.push(format!(
        "Entries reads: {:?}.",
        compactor.outcomes.lock().unwrap()
    ));
    notes.push(format!(
        "After the run the window starts at ordinal {} of {} messages.",
        session.conversation.window_start(),
        session.conversation.placed
    ));
    session.close().await.unwrap();

    let all = cycles(&events);
    let (warmup_cycles, cycles): (Vec<Cycle>, Vec<Cycle>) =
        all.into_iter().partition(|cycle| cycle.step <= warmup);
    let wire_bytes = executor.provider.wire_bytes.lock().unwrap().clone();
    let rules = compactor.rules.lock().unwrap().clone();
    Measured {
        size: built.messages,
        case: case.to_owned(),
        built: built.clone(),
        cycles,
        warmup_cycles,
        before: OpenCosts::default(),
        after: OpenCosts::default(),
        notes,
        wire_bytes,
        view,
        rules,
        planner: Vec::new(),
    }
}

/// What planning the run's view over the session's window costs, at least `samples` times,
/// after the run: `render::plan_for_render`, the part of every render of a request that runs
/// before any message is rendered (the view's checksum, and the shared view crate's
/// `edits_for_transcript`, `seal_with_policy` and `plan_view`, each over every window entry or
/// every edit). Measured only where the view's targets are all in the window (`open-log`,
/// `no-cut`): after a seal the transcript holds sealed entries the window doesn't.
async fn planner_costs(root: &Path, measured: &Measured, samples: usize) -> Vec<Duration> {
    if !matches!(measured.case.as_str(), "open-log" | "no-cut") {
        return Vec::new();
    }
    let Some(rules) = measured
        .rules
        .clone()
        .and_then(|rules| serde_json::from_value(rules).ok())
    else {
        return Vec::new();
    };
    let session = Session::load(
        LocalBackend::new(root.to_path_buf()),
        SessionAddr(ADDR.into()),
        &triple(),
        &deployment(),
    )
    .await
    .unwrap();
    let entries = window_entries(&session.conversation);
    session.close().await.unwrap();
    (0..samples.max(5))
        .map(|_| {
            let started = Instant::now();
            broca_core::compaction_v2::render::plan_for_render(
                &entries,
                Some(&measured.view),
                &rules,
                broca_core::compaction_v2::Floor::none(),
            )
            .expect("the run's view plans over the window");
            started.elapsed()
        })
        .collect()
}

/// The output of `command` with `args`, trimmed, or what failed.
fn command_output(command: &str, args: &[&str]) -> String {
    std::process::Command::new(command)
        .args(args)
        .output()
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_else(|error| format!("({command} failed: {error})"))
}

fn machine() -> String {
    format!(
        "{} ({} cores, {} GB), {} {}",
        command_output("sysctl", &["-n", "machdep.cpu.brand_string"]),
        command_output("sysctl", &["-n", "hw.ncpu"]),
        command_output("sysctl", &["-n", "hw.memsize"])
            .parse::<u64>()
            .map_or_else(|_| "?".into(), |bytes| (bytes >> 30).to_string()),
        command_output("uname", &["-s"]),
        command_output("uname", &["-r"]),
    )
}

fn ms3((p50, p95, max): (f64, f64, f64)) -> String {
    format!("{p50:.3} / {p95:.3} / {max:.3}")
}

/// Each part's samples over `cycles`, in nanoseconds.
fn part_samples(cycles: &[Cycle], part: &str) -> Vec<u64> {
    cycles
        .iter()
        .map(|cycle| cycle.parts.get(part).copied().unwrap_or(0))
        .collect()
}

/// Whether `now` keeps the rule against `base`: at most twice the base, or under 2 ms.
fn verdict(base: f64, now: f64) -> &'static str {
    if now < 2.0 || now <= 2.0 * base {
        "pass"
    } else {
        "FAIL"
    }
}

/// Writes each measured size and case's raw data under `out`.
fn write_raw(out: &Path, nominal: u64, measured: &Measured) {
    use std::io::Write as _;
    let name = format!("{nominal}-{}", measured.case);
    let mut steps = std::fs::File::create(out.join(format!("steps-{name}.jsonl"))).unwrap();
    for (warmup, cycle) in measured
        .warmup_cycles
        .iter()
        .map(|cycle| (true, cycle))
        .chain(measured.cycles.iter().map(|cycle| (false, cycle)))
    {
        let line = json!({
            "size": nominal,
            "messages": measured.size,
            "case": measured.case,
            "step": cycle.step,
            "warmup": warmup,
            "model_call_at_ns": cycle.model_call_at,
            "parts_ns": cycle.parts,
            "items": cycle.items.iter().map(|(label, ns, part)| json!([label, ns, part])).collect::<Vec<_>>(),
        });
        writeln!(steps, "{line}").unwrap();
    }
    let mut labels: BTreeMap<(&str, &str), Vec<u64>> = BTreeMap::new();
    for cycle in &measured.cycles {
        let mut seen: BTreeMap<(&str, &str), u64> = BTreeMap::new();
        for (label, ns, part) in &cycle.items {
            *seen.entry((part, label.as_str())).or_insert(0) += ns;
        }
        for (key, ns) in seen {
            labels.entry(key).or_default().push(ns);
        }
    }
    let mut tsv = String::from("part\tlabel\tsteps\tp50_ms\tp95_ms\tmax_ms\n");
    for ((part, label), samples) in &labels {
        let (p50, p95, max) = stats(samples);
        tsv.push_str(&format!(
            "{part}\t{label}\t{}\t{p50:.4}\t{p95:.4}\t{max:.4}\n",
            samples.len()
        ));
    }
    std::fs::write(out.join(format!("labels-{name}.tsv")), tsv).unwrap();
    if !measured.planner.is_empty() {
        std::fs::write(
            out.join(format!("planner-{name}.json")),
            json!({"planner_ns": durations_ns(&measured.planner)}).to_string(),
        )
        .unwrap();
    }
}

/// Request build's median (p50) split, per size: the renders of the request's bytes (the size
/// estimate's canonical render and the provider's wire render, the `render (...)` items) and
/// the rest, with each one's slope per message against the size before, and what planning the
/// view costs offline (see [`planner_costs`]).
fn request_split(measured: &[(u64, Measured)]) -> String {
    let mut out = String::from(
        "\nRequest build, split: byte assembly is the `render (...)` items (the size estimate's \
         canonical render and the provider's wire render); the rest is everything else request \
         build holds. A slope is the p50's change per message against the size before, in µs. \
         The planner column is one plan of the run's view over the session's window after the \
         run, timed offline (`render::plan_for_render`: the view's checksum and the shared view \
         crate's `edits_for_transcript`, `seal_with_policy` and `plan_view`); each render of a \
         request runs it once, before rendering any message.\n\n| size | request build p50 ms | byte assembly p50 ms | rest p50 ms | \
         assembly slope µs/msg | rest slope µs/msg | planner p50 ms (offline) |\n\
         |---|---|---|---|---|---|---|\n",
    );
    let mut previous: Option<(u64, f64, f64)> = None;
    for (size, one) in measured {
        let assembly: Vec<u64> = one
            .cycles
            .iter()
            .map(|cycle| {
                cycle
                    .items
                    .iter()
                    .filter(|(label, _, part)| *part == PARTS[0] && label.starts_with("render ("))
                    .map(|(_, ns, _)| ns)
                    .sum()
            })
            .collect();
        let rest: Vec<u64> = one
            .cycles
            .iter()
            .zip(&assembly)
            .map(|(cycle, assembly)| {
                cycle
                    .parts
                    .get(PARTS[0])
                    .copied()
                    .unwrap_or(0)
                    .saturating_sub(*assembly)
            })
            .collect();
        let (assembly50, rest50) = (stats(&assembly).0, stats(&rest).0);
        let slope = |now: f64, before: f64, from: u64| {
            format!("{:.3}", (now - before) * 1000.0 / (*size - from) as f64)
        };
        let (assembly_slope, rest_slope) = match previous {
            Some((from, assembly, rest)) if *size > from => {
                (slope(assembly50, assembly, from), slope(rest50, rest, from))
            }
            _ => ("-".into(), "-".into()),
        };
        let planner = if one.planner.is_empty() {
            "-".into()
        } else {
            format!("{:.3}", stats(&durations_ns(&one.planner)).0)
        };
        out.push_str(&format!(
            "| {size} | {:.3} | {assembly50:.3} | {rest50:.3} | {assembly_slope} | {rest_slope} | \
             {planner} |\n",
            stats(&part_samples(&one.cycles, PARTS[0])).0,
        ));
        previous = Some((*size, assembly50, rest50));
    }
    out
}

/// The summary of every size of one case.
fn case_summary(case: &str, measured: &[(u64, Measured)]) -> String {
    let mut out = format!("\n## Case `{case}`\n\n");
    out.push_str(
        "Per step cycle (one model call to the next), over the measured steps after warm-up. \
         Milliseconds p50 / p95 / max; ratio is p50 / p95 against the smallest size; the rule \
         passes a part at most 2.0x its smallest-size cost, or under 2 ms.\n\n",
    );
    let Some((base_size, base)) = measured.first() else {
        return out;
    };
    out.push_str("| part |");
    for (size, _) in measured {
        out.push_str(&format!(" {size} msgs p50 / p95 / max |"));
    }
    for (size, _) in measured.iter().skip(1) {
        out.push_str(&format!(" {size} vs {base_size}: ratio p50 / p95, rule |"));
    }
    out.push_str("\n|---|");
    for _ in 0..(measured.len() * 2 - 1) {
        out.push_str("---|");
    }
    out.push('\n');
    for part in PARTS {
        out.push_str(&format!("| {part} |"));
        for (_, one) in measured {
            out.push_str(&format!(
                " {} |",
                ms3(stats(&part_samples(&one.cycles, part)))
            ));
        }
        let (b50, b95, _) = stats(&part_samples(&base.cycles, part));
        for (_, one) in measured.iter().skip(1) {
            let (n50, n95, _) = stats(&part_samples(&one.cycles, part));
            let ratio = |now: f64, base: f64| {
                if base > 0.0 {
                    format!("{:.2}x", now / base)
                } else {
                    "-".into()
                }
            };
            out.push_str(&format!(
                " {} / {}, {} / {} |",
                ratio(n50, b50),
                ratio(n95, b95),
                verdict(b50, n50),
                verdict(b95, n95)
            ));
        }
        out.push('\n');
    }
    out.push_str(&request_split(measured));
    out.push_str("\nNot per step: opening the session's actor, and seeding subscriptions.\n\n");
    out.push_str(
        "| size | when | open log records | window starts at | Session::load ms p50 / p95 / max | \
         actor serving to first answer ms | subscription seed ms |\n|---|---|---|---|---|---|---|\n",
    );
    for (size, one) in measured {
        for (when, costs) in [
            ("before the run", &one.before),
            (
                if matches!(case, "open-log" | "no-cut") {
                    "after the run (still unsealed)"
                } else {
                    "after the run (sealed at the cut)"
                },
                &one.after,
            ),
        ] {
            out.push_str(&format!(
                "| {size} | {when} | {} | {} | {} | {} | {} |\n",
                costs.open_log_records,
                costs.window_start,
                ms3(stats(&durations_ns(&costs.load))),
                ms3(stats(&durations_ns(&costs.actor))),
                ms3(stats(&durations_ns(&costs.seed))),
            ));
        }
    }
    for (size, one) in measured {
        out.push_str(&format!("\n### {size} messages, `{case}`\n\n"));
        out.push_str(&format!(
            "Built {} messages in {:.1} s, {:.1} MB on disk, sealed at {} cuts as it was written; \
             the view {} through ordinal {}. {} measured steps after {} warm-up steps. \
             Anthropic request body p50 {} KB.\n\n",
            one.built.messages,
            one.built.written.as_secs_f64(),
            one.built.bytes as f64 / 1e6,
            one.built.views,
            if case == "no-cut" {
                "drops tool outputs"
            } else {
                "cuts"
            },
            one.built.cut.1,
            one.cycles.len(),
            one.warmup_cycles.len(),
            {
                let mut bytes = one.wire_bytes.clone();
                bytes.sort_unstable();
                bytes.get(bytes.len() / 2).copied().unwrap_or(0) / 1024
            }
        ));
        for note in &one.notes {
            out.push_str(&format!("- {note}\n"));
        }
        if let Some(first) = one.warmup_cycles.first() {
            out.push_str(
                "- The run's start to its first model call (it rides, applies the view and, \
                 unless `open-log`, seals at the cut): ",
            );
            let parts: Vec<String> = first
                .parts
                .iter()
                .map(|(part, ns)| format!("{part} {:.1} ms", *ns as f64 / 1e6))
                .collect();
            out.push_str(&parts.join(", "));
            out.push('\n');
            let mut slowest: Vec<&(String, u64, &str)> = first.items.iter().collect();
            slowest.sort_by_key(|(_, ns, _)| std::cmp::Reverse(*ns));
            out.push_str("  Its slowest items: ");
            let items: Vec<String> = slowest
                .iter()
                .take(5)
                .map(|(label, ns, _)| format!("`{label}` {:.1} ms", *ns as f64 / 1e6))
                .collect();
            out.push_str(&items.join("; "));
            out.push('\n');
        }
        // The items that cost most in a typical measured step.
        let mut labels: BTreeMap<(&str, &str), Vec<u64>> = BTreeMap::new();
        for cycle in &one.cycles {
            let mut seen: BTreeMap<(&str, &str), u64> = BTreeMap::new();
            for (label, ns, part) in &cycle.items {
                *seen.entry((part, label.as_str())).or_insert(0) += ns;
            }
            for (key, ns) in seen {
                labels.entry(key).or_default().push(ns);
            }
        }
        let mut rows: Vec<_> = labels
            .iter()
            .map(|((part, label), samples)| (stats(samples), *part, *label, samples.len()))
            .collect();
        rows.sort_by(|a, b| b.0 .0.total_cmp(&a.0 .0));
        out.push_str("\n| part | item (call, or the loop's work between two calls) | steps | p50 / p95 / max ms |\n|---|---|---|---|\n");
        for (stat, part, label, count) in rows.iter().take(15) {
            out.push_str(&format!(
                "| {part} | `{}` | {count} | {} |\n",
                label.replace('|', "/"),
                ms3(*stat)
            ));
        }
    }
    out
}

fn main() {
    let args = Args::parse();
    std::fs::create_dir_all(&args.out).unwrap();
    std::fs::create_dir_all(&args.store).unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let started = Instant::now();
        let mut header = format!(
            "# Per-step cost of a compacted head on broca's new session storage\n\n\
             Machine: {}. Load at start: `{}`.\n\n\
             Command: `cargo run --release -p broca-session-convert --example compacted_step_cost -- {}`\n\n",
            machine(),
            command_output("uptime", &[]),
            std::env::args().skip(1).collect::<Vec<_>>().join(" "),
        );
        let parts = SharedParts::new(Box::new(MemoryParts::default()));
        let mut templates = Vec::new();
        for (turns, kb) in [(1, 2), (3, 1), (2, 6), (4, 3)] {
            templates.push(template(turns, kb * 1024, &parts).await);
        }
        if args.token_counts {
            token_counts::measure(&args, &templates, &parts).await;
            return;
        }
        header.push_str(&format!(
            "Session shape: runs built from {} template runs the new loop wrote on the \
             compaction contract (a user input, one to four tool steps, each with reasoning, a \
             sentence and one `read` call whose result is 1 to 6 KB, then a final answer), \
             {:.1} messages per run on average; encrypted, on local files; {}. The run measured \
             takes {} tool steps (2 KB results) and an answer (`open-log` above the smallest \
             size: {} warm-up and {} measured steps); the view inserts a summary and cuts \
             through all but about the newest {} messages (`no-cut`: cuts nothing, and replaces \
             the output of every tool call before those messages; its run is admitted with a \
             20,000,000-token window, since the first request's size estimate counts the whole \
             uncut request as growth over the copied history's small requests). `sealed-often` seals at a new \
             cut about every {} messages.\n",
            templates.len(),
            templates.iter().map(|t| t.messages).sum::<u64>() as f64 / templates.len() as f64,
            if args.unbilled {
                "every history run left owed to billing (a module with no usage export)"
            } else {
                "every history run billed (a module that keeps the usage export)"
            },
            args.warmup + args.steps,
            args.open_log_warmup.unwrap_or(args.warmup),
            args.open_log_steps.unwrap_or(args.steps),
            args.keep,
            args.seal_every,
        ));
        if args.interleave {
            header.push_str(
                "\nInterleaved (`--interleave true`): every size's session was built first; then, \
                 case by case, the sizes' measured runs ran at the same time, their model calls \
                 taking turns round-robin in the order the sizes are given, so whatever else \
                 loaded the machine fell on every size alike. A run waiting for its turn is the \
                 `model: turnstile wait` item, counted with the test doubles, outside every part \
                 and the total. The actors' opening costs were measured before the runs started \
                 and after they all ended. Each size's notes say whether the calls kept their \
                 turns.\n",
            );
        }
        let mut by_case: BTreeMap<String, Vec<(u64, Measured)>> = BTreeMap::new();
        let smallest = args.sizes.iter().copied().min().unwrap_or(0);
        // The warm-up and measured steps of `case` at `size`.
        let steps_of = |case: &str, size: u64| match (args.open_log_warmup, args.open_log_steps) {
            (warmup, steps) if case == "open-log" && size > smallest => {
                (warmup.unwrap_or(args.warmup), steps.unwrap_or(args.steps))
            }
            _ => (args.warmup, args.steps),
        };
        // `base`, copied to where `case` measures it at `size`.
        let copied = |base: &Built, case: &str, size: u64| {
            let root = args.store.join(format!("{size}-{case}"));
            copy_dir(&base.root, &root);
            let mut copy = base.clone();
            copy.root = root;
            copy
        };
        // Records `measured` and rewrites the summary with every case measured so far.
        let mut record = |case: &str, size: u64, measured: Measured, load: &str| {
            println!(
                "{size} {case}: total p50 / p95 / max {} ms (load before: {load})",
                ms3(stats(&part_samples(&measured.cycles, "total")))
            );
            write_raw(&args.out, size, &measured);
            let mut measured = measured;
            measured.notes.push(format!("Load before this case: `{load}`."));
            let _ = std::fs::remove_dir_all(&measured.built.root);
            by_case
                .entry(case.to_owned())
                .or_default()
                .push((size, measured));
            let mut summary = header.clone();
            for (case, measured) in &by_case {
                summary.push_str(&case_summary(case, measured));
            }
            std::fs::write(args.out.join("summary.md"), &summary).unwrap();
        };
        if args.interleave {
            // Every size's history, built before any run is measured.
            let mut bases: BTreeMap<(u64, bool), Built> = BTreeMap::new();
            for &size in &args.sizes {
                for case in &args.cases {
                    let often = case == "sealed-often";
                    if let std::collections::btree_map::Entry::Vacant(slot) =
                        bases.entry((size, often))
                    {
                        slot.insert(build_base(&args, size, often, &templates, &parts).await);
                    }
                }
            }
            for case in &args.cases {
                let runs: Vec<(Built, (usize, usize))> = args
                    .sizes
                    .iter()
                    .map(|&size| {
                        let base = &bases[&(size, case == "sealed-often")];
                        (copied(base, case, size), steps_of(case, size))
                    })
                    .collect();
                let load = command_output("uptime", &[]);
                let measured = measure_interleaved(&args, &runs, case, &parts).await;
                for (&size, measured) in args.sizes.iter().zip(measured) {
                    record(case, size, measured, &load);
                }
            }
            for base in bases.values() {
                let _ = std::fs::remove_dir_all(&base.root);
            }
        } else {
            for &size in &args.sizes {
                // One build per way of keeping the history, copied for each case that measures
                // it.
                let mut bases: BTreeMap<bool, Built> = BTreeMap::new();
                for case in &args.cases {
                    let often = case == "sealed-often";
                    if let std::collections::btree_map::Entry::Vacant(slot) = bases.entry(often) {
                        slot.insert(build_base(&args, size, often, &templates, &parts).await);
                    }
                }
                for case in &args.cases {
                    let copy = copied(&bases[&(case == "sealed-often")], case, size);
                    let load = command_output("uptime", &[]);
                    let measured = measure(&args, &copy, case, steps_of(case, size), &parts).await;
                    record(case, size, measured, &load);
                }
                for base in bases.values() {
                    let _ = std::fs::remove_dir_all(&base.root);
                }
            }
        }
        println!(
            "done in {:.1} s; load at the end: {}",
            started.elapsed().as_secs_f64(),
            command_output("uptime", &[])
        );
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Three seats making 3, 1 and 2 calls, each from its own task, the later seats arriving
    /// first: the turnstile lets the calls in round-robin in seat order, and passes over a seat
    /// once it made its last call.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_turnstile_lets_calls_in_round_robin() {
        let turnstile = Turnstile::new(3);
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut tasks = Vec::new();
        for (seat, calls) in [3usize, 1, 2].into_iter().enumerate() {
            let (turnstile, order) = (Arc::clone(&turnstile), Arc::clone(&order));
            tasks.push(tokio::spawn(async move {
                for call in 0..calls {
                    tokio::time::sleep(Duration::from_millis(3 - seat as u64)).await;
                    turnstile
                        .enter(seat, call + 1 == calls, || {
                            order.lock().unwrap().push(seat);
                            0
                        })
                        .await;
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(*order.lock().unwrap(), [0, 1, 2, 0, 2, 0]);
    }

    /// A seat whose run ends without its last call leaves, and the others' calls go on.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_seat_that_leaves_holds_up_no_one() {
        let turnstile = Turnstile::new(2);
        turnstile.enter(0, false, || 0).await;
        turnstile.leave(0);
        let second = Arc::clone(&turnstile);
        let calls = tokio::spawn(async move {
            second.enter(1, false, || 0).await;
            second.enter(1, true, || 0).await;
        });
        tokio::time::timeout(Duration::from_secs(5), calls)
            .await
            .expect("the remaining seat's calls went in")
            .unwrap();
    }

    /// The check of the step files' call times: calls that alternate are all in turn, a run
    /// left alone once the other ran out is in turn, and two calls of one run in a row aren't.
    #[test]
    fn calls_out_of_turn_are_counted() {
        assert_eq!(out_of_turn(&[vec![1, 3, 5], vec![2, 4, 6]]), 0);
        assert_eq!(out_of_turn(&[vec![1, 3], vec![2, 4, 5, 6]]), 0);
        assert_eq!(out_of_turn(&[vec![1, 2, 5], vec![3, 4, 6]]), 2);
    }
}
