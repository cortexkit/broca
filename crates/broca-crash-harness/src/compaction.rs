//! Compaction crash scenarios use the core traits directly, but keep the file WAL and run loop
//! real: Setup from a scripted provider, and an asynchronous view announced by `view_ready`,
//! fetched with `view.get` at the first step boundary, recorded, rendered, and reported in
//! advance notices.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_core::compaction_provider::{
    lineage::MessageRef,
    notice::ViewReady,
    setup::{SetupAnswer, SetupRequest},
    view::{CompactionView, PartEdit, ViewGetAnswer, ViewGetRequest, ViewRange},
};
use broca_core::{
    CompactionPlane, CompactionUnavailable, CompactionViews, RunControl, RunRequest, StubReply,
    StubToolPlane,
};
use broca_protocol::content::{ContentBlock, ToolCall, ToolOutput};
use broca_protocol::{
    AssistantMessage, BindIdentity, FinishReason, MessageId, NormalizedEvent, ProviderControl,
    RunFinishReason, RunId, SubmissionId, ToolCallId, Usage,
};
use broca_wal::{CompactionMessageKind, OpaqueJson, WalRecord};
use serde_json::{json, Value};

use crate::{
    wal_path, HarnessPlane, InFlight, KillPoint, KillReport, KillTrigger, LogImage, Render,
    Resumed, RunnerHandle, Scenario, Suite,
};

const PROVIDER: &str = "compactor";
const APPLIED_HEAD: &str = "durable compacted head";

const CEILING_NOW_MS: u64 = 1_000_000;
const CEILING_WAIT_MS: u64 = 5;

struct CeilingTime {
    root: PathBuf,
    clock: broca_core::Clock,
}

impl CeilingTime {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "ceiling-clock-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join("test-clock"),
            json!({"now_ms": CEILING_NOW_MS}).to_string(),
        )
        .unwrap();
        Self {
            clock: broca_core::Clock::for_state_root(&root).unwrap(),
            root,
        }
    }

    fn expire(&self) {
        if self.clock.now_ms() == CEILING_NOW_MS + CEILING_WAIT_MS {
            return;
        }
        std::fs::write(
            self.root.join("test-clock"),
            json!({"now_ms": CEILING_NOW_MS + CEILING_WAIT_MS}).to_string(),
        )
        .unwrap();
        self.clock.refresh().unwrap();
    }
}

impl Drop for CeilingTime {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn request() -> RunRequest {
    let submission = serde_json::from_value(json!({
        "model":{"provider_module_id":"stub","model_id":"model"},
        "input":[{"role":"user","content":[{"type":"text","text":"original user"}]}],
        "tools":[{"name":"edit","description":"edit","input_schema":{"type":"object"}}]
    }))
    .unwrap();
    let mut request = RunRequest::from_submission(
        submission,
        RunId("compaction-run".into()),
        BindIdentity::new("/crash-harness/compaction", "caller", "session"),
        SubmissionId("compaction-send".into()),
    );
    request.policy_allowed.insert("edit".into());
    request
}

fn control() -> RunControl {
    let control = RunControl {
        manifest: Some(Arc::new(broca_wal::PlanManifest {
            rich_ui: None,
            plan: OpaqueJson::from_json_text(
                json!({"compaction_item":{"provider":PROVIDER,"preset":"summary","params":{}},"composition":{}})
                    .to_string(),
            )
            .unwrap(),
            plan_identity: "compaction-plan".into(),
            composition_digest: "composition".into(),
            tools: Arc::new(vec![]),
            system_text: String::new(),
            system_digest: String::new(),
            join_version: 1,
            role_versions: [("compaction-provider".into(), "v1".into())].into(),
            fetches: vec![],
            declarations: vec![],
        })),
        ..Default::default()
    };
    with_compaction(control, Arc::new(ScriptedCompaction::default()))
}

/// `control` with `provider` as the session's compaction provider: the one scripted provider
/// serves Setup and `view.get` and is the source of the run's `view_ready` events.
fn with_compaction(mut control: RunControl, provider: Arc<ScriptedCompaction>) -> RunControl {
    if let Some(time) = &provider.ceiling_time {
        control.clock = Some(time.clock.clone());
    }
    control.compaction_plane = Some(provider.clone());
    control.compaction_views = Some(provider);
    control
}

/// Bind the signed scenario's announcements to durable progress, including on restart. The
/// runner also queries newer versions before applying a fetched view, so a version filter
/// alone cannot show whether the run has reached the next view's publication point.
pub(crate) fn schedule_views(control: &mut RunControl, request: &RunRequest, state_root: &Path) {
    if request.run_id != signed_request().run_id {
        return;
    }
    if let Some(source) = control.compaction_views.take() {
        control.compaction_views = Some(Arc::new(SignedAnnouncements {
            source,
            wal: wal_path(state_root, &request.session),
        }));
    }
}

/// Model a session enrolled before view features existed. Seeding the actual WAL makes both
/// an ordinary run and a killed child recover the old declaration instead of calling Setup.
pub(crate) fn seed_legacy_setup(store: &impl broca_core::WalStore, request: &RunRequest) {
    if request.session != legacy_request().session && request.session != signed_request().session {
        return;
    }
    let mut wal = store.open(&request.session).unwrap();
    if !wal.replay().unwrap().is_empty() {
        return;
    }
    let replacement = Arc::new(vec![serde_json::from_value(json!({
        "role":"user", "content":[{"type":"text", "text":"setup head"}]
    }))
    .unwrap()]);
    let setup = WalRecord::CompactionMessage {
        provider: PROVIDER.into(),
        kind: CompactionMessageKind::Setup,
        compaction_id: "summary".into(),
        version: 1,
        range: broca_wal::CompactionRange { from: 0, to: 0 },
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
    wal.append(1, &setup, true).unwrap();
}

struct SignedAnnouncements {
    source: Arc<dyn CompactionViews>,
    wal: PathBuf,
}

impl CompactionViews for SignedAnnouncements {
    fn newest_ready(
        &self,
        session_ref: &str,
        lineage_id: &str,
        after: Option<u64>,
    ) -> Option<ViewReady> {
        let records = LogImage::read(&self.wal).records();
        let applied_first = records.iter().any(|record| {
            matches!(
                record,
                WalRecord::CompactionViewApplied {
                    view_version: 2,
                    ..
                }
            )
        });
        let tool_result = records.iter().any(|record| {
            matches!(record, WalRecord::ToolResult { result, .. } if result.tool_call_id.0 == "edit-1")
        });
        // The second view edits the tool result. It cannot be published while the first view
        // is being fetched, or before the tool result exists. Reading the WAL rather than
        // counting queries preserves that publication order across every crash cut.
        let first = self.source.newest_ready(session_ref, lineage_id, None)?;
        let ready = if applied_first && tool_result {
            ViewReady::for_view(&second_view(session_ref, lineage_id))
        } else {
            first
        };
        after
            .is_none_or(|version| ready.view_version > version)
            .then_some(ready)
    }
}

fn assistant(id: &str, content: Vec<ContentBlock>, finish: FinishReason) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(id.into()),
                content,
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: finish,
            retries_used: 0,
        }),
    ]
}

fn scripts() -> Vec<Vec<NormalizedEvent>> {
    vec![assistant(
        "reply",
        vec![ContentBlock::Text {
            text: "done".into(),
        }],
        FinishReason::Stop,
    )]
}

fn tool_scripts() -> Vec<Vec<NormalizedEvent>> {
    vec![
        assistant(
            "tool-reply",
            vec![ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId("edit-1".into()),
                tool_name: "edit".into(),
                input: json!({}),
            })],
            FinishReason::ToolCalls,
        ),
        scripts().remove(0),
    ]
}

fn tools() -> StubToolPlane {
    StubToolPlane::new().with_tool("edit", |_| {
        StubReply::Output(ToolOutput::Text {
            text: "edited".into(),
        })
    })
}

const SCENARIO: Scenario = Scenario {
    name: "compaction",
    request,
    control,
    scripts,
    render: Render::Canonical,
    plane: tools,
    extension: None,
    expected: RunFinishReason::Completed,
};

struct SlowRide;

impl crate::scenario::ScenarioExtension for SlowRide {
    fn setup(&self, root: &std::path::Path, control: &mut RunControl, _: &mut crate::HarnessPlane) {
        std::fs::write(root.join("test-clock"), r#"{"now_ms":1000}"#).unwrap();
        let clock = broca_core::Clock::for_state_root(root).unwrap();
        control.clock = Some(clock.clone());
        control.compaction_signals = true;
        control.compaction_plane = Some(Arc::new(ScriptedCompaction {
            slow_ride: Some((root.to_path_buf(), clock)),
            ..Default::default()
        }));
        control.compaction_views = None;
    }
}

const RIDE_SCENARIO: Scenario = Scenario {
    name: "compaction-ride-wait",
    render: Render::CompactionChainReset,
    extension: Some(&SlowRide),
    ..SCENARIO
};

fn old_wait_started() -> WalRecord {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../broca-wal/tests/fixtures/pre_flow_scoped_run_started.json"
    ))
    .unwrap();
    serde_json::from_value(fixture["record"].clone()).unwrap()
}

fn old_wait_request() -> RunRequest {
    let WalRecord::RunStarted {
        run_id,
        session,
        config,
        input,
        scope,
        principal,
        project_id,
        submission_id,
        send_id,
        request_identity_digest,
        ..
    } = old_wait_started()
    else {
        unreachable!()
    };
    let submission = serde_json::from_value(json!({"model":config.model,"input":input,"tools":[],"stop_when":[{"type":"step_count","n":1}]})).unwrap();
    let mut request =
        RunRequest::from_submission(submission, run_id, session, SubmissionId("old-wait".into()));
    request.scope = scope;
    request.principal = principal;
    request.project_id = project_id;
    request.submission_id = submission_id;
    request.send_id = send_id;
    request.request_identity_digest = request_identity_digest;
    request
}

struct OldOpenWait;

impl crate::scenario::ScenarioExtension for OldOpenWait {
    fn setup(&self, _: &std::path::Path, _: &mut RunControl, _: &mut HarnessPlane) {}

    fn seed_reference(&self, store: &crate::wal::ObservedWalStore, request: &RunRequest) -> usize {
        use broca_core::WalStore;
        let mut wal = store.open(&request.session).unwrap();
        wal.append(1, &old_wait_started(), true).unwrap();
        let replacement = Arc::new(vec![serde_json::from_value(
            json!({"role":"user","content":[{"type":"text","text":"setup head"}]}),
        )
        .unwrap()]);
        wal.append(
            1,
            &WalRecord::CompactionMessage {
                provider: "provider".into(),
                kind: CompactionMessageKind::Setup,
                compaction_id: "summary".into(),
                version: 1,
                range: broca_wal::CompactionRange { from: 0, to: 0 },
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
            true,
        )
        .unwrap();
        let lineage = wal.lineage_id().unwrap().to_string();
        wal.append(
            1,
            &WalRecord::CompactionRequest {
                provider: "provider".into(),
                request_id: "old-request".into(),
            },
            true,
        )
        .unwrap();
        wal.append(
            1,
            &WalRecord::CompactionAnswer {
                provider: "provider".into(),
                request_id: "old-request".into(),
                cursor: broca_wal::CompactionCursor {
                    lineage_id: lineage,
                    after_ordinal: None,
                },
                disposition: broca_wal::CompactionDisposition::Wait,
                rejected: None,
                wait: Some(broca_wal::CompactionWait {
                    reason: "work".into(),
                    bound_ms: 10,
                }),
            },
            true,
        )
        .unwrap();
        4
    }

    fn check(&self, kill: &KillReport, resumed: &Resumed) {
        let end = kill.durable.end_of_records(4);
        assert!(
            resumed.log.bytes.starts_with(&kill.durable.bytes[..end]),
            "the old wait prefix must remain byte-identical"
        );
        assert_eq!(
            resumed
                .log
                .records()
                .iter()
                .filter(|record| matches!(record, WalRecord::CompactionWait { .. }))
                .count(),
            1
        );
        assert!(resumed.plane.dispatched().is_empty());
        assert_eq!(
            resumed.provider.request_count(),
            0,
            "an old open wait cannot authorize a provider call"
        );
    }
}

const OLD_WAIT_SCENARIO: Scenario = Scenario {
    name: "old-open-compaction-wait",
    request: old_wait_request,
    control: RunControl::default,
    scripts: Vec::new,
    render: Render::Canonical,
    plane: StubToolPlane::new,
    expected: RunFinishReason::Interrupted,
    extension: Some(&OldOpenWait),
};

#[tokio::test]
async fn old_open_wait_recovery_preserves_seed_and_closes_once_at_every_cut() {
    let handle = RunnerHandle::new(OLD_WAIT_SCENARIO).await;
    let zero = handle
        .kill_at(
            KillPoint::RecordBoundary {
                records: 0,
                torn: false,
            },
            KillTrigger::Truncate,
        )
        .await;
    assert_eq!(
        zero.durable.record_count(),
        4,
        "cut zero must preserve the historical input"
    );
    assert!(matches!(
        zero.durable.records().last(),
        Some(WalRecord::CompactionAnswer {
            disposition: broca_wal::CompactionDisposition::Wait,
            ..
        })
    ));
    assert_eq!(
        handle
            .reference
            .log
            .records()
            .iter()
            .filter(|record| matches!(record, WalRecord::CompactionWait { .. }))
            .count(),
        1
    );
    assert!(
        handle.reference.sent.is_empty(),
        "legacy wait recovery must not request a model step"
    );
    let kills = handle.every_record_boundary(true).await;
    assert_eq!(kills.len(), handle.record_boundaries(true).len());
}

crate::require_process_kill!();

fn legacy_request() -> RunRequest {
    let mut request = request();
    request.session.session = "legacy-session".into();
    request
}

const LEGACY_SCENARIO: Scenario = Scenario {
    name: "compaction-legacy",
    request: legacy_request,
    ..SCENARIO
};

const TOOL_SCENARIO: Scenario = Scenario {
    name: "compaction-tools",
    scripts: tool_scripts,
    ..SCENARIO
};

fn ceiling_control() -> RunControl {
    ceiling_control_with(true)
}
fn ceiling_timeout_control() -> RunControl {
    ceiling_control_with(false)
}
fn ceiling_control_with(view: bool) -> RunControl {
    let mut control = control();
    control.admitted_context_limit = Some(20_000);
    with_compaction(control, Arc::new(ScriptedCompaction::at_ceiling(view)))
}
const CEILING_SCENARIO: Scenario = Scenario {
    name: "compaction-ceiling",
    control: ceiling_control,
    ..SCENARIO
};
const CEILING_TIMEOUT_SCENARIO: Scenario = Scenario {
    name: "compaction-ceiling-timeout",
    control: ceiling_timeout_control,
    expected: RunFinishReason::ContextCeiling,
    ..SCENARIO
};

/// A send whose history carries an earlier answer with signed reasoning.
fn signed_request() -> RunRequest {
    let submission = serde_json::from_value(json!({
        "model":{"provider_module_id":"stub","model_id":"model"},
        "input":[
            {"role":"user","content":[{"type":"text","text":"original user"}]},
            {"role":"assistant","origin":{"provider_module_id":"stub","model_id":"model"},
             "content":[{"type":"reasoning","text":"old thinking","signature":"old-signature"},
                        {"type":"text","text":"old answer"}]},
            {"role":"user","content":[{"type":"text","text":"next question"}]}
        ],
        "tools":[{"name":"edit","description":"edit","input_schema":{"type":"object"}}]
    }))
    .unwrap();
    let mut request = RunRequest::from_submission(
        submission,
        RunId("compaction-signed-run".into()),
        BindIdentity::new("/crash-harness/compaction-signed", "caller", "session"),
        SubmissionId("compaction-signed-send".into()),
    );
    request.policy_allowed.insert("edit".into());
    request
}

/// The plan renders runner tags, and the provider's views follow the two-view schedule.
fn signed_control() -> RunControl {
    let mut control = control();
    let mut manifest = control.manifest.as_deref().unwrap().clone();
    manifest.plan = OpaqueJson::from_json_text(
        json!({"compaction_item":{"provider":PROVIDER,"preset":"summary","params":{}},
            "composition":{},"render_tags":true})
        .to_string(),
    )
    .unwrap();
    control.manifest = Some(Arc::new(manifest));
    with_compaction(control, Arc::new(ScriptedCompaction::signed()))
}

fn signed(text: &str) -> ContentBlock {
    ContentBlock::Reasoning {
        text: format!("{text} thinking"),
        signature: Some(format!("{text}-signature")),
    }
}

/// A tool call with signed reasoning, then a final answer with signed reasoning.
fn signed_scripts() -> Vec<Vec<NormalizedEvent>> {
    vec![
        assistant(
            "plan-reply",
            vec![
                signed("plan"),
                ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId("edit-1".into()),
                    tool_name: "edit".into(),
                    input: json!({}),
                }),
            ],
            FinishReason::ToolCalls,
        ),
        assistant(
            "final-reply",
            vec![
                signed("final"),
                ContentBlock::Text {
                    text: "done".into(),
                },
            ],
            FinishReason::Stop,
        ),
    ]
}

/// Signed reasoning on a prefix-bound provider, runner tags, and two views: the first folds
/// the first question and strips the signed reasoning after it; the second, at the next
/// boundary, compresses the tool's output after the open tool turn and so applies as is.
/// The recorded Setup omits `tag_bytes_edits`, so the tool-result bytes edit gets no runner tag.
const SIGNED_SCENARIO: Scenario = Scenario {
    name: "compaction-signed",
    request: signed_request,
    control: signed_control,
    scripts: signed_scripts,
    render: Render::CanonicalPrefixBound,
    plane: tools,
    extension: None,
    expected: RunFinishReason::Completed,
};

fn declared_signed_request() -> RunRequest {
    let mut request = signed_request();
    request.session.session = "declared-session".into();
    request
}

const DECLARED_SIGNED_SCENARIO: Scenario = Scenario {
    name: "compaction-signed-declared",
    request: declared_signed_request,
    ..SIGNED_SCENARIO
};

pub(crate) fn find(name: &str) -> Option<Scenario> {
    [
        RIDE_SCENARIO,
        SCENARIO,
        LEGACY_SCENARIO,
        TOOL_SCENARIO,
        SIGNED_SCENARIO,
        DECLARED_SIGNED_SCENARIO,
        CEILING_SCENARIO,
        CEILING_TIMEOUT_SCENARIO,
        V2_SCENARIO,
    ]
    .into_iter()
    .find(|scenario| scenario.name == name)
}

fn replacement(version: u64, text: &str) -> Value {
    json!({"compaction_id":"summary","version":version,"range":{"from":0,"to":0},
        "replacement":[{"role":"user","content":[{"type":"text","text":text}]}]})
}

/// The scripted compaction provider: Setup, then its view of the session (the first user
/// message folded into a summary), announced with `view_ready` and served by `view.get`, so
/// the run fetches every view itself at a step boundary. A view is announced at every
/// boundary whose applied view is older (`stale` announces an older one).
#[derive(Default)]
struct ScriptedCompaction {
    slow_ride: Option<(std::path::PathBuf, broca_core::Clock)>,
    /// At the ceiling, return a smaller view or let the bound expire.
    ceiling: Option<bool>,
    ceiling_time: Option<CeilingTime>,
    ride_answered: AtomicBool,
    late_ride: bool,
    block_ride: Option<std::path::PathBuf>,
    setups: AtomicUsize,
    /// `view.get` calls answered with a view.
    offered: AtomicUsize,
    /// Announce an older view (version 1, "stale summary") at every boundary, even after
    /// version 2 applied.
    stale: bool,
    /// Serve the signed scenario's second view once its WAL-backed schedule announces it.
    signed: bool,
    lineage: Mutex<Option<String>>,
}

impl ScriptedCompaction {
    fn at_ceiling(view: bool) -> Self {
        Self {
            ceiling: Some(view),
            // Keep the five-millisecond policy, but spend none of it on scheduling or WAL I/O.
            ceiling_time: (!view).then(CeilingTime::new),
            ..Default::default()
        }
    }

    fn signed() -> Self {
        Self {
            signed: true,
            ..Default::default()
        }
    }

    /// The view the provider publishes under `version`, if it publishes one.
    fn published(&self, session_ref: &str, version: u64) -> Option<CompactionView> {
        let lineage = self.lineage.lock().unwrap().clone()?;
        match (version, self.signed, self.stale) {
            (1, false, true) => Some(view(session_ref, &lineage, 1, "stale summary")),
            (2, _, false) => Some(view(session_ref, &lineage, 2, APPLIED_HEAD)),
            (3, true, false) => Some(second_view(session_ref, &lineage)),
            _ => None,
        }
    }
}

#[async_trait]
impl CompactionPlane for ScriptedCompaction {
    async fn setup(
        &self,
        provider: &str,
        bind: &BindIdentity,
        _scope: Option<&broca_protocol::SessionScope>,
        request: SetupRequest,
    ) -> Result<SetupAnswer, CompactionUnavailable> {
        assert_eq!(provider, PROVIDER);
        assert_eq!(request.harness, bind.harness);
        self.setups.fetch_add(1, Ordering::SeqCst);
        if let Some(view) = self.ceiling {
            *self.lineage.lock().unwrap() = request.lineage_id.clone();
            return Ok(serde_json::from_value(
                json!({"answer":"ready", "request_id":request.request_id,
                "initial":replacement(1, &"large head ".repeat(800)),
                "ceiling":{"at_share":0.1,"wait_ms":if view { 300_000 } else { CEILING_WAIT_MS }}}),
            )
            .unwrap());
        }
        if self.slow_ride.is_some() {
            return Ok(serde_json::from_value(
                json!({"answer":"ready","request_id":request.request_id,
                "initial":replacement(1, "setup head"),"ride_wait_ms":30000}),
            )
            .unwrap());
        }
        Ok(
            serde_json::from_value(json!({"answer":"ready","request_id":request.request_id,
            "initial":replacement(1, "setup head")}))
            .unwrap(),
        )
    }

    async fn view_get(
        &self,
        provider: &str,
        _bind: &BindIdentity,
        _scope: Option<&broca_protocol::SessionScope>,
        request: ViewGetRequest,
    ) -> Result<ViewGetAnswer, CompactionUnavailable> {
        assert_eq!(provider, PROVIDER);
        Ok(
            match self.published(&request.session_ref, request.view_version) {
                Some(view) => {
                    self.offered.fetch_add(1, Ordering::SeqCst);
                    ViewGetAnswer::view(view)
                }
                None => ViewGetAnswer::refuse("unknown_version", "not published"),
            },
        )
    }

    async fn ride(
        &self,
        _: &str,
        _: &BindIdentity,
        _scope: Option<&broca_protocol::SessionScope>,
        request: broca_core::compaction_provider::ride::RideRequest,
    ) -> Result<broca_core::compaction_provider::ride::RideAnswer, CompactionUnavailable> {
        if let Some((root, clock)) = &self.slow_ride {
            let now = clock.now_ms()
                + cortexkit_role_llm_runner::run::COMPACTION_WAIT_REPORT_AFTER_MS
                + 1;
            std::fs::write(root.join("test-clock"), json!({"now_ms":now}).to_string()).unwrap();
            clock.refresh().unwrap();
            broca_test_support::wait_until("durable compaction ride wait", || async {
                use broca_core::WalStore;
                let frames = broca_core::FileWalStore::new(root.join("wal"))
                    .read_records(&crate::compaction::request().session)
                    .unwrap();
                if frames
                    .iter()
                    .any(|frame| matches!(frame.record, WalRecord::CompactionRideWait { .. }))
                {
                    Ok(())
                } else {
                    Err("the ride wait has not been recorded".to_owned())
                }
            })
            .await;
        }
        if let Some(marker) = &self.block_ride {
            std::fs::write(marker, b"ceiling ride entered").unwrap();
            return std::future::pending().await;
        }
        if self.late_ride {
            // Make the response miss its absolute deadline even if select polls it first.
            self.ceiling_time.as_ref().unwrap().expire();
        }
        self.ride_answered.store(true, Ordering::SeqCst);
        Ok(match self.ceiling {
            Some(true) => broca_core::compaction_provider::ride::RideAnswer::view(
                self.published(&request.session_ref, 2).unwrap(),
            ),
            _ => broca_core::compaction_provider::ride::RideAnswer::nothing(),
        })
    }
}

/// `SIGNED_SCENARIO`'s second view: the first question still folded, and the tool's output
/// (message 4) replaced by a shorter one.
fn second_view(session_ref: &str, lineage_id: &str) -> CompactionView {
    let summary =
        json!([{"role":"user","content":[{"type":"text","text":APPLIED_HEAD}]}]).to_string();
    let short = json!({"type":"tool_result","tool_call_id":"edit-1",
        "output":{"kind":"text","text":"edited (short)"},"is_error":false})
    .to_string();
    CompactionView::new(
        session_ref,
        3,
        lineage_id,
        MessageRef {
            ordinal: 4,
            mid: "m4".into(),
        },
        vec![
            ViewRange::replace(0, 1, summary),
            ViewRange::keep_with_edits(
                1,
                5,
                vec![PartEdit::replace(
                    4,
                    "m4",
                    broca_protocol::tags::part_key(4, 0),
                    short,
                )],
            ),
        ],
    )
}

fn view(session_ref: &str, lineage_id: &str, version: u64, text: &str) -> CompactionView {
    let bytes = json!([{"role":"user","content":[{"type":"text","text":text}]}]).to_string();
    CompactionView::new(
        session_ref,
        version,
        lineage_id,
        MessageRef {
            ordinal: 0,
            mid: "m0".into(),
        },
        vec![ViewRange::replace(0, 1, bytes)],
    )
}

impl CompactionViews for ScriptedCompaction {
    fn newest_ready(
        &self,
        session_ref: &str,
        lineage_id: &str,
        after: Option<u64>,
    ) -> Option<ViewReady> {
        // A `view_ready` event names no lineage; the scripted provider learns the run's from
        // its first boundary, as a real provider learns it from the run's notices.
        *self.lineage.lock().unwrap() = Some(lineage_id.into());
        if self.ride_answered.load(Ordering::SeqCst) {
            if let Some(time) = &self.ceiling_time {
                // The run queries announcements again only after recording the ride answer.
                // Expire the wait here, not inside ride(), where Nothing would become late.
                time.expire();
            }
        }
        if self.ceiling.is_some() {
            return None;
        }
        let version = if self.stale {
            1
        } else if after.is_none_or(|applied| applied < 2) {
            2
        } else {
            return None;
        };
        let view = self.published(session_ref, version)?;
        Some(ViewReady::for_view(&view))
    }
}

// Mode 3 (compaction-provider/v2). A session created by this build calls the provider's v2
// methods only; the v1 scenarios above cover logs recorded before the switch.

fn v2_request() -> RunRequest {
    let submission = serde_json::from_value(json!({
        "model":{"provider_module_id":"stub","model_id":"model"},
        "input":[
            {"role":"user","content":[{"type":"text","text":"original user"}]},
            {"role":"user","content":[{"type":"text","text":"second question"}]}
        ],
        "tools":[{"name":"edit","description":"edit","input_schema":{"type":"object"}}]
    }))
    .unwrap();
    let mut request = RunRequest::from_submission(
        submission,
        RunId("compaction-v2-run".into()),
        BindIdentity::new("/crash-harness/compaction-v2", "caller", "session"),
        SubmissionId("compaction-v2-send".into()),
    );
    request.policy_allowed.insert("edit".into());
    request.context_limit = Some(100_000);
    request
}

fn v2_control() -> RunControl {
    RunControl {
        manifest: Some(Arc::new(broca_wal::PlanManifest {
            rich_ui: None,
            plan: OpaqueJson::from_json_text(
                json!({"compaction_item":{"provider":PROVIDER,"preset":"summary","params":{}},"composition":{}})
                    .to_string(),
            )
            .unwrap(),
            plan_identity: "compaction-v2-plan".into(),
            composition_digest: "composition".into(),
            tools: Arc::new(vec![]),
            system_text: String::new(),
            system_digest: String::new(),
            join_version: 1,
            role_versions: [("compaction-provider".into(), "v2".into())].into(),
            fetches: vec![],
            declarations: vec![],
        })),
        new_log_compaction_plan_version: Some(broca_core::compaction_v2::PLAN_VERSION),
        admitted_context_limit: Some(100_000),
        ..Default::default()
    }
}

/// Installs the scripted v2 provider for the run's own log, in the reference run, in every
/// killed child and in every resume.
struct V2Provider;

impl crate::scenario::ScenarioExtension for V2Provider {
    fn setup(&self, root: &Path, control: &mut RunControl, _: &mut HarnessPlane) {
        control.compaction_plane = Some(Arc::new(V2Plane {
            wal: wal_path(root, &v2_request().session),
        }));
        control.compaction_views = None;
    }
}

/// A v2 provider that answers Setup `ready`, takes every notice, and answers each ride with
/// one view, version 1, cutting through the session's first message.
struct V2Plane {
    wal: PathBuf,
}

#[async_trait]
impl CompactionPlane for V2Plane {
    async fn setup(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        _: SetupRequest,
    ) -> Result<SetupAnswer, CompactionUnavailable> {
        panic!("a mode-3 session never calls the retired v1 Setup")
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
        let answer = match method {
            "compaction.setup" => json!({"answer":"ready"}),
            "compaction.notice" => json!({"answer":"received"}),
            "compaction.ride" => json!({"answer":"view","view":v2_cut(&self.wal)}),
            other => panic!("unexpected compaction method {other}"),
        };
        Ok(serde_json::value::to_raw_value(&answer).unwrap())
    }
}

/// Version 1 of the provider's view: a cut through the first message ("original user"). A
/// message id carries its log's lineage and the message's 0-based position, so the cut is built
/// from the log the run is writing.
fn v2_cut(wal: &Path) -> broca_core::compaction_v2::protocol::view::View {
    use broca_core::compaction_v2::protocol::view::{Edit, EditOp, View};
    let lineage = LogImage::read(wal)
        .lineage_id()
        .expect("a ride comes after the run wrote its log's lineage")
        .to_string();
    let through = broca_core::message_mid(true, Some(&lineage), 0).unwrap();
    View::seal(1, vec![Edit::new(1, EditOp::Cut { through })])
}

const V2_SCENARIO: Scenario = Scenario {
    name: "compaction-v2",
    request: v2_request,
    control: v2_control,
    scripts: tool_scripts,
    // The scripted provider announces a chain reset before its first request, so the first
    // step rides for that cause and gets the view. A fresh session has no other cause to ride
    // for: broca claims `cache_expired` only when its cache clock proves it.
    render: Render::CompactionChainReset,
    plane: tools,
    extension: Some(&V2Provider),
    expected: RunFinishReason::Completed,
};

/// Accepted v2 views: each is recorded once, when the runner accepts it.
fn accepted_views(records: &[WalRecord]) -> usize {
    records
        .iter()
        .filter(|record| matches!(record, WalRecord::CompactionViewV2(_)))
        .count()
}

/// A mode-3 run cut at every record boundary, and killed in flight, resumes sending exactly the
/// bytes the uncut run sent under its applied view. A view already durable is rendered from the
/// log on resume, and no view is ever accepted twice.
#[tokio::test]
async fn compaction_v2_every_record_boundary_and_real_kill_replay_identical_bytes() {
    let handle = RunnerHandle::new(V2_SCENARIO).await;
    let reference = handle.reference.sent.get(&0).expect("reference model call");
    let text = String::from_utf8_lossy(reference);
    assert!(text.contains("second question"), "{text}");
    assert!(
        !text.contains("original user"),
        "the view's cut must hide the first message: {text}"
    );
    let reference_records = handle.reference.log.records();
    assert_eq!(accepted_views(&reference_records), 1);
    assert!(reference_records
        .iter()
        .any(|record| matches!(record, WalRecord::CompactionViewAppliedV2 { .. })));
    assert!(
        !reference_records
            .iter()
            .any(|record| matches!(record, WalRecord::CompactionMessage { .. })),
        "a mode-3 session writes no v1 compaction records"
    );
    let mut suite = Suite::new("compaction-v2");
    let mut durable_view_model_resumes = 0;
    let mut points = handle.record_boundaries(true);
    points.push(KillPoint::InFlight(InFlight::ProviderStep(1)));
    for point in points {
        let trigger = if matches!(point, KillPoint::InFlight(_)) {
            KillTrigger::ProcessKill
        } else {
            KillTrigger::Truncate
        };
        let kill = handle.kill_at(point, trigger).await;
        let resumed = handle.resume(&kill).await;
        // Compares every resumed request with the reference's request for the same step.
        handle.check(&kill, &resumed);
        if accepted_views(&kill.durable.records()) > 0 {
            durable_view_model_resumes += usize::from(resumed.provider.request_count() > 0);
        }
        assert!(
            accepted_views(&resumed.log.records()) <= 1,
            "{}: a view was accepted twice",
            kill.label()
        );
        suite.record([kill]);
    }
    assert!(
        durable_view_model_resumes > 0,
        "durable views must be rendered on resume, not just recorded"
    );
    let methods = suite.finish();
    assert_eq!(methods.get("Sigkill"), Some(&1));
}
