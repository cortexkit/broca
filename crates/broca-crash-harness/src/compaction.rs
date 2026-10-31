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
    CompactionPlane, CompactionUnavailable, CompactionViews, FileWalStore, RunControl, RunRequest,
    StubReply, StubToolPlane,
};
use broca_protocol::content::{ContentBlock, ToolCall, ToolOutput};
use broca_protocol::{
    AssistantMessage, BindIdentity, FinishReason, MessageId, NormalizedEvent, ProviderControl,
    RunFinishReason, RunId, SubmissionId, ToolCallId, Usage,
};
use broca_wal::{CompactionMessageKind, OpaqueJson, WalRecord};
use serde_json::{json, Value};

use crate::{
    drive, wal_path, HarnessPlane, InFlight, KillPoint, KillReport, KillTrigger, LogImage, Render,
    Resumed, RunnerHandle, Scenario, ScriptedProvider, Suite,
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

#[tokio::test]
async fn compaction_requests_match_the_pre_windowing_bytes() {
    let mut requests = Vec::new();
    for scenario in [LEGACY_SCENARIO, SIGNED_SCENARIO] {
        let handle = RunnerHandle::new(scenario).await;
        requests.push(
            handle
                .reference
                .sent
                .values()
                .map(|bytes| String::from_utf8(bytes.clone()).expect("canonical request JSON"))
                .collect::<Vec<_>>(),
        );
    }
    let expected: Vec<Vec<String>> =
        serde_json::from_str(include_str!("fixtures/compaction_request_bytes.json")).unwrap();
    assert_eq!(requests, expected);
}

#[tokio::test]
async fn compaction_requests_with_declared_features_match_the_producer_bytes() {
    let mut requests = Vec::new();
    for scenario in [SCENARIO, DECLARED_SIGNED_SCENARIO] {
        let handle = RunnerHandle::new(scenario).await;
        let records = handle.reference.log.records();
        let features = records.iter().find_map(|record| match record {
            WalRecord::CompactionMessage {
                kind: CompactionMessageKind::Setup,
                view_features,
                ..
            } => view_features.as_ref(),
            _ => None,
        });
        assert_eq!(features.unwrap(), &["truncate", "tag_bytes_edits"]);
        requests.push(
            handle
                .reference
                .sent
                .values()
                .map(|bytes| String::from_utf8(bytes.clone()).expect("canonical request JSON"))
                .collect::<Vec<_>>(),
        );
    }
    let expected: Vec<Vec<String>> = serde_json::from_str(include_str!(
        "fixtures/compaction_request_bytes_declared.json"
    ))
    .unwrap();
    assert_eq!(requests, expected);
    assert!(requests[1][1].contains("§4§ edited (short)"));
}

#[tokio::test]
async fn bytes_edit_resume_uses_the_recorded_setup_features() {
    for (scenario, declared, fixture) in [
        (
            SIGNED_SCENARIO,
            false,
            include_str!("fixtures/compaction_request_bytes.json"),
        ),
        (
            DECLARED_SIGNED_SCENARIO,
            true,
            include_str!("fixtures/compaction_request_bytes_declared.json"),
        ),
    ] {
        let handle = RunnerHandle::new(scenario).await;
        let records = handle.reference.log.records();
        let applied = records
            .iter()
            .position(|record| {
                matches!(
                    record,
                    WalRecord::CompactionViewApplied {
                        view_version: 3,
                        ..
                    }
                )
            })
            .expect("the bytes edit was applied");
        let kill = handle
            .kill_at(
                KillPoint::RecordBoundary {
                    records: applied + 1,
                    torn: false,
                },
                KillTrigger::Truncate,
            )
            .await;
        let compaction = Arc::new(ScriptedCompaction::signed());
        let resumed = resume_with(scenario, &kill, compaction.clone()).await;
        handle.check(&kill, &resumed);
        assert_eq!(compaction.setups.load(Ordering::SeqCst), 0);
        for log in [&kill.durable, &resumed.log] {
            let records = log.records();
            let setup = records
                .iter()
                .find(|record| {
                    matches!(
                        record,
                        WalRecord::CompactionMessage {
                            kind: CompactionMessageKind::Setup,
                            ..
                        }
                    )
                })
                .expect("the session has a recorded Setup");
            let json = serde_json::to_value(setup).unwrap();
            if declared {
                assert_eq!(
                    json["view_features"],
                    json!(["truncate", "tag_bytes_edits"])
                );
            } else {
                assert!(json.get("view_features").is_none());
            }
            assert!(json.get("view_due").is_none());
        }
        let expected: Vec<Vec<String>> = serde_json::from_str(fixture).unwrap();
        let sent = resumed.provider.sent();
        assert_eq!(
            sent.len(),
            1,
            "only the request after the bytes edit resumes"
        );
        assert_eq!(sent.get(&1).unwrap(), expected[1][1].as_bytes());
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
    expected: RunFinishReason::Completed,
};

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
        SCENARIO,
        LEGACY_SCENARIO,
        TOOL_SCENARIO,
        SIGNED_SCENARIO,
        DECLARED_SIGNED_SCENARIO,
        CEILING_SCENARIO,
        CEILING_TIMEOUT_SCENARIO,
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
        request: broca_core::compaction_provider::ride::RideRequest,
    ) -> Result<broca_core::compaction_provider::ride::RideAnswer, CompactionUnavailable> {
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

async fn resume_with(
    scenario: Scenario,
    kill: &KillReport,
    compaction: Arc<ScriptedCompaction>,
) -> Resumed {
    // A recorded answer is not ridden again on replay. Give that prefix the same clock
    // transition as a fresh timely Nothing, without changing its persisted deadline.
    compaction.ride_answered.store(
        kill.durable.records().iter().any(|record| {
            matches!(
                record,
                WalRecord::ContextCeilingWait {
                    unavailable: Some(false),
                    ..
                }
            )
        }),
        Ordering::SeqCst,
    );
    let provider = ScriptedProvider::for_scenario(&scenario);
    let plane = HarnessPlane::for_scenario(&scenario);
    let store = FileWalStore::new(kill.state_root.join("wal"));
    let control = with_compaction((scenario.control)(), compaction);
    let outcome = drive(
        (scenario.request)(),
        control,
        &kill.state_root,
        &store,
        &provider,
        &plane,
    )
    .await;
    let log = LogImage::read(&wal_path(&kill.state_root, &(scenario.request)().session));
    Resumed {
        reason: outcome.reason,
        provider,
        plane,
        log,
        outcome,
    }
}

fn has_setup(records: &[WalRecord]) -> bool {
    records.iter().any(|record| {
        matches!(
            record,
            WalRecord::CompactionMessage {
                kind: CompactionMessageKind::Setup,
                ..
            }
        )
    })
}

fn views_applied(records: &[WalRecord]) -> usize {
    records
        .iter()
        .filter(|record| matches!(record, WalRecord::CompactionViewApplied { .. }))
        .count()
}

/// Every notice's sequence number, in WAL order.
fn notice_seqs(records: &[WalRecord]) -> Vec<u64> {
    records
        .iter()
        .filter_map(|record| match record {
            WalRecord::CompactionNotice { seq, headers } => {
                assert_eq!(headers["seq"], seq.to_string());
                Some(*seq)
            }
            _ => None,
        })
        .collect()
}

/// A run cut at every record boundary, and killed in flight, resumes sending exactly the bytes
/// the uncut run sent under the applied view. A view already durable is rendered from the WAL
/// and never applied again; Setup never runs twice; and notices keep one gapless sequence, so a
/// notice republished after the crash reuses its number and none is skipped or repeated.
#[tokio::test]
async fn compaction_every_record_boundary_and_real_kill_replay_identical_bytes() {
    let handle = RunnerHandle::new(SCENARIO).await;
    let reference = handle.reference.sent.get(&0).expect("reference model call");
    assert!(String::from_utf8_lossy(reference).contains(APPLIED_HEAD));
    assert!(
        !String::from_utf8_lossy(reference).contains("original user"),
        "the view must replace raw history, not merely insert a head"
    );
    assert!(
        !String::from_utf8_lossy(reference).contains("setup head"),
        "the view replaces Setup's head entirely"
    );
    let reference_records = handle.reference.log.records();
    assert_eq!(views_applied(&reference_records), 1);
    let reference_notices = notice_seqs(&reference_records);
    assert!(
        !reference_notices.is_empty(),
        "the run announces its transcript"
    );
    let mut suite = Suite::new("compaction");
    let mut unsynced = 0;
    let mut durable_view_resumes = 0;
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
        unsynced += usize::from(kill.after_sync == Some(false));
        let compaction = Arc::new(ScriptedCompaction::default());
        let resumed = resume_with(SCENARIO, &kill, compaction.clone()).await;
        handle.check(&kill, &resumed);
        let prefix = kill.durable.records();
        if has_setup(&prefix) {
            assert_eq!(
                compaction.setups.load(Ordering::SeqCst),
                0,
                "{}: Setup repeated",
                kill.label()
            );
        }
        let after = resumed.log.records();
        if views_applied(&prefix) > 0 {
            durable_view_resumes += 1;
            durable_view_model_resumes += usize::from(resumed.provider.request_count() > 0);
            assert_eq!(
                compaction.offered.load(Ordering::SeqCst),
                0,
                "{}: a durable view was fetched again",
                kill.label()
            );
        }
        assert!(
            views_applied(&after) <= 1,
            "{}: the view was applied twice",
            kill.label()
        );
        for bytes in resumed.provider.sent().values() {
            assert_eq!(
                bytes,
                reference,
                "{}: compaction request bytes changed",
                kill.label()
            );
        }
        let seqs = notice_seqs(&after);
        assert_eq!(
            seqs,
            (1..=seqs.len() as u64).collect::<Vec<_>>(),
            "{}: notice sequence repeated or skipped a number",
            kill.label()
        );
        assert_eq!(
            after
                .iter()
                .filter(|record| matches!(
                    record,
                    WalRecord::CompactionMessage {
                        kind: CompactionMessageKind::Setup,
                        ..
                    }
                ))
                .count(),
            1,
            "{}: Setup record count",
            kill.label()
        );
        suite.record([kill]);
    }
    assert!(unsynced > 0, "scenario must cut inside unsynced batches");
    assert!(
        durable_view_resumes > 0,
        "scenario must resume durable views"
    );
    assert!(
        durable_view_model_resumes > 0,
        "durable views must be rendered on resume, not just sealed"
    );
    let methods = suite.finish();
    assert_eq!(methods.get("Sigkill"), Some(&1));
    assert_eq!(
        methods.get("Truncation"),
        Some(&(2 * handle.reference.log.record_count() + 1))
    );
}

/// After a restart that resumes a run whose view is durable, a view older than it is never
/// applied, and the resumed requests keep the newer view's bytes.
#[tokio::test]
async fn stale_view_offered_after_restart_is_never_applied() {
    let handle = RunnerHandle::new(TOOL_SCENARIO).await;
    let records = handle.reference.log.records();
    let applied = records
        .iter()
        .position(|record| matches!(record, WalRecord::CompactionViewApplied { .. }))
        .expect("applied view")
        + 1;
    let kill = handle
        .kill_at(
            KillPoint::RecordBoundary {
                records: applied,
                torn: false,
            },
            KillTrigger::Truncate,
        )
        .await;
    let compaction = Arc::new(ScriptedCompaction {
        stale: true,
        ..Default::default()
    });
    let resumed = resume_with(TOOL_SCENARIO, &kill, compaction.clone()).await;
    handle.check(&kill, &resumed);
    assert_eq!(compaction.setups.load(Ordering::SeqCst), 0);
    assert!(
        compaction.offered.load(Ordering::SeqCst) > 0,
        "the stale view must actually be fetched and offered"
    );
    let after = resumed.log.records();
    assert_eq!(views_applied(&after), 1, "the stale view was applied");
    assert_eq!(resumed.provider.sent(), handle.reference.sent);
    assert!(resumed
        .provider
        .sent()
        .values()
        .all(|bytes| !String::from_utf8_lossy(bytes).contains("stale summary")));
}

/// The signed scenario cut at every record boundary, and killed while the second model call
/// is in flight, resumes sending exactly the uncut run's bytes: the first view's companion
/// strip of signed reasoning, recorded with it, stays applied under the second view, runner
/// tags render the same, neither view is applied twice, and notices keep one gapless sequence.
#[tokio::test]
async fn signed_reasoning_strip_and_a_second_view_replay_identical_bytes_at_every_cut() {
    let handle = RunnerHandle::new(SIGNED_SCENARIO).await;
    let first =
        String::from_utf8_lossy(handle.reference.sent.get(&0).expect("first call")).into_owned();
    let second =
        String::from_utf8_lossy(handle.reference.sent.get(&1).expect("second call")).into_owned();
    for sent in [&first, &second] {
        assert!(sent.contains(APPLIED_HEAD), "{sent}");
        assert!(!sent.contains("original user"), "{sent}");
        assert!(
            !sent.contains("old thinking"),
            "the strip leaves it out: {sent}"
        );
        assert!(sent.contains('§'), "runner tags are rendered: {sent}");
    }
    assert!(
        second.contains("plan thinking"),
        "reasoning after the views is kept"
    );
    assert!(
        second.contains("edited (short)") && !second.contains("§4§ edited"),
        "{second}"
    );
    let records = handle.reference.log.records();
    let choices: Vec<_> = records
        .iter()
        .filter_map(|record| match record {
            WalRecord::CompactionViewApplied {
                view_version,
                choice,
                ..
            } => Some((
                *view_version,
                matches!(choice, broca_wal::CompactionViewChoice::Companion { .. }),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(choices, vec![(2, true), (3, false)]);
    let mut suite = Suite::new("compaction-signed");
    let mut strip_resumes = 0;
    let mut points = handle.record_boundaries(true);
    points.push(KillPoint::InFlight(InFlight::ProviderStep(2)));
    for point in points {
        let trigger = if matches!(point, KillPoint::InFlight(_)) {
            KillTrigger::ProcessKill
        } else {
            KillTrigger::Truncate
        };
        let kill = handle.kill_at(point, trigger).await;
        let compaction = Arc::new(ScriptedCompaction::signed());
        let resumed = resume_with(SIGNED_SCENARIO, &kill, compaction.clone()).await;
        handle.check(&kill, &resumed);
        let prefix = kill.durable.records();
        if has_setup(&prefix) {
            assert_eq!(
                compaction.setups.load(Ordering::SeqCst),
                0,
                "{}",
                kill.label()
            );
        }
        strip_resumes +=
            usize::from(views_applied(&prefix) > 0 && resumed.provider.request_count() > 0);
        for (step, bytes) in resumed.provider.sent() {
            assert_eq!(
                Some(&bytes),
                handle.reference.sent.get(&step),
                "{}: step {step} request bytes changed",
                kill.label()
            );
        }
        let after = resumed.log.records();
        // A cut inside the tool batch can end the resumed run before its second model call
        // (an unknown dispatch outcome stops for review), and then the second view's boundary
        // never comes. Every resume that reaches the second call applied both views, once.
        let reached_second_call = after
            .iter()
            .filter(|record| matches!(record, WalRecord::ModelStepFinished { .. }))
            .count()
            == 2;
        assert!(views_applied(&after) <= 2, "{}", kill.label());
        if reached_second_call {
            assert_eq!(views_applied(&after), 2, "{}", kill.label());
        }
        let seqs = notice_seqs(&after);
        assert_eq!(
            seqs,
            (1..=seqs.len() as u64).collect::<Vec<_>>(),
            "{}: notice sequence repeated or skipped a number",
            kill.label()
        );
        suite.record([kill]);
    }
    assert!(
        strip_resumes > 0,
        "some cut must resume over a durable strip"
    );
    let methods = suite.finish();
    assert_eq!(methods.get("Sigkill"), Some(&1));
}

#[tokio::test]
async fn ceiling_late_nothing_is_provider_unavailable() {
    let compaction = Arc::new(ScriptedCompaction {
        late_ride: true,
        ..ScriptedCompaction::at_ceiling(false)
    });
    let root = compaction.ceiling_time.as_ref().unwrap().root.clone();
    let provider = ScriptedProvider::for_scenario(&CEILING_TIMEOUT_SCENARIO);
    let plane = HarnessPlane::for_scenario(&CEILING_TIMEOUT_SCENARIO);
    let outcome = drive(
        request(),
        with_compaction(ceiling_timeout_control(), compaction.clone()),
        &root,
        &FileWalStore::new(root.join("wal")),
        &provider,
        &plane,
    )
    .await;
    assert_eq!(outcome.reason, RunFinishReason::ContextCeiling);
    assert_eq!(provider.request_count(), 0);
    let log = LogImage::read(&wal_path(&root, &request().session));
    assert!(log.records().iter().any(|record| matches!(
        record,
        WalRecord::ContextCeilingWait {
            unavailable: Some(true),
            ..
        }
    )));
    let state = broca_core::reconcile_run_state(log.records().iter(), "compaction-run").unwrap();
    let detail = state.context_ceiling.unwrap();
    println!("late Nothing: {}", detail.cause);
    assert_eq!(detail.cause, "provider_unavailable");
    assert!(state.error.is_none());
}

#[tokio::test]
async fn ceiling_wait_and_terminal_state_survive_every_record_and_torn_frame_cut() {
    for (scenario, sends_view) in [(CEILING_SCENARIO, true), (CEILING_TIMEOUT_SCENARIO, false)] {
        let handle = RunnerHandle::new(scenario).await;
        let records = handle.reference.log.records();
        assert!(records
            .iter()
            .any(|r| matches!(r, WalRecord::ContextCeilingWait { .. })));
        assert!(records
            .iter()
            .any(|r| matches!(r, WalRecord::ContextCeilingDecided { .. })));
        let mut waited = 0;
        let mut decided = 0;
        for point in handle.record_boundaries(true) {
            let kill = handle.kill_at(point, KillTrigger::Truncate).await;
            let compaction = Arc::new(ScriptedCompaction {
                lineage: Mutex::new(kill.durable.lineage_id().map(|id| id.to_string())),
                ..ScriptedCompaction::at_ceiling(sends_view)
            });
            let resumed = resume_with(scenario, &kill, compaction).await;
            handle.check(&kill, &resumed);
            waited += usize::from(
                kill.durable
                    .records()
                    .iter()
                    .any(|r| matches!(r, WalRecord::ContextCeilingWait { .. })),
            );
            decided += usize::from(
                kill.durable
                    .records()
                    .iter()
                    .any(|r| matches!(r, WalRecord::ContextCeilingDecided { .. })),
            );
            if sends_view {
                for bytes in resumed.provider.sent().values() {
                    assert_eq!(
                        Some(bytes),
                        handle.reference.sent.get(&0),
                        "{}",
                        kill.label()
                    );
                }
            } else {
                assert_eq!(resumed.provider.request_count(), 0, "{}", kill.label());
                let after = resumed.log.records();
                let state =
                    broca_core::reconcile_run_state(after.iter(), "compaction-run").unwrap();
                assert_eq!(
                    state.context_ceiling.as_ref().unwrap().cause,
                    "view_timeout",
                    "{}",
                    kill.label()
                );
                assert!(state.error.is_none());
            }
        }
        assert!(waited > 0 && decided > 0);
    }
}

#[tokio::test]
async fn ceiling_wait_real_kill_resumes_the_same_request_bytes() {
    const CHILD: &str = "BROCA_CEILING_CHILD_ROOT";
    if let Some(root) = std::env::var_os(CHILD) {
        let root = std::path::PathBuf::from(root);
        let compaction = Arc::new(ScriptedCompaction {
            ceiling: Some(true),
            block_ride: Some(root.join("ceiling-entered")),
            ..Default::default()
        });
        let control = with_compaction(ceiling_control(), compaction);
        let provider = ScriptedProvider::for_scenario(&CEILING_SCENARIO);
        let plane = HarnessPlane::for_scenario(&CEILING_SCENARIO);
        drive(
            request(),
            control,
            &root,
            &FileWalStore::new(root.join("wal")),
            &provider,
            &plane,
        )
        .await;
        panic!("child finished instead of blocking at the ceiling");
    }
    let root = broca_test_support::scratch_root()
        .join(format!("ceiling-real-kill-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    crate::kill_child_test_at_marker(
        "ceiling wait",
        "compaction::ceiling_wait_real_kill_resumes_the_same_request_bytes",
        &[(CHILD, root.as_os_str())],
        &root.join("ceiling-entered"),
    );
    let prefix = LogImage::read(&wal_path(&root, &request().session));
    assert!(prefix
        .records()
        .iter()
        .any(|r| matches!(r, WalRecord::ContextCeilingWait { .. })));
    let provider = ScriptedProvider::for_scenario(&CEILING_SCENARIO);
    let plane = HarnessPlane::for_scenario(&CEILING_SCENARIO);
    let compaction = Arc::new(ScriptedCompaction {
        ceiling: Some(true),
        lineage: Mutex::new(prefix.lineage_id().map(|id| id.to_string())),
        ..Default::default()
    });
    let outcome = drive(
        request(),
        with_compaction(ceiling_control(), compaction),
        &root,
        &FileWalStore::new(root.join("wal")),
        &provider,
        &plane,
    )
    .await;
    assert_eq!(outcome.reason, RunFinishReason::Completed);
    let reference = RunnerHandle::new(CEILING_SCENARIO).await;
    assert_eq!(provider.sent(), reference.reference.sent);
    std::fs::remove_dir_all(root).unwrap();
}
