//! Compaction crash scenarios use the core traits directly, but keep the file WAL and run loop
//! real: Setup from a scripted provider, and an asynchronous view announced by `view_ready`,
//! fetched with `view.get` at the first step boundary, recorded, rendered, and reported in
//! advance notices.

use std::sync::atomic::{AtomicUsize, Ordering};
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
    control.compaction_plane = Some(provider.clone());
    control.compaction_views = Some(provider);
    control
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

const TOOL_SCENARIO: Scenario = Scenario {
    name: "compaction-tools",
    scripts: tool_scripts,
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
const SIGNED_SCENARIO: Scenario = Scenario {
    name: "compaction-signed",
    request: signed_request,
    control: signed_control,
    scripts: signed_scripts,
    render: Render::CanonicalPrefixBound,
    plane: tools,
    expected: RunFinishReason::Completed,
};

pub(crate) fn find(name: &str) -> Option<Scenario> {
    [SCENARIO, TOOL_SCENARIO, SIGNED_SCENARIO]
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
    setups: AtomicUsize,
    /// `view.get` calls answered with a view.
    offered: AtomicUsize,
    /// Announce an older view (version 1, "stale summary") at every boundary, even after
    /// version 2 applied.
    stale: bool,
    /// Follow `SIGNED_SCENARIO`'s two-view schedule instead.
    signed: bool,
    lineage: Mutex<Option<String>>,
}

impl ScriptedCompaction {
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
        let version = if self.signed {
            match after {
                None | Some(0..=1) => 2,
                Some(2) => 3,
                Some(_) => return None,
            }
        } else if self.stale {
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
