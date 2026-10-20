//! Compaction crash scenarios use the core traits directly, but keep the file WAL and run loop
//! real: Setup from a scripted provider, and an asynchronous view offered at the first step
//! boundary, recorded, rendered, and announced in advance notices.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_core::compaction_provider::{
    lineage::MessageRef,
    notice::ViewReady,
    setup::{SetupAnswer, SetupRequest},
    view::{CompactionView, ViewRange},
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
    RunControl {
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
        compaction_plane: Some(Arc::new(ScriptedCompaction::default())),
        compaction_views: Some(Arc::new(ScriptedViews::default())),
        ..Default::default()
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

const TOOL_SCENARIO: Scenario = Scenario {
    name: "compaction-tools",
    scripts: tool_scripts,
    ..SCENARIO
};

pub(crate) fn find(name: &str) -> Option<Scenario> {
    [SCENARIO, TOOL_SCENARIO]
        .into_iter()
        .find(|scenario| scenario.name == name)
}

fn replacement(version: u64, text: &str) -> Value {
    json!({"compaction_id":"summary","version":version,"range":{"from":0,"to":0},
        "replacement":[{"role":"user","content":[{"type":"text","text":text}]}]})
}

#[derive(Default)]
struct ScriptedCompaction {
    setups: AtomicUsize,
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
}

/// The provider's view of the session: the first user message folded into a summary. It is
/// offered at every boundary whose applied view is older (`stale` offers an older one).
#[derive(Default)]
struct ScriptedViews {
    /// Offers that returned a view.
    offered: AtomicUsize,
    /// Offer an older view (version 1, "stale summary") at every boundary, even after
    /// version 2 applied.
    stale: bool,
    lineage: Mutex<Option<String>>,
}

fn view(
    session_ref: &str,
    lineage_id: &str,
    version: u64,
    text: &str,
) -> (CompactionView, ViewReady) {
    let bytes = json!([{"role":"user","content":[{"type":"text","text":text}]}]).to_string();
    let view = CompactionView::new(
        session_ref,
        version,
        lineage_id,
        MessageRef {
            ordinal: 0,
            mid: "m0".into(),
        },
        vec![ViewRange::replace(0, 1, bytes)],
    );
    let ready = ViewReady::for_view(&view);
    (view, ready)
}

impl CompactionViews for ScriptedViews {
    fn newest(
        &self,
        session_ref: &str,
        lineage_id: &str,
        after: Option<u64>,
    ) -> Option<(CompactionView, ViewReady)> {
        *self.lineage.lock().unwrap() = Some(lineage_id.into());
        let offer = if self.stale {
            Some(view(session_ref, lineage_id, 1, "stale summary"))
        } else if after.is_none_or(|applied| applied < 2) {
            Some(view(session_ref, lineage_id, 2, APPLIED_HEAD))
        } else {
            None
        };
        self.offered
            .fetch_add(usize::from(offer.is_some()), Ordering::SeqCst);
        offer
    }
}

async fn resume_with(
    scenario: Scenario,
    kill: &KillReport,
    compaction: Arc<ScriptedCompaction>,
    views: Arc<ScriptedViews>,
) -> Resumed {
    let provider = ScriptedProvider::for_scenario(&scenario);
    let plane = HarnessPlane::for_scenario(&scenario);
    let store = FileWalStore::new(kill.state_root.join("wal"));
    let mut control = control();
    control.compaction_plane = Some(compaction);
    control.compaction_views = Some(views);
    let outcome = drive(
        (scenario.request)(),
        control,
        &kill.state_root,
        &store,
        &provider,
        &plane,
    )
    .await;
    let log = LogImage::read(&wal_path(&kill.state_root, &request().session));
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
        let views = Arc::new(ScriptedViews::default());
        let resumed = resume_with(SCENARIO, &kill, compaction.clone(), views.clone()).await;
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
                views.offered.load(Ordering::SeqCst),
                0,
                "{}: a durable view was offered again",
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
    let compaction = Arc::new(ScriptedCompaction::default());
    let views = Arc::new(ScriptedViews {
        stale: true,
        ..Default::default()
    });
    let resumed = resume_with(TOOL_SCENARIO, &kill, compaction.clone(), views.clone()).await;
    handle.check(&kill, &resumed);
    assert_eq!(compaction.setups.load(Ordering::SeqCst), 0);
    assert!(
        views.offered.load(Ordering::SeqCst) > 0,
        "the stale view must actually be offered"
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
