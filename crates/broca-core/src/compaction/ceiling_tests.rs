use super::*;
use crate::compaction_provider::{
    notice::ViewReady,
    ride::{RideAnswer, RideCause, RideRequest},
    view::{CompactionView, ViewGetAnswer, ViewGetRequest, ViewRange},
};

#[derive(Clone, Copy)]
enum CeilingAnswer {
    View,
    Nothing,
    Unavailable,
    Late,
    ReadyAfterFailure,
}

#[test]
fn a_smaller_view_can_lower_a_usage_calibrated_request_estimate() {
    assert_eq!(estimate_tokens(1_000, Some((8_000, 8_000))), 1_000);
    assert_eq!(estimate_tokens(8_000, Some((8_000, 8_000))), 8_000);
    assert_eq!(estimate_tokens(8_100, Some((8_000, 8_000))), 8_050);
    assert_eq!(
        estimate_tokens(1, Some((u64::MAX, 2))),
        9_223_372_036_854_775_808
    );
}
struct CeilingPlane {
    answer: CeilingAnswer,
    wait_ms: u64,
    lineage: Mutex<String>,
    rides: Mutex<Vec<RideRequest>>,
    fetches: AtomicUsize,
    entered: tokio::sync::Notify,
}
impl CeilingPlane {
    fn new(answer: CeilingAnswer) -> Self {
        Self {
            answer,
            wait_ms: 50,
            lineage: Mutex::new(fixture_lineage_id().to_string()),
            rides: Mutex::new(Vec::new()),
            fetches: AtomicUsize::new(0),
            entered: tokio::sync::Notify::new(),
        }
    }
    fn view(&self, session_ref: &str) -> CompactionView {
        CompactionView::new(
            session_ref,
            3,
            self.lineage.lock().unwrap().clone(),
            crate::compaction_provider::lineage::MessageRef {
                ordinal: 0,
                mid: "m0".into(),
            },
            vec![ViewRange::replace(
                0,
                1,
                serde_json::to_string(&vec![text(Role::User, "small")]).unwrap(),
            )],
        )
    }
}
impl CompactionViews for CeilingPlane {
    fn newest_ready(
        &self,
        session_ref: &str,
        lineage_id: &str,
        after: Option<u64>,
    ) -> Option<ViewReady> {
        *self.lineage.lock().unwrap() = lineage_id.into();
        if matches!(self.answer, CeilingAnswer::ReadyAfterFailure) && after.is_none_or(|v| v < 3) {
            Some(ViewReady::for_view(&self.view(session_ref)))
        } else {
            None
        }
    }
}
#[async_trait::async_trait]
impl CompactionPlane for CeilingPlane {
    async fn setup(
        &self,
        _: &str,
        _: &BindIdentity,
        request: SetupRequest,
    ) -> Result<SetupAnswer, CompactionUnavailable> {
        *self.lineage.lock().unwrap() = request.lineage_id.unwrap();
        let mut answer = ready();
        answer["request_id"] = request.request_id.into();
        answer["initial"]["replacement"][0]["content"][0]["text"] = "long head ".repeat(400).into();
        answer["ceiling"] = json!({"at_share":0.1, "wait_ms":self.wait_ms});
        Ok(serde_json::from_value(answer).unwrap())
    }
    async fn ride(
        &self,
        _: &str,
        _: &BindIdentity,
        request: RideRequest,
    ) -> Result<RideAnswer, CompactionUnavailable> {
        assert_eq!(request.cause, RideCause::Ceiling);
        assert!(
            request.deadline_ms > 1 && request.deadline_ms <= self.wait_ms,
            "uses remaining wait, not the 1ms call budget"
        );
        assert!(request.overflow.is_none());
        let session_ref = request.session_ref.clone();
        self.rides.lock().unwrap().push(request);
        self.entered.notify_one();
        match self.answer {
            CeilingAnswer::View => Ok(RideAnswer::view(self.view(&session_ref))),
            CeilingAnswer::Nothing | CeilingAnswer::ReadyAfterFailure => Ok(RideAnswer::nothing()),
            CeilingAnswer::Unavailable => Err(CompactionUnavailable),
            CeilingAnswer::Late => std::future::pending().await,
        }
    }
    async fn view_get(
        &self,
        _: &str,
        _: &BindIdentity,
        request: ViewGetRequest,
    ) -> Result<ViewGetAnswer, CompactionUnavailable> {
        if self.fetches.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(CompactionUnavailable);
        }
        Ok(ViewGetAnswer::view(self.view(&request.session_ref)))
    }
}
async fn ceiling_run(
    shared: &Shared,
    plane: Arc<CeilingPlane>,
) -> (crate::RunSummary, crate::StubProvider) {
    ceiling_run_with_control(shared, plane, crate::RunControl::default()).await
}
async fn ceiling_run_with_control(
    shared: &Shared,
    plane: Arc<CeilingPlane>,
    signals: crate::RunControl,
) -> (crate::RunSummary, crate::StubProvider) {
    let writer = shared.writer();
    let model = crate::StubProvider::new(vec![model_events()]);
    let (out, _events) = tokio::sync::mpsc::channel(256);
    let control = crate::RunControl {
        manifest: Some(manifest()),
        compaction_plane: Some(plane.clone()),
        compaction_views: Some(plane),
        admitted_context_limit: Some(10_000),
        compaction_config: CompactionConfig { call_budget_ms: 1 },
        ..signals
    };
    let summary = crate::run_loop_on_writer(
        request(),
        &writer,
        crate::StartMode::FreshOrResume,
        &model,
        &crate::StubToolPlane::new(),
        &crate::AllowAll,
        shared,
        None,
        control,
        &out,
    )
    .await
    .unwrap();
    writer.shutdown().await.unwrap();
    (summary, model)
}

#[tokio::test]
async fn a_ceiling_ride_applies_a_view_before_sending() {
    let shared = Shared::default();
    let plane = Arc::new(CeilingPlane::new(CeilingAnswer::View));
    let (summary, model) = ceiling_run(&shared, plane.clone()).await;
    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert_eq!(plane.rides.lock().unwrap().len(), 1);
    let calls = model.calls();
    assert_eq!(calls.len(), 1);
    let sent = String::from_utf8(crate::canonical_request_bytes(&calls[0])).unwrap();
    assert!(
        sent.contains("small") && !sent.contains("long head"),
        "{sent}"
    );
    assert!(shared
        .records()
        .iter()
        .any(|r| matches!(r, WalRecord::ContextCeilingDecided { detail: None, .. })));
    let records = shared.records();
    let headers = records
        .iter()
        .rev()
        .find_map(|r| match r {
            WalRecord::CompactionNotice { headers, .. } => Some(headers),
            _ => None,
        })
        .unwrap();
    assert_eq!(headers["applied_view_version"], "3");
    assert!(
        headers["est_tokens"].parse::<u64>().unwrap() < 1_000,
        "the closing notice must estimate the smaller request, not Setup's large head"
    );
}

#[tokio::test]
async fn a_ceiling_wait_is_active_until_its_bound_then_terminal_without_a_provider_error() {
    for (answer, expected) in [
        (CeilingAnswer::Nothing, "view_timeout"),
        (CeilingAnswer::Unavailable, "provider_unavailable"),
        (CeilingAnswer::Late, "provider_unavailable"),
    ] {
        let shared = Shared::default();
        let plane = Arc::new(CeilingPlane::new(answer));
        let run = ceiling_run(&shared, plane.clone());
        let observe = async {
            plane.entered.notified().await;
            let records = shared.records();
            let opened = records
                .iter()
                .position(|r| {
                    matches!(
                        r,
                        WalRecord::ContextCeilingWait {
                            unavailable: None,
                            ..
                        }
                    )
                })
                .unwrap();
            assert!(
                shared.durable() > opened,
                "the wait is durable before the ride starts"
            );
            let state = crate::reconcile_run_state(records.iter(), "run").unwrap();
            assert_eq!(state.state, crate::RunIndexState::Active);
            assert!(state
                .ceiling_deadline_ms
                .is_some_and(|deadline| deadline > now()));
            assert!(state.context_ceiling.is_none());
            let replay = crate::replay(&records).unwrap();
            assert!(replay.compaction.state().unwrap().ceiling_check.is_some());
        };
        let ((summary, model), ()) = tokio::join!(run, observe);
        assert_eq!(summary.reason, RunFinishReason::ContextCeiling);
        assert!(model.calls().is_empty());
        let records = shared.records();
        let state = crate::reconcile_run_state(records.iter(), "run").unwrap();
        assert_eq!(state.state, crate::RunIndexState::ContextCeiling);
        assert!(state.ceiling_deadline_ms.is_none());
        assert!(state.error.is_none());
        let detail = state.context_ceiling.unwrap();
        assert_eq!(detail.cause, expected);
        assert_eq!(detail.window, 10_000);
        assert!(detail.size >= 1000 && detail.size <= detail.window);
    }
}

#[tokio::test]
async fn the_ceiling_fetch_bypasses_the_unavailable_boundary_backoff() {
    let shared = Shared::default();
    let plane = Arc::new(CeilingPlane::new(CeilingAnswer::ReadyAfterFailure));
    let (summary, model) = ceiling_run(&shared, plane.clone()).await;
    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert_eq!(model.calls().len(), 1);
    assert_eq!(plane.fetches.load(Ordering::SeqCst), 2);
    assert_eq!(plane.rides.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn unavailable_view_get_is_not_retried_at_every_boundary() {
    let shared = Shared::default();
    let (model, views) = run_history_steps(
        &shared,
        OneView {
            unavailable: true,
            ..Default::default()
        },
        true,
    )
    .await;
    assert_eq!(
        model.0.calls().len(),
        2,
        "the outage must not block ordinary model progress"
    );
    assert_eq!(
        views.fetches.lock().unwrap().len(),
        1,
        "the second boundary must not spend another call budget"
    );
}

#[tokio::test]
async fn a_recorded_ceiling_terminal_decision_resumes_without_model_io_at_every_cut() {
    let shared = Shared::default();
    let (summary, _) =
        ceiling_run(&shared, Arc::new(CeilingPlane::new(CeilingAnswer::Nothing))).await;
    assert_eq!(summary.reason, RunFinishReason::ContextCeiling);
    let records = shared.records();
    let wait = records
        .iter()
        .position(|r| matches!(r, WalRecord::ContextCeilingWait { .. }))
        .unwrap();
    for cut in wait + 1..=records.len() {
        let resumed = Shared::with_records(records[..cut].to_vec());
        let (summary, model) = ceiling_run(
            &resumed,
            Arc::new(CeilingPlane::new(CeilingAnswer::Nothing)),
        )
        .await;
        assert_eq!(summary.reason, RunFinishReason::ContextCeiling, "cut {cut}");
        assert!(model.calls().is_empty(), "cut {cut}: refused model I/O");
        let after = resumed.records();
        let state = crate::reconcile_run_state(after.iter(), "run").unwrap();
        assert_eq!(
            state.context_ceiling.as_ref().unwrap().cause,
            "view_timeout",
            "cut {cut}"
        );
        assert!(state.error.is_none());
        let deadlines: Vec<_> = after
            .iter()
            .filter_map(|r| match r {
                WalRecord::ContextCeilingWait { deadline_ms, .. } => Some(*deadline_ms),
                _ => None,
            })
            .collect();
        assert!(
            deadlines.iter().all(|d| *d == deadlines[0]),
            "cut {cut}: wait extended"
        );
    }
}

#[test]
fn ceiling_records_and_terminal_require_a_named_reader_gate() {
    let records = [
        WalRecord::ContextCeilingWait {
            step_id: broca_protocol::StepId(1),
            deadline_ms: 12,
            unavailable: None,
        },
        WalRecord::ContextCeilingDecided {
            step_id: broca_protocol::StepId(1),
            detail: Some(broca_protocol::ContextCeilingDetail::new(
                10,
                11,
                "over_window",
            )),
        },
        WalRecord::RunFinished {
            interrupt_reason: None,
            reason: RunFinishReason::ContextCeiling,
            ts_ms: None,
            error: None,
            usage: Usage::default(),
            retries_used: 0,
        },
    ];
    for record in records {
        assert!(
            record.required_features().contains(&"context-ceiling/v1"),
            "{record:?}"
        );
    }
}

#[tokio::test]
async fn cancellation_and_shutdown_stop_a_ceiling_ride_without_sending() {
    for cancel in [true, false] {
        let shared = Shared::default();
        let plane = Arc::new(CeilingPlane::new(CeilingAnswer::Late));
        let signal = tokio_util::sync::CancellationToken::new();
        let control = crate::RunControl {
            cancel: cancel.then(|| signal.clone()),
            stop_requested: (!cancel).then(|| signal.clone()),
            ..Default::default()
        };
        let run = ceiling_run_with_control(&shared, plane.clone(), control);
        let stop = async {
            plane.entered.notified().await;
            signal.cancel();
        };
        let ((summary, model), ()) = tokio::join!(run, stop);
        assert_eq!(
            summary.reason,
            if cancel {
                RunFinishReason::Cancelled
            } else {
                RunFinishReason::Interrupted
            }
        );
        assert!(model.calls().is_empty());
        assert!(!matches!(
            crate::replay(&shared.records())
                .unwrap()
                .compaction
                .state()
                .unwrap()
                .ceiling_check,
            Some(CeilingCheck::Waiting { .. })
        ));
    }
}

#[tokio::test]
async fn a_recorded_ceiling_wait_resumes_the_same_fitting_request_at_every_cut() {
    let plane = || {
        Arc::new(CeilingPlane {
            wait_ms: 300_000,
            ..CeilingPlane::new(CeilingAnswer::View)
        })
    };
    let shared = Shared::default();
    let (_, model) = ceiling_run(&shared, plane()).await;
    let sent = crate::canonical_request_bytes(&model.calls()[0]);
    let records = shared.records();
    let first_wait = records
        .iter()
        .position(|r| matches!(r, WalRecord::ContextCeilingWait { .. }))
        .unwrap();
    let mut request_bearing_cuts = 0;
    for cut in first_wait + 1..=records.len() {
        let expected_requests = usize::from(
            !records[..cut]
                .iter()
                .any(|record| matches!(record, WalRecord::ModelStepFinished { .. })),
        );
        let resumed = Shared::with_records(records[..cut].to_vec());
        let (summary, model) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            ceiling_run(&resumed, plane()),
        )
        .await
        .expect("ceiling recovery must not extend or restart the wait");
        assert_eq!(summary.reason, RunFinishReason::Completed, "cut {cut}");
        let calls = model.calls();
        assert_eq!(calls.len(), expected_requests, "cut {cut}");
        request_bearing_cuts += expected_requests;
        for call in calls {
            assert_eq!(crate::canonical_request_bytes(&call), sent, "cut {cut}");
        }
    }
    assert!(request_bearing_cuts > 0);
}
