//! Compare sparse live prompts with a full-transcript control through the real run loop.

use super::*;
use crate::compaction_provider::{lineage::MessageRef, notice::ViewReady, view::*};

#[path = "media_resume_tests.rs"]
mod media;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WaitViews {
    Off,
    RideOnly,
    EventOnly,
    BoundaryThenRide,
}

/// What the provider's second view (version 3) does with the messages the first one replaced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Later {
    /// The same shape as the first view, covering more.
    Same,
    /// Replace `[0, 2)` with a summary and a copy of the stored signed message at ordinal 1
    /// (with one byte of its signature changed when `tamper`), and keep `[2, coverage]`.
    CopySigned { tamper: bool },
    /// Replace `[0, 2)` with a summary and keep `[2, coverage]`, editing the text of ordinal 2.
    EditReleased,
}

struct Windows {
    wal: Shared,
    newer: bool,
    later: Later,
    lineage: Mutex<String>,
    wait: WaitViews,
    releases: Arc<AtomicUsize>,
    ready_after_ride: std::sync::atomic::AtomicBool,
}

impl Windows {
    fn candidate(&self, session: &str, version: u64) -> CompactionView {
        let state = crate::replay(&self.wal.records()).unwrap();
        let covered = if version == 2 { 2 } else { state.extent - 1 };
        if version == 3 && self.later != Later::Same {
            let mut replacement = vec![text(Role::User, "summary of the first exchange")];
            let mut edits = Vec::new();
            match self.later {
                Later::CopySigned { tamper } => {
                    let mut copy = state.prompt[1].clone();
                    assert!(copy.content.iter().any(super::is_signed));
                    if tamper {
                        let ContentBlock::Reasoning {
                            signature: Some(signature),
                            ..
                        } = &mut copy.content[0]
                        else {
                            panic!("ordinal 1 starts with signed reasoning");
                        };
                        signature.pop();
                        signature.push('f');
                    }
                    replacement.push(copy);
                }
                Later::EditReleased => edits.push(PartEdit::replace(
                    2,
                    "m2",
                    broca_protocol::tags::part_key(2, 0),
                    json!({"type":"text","text":"edited second question"}).to_string(),
                )),
                Later::Same => unreachable!(),
            }
            return CompactionView::new(
                session,
                version,
                self.lineage.lock().unwrap().clone(),
                MessageRef {
                    ordinal: covered,
                    mid: format!("m{covered}"),
                },
                vec![
                    ViewRange::replace(0, 2, serde_json::to_string(&replacement).unwrap()),
                    ViewRange::keep_with_edits(2, covered + 1, edits),
                ],
            );
        }
        let mut continuation = text(Role::User, "continue");
        continuation.content.extend(
            super::super::media_refs(&state.prompt[2].content)
                .cloned()
                .map(ContentBlock::Media),
        );
        CompactionView::new(
            session,
            version,
            self.lineage.lock().unwrap().clone(),
            MessageRef {
                ordinal: covered,
                mid: format!("m{covered}"),
            },
            vec![
                ViewRange::keep(0, 1),
                ViewRange::replace(
                    1,
                    covered + 1,
                    serde_json::to_string(&vec![text(Role::Assistant, "summary"), continuation])
                        .unwrap(),
                ),
            ],
        )
    }
}

impl CompactionViews for Windows {
    fn newest_ready(&self, session: &str, lineage: &str, after: Option<u64>) -> Option<ViewReady> {
        *self.lineage.lock().unwrap() = lineage.into();
        if self.wait == WaitViews::RideOnly
            || (self.wait == WaitViews::EventOnly && !self.ready_after_ride.load(Ordering::SeqCst))
        {
            return None;
        }
        let steps = self
            .wal
            .records()
            .iter()
            .filter(|record| matches!(record, WalRecord::ModelStepFinished { .. }))
            .count();
        let version = if self.newer && steps >= 2 { 3 } else { 2 };
        (after.is_none_or(|after| version > after))
            .then(|| ViewReady::for_view(&self.candidate(session, version)))
    }
}

#[async_trait::async_trait]
impl CompactionPlane for Windows {
    async fn setup(
        &self,
        _: &str,
        _: &BindIdentity,
        request: SetupRequest,
    ) -> Result<SetupAnswer, CompactionUnavailable> {
        let mut answer = ready();
        if let Some(lineage) = &request.lineage_id {
            *self.lineage.lock().unwrap() = lineage.clone();
        }
        answer["request_id"] = request.request_id.into();
        answer["initial"]["replacement"] = json!([]);
        if self.wait != WaitViews::Off {
            answer["ceiling"] = json!({"at_share":0.0001,"on_overflow":true,"wait_ms":10000});
        }
        Ok(serde_json::from_value(answer).unwrap())
    }

    async fn view_get(
        &self,
        _: &str,
        _: &BindIdentity,
        request: ViewGetRequest,
    ) -> Result<ViewGetAnswer, CompactionUnavailable> {
        Ok(ViewGetAnswer::view(
            self.candidate(&request.session_ref, request.view_version),
        ))
    }

    async fn ride(
        &self,
        _: &str,
        _: &BindIdentity,
        request: crate::compaction_provider::ride::RideRequest,
    ) -> Result<crate::compaction_provider::ride::RideAnswer, CompactionUnavailable> {
        if self.wait != WaitViews::Off
            && matches!(
                request.cause,
                crate::compaction_provider::ride::RideCause::Ceiling
            )
        {
            if self.wait == WaitViews::EventOnly {
                self.ready_after_ride.store(true, Ordering::SeqCst);
                return Ok(crate::compaction_provider::ride::RideAnswer::nothing());
            }
            let version = if self.wait == WaitViews::BoundaryThenRide {
                assert_eq!(
                    self.releases.load(Ordering::SeqCst),
                    1,
                    "ordinary view released before waiting for the ceiling provider"
                );
                3
            } else {
                2
            };
            return Ok(crate::compaction_provider::ride::RideAnswer::view(
                self.candidate(&request.session_ref, version),
            ));
        }
        Ok(crate::compaction_provider::ride::RideAnswer::nothing())
    }
}

#[derive(Debug)]
struct Held {
    ordinals: Vec<u64>,
    extent: u64,
    originals: usize,
    episodes: Vec<(usize, Option<usize>, Option<u64>, u64)>,
    extra_wal_input_owners: usize,
    approval_base: usize,
}

struct HistoryModel(crate::StubProvider);

struct NoPendingElicitation;
#[async_trait::async_trait]
impl crate::tool_plane::ElicitationPlane for NoPendingElicitation {}
#[async_trait::async_trait]
impl crate::Provider for HistoryModel {
    async fn step(
        &self,
        step: broca_protocol::StepId,
        opts: &CallOptions,
    ) -> tokio::sync::mpsc::Receiver<broca_protocol::NormalizedEvent> {
        self.0.step(step, opts).await
    }
    fn reasoning_is_prefix_bound(&self) -> bool {
        true
    }
    fn render_config_blob(&self) -> Value {
        json!({"render_config":"stateless"})
    }
}

async fn run(
    shared: &Shared,
    retain: bool,
    newer: bool,
    scripts: Vec<Vec<broca_protocol::NormalizedEvent>>,
    store: Option<Arc<dyn crate::SessionStore>>,
) -> (Vec<CallOptions>, Vec<Held>) {
    run_episode(
        shared,
        retain,
        newer,
        scripts,
        store,
        crate::StartMode::FreshOrResume,
        WaitViews::Off,
    )
    .await
}

async fn run_episode(
    shared: &Shared,
    retain: bool,
    newer: bool,
    scripts: Vec<Vec<broca_protocol::NormalizedEvent>>,
    store: Option<Arc<dyn crate::SessionStore>>,
    mode: crate::StartMode,
    wait: WaitViews,
) -> (Vec<CallOptions>, Vec<Held>) {
    run_configured(
        shared,
        retain,
        newer,
        scripts,
        store,
        EpisodeScenario {
            mode,
            wait,
            ..EpisodeScenario::fresh(Later::Same)
        },
    )
    .await
}

/// The prompt a run held when each view was first rendered, by view version.
type ViewPrompts = Arc<Mutex<Vec<(u64, Vec<Message>)>>>;

struct EpisodeScenario {
    mode: crate::StartMode,
    wait: WaitViews,
    budget: bool,
    later: Later,
    media: bool,
    /// The WAL refuses to be read once a view applied, so released messages cannot be read
    /// back from it.
    history_unavailable: bool,
    /// Receives the prompt the run holds when each view is first rendered, by view version.
    prompts: Option<ViewPrompts>,
}

impl EpisodeScenario {
    fn fresh(later: Later) -> Self {
        Self {
            mode: crate::StartMode::FreshOrResume,
            wait: WaitViews::Off,
            budget: true,
            later,
            media: false,
            history_unavailable: false,
            prompts: None,
        }
    }
}

async fn run_configured(
    shared: &Shared,
    retain: bool,
    newer: bool,
    scripts: Vec<Vec<broca_protocol::NormalizedEvent>>,
    store: Option<Arc<dyn crate::SessionStore>>,
    scenario: EpisodeScenario,
) -> (Vec<CallOptions>, Vec<Held>) {
    let EpisodeScenario {
        mode,
        wait,
        budget,
        later,
        media,
        history_unavailable,
        prompts,
    } = scenario;
    let (mut request, mut control, _) = history_request();
    request.input.extend([
        text(Role::Assistant, "third answer"),
        text(Role::User, "third question"),
    ]);
    if media {
        request.input[2]
            .content
            .push(ContentBlock::Media(super::image()));
    }
    request.input[3].content.insert(
        0,
        ContentBlock::Reasoning {
            text: "retained thinking".into(),
            signature: Some("retained-signature".into()),
        },
    );
    if matches!(mode, crate::StartMode::AppendEpisode) {
        request.run_id = broca_protocol::RunId("appended".into());
        request.submission_id = Some(broca_protocol::SubmissionId("appended".into()));
        request.input = vec![text(Role::User, "new episode")];
    }
    request.tools = vec![serde_json::from_value(
        json!({"name":"read","description":"read","input_schema":{"type":"object"}}),
    )
    .unwrap()];
    request.policy_allowed.insert("read".into());
    request.stop_when = vec![broca_protocol::StopCondition::StepCount { n: 10 }];
    request.budget = budget.then(|| {
        serde_json::from_value(json!({"nudges":[{"at_tool_calls":2,"text":"budget reminder"}]}))
            .unwrap()
    });
    let releases = Arc::new(AtomicUsize::new(0));
    let cancel = tokio_util::sync::CancellationToken::new();
    if wait != WaitViews::Off {
        control.admitted_context_limit = Some(10000);
        control.cancel = Some(cancel.clone());
    }
    let windows = Arc::new(Windows {
        wal: shared.clone(),
        newer,
        later,
        lineage: Mutex::new(String::new()),
        wait,
        releases: releases.clone(),
        ready_after_ride: std::sync::atomic::AtomicBool::new(false),
    });
    control.compaction_views = Some(windows.clone());
    control.compaction_plane = Some(windows);
    if shared
        .records()
        .iter()
        .any(|record| matches!(record, WalRecord::ToolCallDeferred { .. }))
    {
        control.elicitation_plane = Some(Arc::new(NoPendingElicitation));
    }
    control.retain_full_prompt = retain;
    let release_count = releases.clone();
    control.view_release_probe = Some(Arc::new(move |prompt, extent, state| {
        let view = &state.view.as_ref().unwrap().view;
        if let Some(prompts) = &prompts {
            let mut prompts = prompts.lock().unwrap();
            if prompts.last().map(|(version, _)| *version) != Some(view.view_version) {
                prompts.push((view.view_version, prompt.to_vec()));
            }
        }
        if !retain {
            let expected = (0..extent)
                .filter(|ordinal| {
                    let mut message = Message::new(Role::User, vec![]);
                    message.ordinal = Some(*ordinal);
                    crate::run::held_prompt::held_by(view, &message)
                })
                .collect::<Vec<_>>();
            assert_eq!(
                prompt
                    .iter()
                    .map(|message| message.ordinal.unwrap())
                    .collect::<Vec<_>>(),
                expected
            );
        }
        if wait != WaitViews::Off {
            release_count.fetch_add(1, Ordering::SeqCst);
            if matches!(state.ceiling_check, Some(CeilingCheck::Waiting { .. })) {
                cancel.cancel();
            }
        }
    }));
    let held = Arc::new(Mutex::new(Vec::new()));
    let capture = held.clone();
    let input = shared.records().iter().find_map(|record| match record {
        WalRecord::RunStarted { input, .. } => Some(Arc::downgrade(input)),
        _ => None,
    });
    let input_owners = input.as_ref().map_or(0, |input| input.strong_count());
    control.held_prompt_probe = Some(Arc::new(
        move |prompt, extent, originals, history, approval_base| {
            capture.lock().unwrap().push(Held {
                ordinals: prompt
                    .iter()
                    .map(|message| message.ordinal.unwrap())
                    .collect(),
                extent,
                originals,
                episodes: history
                    .iter()
                    .map(|episode| {
                        (
                            episode.render.prompt_start,
                            episode.render.first_user,
                            episode.first_step,
                            episode.end,
                        )
                    })
                    .collect(),
                extra_wal_input_owners: input
                    .as_ref()
                    .map_or(0, |input| input.strong_count().saturating_sub(input_owners)),
                approval_base,
            });
        },
    ));
    let model = HistoryModel(crate::StubProvider::new(scripts));
    let tools = crate::StubToolPlane::new().with_tool("read", |_| {
        crate::StubReply::Output(broca_protocol::ToolOutput::Text {
            text: "same".into(),
        })
    });
    let writer = shared.writer();
    let (out, _events) = tokio::sync::mpsc::channel(256);
    crate::run_loop_on_writer(
        request,
        &writer,
        mode,
        &model,
        &tools,
        &crate::AllowAll,
        &ResumeWal(shared, history_unavailable),
        store,
        control,
        &out,
    )
    .await
    .unwrap();
    writer.shutdown().await.unwrap();
    if wait != WaitViews::Off {
        assert_eq!(
            releases.load(Ordering::SeqCst),
            if wait == WaitViews::BoundaryThenRide {
                2
            } else {
                1
            },
            "view releases observed before more ceiling I/O"
        );
    }
    (
        model.0.calls(),
        Arc::try_unwrap(held).unwrap().into_inner().unwrap(),
    )
}

fn scripts(from: u64) -> Vec<Vec<broca_protocol::NormalizedEvent>> {
    (from..4)
        .map(|step| tool_call_events("read", step + 1))
        .chain(std::iter::once(model_events()))
        .collect()
}

fn stable_records(records: &[WalRecord]) -> Vec<Value> {
    records
        .iter()
        .map(|record| {
            let mut value = serde_json::to_value(record).unwrap();
            // Independent executions have different wall-clock I/O times, not different records.
            for key in [
                "ts_ms",
                "dispatched_at_ms",
                "finished_at_ms",
                "deadline_ms",
                "written_at_ms",
            ] {
                value.as_object_mut().unwrap().remove(key);
            }
            value
        })
        .collect()
}

#[tokio::test]
async fn views_release_before_continuing_a_ceiling_wait() {
    for wait in [
        WaitViews::RideOnly,
        WaitViews::EventOnly,
        WaitViews::BoundaryThenRide,
    ] {
        let sparse = Shared::default();
        let (actual, _) = run_episode(
            &sparse,
            false,
            false,
            scripts(0),
            None,
            crate::StartMode::FreshOrResume,
            wait,
        )
        .await;
        let full = Shared::default();
        let (expected, _) = run_episode(
            &full,
            true,
            false,
            scripts(0),
            None,
            crate::StartMode::FreshOrResume,
            wait,
        )
        .await;
        assert!(
            expected.is_empty() && actual.is_empty(),
            "cancellation occurs inside the wait"
        );
        let records = sparse.records();
        assert_records(&records, &full.records());
        let waits: Vec<_> = records
            .iter()
            .enumerate()
            .filter_map(|(index, record)| match record {
                WalRecord::ContextCeilingWait { deadline_ms, .. } => Some((index, *deadline_ms)),
                _ => None,
            })
            .collect();
        assert_eq!(
            waits.len(),
            2,
            "the ride result is committed once: {wait:?}"
        );
        assert_eq!(
            waits[0].1, waits[1].1,
            "yielding does not reset the absolute deadline"
        );
        let applied: Vec<_> = records
            .iter()
            .enumerate()
            .filter_map(|(index, record)| {
                matches!(record, WalRecord::CompactionViewApplied { .. }).then_some(index)
            })
            .collect();
        assert!(applied.last().unwrap() > &waits[0].0);
        if wait == WaitViews::BoundaryThenRide {
            assert!(applied[0] < waits[0].0);
            assert_eq!(applied.len(), 2);
        }
        let through = records.iter().rev().find_map(|record| match record {
            WalRecord::CompactionNotice { headers, .. } => {
                view::Announced::from_headers(headers).through_ordinal
            }
            _ => None,
        });
        assert_eq!(
            through,
            Some(4),
            "notices retain the newest message's ordinal and digest after release"
        );
    }
}

fn assert_records(actual: &[WalRecord], expected: &[WalRecord]) {
    let applied = expected
        .iter()
        .position(|record| matches!(record, WalRecord::CompactionViewApplied { .. }))
        .unwrap();
    assert_eq!(actual.len(), expected.len());
    for (index, (actual, expected)) in stable_records(actual)
        .iter()
        .zip(stable_records(expected))
        .enumerate()
        .skip(applied)
    {
        assert_eq!(*actual, expected, "WAL record {index}");
    }
}

struct ResumeWal<'a>(&'a Shared, bool);
impl crate::WalStore for ResumeWal<'_> {
    fn open(&self, session: &BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
        crate::WalStore::open(self.0, session)
    }
    fn read_records(&self, _: &BindIdentity) -> Result<Vec<FramedRecord>, WalError> {
        let applied = self
            .0
            .records()
            .iter()
            .any(|record| matches!(record, WalRecord::CompactionViewApplied { .. }));
        if self.1 && applied {
            return Err(WalError::Io(std::io::Error::other(
                "the WAL cannot be read",
            )));
        }
        Ok(self.0.frames())
    }
    fn read_records_range(
        &self,
        _: &BindIdentity,
        after: u64,
        through: Option<u64>,
        _: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<broca_wal::WalRangeRead, WalError> {
        let mut range = broca_wal::WalRangeRead::without_anchors(
            self.0
                .frames()
                .into_iter()
                .filter(|frame| frame.seq > after && through.is_none_or(|head| frame.seq <= head))
                .collect(),
        );
        range.lineage_id = Some(fixture_lineage_id());
        Ok(range)
    }
}

#[tokio::test]
async fn release_keeps_absolute_positions_request_bytes_and_later_wal_records() {
    let full = Shared::default();
    let (full_calls, _) = run(&full, true, true, scripts(0), None).await;
    let sparse = Shared::default();
    let (sparse_calls, held) = run(&sparse, false, true, scripts(0), None).await;
    assert_eq!(held[0].ordinals, vec![0, 3, 4]);
    assert_eq!(held[0].extent, 5);
    assert_eq!(held[0].originals, 3);
    assert_eq!(held[2].ordinals, vec![0]);
    assert_eq!(
        held[2].extent, 10,
        "newer view covers the newest message, including a budget firing"
    );
    assert_eq!(held[3].ordinals, vec![0, 10, 11]);
    assert_eq!(sparse_calls.len(), full_calls.len());
    for (sparse, full) in sparse_calls.iter().zip(&full_calls) {
        assert_eq!(
            crate::canonical_request_bytes(sparse),
            crate::canonical_request_bytes(full)
        );
    }
    assert_records(&sparse.records(), &full.records());
    assert!(sparse
        .records()
        .iter()
        .any(|record| matches!(record, WalRecord::BudgetFired { ordinal: 9, .. })));
    assert!(sparse.records().iter().any(|record| matches!(
        record,
        WalRecord::CompactionViewApplied {
            choice: broca_wal::CompactionViewChoice::Companion { .. },
            ..
        }
    )));
    let text =
        String::from_utf8(crate::canonical_request_bytes(sparse_calls.last().unwrap())).unwrap();
    assert!(
        text.contains("returned exactly the same result 4 times"),
        "the active observer survives release: {text}"
    );
}

#[tokio::test]
async fn full_wal_resume_releases_before_its_first_request_and_preserves_guidance() {
    let full = Shared::default();
    let (expected, _) = run(&full, true, false, scripts(0), None).await;
    let records = full.records();
    let cut = records
        .iter()
        .position(|record| matches!(record, WalRecord::StepStarted { step_id } if step_id.0 == 4))
        .unwrap();
    let resumed = Shared::with_records(records[..cut].to_vec());
    let (calls, held) = run(&resumed, false, false, scripts(3), None).await;
    assert_eq!(held[0].ordinals, vec![0, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    assert_eq!(held[0].extent, 12);
    assert_eq!(
        held[0].approval_base, 0,
        "ordinary runs have no approval recovery owner"
    );
    assert_eq!(
        held[0].extra_wal_input_owners, 0,
        "the complete replay's WAL frames were released"
    );
    assert!(!expected[3..].is_empty());
    assert_eq!(calls.len(), expected[3..].len());
    for (actual, expected) in calls.iter().zip(&expected[3..]) {
        assert_eq!(
            crate::canonical_request_bytes(actual),
            crate::canonical_request_bytes(expected)
        );
    }
}

#[tokio::test]
async fn deferred_approval_history_keeps_its_independent_frame_owner() {
    let full = Shared::default();
    let (expected, _) = run(&full, true, false, scripts(0), None).await;
    let mut records = full.records();
    let intent = records
        .iter()
        .position(|record| {
            matches!(record,
        WalRecord::ToolDispatchIntent { tool_call_id, .. } if tool_call_id.0 == "call-1")
        })
        .unwrap();
    let (batch, id, args) = match &records[intent] {
        WalRecord::ToolDispatchIntent {
            batch_id,
            tool_call_id,
            args,
            ..
        } => (
            batch_id.0.clone(),
            tool_call_id.0.clone(),
            args.as_ref().clone(),
        ),
        _ => unreachable!(),
    };
    let mut deferred = crate::replay::deferred_tests::prepared(&id, &batch);
    if let WalRecord::ToolCallDeferred { tool, args_jcs, .. } = &mut deferred {
        *tool = "read".into();
        *args_jcs = serde_json::to_string(&args).unwrap();
    }
    // Prepared call keys bind the WAL lineage and the deferred record's absolute sequence.
    let key = format!("{}:{}", fixture_lineage_id(), intent + 1);
    let mut approved =
        crate::replay::deferred_tests::approval(&id, &key, broca_wal::DeferredDecision::Approved);
    if let WalRecord::ApprovalRecorded {
        approval_digest, ..
    } = &mut approved
    {
        *approval_digest = None;
    }
    records.insert(intent, deferred);
    records.insert(intent + 1, approved);
    if let WalRecord::ToolDispatchIntent {
        call_key,
        approval_digest,
        ..
    } = &mut records[intent + 2]
    {
        *call_key = Some(key);
        *approval_digest = None;
    }
    let cut = records
        .iter()
        .position(|record| {
            matches!(record,
        WalRecord::StepStarted { step_id } if step_id.0 == 4)
        })
        .unwrap();
    let resumed = Shared::with_records(records[..cut].to_vec());
    let (calls, held) = run(&resumed, false, false, scripts(3), None).await;
    assert_eq!(calls.len(), 2);
    assert_eq!(
        held[0].approval_base, cut,
        "approval recovery retains its complete source frames"
    );
    assert_eq!(
        held[0].extra_wal_input_owners, 1,
        "History, not the ordinary admission owner, keeps the raw WAL"
    );
    assert_eq!(held[0].ordinals, vec![0, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    for (actual, expected) in calls.iter().zip(&expected[3..]) {
        assert_eq!(
            crate::canonical_request_bytes(actual),
            crate::canonical_request_bytes(expected)
        );
    }
}

#[derive(Clone, Copy, Debug)]
enum Fallback {
    PrefixRead,
    Watermark,
    Lineage,
    SeededReplay,
    BudgetSeed,
    IncompleteCheckpoint,
    StaleCheckpoint,
    BudgetWatermarkRace,
    MissingEpisodes,
}

struct BrokenStore {
    prefix: crate::StorePrefix,
    admission: crate::AdmissionStoreSnapshot,
    failure: Fallback,
    prefix_reads: AtomicUsize,
}

impl crate::MaterializedStore for BrokenStore {
    fn read_admission_snapshot(
        &self,
        _: &BindIdentity,
    ) -> Result<Option<crate::AdmissionStoreSnapshot>, String> {
        let mut admission = self.admission.clone();
        let checkpoint = admission.checkpoint.as_mut().unwrap();
        match self.failure {
            Fallback::BudgetSeed => {
                checkpoint
                    .last_episode
                    .as_mut()
                    .unwrap()
                    .budget_replay_state = None
            }
            Fallback::IncompleteCheckpoint => admission.checkpoint = None,
            Fallback::StaleCheckpoint => checkpoint.at_seq -= 1,
            _ => {}
        }
        Ok(Some(admission))
    }

    fn read_prefix_with_watermark(&self, _: &BindIdentity) -> Result<crate::StorePrefix, String> {
        self.prefix_reads.fetch_add(1, Ordering::SeqCst);
        let mut prefix = self.prefix.clone();
        match self.failure {
            Fallback::PrefixRead => return Err("forced prefix read failure".into()),
            Fallback::Watermark => prefix.watermark_seq = u64::MAX,
            Fallback::Lineage => prefix.lineage_id = Some("another-lineage".into()),
            Fallback::SeededReplay => {
                prefix.prefix.pop();
            }
            Fallback::BudgetWatermarkRace => prefix.watermark_seq += 1,
            _ => {}
        }
        Ok(prefix)
    }
}

#[tokio::test]
async fn active_episode_without_held_messages_keeps_its_live_observer() {
    let seed = Shared::default();
    run(&seed, true, false, scripts(0), None).await;
    let prefix = seed.records();
    let extent = crate::replay(&prefix).unwrap().extent;
    let full = Shared::with_records(prefix.clone());
    let (expected, _) = run_episode(
        &full,
        true,
        true,
        scripts(0),
        None,
        crate::StartMode::AppendEpisode,
        WaitViews::Off,
    )
    .await;
    let sparse = Shared::with_records(prefix);
    let (actual, held) = run_episode(
        &sparse,
        false,
        true,
        scripts(0),
        None,
        crate::StartMode::AppendEpisode,
        WaitViews::Off,
    )
    .await;
    assert_eq!(
        held[0].ordinals,
        vec![0],
        "no message of the active episode is held"
    );
    assert_eq!(held[0].extent, extent + 1);
    assert_eq!(
        held[0].episodes,
        vec![
            (0, Some(0), Some(5), extent),
            (extent as usize, Some(extent as usize), None, extent + 1)
        ]
    );
    assert_eq!(held[1].ordinals, vec![0, extent + 1, extent + 2]);
    assert_eq!(held[1].episodes.last().unwrap().2, Some(extent + 1));
    assert_eq!(actual.len(), 5);
    for (actual, expected) in actual.iter().zip(&expected) {
        assert_eq!(
            crate::canonical_request_bytes(actual),
            crate::canonical_request_bytes(expected)
        );
    }
    assert_records(&sparse.records(), &full.records());
}

impl crate::Persister for BrokenStore {
    fn project(&self, _: &BindIdentity, _: &[FramedRecord]) -> Result<u64, String> {
        Ok(self.prefix.watermark_seq)
    }
}

#[tokio::test]
async fn every_full_replay_fallback_releases_before_the_first_request() {
    let full = Shared::default();
    let (expected, _) = run(&full, true, false, scripts(0), None).await;
    let records = full.records();
    let cut = records
        .iter()
        .position(|record| matches!(record, WalRecord::StepStarted { step_id } if step_id.0 == 4))
        .unwrap();
    let watermark = records
        .iter()
        .position(|record| matches!(record, WalRecord::ToolBatchFinished { .. }))
        .unwrap()
        + 1;
    let projected = crate::replay(&records[..watermark]).unwrap();
    let checkpoint =
        crate::AdmissionCheckpointV1::from_frames(&full.frames()[..watermark]).unwrap();
    let prefix = crate::StorePrefix {
        lineage_id: Some(fixture_lineage_id().to_string()),
        prefix: projected.prompt,
        completed_steps: projected.completed_steps,
        usage: projected.usage,
        retries_used: projected.retries_used,
        last_step_usage: projected.last_step_usage,
        last_step_finish_reason: projected.last_step_finish_reason,
        watermark_seq: watermark as u64,
        context_epoch: 0,
    };
    for failure in [
        Fallback::PrefixRead,
        Fallback::Watermark,
        Fallback::Lineage,
        Fallback::SeededReplay,
        Fallback::BudgetSeed,
        Fallback::IncompleteCheckpoint,
        Fallback::StaleCheckpoint,
        Fallback::BudgetWatermarkRace,
        Fallback::MissingEpisodes,
    ] {
        let store = Arc::new(BrokenStore {
            prefix: prefix.clone(),
            admission: crate::AdmissionStoreSnapshot {
                watermark_seq: watermark as u64,
                lineage_id: prefix.lineage_id.clone(),
                anchor: None,
                checkpoint: Some(checkpoint.clone()),
            },
            failure,
            prefix_reads: AtomicUsize::new(0),
        });
        let resumed = Shared::with_records(records[..cut].to_vec());
        let (calls, held) = run(&resumed, false, false, scripts(3), Some(store.clone())).await;
        assert!(
            !held.is_empty(),
            "{failure:?}: no first request; terminal: {:?}",
            resumed.records().last()
        );
        assert_eq!(
            held[0].ordinals,
            vec![0, 3, 4, 5, 6, 7, 8, 9, 10, 11],
            "{failure:?}"
        );
        assert_eq!(held[0].extent, 12, "{failure:?}");
        assert_eq!(
            held[0].episodes,
            vec![(0, Some(0), Some(5), 12)],
            "{failure:?}: original admitted first user"
        );
        assert_eq!(calls.len(), expected.len() - 3, "{failure:?}");
        for (actual, expected) in calls.iter().zip(&expected[3..]) {
            assert_eq!(
                crate::canonical_request_bytes(actual),
                crate::canonical_request_bytes(expected),
                "{failure:?}"
            );
        }
        if matches!(
            failure,
            Fallback::PrefixRead
                | Fallback::Watermark
                | Fallback::Lineage
                | Fallback::SeededReplay
                | Fallback::BudgetWatermarkRace
                | Fallback::MissingEpisodes
        ) {
            assert!(
                store.prefix_reads.load(Ordering::SeqCst) > 0,
                "{failure:?} never reached prefix validation"
            );
        }
    }
}

#[tokio::test]
async fn admission_only_prefix_and_tail_views_keep_episode_metadata() {
    let full = Shared::default();
    let (expected, _) = run_configured(
        &full,
        true,
        true,
        scripts(0),
        None,
        EpisodeScenario {
            budget: false,
            ..EpisodeScenario::fresh(Later::Same)
        },
    )
    .await;
    let records = full.records();
    let watermark = records
        .iter()
        .position(|record| matches!(record, WalRecord::RunStarted { .. }))
        .unwrap()
        + 1;
    let cut = records
        .iter()
        .position(|record| matches!(record, WalRecord::StepStarted { step_id } if step_id.0 == 4))
        .unwrap();
    let projected = crate::replay(&records[..watermark]).unwrap();
    let store = Arc::new(BrokenStore {
        prefix: crate::StorePrefix {
            lineage_id: Some(fixture_lineage_id().to_string()),
            prefix: projected.prompt,
            completed_steps: projected.completed_steps,
            usage: projected.usage,
            retries_used: projected.retries_used,
            last_step_usage: projected.last_step_usage,
            last_step_finish_reason: projected.last_step_finish_reason,
            watermark_seq: watermark as u64,
            context_epoch: 0,
        },
        admission: crate::AdmissionStoreSnapshot {
            watermark_seq: watermark as u64,
            lineage_id: Some(fixture_lineage_id().to_string()),
            anchor: None,
            checkpoint: Some(
                crate::AdmissionCheckpointV1::from_frames(&full.frames()[..watermark]).unwrap(),
            ),
        },
        failure: Fallback::MissingEpisodes,
        prefix_reads: AtomicUsize::new(0),
    });
    let resumed = Shared::with_records(records[..cut].to_vec());
    let (calls, held) = run_configured(
        &resumed,
        false,
        true,
        scripts(3),
        Some(store.clone()),
        EpisodeScenario {
            budget: false,
            ..EpisodeScenario::fresh(Later::Same)
        },
    )
    .await;
    assert!(store.prefix_reads.load(Ordering::SeqCst) > 0);
    assert_eq!(held[0].ordinals, vec![0, 9, 10]);
    assert_eq!(held[0].episodes, vec![(0, Some(0), Some(5), 11)]);
    assert_eq!(calls.len(), 2);
    for (actual, expected) in calls.iter().zip(&expected[3..]) {
        assert_eq!(
            crate::canonical_request_bytes(actual),
            crate::canonical_request_bytes(expected)
        );
    }
}

/// Run the scripted episode with `later` as the provider's second view, holding everything
/// (`retain`) or releasing history; returns the requests, the held prompts and the prompt the
/// run held each time a view applied.
async fn run_later(
    shared: &Shared,
    retain: bool,
    later: Later,
    history_unavailable: bool,
) -> (Vec<CallOptions>, Vec<Held>, Vec<(u64, Vec<Message>)>) {
    let prompts = Arc::new(Mutex::new(Vec::new()));
    let (calls, held) = run_configured(
        shared,
        retain,
        true,
        scripts(0),
        None,
        EpisodeScenario {
            history_unavailable,
            prompts: Some(prompts.clone()),
            ..EpisodeScenario::fresh(later)
        },
    )
    .await;
    let prompts = std::mem::take(&mut *prompts.lock().unwrap());
    (calls, held, prompts)
}

fn assert_same_requests(actual: &[CallOptions], expected: &[CallOptions]) {
    assert_eq!(actual.len(), expected.len());
    for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
        assert_eq!(
            String::from_utf8(crate::canonical_request_bytes(actual)).unwrap(),
            String::from_utf8(crate::canonical_request_bytes(expected)).unwrap(),
            "request {index}"
        );
    }
}

fn request_text(call: &CallOptions) -> String {
    String::from_utf8(crate::canonical_request_bytes(call)).unwrap()
}

fn records_text(records: &[WalRecord]) -> String {
    serde_json::to_string(records).unwrap()
}

#[tokio::test]
async fn a_later_view_copying_a_released_signed_message_is_accepted_like_a_full_run() {
    let later = Later::CopySigned { tamper: false };
    let full = Shared::default();
    let (expected, _, _) = run_later(&full, true, later, false).await;
    let sparse = Shared::default();
    let (actual, held, _) = run_later(&sparse, false, later, false).await;
    assert_eq!(views_applied(&full.records()), vec![2, 3]);
    assert_eq!(
        views_applied(&sparse.records()),
        vec![2, 3],
        "the copy of the released signed message is judged against its stored bytes"
    );
    assert_same_requests(&actual, &expected);
    assert_records(&sparse.records(), &full.records());
    let last = request_text(actual.last().unwrap());
    for shown in [
        "summary of the first exchange",
        "first answer",
        "second question",
    ] {
        assert!(last.contains(shown), "{shown} is rendered: {last}");
    }
    // Ordinal 2 was released under the first view; the second keeps it, so it is held again.
    let last_held = held.last().unwrap();
    assert_eq!(last_held.ordinals.first(), Some(&2));
    assert_eq!(
        last_held.ordinals,
        (2..last_held.extent).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn a_later_view_with_one_changed_byte_in_a_released_signed_copy_is_refused() {
    let later = Later::CopySigned { tamper: true };
    let full = Shared::default();
    let (expected, _, _) = run_later(&full, true, later, false).await;
    let sparse = Shared::default();
    let (actual, held, _) = run_later(&sparse, false, later, false).await;
    for records in [full.records(), sparse.records()] {
        assert_eq!(
            views_applied(&records),
            vec![2],
            "the first view stays applied"
        );
        assert!(records_text(&records).contains("view_refused:3:provider_constraint"));
    }
    assert_same_requests(&actual, &expected);
    assert_records(&sparse.records(), &full.records());
    assert!(!request_text(actual.last().unwrap()).contains("summary of the first exchange"));
    let last_held = held.last().unwrap();
    assert_eq!(
        last_held.ordinals,
        std::iter::once(0)
            .chain(3..last_held.extent)
            .collect::<Vec<_>>(),
        "the messages read back to judge the refused view were released again"
    );
}

#[tokio::test]
async fn a_later_view_editing_a_released_message_renders_like_a_full_run() {
    let later = Later::EditReleased;
    let full = Shared::default();
    let (expected, _, _) = run_later(&full, true, later, false).await;
    let sparse = Shared::default();
    let (actual, _, _) = run_later(&sparse, false, later, false).await;
    assert_eq!(views_applied(&full.records()), vec![2, 3]);
    assert_eq!(views_applied(&sparse.records()), vec![2, 3]);
    assert_same_requests(&actual, &expected);
    assert_records(&sparse.records(), &full.records());
    assert!(request_text(actual.last().unwrap()).contains("edited second question"));
}

#[tokio::test]
async fn a_later_view_whose_released_messages_cannot_be_read_is_refused_not_rendered_short() {
    let sparse = Shared::default();
    let (actual, held, _) =
        run_later(&sparse, false, Later::CopySigned { tamper: false }, true).await;
    let records = sparse.records();
    assert_eq!(views_applied(&records), vec![2]);
    assert!(records_text(&records).contains("view_refused:3:history_unavailable"));
    for call in &actual {
        assert!(!request_text(call).contains("summary of the first exchange"));
    }
    let last_held = held.last().unwrap();
    assert_eq!(
        last_held.ordinals,
        std::iter::once(0)
            .chain(3..last_held.extent)
            .collect::<Vec<_>>()
    );
}

/// The full-run prompt at the second view's decision, the first view, the second view, and
/// that prompt released to the first view's held set.
fn released_case(
    shared: &Shared,
    prompts: &[(u64, Vec<Message>)],
) -> (Vec<Message>, CompactionView, CompactionView, Vec<Message>) {
    let applied: Vec<CompactionView> = shared
        .records()
        .iter()
        .filter_map(|record| match record {
            WalRecord::CompactionViewApplied { view, choice, .. } => {
                Some(view::AppliedView::decode(view, choice).unwrap().view)
            }
            _ => None,
        })
        .collect();
    let [first, second] = <[CompactionView; 2]>::try_from(applied).unwrap();
    // The run holds everything, so the prompt it held when the second view first rendered is
    // the prompt that view was decided over at that boundary.
    let full = prompts
        .iter()
        .find(|(version, _)| *version == second.view_version)
        .unwrap()
        .1
        .clone();
    let released: Vec<Message> = full
        .iter()
        .filter(|message| crate::run::held_prompt::held_by(&first, message))
        .cloned()
        .collect();
    (full, first, second, released)
}

fn prompt_form() -> crate::run::released_history::PromptForm {
    crate::run::released_history::PromptForm {
        render_tags: true,
        render_config: crate::Provider::render_config_blob(&HistoryModel(
            crate::StubProvider::new(vec![]),
        )),
    }
}

#[tokio::test]
async fn released_lookup_returns_the_transcript_message_of_the_full_prompt() {
    let full_wal = Shared::default();
    let (_, _, prompts) = run_later(&full_wal, true, Later::EditReleased, false).await;
    let (full, _, second, released) = released_case(&full_wal, &prompts);
    let extent = full.last().unwrap().ordinal.unwrap() + 1;
    assert!(
        !released.iter().any(|message| message.ordinal == Some(2)),
        "the edited message lies outside the held set"
    );
    let stored = StoredTranscript::plain(&released).with_extent(extent);
    let needs = released::released_needs(&second, &released, stored, extent);
    assert!(needs.prompt.iter().any(|range| range.contains(&2)));
    let wal = ResumeWal(&full_wal, false);
    let writer = full_wal.writer();
    let history = crate::run::released_history::RunHistory {
        wal: Some(&wal),
        store: None,
        writer: &writer,
        session: bind(),
        form: prompt_form(),
    };
    let loaded = history.load(&needs).await.unwrap();
    let judging = released::Judging::new(&released, stored, &loaded);
    // `dispose_fetched` looks up the coverage's last message and each edited message.
    for ordinal in [2, second.covers_through.ordinal] {
        let looked_up = view::transcript_at(&judging.prompt, ordinal);
        assert!(looked_up.is_some());
        assert_eq!(looked_up, view::transcript_at(&full, ordinal), "m{ordinal}");
    }
    // Every message read back in prompt form has the full prompt's bytes, tags and repetition
    // guidance included.
    let everything = ReleasedNeeds {
        prompt: std::iter::once(0..extent).collect(),
        stored: Vec::new(),
    };
    let all = history.load(&everything).await.unwrap();
    assert_eq!(all.prompt, full);
    assert!(
        serde_json::to_string(&all.prompt)
            .unwrap()
            .contains("Tool repetition guidance"),
        "the fixture exercises the repetition guidance fold"
    );
    writer.shutdown().await.unwrap();
}

/// A materialized store holding a whole transcript: raw rows by page and the episode
/// projection, counting the page reads it answers.
struct PagedStore {
    rows: Vec<Message>,
    episodes: Vec<crate::context::EpisodeRenderProjection>,
    page_reads: AtomicUsize,
}

impl PagedStore {
    fn from_wal(records: &[WalRecord]) -> Self {
        let replay = crate::replay(records).unwrap();
        let spans = replay.episode_step_spans.clone().unwrap();
        let render = replay.render_episodes.clone().unwrap();
        let episodes = render
            .iter()
            .enumerate()
            .map(|(index, episode)| {
                let start = episode.prompt_start as u64;
                let end = render
                    .get(index + 1)
                    .map_or(replay.extent, |next| next.prompt_start as u64);
                crate::context::EpisodeRenderProjection {
                    start: Some(start),
                    first_user: Some(episode.first_user.map(|user| user as u64)),
                    first_step: Some(
                        spans
                            .iter()
                            .map(|span| span.start as u64)
                            .find(|first| start <= *first && *first < end),
                    ),
                    render_config: Some(episode.frozen_config.clone()),
                }
            })
            .collect();
        Self {
            rows: replay.prompt,
            episodes,
            page_reads: AtomicUsize::new(0),
        }
    }
}

impl crate::MaterializedStore for PagedStore {
    fn read_prefix_with_watermark(&self, _: &BindIdentity) -> Result<crate::StorePrefix, String> {
        panic!("released messages are read by page, never as a whole prefix")
    }

    fn read_page_with_watermark(
        &self,
        _: &BindIdentity,
        from: Option<u64>,
        limit: u64,
        view: crate::context::ReadView,
    ) -> Result<crate::context::StorePageSnapshot, String> {
        assert_eq!(view, crate::context::ReadView::Raw);
        self.page_reads.fetch_add(1, Ordering::SeqCst);
        let from = from.unwrap();
        // Pages are capped, so a long range takes several reads.
        let end = from + limit.min(3);
        Ok(crate::context::StorePageSnapshot {
            watermark_seq: 7,
            lineage_id: Some(fixture_lineage_id().to_string()),
            prefix_len: self.rows.len() as u64,
            rows: self
                .rows
                .iter()
                .filter(|m| m.ordinal.is_some_and(|o| from <= o && o < end))
                .map(|m| crate::context::StorePageRow {
                    written_at: None,
                    ordinal: m.ordinal.unwrap(),
                    message: m.clone(),
                    run: None,
                })
                .collect(),
            ..crate::context::StorePageSnapshot::empty()
        })
    }

    fn read_episode_projection(
        &self,
        _: &BindIdentity,
    ) -> Result<Option<Vec<crate::context::EpisodeRenderProjection>>, String> {
        Ok(Some(self.episodes.clone()))
    }
}

impl crate::Persister for PagedStore {
    fn project(&self, _: &BindIdentity, _: &[FramedRecord]) -> Result<u64, String> {
        Ok(7)
    }
}

#[tokio::test]
async fn released_messages_read_from_store_pages_match_the_wal() {
    let full_wal = Shared::default();
    let (_, _, prompts) =
        run_later(&full_wal, true, Later::CopySigned { tamper: false }, false).await;
    let (full, _, second, released) = released_case(&full_wal, &prompts);
    let extent = full.last().unwrap().ordinal.unwrap() + 1;
    let store = Arc::new(PagedStore::from_wal(&full_wal.records()));
    let writer = full_wal.writer();
    writer.sync().await.unwrap();
    let wal = ResumeWal(&full_wal, false);
    let from_wal = crate::run::released_history::RunHistory {
        wal: Some(&wal),
        store: None,
        writer: &writer,
        session: bind(),
        form: prompt_form(),
    };
    // No WAL reader: only the store can answer.
    let from_store = crate::run::released_history::RunHistory {
        wal: None,
        store: Some(store.clone()),
        writer: &writer,
        session: bind(),
        form: prompt_form(),
    };
    let stored = StoredTranscript::plain(&released).with_extent(extent);
    let needs = released::released_needs(&second, &released, stored, extent);
    assert!(
        needs.stored.iter().any(|range| range.contains(&1)),
        "the signed copy is judged against the stored message it stands for"
    );
    let everything = ReleasedNeeds {
        prompt: std::iter::once(0..extent).collect(),
        stored: vec![],
    };
    for needs in [needs, everything] {
        let expected = from_wal.load(&needs).await.unwrap();
        let actual = from_store.load(&needs).await.unwrap();
        assert_eq!(actual.prompt, expected.prompt);
        assert_eq!(actual.originals, expected.originals);
        assert_eq!(actual.stored, expected.stored);
        let renders = |loaded: &ReleasedMessages| {
            loaded
                .episodes
                .iter()
                .map(|episode| (episode.render.clone(), episode.first_step))
                .collect::<Vec<_>>()
        };
        assert_eq!(renders(&actual), renders(&expected));
    }
    assert!(store.page_reads.load(Ordering::SeqCst) > 2);
    writer.shutdown().await.unwrap();
}
