use super::*;
use broca_session_store::backend::SessionBackend;

tokio::task_local! {
    static DECLARED: bool;
}

pub(super) fn declared() -> bool {
    DECLARED.try_with(|declared| *declared).unwrap_or(false)
}

fn stamp(method: &str, params: &Value) -> u64 {
    let state = if method == "compaction.setup" {
        params
    } else {
        &params["conditions"]
    };
    state["statement_seq"]
        .as_u64()
        .expect("declaring provider receives a stamp")
}

fn monotonic(calls: &[(String, Value)]) -> Vec<u64> {
    let stamps: Vec<_> = calls
        .iter()
        .map(|(method, params)| stamp(method, params))
        .collect();
    assert!(!stamps.is_empty());
    assert!(
        stamps.windows(2).all(|pair| pair[0] <= pair[1]),
        "{calls:?}"
    );
    stamps
}

#[tokio::test]
async fn compaction_statement_seq_survives_seal_reopen_and_daemon_state_restart() {
    DECLARED
        .scope(true, async {
            in_each_cipher(|| async {
                let backend = MemoryBackend::default();
                let mut session = enrollment_session(&backend).await;
                let mut parts = MemoryParts::default();
                let spy = CompactionSpy::new(&CompactionScript {
                    context_limit: 1200,
                    rides: vec![Ride::Trim(2)],
                });
                let mut first_run = contract_run(
                    "r1",
                    "one",
                    vec![
                        answer(
                            "a1",
                            vec![call("c1", "big", json!({}))],
                            FinishReason::ToolCalls,
                        ),
                        text_step("a2", "first"),
                    ],
                );
                first_run.limit = Some(1200);
                enroll_run(&mut session, &mut parts, &spy, &first_run)
                    .await
                    .unwrap();
                let first = {
                    let calls = spy.calls.lock().unwrap();
                    let stamps = monotonic(&calls);
                    assert_eq!(calls[0].0, "compaction.setup");
                    // Setup covers the window it names, so no notice repeats an entry Setup
                    // already saw (compaction N6).
                    let newest = &calls[0].1["newest"];
                    for (method, params) in &calls[1..] {
                        if method != "compaction.notice" {
                            continue;
                        }
                        for entry in params["entries"].as_array().into_iter().flatten() {
                            assert!(
                                entry["ordinal"].as_u64() > newest["ordinal"].as_u64()
                                    || (entry["ordinal"] == newest["ordinal"]
                                        && entry["digest"] != newest["digest"]),
                                "a notice repeated an entry Setup saw: {entry} (Setup named {newest})"
                            );
                        }
                    }
                    assert!(calls.iter().any(|(method, _)| method == "compaction.ride"));
                    stamps
                };
                let compaction = broca_core::compaction_v2::session::Session::default();
                compaction
                    .restore(
                        broca_session_loop::compaction::restore(
                            &session.open_log_records().await.unwrap(),
                            &bind(),
                            &session.operations,
                        )
                        .unwrap(),
                    )
                    .await;
                broca_session_loop::compaction::seal_ledger(&mut session, &compaction, 1000)
                    .await
                    .unwrap();
                let after_seal = session.next_seq() - 1;
                assert!(after_seal >= *first.last().unwrap());
                drop(session);
                let mut reopened = Session::load(
                    &backend,
                    enrollment_addr(),
                    &enrollment_triple(),
                    &enrollment_key(),
                )
                .await
                .unwrap();
                // No session/runtime object from the former process is passed to the run helper.
                let before = spy.calls.lock().unwrap().len();
                enroll_run(
                    &mut reopened,
                    &mut parts,
                    &spy,
                    &contract_run("r2", "two", vec![text_step("a2", "second")]),
                )
                .await
                .unwrap();
                let calls = spy.calls.lock().unwrap();
                let all = monotonic(&calls);
                assert_eq!(calls[before].0, "compaction.setup");
                assert!(all[before] >= after_seal);
            })
            .await;
        })
        .await;
}

/// A torn unsynced suffix can reuse positions. The reopened session must replace the
/// provider's stamp with Setup before sending any conditions from its recovered state.
#[tokio::test]
async fn compaction_statement_seq_after_a_torn_unsynced_tail_starts_with_setup() {
    DECLARED
        .scope(true, async {
            in_each_cipher(|| async {
                let backend = MemoryBackend::default();
                let mut session = enrollment_session(&backend).await;
                let mut parts = MemoryParts::default();
                let spy = CompactionSpy::new(&CompactionScript {
                    context_limit: ENROLLED_LIMIT,
                    rides: Vec::new(),
                });
                enroll_run(
                    &mut session,
                    &mut parts,
                    &spy,
                    &contract_run("r1", "one", vec![text_step("a1", "first")]),
                )
                .await
                .unwrap();
                let (log, durable_end) = session.open_log();
                let generation = session.head().generation;
                let unsynced: Vec<_> = (0..8)
                    .map(|_| {
                        Record::WriterOpened(broca_session_store::record::WriterOpened {
                            generation,
                        })
                    })
                    .collect();
                session.append(&unsynced, false).await.unwrap();
                let old_stamp = session.next_seq() - 1;
                tell(&session, &spy).await;
                {
                    let calls = spy.calls.lock().unwrap();
                    let (method, params) = calls.last().unwrap();
                    assert_eq!(stamp(method, params), old_stamp);
                }
                let end = backend.log_end(&enrollment_addr(), &log).await.unwrap();
                let mut bytes = backend
                    .read_log(&enrollment_addr(), &log, 0..end)
                    .await
                    .unwrap();
                bytes.truncate(durable_end as usize + 1);
                backend.set_log(&enrollment_addr(), &log, bytes);
                drop(session);
                let mut reopened = Session::load(
                    &backend,
                    enrollment_addr(),
                    &enrollment_triple(),
                    &enrollment_key(),
                )
                .await
                .unwrap();
                assert!(
                    reopened.next_seq() - 1 < old_stamp,
                    "the crash actually discarded stamped positions"
                );
                let before = spy.calls.lock().unwrap().len();
                enroll_run(
                    &mut reopened,
                    &mut parts,
                    &spy,
                    &contract_run("r2", "two", vec![text_step("a2", "second")]),
                )
                .await
                .unwrap();
                let calls = spy.calls.lock().unwrap();
                assert_eq!(calls[before].0, "compaction.setup");
                monotonic(&calls[before..]);
            })
            .await;
        })
        .await;
}

async fn tell(session: &Session<&MemoryBackend>, spy: &CompactionSpy) {
    let provider = ScriptedProvider::new(Vec::new());
    let scenario = compacted_scenario(Vec::new());
    let mut request = scenario.plain_request_for("observe", &[user("observe")]);
    let control = RunControl {
        manifest: scenario.manifest.clone(),
        admitted_context_limit: Some(ENROLLED_LIMIT),
        ..Default::default()
    };
    request.context_limit = control.admitted_context_limit;
    let admission = broca_session_loop::admit::admission(
        &mut request,
        &control,
        &provider,
        &session.operations,
        Some(1),
    )
    .unwrap();
    let tools = enrollment_plane();
    let seams = Seams {
        provider: &provider,
        tools: &tools,
        bind: bind(),
        out: None,
        cancel: CancellationToken::new(),
        stop: Default::default(),
        held: None,
        approval: &HarnessPolicy,
        hook_plane: None,
        now_ms: &harness_now,
        session_address: None,
        compaction_plane: Some(spy),
        compaction_session: None,
        compaction_changes: false,
        keep_warm: None,
        account_reports: None,
        max_same_account_wait: Default::default(),
        auth_selection: None,
        step_progress: None,
        model_call_ready: None,
        pause_for_auth_at_start: None,
        account_continue: None,
    };
    let ctx = broca_session_loop::compaction::Ctx {
        session: Arc::default(),
        provider: "compactor".into(),
        preset: "head".into(),
        earlier: None,
        window: Default::default(),
    };
    broca_session_loop::compaction::notice(session, &admission, &seams, &ctx, None)
        .await
        .unwrap();
}

#[tokio::test]
async fn compaction_statement_seq_on_todays_storage_continues_after_restart() {
    DECLARED
        .scope(true, async {
            LAYOUT
                .scope(
                    Some(cortexkit_compaction_view::PLAN_VERSION),
                    COMPACTION.scope(
                        Some(Arc::new(CompactionScript {
                            context_limit: 32768,
                            rides: Vec::new(),
                        })),
                        async {
                            let scenario = compacted_scenario(vec![
                                answer(
                                    "a1",
                                    vec![call("c1", "read", json!({}))],
                                    FinishReason::ToolCalls,
                                ),
                                text_step("a2", "second"),
                            ]);
                            let (first, image, name) =
                                today_from(&scenario, None, scenario.steps.clone()).await;
                            let framed = image.framed();
                            let boundary = framed
                                .iter()
                                .rposition(|frame| {
                                    matches!(frame.record, WalRecord::StepStarted { .. })
                                })
                                .unwrap();
                            let cut = image.end_of_records(boundary + 1);
                            let last = framed[boundary].seq;
                            let (resumed, _, _) = today_from(
                                &scenario,
                                Some((&name, &image.bytes[..cut])),
                                vec![text_step("a2", "second")],
                            )
                            .await;
                            let parse = |outcome: &Outcome| {
                                outcome
                                    .calls
                                    .iter()
                                    .filter(|call| call.starts_with("compaction."))
                                    .map(|call| {
                                        let (method, _) = call.split_once(' ').unwrap();
                                        let params =
                                            serde_json::from_str(&call[call.find('{').unwrap()..])
                                                .unwrap();
                                        (method.to_owned(), params)
                                    })
                                    .collect::<Vec<(String, Value)>>()
                            };
                            let first = parse(&first);
                            let before = monotonic(&first)
                                .into_iter()
                                .filter(|seq| *seq <= last)
                                .max()
                                .unwrap();
                            let resumed = parse(&resumed);
                            assert_eq!(resumed[0].0, "compaction.setup");
                            assert!(monotonic(&resumed)[0] >= before);
                        },
                    ),
                )
                .await;
        })
        .await;
}
