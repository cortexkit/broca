use super::revision14::{body, effort, route, send, signed, start};
use super::*;
use broca_core::compaction_v2::received::ReceivedView;
use broca_provider::variant::VariantRequest;
use broca_session_store::record::{PartKind, Pause};
use cortexkit_role_compaction::view::{Edit, EditOp, View};

const OPUS: &str = "claude-opus-5-5";

fn cut_before_first_attempt(source: Source, run: &str, paused: bool) -> Start {
    let mut cut = start(source);
    let step_start = cut
        .records
        .iter()
        .position(|record| matches!(record, Record::StepStart(step) if step.run_id == run))
        .unwrap();
    cut.records.truncate(step_start + usize::from(!paused));
    if paused {
        cut.records.push(Record::Pause(Pause {
            run_id: run.into(),
            reason: PauseReason::Restart,
            ts_ms: None,
            error: None,
            account_limit: None,
        }));
    }
    assert!(!cut
        .records
        .iter()
        .any(|record| matches!(record, Record::Attempt(attempt) if attempt.run_id == run)));
    cut
}

async fn resume_on(
    target: (ModelHandle, FrozenRenderConfig),
    cut: Start,
    steps: Vec<Vec<NormalizedEvent>>,
) -> Source {
    let scenario = reader_scenario(steps.clone());
    ROUTE
        .scope(target, Box::pin(new_source(&scenario, Some(cut), steps)))
        .await
}

#[tokio::test]
async fn first_step_start_without_an_attempt_can_resume() {
    let modern = route(OPUS, 4);
    let initial = send("r1", modern.clone(), None, vec![signed("first", false)]).await;
    let cut = cut_before_first_attempt(initial, "r1", false);
    assert!(matches!(cut.records.last(), Some(Record::StepStart(_))));
    let resumed = resume_on(modern, cut, vec![signed("first", false)]).await;
    assert_eq!(resumed.outcome.requests.len(), 1);
    assert!(resumed
        .records
        .iter()
        .any(|record| matches!(record, Record::Attempt(attempt) if attempt.run_id == "r1")));
}

#[tokio::test]
async fn an_effort_projection_reset_does_not_echo_a_reply_that_saw_the_update() {
    let a = send("r1", effort("low"), None, vec![signed("low", false)]).await;
    let b = send(
        "r2",
        effort("high"),
        Some(start(a)),
        vec![signed("high", false)],
    )
    .await;
    let sent = body(&effort("high").1, &b.outcome.requests[0]);
    assert!(
        sent.contains(r#""content":[],"output_config":{"effort":"high"}"#),
        "{sent}"
    );
    let cut = start(b);
    for enable_messages in [true, false] {
        let mut changed = effort("high");
        if enable_messages {
            let VariantRequest::Anthropic {
                thinking: Some(thinking),
                ..
            } = &mut changed.1.variant.as_mut().unwrap().request
            else {
                unreachable!()
            };
            thinking["display"] = "summarized".into();
        } else {
            changed.1.effort_change_cache = false;
        }
        let prior = Start {
            records: cut.records.clone(),
            parts: cut.parts.clone(),
            snapshot: None,
            converted: false,
        };
        let c = send(
            "r3",
            changed.clone(),
            Some(prior),
            vec![signed("new", false)],
        )
        .await;
        let sent = body(&changed.1, &c.outcome.requests[0]);
        assert!(!sent.contains(r#""content":[],"output_config"#), "{sent}");
        assert!(!sent.contains("signature-high"), "{sent}");
    }
}

#[tokio::test]
async fn roll_forward_checks_the_effective_renderer_that_sent_the_attempt() {
    let modern = route(OPUS, 4);
    let first = send("r1", modern.clone(), None, vec![signed("first", false)]).await;
    let second = send(
        "r2",
        modern.clone(),
        Some(start(first)),
        vec![signed("second", false)],
    )
    .await;
    let downgraded = resume_on(
        route(OPUS, 2),
        cut_before_first_attempt(second, "r2", true),
        vec![signed("older", false)],
    )
    .await;
    assert_eq!(downgraded.outcome.requests.len(), 1);
    assert!(downgraded.records.iter().any(
        |record| matches!(record, Record::ConfigRefrozen(config) if config.renderer_version == 2)
    ));
    let next = send(
        "r3",
        modern.clone(),
        Some(start(downgraded)),
        vec![signed("roll-forward", false)],
    )
    .await;
    let request = body(&modern.1, &next.outcome.requests[0]);
    assert!(
        !request.contains("signature-older") && !request.contains("signature-first"),
        "{request}"
    );
}

#[tokio::test]
async fn run_start_cut_uses_its_input_even_when_prior_ordinal_cannot_be_named() {
    let modern = route(OPUS, 4);
    let a = send("r1", route(OPUS, 2), None, vec![signed("older", false)]).await;
    let b = CUTOFF_INDEX_MISSING
        .scope(
            (),
            send(
                "r2",
                modern.clone(),
                Some(start(a)),
                vec![signed("fresh", false)],
            ),
        )
        .await;
    assert_eq!(b.outcome.requests.len(), 1);
    let request = &b.outcome.requests[0];
    let input = b
        .records
        .iter()
        .find_map(|record| match record {
            Record::EntryFacts(facts) if facts.ordinal == 3 => Some(facts.mid.as_str()),
            _ => None,
        })
        .unwrap();
    assert!(b.records.iter().any(|record| matches!(record, Record::Attempt(attempt) if attempt.run_id == "r2" && attempt.signed_floor.as_deref() == Some(input))));
    assert_eq!(request.render_episodes.signed_floor, Some(3));
    let mut prior = request.clone();
    Arc::make_mut(&mut prior.render_episodes).signed_floor = Some(2);
    assert_eq!(body(&modern.1, request), body(&modern.1, &prior));
    assert!(!body(&modern.1, request).contains("signature-older"));
}

fn view_cut(records: &[Record], ordinal: u64) -> Record {
    let through = records
        .iter()
        .find_map(|record| match record {
            Record::EntryFacts(facts) if facts.ordinal == ordinal => Some(facts.mid.clone()),
            _ => None,
        })
        .unwrap();
    let view = ReceivedView::from(View::seal(1, vec![Edit::new(1, EditOp::Cut { through })]));
    broca_session_loop::compaction::view_record(None, &view, None).unwrap()
}

#[tokio::test]
async fn post_seal_historical_config_damage_ends_with_the_same_named_error() {
    let modern = route(OPUS, 4);
    LAYOUT.scope(Some(cortexkit_compaction_view::PLAN_VERSION), COMPACTION.scope(Some(Arc::new(CompactionScript { context_limit: 100_000, rides: vec![] })), Box::pin(async {
        let first = compacted_scenario(vec![signed("first", false)]);
        let first = ROUTE.scope(modern.clone(), Box::pin(new_source(&first, None, first.steps.clone()))).await;
        let digest = first.records.iter().find_map(|record| match record { Record::RunStart(start) => start.setup.iter().find(|part| part.kind == PartKind::RenderConfig).map(|part| part.digest), _ => None }).unwrap();
        let view = view_cut(&first.records, 2);
        let mut cut = start(first); cut.records.push(view);
        let script = compacted_scenario(vec![signed("not-sent", false)]);
        let rejected = DAMAGED_AFTER_SEAL.scope((digest, std::cell::Cell::new(false)), ROUTE.scope(modern, FRESH_RUN.scope("r2", Box::pin(new_source(&script, Some(cut), script.steps.clone()))))).await;
        assert!(rejected.outcome.requests.is_empty());
        assert!(rejected.records.iter().any(|record| matches!(record, Record::RunEnd(end) if end.error.as_ref().is_some_and(|error| error.parse()["provider_code"] == "historical_render_config_unavailable"))));
    }))).await;
}

#[tokio::test]
async fn coalesced_runs_do_not_extend_the_ledger_walk_past_the_latest_attempt() {
    LAYOUT.scope(Some(cortexkit_compaction_view::PLAN_VERSION), COMPACTION.scope(Some(Arc::new(CompactionScript { context_limit: 100_000, rides: vec![] })), Box::pin(async {
        let modern = route(OPUS, 4);
        let script = compacted_scenario(vec![signed("first", false)]);
        let a = ROUTE.scope(modern.clone(), Box::pin(new_source(&script, None, script.steps.clone()))).await;
        let b = ROUTE.scope(modern.clone(), FRESH_RUN.scope("r2", Box::pin(new_source(&script, Some(start(a)), script.steps.clone())))).await;
        let c = ROUTE.scope(modern, FRESH_RUN.scope("r3", Box::pin(new_source(&script, Some(start(b)), script.steps.clone())))).await;
        let pre_send = cut_before_first_attempt(c, "r3", true);
        let split = pre_send.records.iter().position(|record| matches!(record, Record::RunStart(run) if run.run_id == "r2")).unwrap();
        let mut stored = historical::session_at(&pre_send.records[..split]).await;
        stored.seal(None, 0).await.unwrap();
        let addr = SessionAddr("0123456789abcdef".into());
        let earlier_ledger = stored.backend().object_names(&addr).into_iter().find(|name| name.ends_with("/l")).unwrap();
        stored.append(&pre_send.records[split..], true).await.unwrap();
        stored.seal_at_entry(5, &Default::default(), 0).await.unwrap();
        assert!(stored.conversation.sealed_episodes.iter().any(|episode| episode.run_id == "r1"));
        assert!(!stored.conversation.sealed_episodes.iter().any(|episode| episode.run_id == "r2"));
        stored.backend().set_object(&addr, &earlier_ledger, vec![0]);
        let read = Writer::cut_records(&stored).await.unwrap();
        assert!(matches!(read.first(), Some(Record::Attempt(attempt)) if attempt.run_id == "r2"));
        assert_eq!(read.len(), 2, "only the anchor attempt and the next run start: {read:?}");
        assert!(stored.ledger_tail_until(|record| matches!(record, Record::RunStart(run) if run.run_id == "r1")).await.is_err());
    }))).await;
}

#[tokio::test]
async fn an_unchanged_unsent_run_does_not_discard_the_previous_signer() {
    let modern = route(OPUS, 4);
    let a = send("r1", modern.clone(), None, vec![signed("known", false)]).await;
    let b = send(
        "r2",
        modern.clone(),
        Some(start(a)),
        vec![signed("unsent", false)],
    )
    .await;
    let c = send(
        "r3",
        modern.clone(),
        Some(cut_before_first_attempt(b, "r2", true)),
        vec![signed("sent", false)],
    )
    .await;
    assert_eq!(c.outcome.requests.len(), 1);
    assert!(body(&modern.1, &c.outcome.requests[0]).contains("signature-known"));
}

#[tokio::test]
async fn compaction_notice_names_the_new_input_without_retaining_earlier_thinking() {
    let modern = route(OPUS, 4);
    LAYOUT
        .scope(
            Some(cortexkit_compaction_view::PLAN_VERSION),
            COMPACTION.scope(
                Some(Arc::new(CompactionScript {
                    context_limit: 100_000,
                    rides: vec![],
                })),
                Box::pin(async {
                    let old = compacted_scenario(vec![signed("older", false)]);
                    let old = ROUTE
                        .scope(
                            route(OPUS, 2),
                            Box::pin(new_source(&old, None, old.steps.clone())),
                        )
                        .await;
                    let current = compacted_scenario(vec![signed("fresh", false)]);
                    let current = ROUTE
                        .scope(
                            modern.clone(),
                            FRESH_RUN.scope(
                                "r2",
                                Box::pin(new_source(
                                    &current,
                                    Some(start(old)),
                                    current.steps.clone(),
                                )),
                            ),
                        )
                        .await;
                    let input = current
                        .records
                        .iter()
                        .find_map(|record| match record {
                            Record::EntryFacts(facts) if facts.ordinal == 3 => {
                                Some(facts.mid.as_str())
                            }
                            _ => None,
                        })
                        .unwrap();
                    assert!(current.records.iter().any(|record| matches!(record, Record::Attempt(attempt) if attempt.run_id == "r2" && attempt.signed_floor.as_deref() == Some(input))));
                    let params = |method: &str| current.outcome.calls.iter().find_map(|call| serde_json::from_str::<Value>(call.strip_prefix(method)?).ok()).unwrap();
                    let setup = params("compaction.setup ");
                    let notice = params("compaction.notice ");
                    assert_eq!(setup["newest"]["ordinal"], 3);
                    // The spy normalizes each ID, retaining the shared sequence number.
                    let newest = setup["newest"]["mid"].as_str().unwrap().strip_prefix("<mid").unwrap();
                    let through = notice["conditions"]["signed_floor"]["through"].as_str().unwrap().strip_prefix("<through").unwrap();
                    assert_eq!(through, newest);
                    let opts = &current.outcome.requests[0];
                    let mut old_spelling = opts.clone();
                    Arc::make_mut(&mut old_spelling.render_episodes).signed_floor = Some(2);
                    assert_eq!(body(&modern.1, opts), body(&modern.1, &old_spelling));
                    assert!(!body(&modern.1, opts).contains("signature-older"));
                }),
            ),
        )
        .await;
}

#[tokio::test]
async fn the_reader_can_rebuild_an_attempt_using_the_still_supported_legacy_echo() {
    let target = route(OPUS, 2);
    let mut raw = serde_json::to_value(&target.1).unwrap();
    raw["reasoning"]
        .as_object_mut()
        .unwrap()
        .remove("echo_scope");
    let scenario = reader_scenario(vec![signed("legacy", false)]);
    let source = RAW_RENDER
        .scope(
            raw,
            ROUTE.scope(
                target,
                Box::pin(new_source(&scenario, None, scenario.steps.clone())),
            ),
        )
        .await;
    let stored = historical::session_at(&source.records).await;
    let parts = MemoryParts {
        parts: source.parts,
    };
    let records = stored.open_log_records().await.unwrap();
    let replay = broca_session_loop::recorded_request::rebuild(
        &stored,
        &records,
        &parts,
        &bind(),
        "r1",
        Some(1),
        broca_session_loop::recorded_request::Through::Request,
    )
    .await;
    assert!(replay.is_ok(), "{replay:?}");
}

#[tokio::test]
async fn an_intact_presend_resume_of_run_two_keeps_the_existing_prefix() {
    let config = route(OPUS, 4);
    let first = send("r1", config.clone(), None, vec![signed("old", false)]).await;
    let second = send(
        "r2",
        config.clone(),
        Some(start(first)),
        vec![signed("new", false)],
    )
    .await;
    let expected = body(&config.1, &second.outcome.requests[0]);
    assert!(expected.contains("signature-old"));
    let resumed = resume_on(
        config.clone(),
        cut_before_first_attempt(second, "r2", false),
        vec![signed("new", false)],
    )
    .await;
    assert_eq!(resumed.outcome.requests.len(), 1);
    assert_eq!(body(&config.1, &resumed.outcome.requests[0]), expected);
}

#[tokio::test]
async fn a_presend_refreeze_and_its_next_run_agree_on_the_sent_effort_prefix() {
    let first = send("r1", effort("low"), None, vec![signed("low", false)]).await;
    let second = send(
        "r2",
        effort("high"),
        Some(start(first)),
        vec![signed("high", false)],
    )
    .await;
    let mut refrozen = effort("high");
    let VariantRequest::Anthropic {
        thinking: Some(thinking),
        ..
    } = &mut refrozen.1.variant.as_mut().unwrap().request
    else {
        unreachable!()
    };
    thinking["display"] = "summarized".into();
    let second = resume_on(
        refrozen.clone(),
        cut_before_first_attempt(second, "r2", true),
        vec![signed("refrozen-high", false)],
    )
    .await;
    assert!(second.records.iter().any(
        |record| matches!(record, Record::ConfigRefrozen(config) if config.renderer_version == 4)
    ));
    let sent = body(&refrozen.1, &second.outcome.requests[0]);
    assert!(!sent.contains("signature-low"), "{sent}");
    assert!(
        sent.contains(r#""content":[],"output_config":{"effort":"high"}"#),
        "{sent}"
    );
    let third = send(
        "r3",
        refrozen.clone(),
        Some(start(second)),
        vec![signed("next", false)],
    )
    .await;
    let sent = body(&refrozen.1, &third.outcome.requests[0]);
    assert!(!sent.contains(r#""content":[],"output_config"#), "{sent}");
    assert!(!sent.contains("signature-refrozen-high"), "{sent}");
}

#[tokio::test]
async fn reverse_refreeze_uses_v4_and_does_not_reintroduce_reasoning() {
    let modern = route(OPUS, 4);
    let first = send(
        "r1",
        route(OPUS, 2),
        None,
        vec![signed("pre-upgrade", false)],
    )
    .await;
    let second = send(
        "r2",
        modern.clone(),
        Some(start(first)),
        vec![signed("signed-after-cut", false)],
    )
    .await;
    let third = send(
        "r3",
        route(OPUS, 2),
        Some(start(second)),
        vec![signed("unused", false)],
    )
    .await;
    let resumed = resume_on(
        modern.clone(),
        cut_before_first_attempt(third, "r3", true),
        vec![signed("new", false)],
    )
    .await;
    assert!(resumed.records.iter().any(
        |record| matches!(record, Record::ConfigRefrozen(config) if config.renderer_version == 4)
    ));
    let sent = body(&modern.1, &resumed.outcome.requests[0]);
    assert!(!sent.contains("signature-pre-upgrade"), "{sent}");
    assert!(!sent.contains("signature-signed-after-cut"), "{sent}");
}

#[tokio::test]
async fn older_ledger_damage_does_not_interrupt_an_intact_unsent_step() {
    let config = route(OPUS, 4);
    let first = send("r1", config.clone(), None, vec![signed("old", false)]).await;
    let second = send(
        "r2",
        config.clone(),
        Some(start(first)),
        vec![signed("new", false)],
    )
    .await;
    let mut cut = cut_before_first_attempt(second, "r2", false);
    let attempt = cut
        .records
        .iter()
        .position(|record| matches!(record, Record::Attempt(attempt) if attempt.run_id == "r1"))
        .unwrap();
    let mut stored = historical::session_at(&cut.records[..attempt]).await;
    stored.seal(None, 0).await.unwrap();
    let addr = SessionAddr("0123456789abcdef".into());
    let intact = stored.backend().object_names(&addr);
    stored
        .append(&cut.records[attempt..=attempt], true)
        .await
        .unwrap();
    stored.seal(None, 0).await.unwrap();
    let older_ledger = stored
        .backend()
        .object_names(&addr)
        .into_iter()
        .find(|name| name.ends_with("/l") && !intact.contains(name))
        .unwrap();
    stored
        .append(&cut.records[attempt + 1..], true)
        .await
        .unwrap();
    stored.backend().set_object(&addr, &older_ledger, vec![0]);
    cut.snapshot = Some(stored.backend().snapshot());
    let resumed = resume_on(config.clone(), cut, vec![signed("new", false)]).await;
    assert_eq!(resumed.outcome.requests.len(), 1);
    assert!(!body(&config.1, &resumed.outcome.requests[0]).contains("signature-old"));
    assert_eq!(
        resumed.outcome.requests[0].render_episodes.signed_floor,
        Some(3)
    );
}

#[tokio::test]
async fn a_presend_refreeze_that_disables_per_message_effort_strips_older_signatures() {
    let first = send("r1", effort("low"), None, vec![signed("low", false)]).await;
    let second = send(
        "r2",
        effort("high"),
        Some(start(first)),
        vec![signed("high", false)],
    )
    .await;
    let update = r#""content":[],"output_config":{"effort":"high"}"#;
    assert!(body(&effort("high").1, &second.outcome.requests[0]).contains(update));
    let third = send(
        "r3",
        effort("high"),
        Some(start(second)),
        vec![signed("unused", false)],
    )
    .await;
    let mut refrozen = effort("high");
    refrozen.1.effort_change_cache = false;
    let resumed = resume_on(
        refrozen.clone(),
        cut_before_first_attempt(third, "r3", true),
        vec![signed("refrozen", false)],
    )
    .await;
    assert!(resumed.records.iter().any(|record| matches!(record, Record::ConfigRefrozen(config) if config.render_config["effort_change_cache"] != true)));
    let sent = body(&refrozen.1, &resumed.outcome.requests[0]);
    assert!(!sent.contains(update), "{sent}");
    assert!(
        !sent.contains("signature-high") && !sent.contains("signature-low"),
        "{sent}"
    );
}

#[tokio::test]
async fn following_an_effort_refreeze_does_not_echo_over_a_different_top_level_effort() {
    let first = send("r1", effort("low"), None, vec![signed("unused", false)]).await;
    let high = effort("high");
    let first = resume_on(
        high.clone(),
        cut_before_first_attempt(first, "r1", true),
        vec![signed("refrozen-high", false)],
    )
    .await;
    let wire = |opts: &CallOptions| serde_json::from_str::<Value>(&body(&high.1, opts)).unwrap();
    assert_eq!(
        wire(&first.outcome.requests[0])["output_config"]["effort"],
        "high"
    );
    let next = send(
        "r2",
        high.clone(),
        Some(start(first)),
        vec![signed("following", false)],
    )
    .await;
    let sent = wire(&next.outcome.requests[0]);
    assert_eq!(sent["output_config"]["effort"], "low");
    assert!(
        !sent.to_string().contains("signature-refrozen-high"),
        "{sent}"
    );
}

#[tokio::test]
async fn a_refrozen_tool_loop_keeps_its_own_thinking_but_later_runs_do_not() {
    let high = effort("high");
    let first = send("r1", effort("low"), None, vec![signed("unused", false)]).await;
    let first = resume_on(
        high.clone(),
        cut_before_first_attempt(first, "r1", true),
        vec![
            signed("refrozen-call", true),
            signed("refrozen-done", false),
        ],
    )
    .await;
    assert_eq!(first.outcome.requests.len(), 2);
    assert!(body(&high.1, &first.outcome.requests[1]).contains("signature-refrozen-call"));
    let second = send(
        "r2",
        high.clone(),
        Some(start(first)),
        vec![signed("following", false)],
    )
    .await;
    let floor = second.outcome.requests[0]
        .render_episodes
        .signed_floor
        .unwrap_or(0);
    let sent = body(&high.1, &second.outcome.requests[0]);
    assert!(
        !sent.contains("signature-refrozen-call") && !sent.contains("signature-refrozen-done"),
        "{sent}"
    );
    let third = send(
        "r3",
        high.clone(),
        Some(start(second)),
        vec![signed("next", false)],
    )
    .await;
    let request = &third.outcome.requests[0];
    let sent = body(&high.1, request);
    assert!(request.render_episodes.signed_floor.unwrap_or(0) >= floor);
    assert!(
        !sent.contains("signature-refrozen-call") && !sent.contains("signature-refrozen-done"),
        "{sent}"
    );
    assert!(sent.contains("signature-following"), "{sent}");
}

#[tokio::test]
async fn a_transient_signing_config_read_takes_the_run_start_cut_and_sends() {
    let config = route(OPUS, 4);
    let first = send("r1", config.clone(), None, vec![signed("older", false)]).await;
    let second = send(
        "r2",
        config.clone(),
        Some(start(first)),
        vec![signed("unused", false)],
    )
    .await;
    let resumed = SIGNING_CONFIG_READ_FAILS
        .scope(
            (),
            resume_on(
                config.clone(),
                cut_before_first_attempt(second, "r2", false),
                vec![signed("sent", false)],
            ),
        )
        .await;
    assert_eq!(resumed.outcome.requests.len(), 1);
    assert!(!body(&config.1, &resumed.outcome.requests[0]).contains("signature-older"));
    assert_eq!(
        resumed.outcome.requests[0].render_episodes.signed_floor,
        Some(3)
    );
}
