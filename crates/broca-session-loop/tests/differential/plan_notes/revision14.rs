use super::conservative::{apply, refresh, APPLY};
use super::*;
use broca_provider::{
    families::{anthropic_frozen_config, AnthropicMessages},
    variant::{FrozenVariant, VariantRequest},
    wire::{RefusalFallbackConfig, WireFamily},
};
use broca_session_loop::Parts;
use broca_session_store::record::PartKind;

const OPUS: &str = "claude-opus-5-5";
const FABLE: &str = "claude-fable-5-1";

pub(super) fn route(model: &str, version: u32) -> (ModelHandle, FrozenRenderConfig) {
    let mut frozen = anthropic_frozen_config(model);
    frozen.renderer_version = version;
    (ModelHandle::new("anthropic", model), frozen)
}

pub(super) fn start(source: Source) -> Start {
    Start {
        records: source.records,
        parts: source.parts,
        snapshot: None,
        converted: false,
    }
}

pub(super) fn signed(id: &str, tool: bool) -> Vec<NormalizedEvent> {
    let tail = if tool {
        call(id, "read", json!({}))
    } else {
        ContentBlock::Text { text: id.into() }
    };
    attempted(
        answer(
            id,
            vec![
                ContentBlock::Reasoning {
                    text: "private thought".into(),
                    signature: Some(format!("signature-{id}").into()),
                },
                tail,
            ],
            if tool {
                FinishReason::ToolCalls
            } else {
                FinishReason::Stop
            },
        ),
        vec![attempt(1, 200, "complete")],
    )
}

pub(super) async fn send(
    name: &'static str,
    route: (ModelHandle, FrozenRenderConfig),
    prior: Option<Start>,
    steps: Vec<Vec<NormalizedEvent>>,
) -> Source {
    let scenario = reader_scenario(steps.clone());
    ROUTE
        .scope(
            route,
            FRESH_RUN.scope(name, Box::pin(new_source(&scenario, prior, steps))),
        )
        .await
}

pub(super) fn body(frozen: &FrozenRenderConfig, opts: &CallOptions) -> String {
    String::from_utf8(AnthropicMessages.render(frozen, opts)).unwrap()
}

#[tokio::test]
async fn a_configured_but_unused_fallback_keeps_primary_reasoning() {
    let mut primary = route(OPUS, 4);
    let (model, frozen) = route(FABLE, 4);
    primary.1.refusal_fallback = Some(Box::new(RefusalFallbackConfig {
        model,
        frozen,
        context_limit: Some(100_000),
    }));
    let first = send("r1", primary.clone(), None, vec![signed("primary", false)]).await;
    let next = send(
        "r2",
        primary.clone(),
        Some(start(first)),
        vec![signed("second", false)],
    )
    .await;
    assert_eq!(next.outcome.requests[0].render_episodes.signed_floor, None);
    assert_eq!(
        body(&primary.1, &next.outcome.requests[0]),
        include_str!("primary_3f1914862.json").trim_end()
    );
}

#[tokio::test]
async fn an_upgrade_discards_older_reasoning_once_even_if_an_old_reply_signed_after_the_apply() {
    let old = route(OPUS, 2);
    let modern = route(OPUS, 4);
    let first = send("r1", old.clone(), None, vec![signed("before-apply", false)]).await;
    let signed_after = APPLY
        .scope(
            apply(1, None),
            send(
                "r2",
                old,
                Some(refresh(first, 1)),
                vec![signed("older-after-apply", false)],
            ),
        )
        .await;
    let admitted = send(
        "r3",
        modern.clone(),
        Some(start(signed_after)),
        vec![signed("modern", false)],
    )
    .await;
    assert_eq!(
        admitted.outcome.requests[0].render_episodes.signed_floor,
        Some(5)
    );
    let sent = body(&modern.1, &admitted.outcome.requests[0]);
    assert!(
        !sent.contains("signature-before-apply") && !sent.contains("signature-older-after-apply")
    );
    let later = send(
        "r4",
        modern.clone(),
        Some(start(admitted)),
        vec![signed("later", false)],
    )
    .await;
    assert_eq!(
        later.outcome.requests[0].render_episodes.signed_floor,
        Some(5)
    );
    assert!(body(&modern.1, &later.outcome.requests[0]).contains("signature-modern"));
}

#[tokio::test]
async fn switching_models_and_back_drops_at_the_boundary_and_then_keeps_new_signers() {
    let opus = route(OPUS, 4);
    let fable = route(FABLE, 4);
    let a = send("r1", opus.clone(), None, vec![signed("opus-first", false)]).await;
    let b = send(
        "r2",
        fable.clone(),
        Some(start(a)),
        vec![signed("fable", false)],
    )
    .await;
    assert_eq!(b.outcome.requests[0].render_episodes.signed_floor, Some(3));
    assert!(!body(&fable.1, &b.outcome.requests[0]).contains("signature-opus-first"));
    let c = send(
        "r3",
        opus.clone(),
        Some(start(b)),
        vec![signed("opus-again", false)],
    )
    .await;
    assert_eq!(c.outcome.requests[0].render_episodes.signed_floor, Some(5));
    let sent = body(&opus.1, &c.outcome.requests[0]);
    assert!(!sent.contains("signature-opus-first") && !sent.contains("signature-fable"));
    let d = send(
        "r4",
        opus.clone(),
        Some(start(c)),
        vec![signed("next", false)],
    )
    .await;
    assert!(body(&opus.1, &d.outcome.requests[0]).contains("signature-opus-again"));
}

#[tokio::test]
async fn a_floorless_attempt_reopens_after_a_compaction_seal_without_stripping_its_reply() {
    use broca_core::compaction_v2::received::ReceivedView;
    use cortexkit_role_compaction::view::{Edit, EditOp, View};
    let modern = route(OPUS, 4);
    LAYOUT.scope(Some(cortexkit_compaction_view::PLAN_VERSION), COMPACTION.scope(Some(Arc::new(CompactionScript { context_limit: 100_000, rides: vec![] })), Box::pin(async {
        let a = compacted_scenario(vec![attempted(text_step("a", "first"), vec![attempt(1, 200, "complete")])]);
        let a = ROUTE.scope(modern.clone(), Box::pin(new_source(&a, None, a.steps.clone()))).await;
        let through = a.records.iter().find_map(|record| match record { Record::EntryFacts(facts) if facts.ordinal == 2 => Some(facts.mid.clone()), _ => None }).unwrap();
        let view = ReceivedView::from(View::seal(1, vec![Edit::new(1, EditOp::Cut { through })]));
        let mut a = start(a);
        a.records.push(broca_session_loop::compaction::view_record(None, &view, None).unwrap());
        let b = compacted_scenario(vec![signed("floorless", false)]);
        let b = ROUTE.scope(modern.clone(), FRESH_RUN.scope("r2", Box::pin(new_source(&b, Some(a), b.steps.clone())))).await;
        assert_eq!(b.outcome.requests[0].render_episodes.signed_floor, None);
        let snapshot = b.snapshots.last().unwrap().snapshot();
        let store = Session::load(&snapshot, SessionAddr("0123456789abcdef".into()), &SessionTriple { project_root:"/project".into(), harness:"differential".into(), session:"s1".into() }, &DeploymentKey::new(1, &[7;32])).await.unwrap();
        assert_eq!(store.conversation.window_start(), 3);
        let log = store.open_log_records().await.unwrap();
        assert!(!log.iter().any(|(_, record)| matches!(record, Record::StepStart(step) if step.run_id == "r2")));
        assert!(log.iter().any(|(_, record)| matches!(record, Record::Attempt(attempt) if attempt.run_id == "r2" && attempt.signed_floor.is_none())));
        let c = compacted_scenario(vec![signed("after-seal", false)]);
        let mut start = start(b); start.snapshot = Some(snapshot);
        let c = ROUTE.scope(modern.clone(), FRESH_RUN.scope("r3", Box::pin(new_source(&c, Some(start), c.steps.clone())))).await;
        assert_eq!(c.outcome.requests[0].render_episodes.signed_floor, None);
        assert!(body(&modern.1, &c.outcome.requests[0]).contains("signature-floorless"));
    }))).await;
}

#[tokio::test]
async fn a_paused_run_reopens_with_its_own_attempt_cut_not_a_new_floor() {
    let modern = route(OPUS, 4);
    let first = send("r1", route(FABLE, 2), None, vec![signed("old", false)]).await;
    let old = send(
        "r2",
        modern.clone(),
        Some(start(first)),
        vec![signed("own", true), signed("done", false)],
    )
    .await;
    let sent = body(&modern.1, &old.outcome.requests[1]);
    assert!(sent.contains("signature-own") && !sent.contains("signature-old"));
    let through = old.records.iter().position(|record| matches!(record, Record::StepStart(step) if step.run_id == "r2" && step.step_id == 2)).unwrap();
    let records = old.records[..=through].to_vec();
    let mut stored = historical::session_at(&records).await;
    stored.seal(None, 0).await.unwrap();
    let script = reader_scenario(vec![signed("done", false)]);
    let resume = Start {
        records: records.clone(),
        parts: old.parts.clone(),
        snapshot: Some(stored.backend().snapshot()),
        converted: false,
    };
    let resumed = ROUTE
        .scope(
            modern.clone(),
            Box::pin(new_source(&script, Some(resume), script.steps.clone())),
        )
        .await;
    assert_eq!(body(&modern.1, &resumed.outcome.requests[0]), sent);
    let unavailable = CUT_EVIDENCE_MISSING
        .scope(
            (),
            ROUTE.scope(
                modern,
                Box::pin(new_source(
                    &script,
                    Some(Start {
                        records,
                        parts: old.parts,
                        snapshot: Some(stored.backend().snapshot()),
                        converted: false,
                    }),
                    script.steps.clone(),
                )),
            ),
        )
        .await;
    assert!(unavailable.outcome.requests.is_empty());
    assert!(unavailable.records.iter().any(|record| matches!(record, Record::RunEnd(end) if end.error.as_ref().is_some_and(|error| error.parse()["provider_code"] == "signed_history_unverifiable"))));
}

pub(super) fn effort(name: &str) -> (ModelHandle, FrozenRenderConfig) {
    let mut r = route(OPUS, 4);
    r.1.system_prompt = broca_provider::policy::SystemPromptPolicy::TopLevelField;
    r.1.effort_change_cache = true;
    r.1.variant = Some(FrozenVariant {
        name: name.into(),
        request: VariantRequest::Anthropic {
            thinking: Some(json!({"type":"adaptive"})),
            effort: Some(name.into()),
        },
    });
    r
}

#[tokio::test]
async fn missing_historical_effort_config_refuses_and_repair_renders_it() {
    let a = send("r1", effort("low"), None, vec![signed("low", false)]).await;
    let b = send(
        "r2",
        effort("high"),
        Some(start(a)),
        vec![signed("high", false)],
    )
    .await;
    let missing = b
        .records
        .iter()
        .find_map(|record| match record {
            Record::RunStart(run) if run.run_id == "r2" => run
                .setup
                .iter()
                .find(|part| part.kind == PartKind::RenderConfig)
                .map(|part| part.digest),
            _ => None,
        })
        .unwrap();
    let retained = start(b);
    let broken = Start {
        records: retained.records.clone(),
        parts: retained.parts.clone(),
        snapshot: None,
        converted: false,
    };
    let refused = MISSING_RENDER_CONFIG
        .scope(
            missing,
            send(
                "r3",
                effort("low"),
                Some(broken),
                vec![signed("must-not-sign", false)],
            ),
        )
        .await;
    assert!(refused.outcome.requests.is_empty());
    let c = send(
        "r4",
        effort("low"),
        Some(retained),
        vec![signed("healthy", false)],
    )
    .await;
    let sent = AnthropicMessages.shape(&effort("low").1, &c.outcome.requests[0]);
    assert!(sent["messages"].to_string().contains("output_config"));
    assert!(!sent.to_string().contains("signature-must-not-sign"));
}

pub(crate) struct FaultableParts<'a>(pub(crate) &'a mut MemoryParts);
impl Parts for FaultableParts<'_> {
    fn put(
        &mut self,
        kind: PartKind,
        value: &Value,
    ) -> Result<broca_session_store::record::PartRef, String> {
        self.0.put(kind, value)
    }
    fn get(&self, part: &broca_session_store::record::PartRef) -> Result<Value, String> {
        if MISSING_RENDER_CONFIG
            .try_with(|&missing| part.digest == missing)
            .unwrap_or(false)
            || DAMAGED_AFTER_SEAL
                .try_with(|(digest, damaged)| *digest == part.digest && damaged.get())
                .unwrap_or(false)
        {
            return Err("fixture: historical render config is missing".into());
        }
        self.0.get(part)
    }

    fn get_checked(
        &self,
        part: &broca_session_store::record::PartRef,
    ) -> Result<Value, broca_session_loop::parts::PartReadError> {
        self.get(part)
            .map_err(broca_session_loop::parts::PartReadError::Damaged)
    }
}

#[tokio::test]
async fn an_apply_beyond_a_real_sealed_revert_drops_older_reasoning_only_once() {
    let modern = route(OPUS, 4);
    let a = send("r1", modern.clone(), None, vec![signed("kept", false)]).await;
    let b = send(
        "r2",
        modern.clone(),
        Some(start(a)),
        vec![signed("dropped", false)],
    )
    .await;
    let input = b
        .records
        .iter()
        .find_map(|record| match record {
            Record::EntryFacts(entry) if entry.ordinal == 3 => Some(entry.mid.clone()),
            _ => None,
        })
        .unwrap();
    let mut stored = historical::session_at(&b.records).await;
    stored.seal(None, 0).await.unwrap();
    let snapshot = stored.backend().snapshot();
    let mut next = refresh(b, 1);
    let proposed = next.records.last().unwrap().clone();
    stored.append(&[proposed], true).await.unwrap();
    next.snapshot = Some(stored.backend().snapshot());
    let mut changed = reader_scenario(vec![signed("after-apply", false)]);
    changed.tools[0].description = "Read the updated source".into();
    let c = APPLY
        .scope(
            apply(1, None),
            ROUTE.scope(
                modern.clone(),
                FRESH_RUN.scope(
                    "r3",
                    Box::pin(new_source(&changed, Some(next), changed.steps.clone())),
                ),
            ),
        )
        .await;
    let backend = c.snapshots.last().unwrap().snapshot();
    let mut stored = Session::load(
        &backend,
        SessionAddr("0123456789abcdef".into()),
        &SessionTriple {
            project_root: "/project".into(),
            harness: "differential".into(),
            session: "s1".into(),
        },
        &DeploymentKey::new(1, &[7; 32]),
    )
    .await
    .unwrap();
    assert!(stored.head().ledger_dir.is_some());
    let mut redo = c
        .records
        .iter()
        .find_map(|record| match record {
            Record::RunStart(start) if start.run_id == "r2" => Some(start.clone()),
            _ => None,
        })
        .unwrap();
    redo.run_id = "redo".into();
    redo.send_id = Some("redo-send".into());
    let mut replacement = c
        .records
        .iter()
        .find_map(|record| match record {
            Record::Input(input) if input.run_id == "r2" => Some(input.clone()),
            _ => None,
        })
        .unwrap();
    replacement.run_id = "redo".into();
    let entry = c
        .records
        .iter()
        .find_map(|record| match record {
            Record::EntryFacts(entry) if entry.ordinal == 3 => Some(entry.clone()),
            _ => None,
        })
        .unwrap();
    stored
        .revert(
            &input,
            vec![
                Record::RunStart(redo),
                Record::Input(replacement),
                Record::EntryFacts(entry),
            ],
            None,
            100,
        )
        .await
        .unwrap();
    let history = stored
        .stored_records(&backend)
        .records_between(1, stored.next_seq() - 1)
        .await
        .unwrap();
    assert!(history.iter().any(
        |(_, record)| matches!(record, Record::TranscriptReverted(revert) if revert.ordinal == 3)
    ));
    let history: Vec<Record> = history.into_iter().map(|(_, record)| record).collect();
    let script = reader_scenario(vec![signed("first-kept-reply", false)]);
    let d = ROUTE
        .scope(
            modern.clone(),
            Box::pin(new_source(
                &script,
                Some(Start {
                    records: history,
                    parts: c.parts,
                    snapshot: Some(backend.snapshot()),
                    converted: false,
                }),
                script.steps.clone(),
            )),
        )
        .await;
    assert_eq!(d.outcome.requests[0].render_episodes.signed_floor, Some(3));
    assert!(!body(&modern.1, &d.outcome.requests[0]).contains("signature-kept"));
    let e = send(
        "r5",
        modern.clone(),
        Some(Start {
            parts: d.parts,
            records: d.records,
            snapshot: Some(d.snapshots.last().unwrap().snapshot()),
            converted: false,
        }),
        vec![signed("later", false)],
    )
    .await;
    assert!(body(&modern.1, &e.outcome.requests[0]).contains("signature-first-kept-reply"));
    drop(snapshot);
}
