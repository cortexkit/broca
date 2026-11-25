use super::*;
use serde_json::json;

fn facts(selection: Option<PlanModel>, run_model: Option<HeadModel>) -> HeadFacts {
    HeadFacts {
        lineage_state: SessionReadLineageState {
            last_run_id: None,
            state: SessionReadStateKind::Idle,
            reason: None,
            error: None,
            usage: None,
        },
        prompt_len: 0,
        interrupt_reason: None,
        selection,
        run_model,
        rich_ui: None,
        usage: None,
        title: None,
        mode3: false,
        updated_at: None,
        resume_not_before_ms: None,
    }
}

fn model(provider: &str, model: &str, variant: Option<&str>, default: Option<&str>) -> HeadModel {
    let mut head = HeadModel::new(provider, model);
    head.variant = variant.map(str::to_owned);
    head.default_variant = default.map(str::to_owned);
    head
}

/// A run's default effort is the variant recorded in its frozen render config, or else an effort
/// the model froze (a ChatGPT pro model's). A run that froze neither sends no effort, so it has
/// none.
#[test]
fn the_default_effort_is_the_frozen_variant_or_effort() {
    let variant = json!({"variant": {"name": "medium", "request": {"kind": "open_ai_responses", "effort": "medium"}}});
    assert_eq!(default_effort(&variant).as_deref(), Some("medium"));
    assert_eq!(
        default_effort(&json!({"reasoning_effort": "max"})).as_deref(),
        Some("max")
    );
    assert_eq!(
        default_effort(&json!({"family_id": "anthropic_messages"})),
        None
    );
}

/// The head shows a default effort only beside a model with no chosen variant, and only for
/// the model the latest run used: another selected model's default isn't known yet.
#[test]
fn a_selected_model_shows_the_latest_runs_default_only_when_it_is_that_model() {
    let run = || Some(model("openai", "gpt-6.2", None, Some("medium")));
    let head = |selection| head_reply(facts(selection, run())).model.unwrap();

    let same = head(Some(PlanModel::new("openai", "gpt-6.2")));
    assert_eq!(same.default_variant.as_deref(), Some("medium"));

    let mut chosen = PlanModel::new("openai", "gpt-6.2");
    chosen.variant = Some("high".into());
    let chosen = head(Some(chosen));
    assert_eq!(
        (chosen.variant.as_deref(), chosen.default_variant),
        (Some("high"), None)
    );

    let other = head(Some(PlanModel::new("openai", "gpt-6.2-astra")));
    assert_eq!(other.default_variant, None);

    let unselected = head(None);
    assert_eq!(unselected.default_variant.as_deref(), Some("medium"));
}

/// Kept heads forget the least recently used first: a head read or kept again since stays while
/// heads kept after it fill the table.
#[test]
fn kept_heads_forget_the_least_recently_used_first() {
    use broca_session_store::codec::Id128;
    use broca_session_store::session::Position;
    let heads = KeptHeads::default();
    let head = StoreHead {
        reply: Default::default(),
        input: InputSource::Unknown,
        ownership: serde_json::from_value(json!({})).unwrap(),
        senders: Vec::new(),
        read_policy: None,
    };
    let at = |n: u64| Position {
        version: n,
        log: Id128([1; 16]),
        len: n,
    };
    heads.keep("read".into(), at(1), &head, "read");
    heads.keep("kept again".into(), at(1), &head, "read");
    for n in 0..HEADS_KEPT - 2 {
        heads.keep(format!("s{n}"), at(1), &head, "read");
    }
    assert!(heads.get("read", at(1)).is_some());
    heads.keep("kept again".into(), at(2), &head, "read");
    heads.keep("new 1".into(), at(1), &head, "read");
    heads.keep("new 2".into(), at(1), &head, "read");
    assert!(
        heads.get("read", at(1)).is_some(),
        "a head read since stays"
    );
    assert!(
        heads.get("kept again", at(2)).is_some(),
        "a head kept again since stays"
    );
    assert!(heads.get("s0", at(1)).is_none());
    assert!(heads.get("s1", at(1)).is_none());
    assert!(heads.get("s2", at(1)).is_some());
    // The memory report counts them and what they hold.
    let memory = heads.memory();
    assert_eq!(
        (memory.entries, memory.max_entries),
        (HEADS_KEPT as u64, HEADS_KEPT as u64)
    );
    assert!(memory.estimated_bytes > HEADS_KEPT as u64 * std::mem::size_of::<StoreHead>() as u64);
    // Beside the counts of how heads were kept and served.
    let counts = heads.counts();
    assert_eq!(counts.held, memory);
    assert_eq!(counts.kept.get("read"), Some(&(HEADS_KEPT as u64 + 3)));
}

#[test]
fn kept_heads_hold_only_read_admission_plan_members() {
    use broca_session_store::codec::Id128;
    use broca_session_store::session::Position;
    use std::sync::Arc;
    let providers = [
        "aft",
        "prefrontal",
        "broca",
        "cerebellum",
        "hippocampus",
        "motor",
    ]
    .map(|provider| {
        let tools: Vec<_> = (0..80)
            .map(|n| {
                json!({"name": format!("{provider}.tool_{n:02}"), "capabilities": [
                    format!("{provider}:project.read/v1"), "tool-result/v1"
                ]})
            })
            .collect();
        json!({"provider": provider, "tools": tools})
    });
    let plan = json!({
        "composition": {"providers": providers, "step_transform_items": []},
        "system_text_items": [{"provider": "prefrontal", "preset": "worker"}],
        "readers": ["prefrontal"], "senders": [{"module": "prefrontal"}],
        "compaction_item": {"provider": "ck-mc", "preset": "worker", "params": {}}
    })
    .to_string();
    let full_plan_bytes = plan.len() as u64;
    assert!(full_plan_bytes > 40_000, "tool composition must be large");
    let manifest: Arc<broca_records::record::PlanManifest> = Arc::new(
        serde_json::from_value(json!({
            "plan": plan, "plan_identity": "plan", "composition_digest": "composition",
            "tools": [], "system_text": "x".repeat(100_000), "system_digest": "system",
            "join_version": 1, "role_versions": {}, "declarations": [], "fetches": []
        }))
        .unwrap(),
    );
    let released = Arc::downgrade(&manifest);
    let mut head = StoreHead {
        reply: Default::default(),
        input: InputSource::Unknown,
        ownership: Default::default(),
        senders: Vec::new(),
        read_policy: Some(crate::reader_admission::ReadPolicy::with_manifest(
            &manifest,
        )),
    };
    drop(manifest);
    let heads = KeptHeads::default();
    let at = Position {
        version: 1,
        log: Id128([1; 16]),
        len: 1,
    };
    heads.keep("s0".into(), at, &head, "read");
    head.read_policy = None;
    assert!(
        released.upgrade().is_none(),
        "a kept head holds its plan's decoded manifest"
    );
    let held = heads.memory().estimated_bytes;
    heads.keep("s0".into(), at, &head, "read");
    let without_policy = heads.memory().estimated_bytes;
    let projected_plan_bytes = json!({
        "readers": ["prefrontal"], "senders": [{"module": "prefrontal"}],
        "compaction_item": {"provider": "ck-mc"}
    })
    .to_string()
    .len() as u64;
    eprintln!(
        "worker plan: {full_plan_bytes} -> {projected_plan_bytes} bytes per plan; \
         full-plan head estimate: {} bytes, projected estimate: {} bytes, held: {held} bytes",
        without_policy + 2 * full_plan_bytes,
        without_policy + 2 * projected_plan_bytes
    );
    assert_eq!(
        held - without_policy,
        2 * projected_plan_bytes,
        "a kept head should count only the effective and accepted projections"
    );
}
