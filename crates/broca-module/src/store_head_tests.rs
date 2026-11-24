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

/// Heads whose read policies share one plan manifest count it once: the memory report shows
/// what the kept heads hold, not the manifest again for every head that points at it.
#[test]
fn kept_heads_count_a_shared_plan_manifest_once() {
    use broca_session_store::codec::Id128;
    use broca_session_store::session::Position;
    use std::sync::Arc;
    let manifest = || -> Arc<broca_records::record::PlanManifest> {
        Arc::new(
            serde_json::from_value(json!({
                "plan": "{}", "plan_identity": "plan", "composition_digest": "composition",
                "tools": [], "system_text": "x".repeat(100_000), "system_digest": "system",
                "join_version": 1, "role_versions": {}, "declarations": [], "fetches": []
            }))
            .unwrap(),
        )
    };
    let head = |manifest: Arc<broca_records::record::PlanManifest>| StoreHead {
        reply: Default::default(),
        input: InputSource::Unknown,
        ownership: serde_json::from_value(json!({})).unwrap(),
        senders: Vec::new(),
        read_policy: Some(crate::reader_admission::ReadPolicy::with_manifest(manifest)),
    };
    let at = Position {
        version: 1,
        log: Id128([1; 16]),
        len: 1,
    };
    let held = |manifests: [Arc<broca_records::record::PlanManifest>; 2]| {
        let heads = KeptHeads::default();
        for (n, manifest) in manifests.into_iter().enumerate() {
            heads.keep(format!("s{n}"), at, &head(manifest), "read");
        }
        heads.memory().estimated_bytes
    };
    let shared = manifest();
    let once = held([Arc::clone(&shared), shared]);
    let twice = held([manifest(), manifest()]);
    assert!(once > 100_000, "the manifest is counted: {once}");
    assert!(
        twice - once >= 100_000,
        "a shared manifest is counted once: {once} shared, {twice} separate"
    );
}
