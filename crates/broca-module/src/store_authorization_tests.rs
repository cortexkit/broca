use super::*;
use broca_records::record::PlanManifest;
use broca_session_store::codec::Id128;
use serde_json::json;

fn position() -> Position {
    Position {
        version: 1,
        log: Id128([1; 16]),
        len: 1,
    }
}

fn manifest(provider_count: usize, tools_per_provider: usize) -> PlanManifest {
    let providers = [
        "aft",
        "prefrontal",
        "broca",
        "cerebellum",
        "hippocampus",
        "motor",
        "ck-mc",
        "fs",
    ];
    let mut admitted_tools = Vec::new();
    let composition: Vec<_> = providers[..provider_count]
        .iter()
        .map(|provider| {
            let tools: Vec<_> = (0..tools_per_provider)
                .map(|n| {
                    let name = format!("{provider}.tool_{n:02}");
                    if n < tools_per_provider / 5 {
                        admitted_tools.push(json!({
                            "name": name,
                            "description": "fixture tool",
                            "input_schema": {"type": "object"}
                        }));
                    }
                    json!({
                        "name": name,
                        "capabilities": [format!("{provider}:project.read/v1"), "tool-result/v1"]
                    })
                })
                .collect();
            json!({ "provider": provider, "tools": tools })
        })
        .collect();
    let plan = json!({
        "composition": {"providers": composition, "step_transform_items": []},
        "system_text_items": [{"provider": "prefrontal", "preset": "worker"}],
        "step_transform_items": [{"subscriptions": [{"phase": "approve"}]}],
        "readers": ["prefrontal"], "senders": [{"module": "prefrontal", "interrupt": true}],
        "compaction_item": {"provider": "ck-mc", "preset": "worker", "params": {}}
    })
    .to_string();
    serde_json::from_value(json!({
        "plan": plan, "plan_identity": "plan", "composition_digest": "composition",
        "tools": admitted_tools, "system_text": "x".repeat(100_000), "system_digest": "system",
        "join_version": 1, "role_versions": {}, "declarations": [], "fetches": []
    }))
    .unwrap()
}

#[test]
fn closed_authorization_compacts_mason_and_head_fixtures() {
    struct PreviousFacts {
        _ownership: Option<SessionOwnershipV1>,
        _read_policy: Option<ReadPolicy>,
    }
    // Model two compositions: six providers with 80 tool references each for the mason,
    // and eight providers with 100 references each for the head.
    for (name, providers, tools) in [("mason", 6, 80), ("head", 8, 100)] {
        let manifest = Arc::new(manifest(providers, tools));
        let first =
            FirstEpisodeV1::new(broca_protocol::RunId("first".into()), &[], Some(&manifest));
        let tool_names = first.tool_names.len();
        assert_eq!(tool_names, providers * tools / 5);
        let plan = Arc::downgrade(first.plan.as_ref().unwrap());
        let policy = ReadPolicy::with_manifest(&manifest);
        let projection_bytes = json!({
            "readers": ["prefrontal"], "senders": [{"module": "prefrontal"}],
            "compaction_item": {"provider": "ck-mc"}
        })
        .to_string()
        .len();
        assert_eq!(
            policy.heap_bytes(),
            2 * projection_bytes + serde_heap_bytes(&SessionOwnershipV1::default())
        );
        let cache = AuthorizationCache::default();
        let key = BindIdentity::new("/repo", "broca", name);
        let class = StorageClass::Ordinary;
        let mut full = SessionOwnershipV1 {
            episodes: 1,
            first_episode: Some(first),
            ..Default::default()
        };
        let first_grants = OwnershipChecks::project(full.clone());
        assert_eq!(first_grants.interrupt_senders, ["prefrontal"]);
        assert!(crate::sender_admission::interrupt_granted(
            &full,
            "prefrontal"
        ));
        full.sender_admission = Some(broca_core::admission::SenderAdmissionV1 {
            version: 1,
            accepted_senders: Some(broca_core::admission::plan_senders(
                &broca_records::record::OpaqueJson::from_json_text(
                    r#"{"senders":[{"module":"refreshed","interrupt":true}]}"#,
                )
                .unwrap(),
            )),
        });
        let before = AuthorizationCache::key_bytes(&key)
            + std::mem::size_of::<PreviousFacts>()
            + 2 * std::mem::size_of::<usize>()
            + serde_heap_bytes(&full)
            + frozen_plan_bytes(&plan.upgrade().unwrap())
            + policy.heap_bytes();
        let projected = OwnershipChecks::project(full.clone());
        assert_eq!(projected.first_senders, ["prefrontal"]);
        assert_eq!(projected.interrupt_senders, ["refreshed"]);
        assert!(
            projected.has_first_episode && projected.requires_hooks && projected.requires_approval
        );
        assert_eq!(projected.first_plan_identity.as_deref(), Some("plan"));
        assert_eq!(projected.owner, full.for_owner_access());
        let weak = Arc::downgrade(&manifest);
        drop((full, manifest));
        cache.keep(
            &key,
            class,
            position(),
            Arc::new(AuthorizationFacts {
                ownership: Some(projected),
                read_policy: Some(policy),
            }),
        );
        assert!(
            weak.upgrade().is_none(),
            "the cache must not retain the manifest"
        );
        let kept = cache.get(&key, class, position()).unwrap();
        let StoreReadReply::AuthorizationChecks(Ok(checks)) =
            kept.reply(&StoreReadRequest::AuthorizationChecks).unwrap()
        else {
            panic!("ownership is retained");
        };
        assert!(checks.owner.first_episode.is_none());
        assert!(
            plan.upgrade().is_none(),
            "the cache must not retain the opaque plan"
        );
        let memory = cache.memory();
        assert_eq!(memory.entries, 1);
        assert!(memory.estimated_bytes < (before / 8) as u64);
        assert!(memory.estimated_bytes * 1_024 <= memory.max_bytes.unwrap());
        eprintln!(
            "{name} fixture: {tool_names} tool names; {before} -> {} counted auth entry bytes",
            memory.estimated_bytes
        );
        for n in 1..1_024 {
            let key = BindIdentity::new("/repo", "broca", format!("{name}-{n}"));
            cache.keep(&key, class, position(), Arc::clone(&kept));
        }
        assert_eq!(cache.memory().entries, 1_024);
    }
}

#[test]
fn closed_authorization_evicts_by_bytes_and_skips_oversized_ownership() {
    let facts_with_senders = |bytes: usize| {
        Arc::new(AuthorizationFacts {
            ownership: Some(OwnershipChecks {
                first_senders: vec!["x".repeat(bytes)],
                ..Default::default()
            }),
            read_policy: Some(ReadPolicy::with_manifest(&manifest(1, 1))),
        })
    };
    let cache = AuthorizationCache::default();
    let class = StorageClass::Ordinary;
    let keys = ["old", "recent", "large"].map(|name| BindIdentity::new("/repo", "broca", name));
    let half = facts_with_senders(MAX_BYTES / 2);
    for key in &keys[..2] {
        cache.keep(key, class, position(), Arc::clone(&half));
    }
    assert!(cache.get(&keys[0], class, position()).is_none());
    assert!(cache.get(&keys[1], class, position()).is_some());
    cache.keep(&keys[2], class, position(), facts_with_senders(MAX_BYTES));
    let kept = cache.get(&keys[2], class, position()).unwrap();
    assert!(
        kept.ownership.is_none(),
        "the ownership summary alone is over the bound"
    );
    assert!(
        kept.read_policy.is_some(),
        "the policy fits without the ownership summary"
    );
    let memory = cache.memory();
    assert_eq!(memory.entries, 2);
    assert!(memory.estimated_bytes <= memory.max_bytes.unwrap());
}
