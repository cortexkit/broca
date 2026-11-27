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
        "readers": ["prefrontal"], "senders": [{"module": "prefrontal"}],
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
fn closed_authorization_estimates_mason_and_head_fixtures_without_decoded_setup() {
    // Model two compositions: six providers with 80 tool references each for the mason,
    // and eight providers with 100 references each for the head.
    for (name, providers, tools) in [("mason", 6, 80), ("head", 8, 100)] {
        let manifest = Arc::new(manifest(providers, tools));
        let opaque_bytes = manifest.plan.as_json_text().len();
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
        let weak = Arc::downgrade(&manifest);
        drop(manifest);
        let cache = AuthorizationCache::default();
        let key = BindIdentity::new("/repo", "broca", name);
        let class = StorageClass::Ordinary;
        cache.keep(
            &key,
            class,
            position(),
            Arc::new(AuthorizationFacts {
                ownership: Some(SessionOwnershipV1 {
                    first_episode: Some(first),
                    ..Default::default()
                }),
                read_policy: Some(policy),
            }),
        );
        assert!(
            weak.upgrade().is_none(),
            "the cache must not retain the manifest"
        );
        let kept = cache.get(&key, class, position()).unwrap();
        let StoreReadReply::AuthorizationOwnership(Ok(ownership)) = kept
            .reply(&StoreReadRequest::AuthorizationOwnership)
            .unwrap()
        else {
            panic!("ownership is retained");
        };
        assert!(Arc::ptr_eq(
            ownership.first_episode.unwrap().plan.as_ref().unwrap(),
            &plan.upgrade().unwrap()
        ));
        let memory = cache.memory();
        assert_eq!(memory.entries, 1);
        assert!(memory.estimated_bytes > opaque_bytes as u64);
        assert!(memory.estimated_bytes < opaque_bytes as u64 + (tool_names as u64 * 64) + 4_096);
        eprintln!(
            "{name} fixture: {opaque_bytes} opaque plan bytes, {tool_names} tool names, {} estimated auth entry bytes",
            memory.estimated_bytes
        );
    }
}

#[test]
fn closed_authorization_evicts_by_bytes_and_skips_oversized_ownership() {
    let facts_with_plan = |bytes: usize| {
        let mut manifest = manifest(1, 1);
        manifest.plan = broca_records::record::OpaqueJson::from_json_text(format!(
            "{{\"padding\":\"{}\"}}",
            "x".repeat(bytes)
        ))
        .unwrap();
        Arc::new(AuthorizationFacts {
            ownership: Some(SessionOwnershipV1 {
                first_episode: Some(FirstEpisodeV1::new(
                    broca_protocol::RunId("first".into()),
                    &[],
                    Some(&manifest),
                )),
                ..Default::default()
            }),
            read_policy: Some(ReadPolicy::with_manifest(&manifest)),
        })
    };
    let cache = AuthorizationCache::default();
    let class = StorageClass::Ordinary;
    let keys = ["old", "recent", "large"].map(|name| BindIdentity::new("/repo", "broca", name));
    let half = facts_with_plan(MAX_BYTES / 2);
    for key in &keys[..2] {
        cache.keep(key, class, position(), Arc::clone(&half));
    }
    assert!(cache.get(&keys[0], class, position()).is_none());
    assert!(cache.get(&keys[1], class, position()).is_some());
    cache.keep(&keys[2], class, position(), facts_with_plan(MAX_BYTES));
    let kept = cache.get(&keys[2], class, position()).unwrap();
    assert!(
        kept.ownership.is_none(),
        "the opaque plan alone is over the bound"
    );
    assert!(
        kept.read_policy.is_some(),
        "the policy fits without the opaque plan"
    );
    let memory = cache.memory();
    assert_eq!(memory.entries, 2);
    assert!(memory.estimated_bytes <= memory.max_bytes.unwrap());
}
