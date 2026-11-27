use super::*;
use broca_core::compaction_v2::protocol::read::ReadPage;
use broca_core::compaction_v2::protocol::view::{Edit, EditOp, View};
use broca_session_store::session::COMPACTION_PAGE_ENTRIES_IN_MEMORY;
use serde_json::value::{to_raw_value, RawValue};

async fn actor_reply(side: &Side, method: &str, params: &Value) -> Box<RawValue> {
    assert!(side.registry.open_handle(&side.key).is_some());
    side.registry
        .route_compaction_v2(
            &side.key,
            Some("ck-mc".into()),
            method.into(),
            to_raw_value(params).unwrap(),
        )
        .await
        .unwrap()
}

async fn stored_reply(side: &Side, method: &str, params: &Value) -> Box<RawValue> {
    let runtime = side.registry.store_runtime().unwrap();
    let class = runtime.class_of(&side.key).unwrap().unwrap();
    let view = runtime.current_view(&side.key).await.unwrap().unwrap();
    crate::store_entries::serve_compaction(
        runtime.parts_for(class, view.head().cipher()),
        Some(&view),
        &side.key,
        Some("ck-mc"),
        method,
        &to_raw_value(params).unwrap(),
        &crate::store_entries::EntryAttribution::Known(false),
        None,
    )
    .await
    .unwrap()
}

async fn view_reads(side: &Side) -> (u64, u64) {
    let counts = side
        .registry
        .memory_report()
        .await
        .closed_session_views
        .loads_by_reason
        .unwrap_or_default();
    counts
        .get("compaction")
        .map_or((0, 0), |c| (c.loads, c.hits))
}

async fn run(side: &Side, id: &str) {
    assert!(matches!(
        side.send_request(
            id,
            untooled("another message"),
            Some(planned("plan-1", None))
        )
        .await,
        SendOutcome::Active { .. } | SendOutcome::Pending { .. }
    ));
    side.settled(id).await;
    side.assert_completed(id).await;
}

async fn accept(side: &Side, view: &View) {
    assert_eq!(
        side.compaction(Some("ck-mc"), "session.apply_view", json!({ "view": view }))
            .await,
        Ok(json!({"answer": "accepted"}))
    );
}

async fn mid(side: &Side, ordinal: u64) -> String {
    let reply = actor_reply(
        side,
        "session.entries",
        &json!({"select":{"by":"after","after_ordinal":ordinal-1,"limit":1},"detail":"full"}),
    )
    .await;
    let page: Value = serde_json::from_str(reply.get()).unwrap();
    assert_eq!(page["entries"][0]["ordinal"], ordinal);
    page["entries"][0]["mid"].as_str().unwrap().to_owned()
}

async fn sealed_session() -> Side {
    sealed_session_with(64, 72).await
}

async fn sealed_session_with(fill_runs: u64, seal_through: u64) -> Side {
    let steps = (0..=fill_runs)
        .map(|n| text_step(&format!("seal-{n}"), "another answer"))
        .collect();
    let side = contract_sessions_with(false, steps, Duration::from_secs(60)).await;
    for n in 0..fill_runs {
        run(&side, &format!("fill-{n}")).await;
    }
    let through = mid(&side, seal_through).await;
    accept(
        &side,
        &View::seal(1, vec![Edit::new(1, EditOp::Cut { through })]),
    )
    .await;
    run(&side, "after-cut").await;
    side
}

#[tokio::test]
async fn compaction_page_at_the_advertised_limit_does_not_load_a_whole_view() {
    let side = sealed_session().await;
    let limit = (COMPACTION_PAGE_ENTRIES_IN_MEMORY - 1) as u64;
    let before = view_reads(&side).await;
    let reply = actor_reply(
        &side,
        "session.entries",
        &json!({"select":{"by":"after","after_ordinal":2,"limit":limit},"detail":"compact"}),
    )
    .await;
    let page: ReadPage = serde_json::from_str(reply.get()).unwrap();
    assert_eq!(page.entries.len() as u64, limit);
    assert_eq!(page.next_after, Some(limit + 2));
    assert_eq!(
        view_reads(&side).await,
        before,
        "the actor used a whole view"
    );
}

#[tokio::test]
async fn compaction_pages_walk_sealed_and_open_entries_without_loading_a_whole_view() {
    let fill_runs = 200;
    let seal_through = 270;
    let side = sealed_session_with(fill_runs, seal_through).await;
    // Six entries from the fixture's first two runs, then two per fill run and two after the cut.
    let total = 6 + 2 * fill_runs + 2;
    let limit = (COMPACTION_PAGE_ENTRIES_IN_MEMORY - 1) as u64;
    let window_start = side
        .registry
        .store_runtime()
        .unwrap()
        .current_view(&side.key)
        .await
        .unwrap()
        .unwrap()
        .conversation
        .window_start();
    assert_eq!(window_start, seal_through + 1);

    let before = view_reads(&side).await;
    let mut after = 0;
    loop {
        let reply = actor_reply(
            &side,
            "session.entries",
            &json!({
                "select": {"by": "after", "after_ordinal": after, "limit": limit},
                "detail": "compact",
                "max_bytes": 1024 * 1024,
            }),
        )
        .await;
        let page: ReadPage = serde_json::from_str(reply.get()).unwrap();
        let last = (after + limit).min(total);
        assert_eq!(page.entries.len() as u64, last - after, "after {after}");
        for (ordinal, entry) in (after + 1..=last).zip(&page.entries) {
            assert_eq!(entry.ordinal, ordinal);
        }
        assert_eq!(page.next_after, (last < total).then_some(last));
        match page.next_after {
            Some(next_after) => after = next_after,
            None => break,
        }
    }
    assert_eq!(
        view_reads(&side).await,
        before,
        "the actor used a whole view"
    );
}

#[tokio::test]
async fn sealed_compaction_entries_do_not_load_a_whole_view() {
    let side = sealed_session().await;
    let method = "session.entries";
    let count = json!({"select":{"by":"after","after_ordinal":2,"limit":37},"detail":"compact"});
    let before = view_reads(&side).await;
    let counted = actor_reply(&side, method, &count).await;
    let page: Value = serde_json::from_str(counted.get()).unwrap();
    assert_eq!(page["entries"].as_array().unwrap().len(), 37);
    assert_eq!(page["next_after"], 39);
    let cap = serde_json::to_vec(&page["entries"]).unwrap().len();
    let bytes =
        json!({"select":{"by":"after","after_ordinal":2},"detail":"compact","max_bytes":cap});
    let capped = actor_reply(&side, method, &bytes).await;
    assert_eq!(capped.get(), counted.get());
    assert_eq!(
        view_reads(&side).await,
        before,
        "the actor used a whole view"
    );
}

#[tokio::test]
async fn sealed_compaction_entries_match_stored_reply_bytes() {
    let side = sealed_session().await;
    let method = "session.entries";
    let runtime = side.registry.store_runtime().unwrap();
    let view = runtime.current_view(&side.key).await.unwrap().unwrap();
    assert_eq!(view.conversation.window_start(), 73);
    for params in [
        // First read crosses a chunk boundary at the look-ahead, and the second crosses the seal.
        json!({"select":{"by":"after","after_ordinal":1,"limit":32},"detail":"full"}),
        json!({"select":{"by":"after","after_ordinal":2,"limit":115},"detail":"compact"}),
        json!({"select":{"by":"after","after_ordinal":2},"detail":"compact","max_bytes":12000}),
        json!({"select":{"by":"after","after_ordinal":2},"detail":"full","max_bytes":1}),
        // Both count and byte requests that need more work than the actor allows stay full-size.
        json!({"select":{"by":"after","limit":128},"detail":"full"}),
        json!({"select":{"by":"after"},"detail":"full","max_bytes":1000000}),
    ] {
        let actor = actor_reply(&side, method, &params).await;
        let stored = stored_reply(&side, method, &params).await;
        assert_eq!(actor.get(), stored.get(), "{params}");
    }
}

async fn applied_session() -> Side {
    let side = contract_sessions_with(
        false,
        vec![text_step("applied", "after the view")],
        Duration::from_secs(60),
    )
    .await;
    let dropped = mid(&side, 2).await;
    let built_through = mid(&side, 6).await;
    let view = |version| {
        View::seal_with_policy(
            version,
            vec![Edit::new(
                1,
                EditOp::Drop {
                    mid: dropped.clone(),
                    part: None,
                },
            )],
            Some(built_through.clone()),
            Some("strip".into()),
        )
    };
    accept(&side, &view(1)).await;
    run(&side, "after-view").await;
    // Accepting a newer view must not change the applied-view read before the next step.
    accept(&side, &view(2)).await;
    side
}

#[tokio::test]
async fn applied_compaction_view_does_not_load_a_whole_view() {
    let side = applied_session().await;
    let before = view_reads(&side).await;
    let reply = actor_reply(&side, "session.applied_view", &json!({})).await;
    let answer: Value = serde_json::from_str(reply.get()).unwrap();
    assert_eq!(answer["view"]["version"], 1);
    assert_eq!(answer["applied_by"], "ck-mc");
    assert_eq!(
        view_reads(&side).await,
        before,
        "the actor used a whole view"
    );
}

#[tokio::test]
async fn applied_compaction_view_matches_replayed_reply_bytes() {
    let side = applied_session().await;
    let method = "session.applied_view";
    let params = json!({});
    let actor = actor_reply(&side, method, &params).await;
    let stored = stored_reply(&side, method, &params).await;
    assert_eq!(actor.get(), stored.get());
}
