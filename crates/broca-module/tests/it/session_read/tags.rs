use super::*;
use cortexkit_role_llm_runner::read::MediaMode;
use serde_json::{json, Value};

fn tagged_episode(key: &BindIdentity) -> Vec<WalRecord> {
    let mut records = dispatch_module_episode(key, "tagged", "call-1");
    if let WalRecord::RunStarted { config, .. } = &mut records[0] {
        config.render_tags = true;
        config.computed_tags = Some(broca_wal::ComputedTagRules { checkpoint: 0 });
    }
    records
}

async fn page_bytes(rig: &Rig, key: &BindIdentity, media: MediaMode) -> Value {
    let mut request = read_request(key, Some(0), Some(10));
    request.media = media;
    let response = rig.registry.read(request).await.expect("session.read");
    for entry in &response.messages {
        let tags = if entry.tags.is_empty() {
            "<absent>".into()
        } else {
            serde_json::to_string(&entry.tags).unwrap()
        };
        let digests = entry
            .part_digests
            .as_ref()
            .map(|parts| serde_json::to_string(parts).unwrap())
            .unwrap_or("<absent>".into());
        eprintln!(
            "{} {media:?}: tags={tags}; part_digests={digests}",
            entry.mid
        );
        assert_eq!(
            entry.part_digests.is_some(),
            media == MediaMode::Descriptors
        );
    }
    serde_json::to_value(response).unwrap()
}

fn assert_rows(entry: &Value, expected: Value) {
    assert_eq!(
        entry["tags"], expected,
        "every exposed part needs a tag row"
    );
    if entry.get("part_digests").is_none() {
        return;
    }
    let keys: Vec<_> = entry["tags"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| &row["part"])
        .collect();
    let digests: Vec<_> = entry["part_digests"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| &row["part"])
        .collect();
    assert_eq!(
        keys, digests,
        "tag and digest keys must agree in part order"
    );
}

#[tokio::test]
async fn open_batch_has_rows_before_results_commit() {
    let rig = Rig::new("open-batch-tags", Arc::new(NoopExec));
    // A tag-rendering run numbers the open turn when a read exposes it; a session with no
    // numbering ledger at all (written before tags existed) still needs every part's row.
    let tagged = rig.key("open");
    let ledgerless = rig.key("open-ledgerless");
    append_seed(&rig.wal_store, &tagged, &tagged_episode(&tagged)[..4]);
    append_seed(
        &rig.wal_store,
        &ledgerless,
        &dispatch_module_episode(&ledgerless, "ledgerless", "call-1")[..4],
    );
    let mut pages = Vec::new();
    for key in [&tagged, &ledgerless] {
        for media in [MediaMode::Inline, MediaMode::Descriptors] {
            let page = page_bytes(&rig, key, media).await;
            pages.push(page);
        }
    }
    for page in pages {
        assert_eq!(page["messages"].as_array().unwrap().len(), 2);
        assert_rows(&page["messages"][1], json!([{"part": "m1#0"}]));
    }
}

#[tokio::test]
async fn open_and_committed_rows_keep_the_same_part_keys() {
    let rig = Rig::new("stable-batch-tags", Arc::new(NoopExec));
    let key = rig.key("stable");
    let records = tagged_episode(&key);
    append_seed(&rig.wal_store, &key, &records[..4]);
    let open = page_bytes(&rig, &key, MediaMode::Descriptors).await;
    append_seed(&rig.wal_store, &key, &records[4..]);
    let mut committed_pages = Vec::new();
    for media in [MediaMode::Inline, MediaMode::Descriptors] {
        let committed = page_bytes(&rig, &key, media).await;
        committed_pages.push((media, committed));
    }
    for (media, committed) in committed_pages {
        assert_eq!(
            open["messages"][1]["tags"], committed["messages"][1]["tags"],
            "part keys must survive batch commitment"
        );
        if media == MediaMode::Descriptors {
            assert_eq!(
                open["messages"][1]["part_digests"],
                committed["messages"][1]["part_digests"]
            );
        }
        assert_rows(&committed["messages"][1], json!([{"part": "m1#0"}]));
    }
    materialize_all(&rig, &key);
    let stored = page_bytes(&rig, &key, MediaMode::Inline).await;
    assert_eq!(open["messages"][1]["tags"], stored["messages"][1]["tags"]);
}

/// A read that lands after a tool turn is written but before its batch commits exposes the
/// turn's text. The text counts, so the read must already show the number commit gives it:
/// llm-runner §9.6 makes any change to an exposed part's number a new lineage, and an
/// unnumbered row that later gains a number is such a change.
#[tokio::test]
async fn open_text_shows_its_final_number_before_batch_commit() {
    let rig = Rig::new("open-text-tags", Arc::new(NoopExec));
    let key = rig.key("open-text");
    let mut records = tagged_episode(&key);
    if let WalRecord::ModelStepFinished {
        assistant_message, ..
    } = &mut records[1]
    {
        Arc::make_mut(assistant_message).content.insert(
            0,
            ContentBlock::Text {
                text: "calling lookup".into(),
            },
        );
    }
    let expected = json!([{"part": "m1#0", "tag": "§2§"}, {"part": "m1#1"}]);
    // Stop the WAL between the step's dispatch intent and its results: the batch is open.
    append_seed(&rig.wal_store, &key, &records[..4]);
    let mut open_pages = Vec::new();
    for media in [MediaMode::Inline, MediaMode::Descriptors] {
        let open = page_bytes(&rig, &key, media).await;
        assert_eq!(
            open["messages"][1]["tags"], expected,
            "an exposed counted part shows its final number before batch commitment"
        );
        assert_rows(&open["messages"][1], expected.clone());
        open_pages.push(open);
    }
    append_seed(&rig.wal_store, &key, &records[4..]);
    for (media, open) in [MediaMode::Inline, MediaMode::Descriptors]
        .into_iter()
        .zip(open_pages)
    {
        let committed = page_bytes(&rig, &key, media).await;
        assert_eq!(open["lineage_id"], committed["lineage_id"]);
        assert_eq!(
            open["messages"][1]["tags"], committed["messages"][1]["tags"],
            "batch commitment changes no number a read exposed"
        );
        assert_eq!(
            open["messages"][1]["part_digests"],
            committed["messages"][1]["part_digests"]
        );
        assert_rows(&committed["messages"][1], expected.clone());
    }
}

#[tokio::test]
async fn live_tail_and_steer_rows_cover_parts_before_materialization() {
    let rig = Rig::new("tail-steer-tags", Arc::new(NoopExec));
    let key = rig.key("tail");
    let mut records = tagged_episode(&key);
    records.truncate(1);
    append_seed(&rig.wal_store, &key, &records);
    materialize_all(&rig, &key);
    let mut step = assistant_text(0, "first answer");
    if let WalRecord::ModelStepFinished {
        assistant_message, ..
    } = &mut step
    {
        Arc::make_mut(assistant_message)
            .content
            .push(ContentBlock::Text {
                text: "second part".into(),
            });
    }
    append_seed(
        &rig.wal_store,
        &key,
        &[
            WalRecord::StepStarted { step_id: StepId(0) },
            WalRecord::Steered {
                tags: None,
                retention: None,
                ts_ms: None,
                written_at_ms: None,
                title: None,
                send_id: Some("steer-1".into()),
                request_identity_digest: None,
                delivery: Some(broca_protocol::Delivery::Steer),
                input: vec![Message::new(
                    Role::User,
                    vec![ContentBlock::Text {
                        text: "steer now".into(),
                    }],
                )]
                .into(),
                hooks: None,
            },
            step,
            WalRecord::StepStarted { step_id: StepId(1) },
        ],
    );
    let mut pages = Vec::new();
    for media in [MediaMode::Inline, MediaMode::Descriptors] {
        let page = page_bytes(&rig, &key, media).await;
        pages.push(page);
    }
    for page in &pages {
        assert_eq!(page["messages"].as_array().unwrap().len(), 3);
        assert_rows(
            &page["messages"][0],
            json!([{"part": "m0#0", "tag": "§1§"}]),
        );
        assert_rows(
            &page["messages"][1],
            json!([{"part": "m1#0", "tag": "§2§"}, {"part": "m1#1", "tag": "§3§"}]),
        );
        assert_rows(
            &page["messages"][2],
            json!([{"part": "m2#0", "tag": "§4§"}]),
        );
    }
    materialize_all(&rig, &key);
    for (media, before) in [MediaMode::Inline, MediaMode::Descriptors]
        .into_iter()
        .zip(pages)
    {
        let after = page_bytes(&rig, &key, media).await;
        assert_eq!(before["messages"], after["messages"]);
    }
}

#[tokio::test]
async fn crash_sealed_batch_rows_keep_exposed_keys() {
    let rig = Rig::new("crash-seal-tags", Arc::new(NoopExec));
    let key = rig.key("crash");
    let records = tagged_episode(&key);
    append_seed(&rig.wal_store, &key, &records[..4]);
    rig.store
        .index_run_active(&RunIndexRow {
            withdrawals_owed: false,
            title: None,
            run_id: "tagged".into(),
            session: key.clone(),
            submission_id: None,
            request_identity_digest: None,
            state: RunIndexState::Active,
            usage: None,
            final_step_finish_reason: None,
            retries_used: 0,
        })
        .unwrap();
    let before = page_bytes(&rig, &key, MediaMode::Descriptors).await;
    let marked = rig.registry.mark_crash_orphans(
        rig.wal_store.as_ref(),
        vec![(RunId("tagged".into()), key.clone())],
        std::time::SystemTime::now() + Duration::from_secs(1),
    );
    let swept = rig.registry.seal_marked_crash_orphans(marked).await;
    assert_eq!(swept.sealed, 1, "{swept:?}");
    let mut pages = Vec::new();
    for media in [MediaMode::Inline, MediaMode::Descriptors] {
        let after = page_bytes(&rig, &key, media).await;
        pages.push(after);
    }
    for after in pages {
        assert_rows(&after["messages"][1], json!([{"part": "m1#0"}]));
        assert_rows(
            &after["messages"][2],
            json!([{"part": "m2#0", "tag": "§2§"}]),
        );
        assert_eq!(
            before["messages"][1]["tags"], after["messages"][1]["tags"],
            "sealing must preserve exposed keys"
        );
    }
}

#[tokio::test]
async fn counted_default_read_bytes_are_pinned() {
    let rig = Rig::new("counted-read-bytes", Arc::new(NoopExec));
    let key = rig.key("pinned");
    let mut records = two_episode_tool_seed(&key);
    for record in &mut records {
        if let WalRecord::RunStarted { config, .. } = record {
            config.render_tags = true;
            config.computed_tags = Some(broca_wal::ComputedTagRules { checkpoint: 0 });
        }
    }
    write_legacy_seed(&rig.wal_store, &key, &records);
    let page = rig
        .registry
        .read(read_request(&key, Some(0), Some(10)))
        .await
        .unwrap();
    let bytes = serde_json::to_string(&page).unwrap();
    assert_eq!(
        bytes,
        include_str!("../../golden/session_read.counted.json").trim_end()
    );
}
