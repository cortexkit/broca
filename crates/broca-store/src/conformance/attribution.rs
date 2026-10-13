use super::*;

fn terminal(seq: u64, reason: RunFinishReason) -> FramedRecord {
    framed(
        seq,
        WalRecord::RunFinished {
            reason,
            ts_ms: None,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
    )
}

fn raw<S: SessionStore>(store: &S, session: &BindIdentity) -> StorePageSnapshot {
    store
        .read_page_with_watermark(session, Some(0), u64::MAX, broca_core::ReadView::Raw)
        .unwrap()
}

fn check(run: &Option<broca_core::RunAttribution>, id: &str, final_message: bool) {
    let run = run
        .as_ref()
        .expect("attributable message must name its run");
    assert_eq!(run.run_id, id);
    assert_eq!(run.episode, id);
    assert_eq!(run.is_final, final_message);
}

pub fn run_attribution_follows_each_episode<S: SessionStore>(store: S) {
    let session = session("message-runs");
    let model = ModelHandle {
        provider_module_id: "mock".into(),
        model_id: "test".into(),
    };
    let mut records = vec![
        run_started(1, "first", &session, "first input", model.clone()),
        text_step(2, 1, "first answer"),
        terminal(3, RunFinishReason::Completed),
    ];
    records.extend([
        run_started(4, "second", &session, "queued input", model),
        text_step(5, 1, "second answer"),
        terminal(6, RunFinishReason::Completed),
    ]);
    Persister::project(&store, &session, &records).unwrap();
    let snapshot = raw(&store, &session);
    assert_eq!(snapshot.rows.len(), 4);
    for (row, id, final_message) in [
        (0, "first", false),
        (1, "first", true),
        (2, "second", false),
        (3, "second", true),
    ] {
        check(&snapshot.rows[row].run, id, final_message);
    }
    let wal_page =
        read_session_page(&StorePageSnapshot::empty(), &records, Some(0), Some(10)).unwrap();
    assert_eq!(
        snapshot.rows.iter().map(|row| &row.run).collect::<Vec<_>>(),
        wal_page
            .messages
            .iter()
            .map(|row| &row.run)
            .collect::<Vec<_>>()
    );
    records.extend([
        run_started(
            7,
            "third",
            &session,
            "third input",
            ModelHandle::new("mock", "test"),
        ),
        framed(
            8,
            WalRecord::Steered {
                retention: None,
                ts_ms: None,
                send_id: Some("steer".into()),
                request_identity_digest: None,
                delivery: Some(broca_protocol::Delivery::Steer),
                title: None,
                hooks: None,
                input: vec![Message::new(
                    Role::User,
                    vec![ContentBlock::Text {
                        text: "unrendered steer".into(),
                    }],
                )]
                .into(),
            },
        ),
        terminal(9, RunFinishReason::Cancelled),
    ]);
    let tail = &records[snapshot.watermark_seq as usize..];
    let page = read_session_page(&snapshot, tail, Some(0), Some(10)).unwrap();
    assert_eq!(page.messages.len(), 6);
    check(&page.messages[4].run, "third", false);
    check(&page.messages[5].run, "third", true);
    assert_eq!(
        page.messages[5].message.role,
        Role::User,
        "an unrendered steer is the last produced message at terminal"
    );
}

pub fn run_attribution_waits_for_terminal<S: SessionStore>(store: S) {
    let session = session("active-run");
    let mut records = base_records(&session);
    records.extend(tool_turn(2, 1));
    Persister::project(&store, &session, &records).unwrap();
    let before = raw(&store, &session);
    assert_eq!(before.rows.len(), 3);
    assert!(before
        .rows
        .iter()
        .all(|row| !row.run.as_ref().unwrap().is_final));
    records.push(terminal(
        records.len() as u64 + 1,
        RunFinishReason::Completed,
    ));
    let tail = &records[before.watermark_seq as usize..];
    let during_lag = read_session_page(&before, tail, Some(0), Some(10)).unwrap();
    assert!(
        during_lag
            .messages
            .last()
            .unwrap()
            .run
            .as_ref()
            .unwrap()
            .is_final
    );
    Persister::project(&store, &session, tail).unwrap();
    let after = raw(&store, &session);
    assert_eq!(
        after.watermark_seq, before.watermark_seq,
        "terminal stays in WAL for rollback readers"
    );
    assert!(
        after.rows.last().unwrap().run.as_ref().unwrap().is_final,
        "terminal projection must update an already materialized row"
    );
    assert!(after.rows[..2]
        .iter()
        .all(|row| !row.run.as_ref().unwrap().is_final));
}

pub fn interrupted_tool_result_is_final<S: SessionStore>(store: S) {
    let session = session("tool-final");
    let mut records = base_records(&session);
    records.extend(tool_turn(2, 1));
    records.push(terminal(
        records.len() as u64 + 1,
        RunFinishReason::Interrupted,
    ));
    Persister::project(&store, &session, &records).unwrap();
    let snapshot = raw(&store, &session);
    let last = snapshot.rows.last().unwrap();
    assert_eq!(last.message.role, Role::Tool);
    assert!(
        last.run.as_ref().unwrap().is_final,
        "the last message is final regardless of role"
    );
    assert!(snapshot.rows[..snapshot.rows.len() - 1]
        .iter()
        .all(|row| !row.run.as_ref().unwrap().is_final));
}

pub fn imported_and_pre_run_id_messages_omit_run<S: SessionStore>(store: S) {
    let session = session("unknown-origin");
    let mut records = base_records(&session);
    records[0].seq = 1;
    if let WalRecord::RunStarted { origin, .. } = &mut records[0].record {
        *origin = RunOrigin::SeededFrom {
            source_ref: "fixture:external".into(),
            origin_device_pseudonym: None,
            base_seq: None,
            origin_kind: SeedOriginKind::Import,
        };
    }
    records.push(terminal(2, RunFinishReason::Completed));
    records.extend([
        run_started(
            3,
            "real",
            &session,
            "real input",
            ModelHandle::new("mock", "test"),
        ),
        text_step(4, 1, "real answer"),
        terminal(5, RunFinishReason::Completed),
    ]);
    Persister::project(&store, &session, &records).unwrap();
    let snapshot = raw(&store, &session);
    assert_eq!(snapshot.rows.len(), 3);
    assert_eq!(
        snapshot.rows[0].run, None,
        "synthetic import id cannot name the historical producer"
    );
    check(&snapshot.rows[1].run, "real", false);
    check(&snapshot.rows[2].run, "real", true);
    let legacy = read_session_page(
        &StorePageSnapshot::empty(),
        &[text_step(1, 1, "pre-run-id answer")],
        Some(0),
        Some(10),
    )
    .unwrap();
    assert_eq!(legacy.messages.len(), 1);
    assert_eq!(legacy.messages[0].run, None);
}

pub fn rollback_rows_require_attribution_backfill<S: PageUsageConformance>(store: S) {
    let session = session("rollback-attribution");
    let mut records = base_records(&session);
    records.extend(text_turn(2, 1, "old-format row"));
    records.push(terminal(4, RunFinishReason::Completed));
    Persister::project(&store, &session, &records).unwrap();
    assert!(raw(&store, &session).rows[1].run.is_some());
    for upgraded in [false, true] {
        store
            .erase_attribution_for_conformance(&session, upgraded)
            .unwrap();
        assert_eq!(
            store
                .read_page_with_attribution(&session, Some(0), 10, broca_core::ReadView::Raw)
                .unwrap_err(),
            broca_core::StorePageReadError::AttributionStale
        );
        assert!(Persister::projection_seed(&store, &session)
            .unwrap()
            .is_none());
        let page =
            read_session_page(&StorePageSnapshot::empty(), &records, Some(0), Some(10)).unwrap();
        assert!(page.messages[1].run.as_ref().unwrap().is_final);
        Persister::project(&store, &session, &records).unwrap();
        assert!(raw(&store, &session).rows[1].run.as_ref().unwrap().is_final);
    }
}
