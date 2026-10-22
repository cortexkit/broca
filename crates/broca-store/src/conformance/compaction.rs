use std::sync::Arc;

use broca_core::{compaction::CompactionCheckpoint, ReadView};
use broca_wal::{
    CompactionCursor, CompactionMessageKind, CompactionRange, CompactionStabilityRank,
};

use super::*;

fn records(session: &BindIdentity) -> Vec<FramedRecord> {
    let mut records = base_records(session);
    let WalRecord::RunStarted { manifest, .. } = &mut records[0].record else {
        unreachable!()
    };
    *manifest = Some(Box::new(broca_wal::PlanManifest {
        plan: broca_wal::OpaqueJson::from_json_text(
            serde_json::json!({"compaction_item":{"provider":"provider","preset":"head","params":{}}})
                .to_string(),
        )
        .unwrap(),
        plan_identity: "plan".into(),
        composition_digest: "composition".into(),
        tools: Arc::new(vec![]),
        system_text: String::new(),
        system_digest: String::new(),
        join_version: 1,
        role_versions: Default::default(),
        fetches: vec![],
        declarations: vec![],
    }));
    records.push(compaction_message(1, CompactionMessageKind::Setup));
    records.extend(text_turn(2, 1, "first"));
    records
}

fn compaction_message(seq: u64, kind: CompactionMessageKind) -> FramedRecord {
    let setup = kind == CompactionMessageKind::Setup;
    let replacement = Arc::new(vec![Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: if setup {
                "initial head"
            } else {
                "updated head"
            }
            .into(),
        }],
    )]);
    framed(
        seq,
        WalRecord::CompactionMessage {
            provider: "provider".into(),
            kind,
            compaction_id: "compaction".into(),
            version: if setup { 1 } else { 2 },
            range: CompactionRange {
                from: 0,
                to: if setup { 0 } else { 2 },
            },
            sha256: broca_wal::compaction_replacement_sha256(&replacement).unwrap(),
            replacement,
            request_id: (!setup).then(|| "request-2".into()),
            cursor: (!setup).then(|| CompactionCursor {
                lineage_id: "r".into(),
                after_ordinal: Some(1),
            }),
            call_when: setup.then(|| {
                broca_wal::OpaqueJson::from_json_text(r#"{"kind":"context_share","share":0.5}"#)
                    .unwrap()
            }),
            stability: setup.then(|| vec![CompactionStabilityRank { index: 0, rank: 7 }]),
            ceiling: None,
            tag_checkpoint: None,
        },
    )
}

fn append_step(records: &mut Vec<FramedRecord>) {
    records.push(framed(
        4,
        WalRecord::CompactionRequest {
            provider: "provider".into(),
            request_id: "request-2".into(),
        },
    ));
    records.push(compaction_message(5, CompactionMessageKind::Step));
    records.extend(text_turn(6, 2, "second"));
}

fn page<S: SessionStore>(store: &S, session: &BindIdentity, view: ReadView) -> StorePageSnapshot {
    MaterializedStore::read_page_with_watermark(store, session, Some(0), u64::MAX, view)
        .expect("read page")
}

fn assert_model_snapshot(snapshot: &StorePageSnapshot, records: &[FramedRecord]) {
    let prefix: Vec<_> = records
        .iter()
        .filter(|frame| frame.seq <= snapshot.watermark_seq)
        .cloned()
        .collect();
    let checkpoint = AdmissionCheckpointV1::from_frames(&prefix).unwrap();
    let replay = replay(&records_only(&prefix)).unwrap();
    assert_eq!(snapshot.compaction, checkpoint.compaction);
    assert_eq!(
        snapshot
            .rows
            .iter()
            .map(|row| row.message.clone())
            .collect::<Vec<_>>(),
        replay.prompt,
    );
    assert_eq!(snapshot.prefix_len, snapshot.rows.len() as u64);
}

/// Replacement state and transcript rows share one watermark. An uncommitted turn or
/// a failed projection must leave both at the last successfully projected turn.
pub fn model_pages_return_atomic_compaction_and_rows<S: PageUsageConformance>(store: S) {
    let session = session("model-compaction-atomic");
    assert_eq!(page(&store, &session, ReadView::Model).compaction, None);
    let mut records = records(&session);
    Persister::project(&store, &session, &records).unwrap();
    let old = page(&store, &session, ReadView::Model);
    assert_eq!(old.watermark_seq, 3);
    assert_model_snapshot(&old, &records);
    assert_eq!(
        old.compaction
            .as_ref()
            .unwrap()
            .state()
            .unwrap()
            .applied
            .as_ref()
            .unwrap()
            .version,
        1
    );

    for limit in [0, u64::MAX] {
        let raw = MaterializedStore::read_page_with_watermark(
            &store,
            &session,
            Some(0),
            limit,
            ReadView::Raw,
        )
        .unwrap();
        assert_eq!(
            raw.compaction, None,
            "raw and head reads do not expose compaction"
        );
        assert_eq!(raw.watermark_seq, old.watermark_seq);
    }

    append_step(&mut records);
    Persister::project(&store, &session, &records[..records.len() - 1]).unwrap();
    assert_eq!(
        page(&store, &session, ReadView::Model),
        old,
        "an open turn stays in the WAL tail"
    );
    Persister::project(&store, &session, &records).unwrap();
    let updated = page(&store, &session, ReadView::Model);
    assert_eq!(updated.watermark_seq, 7);
    assert_model_snapshot(&updated, &records);
    assert_eq!(
        updated
            .compaction
            .as_ref()
            .unwrap()
            .state()
            .unwrap()
            .applied
            .as_ref()
            .unwrap()
            .version,
        2
    );

    let mut corrupt_tail = vec![compaction_message(8, CompactionMessageKind::Step)];
    corrupt_tail.push(run_started(
        9,
        "overlap",
        &session,
        "illegal",
        ModelHandle::new("stub", "m"),
    ));
    corrupt_tail.extend(text_turn(10, 3, "must not persist"));
    assert!(Persister::project(&store, &session, &corrupt_tail).is_err());
    assert_eq!(page(&store, &session, ReadView::Model), updated);
    assert_store_state_matches_full_projection(&store, &session, &records);

    let captured = AdmissionCheckpointV1::from_frames(&records).unwrap();
    let mut mismatched = captured.clone();
    mismatched.at_seq -= 1;
    store
        .replace_admission_checkpoint_for_conformance(&session, &mismatched)
        .unwrap();
    assert_eq!(page(&store, &session, ReadView::Model).compaction, None);
    store
        .replace_admission_checkpoint_json_for_conformance(&session, Some("not a checkpoint"))
        .unwrap();
    assert_eq!(page(&store, &session, ReadView::Model).compaction, None);
    store
        .replace_admission_checkpoint_for_conformance(&session, &captured)
        .unwrap();
    assert_eq!(page(&store, &session, ReadView::Model), updated);
}

/// Checkpoints written without compaction state need the whole WAL to recover the initial
/// replacement and frozen setup settings, even when later records contain an applied update.
pub fn absent_compaction_checkpoint_refolds_complete_wal<S: PageUsageConformance>(store: S) {
    let session = session("reader-compaction-checkpoint");
    let mut records = records(&session);
    Persister::project(&store, &session, &records).unwrap();
    let mut reader_checkpoint = AdmissionCheckpointV1::from_frames(&records).unwrap();
    reader_checkpoint.compaction = None;
    assert!(reader_checkpoint.ownership_complete());
    assert!(!reader_checkpoint.compaction_complete());
    store
        .replace_admission_checkpoint_for_conformance(&session, &reader_checkpoint)
        .unwrap();
    let reader_page = page(&store, &session, ReadView::Model);
    assert_eq!(reader_page.watermark_seq, 3);
    assert_eq!(
        reader_page.compaction, None,
        "missing state requests a complete model-page read"
    );
    assert!(
        Persister::projection_seed(&store, &session)
            .unwrap()
            .is_none(),
        "a compaction session needs the complete WAL to recover Setup"
    );

    append_step(&mut records);
    Persister::project(&store, &session, &records).unwrap();
    assert_store_state_matches_full_projection(&store, &session, &records);
    assert_model_snapshot(&page(&store, &session, ReadView::Model), &records);
    let seed = Persister::projection_seed(&store, &session)
        .unwrap()
        .unwrap();
    let state = seed
        .admission_checkpoint
        .compaction
        .as_ref()
        .unwrap()
        .state()
        .unwrap();
    assert_eq!(state.applied.as_ref().unwrap().version, 2);
    assert!(
        state.call_when.is_some(),
        "Setup's frozen schedule survives the refold"
    );
    assert_eq!(
        state.stability,
        vec![CompactionStabilityRank { index: 0, rank: 7 }]
    );
    // The synchronous client's request ids and cursor are no longer kept: nothing calls the
    // provider per step, so only the applied replacement and Setup's declarations matter.
    assert_eq!(state.provider, "provider");

    let mut captured_none = seed.admission_checkpoint;
    captured_none.compaction = Some(CompactionCheckpoint::None);
    store
        .replace_admission_checkpoint_for_conformance(&session, &captured_none)
        .unwrap();
    assert!(
        Persister::projection_seed(&store, &session)
            .unwrap()
            .is_some(),
        "captured pre-Setup state is a valid seed"
    );
    assert_eq!(
        page(&store, &session, ReadView::Model).compaction,
        Some(CompactionCheckpoint::None)
    );

    let ordinary = super::session("ordinary-reader-checkpoint");
    let mut ordinary_records = base_records(&ordinary);
    ordinary_records.extend(text_turn(2, 1, "ordinary"));
    Persister::project(&store, &ordinary, &ordinary_records).unwrap();
    let mut ordinary_checkpoint = AdmissionCheckpointV1::from_frames(&ordinary_records).unwrap();
    ordinary_checkpoint.compaction = None;
    assert!(ordinary_checkpoint.compaction_complete());
    store
        .replace_admission_checkpoint_for_conformance(&ordinary, &ordinary_checkpoint)
        .unwrap();
    assert!(
        Persister::projection_seed(&store, &ordinary)
            .unwrap()
            .is_some(),
        "ordinary sessions retain reader-era incremental seeds"
    );
}
