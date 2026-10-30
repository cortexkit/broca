use super::*;
use broca_core::{
    context::{AdmissionStoreSnapshot, StorePrefix},
    MaterializedStore, WalStore,
};
use broca_wal::{FramedRecord, WalByteAnchor, WalError, WalRangeRead};
use std::sync::atomic::{AtomicUsize, Ordering};

struct CountingWal {
    inner: broca_core::FileWalStore,
    full: AtomicUsize,
    ranged: AtomicUsize,
}

impl WalStore for CountingWal {
    fn open(&self, session: &BindIdentity) -> Result<Box<dyn broca_wal::Wal + Send>, WalError> {
        self.inner.open(session)
    }
    fn read_records(&self, session: &BindIdentity) -> Result<Vec<FramedRecord>, WalError> {
        self.full.fetch_add(1, Ordering::SeqCst);
        self.inner.read_records(session)
    }
    fn read_records_range(
        &self,
        session: &BindIdentity,
        after: u64,
        through: Option<u64>,
        anchor: Option<&WalByteAnchor>,
    ) -> Result<WalRangeRead, WalError> {
        self.ranged.fetch_add(1, Ordering::SeqCst);
        self.inner
            .read_records_range(session, after, through, anchor)
    }
}

struct StoredCheckpoint(AdmissionStoreSnapshot);
impl MaterializedStore for StoredCheckpoint {
    fn read_prefix_with_watermark(&self, _: &BindIdentity) -> Result<StorePrefix, String> {
        panic!("admission repair must not infer capture state from stored message rows")
    }
    fn read_admission_snapshot(
        &self,
        _: &BindIdentity,
    ) -> Result<Option<AdmissionStoreSnapshot>, String> {
        Ok(Some(self.0.clone()))
    }
}

#[test]
fn admission_loader_repairs_each_bad_media_seed_from_file_wal_with_prepared_request_parity() {
    use broca_core::media_request::DurableMediaSeed;
    let fixture = Fixture::new();
    let frozen = super::live_tests::frozen(WireFamilyId::AnthropicMessages);
    let records = super::live_tests::history(&fixture, &frozen);
    let WalRecord::RunStarted {
        session: identity, ..
    } = &records[0]
    else {
        unreachable!()
    };
    let wal = CountingWal {
        inner: broca_core::FileWalStore::new(fixture.root.join("wal")),
        full: AtomicUsize::new(0),
        ranged: AtomicUsize::new(0),
    };
    let mut writer = wal.open(identity).unwrap();
    for record in &records {
        writer.append(1, record, true).unwrap();
    }
    let lineage = writer.lineage_id().unwrap().to_string();
    drop(writer);
    let complete_frames = wal.inner.read_records(identity).unwrap();
    let head = complete_frames.last().unwrap().seq;
    let complete = broca_core::AdmissionCheckpointV1::from_frames(&complete_frames).unwrap();
    let good_store = AdmissionStoreSnapshot {
        watermark_seq: head,
        lineage_id: Some(lineage),
        anchor: None,
        checkpoint: Some(complete.clone()),
    };
    let good = broca_core::load_prior_wal_snapshot(
        &wal,
        identity,
        head,
        Some(&StoredCheckpoint(good_store.clone())),
    )
    .unwrap();
    assert_eq!(
        good.base_watermark, head,
        "the control is eligible for checkpoint reuse"
    );
    assert_eq!(wal.full.load(Ordering::SeqCst), 0);
    assert_eq!(wal.ranged.load(Ordering::SeqCst), 1);

    let profile = frozen.media.as_ref().unwrap();
    let opts =
        broca_protocol::CallOptions::new(broca_core::replay::replay(&records).unwrap().prompt);
    let mut full_session = fixture.session(&[]);
    let full_aging = MediaAging::replay_wal(profile, &mut full_session, &records).unwrap();
    let mut bundle = prepare(&full_session, Some(profile), &opts.prompt, &full_aging).unwrap();
    assert_eq!(
        bundle.media.len(),
        2,
        "the oracle contains hydrated person and tool media"
    );
    let expected = render_prepared(
        &*crate::registry::wire_family(frozen.family_id).unwrap(),
        &frozen,
        &opts,
        Some(&mut bundle),
        &|prompt| prompt.to_vec(),
    )
    .unwrap();
    let seed = serde_json::to_value(complete.media.as_ref().unwrap()).unwrap();
    let mut malformed = seed.clone();
    malformed["index"] = json!("malformed index");
    let mut bad_digest = seed;
    bad_digest["sha256"] = json!("0".repeat(64));
    for (name, damaged) in [
        ("missing", None),
        ("unknown_version", Some(json!({"version":99}))),
        ("malformed_index", Some(malformed)),
        ("bad_digest", Some(bad_digest)),
    ] {
        let mut stored = good_store.clone();
        stored.checkpoint.as_mut().unwrap().media =
            damaged.map(|value| serde_json::from_value::<DurableMediaSeed>(value).unwrap());
        wal.full.store(0, Ordering::SeqCst);
        wal.ranged.store(0, Ordering::SeqCst);
        let before = broca_core::media_request::seed_fallback_count();
        let repaired = broca_core::load_prior_wal_snapshot(
            &wal,
            identity,
            head,
            Some(&StoredCheckpoint(stored)),
        )
        .unwrap();
        assert_eq!(
            wal.full.load(Ordering::SeqCst),
            1,
            "{name}: repair reads the complete file WAL"
        );
        assert_eq!(
            wal.ranged.load(Ordering::SeqCst),
            0,
            "{name}: an invalid seed is not extended"
        );
        assert!(
            broca_core::media_request::seed_fallback_count() > before,
            "{name}: the named media repair gate ran"
        );
        assert_eq!(repaired.base_watermark, 0, "{name}");
        assert!(repaired.checkpoint_at_base.is_none(), "{name}");
        assert_eq!(repaired.checkpoint_at_head, complete, "{name}");
        let Some(DurableMediaSeed::Ready(seed)) = repaired.checkpoint_at_head.media else {
            panic!("{name}: repair must build a usable seed")
        };
        let live = LiveMedia::from_checkpoint(
            fixture.store.clone(),
            *seed,
            None,
            Some(profile.clone()),
            Box::new(move |after| {
                assert_eq!(
                    after, head,
                    "a repaired seed must not read its prefix again"
                );
                Ok(Vec::new())
            }),
        )
        .unwrap();
        assert_eq!(
            live.render(&serde_json::to_value(&frozen).unwrap(), &opts, false, true)
                .unwrap(),
            expected,
            "{name}: repaired preparation equals complete replay"
        );
    }
}
