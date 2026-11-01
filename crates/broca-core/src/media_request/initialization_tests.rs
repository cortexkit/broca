use super::*;
use crate::FileWalStore;
use broca_wal::{FramedRecord, WalLineageId, WalRangeRead, WalRecord};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
};

struct Fixture {
    root: PathBuf,
    store: Arc<FileWalStore>,
    writer: Arc<WalHandle>,
    snapshot: PriorWalSnapshot,
    session: BindIdentity,
}

impl Fixture {
    async fn new(media: bool) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "media-init-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let session = BindIdentity::new(&root, "test", "initializer");
        let store = Arc::new(FileWalStore::new(root.join("wal")));
        let mut input =
            serde_json::json!([{"role":"user","content":[{"type":"text","text":"original"}]}]);
        if media {
            input[0]["content"]
                .as_array_mut()
                .unwrap()
                .push(reference('a'));
        }
        let started = serde_json::from_value(serde_json::json!({
            "type":"run_started","run_id":"run","session":session,
            "config":{"model":{"provider_module_id":"fake","model_id":"fake"},"tools":[],
                "tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},
                "context_epoch":0,"renderer_version":1,"frozen_render_config":null},
            "input":input,"origin":{"kind":"fresh"}
        }))
        .unwrap();
        let mut wal = store.open(&session).unwrap();
        wal.append(1, &started, true).unwrap();
        drop(wal);
        let frames = store.read_records(&session).unwrap();
        let range = store
            .read_records_range(&session, 0, Some(1), None)
            .unwrap();
        let snapshot = PriorWalSnapshot {
            lineage_id: range.lineage_id.map(|lineage| lineage.to_string()),
            anchor: range.next_anchor,
            base_watermark: 0,
            head_seq: 1,
            tail: Arc::new(frames.clone()),
            checkpoint_at_base: None,
            checkpoint_at_head: crate::AdmissionCheckpointV1::from_frames(&frames).unwrap(),
        };
        let writer = Arc::new(WalHandle::spawn(store.open(&session).unwrap(), 1).unwrap());
        writer.seed_lineage_tags(Some(crate::tags::lineage_tags(
            frames.iter().map(|frame| &frame.record),
        )));
        crate::run::seed_display_placement(&writer, &snapshot, store.as_ref(), None, &session);
        Self {
            root,
            store,
            writer,
            snapshot,
            session,
        }
    }

    async fn advance(&self, media: bool) {
        let mut content = vec![serde_json::json!({"type":"text","text":"steered"})];
        if media {
            content.push(reference('b'));
        }
        self.writer
            .record(
                serde_json::from_value(serde_json::json!({
                    "type":"steered","input":[{"role":"user","content":content}]
                }))
                .unwrap(),
            )
            .into_result()
            .unwrap();
        self.writer.sync().await.unwrap();
    }

    async fn close(self) {
        Arc::try_unwrap(self.writer)
            .unwrap_or_else(|_| panic!("the initializer released its writer clone"))
            .shutdown()
            .await
            .unwrap();
        std::fs::remove_dir_all(self.root).unwrap();
    }
}

fn reference(digit: char) -> serde_json::Value {
    serde_json::json!({"type":"media","kind":"image","mime":"image/png","byte_size":1,
        "sha256":digit.to_string().repeat(64),"width":1,"height":1})
}

enum Fault {
    None,
    Foreign(WalLineageId),
    UnknownLineage,
    Unreadable,
    Incomplete,
    MissingAnchor,
    MismatchedAnchor,
    RejectOrigin,
}

struct ObservedWal {
    inner: Arc<FileWalStore>,
    fault: Fault,
    reads: AtomicUsize,
    append_during_read: Mutex<Option<(Arc<WalHandle>, tokio::runtime::Handle)>>,
}

impl ObservedWal {
    fn new(inner: Arc<FileWalStore>, fault: Fault) -> Arc<Self> {
        Arc::new(Self {
            inner,
            fault,
            reads: AtomicUsize::new(0),
            append_during_read: Mutex::new(None),
        })
    }
}

impl WalStore for ObservedWal {
    fn open(
        &self,
        key: &BindIdentity,
    ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
        self.inner.open(key)
    }
    fn read_records(&self, key: &BindIdentity) -> Result<Vec<FramedRecord>, broca_wal::WalError> {
        self.inner.read_records(key)
    }
    fn file_identity(&self, key: &BindIdentity) -> Result<Option<u64>, broca_wal::WalError> {
        self.inner.file_identity(key)
    }
    fn live_wal_bytes(&self, key: &BindIdentity) -> Result<Option<u64>, broca_wal::WalError> {
        self.inner.live_wal_bytes(key)
    }
    fn read_records_range(
        &self,
        key: &BindIdentity,
        after: u64,
        through: Option<u64>,
        anchor: Option<&WalByteAnchor>,
    ) -> Result<WalRangeRead, broca_wal::WalError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if matches!(self.fault, Fault::RejectOrigin) && after == 0 {
            return Err(broca_wal::WalError::Io(std::io::Error::other(
                "origin prefix is archived or exceeds the read cap",
            )));
        }
        if matches!(self.fault, Fault::Unreadable) {
            return Err(broca_wal::WalError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unreadable advance",
            )));
        }
        let mut range = self.inner.read_records_range(key, after, through, anchor)?;
        match self.fault {
            Fault::Foreign(lineage) => range.lineage_id = Some(lineage),
            Fault::UnknownLineage => range.lineage_id = None,
            Fault::Incomplete => {
                let expected_anchor = range.next_anchor;
                range = self.inner.read_records_range(
                    key,
                    after,
                    through.map(|seq| seq - 1),
                    anchor,
                )?;
                range.next_anchor = expected_anchor;
            }
            Fault::MissingAnchor => range.next_anchor = None,
            Fault::MismatchedAnchor => range.next_anchor.as_mut().unwrap().seq += 1,
            _ => {}
        }
        let append = self.append_during_read.lock().unwrap().take();
        if let Some((writer, runtime)) = append {
            writer
                .record(WalRecord::SessionPrefixFlushed {
                    send_id: "later".into(),
                    pending_generation: None,
                })
                .into_result()
                .map_err(|error| {
                    broca_wal::WalError::Io(std::io::Error::other(error.to_string()))
                })?;
            runtime.block_on(writer.sync()).map_err(|error| {
                broca_wal::WalError::Io(std::io::Error::other(error.to_string()))
            })?;
        }
        Ok(range)
    }
}

#[tokio::test]
async fn coherent_media_seed_reuses_the_admission_prefix_without_replay() {
    for captured in [false, true] {
        let fixture = Fixture::new(captured).await;
        let observed = ObservedWal::new(fixture.store.clone(), Fault::None);
        let initialized = initialize_media_checkpoint(
            &fixture.writer,
            observed.clone(),
            &fixture.session,
            &fixture.snapshot,
        )
        .await
        .unwrap();
        assert_eq!(initialized.checkpoint.at_seq, 1);
        assert_eq!(initialized.anchor.unwrap().seq, 1);
        assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
        if let Some(DurableMediaSeed::Ready(seed)) = &fixture.snapshot.checkpoint_at_head.media {
            assert_eq!(initialized.checkpoint.encode(), seed.encode());
        }
        drop(observed);
        fixture.close().await;
    }
}

#[tokio::test]
async fn forward_media_initialization_matches_full_replay_and_keeps_new_captures() {
    for captured_before in [false, true] {
        let fixture = Fixture::new(captured_before).await;
        fixture.advance(true).await;
        let initialized = initialize_media_checkpoint(
            &fixture.writer,
            fixture.store.clone(),
            &fixture.session,
            &fixture.snapshot,
        )
        .await
        .unwrap();
        let full = crate::AdmissionCheckpointV1::from_frames(
            &fixture.store.read_records(&fixture.session).unwrap(),
        )
        .unwrap();
        let Some(DurableMediaSeed::Ready(seed)) = full.media else {
            panic!("full replay contains the new capture")
        };
        assert_eq!(initialized.checkpoint.encode(), seed.encode());
        assert_eq!(initialized.checkpoint.at_seq, 2);
        assert_eq!(initialized.anchor.unwrap().seq, 2);
        assert!(initialized
            .checkpoint
            .encode()
            .to_string()
            .contains(&"b".repeat(64)));
        fixture.close().await;
    }
}

#[tokio::test]
async fn ready_media_seed_folds_a_text_only_advance_without_reading_the_origin() {
    let fixture = Fixture::new(true).await;
    fixture.advance(false).await;
    let observed = ObservedWal::new(fixture.store.clone(), Fault::RejectOrigin);
    let initialized = initialize_media_checkpoint(
        &fixture.writer,
        observed.clone(),
        &fixture.session,
        &fixture.snapshot,
    )
    .await
    .unwrap();
    let full = crate::AdmissionCheckpointV1::from_frames(
        &fixture.store.read_records(&fixture.session).unwrap(),
    )
    .unwrap();
    let Some(DurableMediaSeed::Ready(seed)) = full.media else {
        panic!("earlier capture survives")
    };
    assert_eq!(initialized.checkpoint.encode(), seed.encode());
    assert_eq!(initialized.checkpoint.at_seq, 2);
    assert_eq!(initialized.anchor.unwrap().seq, 2);
    drop(observed);
    fixture.close().await;
}

#[tokio::test]
async fn advanced_media_initialization_refuses_foreign_lineage() {
    let fixture = Fixture::new(false).await;
    let foreign = Fixture::new(false).await;
    fixture.advance(false).await;
    let lineage = foreign.writer.lineage_id().unwrap();
    assert_ne!(fixture.writer.lineage_id(), Some(lineage));
    let observed = ObservedWal::new(fixture.store.clone(), Fault::Foreign(lineage));
    let error = initialize_media_checkpoint(
        &fixture.writer,
        observed.clone(),
        &fixture.session,
        &fixture.snapshot,
    )
    .await
    .unwrap_err();
    assert!(error.contains("lineage"), "{error}");
    drop(observed);
    foreign.close().await;
    fixture.close().await;
}

#[tokio::test]
async fn cached_media_seed_refuses_a_lineage_change_since_admission() {
    let mut fixture = Fixture::new(true).await;
    let foreign = Fixture::new(false).await;
    fixture.snapshot.lineage_id = foreign.snapshot.lineage_id.clone();
    let error = initialize_media_checkpoint(
        &fixture.writer,
        fixture.store.clone(),
        &fixture.session,
        &fixture.snapshot,
    )
    .await
    .unwrap_err();
    assert!(error.contains("lineage changed"), "{error}");
    foreign.close().await;
    fixture.close().await;
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn verified_media_suffix_can_extend_a_seed_beyond_the_full_read_cap() {
    let fixture = Fixture::new(true).await;
    fixture.advance(false).await;
    let limited = Arc::new(FileWalStore::new(fixture.root.join("wal")).with_read_cap_for_test(512));
    assert!(
        limited.read_records(&fixture.session).is_err(),
        "the complete prefix exceeds the read cap"
    );
    let initialized = initialize_media_checkpoint(
        &fixture.writer,
        limited,
        &fixture.session,
        &fixture.snapshot,
    )
    .await
    .unwrap();
    assert_eq!(initialized.checkpoint.at_seq, 2);
    assert_eq!(initialized.anchor.unwrap().seq, 2);
    fixture.close().await;
}

#[tokio::test]
async fn media_initialization_refuses_a_backwards_head() {
    let fixture = Fixture::new(false).await;
    let mut future = fixture.snapshot.clone();
    future.head_seq = 2;
    let error = initialize_media_checkpoint(
        &fixture.writer,
        fixture.store.clone(),
        &fixture.session,
        &future,
    )
    .await
    .unwrap_err();
    assert!(error.contains("backwards"), "{error}");
    fixture.close().await;
}

#[tokio::test]
async fn advanced_media_initialization_refuses_unverified_prefixes() {
    let fixture = Fixture::new(false).await;
    fixture.advance(false).await;
    for (fault, reason) in [
        (Fault::UnknownLineage, "lineage"),
        (Fault::Unreadable, "unreadable"),
        (Fault::Incomplete, "contiguous head"),
        (Fault::MissingAnchor, "anchor"),
        (Fault::MismatchedAnchor, "anchor"),
    ] {
        let observed = ObservedWal::new(fixture.store.clone(), fault);
        let error = initialize_media_checkpoint(
            &fixture.writer,
            observed.clone(),
            &fixture.session,
            &fixture.snapshot,
        )
        .await
        .unwrap_err();
        assert!(error.contains(reason), "{error}");
        drop(observed);
    }
    fixture.close().await;
}

#[tokio::test]
async fn later_forward_append_keeps_the_reloaded_seed_anchor_at_its_verified_cut() {
    let fixture = Fixture::new(false).await;
    fixture.advance(false).await;
    let observed = ObservedWal::new(fixture.store.clone(), Fault::None);
    *observed.append_during_read.lock().unwrap() =
        Some((fixture.writer.clone(), tokio::runtime::Handle::current()));
    let initialized = initialize_media_checkpoint(
        &fixture.writer,
        observed.clone(),
        &fixture.session,
        &fixture.snapshot,
    )
    .await
    .unwrap();
    assert_eq!(fixture.writer.durable_head().load(Ordering::Acquire), 3);
    assert_eq!(initialized.checkpoint.at_seq, 2);
    let anchor = initialized.anchor.unwrap();
    assert_eq!(anchor.seq, 2);
    let tail = fixture
        .store
        .read_records_range_from_valid_anchor(&fixture.session, 2, Some(3), &anchor)
        .unwrap()
        .unwrap();
    let WalRangeRecords::Complete(records) = tail.into_records() else {
        panic!("later append has a verified tail")
    };
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].seq, 3);
    drop(observed);
    fixture.close().await;
}
