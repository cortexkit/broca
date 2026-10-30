//! A valid media checkpoint keeps per-chunk authorization independent of transcript length.
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine};
use broca_core::{
    AdmissionCheckpointV1, AdmissionStoreSnapshot, FileWalStore, MaterializedStore, WalStore,
};
use broca_module::{
    registry::{ActorHandle, LineageOpener, OpenError, RegistryHandle},
    LineageRegistry, ServeHandler,
};
use broca_protocol::{BindIdentity, ContentBlock, MediaRef, Message, Role};
use broca_wal::{
    FramedRecord, Wal, WalByteAnchor, WalError, WalRangeRead, WalRangeRecords, WalRecord,
};
use serde_json::{json, Value};

const PNG: &[u8] = include_bytes!("../../broca-provider/examples/media-live/fixtures/image.png");
const TAIL_PNG: &[u8] =
    include_bytes!("../../broca-provider/examples/media-live/fixtures/tool-image.png");

struct ReaderOnly;
#[async_trait]
impl LineageOpener for ReaderOnly {
    async fn open(
        &self,
        _: &BindIdentity,
        _: u64,
        _: RegistryHandle,
    ) -> Result<ActorHandle, OpenError> {
        panic!("media reads must not open an actor")
    }
}

struct SnapshotStore(Mutex<Option<AdmissionStoreSnapshot>>);
impl MaterializedStore for SnapshotStore {
    fn read_prefix_with_watermark(
        &self,
        _: &BindIdentity,
    ) -> Result<broca_core::StorePrefix, String> {
        panic!("media reads must not load transcript bodies")
    }
    fn read_admission_snapshot(
        &self,
        _: &BindIdentity,
    ) -> Result<Option<AdmissionStoreSnapshot>, String> {
        Ok(self.0.lock().unwrap().clone())
    }
}

struct CountedWal {
    inner: FileWalStore,
    frames: AtomicUsize,
    reads: Mutex<Vec<(bool, u64)>>,
}
impl CountedWal {
    fn count(&self, strict: bool, after: u64, range: &WalRangeRead) {
        self.reads.lock().unwrap().push((strict, after));
        let frames = match range.records() {
            WalRangeRecords::Complete(records)
            | WalRangeRecords::Partial(records)
            | WalRangeRecords::Unknown(records) => records.len(),
            WalRangeRecords::Absent => 0,
        };
        self.frames.fetch_add(frames, Ordering::Relaxed);
    }
    fn reset(&self) {
        self.frames.store(0, Ordering::Relaxed);
        self.reads.lock().unwrap().clear();
    }
}
impl WalStore for CountedWal {
    fn open(&self, key: &BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
        self.inner.open(key)
    }
    fn read_records(&self, key: &BindIdentity) -> Result<Vec<FramedRecord>, WalError> {
        let frames = self.inner.read_records(key)?;
        self.reads.lock().unwrap().push((false, 0));
        self.frames.fetch_add(frames.len(), Ordering::Relaxed);
        Ok(frames)
    }
    fn read_records_range(
        &self,
        key: &BindIdentity,
        after: u64,
        through: Option<u64>,
        anchor: Option<&WalByteAnchor>,
    ) -> Result<WalRangeRead, WalError> {
        let range = self.inner.read_records_range(key, after, through, anchor)?;
        self.count(false, after, &range);
        Ok(range)
    }
    fn read_records_range_from_valid_anchor(
        &self,
        key: &BindIdentity,
        after: u64,
        through: Option<u64>,
        anchor: &WalByteAnchor,
    ) -> Result<Option<WalRangeRead>, WalError> {
        let range = self
            .inner
            .read_records_range_from_valid_anchor(key, after, through, anchor)?;
        if let Some(range) = &range {
            self.count(true, after, range);
        }
        Ok(range)
    }
    fn file_identity(&self, key: &BindIdentity) -> Result<Option<u64>, WalError> {
        self.inner.file_identity(key)
    }
    fn flush_for_read(&self, key: &BindIdentity) -> Result<Option<u64>, WalError> {
        self.inner.flush_for_read(key)
    }
    fn uploads(&self) -> Option<broca_core::media_uploads::Uploads> {
        self.inner.uploads()
    }
}

fn started(key: &BindIdentity, id: usize, media: Option<&MediaRef>) -> WalRecord {
    let mut content = vec![ContentBlock::Text {
        text: format!("question {id}"),
    }];
    content.extend(media.cloned().map(ContentBlock::Media));
    serde_json::from_value(json!({"type":"run_started","run_id":format!("run-{id}"),"session":key,
        "config":{"model":{"provider_module_id":"fake","model_id":"fake"},"tools":[],"tool_choice":{"type":"auto"},
            "generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},
        "input":[Message::new(Role::User, content)],"origin":{"kind":"fresh"}})).unwrap()
}
fn finished(id: usize) -> WalRecord {
    serde_json::from_value(json!({"type":"run_finished","run_id":format!("run-{id}"),"reason":"completed","usage":{},"retries_used":0})).unwrap()
}

struct Fixture {
    dir: std::path::PathBuf,
    key: BindIdentity,
    wal: Arc<CountedWal>,
    store: Arc<SnapshotStore>,
    png: MediaRef,
    tail_png: MediaRef,
    watermark: u64,
}
impl Fixture {
    fn new(episodes: usize) -> Self {
        let dir = broca_test_support::scratch_root()
            .join(format!("media-read-cost-{}-{episodes}", std::process::id()));
        broca_test_support::remove_rig_dir(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        broca_core::media_store::create_media_directories(&dir).unwrap();
        let key = BindIdentity::new(dir.clone(), "media-read", "session");
        let wal = Arc::new(CountedWal {
            inner: FileWalStore::new(dir.join("wal")),
            frames: AtomicUsize::new(0),
            reads: Mutex::new(Vec::new()),
        });
        let png = broca_core::media_validation::validate_media(PNG, "image/png").unwrap();
        let tail_png = broca_core::media_validation::validate_media(TAIL_PNG, "image/png").unwrap();
        let mut writer = wal.inner.open(&key).unwrap();
        for id in 0..episodes {
            writer
                .append(1, &started(&key, id, (id == 0).then_some(&png)), false)
                .unwrap();
            writer.append(1, &finished(id), false).unwrap();
        }
        writer.sync().unwrap();
        let watermark = writer.last_seq();
        let range = wal
            .inner
            .read_records_range(&key, 0, Some(watermark), None)
            .unwrap();
        let WalRangeRecords::Complete(frames) = range.records() else {
            panic!("complete fixture WAL")
        };
        let checkpoint = AdmissionCheckpointV1::from_frames(frames).unwrap();
        assert!(checkpoint.media_seed_complete());
        let store = Arc::new(SnapshotStore(Mutex::new(Some(AdmissionStoreSnapshot {
            watermark_seq: watermark,
            lineage_id: range.lineage_id.map(|id| id.to_string()),
            anchor: range.anchor_at(watermark),
            checkpoint: Some(checkpoint),
        }))));
        writer
            .append(1, &started(&key, episodes, Some(&tail_png)), false)
            .unwrap();
        writer.append(1, &finished(episodes), true).unwrap();
        drop(writer);
        let media = wal
            .inner
            .uploads()
            .unwrap()
            .session_media_store(&key)
            .unwrap();
        let mut manifest = media.session(&key).unwrap();
        manifest.register_and_append(&png, PNG, || Ok(())).unwrap();
        manifest
            .register_and_append(&tail_png, TAIL_PNG, || Ok(()))
            .unwrap();
        Self {
            dir,
            key,
            wal,
            store,
            png,
            tail_png,
            watermark,
        }
    }
    fn registry(&self) -> LineageRegistry {
        LineageRegistry::new_with_reader(
            Arc::new(ReaderOnly),
            self.wal.clone(),
            Some(self.store.clone()),
        )
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        broca_test_support::remove_rig_dir(&self.dir);
    }
}

async fn read(
    registry: &LineageRegistry,
    fixture: &Fixture,
    media: &MediaRef,
    offset: usize,
) -> Value {
    ServeHandler::media_read(
        registry,
        &fixture.key,
        json!({"sha256":media.sha256,"offset":offset,"length":1}),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn media_chunk_authorization_is_bounded_and_missing_seed_replay_is_named() {
    let mut costs = Vec::new();
    for episodes in [4, 10_000] {
        let fixture = Fixture::new(episodes);
        let registry = fixture.registry();
        fixture.wal.reset();
        for offset in 0..3 {
            let reply = read(&registry, &fixture, &fixture.png, offset).await;
            assert_eq!(reply["data"], STANDARD.encode(&PNG[offset..offset + 1]));
        }
        let tail = read(&registry, &fixture, &fixture.tail_png, 0).await;
        assert_eq!(tail["data"], STANDARD.encode(&TAIL_PNG[..1]));
        // A strict read starts at a validated WAL byte position. Invalid positions are
        // rejected rather than used as a reason to decode earlier records.
        // Reading a closed WAL discovers the tail's highest sequence before flushing the
        // WAL file. A second read stops at that sequence, excluding later concurrent appends.
        // Four requests decode the two-frame suffix at most twice per request.
        let frames = fixture.wal.frames.load(Ordering::Relaxed);
        assert!(frames <= 16, "{episodes} episodes decoded {frames} frames");
        assert!(fixture
            .wal
            .reads
            .lock()
            .unwrap()
            .iter()
            .all(|(strict, after)| *strict && *after == fixture.watermark));
        costs.push(frames);
        println!("{episodes} stored episodes: four chunk reads decoded {frames} tail frames");

        fixture
            .store
            .0
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .checkpoint
            .as_mut()
            .unwrap()
            .media = None;
        fixture.wal.reset();
        let before = broca_core::media_request::seed_fallback_count();
        let (reply, logs) =
            broca_test_support::capture_logs(read(&registry, &fixture, &fixture.png, 0)).await;
        assert_eq!(reply["data"], STANDARD.encode(&PNG[..1]));
        assert_eq!(broca_core::media_request::seed_fallback_count(), before + 1);
        assert!(
            logs.iter().any(|event| event
                .fields
                .get("reason")
                .is_some_and(|reason| reason == "media_seed_missing")),
            "{logs:?}"
        );
        assert!(fixture.wal.frames.load(Ordering::Relaxed) >= fixture.watermark as usize);
        assert!(fixture
            .wal
            .reads
            .lock()
            .unwrap()
            .iter()
            .any(|(strict, after)| !strict && *after == 0));
        println!(
            "{episodes} stored episodes: missing media seed used counted media_seed_missing replay"
        );
        *fixture.store.0.lock().unwrap() = None;
        fixture.wal.reset();
        let before = broca_core::media_request::seed_fallback_count();
        let (reply, logs) =
            broca_test_support::capture_logs(read(&registry, &fixture, &fixture.png, 0)).await;
        assert_eq!(reply["data"], STANDARD.encode(&PNG[..1]));
        assert_eq!(broca_core::media_request::seed_fallback_count(), before + 1);
        assert!(
            logs.iter().any(|event| event
                .fields
                .get("reason")
                .is_some_and(|reason| reason == "media_checkpoint_missing")),
            "{logs:?}"
        );
        assert!(fixture.wal.frames.load(Ordering::Relaxed) >= fixture.watermark as usize);
    }
    assert_eq!(
        costs[0], costs[1],
        "authorization work grew with stored history"
    );
}
