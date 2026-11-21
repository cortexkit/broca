use super::*;
use broca_session_store::session::moves;
use sha2::{Digest as _, Sha256};

// The existing 1 GiB read budget applies to the newest whole export, not to older snapshots.
pub(super) const MOVE_IMPORT_BUDGET: u64 = broca_wal::DEFAULT_MAX_READ_BYTES;

impl Driver {
    pub(super) async fn incoming_store(
        &self,
        reference: &str,
        cut: &str,
        page: DoorRead,
        op: Op,
    ) -> Result<Incoming, CutReadFailure> {
        self.incoming_store_bounded(reference, cut, page, MOVE_IMPORT_BUDGET, op)
            .await
    }

    async fn incoming_store_bounded(
        &self,
        reference: &str,
        cut: &str,
        mut page: DoorRead,
        budget: u64,
        op: Op,
    ) -> Result<Incoming, CutReadFailure> {
        let metadata = (page.stream_end, page.digest.clone());
        let mut hash = Sha256::new();
        let mut offset = 0u64;
        let mut position = 0u64;
        let mut header = Vec::new();
        let mut magic = Vec::new();
        let mut remaining = 0u64;
        let mut keep = false;
        let mut newest = moves::STREAM.to_vec();
        loop {
            if (page.stream_end, &page.digest) != (metadata.0, &metadata.1)
                || page.from_offset != offset
                || page.end_offset <= offset && offset != metadata.0
            {
                return Err(unavailable("sealed store metadata or page range changed").into());
            }
            hash.update(&page.bytes);
            let mut bytes = page.bytes.as_slice();
            if magic.len() < moves::STREAM.len() {
                let take = (moves::STREAM.len() - magic.len()).min(bytes.len());
                magic.extend_from_slice(&bytes[..take]);
                bytes = &bytes[take..];
                position += take as u64;
                if magic.len() == moves::STREAM.len() && magic != moves::STREAM {
                    return Err(refusal(
                        "incompatible_version",
                        "unsupported store export stream format",
                        Value::Null,
                    )
                    .into());
                }
            }
            while !bytes.is_empty() {
                if remaining == 0 {
                    let take = (40 - header.len()).min(bytes.len());
                    header.extend_from_slice(&bytes[..take]);
                    bytes = &bytes[take..];
                    position += take as u64;
                    if header.len() < 40 {
                        continue;
                    }
                    remaining = u64::from_le_bytes(header[..8].try_into().unwrap());
                    let end = position
                        .checked_add(remaining)
                        .ok_or_else(|| unavailable("store frame length overflow"))?;
                    if end > metadata.0 || remaining == 0 {
                        return Err(
                            unavailable("store frame does not end within sealed prefix").into()
                        );
                    }
                    keep = end == metadata.0;
                    if keep {
                        if remaining > budget {
                            return Err(refusal("unavailable", "newest store export exceeds the move-import budget", json!({"reason":"move_import_budget_exceeded","export_size":remaining,"budget":budget})).into());
                        }
                        newest.extend_from_slice(&header);
                    }
                    header.clear();
                }
                let take = remaining.min(bytes.len() as u64) as usize;
                if keep {
                    newest.extend_from_slice(&bytes[..take]);
                }
                bytes = &bytes[take..];
                remaining -= take as u64;
                position += take as u64;
            }
            offset = page.end_offset;
            if offset == metadata.0 {
                if remaining != 0 || !header.is_empty() || !keep {
                    return Err(unavailable("sealed store stream ends inside an export").into());
                }
                if broca_session_store::codec::hex(&hash.finalize()) != metadata.1 {
                    return Err(refusal(
                        "digest_mismatch",
                        "store stream differs from sealed digest",
                        Value::Null,
                    )
                    .into());
                }
                moves::last_export(&newest).map_err(broca_module::move_files::store::failure)?;
                return Ok(Incoming {
                    bytes: newest,
                    end: metadata.0,
                    digest: metadata.1,
                    store: true,
                });
            }
            page = self
                .door
                .read(reference, offset, cut)
                .await
                .map_err(|error| door_failure(op, error))?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Door {
        bytes: Vec<u8>,
        calls: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl SessionDoor for Door {
        async fn durable_offset(&self, _: &str) -> Result<u64, SessionDoorError> {
            unreachable!()
        }
        async fn export(&self, _: &str, _: &[u8]) -> Result<(), SessionDoorError> {
            unreachable!()
        }
        async fn read(&self, _: &str, offset: u64, _: &str) -> Result<DoorRead, SessionDoorError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let end = (offset as usize + 64).min(self.bytes.len());
            Ok(DoorRead {
                bytes: self.bytes[offset as usize..end].to_vec(),
                from_offset: offset,
                end_offset: end as u64,
                stream_end: self.bytes.len() as u64,
                digest: broca_wal::migration::digest(&self.bytes),
            })
        }
    }
    fn framed(parts: &[&[u8]]) -> Vec<u8> {
        let mut stream = moves::STREAM.to_vec();
        for part in parts {
            stream.extend_from_slice(&(part.len() as u64).to_le_bytes());
            stream.extend_from_slice(&Sha256::digest(part));
            stream.extend_from_slice(part);
        }
        stream
    }
    struct NoRuns;
    #[async_trait::async_trait]
    impl broca_module::executor::RunExecutor for NoRuns {}

    fn driver(bytes: Vec<u8>) -> (Driver, Arc<Door>) {
        let door = Arc::new(Door {
            bytes,
            calls: AtomicUsize::new(0),
        });
        (
            Driver {
                root: "target/scratch".into(),
                // A registry with no new storage open.
                registry: broca_module::registry::LineageRegistry::new(Arc::new(
                    broca_module::opener::DefaultOpener::new(
                        Arc::new(broca_core::FileLeaseStore::new("target/scratch/leases")),
                        Arc::new(NoRuns),
                    ),
                )),
                wal: Arc::new(FileWalStore::new("target/scratch/wal")),
                index: Arc::new(broca_store::SqliteStore::open_in_memory().unwrap()),
                door: door.clone(),
            },
            door,
        )
    }
    #[tokio::test]
    async fn skips_old_exports_without_counting_them_against_newest_budget() {
        let old = vec![7; 4096];
        let (driver, door) = driver(framed(&[&old, b"last"]));
        let first = door.read("r", 0, "c").await.unwrap();
        let incoming = driver
            .incoming_store_bounded("r", "c", first, 8, Op::Import)
            .await
            .unwrap_or_else(|error| panic!("{:?}", error.refusal));
        assert_eq!(incoming.bytes, framed(&[b"last"]));
        assert_eq!(incoming.end, door.bytes.len() as u64);
        assert_eq!(incoming.digest, broca_wal::migration::digest(&door.bytes));
    }
    #[tokio::test]
    async fn newest_budget_refusal_names_size_before_reading_or_allocating_the_payload() {
        let (driver, door) = driver(framed(&[&vec![7; 4096]]));
        let first = door.read("r", 0, "c").await.unwrap();
        let error = match driver
            .incoming_store_bounded("r", "c", first, 8, Op::Import)
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("over-budget export accepted"),
        };
        let error = serde_json::to_value(error.refusal).unwrap();
        assert_eq!(
            error["detail"],
            json!({"reason":"move_import_budget_exceeded","export_size":4096,"budget":8})
        );
        assert_eq!(door.calls.load(Ordering::SeqCst), 1);
    }
}
