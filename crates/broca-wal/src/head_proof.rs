//! visibility-late/v1: sync the log, publish, then check exposure time again.
//!
//! The sidecar write itself can stall after the pre-write freshness check. A
//! late exposure is always followed by publication of a newer frame: the live
//! writer appends VisibilityLate, syncs, and retries until the post-write check
//! is fresh. If it dies first, recovery's WriterOpened supplies that frame.
//! A surviving open-writer marker always requires reclaim or the next opener
//! to reopen the session, even if its log ends with a durable WriterClosed.
//! Retention of that marker can mean close publication did not finish; a crash
//! between durable close and unlink merely costs a redundant successor.
//!
//! Proof writes remain in-place and unsynced. Readers must handle an unavailable
//! proof as unavailable, and discovery/deletion must honor the marker/recovery
//! obligation rather than considering a late exposure a completed writer.

use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::{framing, fs_ops, Wal, WalError};
pub use broca_wal_reader::proof::{HeadProof, PublishedLog};

pub const VISIBILITY_LATE_FEATURE: &str = "visibility-late/v1";
pub const MAX_PROOF_AGE_MS: u64 = 59_000;

/// A completed barrier borrowing the actual WAL, not a stored watermark.
/// The borrow prevents an intervening append before the publisher scans it.
///
/// ```compile_fail
/// use broca_wal::head_proof::SyncedPrefix;
/// let prefix = SyncedPrefix { durable_seq: 42, durable_end: 8192 };
/// ```
#[must_use = "consume the synced prefix to scan the publication candidate"]
pub struct SyncedPrefix<'a> {
    wal: &'a dyn Wal,
}

impl<'a> SyncedPrefix<'a> {
    pub fn sync(wal: &'a dyn Wal) -> Result<Self, WalError> {
        wal.sync()?;
        Ok(Self { wal })
    }

    /// The witness is constructed only after the append's sync succeeds.
    pub fn append_v4(
        wal: &'a mut dyn Wal,
        fence: u64,
        record: &crate::WalRecord,
        prepared: crate::PreparedV4Frame,
        appended_ms: u64,
    ) -> Result<(u64, Self), WalError> {
        let seq = wal.append_v4(fence, record, true, prepared, appended_ms)?;
        Ok((seq, Self { wal }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationStatus {
    Published,
    /// The candidate was old before the write, or the write exposed it late.
    Late {
        through_seq: u64,
        oldest_appended_ms: u64,
    },
}

pub fn sidecar_path(log: &Path) -> PathBuf {
    let mut path = log.as_os_str().to_os_string();
    path.push(".head");
    path.into()
}

/// Transient scan state belongs to one leased writer. It is rebuilt from the log
/// at open, never resumed from an unvalidated stored watermark.
pub struct Publisher {
    path: PathBuf,
    lineage: [u8; 16],
    scanned_end: u64,
    newest: Option<HeadProof>,
    oldest_unpublished: Option<u64>,
}

impl Publisher {
    /// An empty file surviving a cut may not have a lineage until WriterOpened
    /// appends it. Construct the publisher from that completed barrier, not from
    /// assumptions about what survived the previous open.
    pub fn from_synced(synced: SyncedPrefix<'_>) -> Result<Self, WalError> {
        let wal = synced.wal;
        let path = wal.log_path().ok_or_else(|| {
            WalError::Io(io::Error::other("durable-head proof: WAL has no file path"))
        })?;
        let lineage = wal.lineage_id().ok_or_else(|| {
            WalError::Io(io::Error::other(
                "durable-head proof: synced WAL has no lineage",
            ))
        })?;
        let mut publisher = Self::new(path.to_path_buf(), lineage);
        publisher.scan_synced(synced)?;
        Ok(publisher)
    }

    pub fn new(path: PathBuf, lineage: crate::WalLineageId) -> Self {
        Self {
            path,
            lineage: lineage.as_bytes(),
            scanned_end: 0,
            newest: None,
            oldest_unpublished: None,
        }
    }

    /// The only operation that refreshes the candidate head first syncs the WAL.
    /// A successful sync is not itself permission to publish an old tail.
    pub fn sync(&mut self, wal: &dyn Wal) -> Result<(), WalError> {
        self.scan_synced(SyncedPrefix::sync(wal)?)
    }

    /// Consume proof of a completed lifecycle or ordinary barrier without
    /// redundantly syncing the same prefix a second time.
    pub fn scan_synced(&mut self, synced: SyncedPrefix<'_>) -> Result<(), WalError> {
        let wal = synced.wal;
        self.path = wal
            .log_path()
            .ok_or_else(|| {
                WalError::Io(io::Error::other("durable-head proof: WAL has no file path"))
            })?
            .to_path_buf();
        self.scan(wal.last_seq()).map_err(WalError::Io)
    }

    fn scan(&mut self, expected_seq: u64) -> io::Result<()> {
        let mut file = fs_ops::File::open(&self.path)?;
        file.seek(SeekFrom::Start(self.scanned_end))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let mut tail = bytes.as_slice();
        let previously_published = if self.scanned_end == 0 {
            // Validate the sidecar against the complete prefix before using it
            // to distinguish old published frames from the recovering batch.
            fs_ops::read(sidecar_path(&self.path))
                .ok()
                .and_then(|sidecar| {
                    PublishedLog::open(&bytes, &sidecar)
                        .ok()
                        .flatten()
                        .map(|log| log.head().durable_seq)
                })
                .unwrap_or(0)
        } else {
            self.newest.as_ref().map_or(0, |proof| proof.durable_seq)
        };
        if self.scanned_end == 0 {
            match framing::decode_next(tail) {
                framing::DecodeStep::Lineage {
                    lineage_id,
                    consumed,
                } if lineage_id.as_bytes() == self.lineage => {
                    self.scanned_end += consumed as u64;
                    tail = &tail[consumed..];
                }
                _ => {
                    return Err(io::Error::other(
                        "durable-head proof: missing or mismatched lineage",
                    ))
                }
            }
        }
        while !tail.is_empty() {
            if tail.get(4) == Some(&2) {
                match framing::decode_next(tail) {
                    framing::DecodeStep::Lineage {
                        lineage_id,
                        consumed,
                    } if lineage_id.as_bytes() == self.lineage => {
                        self.scanned_end += consumed as u64;
                        tail = &tail[consumed..];
                        continue;
                    }
                    _ => {
                        return Err(io::Error::other(
                            "durable-head proof: invalid or conflicting lineage metadata",
                        ))
                    }
                }
            }
            let header = broca_wal_reader::frame::decode_header(tail)
                .map_err(io::Error::other)?
                .ok_or_else(|| {
                    io::Error::other("durable-head proof: incomplete header after sync")
                })?;
            let previous = self.newest.as_ref().map_or(0, |proof| proof.durable_seq);
            if header.metadata.seq != previous + 1
                || !broca_wal_reader::frame::digest_verifies(tail)
            {
                return Err(io::Error::other(
                    "durable-head proof: noncontiguous or corrupt synced tail",
                ));
            }
            let consumed = header.frame_len();
            self.scanned_end += consumed as u64;
            if header.metadata.seq > previously_published {
                self.oldest_unpublished
                    .get_or_insert(header.metadata.appended_ms);
            }
            self.newest = Some(HeadProof {
                lineage_id: self.lineage,
                writer_id: header.metadata.writer_id,
                durable_seq: header.metadata.seq,
                durable_end: self.scanned_end,
                frame_identity: header.identity,
                appended_ms: header.metadata.appended_ms,
            });
            tail = &tail[consumed..];
        }
        if self.newest.as_ref().map_or(0, |proof| proof.durable_seq) != expected_seq {
            return Err(io::Error::other(
                "durable-head proof: synced head does not match writer",
            ));
        }
        Ok(())
    }

    /// Returns the late batch that needs a newer visibility_late frame. The
    /// timestamp is sampled after sync, including any time spent stalled there.
    pub fn late(&self, stamp_now: u64) -> Option<(u64, u64)> {
        let proof = self.newest.as_ref()?;
        (proof.appended_ms < stamp_now.saturating_sub(MAX_PROOF_AGE_MS)).then_some((
            proof.durable_seq,
            self.oldest_unpublished.unwrap_or(proof.appended_ms),
        ))
    }

    /// A post-write clock sample detects delays in open/truncate/write as well
    /// as slow fsyncs. Late requires a newer frame and another publication, not
    /// deletion of a proof readers may already have observed.
    pub fn publish(
        &mut self,
        mut stamp_now: impl FnMut() -> u64,
    ) -> Result<PublicationStatus, WalError> {
        if let Some((through_seq, oldest_appended_ms)) = self.late(stamp_now()) {
            return Ok(PublicationStatus::Late {
                through_seq,
                oldest_appended_ms,
            });
        }
        let Some(proof) = &self.newest else {
            return Ok(PublicationStatus::Published);
        };
        {
            let mut options = fs_ops::OpenOptions::new();
            options.write(true).create(true).truncate(true);
            let mut file = options
                .open(sidecar_path(&self.path))
                .map_err(WalError::Io)?;
            file.write_all(&proof.encode()).map_err(WalError::Io)?;
        }
        if let Some((through_seq, oldest_appended_ms)) = self.late(stamp_now()) {
            return Ok(PublicationStatus::Late {
                through_seq,
                oldest_appended_ms,
            });
        }
        self.oldest_unpublished = None;
        Ok(PublicationStatus::Published)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FileWal, PreparedV4Frame, WalConfig, WalRecord};

    #[test]
    fn proof_age_is_inclusive_at_epsilon_minus_one_second() {
        let root =
            broca_test_support::scratch_root().join(format!("proof-age-{}", std::process::id()));
        fs_ops::create_dir_all(&root).unwrap();
        let path = root.join("log.wal");
        let mut wal = FileWal::open(&path)
            .unwrap()
            .with_feature_floor(root.join(crate::FEATURE_FLOOR_FILENAME))
            .with_wal_config(WalConfig::OVERHAUL)
            .unwrap();
        let record = WalRecord::VisibilityLate {
            through_seq: 0,
            oldest_appended_ms: 1,
        };
        wal.append_v4(
            1,
            &record,
            false,
            PreparedV4Frame::prepare(&record).unwrap(),
            1000,
        )
        .unwrap();
        let mut head = Publisher::new(path.clone(), wal.lineage_id().unwrap());
        assert!(!sidecar_path(&path).exists());
        head.sync(&wal).unwrap();
        assert_eq!(head.late(60_000), None);
        assert_eq!(head.late(60_001), Some((1, 1000)));
        assert_eq!(
            head.publish(|| 60_001).unwrap(),
            PublicationStatus::Late {
                through_seq: 1,
                oldest_appended_ms: 1000
            }
        );
        assert!(!sidecar_path(&path).exists());
        assert_eq!(
            head.publish(|| 60_000).unwrap(),
            PublicationStatus::Published
        );
        let bytes = fs_ops::read(&path).unwrap();
        let sidecar = fs_ops::read(sidecar_path(&path)).unwrap();
        assert_eq!(
            PublishedLog::open(&bytes, &sidecar)
                .unwrap()
                .unwrap()
                .head()
                .durable_seq,
            1
        );
        drop(wal);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn empty_existing_overhaul_initializes_a_proof_covered_lineage() {
        let root = broca_test_support::scratch_root()
            .join(format!("proof-empty-overhaul-{}", std::process::id()));
        fs_ops::create_dir_all(&root).unwrap();
        let path = root.join("log.wal");
        fs_ops::File::create(&path).unwrap();
        let mut wal = FileWal::open(&path)
            .unwrap()
            .with_feature_floor(root.join(crate::FEATURE_FLOOR_FILENAME))
            .with_wal_config(WalConfig::OVERHAUL)
            .unwrap()
            .with_append_budget(1_000_000);
        assert!(wal.lineage_id().is_none());
        let metadata = crate::framing::LINEAGE_FRAME_LEN as u64;
        assert_eq!(wal.append_budget().unwrap().1, 0);
        assert_eq!(wal.append_budget().unwrap().2, 2 * metadata);
        let writer_id = wal.writer_id().unwrap();
        let record = WalRecord::WriterOpened {
            writer_id,
            previous_writer_id: None,
            recovered_through_seq: 0,
            cause: "new".into(),
        };
        let prepared = PreparedV4Frame::prepare(&record).unwrap();
        let frame_len = prepared.frame_len() as u64;
        let (seq, synced) = SyncedPrefix::append_v4(&mut wal, 1, &record, prepared, 1000).unwrap();
        let mut publisher = Publisher::from_synced(synced).unwrap();
        assert_eq!(seq, 1);
        assert_eq!(wal.append_budget().unwrap().1, frame_len + 2 * metadata);
        assert_eq!(wal.append_budget().unwrap().2, metadata);
        let close_len = PreparedV4Frame::prepare(&WalRecord::WriterClosed { writer_id })
            .unwrap()
            .frame_len() as u64;
        assert_eq!(
            wal.writer_close_reserve(),
            close_len + metadata,
            "the next close needs only trailing lineage, not the initialization prefix"
        );
        assert_eq!(
            publisher.publish(|| 1000).unwrap(),
            PublicationStatus::Published
        );
        let bytes = fs_ops::read(&path).unwrap();
        assert!(
            matches!(crate::framing::decode_next(&bytes), crate::framing::DecodeStep::Lineage { consumed, .. } if consumed as u64 == metadata)
        );
        let sidecar = fs_ops::read(sidecar_path(&path)).unwrap();
        let bounded = PublishedLog::open(&bytes, &sidecar).unwrap().unwrap();
        assert_eq!(bounded.head().durable_seq, 1);
        assert_eq!(bounded.head().durable_end, metadata + frame_len);
        drop(wal);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn empty_existing_legacy_keeps_record_then_trailing_lineage() {
        let root = broca_test_support::scratch_root()
            .join(format!("proof-empty-legacy-{}", std::process::id()));
        fs_ops::create_dir_all(&root).unwrap();
        let path = root.join("log.wal");
        fs_ops::File::create(&path).unwrap();
        let mut wal = FileWal::open(&path).unwrap();
        wal.append(
            1,
            &WalRecord::TurnFinished {
                step_id: broca_protocol::StepId(1),
            },
            true,
        )
        .unwrap();
        let bytes = fs_ops::read(&path).unwrap();
        let crate::framing::DecodeStep::Record { consumed, .. } =
            crate::framing::decode_next(&bytes)
        else {
            panic!("legacy first append still starts with a record")
        };
        assert!(
            matches!(crate::framing::decode_next(&bytes[consumed..]), crate::framing::DecodeStep::Lineage { consumed: tail, .. } if consumed + tail == bytes.len())
        );
        assert!(!sidecar_path(&path).exists());
        drop(wal);
        std::fs::remove_dir_all(root).unwrap();
    }
}
