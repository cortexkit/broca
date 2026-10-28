//! visibility-late/v1: sync the log, then publish only behind a recent frame.

use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::{framing, fs_ops, Wal, WalError};
pub use broca_wal_reader::proof::{HeadProof, PublishedLog};

pub const VISIBILITY_LATE_FEATURE: &str = "visibility-late/v1";
pub const MAX_PROOF_AGE_MS: u64 = 59_000;

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
        wal.sync()?;
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
                    PublishedLog::open(&bytes, &sidecar).map(|log| log.head().durable_seq)
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

    /// Rechecks freshness at the write boundary. A stale proof is left untouched.
    /// There is deliberately no sidecar fsync: stale/torn/missing proofs fail closed.
    pub fn publish(&mut self, stamp_now: u64) -> Result<bool, WalError> {
        if self.late(stamp_now).is_some() {
            return Ok(false);
        }
        let Some(proof) = &self.newest else {
            return Ok(true);
        };
        let mut options = fs_ops::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        let mut file = options
            .open(sidecar_path(&self.path))
            .map_err(WalError::Io)?;
        file.write_all(&proof.encode()).map_err(WalError::Io)?;
        self.oldest_unpublished = None;
        Ok(true)
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
        assert!(!head.publish(60_001).unwrap());
        assert!(!sidecar_path(&path).exists());
        assert!(head.publish(60_000).unwrap());
        let bytes = fs_ops::read(&path).unwrap();
        let sidecar = fs_ops::read(sidecar_path(&path)).unwrap();
        assert_eq!(
            PublishedLog::open(&bytes, &sidecar)
                .unwrap()
                .head()
                .durable_seq,
            1
        );
        drop(wal);
        std::fs::remove_dir_all(root).unwrap();
    }
}
