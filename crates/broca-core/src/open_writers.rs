//! The writer-id/v1 durability protocol: a durable marker precedes open, and close precedes unlink.

use broca_wal::{fs_ops, Wal, WalError, WalRecord};
use std::path::PathBuf;

pub(crate) struct WriterContext {
    pub(crate) root: PathBuf,
    pub(crate) address: String,
}

pub(crate) struct OpenWriter {
    marker: PathBuf,
    writer_id: [u8; 16],
}

impl WriterContext {
    pub(crate) fn open(
        self,
        wal: &mut dyn Wal,
        fence: u64,
        now: impl Fn() -> u64,
    ) -> Result<(OpenWriter, u64, WalRecord), WalError> {
        let writer_id = wal.writer_id().ok_or_else(|| {
            WalError::Io(std::io::Error::other(
                "writer-id/v1: overhaul WAL has no writer identity",
            ))
        })?;
        let directory = self.root.join("open-writers");
        fs_ops::create_dir_all(&directory).map_err(WalError::Io)?;
        // Sync the parent even when the directory already exists: a prior failed open may have
        // created it without making its name durable.
        fs_ops::sync_dir(&self.root).map_err(WalError::Io)?;
        let id: String = writer_id.iter().map(|byte| format!("{byte:02x}")).collect();
        let marker = directory.join(format!("{}.{id}", self.address));
        let mut options = fs_ops::OpenOptions::new();
        options.write(true).create_new(true);
        let file = options.open(&marker).map_err(WalError::Io)?;
        file.sync_all().map_err(WalError::Io)?;
        fs_ops::sync_dir(&directory).map_err(WalError::Io)?;

        // Recovery is already complete, but its surviving prefix must be synced before it is
        // advertised as durable. Read the log itself, never a stored sequence watermark.
        wal.sync()?;
        let previous_writer_id = wal
            .replay()?
            .iter()
            .rev()
            .find_map(|frame| match frame.record {
                WalRecord::WriterOpened { writer_id, .. } => Some(writer_id),
                _ => None,
            });
        let opened = WalRecord::WriterOpened {
            writer_id,
            previous_writer_id,
            recovered_through_seq: wal.last_seq(),
            cause: if previous_writer_id.is_some() {
                "reopen"
            } else {
                "new"
            }
            .into(),
        };
        let seq = append_barrier(wal, fence, &opened, now())?;
        Ok((OpenWriter { marker, writer_id }, seq, opened))
    }
}

impl OpenWriter {
    pub(crate) fn close(
        &self,
        wal: &mut dyn Wal,
        fence: u64,
        now: u64,
    ) -> Result<(u64, WalRecord), WalError> {
        let record = WalRecord::WriterClosed {
            writer_id: self.writer_id,
        };
        let seq = append_barrier(wal, fence, &record, now)?;
        Ok((seq, record))
    }

    pub(crate) fn unlink(self) -> Result<(), WalError> {
        // No directory sync is needed after unlink. A resurrected marker is conservative:
        // the durable WriterClosed still proves that this writer cannot append again.
        fs_ops::remove_file(&self.marker).map_err(WalError::Io)?;
        Ok(())
    }
}

fn append_barrier(
    wal: &mut dyn Wal,
    fence: u64,
    record: &WalRecord,
    now: u64,
) -> Result<u64, WalError> {
    let prepared = broca_wal::PreparedV4Frame::prepare(record).map_err(WalError::Frame)?;
    let expected = wal.last_seq() + 1;
    let seq = wal.append_v4(fence, record, true, prepared, now)?;
    if seq != expected {
        return Err(WalError::Io(std::io::Error::other(format!(
            "writer-id/v1: lifecycle sequence drift: expected {expected}, assigned {seq}"
        ))));
    }
    Ok(seq)
}
