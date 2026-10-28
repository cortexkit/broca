//! The writer-id/v1 durability protocol: a durable marker precedes open, and close precedes unlink.
//!
//! Every surviving marker requires a successor once the session lease is free, even when
//! WriterClosed is durable: close publication may have been interrupted. The lease-gated
//! reclaim pass owns reopening those sessions and removing their old markers;
//! a normal open records succession but never deletes another writer's marker opportunistically.

use broca_protocol::BindIdentity;
use broca_wal::{fs_ops, head_proof::SyncedPrefix, Wal, WalError, WalRecord};
use std::io::Write;
use std::path::PathBuf;

/// Decode the marker's lease identity, including for a writer that never admitted a run.
/// Callers must also compare its session address with the marker filename before acquiring it.
pub fn decode_writer_marker(bytes: &[u8]) -> Result<BindIdentity, String> {
    let marker: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    let object = marker.as_object().ok_or("writer marker is not an object")?;
    if object.len() != 2 || marker.get("version").and_then(|value| value.as_u64()) != Some(1) {
        return Err("unsupported writer marker version or fields".into());
    }
    let identity = marker
        .get("identity")
        .ok_or("writer marker has no identity")?;
    if identity.as_object().is_none_or(|object| object.len() != 3) {
        return Err("writer marker identity must contain exactly the bind triple".into());
    }
    let identity: BindIdentity =
        serde_json::from_value(identity.clone()).map_err(|error| error.to_string())?;
    identity.validate_keyable()?;
    Ok(identity)
}

pub(crate) struct WriterContext {
    pub(crate) root: PathBuf,
    pub(crate) address: String,
    pub(crate) identity: BindIdentity,
    pub(crate) cause: Option<&'static str>,
}

pub(crate) struct OpenWriter {
    marker: PathBuf,
    writer_id: [u8; 16],
    identity: BindIdentity,
}

impl WriterContext {
    pub(crate) fn open<'a>(
        self,
        wal: &'a mut dyn Wal,
        fence: u64,
        now: impl Fn() -> u64,
    ) -> Result<(OpenWriter, u64, WalRecord, SyncedPrefix<'a>), WalError> {
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
        let temporary = marker.with_extension(format!("{id}.tmp"));
        let identity = self.identity;
        let bytes = serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "identity": &identity,
        }))
        .map_err(|error| WalError::Io(std::io::Error::other(error)))?;
        let mut options = fs_ops::OpenOptions::new();
        options.write(true).create_new(true);
        let mut file = options.open(&temporary).map_err(WalError::Io)?;
        file.write_all(&bytes).map_err(WalError::Io)?;
        file.sync_all().map_err(WalError::Io)?;
        drop(file);
        fs_ops::rename(&temporary, &marker).map_err(WalError::Io)?;
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
            cause: self
                .cause
                .unwrap_or(if previous_writer_id.is_some() {
                    "reopen"
                } else {
                    "new"
                })
                .into(),
        };
        let (seq, synced) = append_barrier(wal, fence, &opened, now())?;
        Ok((
            OpenWriter {
                marker,
                writer_id,
                identity,
            },
            seq,
            opened,
            synced,
        ))
    }
}

impl OpenWriter {
    pub(crate) fn close<'a>(
        &self,
        wal: &'a mut dyn Wal,
        fence: u64,
        now: u64,
    ) -> Result<(u64, WalRecord, SyncedPrefix<'a>), WalError> {
        let record = WalRecord::WriterClosed {
            writer_id: self.writer_id,
        };
        let (seq, synced) = append_barrier(wal, fence, &record, now)?;
        Ok((seq, record, synced))
    }

    pub(crate) fn unlink(self) -> Result<(PathBuf, BindIdentity), WalError> {
        // No directory sync is needed after unlink. A resurrected marker costs a
        // redundant successor; the durable close still forbids this writer from appending.
        fs_ops::remove_file(&self.marker).map_err(WalError::Io)?;
        Ok((self.marker, self.identity))
    }
}

fn append_barrier<'a>(
    wal: &'a mut dyn Wal,
    fence: u64,
    record: &WalRecord,
    now: u64,
) -> Result<(u64, SyncedPrefix<'a>), WalError> {
    let prepared = broca_wal::PreparedV4Frame::prepare(record).map_err(WalError::Frame)?;
    let expected = wal.last_seq() + 1;
    let (seq, synced) = SyncedPrefix::append_v4(wal, fence, record, prepared, now)?;
    if seq != expected {
        return Err(WalError::Io(std::io::Error::other(format!(
            "writer-id/v1: lifecycle sequence drift: expected {expected}, assigned {seq}"
        ))));
    }
    Ok((seq, synced))
}
