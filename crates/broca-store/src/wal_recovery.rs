//! Durable invalidation of rebuildable views before an overhaul cut. The fence
//! uses the existing checkpoint slot; immutable export facts are never touched.

use super::*;
use rusqlite::{OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};

pub(super) const FENCE_PREFIX: &str = broca_core::admission::WAL_RECOVERY_FENCE_PREFIX;

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Fence {
    pub(super) pending: bool,
    cut_ids: Vec<String>,
}

fn decode_fence(text: &str) -> Result<Option<Fence>, StoreError> {
    let value: serde_json::Value = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(error) if text.contains("\"wal_recovery_fence\"") => return Err(error.into()),
        Err(_) => return Ok(None),
    };
    value
        .get("wal_recovery_fence")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(Into::into)
}

fn checkpoint(tx: &Transaction<'_>, key: &str) -> Result<Option<String>, StoreError> {
    Ok(tx
        .query_row(
            "SELECT admission_checkpoint_json FROM meta WHERE session=?1",
            [key],
            |row| row.get(0),
        )
        .optional()?
        .flatten())
}

pub(super) fn read_fence(tx: &Transaction<'_>, key: &str) -> Result<Option<Fence>, StoreError> {
    checkpoint(tx, key)?
        .as_deref()
        .map(decode_fence)
        .transpose()
        .map(Option::flatten)
}

pub(super) fn pending_diagnostics(
    tx: &Transaction<'_>,
) -> Result<Vec<crate::diagnostics::PendingRecovery>, String> {
    let mut statement = tx.prepare(
        "SELECT session, admission_checkpoint_json FROM meta WHERE admission_checkpoint_json LIKE ?1"
    ).map_err(|error| error.to_string())?;
    let rows = statement
        .query_map([format!("{FENCE_PREFIX}%")], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|error| error.to_string())?;
    let mut pending = Vec::new();
    for row in rows {
        let (key, checkpoint) = row.map_err(|error| error.to_string())?;
        let (cut_ids, evidence_unreadable) = match decode_fence(&checkpoint) {
            Ok(Some(fence)) if fence.pending => (fence.cut_ids, false),
            Ok(_) => continue,
            Err(_) => (Vec::new(), true),
        };
        let session = bind_identity_from_store_key(&key)
            .ok_or_else(|| format!("invalid session identity in pending WAL recovery: {key}"))?;
        pending.push(crate::diagnostics::PendingRecovery {
            session,
            cut_ids,
            evidence_unreadable,
        });
    }
    Ok(pending)
}

impl SqliteStore {}
