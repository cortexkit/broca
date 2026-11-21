//! Durable invalidation of rebuildable views before an overhaul cut. The fence
//! uses the existing checkpoint slot; immutable export facts are never touched.

use super::*;
use rusqlite::{OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};

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

impl SqliteStore {}
