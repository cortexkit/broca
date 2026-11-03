//! The production transcript projection materializes neutral entries once.
//! Required written_at values stay solely in the native message-time column.

use std::collections::BTreeMap;

use broca_protocol::Message;
use broca_wal::FramedRecord;
use cortexkit_role_compaction::read::Cursor;

use super::{
    entries::compact,
    metadata::{self, DeliveryUpdate, Fold},
};

pub fn configured(mode3: bool, ownership: Option<&crate::SessionOwnershipV1>) -> bool {
    mode3
        && ownership
            .and_then(|ownership| {
                ownership
                    .latest_manifest
                    .as_deref()
                    .or_else(|| ownership.first_episode.as_ref()?.manifest.as_deref())
            })
            .is_some_and(crate::compaction::has_compaction)
}

pub struct Prepared {
    pub continuation: Fold,
    pub newest: Option<Cursor>,
    pub full: Vec<Option<String>>,
    pub compact: Vec<Option<String>>,
    pub tags: Vec<Option<String>>,
    pub deliveries: Vec<DeliveryUpdate>,
}

pub fn prepare(
    mut continuation: Fold,
    previous_newest: Option<Cursor>,
    frames: &[FramedRecord],
    first_new: u64,
    messages: &mut [Message],
    mut mid: impl FnMut(u64) -> Result<String, String>,
) -> Result<Prepared, String> {
    let mut rows = BTreeMap::new();
    let mut deliveries = Vec::new();
    for frame in frames {
        let delta = continuation.observe(frame)?;
        rows.extend(delta.rows);
        deliveries.extend(delta.deliveries);
    }
    let mut prepared = Prepared {
        continuation,
        newest: previous_newest.filter(|cursor| cursor.ordinal <= first_new),
        full: Vec::new(),
        compact: Vec::new(),
        tags: Vec::new(),
        deliveries: deliveries
            .into_iter()
            .filter(|delivery| delivery.ordinal < first_new)
            .collect(),
    };
    for (index, message) in messages.iter_mut().enumerate() {
        let position = first_new + index as u64;
        let metadata = rows
            .get(&position)
            .ok_or_else(|| format!("compaction metadata missing for native message {position}"))?;
        metadata::attach_native_tags(message, metadata)?;
        let entry = metadata::project_entry(
            message,
            metadata,
            position,
            &mid(position)?,
            0,
            metadata::source_frames(metadata, frames)?,
        )?;
        prepared.newest = Some(Cursor::new(entry.ordinal, &entry.mid, &entry.digest));
        prepared.full.push(Some(
            serde_json::to_string(&entry).map_err(|error| error.to_string())?,
        ));
        prepared.compact.push(Some(
            serde_json::to_string(&compact(&entry)).map_err(|error| error.to_string())?,
        ));
        prepared.tags.push(
            message
                .part_tags()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|error| error.to_string())?,
        );
    }
    Ok(prepared)
}
