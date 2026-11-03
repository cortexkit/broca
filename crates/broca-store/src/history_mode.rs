//! Upgrade the small first-run mode proof without rebuilding any legacy transcript row.

use rusqlite::{params, Connection, OptionalExtension};

pub(crate) fn cache(conn: &mut Connection, key: &str, mode3: bool) -> Result<(), String> {
    let tx = conn.transaction().map_err(|error| error.to_string())?;
    let values: Option<(Option<String>, Option<String>)> = tx
        .query_row(
            "SELECT head_json, admission_checkpoint_json FROM meta WHERE session=?1",
            [key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|error| error.to_string())?;
    if let Some((head, checkpoint)) = values {
        if let Some(head) = head {
            let mut head: Option<broca_core::head::HeadProjection> =
                serde_json::from_str(&head).map_err(|error| error.to_string())?;
            if let Some(head) = &mut head {
                if head.mode3.is_none() {
                    head.mode3 = Some(mode3);
                }
            }
            let encoded = serde_json::to_string(&head).map_err(|error| error.to_string())?;
            tx.execute(
                "UPDATE meta SET head_json=?2 WHERE session=?1",
                params![key, encoded],
            )
            .map_err(|error| error.to_string())?;
        }
        if let Some(mut checkpoint) = checkpoint
            .as_deref()
            .and_then(broca_core::decode_admission_checkpoint)
        {
            if let Some(head) = &mut checkpoint.head {
                if head.mode3.is_none() {
                    head.mode3 = Some(mode3);
                }
            }
            let encoded = broca_core::encode_admission_checkpoint(&checkpoint)
                .map_err(|error| error.to_string())?;
            tx.execute(
                "UPDATE meta SET admission_checkpoint_json=?2 WHERE session=?1",
                params![key, encoded],
            )
            .map_err(|error| error.to_string())?;
        }
    }
    tx.commit().map_err(|error| error.to_string())
}
