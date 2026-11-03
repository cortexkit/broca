//! Neutral pages use their own full/compact columns. A compact read never loads
//! tool output or native message payloads just to discard them afterwards.
//! Cached envelopes keep written_at and a present completed_at at zero; their
//! authoritative value is the native written_at_ms column in the same transaction.

use broca_core::{
    compaction_v2::{
        entries::compact,
        metadata::{apply_delivery, DeliveryUpdate},
        protocol::read::{Detail, Entry, ReadRequest, Select},
    },
    context::CompactionStorePage,
};
use rusqlite::{params, OptionalExtension};

use super::{current_projection_position, wal_anchor, BindIdentity, SqliteStore, StoreError};

pub(super) fn apply_deliveries(
    tx: &rusqlite::Transaction<'_>,
    key: &str,
    updates: Vec<DeliveryUpdate>,
) -> Result<(), StoreError> {
    for update in updates {
        let json: Option<String> = tx
            .query_row(
                "SELECT compaction_json FROM message WHERE session=?1 AND ord=?2",
                params![key, i64::try_from(update.ordinal).map_err(StoreError::Int)?],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        let json = json.ok_or(StoreError::InconsistentSnapshot(
            "compaction delivery has no materialized entry",
        ))?;
        let mut entry: Entry = serde_json::from_str(&json)?;
        apply_delivery(&mut entry, &update.part, update.delivered).map_err(|_| {
            StoreError::InconsistentSnapshot(
                "compaction delivery does not match the materialized tool part",
            )
        })?;
        tx.execute("UPDATE message SET compaction_json=?3,compaction_compact_json=?4 WHERE session=?1 AND ord=?2", params![key, i64::try_from(update.ordinal).map_err(StoreError::Int)?, serde_json::to_string(&entry)?, serde_json::to_string(&compact(&entry))?])?;
    }
    Ok(())
}

pub(super) fn read_page(
    store: &SqliteStore,
    session: &BindIdentity,
    request: &ReadRequest,
) -> Result<CompactionStorePage, String> {
    let key = SqliteStore::key(session);
    let conn = store.conn.lock().unwrap();
    let tx = conn
        .unchecked_transaction()
        .map_err(|error| error.to_string())?;
    let (watermark_seq, prefix_len) =
        current_projection_position(&tx, &key).map_err(|error| error.to_string())?;
    let prefix_len = u64::try_from(prefix_len).map_err(|error| error.to_string())?;
    let metadata = tx.query_row(
        "SELECT head_json,lineage_id,wal_anchor_file_id,wal_anchor_seq,wal_anchor_byte_end FROM meta WHERE session=?1",
        [&key], |row| Ok((row.get::<_,Option<String>>(0)?, row.get::<_,Option<String>>(1)?, row.get::<_,Option<String>>(2)?, row.get::<_,Option<i64>>(3)?, row.get::<_,Option<i64>>(4)?)),
    ).optional().map_err(|error| error.to_string())?;
    let (head, lineage_id, anchor) = match metadata {
        Some((head, lineage, file, seq, end)) => (
            head.as_deref()
                .map(serde_json::from_str::<Option<broca_core::head::HeadProjection>>)
                .transpose()
                .map_err(|error| error.to_string())?
                .flatten(),
            lineage,
            wal_anchor(watermark_seq, file, seq, end),
        ),
        None => (None, None, None),
    };
    let (predicate, selection, limit) = match &request.select {
        Select::After { after_ordinal, limit } => {
            if *limit == Some(0) { return Err("compaction entries limit must be at least 1".into()); }
            ("ord>=CAST(?2 AS INTEGER)", after_ordinal.unwrap_or(0).to_string(), limit.unwrap_or(1000))
        }
        Select::Mids { mids } => ("compaction_compact_json IS NOT NULL AND json_extract(compaction_compact_json,'$.mid') IN (SELECT value FROM json_each(?2))", serde_json::to_string(mids).map_err(|error| error.to_string())?, u64::MAX),
        Select::Tags { tags } => ("ord IN (SELECT ord FROM message_tag WHERE session=?1 AND tag IN (SELECT value FROM json_each(?2)))", serde_json::to_string(tags).map_err(|error| error.to_string())?, u64::MAX),
        _ => return Err("unsupported compaction entry selection".into()),
    };
    let column = if request.detail == Detail::Compact {
        "compaction_compact_json"
    } else {
        "compaction_json"
    };
    let sql = format!("SELECT ord,octet_length({column}),written_at_ms FROM message WHERE session=?1 AND ord<?3 AND {predicate} ORDER BY ord ASC");
    let mut statement = tx.prepare(&sql).map_err(|error| error.to_string())?;
    let rows = statement
        .query_map(
            params![
                key,
                selection,
                i64::try_from(prefix_len).map_err(|error| error.to_string())?
            ],
            |row| {
                Ok((
                    row.get::<_, u64>(0)?,
                    row.get::<_, Option<u64>>(1)?,
                    row.get::<_, Option<u64>>(2)?,
                ))
            },
        )
        .map_err(|error| error.to_string())?;
    let mut entries = Vec::new();
    let mut bytes = 2;
    let mut has_more = false;
    for row in rows {
        let (ordinal, size, written_at) = row.map_err(|error| error.to_string())?;
        let size = size.ok_or("compaction entry projection is not ready")?;
        let written_at =
            written_at.ok_or("compaction entry is missing its projected written_at_ms")?;
        let size = size + written_at.to_string().len() as u64 - 1;
        if !entries.is_empty()
            && (entries.len() as u64 >= limit
                || bytes + size + 1 > request.max_bytes.unwrap_or(1_048_576))
        {
            has_more = true;
            break;
        }
        let json: String = tx
            .query_row(
                &format!("SELECT {column} FROM message WHERE session=?1 AND ord=?2"),
                params![
                    key,
                    i64::try_from(ordinal).map_err(|error| error.to_string())?
                ],
                |row| row.get(0),
            )
            .map_err(|error| error.to_string())?;
        let mut entry: Entry = serde_json::from_str(&json).map_err(|error| error.to_string())?;
        if entry.ordinal != ordinal + 1 {
            return Err("compaction entry ordinal disagrees with its native row".into());
        }
        if entry.written_at != 0 {
            return Err("compaction cached envelope contains a second written_at value".into());
        }
        entry.written_at = written_at;
        if let Some(completed_at) = entry.completed_at {
            if completed_at != 0 {
                return Err(
                    "compaction cached envelope contains a second completed_at value".into(),
                );
            }
            entry.completed_at = broca_core::message_times::completed_at(
                broca_protocol::Role::Assistant,
                Some(written_at),
                true,
            );
        }
        bytes += size + u64::from(!entries.is_empty());
        entries.push(entry);
    }
    drop(statement);
    tx.commit().map_err(|error| error.to_string())?;
    Ok(CompactionStorePage {
        entries,
        head,
        watermark_seq,
        lineage_id,
        anchor,
        prefix_len,
        has_more,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_core::compaction_v2::protocol::{
        part::{Delivered, Part, PartBody, ToolPart, ToolStatus},
        read::Role,
    };
    use broca_core::MaterializedStore;
    use serde_json::json;

    fn entry(position: u64) -> Entry {
        Entry::new(
            position + 1,
            format!("m12345678.{position}"),
            Role::Assistant,
            0,
            vec![Part::new(
                "0",
                PartBody::Tool(
                    ToolPart::new(
                        format!("call-{position}"),
                        "read",
                        json!({}),
                        ToolStatus::Completed,
                    )
                    .with_output("original output")
                    .with_output_bytes(15),
                ),
            )
            .with_tag(position + 1)],
        )
    }

    fn fixture(count: u64) -> (SqliteStore, BindIdentity) {
        let store = SqliteStore::open_in_memory().unwrap();
        let session = BindIdentity::new("/synthetic/compaction-page", "runner", "session");
        let key = SqliteStore::key(&session);
        {
            let conn = store.conn.lock().unwrap();
            conn.execute("INSERT INTO meta(session,watermark_seq,context_epoch,prefix_len,head_json) VALUES(?1,5,0,?2,'null')", params![key, count as i64]).unwrap();
            for position in 0..count {
                let entry = entry(position);
                conn.execute("INSERT INTO message(session,ord,json,compaction_json,compaction_compact_json,written_at_ms) VALUES(?1,?2,'not-native-json',?3,?4,42)", params![key, position as i64, serde_json::to_string(&entry).unwrap(), serde_json::to_string(&compact(&entry)).unwrap()]).unwrap();
                conn.execute(
                    "INSERT INTO message_tag(session,tag,ord,part) VALUES(?1,?2,?3,0)",
                    params![key, position as i64 + 1, position as i64],
                )
                .unwrap();
            }
        }
        (store, session)
    }

    #[test]
    fn compact_entry_page_never_loads_native_or_full_payloads_and_uses_the_time_column() {
        let (store, session) = fixture(1);
        let key = SqliteStore::key(&session);
        store
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE message SET compaction_json='not-full-json' WHERE session=?1",
                [&key],
            )
            .unwrap();
        let before = store.conn.lock().unwrap().total_changes();
        let request = ReadRequest::new(
            Select::After {
                after_ordinal: None,
                limit: Some(1),
            },
            Detail::Compact,
        );
        let page = store
            .read_compaction_page(&session, &request)
            .unwrap()
            .unwrap();
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].written_at, 42);
        assert!(
            matches!(&page.entries[0].parts[0].body, PartBody::Tool(tool) if tool.output.is_none())
        );
        assert_eq!(store.conn.lock().unwrap().total_changes(), before);
        let full = ReadRequest::new(
            Select::After {
                after_ordinal: None,
                limit: Some(1),
            },
            Detail::Full,
        );
        assert!(store.read_compaction_page(&session, &full).is_err());
    }

    #[test]
    fn entry_byte_budget_stops_before_decoding_the_next_full_entry() {
        let (store, session) = fixture(2);
        let key = SqliteStore::key(&session);
        store
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE message SET compaction_json='not-full-json' WHERE session=?1 AND ord=1",
                [&key],
            )
            .unwrap();
        let request = ReadRequest::new(
            Select::After {
                after_ordinal: None,
                limit: None,
            },
            Detail::Full,
        )
        .with_max_bytes(1);
        let page = store
            .read_compaction_page(&session, &request)
            .unwrap()
            .unwrap();
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].ordinal, 1);
        assert!(page.has_more);
    }

    #[test]
    fn entry_selectors_use_stored_mid_and_tag_indexes() {
        let (store, session) = fixture(2);
        let mids = ReadRequest::new(
            Select::Mids {
                mids: vec!["m12345678.1".into(), "foreign-mid".into()],
            },
            Detail::Compact,
        );
        let page = store
            .read_compaction_page(&session, &mids)
            .unwrap()
            .unwrap();
        assert_eq!(
            page.entries
                .iter()
                .map(|entry| entry.ordinal)
                .collect::<Vec<_>>(),
            [2]
        );
        let tags = ReadRequest::new(
            Select::Tags {
                tags: vec![2, 1, 2],
            },
            Detail::Compact,
        );
        let page = store
            .read_compaction_page(&session, &tags)
            .unwrap()
            .unwrap();
        assert_eq!(
            page.entries
                .iter()
                .map(|entry| entry.ordinal)
                .collect::<Vec<_>>(),
            [1, 2]
        );
    }

    #[test]
    fn a_delivery_updates_both_cached_forms_without_changing_the_output_or_native_row() {
        let (store, session) = fixture(1);
        let key = SqliteStore::key(&session);
        let before = entry(0).digest;
        {
            let conn = store.conn.lock().unwrap();
            let tx = conn.unchecked_transaction().unwrap();
            apply_deliveries(
                &tx,
                &key,
                vec![DeliveryUpdate {
                    ordinal: 0,
                    part: "0".into(),
                    delivered: Delivered::new("broca", "remember"),
                }],
            )
            .unwrap();
            tx.commit().unwrap();
            assert_eq!(
                conn.query_row("SELECT json FROM message WHERE session=?1", [&key], |row| {
                    row.get::<_, String>(0)
                })
                .unwrap(),
                "not-native-json"
            );
        }
        let request = ReadRequest::new(
            Select::After {
                after_ordinal: None,
                limit: None,
            },
            Detail::Full,
        );
        let page = store
            .read_compaction_page(&session, &request)
            .unwrap()
            .unwrap();
        let entry = &page.entries[0];
        assert_ne!(entry.digest, before);
        assert!(
            matches!(&entry.parts[0].body, PartBody::Tool(tool) if tool.output.as_deref() == Some("original output") && tool.delivered[0].text.as_deref() == Some("remember"))
        );
        let request = ReadRequest::new(
            Select::After {
                after_ordinal: None,
                limit: None,
            },
            Detail::Compact,
        );
        let page = store
            .read_compaction_page(&session, &request)
            .unwrap()
            .unwrap();
        assert_eq!(page.entries[0].digest, entry.digest);
        assert!(
            matches!(&page.entries[0].parts[0].body, PartBody::Tool(tool) if tool.delivered[0].text.is_none() && tool.delivered[0].text_bytes == 8)
        );
    }

    #[test]
    fn mode3_projection_materializes_entries_with_shared_ids_and_native_time_columns() {
        use broca_core::Persister;
        use broca_protocol::{ContentBlock, Message};
        use broca_wal::{FramedRecord, WalRangeRead, WalRecord};
        use std::sync::Arc;
        let store = SqliteStore::open_in_memory().unwrap();
        let session = BindIdentity::new("/synthetic/projected-compaction", "runner", "session");
        let mut start: WalRecord = serde_json::from_str(
            include_str!("../../broca-wal/tests/fixtures/compaction_v2_records.jsonl")
                .lines()
                .last()
                .unwrap(),
        )
        .unwrap();
        let plan = json!({"compaction_item":{"provider":"ck-mc","preset":"head","params":{}}});
        if let WalRecord::RunStarted {
            session: stored,
            config,
            input,
            manifest,
            compaction_plan_version,
            written_at_ms,
            ..
        } = &mut start
        {
            *stored = session.clone();
            config.global_system = None;
            *compaction_plan_version = Some(2);
            *written_at_ms = Some(10);
            *input = Arc::new(vec![Message::new(
                broca_protocol::Role::User,
                vec![ContentBlock::Text {
                    text: "question".into(),
                }],
            )]);
            *manifest = Some(Box::new(broca_wal::PlanManifest {
                rich_ui: None,
                declarations: Vec::new(),
                plan: broca_wal::OpaqueJson::from_json_text(plan.to_string()).unwrap(),
                plan_identity: "plan".into(),
                composition_digest: "composition".into(),
                fetches: Vec::new(),
                tools: Arc::new(Vec::new()),
                system_text: "instructions outside history".into(),
                system_digest: "system".into(),
                join_version: 1,
                role_versions: Default::default(),
            }));
        }
        let records = vec![start, serde_json::from_value(json!({"type":"step_started","step_id":0})).unwrap(), serde_json::from_value(json!({"type":"model_step_finished","step_id":0,"assistant_message":{"message_id":"a","content":[{"type":"text","text":"answer"}]},"usage":{},"written_at_ms":20})).unwrap(), serde_json::from_value(json!({"type":"turn_finished","step_id":0})).unwrap()];
        let frames = records
            .into_iter()
            .enumerate()
            .map(|(index, record)| FramedRecord {
                seq: index as u64 + 1,
                fence: 1,
                record,
            })
            .collect();
        let mut range = WalRangeRead::without_anchors(frames);
        let path = broca_test_support::scratch_root().join(format!(
            "compaction-entry-projection-{}.wal",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let wal = broca_wal::FileWal::open(&path).unwrap();
        range.lineage_id = broca_wal::Wal::lineage_id(&wal);
        let lineage = range.lineage_id.unwrap().to_string();
        store.project_range(&session, &range).unwrap();
        let request = ReadRequest::new(
            Select::After {
                after_ordinal: None,
                limit: None,
            },
            Detail::Full,
        );
        let page = store
            .read_compaction_page(&session, &request)
            .unwrap()
            .unwrap();
        assert_eq!(page.entries.len(), 2);
        assert_eq!(
            page.entries[0].mid,
            broca_core::message_mid(true, Some(&lineage), 0).unwrap()
        );
        assert_eq!(page.entries[0].written_at, 10);
        assert_eq!(page.entries[1].written_at, 20);
        assert_eq!(page.entries[1].completed_at, Some(20));
        assert_eq!(page.entries[0].parts[0].tag, Some(1));
        assert_eq!(page.entries[1].parts[0].tag, Some(2));
        assert!(page.head.unwrap().compaction_metadata.unwrap().is_current());
        drop(wal);
        std::fs::remove_file(path).unwrap();
    }
}
