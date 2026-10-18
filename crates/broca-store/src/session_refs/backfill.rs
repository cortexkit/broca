//! Resumable, paged backfill. Reads/JSON decoding never own the write lock.
use super::{
    batches::{write_batch, MAX_BATCH_ITEMS},
    init_schema_measured, insert, BackfillCounts,
};
use crate::StoreError;
use broca_protocol::BindIdentity;
use rusqlite::{Connection, OptionalExtension};

pub(super) fn run(conn: &Connection) -> Result<BackfillCounts, StoreError> {
    run_limited(conn, None)
}

pub(super) fn run_limited(
    conn: &Connection,
    limit: Option<usize>,
) -> Result<BackfillCounts, StoreError> {
    let mut counts = BackfillCounts::default();
    init_schema_measured(conn, &mut counts.writes)?;
    let done: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM agent_data_projection WHERE id=1)",
        [],
        |row| row.get(0),
    )?;
    if !done {
        counts.rows = conn.query_row("SELECT count(*) FROM run_index", [], |row| row.get(0))?;
        counts.distinct_sessions =
            conn.query_row("SELECT count(DISTINCT session) FROM run_index", [], |row| {
                row.get(0)
            })?;
        let mut cursor: Option<String> = conn
            .query_row(
                "SELECT last_run_id FROM agent_data_backfill_cursor WHERE id=1",
                [],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        let mut processed = 0;
        loop {
            let rows = read_page(conn, cursor.as_deref())?;
            if rows.is_empty() {
                break;
            }
            let remaining = limit.map_or(rows.len(), |limit| {
                limit.saturating_sub(processed).min(rows.len())
            });
            if remaining == 0 {
                return Ok(counts);
            }
            let used = write_batch(
                conn,
                &rows[..remaining],
                &mut counts.writes,
                true,
                |tx, (_, session)| {
                    if let Some(session) = session {
                        counts.inserted +=
                            insert(tx, &broca_core::session_addr(session), session)? as u64;
                    }
                    Ok(())
                },
                |tx, rows| {
                    let valid = rows.iter().all(|(_, session)| session.is_some());
                    tx.execute("INSERT INTO agent_data_backfill_cursor(id,last_run_id,valid) VALUES(1,?1,?2)
                        ON CONFLICT(id) DO UPDATE SET last_run_id=excluded.last_run_id, valid=MIN(valid,excluded.valid)",
                        rusqlite::params![rows.last().map(|row| &row.0),valid])?;
                    Ok(())
                },
            )?;
            cursor = Some(rows[used - 1].0.clone());
            processed += used;
            if limit.is_some_and(|limit| processed >= limit) {
                return Ok(counts);
            }
        }
        let valid: bool = conn.query_row(
            "SELECT coalesce((SELECT valid FROM agent_data_backfill_cursor WHERE id=1),1)",
            [],
            |row| row.get(0),
        )?;
        write_batch(
            conn,
            &[()],
            &mut counts.writes,
            true,
            |tx, _| {
                tx.execute(
                    "INSERT INTO agent_data_projection VALUES(1,?1)",
                    [if valid { 1 } else { 2 }],
                )?;
                Ok(())
            },
            |_, _| Ok(()),
        )?;
    }
    // Delete only entries actually projected in this batch. New legacy writes
    // racing the drain remain pending instead of disappearing behind completion.
    loop {
        let pending = {
            let mut statement = conn.prepare(
                "SELECT session FROM agent_data_pending_session ORDER BY session LIMIT ?1",
            )?;
            let raw = statement
                .query_map([MAX_BATCH_ITEMS], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            raw.into_iter()
                .map(|raw| {
                    let session = serde_json::from_str::<BindIdentity>(&raw).ok();
                    (raw, session)
                })
                .collect::<Vec<_>>()
        };
        if pending.is_empty() {
            break;
        }
        let used = write_batch(
            conn,
            &pending,
            &mut counts.writes,
            true,
            |tx, (raw, session)| {
                if let Some(session) = session {
                    counts.inserted +=
                        insert(tx, &broca_core::session_addr(session), session)? as u64;
                } else {
                    tx.execute(
                        "UPDATE agent_data_projection SET backfilled=2 WHERE id=1",
                        [],
                    )?;
                }
                tx.execute(
                    "DELETE FROM agent_data_pending_session WHERE session=?1",
                    [raw],
                )?;
                Ok(())
            },
            |_, _| Ok(()),
        )?;
        if done {
            counts.distinct_sessions += used as u64;
        }
    }
    Ok(counts)
}

type PageRow = (String, Option<BindIdentity>);
fn read_page(conn: &Connection, cursor: Option<&str>) -> Result<Vec<PageRow>, StoreError> {
    let raw = if let Some(cursor) = cursor {
        let mut statement = conn.prepare(
            "SELECT run_id,session FROM run_index WHERE run_id>?1 ORDER BY run_id LIMIT ?2",
        )?;
        let rows = statement
            .query_map(rusqlite::params![cursor, MAX_BATCH_ITEMS], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows
    } else {
        let mut statement =
            conn.prepare("SELECT run_id,session FROM run_index ORDER BY run_id LIMIT ?1")?;
        let rows = statement
            .query_map([MAX_BATCH_ITEMS], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows
    };
    Ok(raw
        .into_iter()
        .map(|(id, raw)| (id, serde_json::from_str(&raw).ok()))
        .collect())
}
