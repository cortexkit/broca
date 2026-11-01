//! Historical content evidence, distinct from a bind's identity reservation.

use crate::SqliteStore;
use broca_protocol::BindIdentity;
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
type EvidenceError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Clone, Default)]
pub struct SessionLogEvidence {
    pub content: bool,
    pub removed: bool,
    pub lineages: std::collections::BTreeSet<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionLogState {
    Available,
    Unwritten,
    Removed,
    Missing,
    ForkedStub,
}

impl SessionLogState {
    pub fn missing_history(self) -> bool {
        matches!(self, Self::Missing | Self::ForkedStub)
    }
}

impl SqliteStore {
    pub fn session_log_state(
        &self,
        session: &BindIdentity,
        wal: &dyn broca_core::WalStore,
    ) -> Result<SessionLogState, String> {
        log_state(
            || {
                let conn = self.conn.lock().unwrap();
                evidence(&conn, session)
            },
            session,
            wal,
        )
        .map_err(|error| error.to_string())
    }
    pub fn session_log_evidence(
        &self,
        session: &BindIdentity,
    ) -> Result<SessionLogEvidence, String> {
        let conn = self.conn.lock().unwrap();
        evidence(&conn, session).map_err(|error| error.to_string())
    }
}

/// Shared by serving and the offline audit. The byte length only avoids probing
/// ordinary logs: a forked stub requires a verified, record-free WAL and a lineage
/// different from recorded history. A new subscriber's first lineage remains
/// legitimate, as does a fresh lineage after proven deliberate removal.
/// Evidence is loaded lazily so the filesystem fast path never locks the store's
/// database connection. Offline callers can reuse their existing read snapshot.
pub(crate) fn log_state(
    load_evidence: impl FnOnce() -> Result<SessionLogEvidence, EvidenceError>,
    session: &BindIdentity,
    wal: &dyn broca_core::WalStore,
) -> Result<SessionLogState, EvidenceError> {
    match wal.check_move(session, false) {
        Err(broca_wal::WalError::SessionMoved { .. }) => return Ok(SessionLogState::Removed),
        Err(error) => return Err(error.into()),
        Ok(()) => {}
    }
    let present = wal
        .wal_present(session)
        .ok_or("cannot establish session WAL location")?;
    if present
        && wal
            .live_wal_bytes(session)?
            .is_none_or(|bytes| bytes > broca_wal::framing::LINEAGE_FRAME_LEN as u64)
    {
        return Ok(SessionLogState::Available);
    }
    let history = load_evidence()?;
    if !history.content {
        return Ok(SessionLogState::Unwritten);
    }
    if history.removed {
        return Ok(SessionLogState::Removed);
    }
    if !present {
        return Ok(SessionLogState::Missing);
    }
    let range = wal.read_records_range(session, 0, None, None)?;
    if let (broca_wal::WalRangeRecords::Complete(records), Some(lineage)) =
        (range.records(), range.lineage_id)
    {
        if records.is_empty() && history.lineages.iter().any(|id| id != &lineage.to_string()) {
            return Ok(SessionLogState::ForkedStub);
        }
    }
    Ok(SessionLogState::Available)
}

/// A bind's identity reservation alone reserves no content: subscribe-before-send creates the
/// cursor's first lineage, and an interrupted first bind must remain retryable.
/// A run row in any state, any materialized projection/meta row, a covered log
/// census entry, or a deletion ledger/tombstone is content evidence for the exact
/// key. A census entry is covered only after discovery verified the log's bytes
/// or its already-represented lineage metadata. Once that evidence exists, a missing log must not be replaced by subscribe
/// or any other actor-opening operation. Old removal evidence cannot excuse the
/// loss of a newer, unexpired run or a different materialized lineage.
/// An unexpired run with unknown lineage needs its run ID in a verified
/// pending-deletion log before it can share that older removal proof.
pub(crate) fn evidence(
    conn: &Connection,
    session: &BindIdentity,
) -> Result<SessionLogEvidence, EvidenceError> {
    let key = format!(
        "{}\u{1f}{}\u{1f}{}",
        session.project_root.to_string_lossy(),
        session.harness,
        session.session
    );
    let address = broca_core::session_addr(session);
    let mut content = false;
    let mut lineages = std::collections::BTreeSet::new();
    for table in [
        "meta",
        "message",
        "message_tag",
        "episode_span",
        "retained_head",
        "compaction_facts",
        "bus_event_projection",
        "bus_event_open",
        "retention_lineage",
        "retention_deletion",
    ] {
        if table_exists(conn, table)? {
            content |= conn.query_row(
                &format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE session=?1)"),
                [&key],
                |row| row.get::<_, bool>(0),
            )?;
            let has_lineage: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_table_info(?1) WHERE name='lineage_id')",
                [table],
                |row| row.get(0),
            )?;
            if has_lineage {
                let active = if table == "retention_lineage" {
                    " AND expired_at_ms IS NULL"
                } else {
                    ""
                };
                let mut query = conn.prepare(&format!("SELECT lineage_id FROM {table} WHERE session=?1 AND lineage_id IS NOT NULL{active}"))?;
                for id in query.query_map([&key], |row| row.get::<_, String>(0))? {
                    lineages.insert(id?);
                }
            }
        }
    }
    let run_match = "json_valid(session) AND json_extract(session,'$.project_root')=?1 AND json_extract(session,'$.harness')=?2 AND json_extract(session,'$.session')=?3";
    let args = params![
        session.project_root.to_string_lossy(),
        session.harness,
        session.session
    ];
    let (runs, unexpired): (bool, bool) = conn.query_row(
        &format!("SELECT EXISTS(SELECT 1 FROM run_index WHERE {run_match}), EXISTS(SELECT 1 FROM run_index WHERE {run_match} AND expired_at_ms IS NULL)"),
        args, |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    content |= runs;
    let mut query = conn.prepare(&format!("SELECT lineage_id FROM run_index WHERE {run_match} AND expired_at_ms IS NULL AND lineage_id IS NOT NULL"))?;
    for id in query.query_map(
        params![
            session.project_root.to_string_lossy(),
            session.harness,
            session.session
        ],
        |row| row.get::<_, String>(0),
    )? {
        lineages.insert(id?);
    }
    if table_exists(conn, "agent_data_census")? {
        content |= conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM agent_data_census WHERE address=?1 AND covered=1)",
            [&address],
            |row| row.get::<_, bool>(0),
        )?;
    }
    let mut deletion_lineages = std::collections::HashSet::new();
    let mut ledger_query = conn.prepare(&format!(
        "SELECT lineage_id FROM retention_deletion WHERE {run_match}"
    ))?;
    for id in ledger_query.query_map(
        params![
            session.project_root.to_string_lossy(),
            session.harness,
            session.session
        ],
        |row| row.get::<_, String>(0),
    )? {
        deletion_lineages.insert(id?);
    }
    let ledger = !deletion_lineages.is_empty();
    content |= ledger;
    let mut removed = ledger || (runs && !unexpired);
    let mut stones = std::collections::HashSet::new();
    if let Some(root) = conn.path().and_then(|path| Path::new(path).parent()) {
        let directory = root
            .join("wal-ephemeral")
            .join(format!("{address}.expired"));
        match std::fs::read_dir(directory) {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry?;
                    if !entry.file_type()?.is_file() {
                        continue;
                    }
                    let value: serde_json::Value =
                        serde_json::from_slice(&std::fs::read(entry.path())?)?;
                    let lineage = value["lineage_id"]
                        .as_str()
                        .filter(|id| *id == entry.file_name().to_string_lossy())
                        .ok_or_else(|| {
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "tombstone identity mismatch",
                            )
                        })?;
                    if value["expired_at_ms"].as_u64().is_none() || value["seq"].as_u64().is_none()
                    {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "invalid retention tombstone",
                        )
                        .into());
                    }
                    stones.insert(lineage.to_owned());
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    content |= !stones.is_empty();
    removed |= !stones.is_empty();
    let mut unremoved_run = false;
    if unexpired && removed {
        let retired_lineages: std::collections::HashSet<String> =
            deletion_lineages.union(&stones).cloned().collect();
        let mut proven_retired_runs = None;
        let mut query = conn.prepare(&format!(
            "SELECT run_id,lineage_id FROM run_index WHERE {run_match} AND expired_at_ms IS NULL"
        ))?;
        let mut rows = query.query(params![
            session.project_root.to_string_lossy(),
            session.harness,
            session.session
        ])?;
        while let Some(row) = rows.next()? {
            if let Some(id) = row.get::<_, Option<String>>(1)? {
                unremoved_run |= !retired_lineages.contains(&id);
            } else {
                // An older expiry cannot excuse a newer run whose lineage has
                // not been projected yet. A verified deleting log must name it.
                if proven_retired_runs.is_none() {
                    proven_retired_runs = Some(deleting_run_ids(conn, session, &retired_lineages)?);
                }
                unremoved_run |= !proven_retired_runs
                    .as_ref()
                    .unwrap()
                    .contains(&row.get::<_, String>(0)?);
            }
        }
    }
    let meta: Option<Option<String>> = conn
        .query_row(
            "SELECT lineage_id FROM meta WHERE session=?1",
            [&key],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(lineage) = meta {
        let covered = match lineage {
            Some(id) => stones.contains(&id) || deletion_lineages.contains(&id),
            None => false,
        };
        removed &= covered;
    }
    Ok(SessionLogEvidence {
        content,
        removed: removed && !unremoved_run,
        lineages,
    })
}

fn deleting_run_ids(
    conn: &Connection,
    session: &BindIdentity,
    lineages: &std::collections::HashSet<String>,
) -> Result<std::collections::HashSet<String>, EvidenceError> {
    let mut runs = std::collections::HashSet::new();
    let Some(root) = conn.path().and_then(|path| Path::new(path).parent()) else {
        return Ok(runs);
    };
    let wal = broca_core::FileWalStore::new(root.join("wal"));
    for lineage in lineages {
        // Ledger text becomes a filename component; the verified header below
        // must also match its canonical lineage spelling before it proves a run.
        if lineage.len() != 36
            || !lineage
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
        {
            continue;
        }
        let path = wal.ephemeral_root().join(format!(
            "{}.{lineage}.deleting",
            broca_core::session_addr(session)
        ));
        let range = broca_wal::read_records_range(path, wal.max_read_bytes(), 0, None, None)?;
        if range.lineage_id.map(|id| id.to_string()).as_deref() != Some(lineage.as_str()) {
            continue;
        }
        let broca_wal::WalRangeRecords::Complete(records) = range.records() else {
            continue;
        };
        for frame in records {
            if let broca_wal::WalRecord::RunStarted { run_id, .. } = &frame.record {
                runs.insert(run_id.0.clone());
            }
        }
    }
    Ok(runs)
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool, EvidenceError> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)",
        [table],
        |row| row.get(0),
    )?)
}

#[cfg(test)]
mod tests;
