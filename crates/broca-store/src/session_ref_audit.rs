//! Read-only discovery of historical session references whose log is missing or forked.

use crate::{session_log_evidence, SqliteStore};
use broca_core::{session_addr, FileWalStore, ReadOnlyWalReader, WalStore};
use broca_protocol::BindIdentity;
use rusqlite::{Connection, TransactionBehavior};
use serde::Serialize;
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AlternateLog {
    pub identity: BindIdentity,
    pub address: String,
    pub lineage_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SessionRefFinding {
    pub session_ref: String,
    pub address: String,
    pub identity: BindIdentity,
    pub state: session_log_evidence::SessionLogState,
    pub alternatives: Vec<AlternateLog>,
}

impl SqliteStore {
    /// Streams findings from one read snapshot. Only compact log addresses and
    /// distinct candidate roots are retained, never a run-history join.
    pub fn audit_session_refs(
        &self,
        mut emit: impl FnMut(&SessionRefFinding) -> Result<()>,
    ) -> Result<u64> {
        let mut conn = self.conn.lock().map_err(|error| error.to_string())?;
        session_log_evidence::indexes::require(&conn)?;
        let root = state_root(&conn)?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let wal = FileWalStore::new(root.join("wal"));
        let addresses = ReadOnlyWalReader::new(&root).addresses()?;
        let roots = known_roots(&tx)?;
        let mut count = 0;
        {
            let mut query = tx.prepare("SELECT session_ref,address,project_root,harness,session FROM agent_data_session_ref ORDER BY session_ref")?;
            let mut rows = query.query([])?;
            while let Some(row) = rows.next()? {
                let identity = BindIdentity::new(
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                );
                identity.validate_keyable()?;
                let address: String = row.get(1)?;
                if address != session_addr(&identity) {
                    return Err("session_ref_address_mismatch".into());
                }
                let state = session_log_evidence::log_state(
                    || session_log_evidence::evidence(&tx, &identity),
                    &identity,
                    &wal,
                )?;
                if !state.missing_history() {
                    continue;
                }
                emit(&SessionRefFinding {
                    session_ref: row.get(0)?,
                    address,
                    identity: identity.clone(),
                    state,
                    alternatives: alternate_logs(&tx, &root, &identity, &addresses, &roots)?,
                })?;
                count += 1;
            }
        }
        tx.commit()?;
        Ok(count)
    }
}

fn state_root(conn: &Connection) -> Result<PathBuf> {
    conn.path()
        .filter(|path| !path.is_empty())
        .and_then(|path| Path::new(path).parent())
        .map(Path::to_path_buf)
        .ok_or_else(|| "session-ref audit requires an on-disk run-index.db".into())
}

pub(crate) fn known_roots(conn: &Connection) -> Result<BTreeSet<String>> {
    let mut query = conn.prepare("SELECT project_root FROM agent_data_session_ref UNION SELECT json_extract(session,'$.project_root') FROM run_index WHERE json_valid(session) ORDER BY 1")?;
    let mut roots = BTreeSet::new();
    for root in query.query_map([], |row| row.get::<_, Option<String>>(0))? {
        if let Some(root) = root? {
            roots.insert(root);
        }
    }
    Ok(roots)
}

/// Addresses are not reversible. Candidate roots must be independently recorded
/// in the store; a caller's current folder is never an identity fallback.
pub(crate) fn alternate_logs(
    conn: &Connection,
    root: &Path,
    identity: &BindIdentity,
    addresses: &BTreeSet<String>,
    roots: &BTreeSet<String>,
) -> Result<Vec<AlternateLog>> {
    let wal = FileWalStore::new(root.join("wal"));
    let mut found = Vec::new();
    let mut expected = None;
    for project_root in roots {
        if project_root.as_str() == identity.project_root.to_string_lossy().as_ref() {
            continue;
        }
        let candidate = BindIdentity::new(project_root, &identity.harness, &identity.session);
        candidate.validate_keyable()?;
        let address = session_addr(&candidate);
        if !addresses.contains(&address) {
            continue;
        }
        // Most missing refs have no candidate file. Load the indexed run set
        // only when an independently recorded root actually has a log to check.
        if expected.is_none() {
            expected = Some(session_log_evidence::recorded_run_ids(conn, identity)?);
        }
        let required = expected.as_ref().unwrap();
        let range = wal.read_records_range(&candidate, 0, None, None)?;
        let lineage = range.lineage_id.map(|id| id.to_string());
        let broca_wal::WalRangeRecords::Complete(records) = range.records() else {
            return Err("alternate_log_incomplete".into());
        };
        let observed: BTreeSet<_> = records
            .iter()
            .filter_map(|frame| match &frame.record {
                broca_wal::WalRecord::RunStarted { run_id, .. } => Some(run_id.0.clone()),
                _ => None,
            })
            .collect();
        if !required.is_subset(&observed) {
            continue;
        }
        if required.is_empty() {
            // Preserve discovery for older metadata-only imports that have no
            // run-index rows: their existing lineage proof is still required.
            let evidence = session_log_evidence::evidence(conn, identity)?;
            if !lineage
                .as_ref()
                .is_some_and(|id| evidence.lineages.contains(id))
            {
                continue;
            }
        }
        if records.iter().any(|frame| matches!(&frame.record, broca_wal::WalRecord::RunStarted { session, .. } if session.session == identity.session)) {
            found.push(AlternateLog { identity: candidate, address, lineage_id: lineage });
        }
    }
    Ok(found)
}
