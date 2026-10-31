//! Explicit, offline repair of run-index identities left behind by a WAL re-key.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use broca_core::{agent_session_ref, session_addr, ReadOnlyWalReader, RunIndexState, WalStore};
use broca_protocol::BindIdentity;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::Serialize;

use crate::{session_refs, SqliteStore};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RunRootChange {
    pub run_id: String,
    pub from: BindIdentity,
    pub to: BindIdentity,
    pub state: String,
    #[serde(skip)]
    before_json: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RefRemoval {
    pub session_ref: String,
    pub address: String,
    pub identity: BindIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct DiscoveryRemoval {
    pub table: String,
    pub address: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ProjectionTableRows {
    pub table: String,
    pub rows: u64,
    pub remove: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ProjectionMeta {
    pub lineage_id: Option<String>,
    pub watermark_seq: u64,
}

// These rows are derived from WAL history, not billing, deletion or move authority.
// Unknown tables with stale-key rows refuse cleanup instead of being guessed at.
const PROJECTION_TABLES: &[&str] = &[
    "bus_event_open",
    "bus_event_projection",
    "compaction_facts",
    "episode_span",
    "message",
    "message_tag",
    "meta",
    "retained_head",
    "retention_lineage",
];

/// Every row to change is exposed before mutation. An empty plan is an idempotent no-op.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SessionRootRepair {
    pub harness: String,
    pub session: String,
    pub surviving_identity: BindIdentity,
    pub surviving_address: String,
    pub stale_identity: Option<BindIdentity>,
    pub stale_address: Option<String>,
    pub repoint_runs: Vec<RunRootChange>,
    pub remove_v1_refs: Vec<RefRemoval>,
    pub remove_v2_aliases: Vec<RefRemoval>,
    pub remove_discovery_rows: Vec<DiscoveryRemoval>,
    pub stale_projection_tables: Vec<ProjectionTableRows>,
    pub stale_meta: Option<ProjectionMeta>,
    pub live_log_lineage: Option<String>,
    pub live_meta_present: bool,
    pub remove_retention_discovery_rows: u64,
}

impl SqliteStore {
    /// Open only an existing schema for the explicit repair. Unlike ordinary
    /// daemon startup, maintenance must not change unreported rows via migrations.
    pub fn open_existing_for_session_root_repair(path: &Path) -> Result<Self> {
        let mut store = Self::open_read_only(path)?;
        let conn = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        crate::busy::configure_writer_connection(&conn)?;
        store.conn = std::sync::Mutex::new(conn);
        Ok(store)
    }

    /// Preview only: no schema initialization, lease, WAL writer or discovery writes.
    pub fn preview_session_root_repair(
        &self,
        harness: &str,
        session: &str,
    ) -> Result<SessionRootRepair> {
        let mut conn = self.conn.lock().map_err(|e| e.to_string())?;
        let root = state_root(&conn)?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let plan = plan(&tx, &root, harness, session)?;
        tx.commit()?;
        Ok(plan)
    }

    /// The caller must keep the daemon and its supervisor stopped throughout maintenance.
    /// Both session leases additionally exclude standalone writers. Replanning under the
    /// SQLite writer lock refuses a changed preview instead of applying an unreported edit.
    pub fn apply_session_root_repair(&self, expected: &SessionRootRepair) -> Result<()> {
        if expected.stale_identity.is_none() {
            if &self.preview_session_root_repair(&expected.harness, &expected.session)? != expected
            {
                return Err(
                    "session-root repair preview changed; preview again before writing".into(),
                );
            }
            return Ok(());
        }
        let root = state_root(&*self.conn.lock().map_err(|e| e.to_string())?)?;
        let mut identities = vec![expected.surviving_identity.clone()];
        identities.extend(expected.stale_identity.clone());
        identities.sort_by_key(Self::key);
        let leases = broca_core::lease::FileLeaseStore::new(root.join("leases"));
        use broca_core::lease::LeaseStore;
        let _held = identities
            .iter()
            .map(|identity| leases.acquire(&broca_core::lease::lease_key_for(identity)))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut conn = self.conn.lock().map_err(|e| e.to_string())?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let actual = plan(&tx, &root, &expected.harness, &expected.session)?;
        if &actual != expected {
            return Err("session-root repair preview changed; preview again before writing".into());
        }
        if actual.stale_identity.is_none() {
            return Ok(());
        }
        // Suppress the legacy-write trigger only while updating identities whose
        // surviving candidate is already present in this same transaction.
        tx.execute("INSERT INTO agent_data_writer VALUES(1)", [])?;
        for run in &actual.repoint_runs {
            tx.execute(
                "UPDATE run_index SET session=?1 WHERE run_id=?2 AND session=?3 AND state=?4",
                params![
                    serde_json::to_string(&run.to)?,
                    run.run_id,
                    run.before_json,
                    run.state
                ],
            )?;
        }
        for reference in &actual.remove_v1_refs {
            tx.execute(
                "DELETE FROM agent_data_session_ref WHERE session_ref=?1 AND address=?2",
                params![reference.session_ref, reference.address],
            )?;
        }
        for reference in &actual.remove_v2_aliases {
            tx.execute(
                "DELETE FROM agent_data_session_ref_v2 WHERE session_ref=?1 AND address=?2",
                params![reference.session_ref, reference.address],
            )?;
        }
        for row in &actual.remove_discovery_rows {
            // Table names come only from the fixed discovery-table list in plan().
            tx.execute(
                &format!("DELETE FROM {} WHERE address=?1", row.table),
                [&row.address],
            )?;
        }
        let stale_key = Self::key(actual.stale_identity.as_ref().unwrap());
        for table in actual
            .stale_projection_tables
            .iter()
            .filter(|table| table.remove && table.rows > 0)
        {
            let removed = tx.execute(
                &format!(
                    "DELETE FROM {} WHERE session=?1",
                    quoted_identifier(&table.table)
                ),
                [&stale_key],
            )?;
            if removed as u64 != table.rows {
                return Err(format!(
                    "orphaned projection count changed for table {}; preview again",
                    table.table
                )
                .into());
            }
        }
        tx.execute("DELETE FROM agent_data_writer", [])?;
        tx.commit()?;
        Ok(())
    }
}

fn state_root(conn: &Connection) -> Result<PathBuf> {
    conn.path()
        .filter(|path| !path.is_empty())
        .and_then(|path| Path::new(path).parent())
        .map(Path::to_path_buf)
        .ok_or_else(|| "session-root repair requires an on-disk run-index.db".into())
}

fn plan(conn: &Connection, root: &Path, harness: &str, session: &str) -> Result<SessionRootRepair> {
    if session_refs::projection_gap(Some(root))? || !session_refs::projection_complete(conn)? {
        return Err(
            "session-root repair requires a complete session-ref inventory; finish discovery first"
                .into(),
        );
    }
    let references = conn
        .prepare(
            "SELECT session_ref,address,project_root FROM agent_data_session_ref
         WHERE harness=?1 AND session=?2 ORDER BY project_root,session_ref",
        )?
        .query_map(params![harness, session], |row| {
            Ok(RefRemoval {
                session_ref: row.get(0)?,
                address: row.get(1)?,
                identity: BindIdentity::new(row.get::<_, String>(2)?, harness, session),
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut roots = BTreeMap::new();
    for reference in &references {
        reference.identity.validate_keyable()?;
        if session_addr(&reference.identity) != reference.address {
            return Err("session-root repair found an inventory identity/address mismatch".into());
        }
        if roots
            .insert(reference.address.clone(), reference.identity.clone())
            .is_some_and(|identity| identity != reference.identity)
        {
            return Err(
                "session-root repair found two identities with the same WAL address".into(),
            );
        }
    }
    if !(1..=2).contains(&roots.len()) {
        return Err(format!(
            "session-root repair requires one or exactly two roots, found {}",
            roots.len()
        )
        .into());
    }
    let mut result = SessionRootRepair {
        harness: harness.to_owned(),
        session: session.to_owned(),
        surviving_identity: roots.first_key_value().unwrap().1.clone(),
        surviving_address: roots.first_key_value().unwrap().0.clone(),
        stale_identity: None,
        stale_address: None,
        repoint_runs: Vec::new(),
        remove_v1_refs: Vec::new(),
        remove_v2_aliases: Vec::new(),
        remove_discovery_rows: Vec::new(),
        stale_projection_tables: Vec::new(),
        stale_meta: None,
        live_log_lineage: None,
        live_meta_present: false,
        remove_retention_discovery_rows: 0,
    };
    if roots.len() == 1 {
        return Ok(result);
    }
    let reader = ReadOnlyWalReader::new(root);
    let addresses = reader.addresses()?;
    let present: Vec<_> = roots
        .keys()
        .filter(|address| addresses.contains(*address))
        .collect();
    if present.len() != 1 {
        return Err(format!(
            "session-root repair requires exactly one address with a log, found {}",
            present.len()
        )
        .into());
    }
    let live_address = present[0];
    let (stale_address, stale) = roots
        .iter()
        .find(|(address, _)| *address != live_address)
        .unwrap();
    let live = &roots[live_address];
    check_fences(conn, root, stale_address, stale)?;
    check_fences(conn, root, live_address, live)?;
    let foreign: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM agent_data_session_ref WHERE address=?1 AND (harness<>?2 OR session<>?3))",
        params![stale_address,harness,session], |row| row.get(0))?;
    if foreign {
        return Err("stale address has an unrelated v1 ref".into());
    }
    let (records, _) = reader.records(live_address)?;
    let mut current = None;
    let mut finished = BTreeSet::new();
    for frame in &records {
        match &frame.record {
            broca_wal::WalRecord::RunStarted {
                run_id,
                session: identity,
                ..
            } => {
                current = (identity.harness == harness
                    && identity.session == session
                    && (identity == stale || identity == live))
                    .then_some(run_id.0.clone());
            }
            broca_wal::WalRecord::RunFinished { .. } => {
                if let Some(run_id) = current.take() {
                    finished.insert(run_id);
                }
            }
            _ => {}
        }
    }
    let rows = conn
        .prepare(
            "SELECT run_id,session,state FROM run_index
         WHERE json_extract(CASE WHEN json_valid(session) THEN session ELSE '{}' END,'$.harness')=?1
           AND json_extract(CASE WHEN json_valid(session) THEN session ELSE '{}' END,'$.session')=?2
         ORDER BY run_id",
        )?
        .query_map(params![harness, session], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    for (run_id, raw, state) in rows {
        let identity: BindIdentity = serde_json::from_str(&raw)?;
        if identity == *live {
            continue;
        }
        if identity != *stale {
            return Err(format!("run {run_id} names a third, uninventoried root").into());
        }
        if !RunIndexState::parse(&state).is_some_and(|state| state.is_terminal()) {
            return Err(format!(
                "run {run_id} is not terminal; session-root repair leaves it alone"
            )
            .into());
        }
        if !finished.contains(&run_id) {
            return Err(format!("run {run_id} has no matching completed episode in the surviving log; cannot re-point exactly").into());
        }
        result.repoint_runs.push(RunRootChange {
            run_id,
            from: stale.clone(),
            to: live.clone(),
            state,
            before_json: raw,
        });
    }
    result.surviving_identity = live.clone();
    result.surviving_address = live_address.clone();
    result.stale_identity = Some(stale.clone());
    result.stale_address = Some(stale_address.clone());
    result.remove_v1_refs = references
        .into_iter()
        .filter(|reference| reference.identity == *stale)
        .collect();
    let alias = agent_session_ref(stale);
    result.remove_v2_aliases = conn.prepare(
        "SELECT session_ref FROM agent_data_session_ref_v2 WHERE address=?1 ORDER BY session_ref",
    )?.query_map([stale_address], |row| row.get::<_,String>(0))?
        .map(|reference| {
            let reference = reference?;
            if reference != alias { return Err("stale address has an unrelated v2 alias".into()); }
            Ok(RefRemoval { session_ref: reference, address: stale_address.clone(), identity: stale.clone() })
        }).collect::<Result<Vec<_>>>()?;
    for table in [
        "agent_data_census",
        "agent_data_census_inflight",
        "agent_data_census_unreadable",
    ] {
        let present: bool = conn.query_row(
            &format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE address=?1)"),
            [stale_address],
            |row| row.get(0),
        )?;
        if present {
            result.remove_discovery_rows.push(DiscoveryRemoval {
                table: table.to_owned(),
                address: stale_address.clone(),
            });
        }
    }
    plan_projection_cleanup(conn, root, stale, live, &mut result)?;
    Ok(result)
}

fn quoted_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn plan_projection_cleanup(
    conn: &Connection,
    root: &Path,
    stale: &BindIdentity,
    live: &BindIdentity,
    result: &mut SessionRootRepair,
) -> Result<()> {
    let key = SqliteStore::key(stale);
    let tables = conn
        .prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    for table in tables {
        let session_column: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info(?1) WHERE name='session')",
            [&table],
            |row| row.get(0),
        )?;
        if !session_column {
            continue;
        }
        let rows = conn.query_row(
            &format!(
                "SELECT count(*) FROM {} WHERE session=?1",
                quoted_identifier(&table)
            ),
            [&key],
            |row| row.get::<_, u64>(0),
        )?;
        let remove = PROJECTION_TABLES.contains(&table.as_str());
        if !remove && rows > 0 {
            return Err(format!("table {table} has stale-key rows but is not a known per-session projection; refusing cleanup").into());
        }
        result.stale_projection_tables.push(ProjectionTableRows {
            table,
            rows,
            remove,
        });
    }
    result.stale_meta = conn
        .query_row(
            "SELECT lineage_id,watermark_seq FROM meta WHERE session=?1",
            [&key],
            |row| {
                Ok(ProjectionMeta {
                    lineage_id: row.get(0)?,
                    watermark_seq: row.get(1)?,
                })
            },
        )
        .optional()?;
    result.live_meta_present = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM meta WHERE session=?1)",
        [SqliteStore::key(live)],
        |row| row.get(0),
    )?;
    // The existing lineage-delete trigger removes this derived discovery hint.
    // Report its count as well; it is not deletion or withdrawal authority.
    result.remove_retention_discovery_rows = conn.query_row(
        "SELECT count(*) FROM retention_discovery WHERE lineage_id IN
         (SELECT lineage_id FROM retention_lineage WHERE session=?1)",
        [&key],
        |row| row.get(0),
    )?;
    let wal = broca_core::FileWalStore::new(root.join("wal"));
    let range = wal.read_records_range(live, 0, None, None)?;
    result.live_log_lineage = range.lineage_id.map(|id| id.to_string());
    if let Some(meta) = &result.stale_meta {
        if result.live_log_lineage.is_none() || meta.lineage_id != result.live_log_lineage {
            return Err(format!("stale meta lineage_id {:?} at watermark_seq {} differs from verified live log lineage {:?}; refusing orphaned projection cleanup",
                meta.lineage_id, meta.watermark_seq, result.live_log_lineage).into());
        }
    }
    // Every projection's lineage must match the surviving log. A matching meta
    // row does not prove that another table's rows came from that same log.
    for table in ["compaction_facts", "retained_head", "retention_lineage"] {
        let different: bool = conn.query_row(
            &format!(
                "SELECT EXISTS(SELECT 1 FROM {table} WHERE session=?1 AND lineage_id IS NOT ?2)"
            ),
            params![key, result.live_log_lineage],
            |row| row.get(0),
        )?;
        if different {
            return Err(format!("stale projection table {table} has a different or unverified lineage; refusing cleanup").into());
        }
    }
    if !result.live_meta_present {
        let broca_wal::WalRangeRecords::Complete(records) = range.records() else {
            return Err(
                "live key has no meta row and the complete log needed to rebuild it is unavailable"
                    .into(),
            );
        };
        // Prepare from an empty snapshot without committing it: prove rebuilding
        // succeeds, but leave every live-key row untouched during maintenance.
        SqliteStore::prepare_projection(
            broca_core::projection_plan::ProjectionSnapshot {
                attribution_complete: true,
                seed: None,
                watermark: 0,
                lineage_id: None,
                next_ord: 0,
                previous_control_head: None,
            },
            records,
            Some(&range),
        )?;
    }
    Ok(())
}

fn check_fences(
    conn: &Connection,
    root: &Path,
    address: &str,
    identity: &BindIdentity,
) -> Result<()> {
    broca_core::FileWalStore::new(root.join("wal"))
        .check_move(identity, true)
        .map_err(|error| format!("move/cut fence at address {address}: {error}"))?;
    let pending = conn
        .prepare("SELECT session FROM retention_deletion")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    for raw in pending {
        if serde_json::from_str::<BindIdentity>(&raw)? == *identity {
            return Err(format!("address {address} has a deletion-ledger entry").into());
        }
    }
    let expired: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM retention_lineage WHERE session=?1 AND expired_at_ms IS NOT NULL)",
        [SqliteStore::key(identity)], |row| row.get(0))?;
    if expired {
        return Err(format!("address {address} has an expired retention lineage").into());
    }
    let dir = root.join("wal-ephemeral");
    match std::fs::symlink_metadata(dir.join(format!("{address}.expired"))) {
        Ok(_) => {
            return Err(
                format!("address {address} has a deletion-ledger tombstone directory").into(),
            )
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    for entry in entries {
        let name = entry?.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(&format!("{address}.")) && name.ends_with(".deleting") {
            return Err(format!("address {address} has in-progress retention deletion").into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
