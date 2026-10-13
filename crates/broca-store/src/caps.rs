//! Durable daemon-scoped cap table and the spend-delta receiver.
//!
//! Cap mutations use a dedicated SQLite connection, separate from session WALs and
//! the run index. The admission path reads only the mirror populated after a
//! committed transaction; it never takes the SQLite connection lock.
//!
//! THIS MACHINE HAS NEVER ENFORCED A CAP IN PRODUCTION. Enforcement is dark by design on a fleet
//! billed by subscription rather than by token, so no cap has been armed. The tests here are
//! thorough about ordering, versioning and idempotency, and silent about the enforcement path,
//! because that path has no production instance to be right or wrong about.
//!
//! Counting rows does NOT settle it, which is the trap worth naming. Production caps.db held
//! exactly one row for four days: a contract-seam artifact from a cross-repo wire verification,
//! scoped to a /tmp path that had already been deleted. A count of 1 reads as "this has been
//! used" -- true about the table, false about the question. The row has since been removed by
//! agreement with its author, so the count is 0 again, but the lesson survives the cleanup:
//! ASK FOR A CAP WHOSE SCOPE IS A REAL PROJECT ROOT. When a count is the misleading instrument,
//! name the discriminator rather than correcting the number.
//!
//! What that single execution DID prove is worth keeping: the cap wire shapes are compatible
//! against this live receiver, verified once, in production, by the consumer that depends on
//! them. That is the only production evidence this machine has ever produced.

use std::collections::HashMap;
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;
use std::sync::{Mutex, RwLock};

#[cfg(test)]
use broca_core::CapBaseline;
use broca_core::{
    AppliedCapWatermark, CapInstallRequest, CapMutation, CapOperation, CapRow, CapStore,
    CapStoreError, CapWatermark, SpendDeltaRequest,
};
use rusqlite::{Connection, OptionalExtension, Transaction};

const SPEND_DIMENSION: &str = "spend_dollars";

type ActiveScopeKey = (String, Option<String>);

struct CapMirror {
    by_id: HashMap<String, CapRow>,
    active_by_scope: HashMap<ActiveScopeKey, Vec<String>>,
}

impl CapMirror {
    fn from_by_id(by_id: HashMap<String, CapRow>) -> Self {
        let mut active_by_scope: HashMap<ActiveScopeKey, Vec<String>> = HashMap::new();
        for row in by_id.values().filter(|row| row.active) {
            active_by_scope
                .entry(active_scope_key(row))
                .or_default()
                .push(row.cap_id.clone());
        }
        Self {
            by_id,
            active_by_scope,
        }
    }

    fn remove_active(&mut self, key: &ActiveScopeKey, cap_id: &str) {
        let remove_bucket = if let Some(bucket) = self.active_by_scope.get_mut(key) {
            if let Some(index) = bucket.iter().position(|indexed| indexed == cap_id) {
                bucket.swap_remove(index);
            }
            bucket.is_empty()
        } else {
            false
        };
        if remove_bucket {
            self.active_by_scope.remove(key);
        }
    }

    fn publish(&mut self, row: CapRow) {
        let cap_id = row.cap_id.clone();
        let old_active_key = self
            .by_id
            .get(&cap_id)
            .filter(|old| old.active)
            .map(active_scope_key);
        if let Some(key) = old_active_key {
            self.remove_active(&key, &cap_id);
        }

        let new_active_key = row.active.then(|| active_scope_key(&row));
        self.by_id.insert(cap_id.clone(), row);
        if let Some(key) = new_active_key {
            self.active_by_scope.entry(key).or_default().push(cap_id);
        }
    }
}

fn active_scope_key(row: &CapRow) -> ActiveScopeKey {
    (row.scope_key.clone(), row.project_root.clone())
}

pub struct CapsStore {
    conn: Mutex<Connection>,
    mutation: Mutex<()>,
    mirror: RwLock<CapMirror>,
}

impl CapsStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, CapStoreError> {
        let path = path.as_ref();
        #[cfg(windows)]
        super::prepare_private_sqlite(path).map_err(storage)?;
        #[cfg(unix)]
        {
            let mut options = broca_owner_only::OpenOptions::new();
            options.write(true).create(true).truncate(false);
            options.open(path).map_err(storage)?;
        }
        super::sqlite_open_observed();
        let conn = Connection::open(path).map_err(storage)?;
        Self::from_connection(conn)
    }

    pub fn open_in_memory() -> Result<Self, CapStoreError> {
        Self::from_connection(Connection::open_in_memory().map_err(storage)?)
    }

    fn from_connection(conn: Connection) -> Result<Self, CapStoreError> {
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=FULL;
             CREATE TABLE IF NOT EXISTS cap_rows (
                 cap_id TEXT PRIMARY KEY,
                 cap_version INTEGER NOT NULL,
                 scope_key TEXT NOT NULL,
                 project_root TEXT,
                 constraint_dim TEXT NOT NULL,
                 period TEXT NOT NULL,
                 period_bucket TEXT,
                 period_tz TEXT NOT NULL,
                 hard_cap_nanos INTEGER NOT NULL,
                 max_staleness_ms INTEGER NOT NULL,
                 replaces_cap_id TEXT,
                 spend_nanos INTEGER NOT NULL,
                 unresolved_count INTEGER NOT NULL,
                 watermark_json TEXT NOT NULL,
                 as_of_ms INTEGER NOT NULL,
                 active INTEGER NOT NULL,
                 replaced_by TEXT
             );",
        )
        .map_err(storage)?;
        ensure_period_bucket_column(&conn)?;
        let by_id = load_rows(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
            mutation: Mutex::new(()),
            mirror: RwLock::new(CapMirror::from_by_id(by_id)),
        })
    }

    /// Return a copy for restart and state-machine tests without exposing SQLite.
    pub fn row(&self, cap_id: &str) -> Option<CapRow> {
        self.mirror.read().ok()?.by_id.get(cap_id).cloned()
    }

    fn publish_rows(&self, rows: impl IntoIterator<Item = CapRow>) -> Result<(), CapStoreError> {
        // by_id keeps every durable row, including inactive rows and replacement tombstones. The
        // secondary index keeps only active rows so admission work scales with matching caps.
        // Publish only rows changed by the committed transaction so mutation cost does not scale
        // with historical table size.
        let mut mirror = self
            .mirror
            .write()
            .map_err(|_| storage("cap mirror lock poisoned"))?;
        for row in rows {
            mirror.publish(row);
        }
        Ok(())
    }
}

impl CapStore for CapsStore {
    fn install_cap(&self, request: &CapInstallRequest) -> Result<CapMutation, CapStoreError> {
        validate_install_request(request)?;
        let _mutation = self
            .mutation
            .lock()
            .map_err(|_| storage("cap mutation mutex poisoned"))?;
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| storage("cap connection mutex poisoned"))?;
        let tx = conn.transaction().map_err(storage)?;
        let current = read_row(&tx, &request.cap_id)?;

        if let Some(current) = current.as_ref() {
            if current.replaced_by.is_some() || current.cap_version >= request.cap_version {
                tx.rollback().map_err(storage)?;
                return Ok(CapMutation::Applied(AppliedCapWatermark::from(
                    &current.applied_watermark,
                )));
            }
        }

        match request.op {
            CapOperation::Install => {
                let baseline = request
                    .baseline
                    .as_ref()
                    .ok_or(CapStoreError::BaselineRequired)?;
                let mut retired_row = None;
                if let Some(replaces) = request.replaces_cap_id.as_deref() {
                    if replaces == request.cap_id {
                        tx.rollback().map_err(storage)?;
                        return Err(CapStoreError::Invalid(
                            "replaces_cap_id cannot equal cap_id".into(),
                        ));
                    }
                    if let Some(mut target) = read_row(&tx, replaces)? {
                        match target.replaced_by.as_deref() {
                            Some(successor) if successor != request.cap_id.as_str() => {
                                tx.rollback().map_err(storage)?;
                                return Err(CapStoreError::BaselineRequired);
                            }
                            Some(_) => {}
                            None if target.active => {
                                // Retiring A and inserting B share this transaction. A remains as a
                                // replacement tombstone so a late remove(A) cannot retire B. The
                                // predicate prevents a delayed competing replacement from forking A.
                                let updated = tx
                                    .execute(
                                        "UPDATE cap_rows SET active=0, replaced_by=?1
                                         WHERE cap_id=?2 AND active=1 AND replaced_by IS NULL",
                                        rusqlite::params![request.cap_id, replaces],
                                    )
                                    .map_err(storage)?;
                                if updated != 1 {
                                    tx.rollback().map_err(storage)?;
                                    return Err(CapStoreError::BaselineRequired);
                                }
                                target.active = false;
                                target.replaced_by = Some(request.cap_id.clone());
                                retired_row = Some(target);
                            }
                            None => {
                                tx.rollback().map_err(storage)?;
                                return Err(CapStoreError::BaselineRequired);
                            }
                        }
                    }
                }
                let row = CapRow {
                    cap_id: request.cap_id.clone(),
                    cap_version: request.cap_version,
                    scope_key: request.scope_key.clone(),
                    project_root: request.project_root.clone(),
                    constraint_dim: request.constraint_dim.clone(),
                    period: request.period.clone(),
                    period_tz: request.period_tz.clone(),
                    hard_cap_nanos: request.hard_cap_nanos,
                    max_staleness_ms: request.max_staleness_ms,
                    replaces_cap_id: request.replaces_cap_id.clone(),
                    period_bucket: baseline.period_bucket.clone(),
                    spend_nanos: baseline.spend_nanos,
                    unresolved_count: baseline.unresolved_count,
                    applied_watermark: baseline.watermark.clone(),
                    as_of_ms: baseline.as_of_ms,
                    active: true,
                    replaced_by: None,
                };
                write_row(&tx, &row)?;
                let applied = AppliedCapWatermark::from(&row.applied_watermark);
                tx.commit().map_err(storage)?;
                drop(conn);
                self.publish_rows(retired_row.into_iter().chain([row]))?;
                Ok(CapMutation::Applied(applied))
            }
            CapOperation::Remove => {
                let Some(current) = current else {
                    tx.rollback().map_err(storage)?;
                    return Err(CapStoreError::BaselineRequired);
                };
                tx.execute(
                    "UPDATE cap_rows SET cap_version=?1, active=0, replaced_by=NULL WHERE cap_id=?2",
                    rusqlite::params![to_i64(request.cap_version, "cap_version")?, request.cap_id],
                )
                .map_err(storage)?;
                let applied = AppliedCapWatermark::from(&current.applied_watermark);
                let row = CapRow {
                    cap_version: request.cap_version,
                    active: false,
                    replaced_by: None,
                    ..current
                };
                tx.commit().map_err(storage)?;
                drop(conn);
                self.publish_rows([row])?;
                Ok(CapMutation::Applied(applied))
            }
        }
    }

    fn apply_spend_delta(&self, request: &SpendDeltaRequest) -> Result<CapMutation, CapStoreError> {
        if request.period_bucket.trim().is_empty() {
            return Err(CapStoreError::Invalid("period_bucket is empty".into()));
        }
        let _mutation = self
            .mutation
            .lock()
            .map_err(|_| storage("cap mutation mutex poisoned"))?;
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| storage("cap connection mutex poisoned"))?;
        let tx = conn.transaction().map_err(storage)?;
        let Some(current) = read_row(&tx, &request.cap_id)? else {
            tx.rollback().map_err(storage)?;
            return Ok(CapMutation::BaselineRequired);
        };
        if !current.active {
            tx.rollback().map_err(storage)?;
            return Ok(CapMutation::BaselineRequired);
        }
        validate_period_bucket(&current.period, &request.period_bucket)?;
        if let Some(current_bucket) = current.period_bucket.as_deref() {
            if current_bucket != request.period_bucket {
                tx.rollback().map_err(storage)?;
                return Ok(CapMutation::BaselineRequired);
            }
        }

        // Identity is deliberately checked before either sequence is read. This keeps
        // cross-epoch ordering structurally unreachable and forces a fresh baseline.
        if request.from_watermark.identity() != current.applied_watermark.identity()
            || request.to_watermark.identity() != current.applied_watermark.identity()
        {
            tx.rollback().map_err(storage)?;
            return Ok(CapMutation::BaselineRequired);
        }

        if request.cap_version < current.cap_version {
            tx.rollback().map_err(storage)?;
            return Ok(CapMutation::Applied(AppliedCapWatermark::from(
                &current.applied_watermark,
            )));
        }
        if request.cap_version > current.cap_version {
            tx.rollback().map_err(storage)?;
            return Ok(CapMutation::BaselineRequired);
        }

        let current_seq = current.applied_watermark.consumed_export_seq;
        let from_seq = request.from_watermark.consumed_export_seq;
        let to_seq = request.to_watermark.consumed_export_seq;
        if to_seq <= current_seq {
            tx.rollback().map_err(storage)?;
            return Ok(CapMutation::Applied(AppliedCapWatermark::from(
                &current.applied_watermark,
            )));
        }

        // Only an exact contiguous prefix can be applied. In particular, the
        // from < current < to arm is rejected instead of partially double-counting.
        if from_seq != current_seq {
            tx.rollback().map_err(storage)?;
            return Ok(CapMutation::BaselineRequired);
        }

        let spend_nanos = current
            .spend_nanos
            .checked_add(request.priced_delta_nanos)
            .ok_or_else(|| CapStoreError::Storage("spend_nanos overflow".into()))?;
        let unresolved_count = current
            .unresolved_count
            .checked_add(request.unresolved_delta)
            .ok_or_else(|| CapStoreError::Storage("unresolved_count overflow".into()))?;
        if unresolved_count < 0 {
            // A live over-decrement means the receiver can no longer prove its accumulated
            // state. Request a fresh baseline rather than classifying valid stream identity as
            // malformed or publishing an impossible negative count.
            tx.rollback().map_err(storage)?;
            return Ok(CapMutation::BaselineRequired);
        }
        let row = CapRow {
            spend_nanos,
            unresolved_count,
            applied_watermark: request.to_watermark.clone(),
            period_bucket: Some(request.period_bucket.clone()),
            ..current
        };
        write_row(&tx, &row)?;
        let applied = AppliedCapWatermark::from(&row.applied_watermark);
        tx.commit().map_err(storage)?;
        drop(conn);
        self.publish_rows([row])?;
        Ok(CapMutation::Applied(applied))
    }

    fn matching_caps(&self, scope_key: &str, project_root: &Path) -> Vec<CapRow> {
        let scope_key = scope_key.to_owned();
        let exact_key = (
            scope_key.clone(),
            Some(project_root.to_string_lossy().into_owned()),
        );
        let all_projects_key = (scope_key, None);
        let Ok(mirror) = self.mirror.read() else {
            return Vec::new();
        };
        mirror
            .active_by_scope
            .get(&all_projects_key)
            .into_iter()
            .flatten()
            .chain(mirror.active_by_scope.get(&exact_key).into_iter().flatten())
            .filter_map(|cap_id| mirror.by_id.get(cap_id))
            .filter(|row| row.active)
            .cloned()
            .collect()
    }
}

fn validate_install_request(request: &CapInstallRequest) -> Result<(), CapStoreError> {
    if request.cap_id.trim().is_empty() {
        return Err(CapStoreError::Invalid("cap_id is empty".into()));
    }
    if request.scope_key.trim().is_empty() {
        return Err(CapStoreError::Invalid("scope_key is empty".into()));
    }
    if request.constraint_dim != SPEND_DIMENSION {
        return Err(CapStoreError::Invalid(format!(
            "constraint_dim must be {SPEND_DIMENSION}"
        )));
    }
    if request.period.trim().is_empty() || request.period_tz.trim().is_empty() {
        return Err(CapStoreError::Invalid(
            "period and period_tz are required".into(),
        ));
    }
    if request.op == CapOperation::Install {
        if let Some(baseline) = request.baseline.as_ref() {
            if baseline.unresolved_count < 0 {
                return Err(CapStoreError::Invalid(
                    "baseline.unresolved_count cannot be negative".into(),
                ));
            }
            if let Some(period_bucket) = baseline.period_bucket.as_deref() {
                validate_period_bucket(&request.period, period_bucket)?;
            }
        }
    }
    Ok(())
}

fn validate_period_bucket(period: &str, bucket: &str) -> Result<(), CapStoreError> {
    let valid_shape = match period {
        "calendar_month" => matches_ascii_shape(bucket, &[4, 2]),
        "calendar_day" => matches_ascii_shape(bucket, &[4, 2, 2]),
        // Unknown period kinds are open vocabulary: preserve their opaque instance while
        // rejecting the kind itself as a bucket.
        _ => !bucket.is_empty() && bucket != period,
    };
    if valid_shape {
        Ok(())
    } else {
        Err(CapStoreError::Invalid(format!(
            "period_bucket is not a valid instance for period {period}"
        )))
    }
}

fn matches_ascii_shape(value: &str, component_widths: &[usize]) -> bool {
    let parts: Vec<_> = value.split('-').collect();
    parts.len() == component_widths.len()
        && parts.iter().zip(component_widths).all(|(part, width)| {
            part.len() == *width && part.bytes().all(|byte| byte.is_ascii_digit())
        })
}

fn storage(error: impl std::fmt::Display) -> CapStoreError {
    CapStoreError::Storage(error.to_string())
}

fn to_i64(value: u64, field: &str) -> Result<i64, CapStoreError> {
    i64::try_from(value)
        .map_err(|_| CapStoreError::Storage(format!("{field} exceeds SQLite INTEGER")))
}

fn ensure_period_bucket_column(conn: &Connection) -> Result<(), CapStoreError> {
    let has_period_bucket = {
        let mut statement = conn
            .prepare("PRAGMA table_info(cap_rows)")
            .map_err(storage)?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(storage)?;
        let mut has_period_bucket = false;
        for column in columns {
            if column.map_err(storage)? == "period_bucket" {
                has_period_bucket = true;
                break;
            }
        }
        has_period_bucket
    };
    if !has_period_bucket {
        conn.execute("ALTER TABLE cap_rows ADD COLUMN period_bucket TEXT", [])
            .map_err(storage)?;
    }
    Ok(())
}

fn load_rows(conn: &Connection) -> Result<HashMap<String, CapRow>, CapStoreError> {
    let mut statement = conn
        .prepare(
            "SELECT cap_id, cap_version, scope_key, project_root, constraint_dim, period,
                    period_bucket, period_tz, hard_cap_nanos, max_staleness_ms, replaces_cap_id,
                    spend_nanos, unresolved_count, watermark_json, as_of_ms, active, replaced_by
               FROM cap_rows",
        )
        .map_err(storage)?;
    let rows = statement.query_map([], row_from_sql).map_err(storage)?;
    let mut result = HashMap::new();
    for row in rows {
        let row = row.map_err(storage)?;
        result.insert(row.cap_id.clone(), row);
    }
    Ok(result)
}

fn read_row(tx: &Transaction<'_>, cap_id: &str) -> Result<Option<CapRow>, CapStoreError> {
    tx.query_row(
        "SELECT cap_id, cap_version, scope_key, project_root, constraint_dim, period,
                period_bucket, period_tz, hard_cap_nanos, max_staleness_ms, replaces_cap_id,
                spend_nanos, unresolved_count, watermark_json, as_of_ms, active, replaced_by
           FROM cap_rows WHERE cap_id=?1",
        [cap_id],
        row_from_sql,
    )
    .optional()
    .map_err(storage)
}

fn row_from_sql(row: &rusqlite::Row<'_>) -> rusqlite::Result<CapRow> {
    let watermark_json: String = row.get(13)?;
    let watermark: CapWatermark = serde_json::from_str(&watermark_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(13, rusqlite::types::Type::Text, Box::new(error))
    })?;
    let cap_version = row.get::<_, i64>(1)?;
    let max_staleness_ms = row.get::<_, i64>(9)?;
    Ok(CapRow {
        cap_id: row.get(0)?,
        cap_version: from_sql_u64(cap_version, "cap_version")?,
        scope_key: row.get(2)?,
        project_root: row.get(3)?,
        constraint_dim: row.get(4)?,
        period: row.get(5)?,
        period_tz: row.get(7)?,
        hard_cap_nanos: row.get(8)?,
        max_staleness_ms: from_sql_u64(max_staleness_ms, "max_staleness_ms")?,
        replaces_cap_id: row.get(10)?,
        period_bucket: row.get(6)?,
        spend_nanos: row.get(11)?,
        unresolved_count: row.get(12)?,
        applied_watermark: watermark,
        as_of_ms: row.get(14)?,
        active: row.get::<_, i64>(15)? != 0,
        replaced_by: row.get(16)?,
    })
}

fn from_sql_u64(value: i64, field: &str) -> rusqlite::Result<u64> {
    u64::try_from(value).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Integer,
            format!("{field} is negative").into(),
        )
    })
}

fn write_row(tx: &Transaction<'_>, row: &CapRow) -> Result<(), CapStoreError> {
    let watermark_json = serde_json::to_string(&row.applied_watermark).map_err(storage)?;
    tx.execute(
        "INSERT INTO cap_rows (
             cap_id, cap_version, scope_key, project_root, constraint_dim, period, period_bucket,
             period_tz, hard_cap_nanos, max_staleness_ms, replaces_cap_id, spend_nanos,
             unresolved_count, watermark_json, as_of_ms, active, replaced_by
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)
         ON CONFLICT(cap_id) DO UPDATE SET
             cap_version=excluded.cap_version,
             scope_key=excluded.scope_key,
             project_root=excluded.project_root,
             constraint_dim=excluded.constraint_dim,
             period=excluded.period,
             period_bucket=excluded.period_bucket,
             period_tz=excluded.period_tz,
             hard_cap_nanos=excluded.hard_cap_nanos,
             max_staleness_ms=excluded.max_staleness_ms,
             replaces_cap_id=excluded.replaces_cap_id,
             spend_nanos=excluded.spend_nanos,
             unresolved_count=excluded.unresolved_count,
             watermark_json=excluded.watermark_json,
             as_of_ms=excluded.as_of_ms,
             active=excluded.active,
             replaced_by=excluded.replaced_by",
        rusqlite::params![
            row.cap_id,
            to_i64(row.cap_version, "cap_version")?,
            row.scope_key,
            row.project_root,
            row.constraint_dim,
            row.period,
            row.period_bucket,
            row.period_tz,
            row.hard_cap_nanos,
            to_i64(row.max_staleness_ms, "max_staleness_ms")?,
            row.replaces_cap_id,
            row.spend_nanos,
            row.unresolved_count,
            watermark_json,
            row.as_of_ms,
            i64::from(row.active),
            row.replaced_by,
        ],
    )
    .map_err(storage)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn watermark(epoch: u64, seq: u64) -> CapWatermark {
        CapWatermark {
            source_instance_id: "source-a".into(),
            source_epoch: epoch,
            consumed_export_seq: seq,
        }
    }

    fn install(cap_id: &str, version: u64, baseline_seq: u64) -> CapInstallRequest {
        CapInstallRequest {
            cap_id: cap_id.into(),
            cap_version: version,
            op: CapOperation::Install,
            scope_key: "subject".into(),
            project_root: None,
            constraint_dim: SPEND_DIMENSION.into(),
            period: "calendar_month".into(),
            period_tz: "Europe/Madrid".into(),
            hard_cap_nanos: 50,
            max_staleness_ms: 60_000,
            replaces_cap_id: None,
            baseline: Some(CapBaseline {
                spend_nanos: 10,
                unresolved_count: 0,
                watermark: watermark(1, baseline_seq),
                period_bucket: Some("2026-07".into()),
                as_of_ms: 1_789_000_000_000,
            }),
        }
    }

    fn delta(from: u64, to: u64, spend: i64, unresolved: i64) -> SpendDeltaRequest {
        SpendDeltaRequest {
            cap_id: "cap-a".into(),
            cap_version: 1,
            from_watermark: watermark(1, from),
            to_watermark: watermark(1, to),
            priced_delta_nanos: spend,
            unresolved_delta: unresolved,
            period_bucket: "2026-07".into(),
        }
    }

    fn indexed_cap_ids(store: &CapsStore) -> Vec<String> {
        let mirror = store.mirror.read().unwrap();
        let mut ids = mirror
            .active_by_scope
            .values()
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        ids.sort();
        ids
    }

    #[test]
    fn receiver_state_machine_covers_all_delta_arms_and_row_state() {
        let store = CapsStore::open_in_memory().unwrap();
        store.install_cap(&install("cap-a", 1, 10)).unwrap();

        let applied = store.apply_spend_delta(&delta(10, 11, 3, 1)).unwrap();
        assert_eq!(
            applied,
            CapMutation::Applied(AppliedCapWatermark {
                source_instance_id: "source-a".into(),
                source_epoch: 1,
                seq: 11,
            })
        );
        assert_eq!(store.row("cap-a").unwrap().spend_nanos, 13);
        assert_eq!(store.row("cap-a").unwrap().unresolved_count, 1);

        let behind = store.apply_spend_delta(&delta(10, 11, 99, 99)).unwrap();
        assert_eq!(
            behind,
            CapMutation::Applied(AppliedCapWatermark {
                source_instance_id: "source-a".into(),
                source_epoch: 1,
                seq: 11,
            })
        );
        assert_eq!(store.row("cap-a").unwrap().spend_nanos, 13);

        assert_eq!(
            store.apply_spend_delta(&delta(12, 13, 1, 0)).unwrap(),
            CapMutation::BaselineRequired
        );
        assert_eq!(store.row("cap-a").unwrap().spend_nanos, 13);

        assert_eq!(
            store.apply_spend_delta(&delta(10, 12, 1, 0)).unwrap(),
            CapMutation::BaselineRequired
        );
        assert_eq!(store.row("cap-a").unwrap().spend_nanos, 13);

        let mut epoch_mismatch = delta(11, 12, 1, 0);
        epoch_mismatch.from_watermark.source_epoch = 2;
        epoch_mismatch.to_watermark.source_epoch = 2;
        assert_eq!(
            store.apply_spend_delta(&epoch_mismatch).unwrap(),
            CapMutation::BaselineRequired
        );
        assert_eq!(
            store.row("cap-a").unwrap().applied_watermark,
            watermark(1, 11)
        );

        let zero = delta(11, 12, 0, 0);
        assert_eq!(
            store.apply_spend_delta(&zero).unwrap(),
            CapMutation::Applied(AppliedCapWatermark {
                source_instance_id: "source-a".into(),
                source_epoch: 1,
                seq: 12,
            })
        );
        assert_eq!(store.row("cap-a").unwrap().spend_nanos, 13);
        assert_eq!(
            store.row("cap-a").unwrap().applied_watermark,
            watermark(1, 12)
        );
    }

    #[test]
    fn negative_baseline_is_invalid_and_leaves_store_unchanged() {
        let store = CapsStore::open_in_memory().unwrap();
        let mut request = install("cap-a", 1, 10);
        request.baseline.as_mut().unwrap().unresolved_count = -1;

        assert!(matches!(
            store.install_cap(&request),
            Err(CapStoreError::Invalid(message))
                if message == "baseline.unresolved_count cannot be negative"
        ));
        assert!(store.row("cap-a").is_none());
        assert!(store
            .matching_caps("subject", Path::new("/project"))
            .is_empty());
    }

    #[test]
    fn unresolved_over_decrement_requires_baseline_and_reverts_row_and_mirror() {
        let store = CapsStore::open_in_memory().unwrap();
        let mut request = install("cap-a", 1, 10);
        request.baseline.as_mut().unwrap().unresolved_count = 1;
        store.install_cap(&request).unwrap();
        let before_row = store.row("cap-a").unwrap();
        let before_mirror = store.matching_caps("subject", Path::new("/project"));

        assert_eq!(
            store.apply_spend_delta(&delta(10, 11, 0, -2)).unwrap(),
            CapMutation::BaselineRequired
        );
        assert_eq!(store.row("cap-a").unwrap(), before_row);
        assert_eq!(
            store.matching_caps("subject", Path::new("/project")),
            before_mirror
        );
    }

    #[test]
    fn unresolved_decrement_to_exactly_zero_is_applied() {
        let store = CapsStore::open_in_memory().unwrap();
        let mut request = install("cap-a", 1, 10);
        request.baseline.as_mut().unwrap().unresolved_count = 1;
        store.install_cap(&request).unwrap();

        assert_eq!(
            store.apply_spend_delta(&delta(10, 11, 0, -1)).unwrap(),
            CapMutation::Applied(AppliedCapWatermark {
                source_instance_id: "source-a".into(),
                source_epoch: 1,
                seq: 11,
            })
        );
        assert_eq!(store.row("cap-a").unwrap().unresolved_count, 0);
        assert_eq!(
            store.matching_caps("subject", Path::new("/project"))[0].unresolved_count,
            0
        );
    }

    #[test]
    fn contract_correct_delta_uses_kind_and_instance_separately() {
        let store = CapsStore::open_in_memory().unwrap();
        store.install_cap(&install("cap-a", 1, 10)).unwrap();

        assert!(matches!(
            store.apply_spend_delta(&delta(10, 11, 3, 0)),
            Ok(CapMutation::Applied(_))
        ));
        assert_eq!(store.row("cap-a").unwrap().period, "calendar_month");
        assert_eq!(
            store.row("cap-a").unwrap().period_bucket.as_deref(),
            Some("2026-07")
        );
    }

    #[test]
    fn kind_as_bucket_is_invalid_and_leaves_the_row_unchanged() {
        let store = CapsStore::open_in_memory().unwrap();
        store.install_cap(&install("cap-a", 1, 10)).unwrap();
        let before = store.row("cap-a").unwrap();
        let mut request = delta(10, 11, 3, 0);
        request.period_bucket = "calendar_month".into();

        assert!(matches!(
            store.apply_spend_delta(&request),
            Err(CapStoreError::Invalid(_))
        ));
        assert_eq!(store.row("cap-a").unwrap(), before);
    }

    #[test]
    fn rollover_requires_a_fresh_baseline_for_the_new_bucket() {
        let store = CapsStore::open_in_memory().unwrap();
        store.install_cap(&install("cap-a", 1, 10)).unwrap();
        let before = store.row("cap-a").unwrap();
        let mut rollover = delta(10, 11, 3, 0);
        rollover.period_bucket = "2026-08".into();

        assert_eq!(
            store.apply_spend_delta(&rollover).unwrap(),
            CapMutation::BaselineRequired
        );
        assert_eq!(store.row("cap-a").unwrap(), before);

        let mut new_baseline = install("cap-a", 2, 10);
        new_baseline.baseline.as_mut().unwrap().period_bucket = Some("2026-08".into());
        store.install_cap(&new_baseline).unwrap();
        let mut new_period_delta = delta(10, 11, 3, 0);
        new_period_delta.cap_version = 2;
        new_period_delta.period_bucket = "2026-08".into();
        assert!(matches!(
            store.apply_spend_delta(&new_period_delta),
            Ok(CapMutation::Applied(_))
        ));
        assert_eq!(
            store.row("cap-a").unwrap().period_bucket.as_deref(),
            Some("2026-08")
        );
    }

    #[test]
    fn bucket_validation_supports_days_and_open_vocabulary_kinds() {
        assert!(validate_period_bucket("calendar_day", "2026-07-18").is_ok());
        assert!(validate_period_bucket("calendar_day", "2026-7-18").is_err());
        assert!(validate_period_bucket("fiscal_month", "2026-07").is_ok());
        assert!(validate_period_bucket("fiscal_month", "fiscal_month").is_err());
    }

    #[test]
    fn malformed_calendar_month_buckets_are_invalid() {
        for bucket in ["2026-7", "garbage"] {
            let store = CapsStore::open_in_memory().unwrap();
            store.install_cap(&install("cap-a", 1, 10)).unwrap();
            let before = store.row("cap-a").unwrap();
            let mut request = delta(10, 11, 3, 0);
            request.period_bucket = bucket.into();

            assert!(matches!(
                store.apply_spend_delta(&request),
                Err(CapStoreError::Invalid(_))
            ));
            assert_eq!(store.row("cap-a").unwrap(), before);
        }
    }

    #[test]
    fn absent_baseline_bucket_adopts_the_first_valid_delta_and_then_enforces_it() {
        let store = CapsStore::open_in_memory().unwrap();
        let mut request = install("cap-a", 1, 10);
        request.baseline.as_mut().unwrap().period_bucket = None;
        store.install_cap(&request).unwrap();
        assert_eq!(store.row("cap-a").unwrap().period_bucket, None);

        store.apply_spend_delta(&delta(10, 11, 3, 0)).unwrap();
        assert_eq!(
            store.row("cap-a").unwrap().period_bucket.as_deref(),
            Some("2026-07")
        );

        let mut different_bucket = delta(11, 12, 3, 0);
        different_bucket.period_bucket = "2026-08".into();
        assert_eq!(
            store.apply_spend_delta(&different_bucket).unwrap(),
            CapMutation::BaselineRequired
        );
        assert_eq!(store.row("cap-a").unwrap().spend_nanos, 13);
    }

    #[test]
    fn baseline_without_period_bucket_deserializes_for_old_clients() {
        let baseline: CapBaseline = serde_json::from_value(serde_json::json!({
            "spend_nanos": 10,
            "unresolved_count": 0,
            "watermark": {
                "source_instance_id": "source-a",
                "source_epoch": 1,
                "consumed_export_seq": 10
            },
            "as_of_ms": 1_789_000_000_000i64
        }))
        .unwrap();
        assert_eq!(baseline.period_bucket, None);
    }

    #[test]
    fn inactive_unrelated_history_stays_out_of_the_active_scope_index() {
        const HISTORY_LEN: usize = 32;

        let store = CapsStore::open_in_memory().unwrap();
        let mut previous = None;
        for index in 0..HISTORY_LEN {
            let cap_id = format!("historical-{index}");
            let mut request = install(&cap_id, 1, index as u64);
            request.scope_key = "unrelated".into();
            request.replaces_cap_id = previous;
            store.install_cap(&request).unwrap();
            previous = Some(cap_id);
        }
        let mut remove = install(previous.as_deref().unwrap(), 2, 0);
        remove.op = CapOperation::Remove;
        remove.baseline = None;
        store.install_cap(&remove).unwrap();

        store.install_cap(&install("cap-global", 1, 100)).unwrap();
        let mut project_cap = install("cap-project", 1, 101);
        project_cap.project_root = Some("/project".into());
        store.install_cap(&project_cap).unwrap();

        let mirror = store.mirror.read().unwrap();
        assert_eq!(mirror.by_id.len(), HISTORY_LEN + 2);
        assert_eq!(mirror.active_by_scope.len(), 2);
        assert_eq!(
            mirror.active_by_scope.values().map(Vec::len).sum::<usize>(),
            2
        );
        drop(mirror);
        assert_eq!(indexed_cap_ids(&store), ["cap-global", "cap-project"]);

        let mut matching = store.matching_caps("subject", Path::new("/project"));
        matching.sort_by(|left, right| left.cap_id.cmp(&right.cap_id));
        assert_eq!(
            matching
                .iter()
                .map(|row| row.cap_id.as_str())
                .collect::<Vec<_>>(),
            ["cap-global", "cap-project"]
        );
        assert!(store
            .matching_caps("unrelated", Path::new("/project"))
            .is_empty());
    }

    #[test]
    fn targeted_delta_publish_updates_admission_view_and_preserves_unrelated_rows() {
        let store = CapsStore::open_in_memory().unwrap();
        store.install_cap(&install("cap-a", 1, 10)).unwrap();
        store.install_cap(&install("cap-unrelated", 1, 20)).unwrap();

        store.apply_spend_delta(&delta(10, 11, 3, 0)).unwrap();

        let matching = store.matching_caps("subject", Path::new("/project"));
        assert_eq!(matching.len(), 2);
        assert_eq!(
            matching
                .iter()
                .find(|row| row.cap_id == "cap-a")
                .unwrap()
                .spend_nanos,
            13
        );
        assert_eq!(
            matching
                .iter()
                .find(|row| row.cap_id == "cap-unrelated")
                .unwrap()
                .spend_nanos,
            10
        );
    }

    #[test]
    fn targeted_replacement_publish_keeps_tombstone_and_matches_only_successor() {
        let store = CapsStore::open_in_memory().unwrap();
        store.install_cap(&install("cap-a", 1, 1)).unwrap();
        let mut replacement = install("cap-b", 1, 2);
        replacement.replaces_cap_id = Some("cap-a".into());

        store.install_cap(&replacement).unwrap();

        let retired = store.row("cap-a").unwrap();
        assert!(!retired.active);
        assert_eq!(retired.replaced_by.as_deref(), Some("cap-b"));
        assert!(store.row("cap-b").unwrap().active);
        let matching = store.matching_caps("subject", Path::new("/project"));
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].cap_id, "cap-b");
        assert_eq!(indexed_cap_ids(&store), ["cap-b"]);
    }

    #[test]
    fn targeted_remove_publish_keeps_inactive_row_out_of_admission_view() {
        let store = CapsStore::open_in_memory().unwrap();
        store.install_cap(&install("cap-a", 1, 1)).unwrap();
        let mut remove = install("cap-a", 2, 0);
        remove.op = CapOperation::Remove;
        remove.baseline = None;

        store.install_cap(&remove).unwrap();

        let retired = store.row("cap-a").unwrap();
        assert!(!retired.active);
        assert_eq!(retired.cap_version, 2);
        assert!(store
            .matching_caps("subject", Path::new("/project"))
            .is_empty());
        assert!(indexed_cap_ids(&store).is_empty());
    }

    #[test]
    fn conflicting_late_replacement_requires_baseline_and_preserves_successor() {
        let store = CapsStore::open_in_memory().unwrap();
        store.install_cap(&install("cap-a", 1, 1)).unwrap();

        let mut replacement_c = install("cap-c", 1, 2);
        replacement_c.replaces_cap_id = Some("cap-a".into());
        store.install_cap(&replacement_c).unwrap();

        let mut delayed_replacement_b = install("cap-b", 1, 3);
        delayed_replacement_b.replaces_cap_id = Some("cap-a".into());
        assert!(matches!(
            store.install_cap(&delayed_replacement_b),
            Err(CapStoreError::BaselineRequired)
        ));

        let retired = store.row("cap-a").unwrap();
        assert!(!retired.active);
        assert_eq!(retired.replaced_by.as_deref(), Some("cap-c"));
        assert!(store.row("cap-c").unwrap().active);
        assert!(store.row("cap-b").is_none());
    }

    #[test]
    fn replacement_with_absent_target_installs_without_a_tombstone() {
        let store = CapsStore::open_in_memory().unwrap();
        let mut replacement = install("cap-b", 1, 2);
        replacement.replaces_cap_id = Some("missing-cap".into());

        assert!(matches!(
            store.install_cap(&replacement),
            Ok(CapMutation::Applied(_))
        ));
        assert!(store.row("cap-b").unwrap().active);
        assert!(store.row("missing-cap").is_none());
    }

    /// The tombstone-resurrection sequence: a removed cap comes back at a higher version
    /// with a fresh baseline, and a remove redelivered from before that reinstall cannot
    /// retire it. Consumers reinstall under the same cap id after a removal, so the
    /// surviving row's version is what protects the live cap from a duplicate remove
    /// still in flight.
    #[test]
    fn reinstall_after_remove_survives_a_redelivered_earlier_remove() {
        let store = CapsStore::open_in_memory().unwrap();
        store.install_cap(&install("cap-a", 1, 1)).unwrap();

        let mut removed = install("cap-a", 2, 0);
        removed.op = CapOperation::Remove;
        removed.baseline = None;
        store.install_cap(&removed).unwrap();
        let tombstone = store.row("cap-a").unwrap();
        assert!(!tombstone.active, "remove leaves an inactive row");
        assert_eq!(tombstone.cap_version, 2);

        // Resurrection carries a complete baseline; it cannot inherit the tombstone's spend.
        let mut resurrected = install("cap-a", 3, 9);
        resurrected.hard_cap_nanos = 4_242;
        store.install_cap(&resurrected).unwrap();
        let live = store.row("cap-a").unwrap();
        assert!(live.active);
        assert_eq!(live.cap_version, 3);
        assert_eq!(live.hard_cap_nanos, 4_242);
        let baseline_watermark = live.applied_watermark.clone();
        let baseline_spend = live.spend_nanos;

        // The earlier remove is redelivered after the reinstall.
        let mut late_remove = install("cap-a", 2, 0);
        late_remove.op = CapOperation::Remove;
        late_remove.baseline = None;
        assert!(matches!(
            store.install_cap(&late_remove),
            Ok(CapMutation::Applied(_))
        ));

        let after = store.row("cap-a").unwrap();
        assert!(after.active, "a stale remove must not retire the live cap");
        assert_eq!(after.cap_version, 3);
        assert_eq!(after.hard_cap_nanos, 4_242);
        assert_eq!(after.spend_nanos, baseline_spend);
        assert_eq!(after.applied_watermark, baseline_watermark);
    }

    #[test]
    fn replacement_transaction_survives_late_remove_of_old_cap() {
        let store = CapsStore::open_in_memory().unwrap();
        store.install_cap(&install("cap-a", 1, 1)).unwrap();
        let mut replacement = install("cap-b", 1, 2);
        replacement.replaces_cap_id = Some("cap-a".into());
        store.install_cap(&replacement).unwrap();

        let mut remove_a = install("cap-a", 2, 0);
        remove_a.op = CapOperation::Remove;
        remove_a.baseline = None;
        assert!(matches!(
            store.install_cap(&remove_a),
            Ok(CapMutation::Applied(_))
        ));
        assert!(!store.row("cap-a").unwrap().active);
        assert!(store.row("cap-a").unwrap().replaced_by.is_some());
        assert!(store.row("cap-b").unwrap().active);

        let mut remove_b = install("cap-b", 2, 0);
        remove_b.op = CapOperation::Remove;
        remove_b.baseline = None;
        store.install_cap(&remove_b).unwrap();
        assert!(!store.row("cap-b").unwrap().active);
    }

    #[test]
    fn lower_version_is_an_idempotent_ack_and_restart_preserves_watermark() {
        let path = tempfile_path();
        {
            let store = CapsStore::open(&path).unwrap();
            store.install_cap(&install("cap-a", 4, 40)).unwrap();
            let mut applied_delta = delta(40, 41, 7, 0);
            applied_delta.cap_version = 4;
            store.apply_spend_delta(&applied_delta).unwrap();
            let mut lower = install("cap-a", 3, 0);
            lower.baseline = None;
            assert!(matches!(
                store.install_cap(&lower),
                Ok(CapMutation::Applied(_))
            ));
        }
        {
            let store = CapsStore::open(&path).unwrap();
            assert_eq!(store.row("cap-a").unwrap().spend_nanos, 17);
            assert_eq!(indexed_cap_ids(&store), ["cap-a"]);
            assert_eq!(
                store.row("cap-a").unwrap().applied_watermark,
                watermark(1, 41)
            );
            let mut late_delta = delta(40, 41, 7, 0);
            late_delta.cap_version = 4;
            assert!(matches!(
                store.apply_spend_delta(&late_delta).unwrap(),
                CapMutation::Applied(_)
            ));
            assert_eq!(store.row("cap-a").unwrap().spend_nanos, 17);
        }
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    /// A delta stamped with a version NEWER than the installed row is refused, and the
    /// predecessor row is left exactly as it was.
    ///
    /// This is what makes a changed-cap cutover safe to build: the pending window can contain
    /// outbox rows already stamped with a successor version before that version's `cap.install`
    /// is acknowledged, and those arrive here first. Refusing them means a premature successor
    /// delta is a recoverable no-op rather than silent corruption of the predecessor's accounting.
    #[test]
    fn a_newer_version_delta_is_refused_and_leaves_the_row_untouched() {
        let path = tempfile_path();
        let store = CapsStore::open(&path).unwrap();
        store.install_cap(&install("cap-a", 4, 40)).unwrap();

        let before = store.row("cap-a").unwrap();
        let mut successor = delta(40, 41, 7, 0);
        successor.cap_version = 5;

        assert!(
            matches!(
                store.apply_spend_delta(&successor).unwrap(),
                CapMutation::BaselineRequired
            ),
            "a delta from a version this receiver has not installed must demand a baseline"
        );

        let after = store.row("cap-a").unwrap();
        assert_eq!(after.spend_nanos, before.spend_nanos, "spend must not move");
        assert_eq!(
            after.applied_watermark, before.applied_watermark,
            "the watermark must not advance"
        );
        assert_eq!(
            after.cap_version, before.cap_version,
            "the row keeps its version"
        );
        cleanup(&path);
    }

    /// A delta stamped with a version OLDER than the installed row is a no-op that reports
    /// success -- and the watermark it echoes is the row's CURRENT one, never the delta's own
    /// `to_watermark`.
    ///
    /// The echoed value is the load-bearing part. A caller that treats `Applied` as progress and
    /// deletes its outbox prefix up to the watermark it SENT would discard rows this receiver
    /// never consumed. The variant says "your message was handled"; only the watermark says how
    /// far the receiver actually got.
    #[test]
    fn an_older_version_delta_acks_the_installed_watermark_not_its_own() {
        let path = tempfile_path();
        let store = CapsStore::open(&path).unwrap();
        store.install_cap(&install("cap-a", 4, 40)).unwrap();

        let mut stale = delta(40, 41, 7, 0);
        stale.cap_version = 3;

        let CapMutation::Applied(acked) = store.apply_spend_delta(&stale).unwrap() else {
            panic!("a stale-version delta is acknowledged, not refused");
        };
        assert_eq!(
            acked.seq, 40,
            "the ack carries the watermark the receiver is actually at, not the 41 the delta asked for"
        );
        assert_eq!(
            store.row("cap-a").unwrap().spend_nanos,
            10,
            "and nothing was counted"
        );
        cleanup(&path);
    }

    /// Version is checked BEFORE the contiguous-prefix rule, so a stale-version delta is
    /// acknowledged even when its sequence would otherwise be refused.
    ///
    /// Asserted through a delta that is wrong on both axes: were the order reversed, the sequence
    /// gap would produce `BaselineRequired` instead, so this distinguishes the two orderings
    /// rather than merely exercising them.
    #[test]
    fn version_is_checked_before_the_sequence_prefix() {
        let path = tempfile_path();
        let store = CapsStore::open(&path).unwrap();
        store.install_cap(&install("cap-a", 4, 40)).unwrap();

        let mut stale_and_gapped = delta(99, 100, 7, 0);
        stale_and_gapped.cap_version = 3;

        assert!(
            matches!(
                store.apply_spend_delta(&stale_and_gapped).unwrap(),
                CapMutation::Applied(_)
            ),
            "the version arm decides first; the sequence gap is never reached"
        );
        cleanup(&path);
    }

    /// Lineage identity is checked before version, so a foreign-stream delta is refused rather
    /// than acknowledged as a stale version.
    #[cfg(unix)]
    #[test]
    fn database_and_sqlite_sidecars_are_created_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let path = tempfile_path();
        let store = CapsStore::open(&path).unwrap();
        store.install_cap(&install("cap-a", 1, 10)).unwrap();
        let entries = [
            path.clone(),
            PathBuf::from(format!("{}-wal", path.display())),
            PathBuf::from(format!("{}-shm", path.display())),
        ];
        let offenders: Vec<_> = entries
            .iter()
            .filter_map(|entry| {
                let mode = std::fs::metadata(entry).unwrap().permissions().mode() & 0o777;
                (mode != 0o600).then(|| format!("{}: {mode:o}", entry.display()))
            })
            .collect();
        assert!(
            offenders.is_empty(),
            "database entries not owner-only: {offenders:?}"
        );
        drop(store);
        cleanup(&path);
    }

    #[test]
    fn lineage_identity_is_checked_before_version() {
        let path = tempfile_path();
        let store = CapsStore::open(&path).unwrap();
        store.install_cap(&install("cap-a", 4, 40)).unwrap();

        let mut foreign = delta(40, 41, 7, 0);
        foreign.cap_version = 3;
        foreign.from_watermark.source_instance_id = "source-b".into();
        foreign.to_watermark.source_instance_id = "source-b".into();

        assert!(
            matches!(
                store.apply_spend_delta(&foreign).unwrap(),
                CapMutation::BaselineRequired
            ),
            "identity decides first; a foreign stream is never acknowledged as a stale version"
        );
        cleanup(&path);
    }

    fn cleanup(path: &PathBuf) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    fn tempfile_path() -> PathBuf {
        let path = broca_test_support::scratch_root().join(format!(
            "broca-caps-{}-{}.db",
            std::process::id(),
            unique_id()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    /// A process-unique suffix for a test fixture path.
    ///
    /// This was a wall-clock nanosecond reading, which is not a uniqueness source: the
    /// clock advertises nanosecond units but does not tick at that resolution, so 98% of
    /// consecutive reads on this host return an IDENTICAL value. Two tests entering this
    /// helper inside the same tick got the same fixture path, and the second one's
    /// `remove_file` deleted the database the first was still using -- which surfaced as a
    /// rare assertion failure in whichever test lost the race, never in the same test
    /// twice, and never on a re-run.
    ///
    /// A counter cannot collide by construction, so the resolution question does not arise.
    fn unique_id() -> u64 {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}
