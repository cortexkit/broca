//! SQLite usage export index for ASTRO metering.
//!
//! Facts are appended only after a run's terminal WAL record is durable.  The
//! coordinator then calls this index after the run-index terminal update; those
//! writes cannot share a transaction because the WAL and SQLite run index have
//! different durability owners. A crash between the two writes leaves a
//! re-derivable fact missing, while the terminal run index supplies catch-up work
//! and `fact_id` makes replay idempotent.

use std::sync::atomic::{AtomicU64, Ordering};

use broca_core::{
    compute_segments_for_run, ChargeBasisResolver, ExportCursor, ExportError, ExportFact,
    ExportIndex, ExportPage, MissingExportRun,
};
use broca_protocol::RunId;
use broca_wal::FramedRecord;
use rusqlite::{Connection, OptionalExtension};

use super::SqliteStore;

pub(crate) fn init_schema(conn: &Connection) -> Result<(String, u64, u64), rusqlite::Error> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS export_meta (
             id                  INTEGER PRIMARY KEY CHECK (id = 1),
             source_epoch       INTEGER NOT NULL DEFAULT 1,
             source_instance_id TEXT NOT NULL,
             high_water         INTEGER NOT NULL DEFAULT 0
         );
         CREATE TABLE IF NOT EXISTS export_facts (
             export_seq   INTEGER PRIMARY KEY AUTOINCREMENT,
             fact_id      TEXT NOT NULL UNIQUE,
             run_id       TEXT NOT NULL,
             segment_json TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS export_imported_runs (
             session TEXT NOT NULL,
             run_id  TEXT NOT NULL,
             cut_id  TEXT NOT NULL,
             PRIMARY KEY (session, run_id, cut_id)
         );",
    )?;
    // Older deployments wrote per-step projections that consumers now read from WALs.
    // Remove this once no deployment could still carry the table. Rolling back to v0.3.125 or v0.3.126
    // recreates it empty on open, which is harmless.
    conn.execute_batch("DROP TABLE IF EXISTS export_steps;")?;

    // Reopening an already-migrated database finds no session column and skips the ALTER.
    let has_session_column = {
        let mut statement = conn.prepare("PRAGMA table_info(export_facts)")?;
        let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
        columns
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .any(|column| column == "session")
    };
    if has_session_column {
        // The complete session remains inside segment_json; this removes only its duplicate copy.
        conn.execute("ALTER TABLE export_facts DROP COLUMN session", [])?;
    }

    // Older schemas added a second fact_id index even though UNIQUE already owns one.
    // Drop only that named index on open; the uniqueness constraint remains authoritative.
    conn.execute_batch(
        "DROP INDEX IF EXISTS export_facts_fact_id;
         CREATE INDEX IF NOT EXISTS export_facts_run_id ON export_facts(run_id);",
    )?;

    let source_instance_id = conn
        .query_row(
            "SELECT source_instance_id FROM export_meta WHERE id=1",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if source_instance_id.is_none() {
        conn.execute(
            "INSERT INTO export_meta (id, source_epoch, source_instance_id, high_water)
             VALUES (1, 1, ?1, 0)",
            [mint_source_instance_id()],
        )?;
    }
    conn.query_row(
        "SELECT source_instance_id, source_epoch, high_water FROM export_meta WHERE id=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
}

fn mint_source_instance_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos() as u64);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let high = nanos ^ counter.rotate_left(17);
    let low =
        (u64::from(std::process::id()) << 32) ^ nanos.rotate_left(29) ^ counter.rotate_left(41);
    format!(
        "{:08x}-{:04x}-4{:03x}-8{:03x}-{:012x}",
        high >> 32,
        (high >> 16) & 0xffff,
        high & 0x0fff,
        (low >> 48) & 0x0fff,
        low & 0xffffffffffff,
    )
}

struct ResolverRef<'a>(&'a dyn ChargeBasisResolver);

impl ChargeBasisResolver for ResolverRef<'_> {
    fn resolve_charge_basis(
        &self,
        provider_id: &str,
        method: &str,
    ) -> (broca_core::ChargeBasis, u32) {
        self.0.resolve_charge_basis(provider_id, method)
    }

    fn charge_basis_overrides(&self) -> String {
        self.0.charge_basis_overrides()
    }
}

fn storage(error: impl std::fmt::Display) -> ExportError {
    ExportError::Storage(error.to_string())
}

fn i64_to_u64(value: i64, field: &str) -> Result<u64, ExportError> {
    u64::try_from(value).map_err(|_| storage(format!("{field} is negative: {value}")))
}

fn u64_to_i64(value: u64, field: &str) -> Result<i64, ExportError> {
    i64::try_from(value).map_err(|_| storage(format!("{field} exceeds SQLite INTEGER")))
}

impl SqliteStore {
    /// Publish a sessionless measurement once. The caller's durable outbox keeps these exact
    /// bytes until this transaction commits, so a crash between databases can only replay it.
    pub fn emit_system_one_fact(
        &self,
        fact: &broca_core::system_one_fact::SystemOneFact,
    ) -> Result<bool, ExportError> {
        let json = serde_json::to_string(fact).map_err(storage)?;
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| storage("store mutex poisoned"))?;
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(storage)?;
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO export_facts (fact_id, run_id, segment_json) VALUES (?1, ?2, ?3)",
            rusqlite::params![fact.fact_id, fact.run_id.as_deref().unwrap_or(""), json],
        ).map_err(storage)?;
        let stored: String = tx
            .query_row(
                "SELECT segment_json FROM export_facts WHERE fact_id=?1",
                [&fact.fact_id],
                |row| row.get(0),
            )
            .map_err(storage)?;
        if stored != json {
            return Err(storage(
                "System One fact identity collided with different immutable bytes",
            ));
        }
        let high_water: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(export_seq), 0) FROM export_facts",
                [],
                |row| row.get(0),
            )
            .map_err(storage)?;
        tx.execute(
            "UPDATE export_meta SET high_water=?1 WHERE id=1",
            [high_water],
        )
        .map_err(storage)?;
        tx.commit().map_err(storage)?;
        self.export_high_water
            .fetch_max(i64_to_u64(high_water, "high_water")?, Ordering::Release);
        Ok(inserted != 0)
    }

    /// The current export high-water mark cached from `export_meta` at open and
    /// advanced after every committed fact batch.
    pub fn high_water(&self) -> u64 {
        self.export_high_water.load(Ordering::Acquire)
    }

    /// Explicit alias for callers that want to distinguish this from a session
    /// projection watermark.
    pub fn export_high_water(&self) -> u64 {
        self.high_water()
    }

    /// Open-time source identity cache. Admission reads these getters without acquiring the
    /// SQLite connection lock; a future rebuild must publish a replacement store instance.
    pub fn source_epoch(&self) -> u64 {
        self.export_source_epoch
    }

    pub fn source_instance_id(&self) -> &str {
        &self.export_source_instance_id
    }

    /// Read one epoch-guarded page from one SQLite transaction.
    pub fn read_export_page(
        &self,
        since: Option<ExportCursor>,
        limit: u64,
    ) -> Result<ExportPage, ExportError> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| storage("store mutex poisoned"))?;
        let tx = conn.transaction().map_err(storage)?;
        let (source_epoch, source_instance_id, high_water) = tx
            .query_row(
                "SELECT source_epoch, source_instance_id, high_water
                   FROM export_meta WHERE id=1",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .map_err(storage)?;
        let source_epoch = i64_to_u64(source_epoch, "source_epoch")?;
        let high_water = i64_to_u64(high_water, "high_water")?;
        if let Some(cursor) = since.as_ref() {
            if cursor.source_instance_id != source_instance_id
                || cursor.source_epoch != source_epoch
            {
                tx.rollback().map_err(storage)?;
                return Err(ExportError::EpochChanged {
                    source_instance_id,
                    source_epoch,
                    high_water,
                });
            }
        }

        let after = since.as_ref().map_or(0, |cursor| cursor.export_seq);
        let after = u64_to_i64(after, "since.export_seq")?;
        let limit = u64_to_i64(limit.min(1_000), "limit")?;
        let mut statement = tx
            .prepare(
                "SELECT export_seq, segment_json FROM export_facts
                   WHERE export_seq > ?1 ORDER BY export_seq ASC LIMIT ?2",
            )
            .map_err(storage)?;
        let rows = statement
            .query_map(rusqlite::params![after, limit], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(storage)?;

        let mut facts = Vec::new();
        for row in rows {
            let (export_seq, segment_json) = row.map_err(storage)?;
            let segment = serde_json::from_str(&segment_json).map_err(storage)?;
            facts.push(ExportFact {
                export_seq: i64_to_u64(export_seq, "export_seq")?,
                segment,
            });
        }
        drop(statement);
        let next = facts.last().map(|fact| ExportCursor {
            source_instance_id: source_instance_id.clone(),
            source_epoch,
            export_seq: fact.export_seq,
        });
        tx.commit().map_err(storage)?;
        Ok(ExportPage {
            source_instance_id,
            source_epoch,
            facts,
            next,
            high_water,
        })
    }

    /// Compute and append only the segments belonging to `run_id`.  The input
    /// is the complete session WAL so refreeze boundaries and terminal metadata
    /// are interpreted by the same pure billing function used elsewhere.
    pub fn emit_export_run(
        &self,
        records: &[FramedRecord],
        run_id: &RunId,
        resolver: &dyn ChargeBasisResolver,
    ) -> Result<(), ExportError> {
        let source_instance_id = self.source_instance_id();
        let resolver = ResolverRef(resolver);
        let segments = compute_segments_for_run(records, run_id, source_instance_id, &resolver);
        if segments.is_empty() {
            return Ok(());
        }

        let mut conn = self
            .conn
            .lock()
            .map_err(|_| storage("store mutex poisoned"))?;
        // Reserve SQLite's database writer before reading the imported-run set,
        // so another connection cannot commit a floor between this read and insertion.
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(storage)?;
        for segment in segments {
            let session_json = serde_json::to_string(&segment.session).map_err(storage)?;
            let imported: bool = tx
                .query_row(
                    "SELECT EXISTS (SELECT 1 FROM export_imported_runs
                                    WHERE session=?1 AND run_id=?2)",
                    rusqlite::params![session_json, segment.run_id.0],
                    |row| row.get(0),
                )
                .map_err(storage)?;
            if imported {
                continue;
            }
            let segment_json = serde_json::to_string(&segment).map_err(storage)?;
            tx.execute(
                "INSERT OR IGNORE INTO export_facts
                    (fact_id, run_id, segment_json) VALUES (?1, ?2, ?3)",
                rusqlite::params![segment.fact_id, segment.run_id.0, segment_json],
            )
            .map_err(storage)?;

            let stored = tx
                .query_row(
                    "SELECT segment_json FROM export_facts WHERE fact_id=?1",
                    [&segment.fact_id],
                    |row| row.get::<_, String>(0),
                )
                .map_err(storage)?;
            // A DISAGREEMENT HAS TWO CAUSES AND ONLY ONE OF THEM NEEDS ANYONE.
            //
            // A shipped fact is immutable, so re-deriving one always produces today's segment
            // shape against yesterday's stored bytes. Every additive field since that fact was
            // written -- `usage_policy_version`, `account_ref`, `resolved_credential_id` -- makes
            // the comparison differ FOR A REASON THAT REQUIRES NO ACTION, and it differs again on
            // every reopen, forever. Measured: 5 log lines over 2 distinct facts, one of them 4
            // times, both predating fields that now exist, on a session that reopens.
            //
            // That is the shape that teaches a reader to skip the words HARD WARNING. The value
            // difference is the one worth keeping loud: same identity, same field, different
            // answer, which means a derivation changed under a fact that cannot change.
            match classify_collision(&stored, &segment_json) {
                CollisionKind::Identical => {}
                CollisionKind::SchemaEvolution => tracing::info!(
                    target: "store",
                    "immutable usage fact {} predates a field the current segment adds; \
                     keeping shipped fact (expected after any additive segment change)",
                    segment.fact_id
                ),
                CollisionKind::EquivalentBytes => tracing::info!(
                    target: "store",
                    "immutable usage fact {} re-derives to the same data in different bytes; \
                     keeping shipped fact (a serialization change, not a schema change)",
                    segment.fact_id
                ),
                CollisionKind::DerivationVersionChange { stamps, added } => tracing::info!(
                    target: "store",
                    "immutable usage fact {} re-derives with moved derivation stamp(s) {}{}; \
                     keeping shipped fact (expected after the shipped value for those moved)",
                    segment.fact_id,
                    stamps.join(", "),
                    if added.is_empty() {
                        String::new()
                    } else {
                        format!(" and added field(s) {}", added.join(", "))
                    }
                ),
                CollisionKind::ValueDisagreement { fields } => tracing::warn!(
                    target: "store",
                    "HARD WARNING: immutable usage fact {} disagrees on {}; keeping \
                     shipped fact. Same identity and a different answer means a derivation \
                     moved under a fact that cannot -- this is NOT the expected \
                     added-field case. EVERY differing key is listed, so a derivation stamp \
                     appearing beside a real field is incidental rather than the whole story",
                    segment.fact_id,
                    fields.join(", ")
                ),
            }
        }

        let high_water = tx
            .query_row(
                "SELECT COALESCE(MAX(export_seq), 0) FROM export_facts",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map_err(storage)?;
        tx.execute(
            "UPDATE export_meta SET high_water=?1 WHERE id=1",
            [high_water],
        )
        .map_err(storage)?;
        tx.commit().map_err(storage)?;
        let high_water = i64_to_u64(high_water, "high_water")?;
        self.export_high_water
            .fetch_max(high_water, Ordering::Release);
        Ok(())
    }
}

/// Why a re-derived segment differs from the one already shipped under its `fact_id`.
#[derive(Debug, PartialEq)]
enum CollisionKind {
    Identical,
    /// Every difference is a key the stored fact does not carry: the fact predates an additive
    /// segment field. Expected, actionless, and guaranteed to recur on every re-derivation.
    SchemaEvolution,
    /// A key present in BOTH carries a different value, or the stored fact has a key the current
    /// segment no longer emits. Either way a derivation moved under an immutable fact.
    ///
    /// CARRIES EVERY DIFFERING KEY. One field would be the alphabetically-first difference
    /// wearing the clothes of a diagnosis.
    ValueDisagreement {
        fields: Vec<String>,
    },
    /// The two segments carry the SAME DATA in different bytes -- no key differs and none was
    /// added. A serialization change under an immutable fact, which loses nothing and is not
    /// the added-field case it would otherwise be reported as.
    EquivalentBytes,
    /// Every difference is a key recording HOW the fact was computed rather than WHAT was
    /// measured, and the shipped value for it has moved since. Expected, actionless, and
    /// guaranteed to recur on every re-derivation of every fact older than that change.
    DerivationVersionChange {
        stamps: Vec<String>,
        /// Keys the current segment adds. Carried so the message cannot say ONLY when an
        /// addition rode along -- which is the production case, not a corner.
        added: Vec<String>,
    },
}

/// Keys whose divergence on a re-derivation is EXPECTED AND ACTIONLESS, because they record
/// HOW a fact was computed rather than WHAT was measured.
///
/// `charge_basis_mapping_version` is the measured instance. The shipped constant went 3 -> 4 on
/// 2026-09-18 and the stamp is resolved from the CURRENT table at close_segment
/// (`billing.rs:208`), not restored from the frozen WAL config -- so re-deriving any fact older
/// than that change produces a difference here. Measured on production: 38,201 of 39,290 facts
/// (97.2%) carry a version other than the shipped one.
///
/// THE ORIGINAL LIST OF ONE WAS MEASURED COMPLETE, not merely the field that happened to be found.
/// Swept every candidate stamp over all 39,311 production facts, asking which would differ if
/// re-derived today:
///
///   charge_basis_mapping_version   4 distinct (1,2,3,4) vs shipped 4   <- MOVES, whitelisted
///   usage_policy_version           derived from the FROZEN config at run start, so an old
///                                  fact re-derives to its own recorded value   <- stable
///   pricing_at_ms / _precision     from `freeze_ts_ms.or(terminal_ts_ms)`, both WAL facts
///   occurred_at_precision          one distinct value across the whole corpus
///
/// That matters because a second moving constant outside this list would take the loud arm and
/// reintroduce the 97.2% noise floor under a different name, which is the failure this whole
/// change exists to remove.
///
/// ADDING A KEY HERE NEEDS THE SAME EVIDENCE: a measurement showing it moves for a reason that
/// needs nobody, not an argument that it looks like provenance. The whole point of the loud arm
/// is that a value differing under an immutable identity is worth waking someone for, and every
/// key added here is one that can no longer do that.
///
/// `charge_basis_overrides` is the second key, admitted on that standard. It is `"none"` or a
/// digest of the local charge-basis override rows in `broca.jsonc`, resolved from the CURRENT
/// table at close_segment exactly like the version:
///
///   - on a deployment with no override it is always `"none"`, so it cannot move there (the
///     production deployment has none);
///   - on one with an override it moves exactly when the override rows change, which is the
///     same kind of event as the version moving with the shipped table;
///   - a change that affects the fact's own answer still surfaces loudly, through the
///     `charge_basis` value itself, which is not a stamp. A digest moving beside a changed
///     `charge_basis` is therefore still a value disagreement.
///
/// A stored fact written before the key existed lacks it entirely; that re-derivation is the
/// added-field case, not this one.
const EXPECTED_DERIVATION_STAMPS: [&str; 2] =
    ["charge_basis_mapping_version", "charge_basis_overrides"];

/// Classify by KEY PRESENCE, not by byte equality.
///
/// Deliberately not a recursive deep-diff: the segment is flat enough that a nested change lands
/// on a top-level key whose value differs, which is already the loud arm. A deep-diff would let a
/// nested ADDED field read as evolution, and nesting is where an unreviewed shape change hides.
///
/// REPORTS EVERY DIFFERING KEY, NEVER THE FIRST. The first version of this returned on the
/// earliest difference in iteration order, and `serde_json::Map` is a `BTreeMap` here (no
/// `preserve_order` feature in the lock), so that order is lexicographic. With
/// `charge_basis_mapping_version` sorting 4th of 18 and differing on 97.2% of facts, the warning
/// was pinned to that stamp and could never have named a disagreement on `usage` (18th),
/// `provider` (11th) or `model` (6th). A message naming ONE field reads as a precise diagnosis
/// while being the alphabetically-first difference, which is worse than naming none.
fn classify_collision(stored: &str, current: &str) -> CollisionKind {
    if stored == current {
        return CollisionKind::Identical;
    }
    let (Ok(serde_json::Value::Object(stored)), Ok(serde_json::Value::Object(current))) = (
        serde_json::from_str::<serde_json::Value>(stored),
        serde_json::from_str::<serde_json::Value>(current),
    ) else {
        // Unparseable or non-object on either side is not an added field. Fail toward loud.
        return CollisionKind::ValueDisagreement {
            fields: vec!["<unparseable segment>".to_string()],
        };
    };
    let mut differing = Vec::new();
    for (key, stored_value) in &stored {
        match current.get(key) {
            Some(value) if value == stored_value => {}
            // Present in both with different answers, OR carried by the stored fact and dropped
            // by the current segment. A dropped key is not the benign direction: it means today's
            // derivation would ship LESS than what already shipped under this identity.
            Some(_) | None => differing.push(key.clone()),
        }
    }
    if differing.is_empty() {
        // NO STORED KEY DIFFERS. That is two facts, not one: the current segment ADDED keys, or
        // the two maps are equal and only their BYTES differ. Reporting the second as
        // "predates a field the current segment adds" would name a schema change that did not
        // happen, and send a reader looking for it.
        //
        // Unreachable today and ONE FEATURE FLAG AWAY: `serde_json::Map` is a `BTreeMap` here,
        // so keys always serialize sorted and an equal map cannot produce different bytes.
        // Enabling `preserve_order` anywhere in the tree makes key order insertion order, and
        // this branch starts lying on the billing path. Checked rather than assumed, because
        // the invariant lives in another crate's feature resolution.
        if current.keys().any(|key| !stored.contains_key(key)) {
            return CollisionKind::SchemaEvolution;
        }
        return CollisionKind::EquivalentBytes;
    }
    // Only when EVERY difference is an expected stamp. One real disagreement alongside a stamp
    // is still a real disagreement, and it is reported with the stamps beside it -- the full set
    // is what tells a reader whether the stamp is incidental or the whole story.
    if differing
        .iter()
        .all(|key| EXPECTED_DERIVATION_STAMPS.contains(&key.as_str()))
    {
        // An addition riding along with a moved stamp is the MEASURED production shape, not a
        // corner: the two colliding facts predate `resolved_credential_id` AND carry mapping
        // version 3. Carrying it means the sentence can say what happened instead of "only".
        let added = current
            .keys()
            .filter(|key| !stored.contains_key(*key))
            .cloned()
            .collect();
        return CollisionKind::DerivationVersionChange {
            stamps: differing,
            added,
        };
    }
    CollisionKind::ValueDisagreement { fields: differing }
}

impl ExportIndex for SqliteStore {
    fn record_imported_runs(
        &self,
        session: &broca_protocol::BindIdentity,
        cut_id: &str,
        run_ids: &[RunId],
    ) -> Result<(), ExportError> {
        let session_json = serde_json::to_string(session).map_err(storage)?;
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| storage("store mutex poisoned"))?;
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(storage)?;
        for run_id in run_ids {
            tx.execute(
                "INSERT OR IGNORE INTO export_imported_runs (session, run_id, cut_id)
                 VALUES (?1, ?2, ?3)",
                rusqlite::params![session_json, run_id.0, cut_id],
            )
            .map_err(storage)?;
        }
        tx.commit().map_err(storage)
    }

    fn missing_terminal_runs(&self) -> Result<Vec<MissingExportRun>, ExportError> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| storage("store mutex poisoned"))?;
        let mut statement = conn
            .prepare(
                "SELECT runs.run_id, runs.session
                   FROM run_index AS runs
                  WHERE runs.state NOT IN ('active', 'paused')
                    AND runs.expired_at_ms IS NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM export_facts AS facts WHERE facts.run_id = runs.run_id
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM export_imported_runs AS imported
                        WHERE imported.session = runs.session AND imported.run_id = runs.run_id
                    )
                  ORDER BY runs.run_id ASC",
            )
            .map_err(storage)?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(storage)?;
        let mut missing = Vec::new();
        for row in rows {
            let (run_id, session_json) = row.map_err(storage)?;
            let session = serde_json::from_str(&session_json).map_err(storage)?;
            missing.push(MissingExportRun {
                run_id: RunId(run_id),
                session,
            });
        }
        Ok(missing)
    }

    fn emit_run(
        &self,
        records: &[FramedRecord],
        run_id: &RunId,
        resolver: &dyn ChargeBasisResolver,
    ) -> Result<(), ExportError> {
        self.emit_export_run(records, run_id, resolver)
    }

    fn read_page(
        &self,
        since: Option<ExportCursor>,
        limit: u64,
    ) -> Result<ExportPage, ExportError> {
        self.read_export_page(since, limit)
    }

    fn high_water(&self) -> u64 {
        self.high_water()
    }

    fn source_epoch(&self) -> u64 {
        self.source_epoch()
    }

    fn source_instance_id(&self) -> &str {
        self.source_instance_id()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A process-unique suffix for a test fixture path.
    ///
    /// Deliberately a counter rather than a wall-clock reading: the clock advertises
    /// nanosecond units but does not tick at that resolution, so two tests entering a
    /// helper inside the same tick receive the same path and the second one's cleanup
    /// deletes the first one's fixture. That surfaces as a rare failure in whichever test
    /// loses the race, never the same one twice.
    fn test_fixture_nonce() -> u64 {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
    use broca_core::{
        catch_up_usage_exports, ChargeBasis, FileWalStore, RunIndex, RunIndexRow, RunIndexState,
        WalStore,
    };
    use broca_protocol::{
        caller::RunFinishReason,
        content::{ContentBlock, Message, Role},
        request::{GenerationConfig, ModelHandle, ResponseFormat, ToolChoice},
        BindIdentity, RunId, StepId, Usage,
    };
    use broca_wal::{AuthSelection, RunConfig, RunOrigin, WalRecord};
    use serde_json::Value;

    fn with_billing_floor_store(test: impl FnOnce(&dyn Fn() -> SqliteStore)) {
        let root = broca_test_support::scratch_root().join(format!(
            "broca-billing-floor-{}-{}",
            std::process::id(),
            test_fixture_nonce()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let db = root.join("store.db");
        test(&|| SqliteStore::open(&db).unwrap());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn billing_floor_blocks_imported_emission_after_reopen_and_projection_drop() {
        with_billing_floor_store(|make| crate::conformance::billing_floor_blocks_emission(make));
    }

    #[test]
    fn billing_floor_excludes_imported_runs_from_backfill_and_retention() {
        with_billing_floor_store(|make| crate::conformance::billing_floor_excludes_backfill(make));
    }

    #[test]
    fn billing_floor_bills_resumed_local_and_post_activation_runs() {
        with_billing_floor_store(|make| {
            crate::conformance::billing_floor_preserves_local_and_new_runs(make)
        });
    }

    fn config(usage_policy: &str) -> RunConfig {
        RunConfig {
            turn_directives_v1: None,
            render_tags: false,
            computed_tags: None,
            model: ModelHandle::new("xai", "grok-4"),
            tools: Vec::new().into(),
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig::default(),
            response_format: ResponseFormat::default(),
            provider_options: Value::Null,
            cache_tiers: Value::Null,
            context_epoch: 0,
            renderer_version: 1,
            frozen_render_config: serde_json::json!({
                "family_id": "openai_chat",
                "renderer_version": 1,
                "wire_model_id": "grok-4",
                "max_output_tokens": 4096,
                "reasoning": {"mode": "none"},
                "tool_id": "passthrough",
                "system_prompt": {"mode": "system_message"},
                "usage": usage_policy,
                "token_limit_field": "max_tokens"
            }),
            auth_selection: Some(AuthSelection {
                credential_id: "apikey:xai:work".into(),
                account_ref: None,
                resolved_credential_id: None,
                vault_request: None,
                account_decision: None,
            }),
            tools_were_omitted: false,
            budget: None,
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: Vec::new(),
            context_management: None,
        }
    }

    fn records(run_id: &str, segments: usize) -> Vec<FramedRecord> {
        records_with_usage_policy(
            run_id,
            segments,
            "open_ai_usage_reasoning_separate",
            BindIdentity::new("/tmp/export", "runner", "metered"),
        )
    }

    fn records_with_usage_policy(
        run_id: &str,
        segments: usize,
        usage_policy: &str,
        session: BindIdentity,
    ) -> Vec<FramedRecord> {
        let mut records = vec![FramedRecord {
            seq: 1,
            fence: 1,
            record: WalRecord::RunStarted {
                written_at_ms: None,
                tags: None,
                session_class: None,
                retention: None,
                hooks: None,
                title: None,
                on_restart: None,
                keep_warm: None,
                marker_order_version: None,
                manifest: None,
                scope: None,
                send_id: None,
                request_identity_digest: None,
                project_id: None,
                principal: None,
                run_id: RunId(run_id.into()),
                session,
                ts_ms: Some(100),
                config: Box::new(config(usage_policy)),
                input: vec![Message::new(
                    Role::User,
                    vec![ContentBlock::Text { text: "hi".into() }],
                )]
                .into(),
                origin: RunOrigin::Fresh,
                submission_id: None,
            },
        }];
        for ordinal in 0..segments {
            if ordinal > 0 {
                records.push(FramedRecord {
                    seq: records.len() as u64 + 1,
                    fence: 1,
                    record: WalRecord::RunConfigRefrozen {
                        renderer_version: ordinal as u32 + 1,
                        ts_ms: Some(100 + ordinal as u64),
                        frozen_render_config: config(usage_policy).frozen_render_config,
                    },
                });
            }
            records.push(FramedRecord {
                seq: records.len() as u64 + 1,
                fence: 1,
                record: WalRecord::ModelStepFinished {
                    fallback: Default::default(),
                    written_at_ms: None,
                    tags: None,
                    hooks: None,
                    step_id: StepId(ordinal as u64),
                    assistant_message: (broca_protocol::content::AssistantMessage {
                        message_id: broca_protocol::MessageId(format!("m{ordinal}")),
                        content: vec![ContentBlock::Text { text: "ok".into() }],
                    })
                    .into(),
                    usage: Usage {
                        input_tokens: Some(10 + ordinal as u64),
                        output_tokens: Some(2),
                        ..Usage::default()
                    },
                    finish_reason: Some(broca_protocol::FinishReason::Stop),
                    retries_used: ordinal as u32,
                    service_tier_used: None,
                    speed_used: None,
                },
            });
        }
        records.push(FramedRecord {
            seq: records.len() as u64 + 1,
            fence: 1,
            record: WalRecord::RunFinished {
                interrupt_reason: None,
                reason: RunFinishReason::Completed,
                ts_ms: Some(200),
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
        });
        records
    }

    fn resolver(provider: &str, method: &str) -> (ChargeBasis, u32) {
        if provider == "xai" && method == "apikey" {
            (ChargeBasis::MeteredApi, 7)
        } else {
            (ChargeBasis::Unknown, 7)
        }
    }

    fn projection_bytes(store: &SqliteStore) -> Vec<Vec<String>> {
        projection_bytes_for(store, None)
    }

    fn projection_bytes_for(
        store: &SqliteStore,
        session: Option<&BindIdentity>,
    ) -> Vec<Vec<String>> {
        let conn = store.conn.lock().unwrap();
        let key = session.map(SqliteStore::key);
        ["message", "meta"]
            .iter()
            .flat_map(|table| {
                let mut statement = conn
                    .prepare(&format!(
                        "SELECT * FROM {table} WHERE (?1 IS NULL OR session=?1) ORDER BY session"
                    ))
                    .unwrap();
                let columns = statement.column_count();
                statement
                    .query_map([key.as_deref()], |row| {
                        (0..columns)
                            .map(|i| row.get_ref(i).map(|value| format!("{value:?}")))
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap()
            })
            .collect()
    }

    #[test]
    fn drop_projection_preserves_facts_floors_and_rebuild_bytes() {
        use broca_core::ExportIndex;
        let store = SqliteStore::open_in_memory().unwrap();
        let session = BindIdentity::new("/tmp/export", "runner", "metered");
        let frames = records("local", 1);
        store.project(&session, &frames).unwrap();
        store
            .emit_export_run(&frames, &RunId("local".into()), &resolver)
            .unwrap();
        store
            .record_imported_runs(&session, "cut", &[RunId("foreign".into())])
            .unwrap();
        let projection = projection_bytes(&store);
        assert!(!projection.is_empty());
        let facts = export_fact_rows(&store.conn.lock().unwrap());
        assert!(!facts.is_empty());
        store.drop_session_projection(&session).unwrap();
        assert!(projection_bytes(&store).is_empty());
        assert_eq!(export_fact_rows(&store.conn.lock().unwrap()), facts);
        store
            .emit_export_run(&records("foreign", 1), &RunId("foreign".into()), &resolver)
            .unwrap();
        assert_eq!(
            export_fact_rows(&store.conn.lock().unwrap()),
            facts,
            "imported floor survives projection deletion"
        );
        store.project(&session, &frames).unwrap();
        assert_eq!(projection_bytes(&store), projection);
    }

    #[test]
    fn drop_projection_failure_rolls_back_messages_and_meta() {
        let store = SqliteStore::open_in_memory().unwrap();
        let session = BindIdentity::new("/tmp/export", "runner", "metered");
        store.project(&session, &records("local", 1)).unwrap();
        let before = projection_bytes(&store);
        store.conn.lock().unwrap().execute_batch("CREATE TEMP TRIGGER fail_meta_drop BEFORE DELETE ON meta BEGIN SELECT RAISE(ABORT, 'injected metadata delete failure'); END;").unwrap();
        assert!(store
            .drop_session_projection(&session)
            .unwrap_err()
            .contains("injected metadata delete failure"));
        assert_eq!(projection_bytes(&store), before);
    }

    #[test]
    fn drop_projection_changes_only_the_named_session() {
        let store = SqliteStore::open_in_memory().unwrap();
        let target = BindIdentity::new("/tmp/export", "runner", "metered");
        let other = BindIdentity::new("/other", "runner", "unrelated");
        store.project(&target, &records("local", 1)).unwrap();
        store
            .project(
                &other,
                &records_with_usage_policy(
                    "other",
                    1,
                    "open_ai_usage_reasoning_separate",
                    other.clone(),
                ),
            )
            .unwrap();
        let before = projection_bytes_for(&store, Some(&other));
        assert!(!before.is_empty());
        store.drop_session_projection(&target).unwrap();
        assert!(projection_bytes_for(&store, Some(&target)).is_empty());
        assert_eq!(projection_bytes_for(&store, Some(&other)), before);
    }

    #[test]
    fn export_persists_distinct_routing_key_and_vault_witness() {
        let root = broca_test_support::scratch_root().join(format!(
            "broca-export-witness-{}-{}",
            std::process::id(),
            test_fixture_nonce()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let store = SqliteStore::open(root.join("store.db")).unwrap();
        let mut frames = records("witness", 1);
        for frame in &mut frames {
            if let WalRecord::RunStarted { config, .. } = &mut frame.record {
                let selection = config.auth_selection.as_mut().unwrap();
                selection.credential_id = "apikey:kimi-code-plan-global".into();
                selection.resolved_credential_id = Some("apikey:kimi-for-coding".into());
            }
        }
        store
            .emit_export_run(&frames, &RunId("witness".into()), &resolver)
            .unwrap();
        let rows = export_fact_rows(&store.conn.lock().unwrap());
        let stored: serde_json::Value = serde_json::from_slice(&rows[0].3).unwrap();
        assert_eq!(stored["auth_selection"], "apikey:kimi-code-plan-global");
        assert_eq!(stored["resolved_credential_id"], "apikey:kimi-for-coding");
        let page = store.read_export_page(None, 1).unwrap();
        assert_eq!(
            page.facts[0]
                .segment
                .as_session()
                .unwrap()
                .resolved_credential_id
                .as_deref(),
            Some("apikey:kimi-for-coding")
        );
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn export_persists_the_vault_request_the_run_froze() {
        let root = broca_test_support::scratch_root().join(format!(
            "broca-export-vault-request-{}-{}",
            std::process::id(),
            test_fixture_nonce()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let store = SqliteStore::open(root.join("store.db")).unwrap();
        let request = broca_core::VaultRequest {
            min_ttl_ms: Some(600_000),
            force_refresh: Some(false),
            op: Some(broca_core::VaultReadOp::Get),
        };
        let mut frames = records("asked", 1);
        for frame in &mut frames {
            if let WalRecord::RunStarted { config, .. } = &mut frame.record {
                config.auth_selection.as_mut().unwrap().vault_request = Some(request.clone());
            }
        }
        store
            .emit_export_run(&frames, &RunId("asked".into()), &resolver)
            .unwrap();
        let rows = export_fact_rows(&store.conn.lock().unwrap());
        let stored: serde_json::Value = serde_json::from_slice(&rows[0].3).unwrap();
        // The operation and both parameters are on the shipped fact; a false force_refresh is
        // spelled out.
        assert_eq!(
            stored["vault_request"],
            serde_json::json!({"min_ttl_ms": 600_000, "force_refresh": false, "op": "get"})
        );
        let page = store.read_export_page(None, 1).unwrap();
        assert_eq!(
            page.facts[0].segment.as_session().unwrap().vault_request,
            Some(request)
        );
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    fn export_fact_index_names(conn: &Connection) -> Vec<String> {
        let mut statement = conn
            .prepare("PRAGMA index_list('export_facts')")
            .expect("list export fact indexes");
        statement
            .query_map([], |row| row.get::<_, String>(1))
            .expect("query export fact indexes")
            .collect::<Result<Vec<_>, _>>()
            .expect("decode export fact index names")
    }

    fn export_fact_column_names(conn: &Connection) -> Vec<String> {
        let mut statement = conn
            .prepare("PRAGMA table_info(export_facts)")
            .expect("list export fact columns");
        statement
            .query_map([], |row| row.get::<_, String>(1))
            .expect("query export fact columns")
            .collect::<Result<Vec<_>, _>>()
            .expect("decode export fact column names")
    }

    fn export_fact_rows(conn: &Connection) -> Vec<(i64, String, String, Vec<u8>)> {
        let mut statement = conn
            .prepare(
                "SELECT export_seq, fact_id, run_id, CAST(segment_json AS BLOB)
                   FROM export_facts ORDER BY export_seq",
            )
            .expect("prepare export fact snapshot");
        statement
            .query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .expect("query export fact snapshot")
            .collect::<Result<Vec<_>, _>>()
            .expect("decode export fact snapshot")
    }

    #[test]
    fn session_column_migration_is_idempotent_and_preserves_other_data() {
        let conn = Connection::open_in_memory().expect("open legacy export database");
        conn.execute_batch(
            "CREATE TABLE export_facts (
                 export_seq   INTEGER PRIMARY KEY AUTOINCREMENT,
                 fact_id      TEXT NOT NULL UNIQUE,
                 session      TEXT NOT NULL,
                 run_id       TEXT NOT NULL,
                 segment_json TEXT NOT NULL
             );
             CREATE INDEX export_facts_run_id ON export_facts(run_id);
             INSERT INTO export_facts
                 (export_seq, fact_id, session, run_id, segment_json)
             VALUES
                 (4, 'billing-fact-4', '{\"session\":\"four\"}', 'run-4',
                  '{\"run_id\":\"run-4\",\"session\":{\"session\":\"four\"}}'),
                 (9, 'billing-fact-9', '{\"session\":\"nine\"}', 'run-9',
                  '{\"run_id\":\"run-9\",\"session\":{\"session\":\"nine\"}}');",
        )
        .expect("create populated legacy export schema");
        let before = export_fact_rows(&conn);

        init_schema(&conn).expect("migrate legacy export schema");
        assert_eq!(
            export_fact_column_names(&conn),
            ["export_seq", "fact_id", "run_id", "segment_json"]
        );
        assert_eq!(export_fact_rows(&conn), before);

        init_schema(&conn).expect("rerun export schema migration");
        assert_eq!(
            export_fact_column_names(&conn),
            ["export_seq", "fact_id", "run_id", "segment_json"]
        );
        assert_eq!(export_fact_rows(&conn), before);
    }

    /// Opening a live index removes populated obsolete step rows without changing facts or metadata.
    #[test]
    fn open_drops_populated_export_steps_and_reopen_is_idempotent() {
        let root = broca_test_support::scratch_root().join(format!(
            "broca-export-step-drop-{}-{}",
            std::process::id(),
            test_fixture_nonce()
        ));
        std::fs::create_dir_all(&root).expect("create fixture directory");
        let db = root.join("run-index.db");
        let conn = Connection::open(&db).expect("open legacy export database");
        init_schema(&conn).expect("initialize export schema");
        conn.execute_batch(
            "INSERT INTO export_facts (fact_id, run_id, segment_json)
             VALUES ('fact-1', 'run-1', '{\"usage_by_service_tier\":{\"priority\":{\"input_tokens\":4}}}');
             UPDATE export_meta SET source_epoch=3, high_water=1;
             CREATE TABLE export_steps (
                 run_id TEXT NOT NULL, step_id INTEGER NOT NULL, step_json TEXT NOT NULL,
                 PRIMARY KEY (run_id, step_id)
             );
             INSERT INTO export_steps VALUES ('run-1', 0, '{\"usage\":{\"input_tokens\":4}}');",
        )
        .expect("populate legacy steps alongside tiered fact");
        let before = export_fact_rows(&conn);
        let steps_table = |conn: &Connection| {
            conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='export_steps'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("look up obsolete table")
        };
        assert_eq!(steps_table(&conn), 1);
        drop(conn);
        for _ in 0..2 {
            let store = crate::SqliteStore::open(&db).expect("open export store");
            assert_eq!(store.source_epoch(), 3);
            assert_eq!(store.export_high_water(), 1);
            let conn = store.conn.lock().expect("lock export store");
            assert_eq!(steps_table(&conn), 0);
            assert_eq!(export_fact_rows(&conn), before);
        }
        std::fs::remove_dir_all(root).expect("remove fixture directory");
    }

    #[test]
    fn emitted_segment_json_matches_pre_column_removal_fixture_byte_for_byte() {
        // The one deliberate difference from the pre-column-removal bytes is
        // `charge_basis_overrides`, which every newly derived fact carries ("none" here: the
        // test resolver has no override rows). Facts already stored without it keep their
        // bytes; broca-core's `a_stored_fact_reserializes_to_its_stored_bytes` pins that.
        const PRE_COLUMN_REMOVAL_SEGMENT_JSON: &str = concat!(
            r#"{"run_id":"run-export-fixture","session":{"project_root":"/tmp/export","harness":"runner","session":"metered"},"segment_ordinal":0,"#,
            r#""fact_id":"f88b51a02c4a07dc832eaef5c1cf738610301c4935be970a290bdf515d289400","provider":"xai","model":"grok-4","#,
            r#""auth_selection":"apikey:xai:work","charge_basis":"metered_api","charge_basis_mapping_version":7,"charge_basis_overrides":"none","usage_policy_version":2,"#,
            r#""usage":{"input_tokens":10,"cached_input_tokens":0,"cache_write_tokens":0,"output_tokens":2,"reasoning_tokens":0},"retries_used":0,"#,
            r#""pricing_at_ms":100,"pricing_at_precision":"exact","occurred_at_ms":200,"occurred_at_precision":"terminal","terminal_reason":"completed"}"#,
        );

        let root = broca_test_support::scratch_root().join(format!(
            "broca-export-segment-json-fixture-{}-{}",
            std::process::id(),
            test_fixture_nonce()
        ));
        std::fs::create_dir_all(&root).expect("create segment fixture directory");
        let db = root.join("store.db");
        {
            let conn = Connection::open(&db).expect("open segment fixture database");
            conn.execute_batch(
                "CREATE TABLE export_meta (
                     id                  INTEGER PRIMARY KEY CHECK (id = 1),
                     source_epoch        INTEGER NOT NULL DEFAULT 1,
                     source_instance_id  TEXT NOT NULL,
                     high_water          INTEGER NOT NULL DEFAULT 0
                 );
                 INSERT INTO export_meta
                     (id, source_epoch, source_instance_id, high_water)
                 VALUES (1, 1, 'pre-column-removal-fixture-source', 0);
                 CREATE TABLE export_facts (
                     export_seq   INTEGER PRIMARY KEY AUTOINCREMENT,
                     fact_id      TEXT NOT NULL UNIQUE,
                     session      TEXT NOT NULL,
                     run_id       TEXT NOT NULL,
                     segment_json TEXT NOT NULL
                 );",
            )
            .expect("create pre-column-removal export schema");
        }

        let store = SqliteStore::open(&db).expect("migrate and open segment fixture database");
        let run_id = RunId("run-export-fixture".into());
        let mut historical_records = records(&run_id.0, 1);
        // Old usage writers recorded every zero explicitly. Keep those measurements
        // in this historical-byte fixture; new unreported usage omits the keys.
        for frame in &mut historical_records {
            if let WalRecord::ModelStepFinished { usage, .. } = &mut frame.record {
                usage.cached_input_tokens = Some(0);
                usage.cache_write_tokens = Some(0);
                usage.reasoning_tokens = Some(0);
            }
        }
        store
            .emit_export_run(&historical_records, &run_id, &resolver)
            .expect("emit fixture export fact");
        let stored_bytes = store
            .conn
            .lock()
            .expect("lock segment fixture database")
            .query_row(
                "SELECT CAST(segment_json AS BLOB) FROM export_facts WHERE export_seq=1",
                [],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .expect("read raw segment_json bytes");
        assert_eq!(stored_bytes, PRE_COLUMN_REMOVAL_SEGMENT_JSON.as_bytes());

        let page = store
            .read_export_page(None, 1)
            .expect("round-trip fixture export fact");
        let round_tripped = serde_json::to_string(&page.facts[0].segment)
            .expect("serialize round-tripped fixture export fact");
        assert_eq!(
            round_tripped.as_bytes(),
            PRE_COLUMN_REMOVAL_SEGMENT_JSON.as_bytes()
        );

        drop(store);
        std::fs::remove_dir_all(root).expect("remove segment fixture directory");
    }

    #[test]
    fn fresh_schema_keeps_unique_fact_id_index_without_redundant_named_index() {
        let store = SqliteStore::open_in_memory().expect("open fresh export store");
        let conn = store.conn.lock().expect("lock export store");
        let indexes = export_fact_index_names(&conn);

        assert!(!indexes.iter().any(|name| name == "export_facts_fact_id"));
        assert!(
            indexes
                .iter()
                .any(|name| name.starts_with("sqlite_autoindex_export_facts_")),
            "the fact_id UNIQUE constraint must retain its SQLite auto-index"
        );
    }

    #[test]
    fn open_drops_redundant_fact_id_index_from_legacy_schema() {
        let root = broca_test_support::scratch_root().join(format!(
            "broca-export-index-migration-{}-{}",
            std::process::id(),
            test_fixture_nonce()
        ));
        std::fs::create_dir_all(&root).expect("create migration test directory");
        let db = root.join("store.db");
        {
            let conn = Connection::open(&db).expect("open legacy export database");
            conn.execute_batch(
                "CREATE TABLE export_facts (
                     export_seq   INTEGER PRIMARY KEY AUTOINCREMENT,
                     fact_id      TEXT NOT NULL UNIQUE,
                     session      TEXT NOT NULL,
                     run_id       TEXT NOT NULL,
                     segment_json TEXT NOT NULL
                 );
                 CREATE INDEX export_facts_fact_id ON export_facts(fact_id);
                 CREATE INDEX export_facts_run_id ON export_facts(run_id);",
            )
            .expect("create legacy export schema");
            assert!(
                export_fact_index_names(&conn)
                    .iter()
                    .any(|name| name == "export_facts_fact_id"),
                "the fixture must contain the legacy duplicate index"
            );
        }

        let store = SqliteStore::open(&db).expect("open and migrate legacy export database");
        {
            let conn = store.conn.lock().expect("lock migrated export store");
            let indexes = export_fact_index_names(&conn);
            assert!(!indexes.iter().any(|name| name == "export_facts_fact_id"));
            assert!(indexes.iter().any(|name| name == "export_facts_run_id"));
            assert!(
                indexes
                    .iter()
                    .any(|name| name.starts_with("sqlite_autoindex_export_facts_")),
                "migration must preserve fact_id uniqueness"
            );
        }
        drop(store);
        std::fs::remove_dir_all(root).expect("remove migration test directory");
    }

    #[test]
    fn emits_idempotent_multi_segment_page_and_persists_high_water() {
        let store = SqliteStore::open_in_memory().unwrap();
        let run_id = RunId("run-export".into());
        let records = records(&run_id.0, 2);
        store.emit_export_run(&records, &run_id, &resolver).unwrap();
        assert_eq!(store.high_water(), 2);
        store.emit_export_run(&records, &run_id, &resolver).unwrap();
        assert_eq!(store.high_water(), 2);

        let page = store.read_export_page(None, 1).unwrap();
        assert_eq!(page.facts.len(), 1);
        assert_eq!(page.facts[0].export_seq, 1);
        assert_eq!(
            page.facts[0].segment.as_session().unwrap().segment_ordinal,
            0
        );
        assert_eq!(
            page.facts[0].segment.as_session().unwrap().charge_basis,
            ChargeBasis::MeteredApi
        );
        assert_eq!(
            page.facts[0]
                .segment
                .as_session()
                .unwrap()
                .usage_policy_version,
            Some(2)
        );
        let segment_json = serde_json::to_value(&page.facts[0].segment).unwrap();
        assert_eq!(
            segment_json["usage_policy_version"], 2,
            "the persisted export fact must expose a queryable numeric policy version"
        );

        let mut legacy_json = segment_json;
        legacy_json
            .as_object_mut()
            .unwrap()
            .remove("usage_policy_version");
        let legacy: broca_core::SpendSegment = serde_json::from_value(legacy_json).unwrap();
        assert_eq!(legacy.usage_policy_version, None);
        assert!(
            serde_json::to_value(&legacy)
                .unwrap()
                .get("usage_policy_version")
                .is_none(),
            "reading an old fact must not back-stamp the field on re-export"
        );
        assert_eq!(
            page.facts[0]
                .segment
                .as_session()
                .unwrap()
                .usage
                .input_tokens,
            Some(10)
        );
        assert_eq!(
            page.next,
            Some(ExportCursor {
                source_instance_id: page.source_instance_id.clone(),
                source_epoch: 1,
                export_seq: 1
            })
        );
        assert_eq!(page.high_water, 2);

        let second = store.read_export_page(page.next, 1).unwrap();
        assert_eq!(second.facts.len(), 1);
        assert_eq!(second.facts[0].export_seq, 2);
        assert_eq!(
            second.facts[0]
                .segment
                .as_session()
                .unwrap()
                .segment_ordinal,
            1
        );
        assert_eq!(
            second.facts[0]
                .segment
                .as_session()
                .unwrap()
                .usage_policy_version,
            Some(2)
        );
        assert_eq!(
            second.next,
            Some(ExportCursor {
                source_instance_id: second.source_instance_id.clone(),
                source_epoch: 1,
                export_seq: 2
            })
        );

        let legacy_segment_json = serde_json::to_string(&legacy).unwrap();
        {
            let conn = store.conn.lock().unwrap();
            conn.execute(
                "UPDATE export_facts SET segment_json=?1 WHERE export_seq=1",
                [&legacy_segment_json],
            )
            .unwrap();
        }
        store.emit_export_run(&records, &run_id, &resolver).unwrap();
        let stored_segment_json = store
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT segment_json FROM export_facts WHERE export_seq=1",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(
            stored_segment_json, legacy_segment_json,
            "re-emitting a run must not back-stamp or rewrite an immutable existing fact"
        );
        assert_eq!(store.high_water(), 2);
    }

    #[test]
    fn startup_catch_up_recovers_terminal_index_gap_once_with_wal_time() {
        let nonce = test_fixture_nonce();
        let root = broca_test_support::scratch_root().join(format!(
            "broca-export-catch-up-{}-{nonce}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let db = root.join("store.db");
        let wal_store = FileWalStore::new(root.join("wal"));
        let session = BindIdentity::new("/tmp/export", "runner", "metered");

        let mut live_records = records("run-live", 1);
        if let Some(FramedRecord {
            record: WalRecord::RunFinished { ts_ms, .. },
            ..
        }) = live_records.last_mut()
        {
            *ts_ms = Some(400);
        }
        let missed_run = RunId("run-missed".into());
        let mut missed_records =
            records_with_usage_policy(&missed_run.0, 1, "open_ai_usage", session.clone());
        if let Some(FramedRecord {
            record: WalRecord::RunFinished { ts_ms, .. },
            ..
        }) = missed_records.last_mut()
        {
            *ts_ms = Some(200);
        }
        let mut wal = wal_store.open(&session).expect("open missed-run WAL");
        for (index, framed) in missed_records.iter().enumerate() {
            wal.append(1, &framed.record, index + 1 == missed_records.len())
                .expect("persist missed-run WAL record");
        }
        drop(wal);

        {
            let store = SqliteStore::open(&db).unwrap();
            store
                .emit_export_run(&live_records, &RunId("run-live".into()), &resolver)
                .unwrap();
            let row = RunIndexRow {
                withdrawals_owed: false,
                title: None,
                run_id: missed_run.0.clone(),
                session: session.clone(),
                submission_id: None,
                request_identity_digest: None,
                state: RunIndexState::Active,
                usage: None,
                final_step_finish_reason: None,
                retries_used: 0,
            };
            store.index_run_active(&row).unwrap();
            store
                .index_run_terminal(
                    &row.run_id,
                    RunIndexState::Completed,
                    Some(Usage::default()),
                    None,
                    0,
                )
                .unwrap();
            let before_catch_up = store.read_export_page(None, 10).unwrap();
            assert_eq!(
                before_catch_up.facts.len(),
                1,
                "the simulated crash leaves the terminal run absent before restart catch-up"
            );
            assert_eq!(
                before_catch_up.facts[0]
                    .segment
                    .as_session()
                    .unwrap()
                    .usage_policy_version,
                Some(2),
                "the v2-era fact written before the crash stays versioned"
            );
        }

        let reopened = SqliteStore::open(&db).unwrap();
        assert_eq!(
            catch_up_usage_exports(&reopened, &wal_store, &resolver).unwrap(),
            1
        );
        assert_eq!(
            catch_up_usage_exports(&reopened, &wal_store, &resolver).unwrap(),
            0,
            "the terminal-index diff is empty after the idempotent repair"
        );
        let page = reopened.read_export_page(None, 10).unwrap();
        assert_eq!(page.facts.len(), 2, "catch-up emits exactly one new fact");
        assert_eq!(
            page.facts[0].segment.as_session().unwrap().run_id.0,
            "run-live"
        );
        assert_eq!(
            page.facts[0].segment.as_session().unwrap().occurred_at_ms,
            Some(400)
        );
        assert_eq!(
            page.facts[0]
                .segment
                .as_session()
                .unwrap()
                .usage_policy_version,
            Some(2)
        );
        assert_eq!(page.facts[1].export_seq, 2);
        assert_eq!(
            page.facts[1].segment.as_session().unwrap().run_id,
            missed_run
        );
        assert_eq!(
            page.facts[1]
                .segment
                .as_session()
                .unwrap()
                .usage_policy_version,
            None,
            "catch-up must preserve the pre-v2 WAL's unversioned usage semantics"
        );
        assert!(
            serde_json::to_value(&page.facts[1].segment)
                .unwrap()
                .get("usage_policy_version")
                .is_none(),
            "an unversioned recovered fact must omit the field rather than stamp 0 or 1"
        );
        assert_eq!(
            page.facts[1].segment.as_session().unwrap().occurred_at_ms,
            Some(200),
            "catch-up preserves occurrence time from the WAL rather than allocation order"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn high_water_and_source_identity_survive_reopen() {
        let path = broca_test_support::scratch_root().join(format!(
            "broca-export-reopen-{}-{}.db",
            std::process::id(),
            1
        ));
        let _ = std::fs::remove_file(&path);
        let run_id = RunId("run-reopen".into());
        let source_instance_id;
        {
            let store = SqliteStore::open(&path).unwrap();
            source_instance_id = store.source_instance_id().to_string();
            store
                .emit_export_run(&records(&run_id.0, 1), &run_id, &resolver)
                .unwrap();
            assert_eq!(store.high_water(), 1);
        }
        let reopened = SqliteStore::open(&path).unwrap();
        assert_eq!(reopened.source_instance_id(), source_instance_id);
        assert_eq!(reopened.high_water(), 1);
        assert_eq!(reopened.read_export_page(None, 10).unwrap().facts.len(), 1);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn source_identity_getters_use_the_open_time_cache() {
        let store = SqliteStore::open_in_memory().unwrap();
        let source_instance_id = store.source_instance_id().to_string();
        let source_epoch = store.source_epoch();
        {
            let conn = store.conn.lock().unwrap();
            conn.execute(
                "UPDATE export_meta SET source_instance_id='changed', source_epoch=42 WHERE id=1",
                [],
            )
            .unwrap();
        }

        assert_eq!(store.source_instance_id(), source_instance_id);
        assert_eq!(store.source_epoch(), source_epoch);
    }

    #[test]
    fn stale_epoch_is_typed_and_collision_keeps_original() {
        let store = SqliteStore::open_in_memory().unwrap();
        let run_id = RunId("run-collision".into());
        let records = records(&run_id.0, 1);
        store.emit_export_run(&records, &run_id, &resolver).unwrap();
        let original = store.read_export_page(None, 10).unwrap().facts[0]
            .segment
            .clone();
        {
            let conn = store.conn.lock().unwrap();
            conn.execute("UPDATE export_meta SET source_epoch=2 WHERE id=1", [])
                .unwrap();
        }
        let error = store
            .read_export_page(
                Some(ExportCursor {
                    source_instance_id: store.source_instance_id().to_string(),
                    source_epoch: 1,
                    export_seq: 0,
                }),
                10,
            )
            .unwrap_err();
        assert_eq!(
            error,
            ExportError::EpochChanged {
                source_instance_id: store.source_instance_id().to_string(),
                source_epoch: 2,
                high_water: 1
            }
        );

        // Changing the resolver changes the serialized payload but not the fact identity.
        let changed = |_: &str, _: &str| (ChargeBasis::SubscriptionIncluded, 8);
        store.emit_export_run(&records, &run_id, &changed).unwrap();
        let kept = store.read_export_page(None, 10).unwrap().facts[0]
            .segment
            .clone();
        assert_eq!(kept, original);
    }
}

#[cfg(test)]
mod collision_tests {
    use super::{classify_collision, CollisionKind};

    /// The measured production case for the BENIGN arm: a fact predating an added field, where
    /// the stored bytes are a strict subset of today's.
    #[test]
    fn a_fact_predating_an_added_field_is_evolution_not_a_disagreement() {
        let stored = r#"{"fact_id":"a","input_tokens":13,"account_ref":"d4bc9dbc"}"#;
        let current = r#"{"fact_id":"a","input_tokens":13,"account_ref":"d4bc9dbc","resolved_credential_id":"apikey:openai"}"#;
        assert_eq!(
            classify_collision(stored, current),
            CollisionKind::SchemaEvolution
        );
    }

    /// The arm the warning exists for: same identity, same field, different answer.
    #[test]
    fn a_changed_value_names_its_field_and_stays_loud() {
        let stored = r#"{"fact_id":"a","input_tokens":13}"#;
        let current = r#"{"fact_id":"a","input_tokens":99}"#;
        assert_eq!(
            classify_collision(stored, current),
            CollisionKind::ValueDisagreement {
                fields: vec!["input_tokens".to_string()]
            }
        );
    }

    /// Key iteration is lexicographic, so a derivation stamp can sort before usage.
    /// Report every differing field: returning at the stamp would hide usage disagreements.
    #[test]
    fn a_real_disagreement_is_reported_even_when_a_stamp_sorts_ahead_of_it() {
        let stored = r#"{"charge_basis_mapping_version":3,"usage":{"input_tokens":13}}"#;
        let current = r#"{"charge_basis_mapping_version":4,"usage":{"input_tokens":99}}"#;
        let CollisionKind::ValueDisagreement { fields } = classify_collision(stored, current)
        else {
            panic!("a changed usage must be loud even beside a moved derivation stamp");
        };
        assert!(
            fields.iter().any(|f| f == "usage"),
            "the masked field must be named; got {fields:?}"
        );
        assert!(
            fields.iter().any(|f| f == "charge_basis_mapping_version"),
            "the stamp belongs beside it, so a reader can see it is incidental; got {fields:?}"
        );
    }

    /// A mapping-version change with identical usage is not a value disagreement.
    /// It takes a separate classification arm from a newly added field.
    #[test]
    fn a_moved_derivation_stamp_alone_is_expected_rather_than_a_disagreement() {
        let stored = r#"{"charge_basis_mapping_version":3,"usage":{"input_tokens":13}}"#;
        let current = r#"{"charge_basis_mapping_version":4,"usage":{"input_tokens":13}}"#;
        assert_eq!(
            classify_collision(stored, current),
            CollisionKind::DerivationVersionChange {
                stamps: vec!["charge_basis_mapping_version".to_string()],
                added: vec![],
            }
        );
    }

    /// A field the current segment NO LONGER emits is not the benign direction: today's
    /// derivation would ship less than what already shipped under this identity.
    #[test]
    fn a_dropped_field_is_loud_because_shipping_less_is_not_evolution() {
        let stored = r#"{"fact_id":"a","input_tokens":13,"account_ref":"d4bc9dbc"}"#;
        let current = r#"{"fact_id":"a","input_tokens":13}"#;
        assert_eq!(
            classify_collision(stored, current),
            CollisionKind::ValueDisagreement {
                fields: vec!["account_ref".to_string()]
            }
        );
    }

    /// Byte-identical is the overwhelming majority and must not reach any log arm.
    #[test]
    fn identical_segments_say_nothing() {
        let same = r#"{"fact_id":"a","input_tokens":13}"#;
        assert_eq!(classify_collision(same, same), CollisionKind::Identical);
    }

    /// Fail toward loud: an unparseable stored segment is not an added field, and treating it
    /// as evolution would silence a genuinely corrupt row.
    #[test]
    fn an_unparseable_segment_is_loud_rather_than_assumed_benign() {
        assert_eq!(
            classify_collision("not json", r#"{"fact_id":"a"}"#),
            CollisionKind::ValueDisagreement {
                fields: vec!["<unparseable segment>".to_string()]
            }
        );
    }

    /// Bytes differ, maps are EQUAL, nothing was added. Reporting this as the added-field case
    /// would name a schema change that did not happen.
    ///
    /// Unreachable while `serde_json::Map` is a `BTreeMap` -- which is a fact about another
    /// crate's feature resolution, not about this code, so it is asserted here rather than
    /// assumed. Enabling `preserve_order` anywhere in the tree makes it live immediately.
    #[test]
    fn equal_maps_in_different_bytes_are_not_the_added_field_case() {
        let stored = r#"{"a":1,"b":2}"#;
        let current = r#"{"b":2,"a":1}"#;
        assert_eq!(
            classify_collision(stored, current),
            CollisionKind::EquivalentBytes
        );
    }

    /// The control that keeps the arm above honest: a genuine addition must still be evolution,
    /// so the new branch cannot be satisfied by routing everything to EquivalentBytes.
    #[test]
    fn a_genuine_addition_is_still_evolution_not_equivalent_bytes() {
        let stored = r#"{"a":1}"#;
        let current = r#"{"a":1,"b":2}"#;
        assert_eq!(
            classify_collision(stored, current),
            CollisionKind::SchemaEvolution
        );
    }

    /// An override swap that leaves this fact's basis alone moves only the override digest.
    /// That is the same actionless event as a shipped-table bump, so it takes the same arm.
    #[test]
    fn a_moved_override_digest_alone_is_a_derivation_stamp_change() {
        let stored = r#"{"charge_basis":"metered_api","charge_basis_mapping_version":4,"charge_basis_overrides":"none","usage":{"input_tokens":13}}"#;
        let current = r#"{"charge_basis":"metered_api","charge_basis_mapping_version":4,"charge_basis_overrides":"2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881","usage":{"input_tokens":13}}"#;
        assert_eq!(
            classify_collision(stored, current),
            CollisionKind::DerivationVersionChange {
                stamps: vec!["charge_basis_overrides".to_string()],
                added: vec![],
            }
        );
    }

    /// An override that changed THIS fact's answer moves the digest and `charge_basis`
    /// together. The digest being a stamp must not quiet that: `charge_basis` is a value.
    #[test]
    fn a_moved_override_digest_beside_a_changed_charge_basis_stays_loud() {
        let stored = r#"{"charge_basis":"subscription_included","charge_basis_mapping_version":4,"charge_basis_overrides":"none","usage":{"input_tokens":13}}"#;
        let current = r#"{"charge_basis":"metered_api","charge_basis_mapping_version":4,"charge_basis_overrides":"2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881","usage":{"input_tokens":13}}"#;
        assert_eq!(
            classify_collision(stored, current),
            CollisionKind::ValueDisagreement {
                fields: vec![
                    "charge_basis".to_string(),
                    "charge_basis_overrides".to_string()
                ]
            }
        );
    }

    /// THE MEASURED PRODUCTION SHAPE: the stamp moved AND a field was added, together. Before
    /// this was carried the message said the fact differed ONLY on the stamp, which was the
    /// fourth instance in this one function of an arm asserting more than its condition
    /// established -- and unlike the other three, it was the case that actually occurs.
    #[test]
    fn a_stamp_and_an_addition_together_name_both_rather_than_only_the_stamp() {
        let stored = r#"{"charge_basis_mapping_version":3,"usage":{"input_tokens":13}}"#;
        let current = r#"{"charge_basis_mapping_version":4,"resolved_credential_id":"x","usage":{"input_tokens":13}}"#;
        assert_eq!(
            classify_collision(stored, current),
            CollisionKind::DerivationVersionChange {
                stamps: vec!["charge_basis_mapping_version".to_string()],
                added: vec!["resolved_credential_id".to_string()],
            }
        );
    }
}
