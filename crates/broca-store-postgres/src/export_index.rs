//! PostgreSQL usage export index.
//!
//! Terminal run-index rows without export facts identify work missed after a crash,
//! and `fact_id` uniqueness makes repeated export emission idempotent.

use std::sync::atomic::{AtomicU64, Ordering};

use broca_core::{
    compute_segments_for_run, ChargeBasisResolver, ExportCursor, ExportError, ExportFact,
    ExportIndex, ExportPage, MissingExportRun,
};
use broca_protocol::RunId;
use broca_wal::FramedRecord;
use postgres::{Client, IsolationLevel};

use super::PostgresStore;

pub(crate) fn init_schema(client: &mut Client) -> Result<(String, i64, i64), postgres::Error> {
    // segment_json stores the complete session, so DROP removes only the duplicate column;
    // IF EXISTS leaves an already-migrated table unchanged.
    client.batch_execute(
        "CREATE TABLE IF NOT EXISTS export_meta (
             id                  BIGINT PRIMARY KEY,
             source_epoch       BIGINT NOT NULL DEFAULT 1,
             source_instance_id TEXT NOT NULL,
             high_water         BIGINT NOT NULL DEFAULT 0
         );
         CREATE TABLE IF NOT EXISTS export_facts (
             export_seq   BIGSERIAL PRIMARY KEY,
             fact_id      TEXT NOT NULL UNIQUE,
             run_id       TEXT NOT NULL,
             segment_json TEXT NOT NULL
         );
         ALTER TABLE export_facts DROP COLUMN IF EXISTS session;
         DROP INDEX IF EXISTS export_facts_fact_id;
         CREATE INDEX IF NOT EXISTS export_facts_run_id ON export_facts(run_id);",
    )?;
    client.batch_execute(
        "CREATE TABLE IF NOT EXISTS export_imported_runs (
             session TEXT NOT NULL,
             run_id  TEXT NOT NULL,
             cut_id  TEXT NOT NULL,
             PRIMARY KEY (session, run_id, cut_id)
         );",
    )?;
    // Older deployments wrote per-step projections that consumers now read from WALs.
    // Remove this once no deployment could still carry the table. Rolling back to v0.3.125 or v0.3.126
    // recreates it empty on open, which is harmless.
    client.batch_execute("DROP TABLE IF EXISTS export_steps;")?;
    let source_instance_id = client
        .query_opt("SELECT source_instance_id FROM export_meta WHERE id=1", &[])?
        .map(|row| row.get::<_, String>(0));
    if source_instance_id.is_none() {
        client.execute(
            "INSERT INTO export_meta (id, source_epoch, source_instance_id, high_water)
             VALUES (1, 1, $1, 0)",
            &[&mint_source_instance_id()],
        )?;
    }
    let row = client.query_one(
        "SELECT source_instance_id, source_epoch, high_water FROM export_meta WHERE id=1",
        &[],
    )?;
    Ok((row.get(0), row.get(1), row.get(2)))
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
    i64::try_from(value).map_err(|_| storage(format!("{field} exceeds BIGINT")))
}

impl PostgresStore {
    pub fn high_water(&self) -> u64 {
        self.export_high_water.load(Ordering::Acquire)
    }

    pub fn export_high_water(&self) -> u64 {
        self.high_water()
    }

    /// Open-time source identity cache. Admission reads these getters without acquiring the
    /// Postgres client lock; a future rebuild must publish a replacement store instance.
    pub fn source_epoch(&self) -> u64 {
        self.export_source_epoch
    }

    pub fn source_instance_id(&self) -> &str {
        &self.export_source_instance_id
    }

    pub fn read_export_page(
        &self,
        since: Option<ExportCursor>,
        limit: u64,
    ) -> Result<ExportPage, ExportError> {
        let mut client = self
            .client
            .lock()
            .map_err(|_| storage("store mutex poisoned"))?;
        let mut tx = client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .map_err(storage)?;
        let row = tx
            .query_one(
                "SELECT source_epoch, source_instance_id, high_water
                   FROM export_meta WHERE id=1",
                &[],
            )
            .map_err(storage)?;
        let source_epoch: i64 = row.get(0);
        let source_instance_id: String = row.get(1);
        let high_water: i64 = row.get(2);
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

        let after = u64_to_i64(
            since.as_ref().map_or(0, |cursor| cursor.export_seq),
            "since.export_seq",
        )?;
        let limit = u64_to_i64(limit.min(1_000), "limit")?;
        let rows = tx
            .query(
                "SELECT export_seq, segment_json FROM export_facts
                   WHERE export_seq > $1 ORDER BY export_seq ASC LIMIT $2",
                &[&after, &limit],
            )
            .map_err(storage)?;
        let mut facts = Vec::with_capacity(rows.len());
        for row in rows {
            let export_seq: i64 = row.get(0);
            let segment_json: String = row.get(1);
            facts.push(ExportFact {
                export_seq: i64_to_u64(export_seq, "export_seq")?,
                segment: serde_json::from_str(&segment_json).map_err(storage)?,
            });
        }
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

        let mut client = self
            .client
            .lock()
            .map_err(|_| storage("store mutex poisoned"))?;
        let mut tx = client.transaction().map_err(storage)?;
        // Serialize writers across connections before allocating any sequence.
        // Otherwise a later sequence can commit first and strand a fact behind
        // an export reader's cursor. The lock is held through high-water commit.
        tx.query_one("SELECT id FROM export_meta WHERE id=1 FOR UPDATE", &[])
            .map_err(storage)?;
        for segment in segments {
            let session_json = serde_json::to_string(&segment.session).map_err(storage)?;
            let imported: bool = tx
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM export_imported_runs
                                    WHERE session=$1 AND run_id=$2)",
                    &[&session_json, &segment.run_id.0],
                )
                .map_err(storage)?
                .get(0);
            if imported {
                continue;
            }
            let segment_json = serde_json::to_string(&segment).map_err(storage)?;
            tx.execute(
                "INSERT INTO export_facts (fact_id, run_id, segment_json)
                 VALUES ($1, $2, $3) ON CONFLICT (fact_id) DO NOTHING",
                &[&segment.fact_id, &segment.run_id.0, &segment_json],
            )
            .map_err(storage)?;
            let stored: String = tx
                .query_one(
                    "SELECT segment_json FROM export_facts WHERE fact_id=$1",
                    &[&segment.fact_id],
                )
                .map_err(storage)?
                .get(0);
            // THE SQLITE TWIN CLASSIFIES THIS AND THIS ONE DOES NOT, deliberately for now.
            //
            // `broca-store-postgres` is not linked by the daemon (`cargo tree -p
            // broca-module-serve -e normal` shows zero edges), so this line cannot reach the
            // fleet log and the noise argument that drove the sqlite change does not apply
            // here. What DOES apply is the classification: a shipped fact is immutable, so a
            // re-derivation differs whenever an additive segment field has landed since, which
            // needs no action and is indistinguishable here from a value that changed under an
            // identity that cannot change.
            //
            // Port `classify_collision` when this backend gets a supervised consumer, or sooner
            // if a postgres deployment starts reading these. Leaving it unported is a judgement
            // about REACH, not a claim that the distinction does not matter.
            if stored != segment_json {
                eprintln!(
                    "broca: HARD WARNING: immutable usage fact_id collision for {}; keeping shipped fact",
                    segment.fact_id
                );
            }
        }
        let high_water_candidate: i64 = tx
            .query_one("SELECT COALESCE(MAX(export_seq), 0) FROM export_facts", &[])
            .map_err(storage)?
            .get(0);
        let high_water = advance_high_water(&mut tx, high_water_candidate)?;
        tx.commit().map_err(storage)?;
        self.export_high_water
            .fetch_max(i64_to_u64(high_water, "high_water")?, Ordering::Release);
        Ok(())
    }
}

/// Publish a candidate without allowing another connection's newer committed head to regress.
fn advance_high_water(
    tx: &mut postgres::Transaction<'_>,
    candidate: i64,
) -> Result<i64, ExportError> {
    tx.query_one(
        "UPDATE export_meta
            SET high_water = GREATEST(high_water, $1)
          WHERE id=1
          RETURNING high_water",
        &[&candidate],
    )
    .map(|row| row.get(0))
    .map_err(storage)
}

impl ExportIndex for PostgresStore {
    fn record_imported_runs(
        &self,
        session: &broca_protocol::BindIdentity,
        cut_id: &str,
        run_ids: &[RunId],
    ) -> Result<(), ExportError> {
        let session_json = serde_json::to_string(session).map_err(storage)?;
        let mut client = self
            .client
            .lock()
            .map_err(|_| storage("store mutex poisoned"))?;
        let mut tx = client.transaction().map_err(storage)?;
        // Both fact insertion and floor writes lock export_meta's singleton row.
        // A fact writer cannot read the imported-run set while this commit is pending.
        tx.query_one("SELECT id FROM export_meta WHERE id=1 FOR UPDATE", &[])
            .map_err(storage)?;
        for run_id in run_ids {
            tx.execute(
                "INSERT INTO export_imported_runs (session, run_id, cut_id)
                 VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                &[&session_json, &run_id.0, &cut_id],
            )
            .map_err(storage)?;
        }
        tx.commit().map_err(storage)
    }

    fn missing_terminal_runs(&self) -> Result<Vec<MissingExportRun>, ExportError> {
        let mut client = self
            .client
            .lock()
            .map_err(|_| storage("store mutex poisoned"))?;
        let rows = client
            .query(
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
                &[],
            )
            .map_err(storage)?;
        let mut missing = Vec::with_capacity(rows.len());
        for row in rows {
            let run_id: String = row.get(0);
            let session_json: String = row.get(1);
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
pub(crate) fn assert_session_column_migration_is_idempotent(url: &str) {
    fn has_session_column(client: &mut Client) -> bool {
        client
            .query_opt(
                "SELECT 1
                   FROM information_schema.columns
                  WHERE table_schema = current_schema()
                    AND table_name = 'export_facts'
                    AND column_name = 'session'",
                &[],
            )
            .expect("query export fact columns")
            .is_some()
    }

    fn export_fact_rows(client: &mut Client) -> Vec<(i64, String, String, String)> {
        client
            .query(
                "SELECT export_seq, fact_id, run_id, segment_json
                   FROM export_facts ORDER BY export_seq",
                &[],
            )
            .expect("query export fact snapshot")
            .into_iter()
            .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
            .collect()
    }

    let initialized = PostgresStore::connect(url).expect("initialize export schema");
    drop(initialized);

    let mut client = Client::connect(url, postgres::NoTls).expect("connect legacy schema fixture");
    client
        .batch_execute(
            "ALTER TABLE export_facts ADD COLUMN IF NOT EXISTS session TEXT;
             UPDATE export_facts SET session = 'legacy-session-fixture' WHERE session IS NULL;
             ALTER TABLE export_facts ALTER COLUMN session SET NOT NULL;
             DELETE FROM export_facts WHERE fact_id = 'session-column-migration-fixture';
             INSERT INTO export_facts
                 (export_seq, fact_id, session, run_id, segment_json)
             VALUES
                 (0, 'session-column-migration-fixture', 'legacy-session-fixture',
                  'session-column-migration-run', '{\"billing\":\"preserved\"}');",
        )
        .expect("create populated legacy export schema");
    assert!(has_session_column(&mut client));
    let before = export_fact_rows(&mut client);
    drop(client);

    let first = PostgresStore::connect(url).expect("migrate legacy export schema");
    {
        let mut client = first.client.lock().expect("lock first migrated store");
        assert!(!has_session_column(&mut client));
        assert_eq!(export_fact_rows(&mut client), before);
    }
    drop(first);

    let second = PostgresStore::connect(url).expect("rerun export schema migration");
    {
        let mut client = second.client.lock().expect("lock second migrated store");
        assert!(!has_session_column(&mut client));
        assert_eq!(export_fact_rows(&mut client), before);
        client
            .execute(
                "DELETE FROM export_facts WHERE fact_id = 'session-column-migration-fixture'",
                &[],
            )
            .expect("remove migration fixture row");
    }
}

#[cfg(test)]
pub(crate) fn assert_redundant_fact_id_index_is_dropped(url: &str) {
    fn named_index(store: &PostgresStore) -> Option<String> {
        store
            .client
            .lock()
            .expect("lock Postgres export store")
            .query_opt(
                "SELECT indexname
                   FROM pg_indexes
                  WHERE schemaname = current_schema()
                    AND tablename = 'export_facts'
                    AND indexname = 'export_facts_fact_id'",
                &[],
            )
            .expect("query redundant export index")
            .map(|row| row.get(0))
    }

    let fresh_store = PostgresStore::connect(url).expect("open export schema without named index");
    assert_eq!(named_index(&fresh_store), None);
    drop(fresh_store);

    let absent_reopen = PostgresStore::connect(url).expect("reopen without named export index");
    assert_eq!(named_index(&absent_reopen), None);
    {
        let mut client = absent_reopen.client.lock().expect("lock Postgres client");
        client
            .batch_execute("CREATE INDEX export_facts_fact_id ON export_facts(fact_id)")
            .expect("create legacy redundant export index");
    }
    assert_eq!(
        named_index(&absent_reopen).as_deref(),
        Some("export_facts_fact_id")
    );
    drop(absent_reopen);

    let migrated_store = PostgresStore::connect(url).expect("drop legacy redundant export index");
    assert_eq!(named_index(&migrated_store), None);
}

#[cfg(test)]
pub(crate) fn assert_populated_export_steps_dropped_on_reopen(url: &str) {
    let store = PostgresStore::connect(url).expect("initialize export schema");
    let before = {
        let mut client = store.client.lock().expect("lock export store");
        client
            .batch_execute(
                r#"CREATE TABLE export_steps (
                     run_id TEXT NOT NULL, step_id BIGINT NOT NULL, step_json TEXT NOT NULL,
                     PRIMARY KEY (run_id, step_id)
                 );
                 INSERT INTO export_steps VALUES ('migration-fixture', 0, '{}');
                 INSERT INTO export_facts (fact_id, run_id, segment_json)
                 VALUES ('step-drop-tier-fixture', 'step-drop-run',
                         '{"usage_by_service_tier":{"priority":{"input_tokens":4}}}')
                 ON CONFLICT (fact_id) DO NOTHING;"#,
            )
            .expect("populate legacy table and tiered fact");
        client
            .query(
                "SELECT fact_id, segment_json FROM export_facts ORDER BY export_seq",
                &[],
            )
            .expect("read fact bytes before removal")
            .iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
            .collect::<Vec<_>>()
    };
    assert!(
        before
            .iter()
            .any(|(id, json)| id == "step-drop-tier-fixture"
                && json.contains("usage_by_service_tier")),
        "the preserved facts include tier usage"
    );
    drop(store);
    for _ in 0..2 {
        let store = PostgresStore::connect(url).expect("reopen export schema");
        let mut client = store.client.lock().expect("lock reopened store");
        let present: Option<String> = client
            .query_one("SELECT to_regclass('export_steps')::text", &[])
            .expect("check obsolete table")
            .get(0);
        assert_eq!(present, None);
        let after = client
            .query(
                "SELECT fact_id, segment_json FROM export_facts ORDER BY export_seq",
                &[],
            )
            .expect("read fact bytes after removal")
            .iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
            .collect::<Vec<_>>();
        assert_eq!(after, before, "migration preserves fact identity and bytes");
    }
    let mut client = Client::connect(url, postgres::NoTls).expect("connect to clean fixture");
    client
        .execute(
            "DELETE FROM export_facts WHERE fact_id='step-drop-tier-fixture'",
            &[],
        )
        .expect("remove migration fact fixture");
}

#[cfg(test)]
pub(crate) fn assert_stale_high_water_candidate_cannot_regress(url: &str) {
    let stale_store = PostgresStore::connect(url).expect("connect stale-candidate store");
    let fresh_store = PostgresStore::connect(url).expect("connect fresh-candidate store");

    let stale_candidate: i64 = stale_store
        .client
        .lock()
        .expect("lock stale-candidate client")
        .query_one("SELECT COALESCE(MAX(export_seq), 0) FROM export_facts", &[])
        .expect("read stale high-water candidate")
        .get(0);

    let fresh_seq = {
        let mut client = fresh_store.client.lock().expect("lock fresh client");
        let mut tx = client.transaction().expect("begin fresh transaction");
        let fact_id = format!("postgres-high-water-regression-{stale_candidate}");
        let run_id = format!("postgres-high-water-regression-run-{stale_candidate}");
        let fresh_seq: i64 = tx
            .query_one(
                "INSERT INTO export_facts (fact_id, run_id, segment_json)
                  SELECT $1, $2, segment_json
                    FROM export_facts
                   ORDER BY export_seq ASC
                   LIMIT 1
                  RETURNING export_seq",
                &[&fact_id, &run_id],
            )
            .expect("insert a later export fact")
            .get(0);
        let fresh_candidate: i64 = tx
            .query_one("SELECT COALESCE(MAX(export_seq), 0) FROM export_facts", &[])
            .expect("read fresh high-water candidate")
            .get(0);
        let persisted = advance_high_water(&mut tx, fresh_candidate)
            .expect("persist fresh high-water candidate");
        tx.commit().expect("commit fresh high water");
        fresh_store.export_high_water.fetch_max(
            i64_to_u64(persisted, "high_water").expect("fresh high water is nonnegative"),
            Ordering::Release,
        );
        assert_eq!(persisted, fresh_seq);
        fresh_seq
    };

    let stale_returned = {
        let mut client = stale_store.client.lock().expect("lock stale client");
        let mut tx = client.transaction().expect("begin stale transaction");
        let persisted =
            advance_high_water(&mut tx, stale_candidate).expect("apply stale high-water candidate");
        tx.commit().expect("commit stale high-water attempt");
        persisted
    };
    stale_store.export_high_water.fetch_max(
        i64_to_u64(stale_returned, "high_water").expect("returned high water is nonnegative"),
        Ordering::Release,
    );

    assert_eq!(stale_returned, fresh_seq);
    let stale_candidate =
        i64_to_u64(stale_candidate, "stale_candidate").expect("stale candidate is nonnegative");
    let fresh_seq = i64_to_u64(fresh_seq, "fresh_seq").expect("fresh sequence is nonnegative");
    assert_eq!(stale_store.high_water(), fresh_seq);

    let page = stale_store
        .read_export_page(
            Some(ExportCursor {
                source_instance_id: stale_store.source_instance_id().to_string(),
                source_epoch: stale_store.source_epoch(),
                export_seq: stale_candidate,
            }),
            1_000,
        )
        .expect("read page after stale update attempt");
    let max_page_seq = page
        .facts
        .iter()
        .map(|fact| fact.export_seq)
        .max()
        .expect("the inserted fact is present");
    assert_eq!(max_page_seq, fresh_seq);
    assert!(page.high_water >= max_page_seq);
}

#[cfg(test)]
pub(crate) fn assert_writers_lock_before_allocating_sequences(
    url: &str,
    records: Vec<FramedRecord>,
) {
    use std::time::{Duration, Instant};
    let schema = format!("export_order_{}", std::process::id());
    let mut observer = Client::connect(url, postgres::NoTls).unwrap();
    observer
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .unwrap();
    let separator = if url.contains('?') { '&' } else { '?' };
    let isolated = format!("{url}{separator}options=-csearch_path%3D{schema}");
    let a = PostgresStore::connect(&isolated).unwrap();
    let b = PostgresStore::connect(&isolated).unwrap();
    let reader = PostgresStore::connect(&isolated).unwrap();
    let pids = [&a, &b].map(|store| {
        store
            .client
            .lock()
            .unwrap()
            .query_one("SELECT pg_backend_pid()", &[])
            .unwrap()
            .get::<_, i32>(0)
    });
    let mut holder = Client::connect(&isolated, postgres::NoTls).unwrap();
    let mut tx = holder.transaction().unwrap();
    tx.query_one("SELECT id FROM export_meta WHERE id=1 FOR UPDATE", &[])
        .unwrap();
    let workers: Vec<_> = [a, b]
        .into_iter()
        .enumerate()
        .map(|(index, store)| {
            let mut records = records.clone();
            let run_id = RunId(format!("writer-{index}"));
            if let broca_wal::WalRecord::RunStarted {
                run_id: recorded, ..
            } = &mut records[0].record
            {
                *recorded = run_id.clone();
            }
            std::thread::spawn(move || {
                let resolver = |_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1);
                store.emit_export_run(&records, &run_id, &resolver).unwrap();
            })
        })
        .collect();
    // Observe a server-side lock wait, not a timing guess that a thread has started.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut blocked_queries = Vec::new();
    while Instant::now() < deadline {
        blocked_queries = observer
            .query(
                "SELECT query FROM pg_stat_activity WHERE pid = ANY($1) AND wait_event_type='Lock'",
                &[&pids.as_slice()],
            )
            .unwrap()
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect();
        if blocked_queries.len() == 2 {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let sequence_called: bool = tx
        .query_one("SELECT is_called FROM export_facts_export_seq_seq", &[])
        .unwrap()
        .get(0);
    let page = reader.read_export_page(None, 1000).unwrap();
    tx.rollback().unwrap();
    for worker in workers {
        worker.join().unwrap();
    }
    let committed = reader.read_export_page(None, 1000).unwrap();
    let after = reader
        .read_export_page(committed.next.clone(), 1000)
        .unwrap();
    observer
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .unwrap();
    assert_eq!(
        blocked_queries.len(),
        2,
        "both real connections must reach the held lock"
    );
    assert!(
        !sequence_called,
        "writers must wait before allocating export_seq"
    );
    assert!(
        blocked_queries
            .iter()
            .all(|query| query.contains("FOR UPDATE")),
        "the first writer statement must lock export_meta: {blocked_queries:?}"
    );
    assert!(page.facts.is_empty());
    assert_eq!(page.high_water, 0);
    assert_eq!(committed.facts.len(), 2);
    assert_eq!(
        committed
            .facts
            .iter()
            .map(|fact| fact.export_seq)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(committed.high_water, 2);
    assert!(after.facts.is_empty());
}
