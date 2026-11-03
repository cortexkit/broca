//! Lazy display facts share the store transaction and lifetime of their session.

use broca_core::display_facts::{DisplayFactVersion, DisplayFactsCache, DisplayFactsSeed};
use broca_protocol::BindIdentity;
use rusqlite::{params, OptionalExtension};

use crate::SqliteStore;

impl SqliteStore {
    pub(crate) fn display_cache_read(
        &self,
        session: &BindIdentity,
        lineage: &str,
        at_seq: u64,
        ordinals: &[u64],
    ) -> Result<DisplayFactsCache, String> {
        let mut conn = self.conn.lock().map_err(|e| e.to_string())?;
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        let key = Self::key(session);
        let publication_epoch = publication_epoch(self, &tx, &key)?;
        let seed = cached_seed(&tx, &key, lineage)?;
        let mut rows = std::collections::BTreeMap::new();
        if seed.is_some() {
            let mut query = tx.prepare("SELECT facts FROM display_fact WHERE session=?1 AND lineage=?2 AND ord=?3 AND seq<=?4 ORDER BY seq DESC LIMIT 1").map_err(|e| e.to_string())?;
            for ordinal in ordinals {
                if let Some(json) = query
                    .query_row(params![key, lineage, ordinal, at_seq], |row| {
                        row.get::<_, String>(0)
                    })
                    .optional()
                    .map_err(|e| e.to_string())?
                {
                    rows.insert(
                        *ordinal,
                        serde_json::from_str(&json).map_err(|e| e.to_string())?,
                    );
                }
            }
        }
        Ok(DisplayFactsCache {
            publication_epoch,
            seed,
            rows,
        })
    }

    pub(crate) fn display_cache_publish(
        &self,
        session: &BindIdentity,
        lineage: &str,
        expected: Option<u64>,
        expected_epoch: Option<i64>,
        seed: &DisplayFactsSeed,
        versions: &[DisplayFactVersion],
    ) -> Result<(), String> {
        let mut conn = self.conn.lock().map_err(|e| e.to_string())?;
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        let key = Self::key(session);
        if publication_epoch(self, &tx, &key)? != expected_epoch {
            return Err("display facts source changed during WAL recovery".into());
        }
        let current = cached_seed(&tx, &key, lineage)?;
        let expired: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM retention_lineage WHERE session=?1 AND lineage_id=?2 AND expired_at_ms IS NOT NULL)", params![key,lineage], |row| row.get(0)).map_err(|e| e.to_string())?;
        if expired {
            return Ok(());
        }
        if current.as_ref().map(|seed| seed.through_seq) != expected {
            return Ok(());
        }
        if current.is_none() {
            tx.execute(
                "DELETE FROM display_fact WHERE session=?1 AND lineage=?2",
                params![key, lineage],
            )
            .map_err(|e| e.to_string())?;
        }
        {
            let mut insert = tx.prepare("INSERT OR REPLACE INTO display_fact(session,lineage,ord,seq,facts) VALUES(?1,?2,?3,?4,?5)").map_err(|e| e.to_string())?;
            for version in versions {
                insert
                    .execute(params![
                        key,
                        lineage,
                        version.ordinal,
                        version.seq,
                        serde_json::to_string(&version.facts).map_err(|e| e.to_string())?
                    ])
                    .map_err(|e| e.to_string())?;
            }
        }
        tx.execute(
            "INSERT OR REPLACE INTO display_fact_seed(session,lineage,state) VALUES(?1,?2,?3)",
            params![
                key,
                lineage,
                serde_json::to_string(seed).map_err(|e| e.to_string())?
            ],
        )
        .map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())
    }
}

fn publication_epoch(
    store: &SqliteStore,
    tx: &rusqlite::Transaction<'_>,
    key: &str,
) -> Result<Option<i64>, String> {
    if !store.wal_config.is_overhaul() {
        return Ok(None);
    }
    if crate::wal_recovery::read_fence(tx, key)
        .map_err(|e| e.to_string())?
        .is_some_and(|fence| fence.pending)
    {
        return Err("display facts are unavailable during WAL recovery".into());
    }
    crate::current_projection_state(tx, key, true)
        .map(|(_, _, epoch)| Some(epoch))
        .map_err(|e| e.to_string())
}

fn cached_seed(
    conn: &rusqlite::Connection,
    session: &str,
    lineage: &str,
) -> Result<Option<DisplayFactsSeed>, String> {
    let json = conn
        .query_row(
            "SELECT state FROM display_fact_seed WHERE session=?1 AND lineage=?2",
            params![session, lineage],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|e| e.to_string())?;
    Ok(json
        .and_then(|json| serde_json::from_str::<DisplayFactsSeed>(&json).ok())
        .filter(|seed| seed.version == broca_core::display_facts::DISPLAY_FACTS_VERSION))
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_core::display_facts::DisplayFacts;

    #[test]
    fn display_rows_and_completion_marker_commit_atomically_and_keep_snapshot_versions() {
        let root = broca_test_support::scratch_root()
            .join(format!("display-cache-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let store = SqliteStore::open(root.join("store.db")).unwrap();
        let session = BindIdentity::new("/project", "runner", "facts");
        let other = BindIdentity::new("/project", "runner", "untouched");
        let version = |seq, text: &str| DisplayFactVersion {
            ordinal: 0,
            seq,
            facts: DisplayFacts {
                client_message_id: Some(text.into()),
                ..Default::default()
            },
        };
        let mut seed = DisplayFactsSeed::default();
        seed.through_seq = 1;
        store.conn.lock().unwrap().execute_batch("CREATE TRIGGER fail_display BEFORE INSERT ON display_fact BEGIN SELECT RAISE(ABORT,'injected display cache failure'); END;").unwrap();
        assert!(store
            .display_cache_publish(
                &session,
                "lineage",
                None,
                None,
                &seed,
                &[version(1, "first")]
            )
            .is_err());
        assert!(store
            .display_cache_read(&session, "lineage", 1, &[0])
            .unwrap()
            .seed
            .is_none());
        store
            .conn
            .lock()
            .unwrap()
            .execute_batch("DROP TRIGGER fail_display")
            .unwrap();
        store
            .display_cache_publish(
                &session,
                "lineage",
                None,
                None,
                &seed,
                &[version(1, "first")],
            )
            .unwrap();
        assert!(store
            .display_cache_read(&other, "lineage", 1, &[0])
            .unwrap()
            .seed
            .is_none());
        seed.through_seq = 2;
        store
            .display_cache_publish(
                &session,
                "lineage",
                Some(1),
                None,
                &seed,
                &[version(2, "second")],
            )
            .unwrap();
        let first = store
            .display_cache_read(&session, "lineage", 1, &[0])
            .unwrap();
        assert_eq!(first.seed.unwrap().through_seq, 2);
        assert_eq!(first.rows[&0].client_message_id.as_deref(), Some("first"));
        store
            .display_cache_publish(
                &session,
                "lineage",
                Some(1),
                None,
                &seed,
                &[version(2, "stale writer")],
            )
            .unwrap();
        let second = store
            .display_cache_read(&session, "lineage", 2, &[0])
            .unwrap();
        assert_eq!(second.rows[&0].client_message_id.as_deref(), Some("second"));
        assert!(store
            .display_cache_read(&session, "another-lineage", 2, &[0])
            .unwrap()
            .seed
            .is_none());
        store.drop_session_projection(&session).unwrap();
        store.conn.lock().unwrap().execute("INSERT INTO retention_lineage(lineage_id,session,retention_s,last_activity_ms,expires_at_ms,wal_bytes,expired_at_ms) VALUES(?1,?2,60,1,NULL,0,2)", params!["lineage", SqliteStore::key(&session)]).unwrap();
        store
            .display_cache_publish(
                &session,
                "lineage",
                None,
                None,
                &seed,
                &[version(2, "expired")],
            )
            .unwrap();
        assert!(
            store
                .display_cache_read(&session, "lineage", 2, &[0])
                .unwrap()
                .seed
                .is_none(),
            "an in-flight reader cannot repopulate an expired lineage"
        );
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
}
