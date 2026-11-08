use super::*;

pub(super) const EMPTY_EXPIRED_JOURNAL: &str = r#"{"expired":true,"outbox":[]}"#;

/// Opaque routing facts owned by the classifier implementation. Null columns identify a
/// legacy pinned batch; opening a store never turns one into a routed batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutingRecord {
    pub choice: String,
    pub runs: String,
    pub pending: bool,
}

impl SystemOneStore {
    pub fn routing_batch_running(&self, role: &str, batch_id: &str) -> Result<bool> {
        Ok(self
            .lock()
            .query_row(
                "SELECT state='running' FROM batch WHERE role=?1 AND batch_id=?2",
                rusqlite::params![role, batch_id],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or(false))
    }

    pub fn routing(&self, role: &str, batch_id: &str) -> Result<Option<RoutingRecord>> {
        Ok(self
            .lock()
            .query_row(
                "SELECT routing_choice, routing_runs, routing_pending FROM batch
             WHERE role=?1 AND batch_id=?2 AND routing_choice IS NOT NULL",
                rusqlite::params![role, batch_id],
                |row| {
                    Ok(RoutingRecord {
                        choice: row.get(0)?,
                        runs: row.get(1)?,
                        pending: row.get::<_, i64>(2)? != 0,
                    })
                },
            )
            .optional()?)
    }

    /// Freeze a choice before dispatch. Replacing it is legal only while no item has been
    /// answered; progress on the same choice does not change the model or frozen prices.
    pub fn freeze_routing(
        &self,
        role: &str,
        batch_id: &str,
        owner: &str,
        model: &str,
        frozen_row: &str,
        routing: &RoutingRecord,
    ) -> Result<bool> {
        let mut conn = self.lock();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = tx.execute(
            "UPDATE batch SET model=?4, frozen_row=?5, routing_choice=?6,
                              routing_runs=?7, routing_pending=?8
             WHERE role=?1 AND batch_id=?2 AND owner=?3 AND state='running'
               AND NOT EXISTS (SELECT 1 FROM item_outcome WHERE role=?1 AND batch_id=?2 AND kind='answered')",
            rusqlite::params![role, batch_id, owner, model, frozen_row, routing.choice, routing.runs, routing.pending],
        )?;
        if changed != 0 {
            // A pre-answer reselection restarts the whole batch, including transient errors.
            tx.execute(
                "DELETE FROM item_outcome WHERE role=?1 AND batch_id=?2",
                rusqlite::params![role, batch_id],
            )?;
        }
        tx.commit()?;
        Ok(changed != 0)
    }

    pub fn save_routing_runs(
        &self,
        role: &str,
        batch_id: &str,
        owner: &str,
        runs: &str,
        pending: bool,
    ) -> Result<bool> {
        Ok(self.lock().execute(
            "UPDATE batch SET routing_runs=?4, routing_pending=?5
             WHERE role=?1 AND batch_id=?2 AND owner=?3 AND state='running' AND routing_choice IS NOT NULL",
            rusqlite::params![role, batch_id, owner, runs, pending],
        )? != 0)
    }

    /// Idle rows only: recovery must claim each batch before changing its outbox.
    pub fn pending_routing_batches(&self, role: &str) -> Result<Vec<String>> {
        let conn = self.lock();
        let mut statement = conn.prepare(
            "SELECT batch_id FROM batch WHERE role=?1 AND state='idle' AND routing_pending=1",
        )?;
        let rows = statement.query_map([role], |row| row.get(0))?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    /// Include expired routed rows even when they have no outstanding RPCs, so
    /// content retention does not depend on the state root's retention worker.
    pub fn routing_recovery_batches(&self, role: &str, now_ms: i64) -> Result<Vec<String>> {
        let conn = self.lock();
        let mut statement = conn.prepare("SELECT batch_id FROM batch WHERE role=?1 AND state='idle' AND routing_choice IS NOT NULL AND (routing_pending=1 OR admitted_at_ms<=?2) ORDER BY admitted_at_ms, batch_id")?;
        let rows = statement.query_map(
            rusqlite::params![role, now_ms.saturating_sub(RETENTION_MS)],
            |row| row.get(0),
        )?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    pub fn claim_routing_recovery(
        &self,
        role: &str,
        batch_id: &str,
        owner: &str,
        now_ms: i64,
    ) -> Result<bool> {
        self.hold_owner_lifetime(owner)?;
        Ok(self.lock().execute("UPDATE batch SET state='running', owner=?3, started_at_ms=?4 WHERE role=?1 AND batch_id=?2 AND state='idle' AND routing_choice IS NOT NULL", rusqlite::params![role, batch_id, owner, now_ms])? != 0)
    }

    /// Remove caller content atomically while retaining only unresolved RPC bodies.
    /// A content-free tombstone prevents reusing an id whose old report is still owed.
    pub fn expire_routed(
        &self,
        role: &str,
        batch_id: &str,
        owner: &str,
        archive: Option<&str>,
    ) -> Result<()> {
        let mut conn = self.lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let held: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM batch WHERE role=?1 AND batch_id=?2 AND owner=?3 AND state='running')", rusqlite::params![role,batch_id,owner], |row| row.get(0))?;
        if !held {
            return Err(super::SystemOneStoreError(
                "routing recovery lost batch hold".into(),
            ));
        }
        tx.execute(
            "DELETE FROM item_outcome WHERE role=?1 AND batch_id=?2",
            rusqlite::params![role, batch_id],
        )?;
        // Keep a content-free running row even with no debt. The owner's release deletes
        // that empty tombstone atomically, without a stale release touching a new batch.
        tx.execute("UPDATE batch SET model='', identity='{}', body_digest='', frozen_row='{}', scope_json=NULL, ceiling_nanos=NULL, spent_nanos=0, routing_choice='null', routing_runs=?3, routing_pending=?4 WHERE role=?1 AND batch_id=?2", rusqlite::params![role,batch_id,archive.unwrap_or(EMPTY_EXPIRED_JOURNAL), i64::from(archive.is_some())])?;
        tx.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_exit_expiry_holds_the_old_incarnation_until_atomic_release() {
        let store = SystemOneStore::open_in_memory().unwrap();
        let scope = BatchScope::Flow {
            flow_id: "flow".into(),
        };
        let mut claim = BatchClaim {
            role: "classifier/v1",
            batch_id: "routed",
            model: "model",
            identity: "private input",
            body_digest: "digest",
            frozen_row: "{}",
            scope: &scope,
            ceiling_nanos: None,
            owner: "same-process",
            now_ms: 1,
        };
        assert!(matches!(
            store.claim_routed(&claim, 1).unwrap(),
            Claimed::Fresh
        ));
        store
            .expire_routed(claim.role, claim.batch_id, claim.owner, None)
            .unwrap();
        let held = store.lookup(claim.role, claim.batch_id).unwrap();
        assert!(
            held.as_ref()
                .is_some_and(|row| row.running && row.identity == "{}"),
            "expiry must not expose a fresh same-id incarnation before the old hold releases"
        );
        claim.now_ms = RETENTION_MS + 2;
        assert!(!matches!(
            store.claim_routed(&claim, claim.now_ms).unwrap(),
            Claimed::Fresh
        ));
        store
            .release(claim.role, claim.batch_id, claim.owner)
            .unwrap();
        assert!(store.lookup(claim.role, claim.batch_id).unwrap().is_none());
        assert!(matches!(
            store.claim_routed(&claim, claim.now_ms).unwrap(),
            Claimed::Fresh
        ));
        assert!(
            matches!(
                store.claim_routed(&claim, claim.now_ms).unwrap(),
                Claimed::InProgress
            ),
            "the new incarnation stays single-flight after the old one is gone"
        );
    }

    #[test]
    fn audit_exit_live_claim_never_resumes_expired_routed_content() {
        let store = SystemOneStore::open_in_memory().unwrap();
        let scope = BatchScope::Flow {
            flow_id: "flow".into(),
        };
        let mut claim = BatchClaim {
            role: "classifier/v1",
            batch_id: "routed",
            model: "model",
            identity: "private input",
            body_digest: "digest",
            frozen_row: "{}",
            scope: &scope,
            ceiling_nanos: None,
            owner: "same-process",
            now_ms: 1,
        };
        store.claim_routed(&claim, 1).unwrap();
        store
            .release(claim.role, claim.batch_id, claim.owner)
            .unwrap();
        claim.now_ms = RETENTION_MS + 1;
        let claimed = store.claim_routed(&claim, claim.now_ms).unwrap();
        assert!(
            !matches!(claimed, Claimed::Fresh | Claimed::Resumed(_)),
            "a live claim must refuse expired routed content before taking its hold: {claimed:?}"
        );
        assert!(
            !store
                .lookup(claim.role, claim.batch_id)
                .unwrap()
                .unwrap()
                .running
        );
    }

    #[test]
    fn audit_exit_stale_expiry_release_cannot_authorize_duplicate_dispatch() {
        let store = SystemOneStore::open_in_memory().unwrap();
        let scope = BatchScope::Flow {
            flow_id: "flow".into(),
        };
        let mut claim = BatchClaim {
            role: "classifier/v1",
            batch_id: "routed",
            model: "model",
            identity: "input",
            body_digest: "digest",
            frozen_row: "{}",
            scope: &scope,
            ceiling_nanos: None,
            owner: "same-process",
            now_ms: 1,
        };
        store.claim_routed(&claim, 1).unwrap();
        store
            .expire_routed(claim.role, claim.batch_id, claim.owner, None)
            .unwrap();
        claim.now_ms = RETENTION_MS + 2;
        let contender = store.claim_routed(&claim, claim.now_ms).unwrap();
        if matches!(contender, Claimed::Fresh) {
            // Reproduce the original interleaving: recovery deleted the old row, a
            // new call created its row, and only then did the old hold release it.
            store
                .release(claim.role, claim.batch_id, claim.owner)
                .unwrap();
            assert_eq!(
                store.claim_routed(&claim, claim.now_ms).unwrap(),
                Claimed::InProgress,
                "the stale expiry release authorized a duplicate provider dispatch"
            );
        } else {
            assert_eq!(contender, Claimed::Expired);
            store
                .release(claim.role, claim.batch_id, claim.owner)
                .unwrap();
            assert_eq!(
                store.claim_routed(&claim, claim.now_ms).unwrap(),
                Claimed::Fresh
            );
            assert_eq!(
                store.claim_routed(&claim, claim.now_ms).unwrap(),
                Claimed::InProgress
            );
        }
    }

    #[test]
    fn audit_routing_archive_scrub_rolls_back_as_one_transaction() {
        let store = SystemOneStore::open_in_memory().unwrap();
        let scope = BatchScope::Flow {
            flow_id: "flow".into(),
        };
        let claim = BatchClaim {
            role: "classifier/v1",
            batch_id: "routed",
            model: "model",
            identity: "private input",
            body_digest: "digest",
            frozen_row: "{}",
            scope: &scope,
            ceiling_nanos: None,
            owner: "owner",
            now_ms: 1,
        };
        store.claim_routed(&claim, 1).unwrap();
        store
            .record(
                claim.role,
                claim.batch_id,
                claim.owner,
                &StoredOutcome {
                    index: 0,
                    outcome: Outcome::Answered("private answer".into()),
                    input_tokens: Some(7),
                    output_tokens: None,
                },
                0,
                1,
            )
            .unwrap();
        store.lock().execute_batch("CREATE TRIGGER interrupt_archive BEFORE UPDATE OF identity ON batch WHEN NEW.identity='{}' BEGIN SELECT RAISE(FAIL, 'crash during scrub'); END;").unwrap();
        let archive =
            r#"{"expired":true,"outbox":[{"method":"route.report","params":{"decision_id":"d"}}]}"#;
        assert!(store
            .expire_routed(claim.role, claim.batch_id, claim.owner, Some(archive))
            .is_err());
        let before = store.lookup(claim.role, claim.batch_id).unwrap().unwrap();
        assert_eq!(before.identity, "private input");
        assert_eq!(
            before.outcomes.len(),
            1,
            "delete must roll back with interrupted scrub"
        );
        store
            .lock()
            .execute_batch("DROP TRIGGER interrupt_archive;")
            .unwrap();
        store
            .expire_routed(claim.role, claim.batch_id, claim.owner, Some(archive))
            .unwrap();
        let after = store.lookup(claim.role, claim.batch_id).unwrap().unwrap();
        assert_eq!(after.identity, "{}");
        assert!(after.outcomes.is_empty());
        assert!(after.scope.is_none());
        assert_eq!(after.model, "");
        assert_eq!(
            store
                .routing(claim.role, claim.batch_id)
                .unwrap()
                .unwrap()
                .runs,
            archive
        );
    }

    #[test]
    fn routing_columns_are_nullable_without_defaults_or_legacy_backfill() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch("CREATE TABLE batch (
            role TEXT NOT NULL, batch_id TEXT NOT NULL, model TEXT NOT NULL, identity TEXT NOT NULL,
            body_digest TEXT NOT NULL, frozen_row TEXT NOT NULL, ceiling_nanos INTEGER,
            spent_nanos INTEGER NOT NULL DEFAULT 0, state TEXT NOT NULL, owner TEXT,
            started_at_ms INTEGER, admitted_at_ms INTEGER NOT NULL, scope_json TEXT,
            PRIMARY KEY(role,batch_id));
            CREATE INDEX batch_admitted ON batch (admitted_at_ms);
            INSERT INTO batch VALUES ('classifier/v1','legacy','typesafe/jev-1.13.0','identity','digest','{}',NULL,42,'idle',NULL,NULL,1,NULL);").unwrap();
        let indexes = |connection: &Connection| {
            let mut statement = connection.prepare("SELECT name,sql FROM sqlite_master WHERE type='index' AND tbl_name='batch' ORDER BY name").unwrap();
            statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
                })
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap()
        };
        let original_indexes = indexes(&connection);
        let store = SystemOneStore::from_connection(connection, None).unwrap();
        assert!(store.routing("classifier/v1", "legacy").unwrap().is_none());
        let connection = store.lock();
        let mut columns = connection.prepare("PRAGMA table_info(batch)").unwrap();
        let columns = columns
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            })
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        for name in ["routing_choice", "routing_runs", "routing_pending"] {
            assert!(columns.contains(&(name.into(), 0, None)), "{columns:?}");
        }
        assert!(connection.query_row("SELECT routing_choice IS NULL AND routing_runs IS NULL AND routing_pending IS NULL AND spent_nanos=42 FROM batch", [], |row| row.get::<_, bool>(0)).unwrap());
        assert_eq!(indexes(&connection), original_indexes);
    }

    #[test]
    fn routed_progress_commits_with_outcome_and_cannot_replace_an_answered_choice() {
        let store = SystemOneStore::open_in_memory().unwrap();
        let scope = BatchScope::Flow {
            flow_id: "flow".into(),
        };
        let claim = BatchClaim {
            role: "classifier/v1",
            batch_id: "routed",
            model: "",
            identity: "identity",
            body_digest: "digest",
            frozen_row: "{}",
            scope: &scope,
            ceiling_nanos: None,
            owner: "owner",
            now_ms: 1,
        };
        assert!(matches!(
            store.claim_routed(&claim, 1).unwrap(),
            Claimed::Fresh
        ));
        assert_eq!(
            store
                .routing(claim.role, claim.batch_id)
                .unwrap()
                .unwrap()
                .choice,
            "null"
        );
        let choice = RoutingRecord {
            choice:
                r#"{"model":"typesafe/jev-1.13.0","credential_id":"apikey:a","decision_id":"d"}"#
                    .into(),
            runs: r#"{"runs":[]}"#.into(),
            pending: true,
        };
        assert!(store
            .freeze_routing(
                claim.role,
                claim.batch_id,
                claim.owner,
                "typesafe/jev-1.13.0",
                "{}",
                &choice
            )
            .unwrap());
        let outcome = StoredOutcome {
            index: 0,
            outcome: Outcome::Answered("{}".into()),
            input_tokens: Some(10),
            output_tokens: None,
        };
        assert!(store
            .record_paid_routed(
                claim.role,
                claim.batch_id,
                claim.owner,
                &outcome,
                &PaidRecord {
                    spend_nanos: 10,
                    now_ms: 2,
                    fact: None
                },
                Some(r#"{"runs":[{"input_tokens":10}]}"#)
            )
            .unwrap());
        assert_eq!(
            store
                .routing(claim.role, claim.batch_id)
                .unwrap()
                .unwrap()
                .runs,
            r#"{"runs":[{"input_tokens":10}]}"#
        );
        assert_eq!(
            store.outcomes(claim.role, claim.batch_id).unwrap(),
            vec![outcome]
        );
        assert!(!store
            .freeze_routing(
                claim.role,
                claim.batch_id,
                claim.owner,
                "another/model",
                "{}",
                &choice
            )
            .unwrap());
        store
            .release(claim.role, claim.batch_id, claim.owner)
            .unwrap();
        assert!(!store
            .save_routing_runs(claim.role, claim.batch_id, claim.owner, "lost-owner", false)
            .unwrap());
        assert_eq!(
            store.sweep(RETENTION_MS + 2).unwrap().batches,
            0,
            "pending reports outlive ordinary batch retention"
        );
    }
}
