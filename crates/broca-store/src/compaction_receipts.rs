//! Durable announcements, never view bodies. The session WAL remains the authority.
use crate::{SqliteStore, StoreError};
use broca_core::compaction_provider::notice::ViewReady;
use rusqlite::{params, Connection};

pub(crate) fn init_schema(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS compaction_view_receipt (
        session_ref TEXT PRIMARY KEY, lineage_id TEXT NOT NULL, view_version TEXT NOT NULL,
        covers_through TEXT NOT NULL, digest TEXT NOT NULL, received_at_ms INTEGER NOT NULL
    ) WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS compaction_receipt_lineage ON compaction_view_receipt(lineage_id);
    CREATE TRIGGER IF NOT EXISTS compaction_receipt_lineage_ended AFTER DELETE ON retention_deletion BEGIN
        DELETE FROM compaction_view_receipt WHERE lineage_id=OLD.lineage_id;
    END;",
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewReceipt {
    pub ready: ViewReady,
    pub lineage_id: String,
    pub received_at_ms: u64,
}

/// What committing a `view_ready` receipt did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReceiptWrite {
    /// A receipt for this version, or a newer one of the same lineage, was already durable.
    Held,
    /// The receipt is durable. The store keeps one receipt per session, so if `replaced` is
    /// `Some`, it is the prior receipt this write replaced, which is no longer stored.
    Recorded { replaced: Option<ReplacedReceipt> },
}

/// The receipt a newer delivery overwrote, as it was stored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplacedReceipt {
    pub lineage_id: String,
    pub view_version: String,
}

impl SqliteStore {
    /// Commit before acknowledging the bus delivery. Decimal strings preserve all u64 versions.
    pub fn receive_compaction_view(
        &self,
        receipt: &ViewReceipt,
    ) -> Result<ReceiptWrite, StoreError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let held = tx.query_row(
            "SELECT lineage_id,view_version FROM compaction_view_receipt WHERE session_ref=?1",
            [&receipt.ready.session_ref],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        );
        let replaced = match held {
            Ok((lineage, version))
                if lineage == receipt.lineage_id
                    && version
                        .parse::<u64>()
                        .ok()
                        .is_some_and(|v| v >= receipt.ready.view_version) =>
            {
                return Ok(ReceiptWrite::Held)
            }
            Ok((lineage_id, view_version)) => Some(ReplacedReceipt {
                lineage_id,
                view_version,
            }),
            Err(rusqlite::Error::QueryReturnedNoRows) => None,
            Err(error) => return Err(error.into()),
        };
        tx.execute(
            "INSERT OR REPLACE INTO compaction_view_receipt VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                receipt.ready.session_ref,
                receipt.lineage_id,
                receipt.ready.view_version.to_string(),
                receipt.ready.covers_through.to_string(),
                receipt.ready.digest,
                receipt.received_at_ms
            ],
        )?;
        tx.commit()?;
        Ok(ReceiptWrite::Recorded { replaced })
    }

    /// Page startup recovery so a large receipt table cannot create an unbounded database read.
    pub fn compaction_receipts(
        &self,
        after: &str,
        limit: usize,
    ) -> Result<Vec<ViewReceipt>, StoreError> {
        let conn = self.conn.lock().unwrap();
        let mut query = conn.prepare_cached("SELECT session_ref,lineage_id,view_version,covers_through,digest,received_at_ms FROM compaction_view_receipt WHERE session_ref>?1 ORDER BY session_ref LIMIT ?2")?;
        let rows = query.query_map(params![after, limit.min(256) as u64], |r| {
            let parse = |column| -> Result<u64, rusqlite::Error> {
                let value: String = r.get(column)?;
                value.parse().map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        column,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })
            };
            Ok(ViewReceipt {
                ready: ViewReady::new(
                    r.get::<_, String>(0)?,
                    parse(2)?,
                    parse(3)?,
                    r.get::<_, String>(4)?,
                ),
                lineage_id: r.get(1)?,
                received_at_ms: r.get(5)?,
            })
        })?;
        rows.collect::<Result<_, _>>().map_err(Into::into)
    }

    /// Compare-and-delete cannot erase a newer receipt arriving during WAL projection.
    pub fn clear_compaction_receipt(&self, receipt: &ViewReceipt) -> Result<(), StoreError> {
        self.conn.lock().unwrap().execute("DELETE FROM compaction_view_receipt WHERE session_ref=?1 AND lineage_id=?2 AND view_version=?3 AND digest=?4", params![receipt.ready.session_ref, receipt.lineage_id, receipt.ready.view_version.to_string(), receipt.ready.digest])?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn receipts_are_durable_newest_only_and_compare_deleted() {
        let path = broca_test_support::scratch_root().join("compaction-receipts.db");
        let first = ViewReceipt {
            ready: ViewReady::new("a".repeat(32), 1, 3, "b".repeat(64)),
            lineage_id: "lineage".into(),
            received_at_ms: 123,
        };
        let mut newer = first.clone();
        newer.ready.view_version = u64::MAX;
        {
            let store = SqliteStore::open(&path).unwrap();
            assert_eq!(
                store.receive_compaction_view(&first).unwrap(),
                ReceiptWrite::Recorded { replaced: None }
            );
            assert_eq!(
                store.receive_compaction_view(&newer).unwrap(),
                ReceiptWrite::Recorded {
                    replaced: Some(ReplacedReceipt {
                        lineage_id: "lineage".into(),
                        view_version: "1".into(),
                    })
                }
            );
            assert_eq!(
                store.receive_compaction_view(&first).unwrap(),
                ReceiptWrite::Held
            );
            store.clear_compaction_receipt(&first).unwrap();
        }
        let store = SqliteStore::open(&path).unwrap();
        assert_eq!(
            store.compaction_receipts("", 256).unwrap(),
            vec![newer.clone()]
        );
        store.clear_compaction_receipt(&newer).unwrap();
        assert!(store.compaction_receipts("", 256).unwrap().is_empty());
        store.receive_compaction_view(&first).unwrap();
        let session = broca_protocol::BindIdentity::new("/test", "broca", "receipt");
        store
            .retention_deletion_pending("old-lineage", &session, true)
            .unwrap();
        store
            .retention_deletion_pending("old-lineage", &session, false)
            .unwrap();
        assert_eq!(store.compaction_receipts("", 256).unwrap().len(), 1);
        store
            .retention_deletion_pending(&first.lineage_id, &session, true)
            .unwrap();
        store
            .retention_deletion_pending(&first.lineage_id, &session, false)
            .unwrap();
        assert!(
            store.compaction_receipts("", 256).unwrap().is_empty(),
            "ended lineage retained a receipt"
        );
    }
}
