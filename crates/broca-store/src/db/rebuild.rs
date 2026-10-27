use rusqlite::params;

use super::{index, schema, DbError, IndexOutcome, OverhaulDb, PublishedSession};

/// Ledger decoding belongs to the retention owner. Rebuild consumes its complete
/// replay output, not the database's potentially stale copy of that output.
pub struct LedgerRows<'a> {
    pub condemned: &'a [CondemnedLineage<'a>],
    pub address_floors: &'a [AddressFloor<'a>],
}
pub struct CondemnedLineage<'a> {
    pub addr: &'a str,
    pub lineage_id: &'a str,
    pub removed_at_ms: i64,
}
pub struct AddressFloor<'a> {
    pub addr: &'a str,
    pub tag_floor: i64,
    pub notice_floor: i64,
}

impl OverhaulDb {
    pub fn rebuild_session(&self, log: &PublishedSession<'_>) -> Result<IndexOutcome, DbError> {
        self.transaction(false, |tx| index::index_in_tx(tx, log, true))
    }

    /// The whitelist is intentional: authoritative tables, including tables added
    /// by later state-machine writers, are never enumerated or cleared by rebuild.
    /// Supply the complete session inventory, not a changed-session subset.
    /// All sessions and ledger rows change atomically, or none do.
    pub fn rebuild<'a>(
        &self,
        sessions: impl IntoIterator<Item = PublishedSession<'a>>,
        ledger: &LedgerRows<'_>,
    ) -> Result<(), DbError> {
        self.transaction(false, |tx| {
            for table in schema::REBUILDABLE_TABLES {
                tx.execute(&format!("DELETE FROM {table}"), [])?;
            }
            for log in sessions {
                index::index_in_tx(tx, &log, false)?;
            }
            for row in ledger.condemned {
                tx.execute(
                    "INSERT INTO condemned (addr,lineage_id,removed_at_ms) VALUES (?1,?2,?3)",
                    params![row.addr, row.lineage_id, row.removed_at_ms],
                )?;
            }
            for row in ledger.address_floors {
                tx.execute(
                    "INSERT INTO address_floor (addr,tag_floor,notice_floor) VALUES (?1,?2,?3)",
                    params![row.addr, row.tag_floor, row.notice_floor],
                )?;
            }
            Ok(())
        })
    }
}
