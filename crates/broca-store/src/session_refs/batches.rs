//! Shared write-lock budget for background discovery migration.
use crate::StoreError;
use rusqlite::{Connection, Transaction, TransactionBehavior};
use std::time::{Duration, Instant};

#[cfg(test)]
thread_local! {
    pub(super) static AFTER_WRITE_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

pub(super) const MAX_BATCH_ITEMS: usize = 64;
pub(super) const MAX_WRITE_WORK: Duration = Duration::from_millis(10);

#[derive(Debug, Default, Clone, Copy)]
pub struct MigrationWriteStats {
    pub transactions: u64,
    /// Successful BEGIN IMMEDIATE through COMMIT/rollback, including fsync.
    pub max_lock_us: u128,
    pub max_batch_items: usize,
}
impl MigrationWriteStats {
    pub(super) fn merge(&mut self, other: Self) {
        self.transactions += other.transactions;
        self.max_lock_us = self.max_lock_us.max(other.max_lock_us);
        self.max_batch_items = self.max_batch_items.max(other.max_batch_items);
    }
}

pub(super) fn write_batch<T>(
    conn: &Connection,
    items: &[T],
    stats: &mut MigrationWriteStats,
    paced: bool,
    mut apply: impl FnMut(&Transaction<'_>, &T) -> Result<(), StoreError>,
    finish: impl FnOnce(&Transaction<'_>, &[T]) -> Result<(), StoreError>,
) -> Result<usize, StoreError> {
    let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
    // Acquisition may wait for another writer; only time actually owning the
    // write lock belongs in this measurement and in the work budget.
    let started = Instant::now();
    let mut used = 0;
    let result = (|| {
        for item in items.iter().take(MAX_BATCH_ITEMS) {
            apply(&tx, item)?;
            used += 1;
            if started.elapsed() >= MAX_WRITE_WORK {
                break;
            }
        }
        finish(&tx, &items[..used])?;
        tx.commit()?;
        Ok(used)
    })();
    let held = started.elapsed();
    stats.transactions += 1;
    stats.max_lock_us = stats.max_lock_us.max(held.as_micros());
    stats.max_batch_items = stats.max_batch_items.max(used);
    #[cfg(test)]
    AFTER_WRITE_HOOK.with(|hook| {
        if let Some(hook) = hook.borrow_mut().take() {
            hook();
        }
    });
    // SQLite's busy handler backs off. An explicit unlocked interval, rather
    // than immediately reacquiring, lets foreground projection/export win.
    if paced {
        std::thread::sleep(held.saturating_mul(3).max(Duration::from_millis(2)));
    }
    std::thread::yield_now();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_write_budget_yields_before_the_row_limit() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE sample(id INTEGER PRIMARY KEY)")
            .unwrap();
        let items = [1, 2, 3, 4];
        let mut stats = MigrationWriteStats::default();
        let used = write_batch(
            &conn,
            &items,
            &mut stats,
            false,
            |tx, id| {
                tx.execute("INSERT INTO sample VALUES(?1)", [id])?;
                std::thread::sleep(MAX_WRITE_WORK.saturating_mul(2));
                Ok(())
            },
            |_, _| Ok(()),
        )
        .unwrap();
        assert_eq!(
            used, 1,
            "slow work must release the write lock before consuming the remaining rows"
        );
        assert_eq!(stats.max_batch_items, 1);
        assert!(stats.max_lock_us >= MAX_WRITE_WORK.as_micros());
        assert_eq!(
            conn.query_row("SELECT count(*) FROM sample", [], |row| row
                .get::<_, u64>(0))
                .unwrap(),
            1
        );
    }

    #[test]
    fn migration_write_budget_caps_fast_rows_too() {
        let conn = Connection::open_in_memory().unwrap();
        let items: Vec<_> = (0..MAX_BATCH_ITEMS * 2).collect();
        let mut stats = MigrationWriteStats::default();
        let used = write_batch(
            &conn,
            &items,
            &mut stats,
            false,
            |_, _| Ok(()),
            |_, _| Ok(()),
        )
        .unwrap();
        assert!(used <= MAX_BATCH_ITEMS);
        assert_eq!(stats.max_batch_items, used);
    }
}
