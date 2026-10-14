# Archived: `broca-store-postgres`

This crate was removed because Broca's running module stores data in SQLite, and no runtime path depends on a PostgreSQL backend. The PostgreSQL crate was used only by its own tests, a CI conformance job, and documentation.

The 2026-10-13 external audit confirmed these five defects. The source locations are listed so they can be checked and fixed before reusing the backend:

1. Withdrawal discovery omits the retention-expiry filter. It can therefore return runs whose WAL has been deleted (`src/lib.rs:1361`; SQLite applies `expired_at_ms IS NULL` at `../broca-store/src/lib.rs:1985-1988`).
2. `index_run_withdrawals_owed` returns success even when its update matches no run-index row (`src/lib.rs:1348-1356`).
3. `try_commit_projection` checks the expected watermark and then writes at PostgreSQL's default `READ COMMITTED` isolation without locking the `meta` row. Its client mutex is process-local, so independent stores can both pass the check and interleave message inserts (`src/lib.rs:603-633`).
4. Connect-time initialization races while applying schema changes and takes heavy locks (`src/lib.rs:364-447`, `src/export_index.rs:42-51`).
5. `projection_seed` opens a read-only transaction, but the shared seed path may update `meta.prefix_len`; PostgreSQL rejects that write (`src/lib.rs:1202-1220,1490`).

Additional audit findings below also affect the SQLite backend or its test coverage:

- Terminal runs that produce zero export segments remain missing indefinitely: both backends return without recording an export fact or import marker, while the missing-run query continues to select them (`src/export_index.rs:217-219,335-353`; SQLite counterpart `../broca-store/src/export_index.rs:251-254,540-558`).
- Projection commits set `withdrawals_owed` by `run_id` alone in both backends (`src/lib.rs:756-761`; SQLite counterpart `../broca-store/src/lib.rs:1397-1402`).
- The `postgres-projection-sets-withdrawal-flag` and `retention-approval-postgres-withdrawal-flag` mutation rows require a live PostgreSQL database, but the CI mutation lane has no service to provide one (`../../mutations.toml:2565-2572,2897-2910`).
