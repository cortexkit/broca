# Legacy keeper fixture

`keepers-v044.db` was generated on Linux from broca 0.4.4, commit
`0897a5e6c642d4ca94644319eb6d3e7cc1b442fb`, before the standalone connection extraction.
It contains synthetic data only. Keep this database frozen: regenerating it with
the extracted implementation would lose the independent compatibility check.

Generation used a temporary unit test in `export_index::tests`:

1. `SqliteStore::open_in_memory()` created the complete legacy schema.
2. `emit_export_run(records("fixture-run", 2), RunId("fixture-run"), resolver)`
   used that module's existing synthetic billing helpers to publish two facts.
3. `record_imported_runs` recorded `fixture-imported`, under `fixture-cut`, for
   `BindIdentity::new("/fixture/original", "fixture", "known")`.
4. The test inserted `agent_data_projection(1,1)` (backfill complete) and
   `agent_data_census_done(1)` (discovery complete), then called
   `resolve_session_address` to reserve that identity with workdir
   `/fixture/original`, owner `Reserved { module_id: "core" }` and scope ref
   `fixture-scope`.
5. `record_session` recorded both `/fixture/one` and `/fixture/two` for
   `(fixture, ambiguous)` to exercise ambiguous aliases.
6. SQLite `VACUUM INTO` wrote the database to this fixture path.

The temporary generator passed as one filtered nextest test and was then removed.
The fixture includes all legacy tables, including empty projections, so compatibility
tests can detect changes to unrelated schema and rows as well as keeper data.
