# WAL fold maintenance

`store-maintenance wal-fold <state-root>` reports candidates and exact excluded-address lists as JSON. It does not create a lease, archive directory, journal, database, or live WAL. Coldness uses the last present record timestamp, not filesystem mtime; the threshold is 14 days. Decode failures, absent timestamps, absent identities, moved/re-keyed identities, empty files, lineage-only files, and warm files are excluded.

Add `--write` to execute. No daemon shutdown is required: the operator shares the daemon's per-session leases. Contention skips that session and is reported separately for each phase. A global OS-held archive-pass lease serializes mutating operators; a competing operator receives a refusal naming the lock path and `lsof -- '<lock-path>'` to identify its current holder. That command was verified against a held lease on macOS. The tool does not infer liveness from a process table or an owner sidecar. Reports do not take this lock.

**Until append-path extraction ships, sending to a folded session is deliberately refused.** Reads serve the sealed archive in place. Do not remove the refusal or create a new live WAL to bypass it: a freshly minted lineage would shadow the archived transcript.

## Journal and recovery

Each pass writes `wal-archive/fold-<unix-ms>.journal` and its sibling `.ark`. The journal is not `wal-manifest`, which remains the live-file diagnostic sidecar. JSON lines are fsynced after each transition:

1. `intended`: identity and pre-fold SHA-256 recorded before appending a member.
2. `archived`: member bytes fsynced, live file untouched.
3. Seal the container and sync its directory before any unlink.
4. Under the session lease, verify the member and compare live bytes with the recorded digest. Matching live bytes are unlinked and recorded `done`; changed bytes stay live and are recorded `superseded`. Already absent live files are recorded `done` after verifying the member. Contention leaves `archived` for resume.
5. `complete` terminates a pass whose entries are all done or superseded.

`removed` counts actual unlinks in this invocation. A fresh completed pass's count equals its `done` entries. On resume, already-done entries are skipped, and an unlink that preceded a crash may be journaled without incrementing this invocation's removed count.

Resume using the **basename**, not an arbitrary path:

```
store-maintenance wal-fold <state-root> --resume fold-123.journal
store-maintenance wal-fold <state-root> --resume fold-123.journal --write
```

A sealed pass resumes phase 2 only. An unsealed pass has not legitimately unlinked sources: resume verifies live presence, marks the old journal `restarted`, removes the unsealed container, and evaluates a new pass with a fresh stamp. Reusing an old stamp for new snapshots could let stale newer-stamped members win. A partial final JSON line is trimmed before the next append; complete lines are preserved. An unfinished journal blocks a new pass and names the resume command.

A missing container refuses resume and names missing live sessions as **LOST DATA**. After investigation, `--resume fold-123.journal --abandon --write` explicitly appends a terminal abandonment record with each session's actual live-file presence. It does not recover data or delete the journal. This acknowledgment permits future passes while preserving evidence of missing bytes. Report-only abandonment shows the same presence census without writing it.

## Operator reversal

`--resume fold-123.journal --unfold` previews restoring its members; add `--write` to restore. Unsealed containers yield zero candidates; missing containers refuse. Live files with different bytes and newer archive members with different bytes refuse rather than being overwritten. Matching existing live files are unchanged. Restores take the pass lock and each session lease, write an owner-only temporary file in `wal/`, fsync it, rename it, then sync `wal/`. Containers and journal evidence remain intact. This explicit operator reversal is not automatic append-path extraction.

## Verification

`wal-addressability.py` resolves store-named archived sessions, checking the member digest. Its filesystem census still counts live files only. `wal-verify baseline|check` counts logical WAL identities and bytes across live files and newest sealed archive members, so a pre-fold baseline remains valid over a partially or completely folded tree. `sweep-empty-wals.py` still targets only empty `.wal` files inside `wal/`; sibling containers are not sweep targets. Control-head repair uses the archive-aware `WalStore` and does not classify a folded session as having no WAL.
