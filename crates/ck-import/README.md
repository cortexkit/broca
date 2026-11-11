# ck-import

`ck-import` converts a foreign harness transcript to canonical broca messages and calls
`session.import` through the authenticated subc connection file.

```console
ck-import opencode --list
ck-import opencode <opencode-session-id> \
  --subc "$XDG_RUNTIME_DIR/subc-connection.json"
```

OpenCode defaults to `~/.local/share/opencode/opencode.db`; use `--source-db` to select another
current-format database. The legacy `storage/message` directory format is not supported yet.

Imports carry transcripts only, not Magic Context compartments. The imported broca session
summarizes its own history at its first compaction Setup, producing summaries with coordinates
from the imported transcript.

For a standalone OpenCode 1 head move, first make a single-session source (no daemon connection
or default database path is used by this command):

```console
ck-import opencode1-extract --db /path/to/opencode.db --session ses_example --out-dir /path/to/import-sources
```

It prints one JSON object `{path, sha256, messages, newest_mid}`. The owner-only SQLite file
contains only that session's `session`, `message` and `part` rows with their original table
schemas and indexes, including committed WAL data in one read-only snapshot. A changed or torn
snapshot is reported as retryable. Pass this file to scoped `session.import` as harness
`opencode1`; unlike the legacy transcript command above, the head move preserves ids, times,
media and the plugin move list. Broca deletes the source only after the import is durable.
The owner's first `session.send` must carry its plan; `session.install_view` may precede it.

The context warning uses a rough character-count / 4 estimate and compares it with
`--context-limit` (default 200,000). Imports are rejected client-side above 100,000 messages or
48 MiB of canonical JSON so a subc frame-limit error is never the first failure. Transcript imports
are idempotent within the same target session; retry with the same `--session <target-session-id>`
printed by the import. A retry without `--session` creates a new lineage and imports the transcript
again, so choose a session id up front when you may need to retry.

## Live smoke test

With a development `ck-broca` registered as the `broca` management module, import one session
listed from a source database and read it back:

```console
ck-import opencode --list
ck-import opencode <source-id> --subc "$XDG_RUNTIME_DIR/subc-connection.json" \
  --session import-smoke --project-root "$PWD"
broca-session --subc "$XDG_RUNTIME_DIR/subc-connection.json" \
  --project-root "$PWD" --harness ck-app --session import-smoke subscribe \
  --from start --until-runs 1
```

The first command is read-only. The import prints the target session and run id; the final command
must show the imported transcript ending in `RUN_FINISHED` while the lineage is idle and ready for
a later `session.send`.
