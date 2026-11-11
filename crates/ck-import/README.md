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

Check an extract before sending it to broca:

```console
ck-import opencode1-check --extract /path/to/extract.db --session ses_example
ck-import opencode1-check --extract /path/to/extract.db --session ses_example \
  --moves-out /path/to/offline-moves.json --tag-floor 4000
```

The command uses the same OpenCode 1 projection and media byte/type validation as serve import.
It prints counts only, never source content, message ids, call ids or paths. Success (exit 0)
reports source rows and imported messages by role; tool calls; mutually exclusive completed,
error, interrupted-as-result and historical open-call-placeholder results; distinct media byte
objects and their occurrences; dropped systems; rows projecting nothing (including systems);
and finished summaries. A refusal (exit 1) prints only its code and the one-based source message
ordinal, or null when no source row is responsible. No partial projection counts are claimed on refusal.

`--moves-out` creates a new owner-only file through the same seeder `session.import` uses;
it must not already exist. `--tag-floor` is optional (default 0) and requires `--moves-out`.
Its wrapper is `{offline: true, unminted_prefix: "unminted:", moves, newest}`, not a real
import reply. `moves` stays wire-decodable: source ids, entry ordinals and summary flags are
exact; added rows keep their exact ordinals and `added: true`, with ids `unminted:1`,
`unminted:2`, and so on. Real added ids need the store's message-id prefix and record sequence.
`newest.ordinal` is exact. Its mid/digest are exact when the newest entry is a source row;
when it is added, both are marked with `unminted:` rather than fabricated. One stderr line
reports the unminted-id count, why those ids are unavailable, and the newest's exactness.
Neither output adds a `source_entry` member, even as null. No message content is written.

The live import reply's `newest` always has the exact `{ordinal, mid, digest}` of the last
imported compaction entry, for plugin and runner shapes. Retries read that original entry,
not the session's current head, so source deletion and later owner runs preserve the reply.
The plugin summary flag follows SQLite's `summary = true/1 AND finish = 'stop'` rule on every
source row, regardless of its role or error. Rows are complete and ordered by `time_created`,
then id, including rows that project nothing.

Old `pending`/`running` tool parts get the host's exact interrupted error result and retain
their input, or `{}` when absent. Such a part in the last source row refuses `import_open_call`;
rows are ordered by `time_created`, then id, even when a row projects nothing. The checker needs
no store: neither mode tests import-root/digest/idempotency checks, object writes, encryption
availability or durability. Without `--moves-out`, it also does not check whether imported part
numbers fit above the caller's highest already-shown number (`tag_floor`). A passing check
does not itself create or import a session.

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
