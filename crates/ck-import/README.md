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
When `~/.local/share/cortexkit/magic-context/context.db` contains compartments for the source
OpenCode session, `ck-import` sends those compartments to the `magic-context` management module
after `session.import` commits. Use `--skip-mc-state` for a lineage-only import or
`--mc-state-db` to select a different read-only compartment database. Missing or unreadable compartment
state emits a warning and does not block the lineage import.

The context warning uses a rough character-count / 4 estimate and compares it with
`--context-limit` (default 200,000). Imports are rejected client-side above 100,000 messages or
48 MiB of canonical JSON so a subc frame-limit error is never the first failure. Both import legs
are idempotent; if compartment seeding fails after lineage import, re-run the same command.

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
