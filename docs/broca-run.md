# broca-run

`broca-run` drives the loop in-process as an unsupervised **Direct** consumer of
subc. `--project-root <dir>` stores local leases and the conversation's write-ahead
log (WAL); on protocol 0.34 it
does not put a folder on a tool bind. Without `--scope`, routing stays unscoped,
and a file-tool provider such as AFT can refuse folder-dependent tools.

## Carry an owner's scope

Use `--scope <owner>/<ref>/<epoch>` to carry a scope registered by a supervised,
reserved owner module. The owner is a module id, not `direct` ownership: a Direct
process cannot register or own the scope. The explicit epoch prevents a reused
scope ref from silently selecting a different session.

After the owner has synced and received acceptance for its record, it hands the
child a **32-byte random bind proof privately**, encoded as exactly 64 hex
characters on stdin, optionally followed by a newline, and closes that pipe.
Stdin is not otherwise used by broca-run. Never put the proof in argv, an
environment variable, a log, or a shell command printed by an orchestrator.
Missing or malformed input is refused as `scope_bind_proof_required` or
`scope_bind_proof_invalid` before any daemon connection is attempted.

```text
broca-run --subc <connection-file> --model <model> --prompt <prompt> \
  --project-root <local-state-folder> --scope rig-owner/run-42/1
```

The command above deliberately contains no proof argument. The supervising
owner should spawn it with a private piped stdin, write the hex proof to that
pipe, then close the write end. The selector sent to subc uses base64url without
padding over the decoded 32 bytes. The proof hash is SHA-256 over those raw bytes,
**not** the 64-character hex representation.

For example, the reserved module `rig-owner` syncs this scope record, replacing
the folder with its canonical absolute rig folder and the hash with its proof's
lowercase 64-character SHA-256 digest:

```json
{
  "ref": "run-42",
  "scope_epoch": 1,
  "kind": "head",
  "workdir": "/absolute/rig-folder",
  "bind_proof_sha256": "<sha256-of-the-raw-32-byte-proof>",
  "carriers": [
    {
      "principal": {"kind": "direct"},
      "targets": ["aft"]
    }
  ],
  "attributes": {}
}
```

The owner is established by its supervised module connection, not by adding an
owner field to the record. Discover `scopes/v1` and `scope-bind-proof/v1` before
using proof-bound scopes. Broca-run refuses this mode if the daemon cannot enforce
both capabilities, rather than opening an unscoped fallback. A wrong proof is
refused by subc as `scope_bind_proof_mismatch`.

Every warm, dispatch and reopened **model tool-plane route** carries that selector
and proof. The daemon stamps the record's workdir on the provider's route; a
different `--project-root` does not override it. Keep `targets` narrow: if the
session needs another tool provider, its owner must explicitly authorize that
module too. There is no fallback to a folder claimed by the CLI.

The process-level **Fusiform catalog and vault routes stay unscoped and need no
carrier entry**. The proof lives only in the tool plane, not in the run's session
identity, model input, or WAL. With `--scope` absent, broca-run does not read stdin
and preserves its existing behavior.
