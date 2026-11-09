# thalamus-export-v1

Real head exports that Thalamus's runners wrote for `session.export {session_ref}`. Prefrontal
core calls that op to move a head into broca, which then imports the export with
`session.import`. The files are copied byte for byte from the Thalamus repository's
`crates/thalamus-harness-module/test-vectors/session-export-v1/`, on branch `cleanup-oc1`, at
commit `8b74c3a07a0e54d6a72a1c0cc5a5e6282d60db90`, the last commit that touches that folder.
`SHA256SUMS` holds Thalamus's own checksums for the files copied here. Their README is not
copied: this file replaces it.

The sessions were recorded from real hosts with a stand-in model: OpenCode 2.2.3 under module
`thalamus-harness` (`opencode2/`), and Pi 1.5.0 under module `thalamus-pi` (`pi/`). Before
publishing, Thalamus rewrote every machine path: a test's scratch folder reads `/scratch`, and
its repository's folder reads `/repo`. Each `reply.json` names its file under a data home of
`/data`, and its `sha256` is taken over the file as stored here.

| File | What it is |
|---|---|
| `opencode2/export.json` | OpenCode 2's own export of the session: `{info, messages}` |
| `opencode2/export.sends.json` | its side file |
| `opencode2/reply.json` | Thalamus's answer: `{path, sha256, messages, newest_mid, tag_floor}` |
| `pi/export.jsonl` | the Pi head's journal, up to its last complete line |
| `pi/export.sends.json` | its side file |
| `pi/reply.json` | Thalamus's answer: `{path, sha256, bytes, newest_mid, tag_floor}` |

Both sessions ran the same story. The owner's first turn calls `bash`, and a reminder rides
that call's result. The owner then steers, sends a second turn (`client_message_id` `cmid-2`),
and the module named in the session's plan as a sender (`magic-context` on OpenCode 2,
`notifier` on Pi) sends its `note` template while the head is idle, which starts a new turn;
that send is marked `notice`. One more owner
turn follows.

The side file is a JSON array with one row per send, oldest first. A send that wrote a message
is listed by `mid`. A send that rode a tool result is listed by the carrying call's `call_key`,
with the `text` it carried. Pi's call keys are `<entry id>/<toolCall id>`; OpenCode 2's are
`<message id>/<call id>`.
