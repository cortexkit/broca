# The session store and resume

broca keeps every session in the session store (`crates/broca-session-store`). This page
describes how a session is laid out on disk, how its log is written and synced, how history
behind a compaction view is sealed, and how a run resumes after a crash or a restart. The run
loop that writes the records is `crates/broca-session-loop`.

## Roots and addresses

Each storage class has its own root in the state root:

| Root | Holds |
|---|---|
| `session-store/` | Ordinary sessions. |
| `session-store-durable/` | Sessions whose creating send carried a class in `backup_classes`, heads imported from another runner, and sessions an operator moved there. This root is backed up. |
| `session-store-ephemeral/` | Sessions with a retention policy. They are deleted when it expires; `expired/` holds the markers of sessions already deleted. |

A session's class is chosen at its first send and never changes. Inside a root, everything that
belongs to one session is named by its **address**: FNV-1a 64 over the session's
`(project_root, harness, session)` triple joined by U+001F, as 16 hex digits. Changing any part
of the triple addresses a different session.

| Path in a root | What it is |
|---|---|
| `heads/<address>/head` | The session's current head. |
| `heads/<address>/versions/` | Earlier head versions. The newest two are kept, subject to age; the head of a session that is moving or moved is never pruned. |
| `logs/<address>/<change-id>` | The session's append-only logs. |
| `objects/` | Immutable objects: sealed segments, directory pages and media, named under the change that wrote them. |
| `setup/`, `setup-plain/` | Setup parts (tool definitions, system text), named by their content and shared by every session that uses them. `setup-plain/` holds the parts of plain sessions. |
| `recovery-candidates/` | The sessions that may need recovery when broca starts (see Resume). |

## The head

A head holds pointers: the open log, the roots of the transcript and ledger directories, the
root of the id index, and the logs and objects a finished change still has to delete. It is
written as a new version with a compare-and-swap: a write names the version it expects and the
next one, and fails as fenced when the stored version is no longer the expected one. Versions only
grow, so the version is also the fencing token. Only the process holding the session's lease
writes, and the compare-and-swap catches a writer that lost its lease without knowing.

## Records

A log holds two streams of records. The **transcript** is the conversation: inputs, assistant
messages, tool replies. The **ledger** is everything that runs it: run starts and ends, steps,
tool intents and outcomes, pauses, compaction views. Transcript record kinds are numbered 1 to 63
and ledger kinds from 64, in the `records!` list in `src/record.rs`.

New kinds and fields follow two rules:

- A new kind takes the next number. A build that meets a kind it doesn't know refuses the log
  rather than skip it.
- A new field takes a field number. A field an older reader could safely skip is ordinary; one
  whose absence would make a reader misread the record carries the must-understand mark, and an
  older reader refuses the record.

Either kind of change ships in a reader release first, and a writer only in a later release, so a
rollback to the previous release can still read every log.

## Encryption

A session's cipher mode is chosen when it is created and recorded in its head.

- An **encrypted** session has a random 256-bit data key, minted with the session. Every frame,
  block and object of the session is sealed with XChaCha20-Poly1305 under that key, each with a
  fresh random 24-byte nonce stored before the ciphertext and its 16-byte tag. The data key is
  stored only in the head, wrapped under the deployment key and bound to the session id and key
  id, so a wrapped key copied into another head fails to unwrap. The head itself is readable and
  authenticated with the data key.
- **Deployment keys** come from the vault (`opaque:broca-deployment:<key-id>`) and stay in
  memory. `session_storage.deployment_key_id` pins the key for new encrypted data. If omitted,
  broca uses the lowest-numbered readable key in its scoped vault listing. A listing refresh can
  switch new writes to a lower key without a restart. On a single computer with key 1 as the
  lowest readable key, nothing changes. Existing session heads and expiry markers name the key
  that locked them. Setup references carry only a content digest. To find an encrypted setup part,
  broca derives its storage name from the digest under each loaded key, starting with the key used
  for new writes. A missing encrypted part is retryable while a granted key is still loading; once
  all granted keys are loaded, a missing part is damage.
- broca starts without waiting for keys and discovers new grants when its scoped listing
  refreshes. When no key is available for new encrypted sessions, or an existing encrypted
  session's key hasn't loaded, broca refuses with `key_unavailable`, which a caller may retry.
  There is no fallback to plaintext. Pairing computers does not re-lock existing data: keep the
  old key records and grant broca read access to each key it needs.
- A **plain** session has no data key: each unit is its plaintext followed by an unkeyed check,
  which catches a torn or damaged unit but not a deliberate edit. New sessions are plain only when
  `session_storage.encryption` is `off` (the default is `on`), and a plain session opens without
  the deployment key.

## Writing a run

A run's records are appended to the open log in batches, and a batch marked as a barrier is synced
to disk before the run goes on. The barriers are exactly the points where broca is about to act
outside the process:

1. The run's start (`RunStart` and what admission froze) is synced before the first model call.
2. A tool batch's start and its `ToolIntent` records are synced before any call is dispatched.
3. Each dispatched call's result is synced as soon as it arrives, while the batch's other calls
   may still run: a reader sees it at once (a quick `say` beside a long `bash`), and a crash can't
   lose a result a reader already saw, which would leave that call's outcome unknown.
4. The batch's end, after every call's result or outcome, is synced before the next model request.
5. A step that calls no tool is synced at its end.

So a step whose model calls N tools costs N + 2 syncs: one before the dispatch, one for each
call's result, and one for the batch's end; a step with one tool call costs three. The
`storage_sync.full_flush` setting changes how a sync is made, not when: off (the default) issues a
plain `fsync`, and on macOS on selects a full drive-cache flush (`F_FULLFSYNC`). Other platforms
behave the same either way.

## Sealed history

When a compaction view cuts the conversation, the records before the cut leave the open log and
are sealed into a segment, an immutable object made of a readable header, individually sealed
blocks, a sealed index and a footer. The new log starts from a checkpoint of the conversation at
the cut. A cut must fall where an input or an assistant message starts, with no tool turn open.

Blocks aim at 16 KiB of encoded records and never split a record. Each input or assistant message
starts a group, and the tool replies and facts that follow it stay in its group, so a tool call and
its results are sealed together; a group larger than the target gets a block of its own. When a
call and its result still fall in different blocks (an imported conversation can do this), a read
joins them again.

History behind the view is read back on demand, block by block: the transcript directory finds the
segment by ordinal, its header and index are read (and cached), and only the block holding the
wanted entry is fetched, authenticated, decompressed and folded.

## Resume

A run cut short by a crash, or stopped by a shutdown, is resumed from its records, never restarted
from its input.

- The next request is rebuilt from the conversation the records replay to and the values admission
  froze into the run's start: model, tools, setup parts, cache policy, stop conditions. Before a
  resumed run calls the provider again, broca checks that the provider still renders with the
  recorded renderer version and render configuration; if either drifted, the run ends rather than
  sending a request the original run would not have sent.
- A tool call whose intent is durable but whose outcome is not may already have had its effect.
  It is never dispatched again: the batch is closed with an `outcome_unknown` result for it (a
  call that was planned but never sent gets a not-run result instead), and the run ends
  Interrupted, naming the call in `indeterminate_tool_calls`.
- `recovery-candidates/` lists the sessions that may need this at start. A session is added
  before a run can write its start, and removed only after its terminal and its billing are
  durable, so a missing entry never hides a session that needs recovery.

## Tests that hold these rules

The session loop's cut tests (`crates/broca-session-loop/tests/differential.rs`) save the log at
every durable append, resume each prefix, and check that an intent in the prefix is never
dispatched again, that an unknown outcome is never retried, and that every resumed request equals
the uninterrupted run's. Named cases cover tool runs, photos, parallel calls, unknown outcomes,
unsent calls, step limits, pauses and compaction.
