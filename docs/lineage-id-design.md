# Durable lineage identity on `session.read`


## Why an ordering cursor is not identity

A transcript consumer can hold a lineage identity token: equal means continue
after the held ordinal, different means re-read. Sequence monotonicity alone is
insufficient: a recreated lineage can pass the held mark before a slow poller
reads it. A resettable counter cannot identify a durable object.

## What can recreate a lineage without any transcript event

Broca keys a WAL file by FNV-1a over `(project_root, harness, session)`. Rename the project
directory and the same session id hashes to a DIFFERENT file: the old transcript is stranded and
the new key reads as an empty lineage. Nothing records it, because from the new key's
perspective nothing happened.


## The design

A durable opaque `lineage_id`, minted ONCE when the WAL is created and persisted with it.
Equality permits after-ordinal continuation; mismatch or absence answers `cursor_expired`.
`head` stays what it is, the delta cursor.

### Placement

Top level of `SessionReadResponse`, beside `head`.

NOT on `SessionReadLineageState`. That struct's docstring states that every field on it
describes the LAST RUN rather than the page or the lineage, and exists specifically to stop
consumers reading run-scoped state as transcript-scoped. A lineage-scoped id underneath it would
contradict the comment written to prevent that mistake.

### Re-key preserves; creation mints

The id lives IN the file, so a re-key — which is a MOVE — carries it without anyone deciding to.
The re-keyed transcript is the same durable object: same bytes, same inode, reachable under a
corrected name. A consumer whose cursor survives that is correct rather than lucky.


**THE CONSTRAINT IS ON THE OPERATION, NOT A HOPE ABOUT IT: a re-key must be a move.** Implemented
as copy-then-delete, two files would briefly share an id, and a copy outliving its source would
be identity forgery. Any future re-key tooling has to satisfy this, and it is the requirement
most likely to be violated by someone writing a "safer" version that copies first.

Anything that CREATES a WAL mints a fresh id with no inheritance path, so a lineage appearing at
a key that once held a different one is a new object with a new id.

### No backfill on read

Existing lineages return ABSENT until their next WRITE (a new episode), which mints and
persists.

Minting on first touch is the obvious alternative and it is wrong: `session.read` is write-free
by design and lease-free BECAUSE of it, which is what lets a phone page a session while a run is
executing. Spending that property to save dormant sessions one full read is a bad trade.

Absence answering `cursor_expired` prices the interim honestly: an old session pays one full
read the first time and is native afterwards; a dormant one stays absent, which costs nothing
because nobody delta-reads a session nobody uses.

## What this does NOT solve, deliberately

The id detects RECREATION. It cannot detect content mutation, because broca's durable transcript
is append-only and cannot mutate — see the note at `Message::mid()`, where positional identity
rests on the same property.

So a consumer must read a mismatch as `cursor_expired` (re-read from the start), never as
`diverged` (content changed at a stable id). Divergence is a case this substrate cannot produce,
and building reconciliation for it would be machinery for an unreachable state.
