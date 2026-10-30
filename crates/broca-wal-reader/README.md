# broca-wal-reader

`broca-wal-reader` is a read-only Rust library for Astrocyte to read billing facts
from broca's version 4 session logs on a `broca-state/2` root. It verifies
published-head proofs, projects provider attempts and cache warms, and validates
checkpoints before resuming. It does not compute prices, acquire writer leases,
repair logs, or write to broca's state.

The reader-safety promise is that no log is deleted until every registered reader's
watermark **T is strictly past that log's last durable `appended_ms`**, maintenance
writes included. Session retention decides when a log becomes due; the watermark
decides when it can actually go. These are deletion's activation requirements,
not deletion implemented by this library.

## Minimal read loop

This scanner uses the API in this source tree. In a Rust binary with
`broca-wal-reader = "0.1"` as a dependency **after publication**, run
`cargo run -- /path/to/session.wal [checkpoint-file]`. Before publication, use a
path dependency on `crates/broca-wal-reader` instead. The checkpoint file, if
supplied, is Astrocyte's saved `Checkpoint::to_bytes()` output, not a sequence
number. The proof is the WAL filename with `.head` appended (`session.wal.head`).

```rust,no_run
use std::{error::Error, fs, path::PathBuf};

use broca_wal_reader::{billing::{self, Item}, open_log};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args_os().skip(1);
    let path = PathBuf::from(args.next().ok_or("usage: reader WAL [CHECKPOINT]")?);
    let saved = args.next().map(fs::read).transpose()?;
    let wal = open_log(&path)?;
    let mut proof_path = path.into_os_string();
    proof_path.push(".head");
    let proof = fs::read(PathBuf::from(proof_path))?;
    let mut reader = billing::billed_from_checkpoint_bytes(
        &wal, &proof, saved.as_deref(),
    )?;

    for item in &mut reader {
        match item? {
            Item::Billed(row) if !row.copied_in => {
                println!("{} {:?}", row.attempt_id, row.kind);
            }
            Item::Billed(_) => {} // Copied history is not a new charge.
            Item::Unreadable(row) => {
                eprintln!("unreadable seq={} reason={:?}", row.position.seq, row.reason);
                // A billing integration must retain row.raw_frame and commit a gap.
            }
            Item::Restarted { reason } => {
                eprintln!("full replay: {reason:?}");
            }
        }
    }
    if let Some(checkpoint) = reader.checkpoint() {
        println!("checkpoint candidate: {} bytes (not persisted)", checkpoint.to_bytes().len());
    }
    Ok(())
}
```

The scanner only prints diagnostics: it **does not record billing, retain raw
evidence, persist a checkpoint, or advance a watermark**. Astrocyte must replace
the match arms with durable recording and commit billing updates, unreadable
raw evidence with coverage gaps, and the checkpoint together. A printed item or
checkpoint candidate is not a committed read. See the integration rules below.

**Release status:** 0.1.0 is being prepared, not yet published. ASTRO signoff,
archive support, the owner-operated registry upload, and ASTRO's staging checks
remain open gates in [CHANGELOG.md](CHANGELOG.md). Archives are not supported by
this source tree. Legacy roots and their billing database export are not inputs
to this crate.

## Astrocyte integration

### Identity and billing items

- `billing::billed` accepts a read-only `ReadAt` source, proof bytes, and an
  optional `Checkpoint`. `billed_from_checkpoint_bytes` also validates the saved
  checkpoint encoding. Use `open_log` for files: it acquires no locks and enables
  share-delete on Windows so the reader cannot prevent eligible log deletion.
- `Item::Billed` contains a `position` (lineage and sequence), `log_incarnation`,
  `attempt_id`, `kind`, `status`, `copied_in`, `reservation`, `envelope`,
  `correction_id`, and `appended_ms`. Reservation and envelope data are JSON
  values; this crate reports facts, not prices or billing-model decisions.
- Deduplicate globally on `attempt_id` alone, never on position or checkpoint.
  An attempt id is 128 random bits encoded as 32 lowercase hexadecimal characters,
  durably reserved before sending the request. It is unchanged on the finish and
  any settling record. Ignore `copied_in` items.
- `BilledKind` distinguishes `Finished`, `Warm`, `PossiblyBilled`, and `Correction`;
  `AttemptStatus` distinguishes `Completed`, `Failed`, `Cancelled`, and
  `PossiblyBilled`. A correction/settlement updates the same attempt, not a second
  charge. Record a possibly-billed attempt as a coverage gap; if a terminal result
  is already recorded, a later gap report must not overwrite it.
- `Item::Unreadable` carries `position`, `raw_frame`, and `reason`. Preserve the
  bytes with a committed coverage gap, never as zero spend. Raw frames can contain
  private conversation data: restrict evidence access and do not print the bytes
  to logs. `Item::Restarted` announces a full replay with a typed reason; previously
  recorded items can be emitted again.

### Published bounds and checkpoints

A missing, torn, or checksum-invalid proof is unavailable, not an empty log:
`ReadError::Unpublished` holds the pass. I/O errors, invalid lineage/head, and
header corruption also leave the pass incomplete. No bytes beyond the published
proof are billing input. A CRC or hash detects corruption, not malicious changes
or a filesystem that lies about durability; checkpoints are trusted local reader
state, not authenticated receipts.

Fully drain the iterator, check every result, then ask for `checkpoint()`. The
accessor returns no checkpoint while items remain, and a header error prevents
checkpoint advancement. Commit the checkpoint bytes only in the same transaction
as the corresponding billed items and raw unreadable evidence/coverage gaps. Do
not persist progress or advance T on an error, even if earlier items were readable.

A resume validates checkpoint version, feature table, lineage, boundary frame
length/hash/sequence, and tail contiguity before trusting saved state. A mismatch
emits `Restarted` and replays from the first record; it does not trust a bare byte
offset or sequence watermark. A restored prefix can also trigger a full replay
when it reopens a reservation. Checkpoints remain behind the oldest open
reservation, so normal resumes can replay items too. Deduplication must therefore
survive process restart, log rewrite, and restore. Keep checkpoints per log
incarnation and do not move them between roots.

This API summary intentionally stays separate from the retention contract: the
consumer API is still under ASTRO review. Additional timestamp, serialization,
and checkpoint-state APIs are not promised until that review closes.

### Feature compatibility

The billing view currently knows `bill-attempts/v1`, `chatgpt-codex-wire/v1`,
`frame-stamps/v1`, and `writer-id/v1`. An unknown `bill-*` requirement yields
`ReadError::NewerWriter { features }` atomically: no partial billing iterator or
checkpoint is returned. Unknown content-only requirements can be skipped by the
billing policy. If such a frame also has billing requirements, it becomes
`Unreadable` with `UnsupportedContent`, not silently missing billing evidence.
The low-level frame decoder's strict policy refuses any unknown required feature.
Use the billing API rather than a raw frame scan for billing ingestion. No reader
release may be assumed to understand features merely because the writer does.

A `usage_by_model` row with absent counters means "this model served part of the step, and its share is only in `usage_raw`"; normalized unattributed totals are not representable in `bill-attempts/v1`.

## Retention: the rule each record follows

A record follows the retention policy of the session incarnation that contains
it, not a separate expiry for each billing item; reading it does not grant
permanent storage in broca. The starter must supply `retention_policy` on the
first send or adopt on a `broca-state/2` root. That
policy freezes for the incarnation: a missing policy is `retention_required`
(with nothing written), and a conflicting later policy is `retention_conflict`.
The reader must durably record its billing facts or evidence before allowing the
source session to be deleted.

The retention clock and billing-safety clock are different:

- **Last activity** is the newest `ts_ms` among `run_started`, `queued`, `steered`,
  `send_delivered`, `run_paused`, and `run_finished`. Cache warms, attempt records,
  writer records, and maintenance do not extend this clock.
- **Last recorded write** is the last durable frame's `appended_ms`, of any kind.
  Writer records, cache warms, and maintenance do extend this clock. It governs
  the watermark gate, independently of when the session becomes due.

There is a **seven-day minimum hold**. Retention deadlines are:

| Policy / state | Due time, subject to the hold |
| --- | --- |
| Unreleased `keep: {days: N}` | Last activity + `max(N, 7)` days |
| Unreleased `keep: "forever"` | Never |
| `keep: "none"`, never released | `max(24 hours, 7 days)` after the last run ends |
| Any released session, including `forever` | Seven days after the later of release and last activity |

Becoming due is not permission to delete. There must also be no active or paused
run, no queued send, and the billing watermark gate below must pass. This library
does not schedule retention, implement `session.release`, or delete records.
These are the specified retention rules when deletion is activated, not a claim
that deletion is active on a staging root.

`copied_in` affects billing identity, not the containing log's retention policy.
Skipping a copied billing item is not permission to delete its bytes. The move
protocol separately keeps the source's moved-away copy until the source reader's
T passes its last recorded write, with no additional minimum hold on that copy.

## Billing watermark contract (design §9.4)

This is the safety contract between Astrocyte and broca, not a sweep or watermark
service implemented by this library. Archiving and deletion remain separately
gated work. A successful per-log scan is necessary, but insufficient, to advance
the root-wide watermark.

### Astrocyte's watermark T

1. Declare `log-read-watermark/v1`. `watermark.get {}` returns
   `{watermark_ms: u64 | null}`. Answer only broca as the daemon-attested caller;
   refuse other callers with `not_a_read_source`.
2. T uses broca's recorded write times, `appended_ms`, never file modification
   times. For a complete pass starting at P, the candidate T is **P − ε**, with
   **ε = 60 seconds**. Advance only after that pass's recording commits. Persist
   T across Astrocyte restarts and keep it monotonic.
3. T must stay **at or before the start of every open reservation** in the
   reader's view. A possibly-billed reservation counts as read only once it has
   been recorded as a gap. Do not substitute the checkpoint sequence, the last
   billed item's timestamp, or the published head's timestamp for this bound.
4. A record counts as read only when decoded and recorded, or preserved as raw
   evidence with a committed coverage gap. Refused/unopenable logs hold T: the
   pass is incomplete. An unreadable record that Astrocyte has set aside with
   evidence and a committed gap counts as read; silent skipping does not. I/O,
   ledger, or checkpoint-commit failures cannot make a pass complete.

### Broca's reader registry and deletion comparison

- Broca registers every configured `retention.log_readers` module at each start
  (production names `astrocyte`), plus every module ever discovered declaring the
  capability. Discovery uses the daemon's `catalog.list`, filtered by
  `capabilities.provides`, then routes `watermark.get` with broca attested as
  caller. Registration survives a stopped or absent module. Only an operator's
  `ck-broca readers remove <module>` removes a reader, and only after the module
  is absent from configuration.
- Every deletion pass queries **every registered reader**, with a **10-second
  timeout**. A stopped, missing, unreachable, slow, refusing, or `null` reader
  means **nothing is deleted in that pass**. With no registered reader at all,
  a development root follows policy and the minimum hold; that is not permission
  to forget a production reader.
- Broca keeps the largest T per reader in memory and ignores lower replies. It
  does not persist T; Astrocyte does. Rebuilding broca's database must not forget
  configured readers or allow deletion to proceed. Health degrades once any due
  deletion has waited **24 hours**.
- Under the session lease, with no writer open, a due log is eligible only if
  its last published frame's `appended_ms` is **strictly less than the smallest
  registered-reader T**. A watermark one millisecond before or equal to that
  last write keeps the log; one millisecond past clears only the watermark gate.
  Writer records, warms, and maintenance writes count here too.
- Complete frames beyond the published proof require broca to open and close a
  `deletion_check` writer and wait for a later pass, not delete from a stale
  proof. For a future archive, the comparison uses the later of the header's
  `last_appended_ms` and `restored_ms`. A cached last-write value may select
  candidates but is not deletion authority.

## License

MIT License

Copyright (c) 2026 Ufuk Altinok

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
