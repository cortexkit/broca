# Requester consent envelopes and responses, v1

JSON request envelopes are the exact bytes submitted to the real handler; reply
files are its untouched JSON bytes, including `result` and no appended newline.
Fresh scratch stores start at `el_1` and clock 10,000 ms. Answers, withdrawals and
dedup changes happen at 11,000 ms; grant-lost expiry happens at 70,000 ms.

The three `*-body.json` files are byte-for-byte copies of prefrontal's
`test-vectors/flow-decision-card-v2/*-request.json` inputs. They are emitted from
the same copied input bytes parsed into the handler envelopes, not invented or
normalized examples. Prefrontal's design document calls the flow-decision card
format "v2.2". The only published example bodies for that format are in
`test-vectors/flow-decision-card-v2`; the three request bodies in these vectors
are copied from that corpus. These vectors prove acceptance of those examples,
not semantic parity with the format called "v2.2". Basal's route is
`reserved:basal` with a fixed bound session; no phone harness is needed.

Every row below expands to the literal files named by its prefix and suffix.

| Files | What they show |
|---|---|
| `reconcile-body.json` | Four-choice reconcile requester body, unchanged from prefrontal's `test-vectors/flow-decision-card-v2/reconcile-request.json`. |
| `reenable-body.json` | Unchanged two-choice re-enable requester body. |
| `grant_lost-body.json` | Unchanged informational lost-grant body, opaque grant and two choices. |
| `reconcile-request.json`, `reconcile-reply.json` | Basal request and resulting new id. |
| `reenable-request.json`, `reenable-reply.json` | Basal request and resulting new id. |
| `grant_lost-request.json`, `grant_lost-reply.json` | Basal request and resulting new id. |
| `reconcile-await-pending-request.json`, `reconcile-await-pending-reply.json` | Zero-timeout wait returns full pending reconcile record. |
| `reenable-await-pending-request.json`, `reenable-await-pending-reply.json` | Zero-timeout wait returns full pending re-enable record. |
| `grant_lost-await-pending-request.json`, `grant_lost-await-pending-reply.json` | Zero-timeout wait returns full pending lost-grant record. |
| `reconcile-answers-request.json`, `reconcile-answers-reply.json` | Owner feed after explicit `send_again`. |
| `reenable-answers-request.json`, `reenable-answers-reply.json` | Owner feed after explicit `reenable`. |
| `grant_lost-answers-request.json`, `grant_lost-answers-reply.json` | Owner feed after explicit `check_now`; no execution or grant creation. |
| `dedup-before-phone-list.json` | Phone list before refresh/replacement; binding is private. |
| `dedup-refresh-request.json`, `dedup-refresh-reply.json` | Identical request refreshes expiry and preserves id. |
| `dedup-refresh-phone-list.json` | Same phone id, new expiry and revision. |
| `dedup-replace-request.json`, `dedup-replace-reply.json` | Changed grant label replaces the pending card with `el_2`. |
| `dedup-old-get-request.json`, `dedup-old-get-reply.json` | Old card now withdrawn; requester binding retained. |
| `dedup-replace-phone-list.json` | Phone now lists the replacement instead of old id. |
| `withdraw-request.json`, `withdraw-reply.json` | Owner withdraws replacement; full withdrawn record returned. |
| `answers-after-withdraw-request.json`, `answers-after-withdraw-reply.json` | Withdrawals consume cursors but are absent from the owner's answers records. |
| `ack-withdrawals-request.json`, `ack-withdrawals-reply.json` | Cumulative ack through those withdrawal positions. |
| `grant-lost-expired-answers-request.json`, `grant-lost-expired-answers-reply.json` | Expiry has no selected choice, not an implicit `keep_disabled`. |
| `ack-expired-request.json`, `ack-expired-reply.json` | Acknowledge processed expiry. |
| `answers-after-ack-request.json`, `answers-after-ack-reply.json` | Empty owner feed retains its global high-water cursor. |
| `report-execution-request.json`, `report-execution-reply.json` | Package requester reports `executed`; flow decisions cannot use this operation. |
| `SHA256SUMS` | Hashes of every JSON file and this README. |
| `README.md` | This index and reproduction instructions. |

From the workspace root:

```sh
cargo run -p cingulate-vectors --example generate_requester_vectors --locked -- --check
cargo test -p cingulate-vectors --test requester_contract --locked
```

With `--check`, the generator regenerates vector bytes and `SHA256SUMS` in memory
and fails on any byte difference or file-coverage mismatch, without writing
files. Running without `--check` rewrites this corpus's files, including
`SHA256SUMS`; it does not write any other corpus. Do that only for an intended
change to what the module returns. After such a change, update `SHA256SUMS`, its
pinned manifest hash in the integrity test, and the corpus version tag. Tests
compare all 44 JSON files byte-for-byte, check directory/hash coverage, and pin
the manifest hash and the three unchanged upstream input hashes. See the
[requester contract](../../docs/contracts/consent-requester-v1.md).
