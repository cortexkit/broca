# Unsynced v4 append-tail recovery fixture

Synthetic billing records captured from the real HTTP gzip-refusal/resend path.

- Durability interval: `refusal_to_resend`.
- H1 image: index `678`, filesystem cut `21`, persistence family `PartialPersistence`.
- Existing `FileWal::open` refuses with `Corrupt { offset: 1456, source: UnsupportedVersion { ver: 0 } }`.
- The intact prefix contains 1 reservation(s) and 0 settlement(s).

The bytes contain synthetic identities only. No real credentials or message content is present.

Reproduce the generating acceptance test on Linux:

```sh
cargo test --locked -p broca-provider -F live -F test-harness attempt_hook_power_loss --lib -- --ignored --nocapture
```

The recovery fix must distinguish this unsynced append-tail loss from damage in the synced prefix. Do not skip refused images or relax the H1 harness. After physical recovery, the test uses the real reclaim writer opener/close and checks each reservation has exactly one settlement or a superseding writer, never both counted as separate attempts.
