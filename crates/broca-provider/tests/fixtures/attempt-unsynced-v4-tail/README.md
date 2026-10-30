# Unsynced v4 append-tail recovery fixture

Synthetic billing records captured from the real HTTP gzip-refusal/resend path.

- Durability interval: `refusal_to_resend`.
- Power-loss model image: index `678`, filesystem cut `21`, persistence family `PartialPersistence`.
- Pre-repair `FileWal::open` refused with `Corrupt { offset: 1456, source: UnsupportedVersion { ver: 0 } }`.
- The intact prefix contains 1 reservation(s) and 0 settlement(s).

The bytes contain synthetic identities only. No real credentials or message content is present.

Reproduce the generating acceptance test on Linux:

```sh
cargo test --locked -p broca-provider -F portable-power-loss attempt_hook_power_loss_portable --lib -- --nocapture
```

The acceptance covers five first-cut durability intervals, divided into shards in
one shared libtest process. Each interval enumerates first-cut histories and
second-loss images with `RecordingFs::enumerate_cut` at every recorded filesystem
operation, as implemented in `crates/broca-provider/src/transport_power_loss_tests.rs`.
After operation-cut coverage is checked, identical complete first and second
filesystems are evaluated once; no distinct persistence outcome is skipped.
Each reservation must have exactly one settlement or a superseding writer, never
both counted as separate attempts. The default v4 recovery path preserves every
removed byte in a durable sidecar and records recovery metadata. Its `.head`
durability proof distinguishes damage in published bytes from an unpublished
append tail; either kind is repairable with the same byte-preservation requirement.
