# Private state creation

`broca-owner-only` is a transport-free, unpublished leaf crate. All broca-owned private
writers use its directory creators and `OpenOptions`; only its Windows module contains
unsafe code. The only dependency is Windows-only `windows-sys`.

## Policy

- Unix creation retains 0700 directories and 0600 files, subject to the same umask as
  before. Existing objects and shared ancestors are not chmod'd by creation or open.
- Windows creation supplies a protected DACL to `CreateDirectoryW`/`CreateFileW`, before
  any bytes can exist. Its two full-control entries grant the account SID (`TokenUser`)
  and SYSTEM. The owner field is explicitly the account SID for helper-created objects.
- Directory entries inherit to files and directories. SQLite creates its own sidecars;
  a pre-open check requires a private, inheritable parent and checks an existing database,
  `-wal`, `-shm` and `-journal` before any SQLite call. A new database is precreated using
  the helper. Recreated sidecars inherit only account+SYSTEM access, not broad ACLs.
- SQLite sidecars keep Windows' token-default owner field. Normally this is the account;
  under an elevated token it can be Administrators. The DACL still grants only account
  and SYSTEM. Administrators may rewrite the DACL through ownership (and can otherwise
  take ownership); broca is intended to run as a non-elevated user daemon. Validation
  checks the access grants, not equality between the owner field and account SID.
- SQLite's `temp_store` setting is unchanged. Temporary databases outside the state root
  use SQLite's normal per-user Windows TEMP directory. This crate does not claim to
  harden that directory or SQLite's external temporary files.
- Existing broad Windows trees are not silently narrowed at open. WAL and SQLite opens
  now refuse insecure or unreadable ACLs; SQLite also refuses broader parent grants or
  missing inheritance. `store-maintenance private-mode` is the explicit repair path.
- Windows repair preserves every object's owner field, replaces only its DACL with
  account+SYSTEM access, and never follows reparse points. Kernel-object DACL updates do
  not propagate automatically; the walker inspects each descendant itself. A skipped
  junction or an unreadable/unrepairable object makes the repair incomplete and nonzero.
- Windows test scratch parents are created private. An older broad scratch parent is
  refused with its path, never adopted or repaired by fixture creation.

## Commons paths

The pinned commons dependencies are unchanged. Daemon logging enters
`cortexkit_log::init(Config::for_module(...))` in `broca-module-serve/src/main.rs`;
`cortexkit-log 0.3.5` prepares its Windows log directory and calls the pinned lease
crate's protection path for segments. The daemon constructs `FileLeaseStore` in the
same startup function. `broca-core/src/lease.rs` reexports that store from
`cortexkit-lease 0.1.2`, and `broca-module/src/opener.rs` acquires it before WAL recovery
and transfers its handle to the actor. Those dependencies own their file ACLs; this
crate does not fork or reimplement them.

## Verification boundary

Windows tests use a deliberately broad parent, an ordinary inherited control file,
and PowerShell ACL readback independent of the Rust validator. Temporary-file hooks
inspect files while open, before publication or cleanup. WAL and SQLite refusal tests
also observe entry into IO/initialisation, rather than relying only on unchanged bytes.
