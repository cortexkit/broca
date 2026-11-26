# Publishing broca

A deploy tag is not a public release. The internal deploy lane (`scripts/deploy.sh`,
`scripts/train-push.sh`, and `scripts/tag-when-green.sh`) stages/places local binaries,
lands changes, and tags green deploys. It does not publish installer assets. The
public lane releases only a chosen version, from its existing deploy tag, and does
not place or restart broca, bump versions, land commits, or create or move tags.
Pushing a tag does not start the `Release` workflow.

## Prepare and release a version

1. Commit nonempty `.cortexkit/release-notes/vX.Y.Z.md` at the version to be
   released. Its body is used as-is on GitHub; include this standalone line:

   ```text
   Windows: release incomplete (native suite has not run).
   ```

2. Set `release-manifest.json`'s `requires_core` to the first compatible public
   core version (`X.Y.Z`, no `v`). The checked-in placeholder is not a version and
   both the script and the workflow refuse it. Land the notes and manifest **before**
   the chosen version is tagged by the internal lane; never retag a prior deploy.
3. On native macOS or Linux with Git, an authenticated upstream `gh` that can read
   drafts and dispatch Actions, Python 3.11+, curl, Rust and cargo-nextest, check out
   clean `master` at the same commit as `origin/master`. Both the workspace version
   and the existing remote `vX.Y.Z` tag must identify that commit, and push CI must
   have passed. Run `scripts/release.sh X.Y.Z --dry-run`, then
   `scripts/release.sh X.Y.Z` to publish. The version must be newer than any published
   version, with no existing GitHub release (even a draft) for that tag.

The preflight downloads the host `ck-subc` ZIP and its SHA-256 sidecar from the immutable
`cortexkit/subconscious` `subc-core-v<requires_core>` release. It verifies the ZIP
before extracting `ck-subc`, checks each binary separately in Cargo's normal
dependency graph, and runs the real-daemon launch, shutdown seal and respawn tests
with `SUBC_CORE_BIN` set to that exact copy and `BROCA_REQUIRE_SUBC=1`.
The dry run performs these checks but does not dispatch Actions or write a release.
Use the script rather than dispatching in the UI, which would skip those real-daemon tests.

The script dispatches `Release` at the existing tag, polls for GitHub's completed
result, and then confirms `components.broca.version` and
`components.broca.requires_core` in <https://cortexkit.io/releases/v1/index.json>.
An index timeout after a successful workflow means the release may already be public.
Do not move its tag or cut another release to work around an index delay.

## What is published

For each `darwin`, `linux`, and `windows` target, on both `arm64` and `x64`, the
release has `ck-broca-<os>-<arch>.zip` and `broca-session-<os>-<arch>.zip`. Each
archive contains its executable at the root (`.exe` on Windows) and has a sibling
`.zip.sha256` in `shasum -a 256` format. The release also has the exact tagged
`release-manifest.json` with `requires_core` and `windows` status. Windows ZIPs are
built, version-smoke-tested and checked like the others; they do **not** attest to
passing the native suite. Readers can check `windows.status == "release incomplete"`
and `windows.native_suite_passed == false`. Only change that status once native
Windows release evidence is available and the release gates have been updated.

The workflow first verifies the commit, tag, notes and manifest and creates a
GitHub draft. After all six builds succeed, it uploads the complete asset set to
that draft, downloads each asset from GitHub Releases by ID, and compares the
bytes and ZIP checksums against the builds. The publish job rechecks the draft's
asset IDs and tag before making it public. Failed builds or asset checks leave
only a draft. Inspect the failed job and the draft before retrying; the script
will not overwrite a draft or republish a released version.

## macOS signing

Configure these repository Actions secrets (no local signing credentials are used):

- `DEVELOPER_ID_APPLICATION_CERTIFICATE_BASE64`
- `DEVELOPER_ID_APPLICATION_CERTIFICATE_PASSWORD`
- `APP_STORE_CONNECT_API_KEY`
- `APP_STORE_CONNECT_API_KEY_ID`
- `APP_STORE_CONNECT_API_ISSUER_ID`

The workflow uses a throwaway keychain under `RUNNER_TEMP` with a Developer ID
Application certificate. Each binary is signed, strictly verified with hardened
runtime, and run before it goes into its ZIP. Apple notarization must report
`Accepted` before the checksum is written. A ZIP containing a standalone executable
cannot be stapled; Gatekeeper verifies its notarization ticket online on first run.
There is no ad-hoc or unsigned fallback for macOS. Linux and Windows assets are
checksum-verified, not platform-signed by this lane.
