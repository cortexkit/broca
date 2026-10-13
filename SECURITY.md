# Security

## Reporting a vulnerability

Report vulnerabilities privately through GitHub's private vulnerability
reporting for this repository. Open the repository's **Security** tab, choose
**Report a vulnerability**, and submit a private report:

https://github.com/cortexkit/broca/security/advisories/new

Include the affected version or commit, reproduction steps, expected impact and
relevant platform details. Remove credentials and private prompt content from
logs and examples. Do not open a public issue for an unpatched vulnerability.

## Threat model

broca handles prompts and provider credentials in memory while running agent
loops. Prompts, tool results and transcripts also become durable session data in
the write-ahead log and its materialized stores. Protect the state directory and
treat exported transcripts as sensitive.

broca trusts the local subconscious daemon's attested identities and route
authorization. The supervised module reads vault credentials by ID using grants
bound to its supervised principal (`reserved:broca`). Knowing a credential ID or
copying a credential-ID alias from configuration does not grant access. A client
without the required attested identity and matching grant cannot obtain that
scoped credential just by supplying the ID. A refused scoped read is not retried
through a handle-based authorization path.

Developer tools use a separate handle-based path. The handles are bearer secrets:
a process that obtains one can use the authority it carries. On Unix, the handle
file must be a regular, non-symlink file with no group or other-user permissions
(normally mode `0600`). This protects against other OS users; it does not keep a
process running as the file's owner from reading the handles. Keep these files
out of repositories and shared directories.

The supervised-principal grant boundary is not a sandbox against a compromised
local user account. A same-user process does not acquire that principal merely
by claiming its name, but it may read owner-readable files, alter owner-writable
configuration or executables, and inspect process memory where the OS permits
it. A leaked developer handle or control of the supervised process can expose
credentials. Protect the daemon, vault, module binaries and user account as part
of the same trust boundary.

The implementation of these boundaries is in
[the vault client](crates/broca-subc/src/vault.rs) and
[the handle-file configuration](crates/broca-module-serve/src/vault_config.rs).
See the [architecture](docs/architecture.md) for the session and storage model.
