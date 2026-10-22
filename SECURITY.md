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

### Environment variables that move a boundary

Three variables in the module's environment change where credentials come from, where model
requests go, or where prompts are written. They are unset in a normal deployment. Anyone who
can set the module's environment (the subc module configuration, or the account running it)
can set them, so protect that configuration like the vault and the binaries.

| Variable | Default when unset | Effect when set |
|---|---|---|
| `BROCA_AUTH_SOURCE` | `vault`: credentials come from the credential vault through the grant boundary above | `opencode` reads provider tokens from the OpenCode auth file (`~/.local/share/opencode/auth.json`) instead. That file is not covered by vault grants: any process running as the same user can read it. Any other value that is not blank after trimming refuses startup. |
| `BROCA_PROVIDER_BASE_URL` | each run's endpoint comes from the model catalog | Every run is sent to this base URL instead, authenticated with no credential, and the vault is never opened. Meant for local mock providers; prompts and tool results go to whatever answers at that URL. |
| `BROCA_DUMP_REQUEST_DIR` | no request dumps | Every rendered model request of a non-ephemeral session is written to this directory **unredacted**: full prompts, tool arguments and tool results. Files and the directory are created owner-only, and old dumps are swept by size and age, but the contents are as sensitive as the WAL. broca logs a warning naming the directory when the first dump is written. |

The code that reads them is in
[the module's startup](crates/broca-module-serve/src/main.rs) and
[the provider transport](crates/broca-provider/src/transport.rs).

The implementation of these boundaries is in
[the vault client](crates/broca-subc/src/vault.rs) and
[the handle-file configuration](crates/broca-module-serve/src/vault_config.rs).
See the [architecture](docs/architecture.md) for the session and storage model.
