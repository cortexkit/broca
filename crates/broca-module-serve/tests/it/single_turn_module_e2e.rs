//! THE single-client / single-turn REAL-MODULE end-to-end proof (Phase 4 (a)+(b) merge).
//!
//! Drives the WHOLE module against a real spine — nothing mocked but the LLM:
//!
//! ```text
//!   broca-session (client binary)
//!        │  route.open ManagementSurface + session.subscribe + session.send  (real wire)
//!        ▼
//!   real subc-core daemon ── supervises ──► broca MODULE (broca-module-serve binary)
//!        │                                        │  the coordinator drives run_loop;
//!        │                                        │  dispatches a tool:
//!        │                                        ▼  SubcToolPlane (CONSUMER) ──► real subc
//!        └────────────────────────────────────────────────────────────────────► fake-aft-stub
//!                                                  the LLM provider (HTTP) ─────► aimock
//! ```
//!
//! This is the FIRST test that drives the real `broca-module-serve` binary (the dual-subc-role
//! module) end to end: a real client route.binds its management surface, sends a turn, and
//! the module runs the agentic loop — model → tool dispatch through real subc to the tool
//! provider → model → final text — streaming the control units back over the subscribe route.
//!
//! ANTI-FALSE-GREEN: the proof keys on BOTH the client's streamed wire events (RUN_STARTED →
//! TOOL_CALL → TOOL_RESULT → RUN_FINISHED) AND the on-disk durable state (the module's WAL
//! exists for the session). A hung loop that merely times out can't produce the terminal
//! event, and a run that didn't persist leaves no WAL — neither can read as a pass.

use std::cell::RefCell;
use std::collections::HashSet;

use super::session_client::{self, ClientRun, CLIENT_DEADLINE};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
#[cfg(unix)]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(unix)]
use tokio::net::TcpListener;
use tokio::process::Command;
#[cfg(unix)]
use tokio::task::JoinHandle;
use tokio::time::{timeout, Instant};

// ───────────────────────── binary discovery ─────────────────────────

/// The module + client binaries cargo built for this crate's integration tests (cargo sets
/// CARGO_BIN_EXE_<name> for every [[bin]] in the crate under test).
fn module_serve_bin() -> PathBuf {
    broca_test_support::ckdev_binary(
        env!("CARGO_BIN_EXE_ck-broca"),
        broca_test_support::scratch_root(),
    )
}
fn broca_session_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_broca-session"))
}

/// A sibling-checkout subc binary (subc-core / fake-aft-stub), env-overridable. Appends the
/// platform exe suffix so the default path resolves on Windows too.
fn subc_bin(name: &str, env_key: &str) -> PathBuf {
    let path = broca_test_support::subc_bin(name, env_key);
    if name.starts_with("ck-") && path.exists() {
        broca_test_support::ckdev_binary(path, broca_test_support::scratch_root())
    } else {
        path
    }
}

/// A sibling-checkout claustrum binary (the credentials vault), env-overridable.
///
/// `CARGO_BIN_EXE_*` resolves only for binaries in the crate under test, so the vault is found
/// the same way the subc binaries are. Release rather than debug: claustrum ships built there,
/// and a debug path that is usually absent would make this skip on every machine but one.
fn claustrum_bin() -> PathBuf {
    std::env::var("CK_CLAUSTRUM_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../../claustrum/target/release")
                .join(format!("ck-claustrum{}", std::env::consts::EXE_SUFFIX))
        })
}

/// The vault's offline CLI, used to seed a credential before the supervisor starts.
fn ck_auth_bin() -> PathBuf {
    std::env::var("CK_AUTH_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../../claustrum/target/release")
                .join(format!("ck-auth{}", std::env::consts::EXE_SUFFIX))
        })
}

/// CI requires the real-subc binaries (anti-masking: a silent skip would give zero coverage
/// while showing green). Locally, absent binaries are a clean skip.
/// Whether skipping is explicitly permitted for this run.
///
/// The one word that turns a loud refusal back into a silent skip. It exists because a
/// contributor without sibling checkouts still needs to run the rest of the suite.
fn skip_allowed() -> bool {
    std::env::var("BROCA_ALLOW_SKIP").is_ok_and(|v| !v.is_empty())
}

/// REQUIRED BY DEFAULT. The BROCA_REQUIRE_* variables and CI=true remain readable as explicit
/// arming and as the reason reported on refusal, but they no longer decide the question: an
/// invocation that sets nothing is required, not optional.
///
/// The default used to be the reverse and it cost a published wrong conclusion. Using this suite
/// as an instrument to test itself, I ran it unguarded and read a 15s silent skip as a slow pass,
/// twice, because the number repeated. The guards existed the whole time. Remembering to set them
/// is not a mechanism, and the person most likely to forget is exactly the one using the suite to
/// measure something rather than to test something.
fn subc_required() -> bool {
    !skip_allowed()
}

/// CI (which runs `npm ci`) requires aimock to boot — a silent skip on a boot failure would
/// hide a real regression while showing green. Locally without aimock it's a clean skip.
/// Which trigger made this run mandatory, for a panic that has to name its own cause.
///
/// Two conditions force these gates, and a message that names only one sends the reader to
/// check a variable that is not set. The gate exists to stop a silent skip reading as a pass;
/// a gate that misreports why it fired reintroduces the same confusion one layer in.
fn required_because() -> Option<&'static str> {
    // REQUIRED BY DEFAULT, opt out explicitly. The old default was the reverse, and it cost a
    // published wrong conclusion: I used this suite to test itself, ran it without arming
    // anything, and read a 15s silent skip as a slow pass -- twice, because the number repeated.
    // The guards existed the whole time. Remembering to set them is not a mechanism, and the
    // person most likely to forget is the one using the suite as an instrument rather than as a
    // test.
    //
    // So an unguarded invocation is now the LOUD one. Skipping still works and now costs a
    // deliberate word, which is the right price for a run that will report `ok` having executed
    // nothing.
    if std::env::var("BROCA_ALLOW_SKIP").is_ok_and(|v| !v.is_empty()) {
        return None;
    }
    if std::env::var_os("BROCA_REQUIRE_AIMOCK").is_some() {
        return Some("BROCA_REQUIRE_AIMOCK");
    }
    if std::env::var_os("BROCA_REQUIRE_SUBC").is_some() {
        return Some("BROCA_REQUIRE_SUBC");
    }
    if std::env::var("CI").map(|v| v == "true").unwrap_or(false) {
        return Some("CI=true");
    }
    Some("the default (set BROCA_ALLOW_SKIP=1 to permit skipping)")
}

/// Whether this run must actually exercise the credential vault.
///
/// Non-EMPTY, not merely present: a per-platform workflow expression writes an empty string on
/// the legs it excludes, and reading that as "required" arms the guard on exactly the platform
/// that opted out.
fn vault_required() -> bool {
    std::env::var("BROCA_REQUIRE_VAULT").is_ok_and(|v| !v.is_empty())
}

/// Unwrap a vault-backed rig, refusing to let a skip masquerade as a pass.
///
/// The vault binary check panics when the binaries are missing, but the constructor returns
/// `None` for every other setup failure too, and the call sites spelled that `else { return }` --
/// which reports `ok`. CI showed PASS on these arms at 0.007s, less time than spawning a daemon
/// takes, and they stayed dark for two days behind it. The binary guard was never the hole; the
/// hole was every OTHER way the rig can decline to start.
fn required_vault_daemon(daemon: Option<Daemon>) -> Option<Daemon> {
    if daemon.is_none() && vault_required() {
        panic!(
            "single_turn_module_e2e: the vault rig did not start, but this run is REQUIRED by \
             BROCA_REQUIRE_VAULT. The binaries resolved (a missing one panics earlier with its \
             path), so the refusal came from the rig setup itself -- daemon spawn, module \
             readiness, or the credential seed. Re-run without BROCA_REQUIRE_VAULT to see the \
             skip notice naming which."
        );
    }
    daemon
}

fn aimock_required() -> bool {
    !skip_allowed()
}

fn aimock_argv() -> Vec<PathBuf> {
    if let Ok(cmd) = std::env::var("AIMOCK_CMD") {
        return cmd.split_whitespace().map(PathBuf::from).collect();
    }
    let cli = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../node_modules/@copilotkit/aimock/dist/cli.js");
    vec![PathBuf::from("node"), cli]
}

// ───────────────────────── the rig ─────────────────────────

/// A booted aimock LLM on an ephemeral port (killed + cleaned on drop).
struct AiMock {
    child: tokio::process::Child,
    base_url: String,
    dir: PathBuf,
}

#[cfg(unix)]
struct ResponsesCapture {
    base_url: String,
    task: Option<JoinHandle<Vec<u8>>>,
}

#[cfg(unix)]
impl ResponsesCapture {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("provider request");
            let mut request = Vec::new();
            let mut chunk = [0; 4096];
            let header_end = loop {
                let read = socket.read(&mut chunk).await.expect("read request");
                assert_ne!(read, 0, "provider closed before sending HTTP headers");
                request.extend_from_slice(&chunk[..read]);
                if let Some(position) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                    break position + 4;
                }
            };
            let headers = String::from_utf8_lossy(&request[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            while request.len() < header_end + content_length {
                let read = socket.read(&mut chunk).await.expect("read request body");
                assert_ne!(read, 0, "provider closed before sending its request body");
                request.extend_from_slice(&chunk[..read]);
            }

            let body = concat!(
                "data: {\"type\":\"response.output_item.added\",\"output_index\":0,",
                "\"item\":{\"id\":\"msg_meta\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[]}}\n\n",
                "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,",
                "\"content_index\":0,\"delta\":\"META-MUSE-OK\"}\n\n",
                "data: {\"type\":\"response.output_text.done\",\"output_index\":0,",
                "\"content_index\":0,\"text\":\"META-MUSE-OK\"}\n\n",
                "data: {\"type\":\"response.output_item.done\",\"output_index\":0,",
                "\"item\":{\"id\":\"msg_meta\",\"type\":\"message\",\"role\":\"assistant\",",
                "\"content\":[{\"type\":\"output_text\",\"text\":\"META-MUSE-OK\"}]}}\n\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_meta\",",
                "\"status\":\"completed\",\"usage\":{\"input_tokens\":4,\"output_tokens\":3}}}\n\n"
            );
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            socket
                .write_all(response.as_bytes())
                .await
                .expect("write Responses stream");
            request
        });
        Self {
            base_url: format!("http://{address}/v1"),
            task: Some(task),
        }
    }

    async fn finish(mut self) -> Vec<u8> {
        timeout(
            Duration::from_secs(10),
            self.task.take().expect("capture task is present"),
        )
        .await
        .expect("Responses capture completed")
        .expect("Responses capture task succeeded")
    }
}

#[cfg(unix)]
impl Drop for ResponsesCapture {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl Drop for AiMock {
    fn drop(&mut self) {
        broca_test_support::kill_and_reap(&mut self.child);
        broca_test_support::dump_rig_process_logs_on_failure(&self.dir, "aimock");
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl AiMock {
    async fn start(tag: &str, fixtures_json: &str) -> Option<AiMock> {
        Self::start_on_port(tag, fixtures_json, 0).await
    }

    async fn start_on_port(tag: &str, fixtures_json: &str, port: u16) -> Option<AiMock> {
        let dir = broca_test_support::scratch_root()
            .join(format!("broca-mod-aimock-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        setup("create aimock dir", std::fs::create_dir_all(&dir).ok())?;
        let fixture_path = dir.join("fixtures.json");
        setup(
            "write aimock fixtures",
            std::fs::write(&fixture_path, fixtures_json).ok(),
        )?;

        let argv = aimock_argv();
        let (program, base_args) = setup("resolve aimock argv", argv.split_first())?;
        let mut child = match broca_test_support::tokio_rig_command(program, &dir, "aimock")
            .unwrap_or_else(|error| panic!("cannot configure aimock log files: {error}"))
            .args(base_args)
            .arg("--fixtures")
            .arg(&fixture_path)
            .arg("--port")
            .arg(port.to_string())
            .arg("--host")
            .arg("127.0.0.1")
            .kill_on_drop(true)
            .spawn()
        {
            Ok(c) => c,
            Err(e) => {
                if aimock_required() {
                    let why = required_because().unwrap_or("unknown");
                    panic!(
                        "single_turn_module_e2e: cannot spawn aimock ({program:?}): {e} \
                         (REQUIRED by {why})"
                    );
                }
                eprintln!(
                    "SKIP single_turn_module_e2e: cannot spawn aimock ({program:?}): {e}. This \
                     reports `ok` -- set BROCA_REQUIRE_AIMOCK=1 to make it a failure instead."
                );
                return None;
            }
        };
        let base_url = match timeout(Duration::from_secs(20), async {
            loop {
                let logs =
                    std::fs::read_to_string(dir.join("aimock.stdout.log")).unwrap_or_default();
                if let Some(addr) = logs.lines().find_map(parse_listening_addr) {
                    return Some(addr);
                }
                if child.try_wait().ok().flatten().is_some() {
                    return None;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        {
            Ok(Some(addr)) => format!("http://{addr}"),
            _ => {
                broca_test_support::kill_and_reap(&mut child);
                eprintln!("{}", broca_test_support::rig_process_logs(&dir, "aimock"));
                if aimock_required() {
                    let why = required_because().unwrap_or("unknown");
                    panic!(
                        "single_turn_module_e2e: aimock never reported a listening port \
                         (REQUIRED by {why})"
                    );
                }
                eprintln!(
                    "SKIP single_turn_module_e2e: aimock never reported a listening port. This \
                     reports `ok` -- set BROCA_REQUIRE_AIMOCK=1 to make it a failure instead."
                );
                return None;
            }
        };
        Some(AiMock {
            child,
            base_url,
            dir,
        })
    }
}

fn parse_listening_addr(line: &str) -> Option<String> {
    let start = line.find("http://")? + "http://".len();
    let rest = &line[start..];
    let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
    let addr = rest[..end].trim_end_matches('/');
    addr.contains(':').then(|| addr.to_string())
}

/// Seed one credential into a fresh offline vault and return its handle.
///
/// Returns `None` if any step fails, so a half-seeded vault can never reach a test as if it
/// were set up. A live credential stops after minting. Dead-credential scenarios vary whether
/// invalidation happens before or after handle minting; the CLI's `status` prints `needs_reauth`
/// for both, so their run assertions distinguish a live handle from a revoked one.
fn seed_vault_credential(
    vault_dir: &std::path::Path,
    key_path: &std::path::Path,
    ck_auth: &std::path::Path,
    seed: &VaultSeed,
) -> Option<String> {
    let run = |args: &[&str]| -> Option<std::process::Output> {
        let out = broca_test_support::retry_executable_file_busy(|| {
            broca_test_support::ckdev_command(ck_auth, broca_test_support::scratch_root())
                .arg("--data-dir")
                .arg(vault_dir)
                .arg("--key-path")
                .arg(key_path)
                .args(args)
                .output()
        })
        .ok()?;
        out.status.success().then_some(out)
    };
    run(&["bootstrap"])?;
    run(&[
        "put",
        "--id",
        &seed.credential_id,
        "--payload",
        "sk-rig-only",
    ])?;
    // The broca daemon reads every credential by grant (credential.get_scoped) as its
    // supervised principal, never by handle, so the rig grants that principal a read on the
    // seeded credential exactly as an operator does. The handle minted below is only for the
    // wire-shape probes that call the vault directly.
    run(&[
        "grant",
        "--principal",
        "reserved:broca",
        "--selector-kind",
        "exact",
        "--selector",
        &seed.credential_id,
        "--operation",
        "read",
    ])?;

    // The handle is stdout; the "store it now" note is stderr. Merged output captures the note.
    let mint = || -> Option<String> {
        let out = run(&["mint-handle", "--id", &seed.credential_id])?;
        let handle = String::from_utf8(out.stdout).ok()?.trim().to_string();
        handle.starts_with("ckh_").then_some(handle)
    };
    let handle = match seed.order {
        VaultSeedOrder::PutThenMint => mint()?,
        VaultSeedOrder::InvalidateThenMint => {
            run(&["invalidate", "--id", &seed.credential_id])?;
            mint()?
        }
        VaultSeedOrder::MintThenInvalidate => {
            let h = mint()?;
            run(&["invalidate", "--id", &seed.credential_id])?;
            h
        }
    };
    Some(handle)
}

fn seed_live_credential(
    vault_dir: &std::path::Path,
    key_path: &std::path::Path,
    ck_auth: &std::path::Path,
    seed: &VaultSeed,
) -> Option<String> {
    debug_assert!(seed.order == VaultSeedOrder::PutThenMint);
    seed_vault_credential(vault_dir, key_path, ck_auth, seed)
}

fn seed_dead_credential(
    vault_dir: &std::path::Path,
    key_path: &std::path::Path,
    ck_auth: &std::path::Path,
    seed: &VaultSeed,
) -> Option<String> {
    debug_assert!(seed.order != VaultSeedOrder::PutThenMint);
    seed_vault_credential(vault_dir, key_path, ck_auth, seed)
}

/// Whether the vault seed stays live or applies invalidation around handle minting.
///
/// `PutThenMint` is the ordinary live credential shape. `MintThenInvalidate` is not a production
/// shape -- a consumer reporting a dead token is not authority to revoke grants -- and exists so
/// the pause test's seed guard can be shown to fire
/// (`seeding_in_the_wrong_order_produces_a_revoked_handle`).
#[derive(Clone, Copy, PartialEq)]
enum VaultSeedOrder {
    PutThenMint,
    InvalidateThenMint,
    MintThenInvalidate,
}

struct VaultSeed {
    credential_id: String,
    order: VaultSeedOrder,
}

/// The real subc-core daemon supervising the broca module + the fake-aft tool provider.
struct Daemon {
    child: RefCell<broca_test_support::RigChild>,
    dir: PathBuf,
    connection_file: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        broca_test_support::kill_and_reap(self.child.get_mut());
        if !std::thread::panicking() {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

impl Daemon {
    /// Spawn subc-core with a TWO-module config: `broca` (the real module-serve binary,
    /// pointed at aimock for its provider + fake-aft for tools) and `aft` (the stub). Returns
    /// None when a required binary is absent (skip locally / panic in CI).
    async fn start(tag: &str, aimock_base_url: &str) -> Option<Daemon> {
        Self::start_with_env(tag, aimock_base_url, &[], &[]).await
    }

    /// Like [`start`](Self::start) but with extra env vars for the fake-aft stub.
    async fn start_with_aft_env(
        tag: &str,
        aimock_base_url: &str,
        aft_env: &[(&str, &str)],
    ) -> Option<Daemon> {
        Self::start_with_env(tag, aimock_base_url, &[], aft_env).await
    }

    /// Boot the real broca module with dependency-related environment overrides.
    async fn start_with_broca_env(
        tag: &str,
        aimock_base_url: &str,
        broca_env: &[(&str, &str)],
    ) -> Option<Daemon> {
        Self::start_with_env(tag, aimock_base_url, broca_env, &[]).await
    }

    async fn start_with_env(
        tag: &str,
        aimock_base_url: &str,
        broca_env: &[(&str, &str)],
        aft_env: &[(&str, &str)],
    ) -> Option<Daemon> {
        Self::start_with_env_and_aft_modules(
            tag,
            aimock_base_url,
            broca_env,
            aft_env,
            &[],
            None,
            None,
        )
        .await
    }

    /// Boot the same real daemon rig with one fake-aft process per test session. The stub's
    /// canned result is process-scoped, so separate provider modules let the test keep the tool
    /// name identical while assigning a distinct result to each session through `Tool.module`.
    async fn start_with_aft_modules(
        tag: &str,
        aimock_base_url: &str,
        aft_modules: &[(&str, &str)],
    ) -> Option<Daemon> {
        Self::start_with_env_and_aft_modules(
            tag,
            aimock_base_url,
            &[],
            &[],
            aft_modules,
            None,
            None,
        )
        .await
    }

    /// Boot the rig with a real credentials vault holding one DEAD credential.
    ///
    /// Seeds offline, before the supervisor starts, because the vault takes a single-writer
    /// lease once its daemon is up and refuses an offline write then. The seed ORDER is
    /// load-bearing: `invalidate` is a compound OPERATOR verb that also revokes existing
    /// handles, so minting first leaves a REVOKED handle -- a state production never reaches,
    /// since a consumer reporting a dead token is not authority to destroy grants. Minting
    /// after invalidate lands on production's shape (dead credential, live handle), because
    /// nothing refuses minting for a dead credential and the compound verb has no handle to
    /// revoke yet.
    async fn start_with_dead_vault_credential(tag: &str, credential_id: &str) -> Option<Daemon> {
        Self::start_with_vault(tag, credential_id, VaultSeedOrder::InvalidateThenMint, None).await
    }

    /// Boot the rig with a real credentials vault holding one LIVE credential.
    // Only the unix-gated provider-401 arm uses this: it needs make_vault_unavailable.
    #[cfg(unix)]
    async fn start_with_live_vault_credential(tag: &str, credential_id: &str) -> Option<Daemon> {
        Self::start_with_vault(tag, credential_id, VaultSeedOrder::PutThenMint, None).await
    }

    #[cfg(unix)]
    async fn start_with_live_vault_credential_and_provider_override(
        tag: &str,
        credential_id: &str,
        provider_base_url: &str,
    ) -> Option<Daemon> {
        Self::start_with_vault(
            tag,
            credential_id,
            VaultSeedOrder::PutThenMint,
            Some(provider_base_url),
        )
        .await
    }

    async fn start_with_vault(
        tag: &str,
        credential_id: &str,
        order: VaultSeedOrder,
        provider_base_url: Option<&str>,
    ) -> Option<Daemon> {
        let claustrum = claustrum_bin();
        let ck_auth = ck_auth_bin();
        if !claustrum.exists() || !ck_auth.exists() {
            let missing = if claustrum.exists() {
                ck_auth
            } else {
                claustrum
            };
            // Deliberately NOT folded into `subc_required()`, which CI sets on every run. The
            // vault lives in a third sibling repository that CI does not check out or build, so
            // requiring it there fails every CI run on a binary it was never going to have. Same
            // exemption the live-provider suites get: a run that CANNOT obtain a dependency must
            // skip, and only a run that can should be held to it.
            // Non-EMPTY, not merely present. A workflow that sets this per-platform writes an
            // empty string on the platforms it means to exclude, and an `is_some()` check reads
            // that as "required" -- turning a deliberate exclusion into a panic on exactly the
            // leg that was opted out.
            if std::env::var("BROCA_REQUIRE_VAULT").is_ok_and(|v| !v.is_empty()) {
                panic!(
                    "single_turn_module_e2e: the credentials vault is missing at {missing:?}, \
                     but this run is REQUIRED by BROCA_REQUIRE_VAULT. Build it in the MAIN \
                     checkout (cd <cortexkit>/claustrum && cargo build --release), then point \
                     this run at it with CK_CLAUSTRUM_BIN and CK_AUTH_BIN. A relative sibling \
                     path does not resolve from a git worktree, which is where this most \
                     often fires."
                );
            }
            eprintln!(
                "SKIP single_turn_module_e2e: vault binary missing at {missing:?}. This reports \
                 `ok` -- set BROCA_REQUIRE_VAULT=1 to make it a failure instead."
            );
            return None;
        }
        Self::start_with_env_and_aft_modules(
            tag,
            "",
            &[("BROCA_AUTH_SOURCE", "vault")],
            &[],
            &[],
            Some(VaultSeed {
                credential_id: credential_id.to_string(),
                order,
            }),
            provider_base_url,
        )
        .await
    }

    async fn start_with_env_and_aft_modules(
        tag: &str,
        aimock_base_url: &str,
        broca_env: &[(&str, &str)],
        aft_env: &[(&str, &str)],
        aft_modules: &[(&str, &str)],
        vault_seed: Option<VaultSeed>,
        provider_base_url: Option<&str>,
    ) -> Option<Daemon> {
        // A test process running under subc supervision inherits that daemon's
        // attestation env (SUBC_MODULE_ID / SUBC_LAUNCH_NONCE); presenting it to THIS
        // rig's fresh daemon fails route opens with `bad_consumer_identity`. Scrub once
        // so the test's clients stay Direct. The supervised modules the rig daemon
        // spawns get their own correct attestation from that daemon, unaffected.
        static SCRUB_ATTESTATION: std::sync::Once = std::sync::Once::new();
        SCRUB_ATTESTATION.call_once(|| {
            std::env::remove_var("SUBC_MODULE_ID");
            std::env::remove_var("SUBC_LAUNCH_NONCE");
        });
        let subc_core = resolve_required("ck-subc", "SUBC_CORE_BIN")?;
        let fake_aft = resolve_required("fake-aft-stub", "FAKE_AFT_STUB_BIN")?;
        let module_serve = module_serve_bin();
        assert!(
            module_serve.exists(),
            "broca-module-serve binary missing at {module_serve:?} (cargo should have built it)"
        );
        let vault_binaries = if vault_seed.is_some() {
            let claustrum = claustrum_bin();
            let ck_auth = ck_auth_bin();
            if !claustrum.exists() || !ck_auth.exists() {
                if subc_required() || vault_required() {
                    panic!(
                        "single_turn_module_e2e: required vault binaries are missing: \
                         ck-claustrum={claustrum:?}, ck-auth={ck_auth:?}"
                    );
                }
                eprintln!(
                    "SKIP single_turn_module_e2e: real vault binaries are missing: \
                     ck-claustrum={claustrum:?}, ck-auth={ck_auth:?}"
                );
                return None;
            }
            Some(broca_test_support::ckdev_binary_pair(
                &claustrum,
                &ck_auth,
                broca_test_support::scratch_root(),
            ))
        } else {
            None
        };

        let dir = broca_test_support::scratch_root()
            .join(format!("broca-mod-rig-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let config_home = dir.join("config");
        let runtime_dir = dir.join("runtime");
        let config_path = config_home.join("cortexkit").join("subc.jsonc");
        setup(
            "create subc config dir",
            std::fs::create_dir_all(config_path.parent().unwrap()).ok(),
        )?;
        setup(
            "create runtime dir",
            std::fs::create_dir_all(&runtime_dir).ok(),
        )?;
        // Seed a last-known-good catalog. Since the fusiform cutover the module carries no
        // compiled-in catalog, so with no fusiform module beside it every model resolution
        // refuses -- correctly, but it takes the rig's credential arms down with it, and the
        // refusal surfaces as a send error rather than anything naming a catalog.
        //
        // Seeding the LKG rather than faking a fusiform module keeps the rig on a path
        // production actually takes (a cold start with fusiform unreachable serves from this
        // same file) instead of inventing a test-only bypass around the refusal.
        // Seeded ONLY for the credential arms, which is what `vault_seed` marks: those drive a
        // real model resolution to reach the vault, and since the fusiform cutover the module
        // carries no compiled-in catalog, so with no fusiform module beside it every resolution
        // refuses and the arm dies before it tests anything.
        //
        // Deliberately NOT seeded everywhere. The health arms assert the opposite state -- that
        // a rig with NEITHER fusiform NOR an LKG reports both sources missing -- and a blanket
        // seed silently retires that assertion, which is how the first version of this broke
        // them. Seeding an LKG rather than faking a fusiform module keeps the rig on a path
        // production takes: a cold start with fusiform unreachable serves from this same file.
        //
        // The state root is the `broca-state` directory this rig names in the module's
        // BROCA_STATE_ROOT below, not the connection file's own directory. The module names the
        // full path in its refusal, which is the only reason this landed on the second try
        // rather than the tenth.
        if let Some(seed) = vault_seed.as_ref() {
            let lkg_dir = runtime_dir.join("broca-state").join("catalog");
            setup("create catalog dir", std::fs::create_dir_all(&lkg_dir).ok())?;
            let mut lkg: serde_json::Value =
                serde_json::from_str(include_str!("../fixtures/fusiform-lkg-minimal.json"))
                    .expect("the checked-in minimal fusiform LKG is valid JSON");
            if seed.order == VaultSeedOrder::PutThenMint {
                // `atomic-chat` is a shipped catalog overlay whose endpoint is the loopback
                // address used by the live-vault 401 arm. Adding only the model fact here keeps
                // `BROCA_PROVIDER_BASE_URL` unset, because that mock override deliberately skips
                // credential resolution and would make the cache proof vacuous.
                lkg["models"]["atomic-chat/cache-eviction-probe"] = json!({
                    "capability.attachment": false,
                    "capability.reasoning": false,
                    "capability.tool_call": false,
                    "existence": "present",
                    "limit.context": 8192
                });
            }
            if provider_base_url.is_some() {
                lkg["models"]["meta/muse-spark-1.1"] = json!({
                    "capability.attachment": false,
                    "capability.reasoning": true,
                    "capability.tool_call": true,
                    "existence": "present",
                    "limit.context": 1048576
                });
            }
            setup(
                "seed lkg catalog",
                std::fs::write(
                    lkg_dir.join("fusiform-lkg.json"),
                    serde_json::to_vec_pretty(&lkg).unwrap(),
                )
                .ok(),
            )?;
        }
        let default_tool_module = aft_modules
            .first()
            .map(|(module_id, _)| *module_id)
            .unwrap_or("aft");

        // The module's state root is named explicitly and lives under the rig. Left unset, it
        // would default to the data home, which the daemon spawn below also pins into the rig;
        // naming it here keeps the WAL where the on-disk anti-false-green check reads it back
        // and where the catalog LKG above is seeded.
        let mut config = json!({
            "version": 1,
            "modules": {
                "broca": {
                    "program": module_serve.to_string_lossy(),
                    "args": [],
                    "env": {
                        "BROCA_STATE_ROOT": runtime_dir.join("broca-state").to_string_lossy(),
                        // Drive the provider through aimock (openai-compatible) — no creds.
                        "BROCA_PROVIDER_BASE_URL": aimock_base_url,
                        // Dispatch tools to the fake-aft stub.
                        "BROCA_TOOL_MODULE": default_tool_module,
                    },
                    "enabled": true,
                },
                "aft": {
                    "program": fake_aft.to_string_lossy(),
                    "args": [],
                    "env": {
                        "FAKE_AFT_MODULE_ID": "aft",
                        "FAKE_AFT_TOOLS": "get_time",
                        "FAKE_AFT_TOOLCALL_RESULT": "MADRID-12:00",
                    },
                    "enabled": true,
                }
            }
        });
        for (key, value) in broca_env {
            config["modules"]["broca"]["env"][*key] = json!(value);
        }
        if let Some(base_url) = provider_base_url {
            let override_path = dir.join("meta-provider-overrides.jsonc");
            setup(
                "write Meta provider URL override",
                std::fs::write(
                    &override_path,
                    serde_json::to_vec(&json!({
                        "providers": {"meta": {"base_url": base_url}}
                    }))
                    .unwrap(),
                )
                .ok(),
            )?;
            config["modules"]["broca"]["env"]["BROCA_CONFIG_PATH"] =
                json!(override_path.to_string_lossy());
        }
        if aft_modules.is_empty() {
            for (key, value) in aft_env {
                config["modules"]["aft"]["env"][*key] = json!(value);
            }
        } else {
            config["modules"].as_object_mut().unwrap().remove("aft");
            for &(module_id, result) in aft_modules {
                config["modules"][module_id] = json!({
                    "program": fake_aft.to_string_lossy(),
                    "args": [],
                    "env": {
                        "FAKE_AFT_MODULE_ID": module_id,
                        "FAKE_AFT_TOOLS": "get_time",
                        "FAKE_AFT_TOOLCALL_RESULT": result,
                    },
                    "enabled": true,
                });
            }
        }
        if broca_env
            .iter()
            .any(|(key, value)| *key == "BROCA_AUTH_SOURCE" && *value == "vault")
        {
            let handles_path = dir.join("vault-handles.json");
            setup(
                "write vault handles fixture",
                std::fs::write(
                    &handles_path,
                    r#"{"handles":{"apikey:openai":"ckh_test_only"}}"#,
                )
                .ok(),
            )?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                setup(
                    "restrict vault handles to 0600",
                    std::fs::set_permissions(&handles_path, std::fs::Permissions::from_mode(0o600))
                        .ok(),
                )?;
            }
            config["modules"]["broca"]["env"]["BROCA_VAULT_HANDLES_PATH"] =
                json!(handles_path.to_string_lossy());
            // Point BROCA_AUTH_METHODS_PATH at an explicit file so developer settings cannot
            // leak into the rig. The Meta capture case declares its OAuth method here; other
            // vault cases declare no overrides and use the API-key fallback. The file must
            // exist: an explicitly named missing file is refused so a typo cannot start
            // cleanly and route every provider to API keys. The rig writes an isolated file
            // containing only the defaults the test intends to exercise.
            let method_defaults = if provider_base_url.is_some() {
                json!({"defaults": {"meta": "oauth"}})
            } else {
                json!({"defaults": {}})
            };
            let methods_path = dir.join("auth-methods.json");
            setup(
                "write isolated auth-methods",
                std::fs::write(&methods_path, serde_json::to_vec(&method_defaults).unwrap()).ok(),
            )?;
            config["modules"]["broca"]["env"]["BROCA_AUTH_METHODS_PATH"] =
                json!(methods_path.to_string_lossy());
        }
        // Seed the vault and declare its module BEFORE the supervisor starts: the vault holds a
        // single-writer lease once its daemon is up, and `reserved` is required because the
        // module echoes the launch nonce to prove it is the process the supervisor spawned.
        if let Some(seed) = vault_seed.as_ref() {
            let (vault_binary, ck_auth_binary) = vault_binaries.as_ref()?;
            let data_home = dir.join("vault-data");
            // subc resolves module storage to <data_home>/cortexkit/<module_id>. Seeding
            // anywhere else yields a vault the daemon cannot see, which presents as a broken
            // module rather than a misplaced file.
            let vault_dir = data_home.join("cortexkit").join("claustrum");
            // Outside the data dir: the vault's resolver rejects a master key stored beside the
            // ciphertext it protects.
            let key_path = dir.join("vault-master.key");
            setup(
                "create vault data dir",
                std::fs::create_dir_all(&vault_dir).ok(),
            )?;
            let handle = setup(
                "seed the vault credential",
                if seed.order == VaultSeedOrder::PutThenMint {
                    seed_live_credential(&vault_dir, &key_path, ck_auth_binary, seed)
                } else {
                    seed_dead_credential(&vault_dir, &key_path, ck_auth_binary, seed)
                },
            )?;

            config["storage"] =
                json!({ "backend": "sqlite", "data_home": data_home.to_string_lossy() });
            let vault_program = {
                #[cfg(unix)]
                {
                    if seed.order == VaultSeedOrder::PutThenMint {
                        // The cache arm must keep claustrum unavailable after killing it, while
                        // leaving broca alive. This wrapper starts the real binary normally, then
                        // makes supervisor respawns exit before registration once the test writes
                        // the sentinel. It avoids racing a newly registered vault between kill polls.
                        let wrapper = dir.join("claustrum-test-wrapper.sh");
                        setup(
                            "write claustrum outage wrapper",
                            std::fs::write(
                                &wrapper,
                                "#!/bin/sh\nif [ -e \"$CK_TEST_VAULT_DISABLED\" ]; then\n  exit 75\nfi\necho $$ > \"$CK_TEST_VAULT_PID_PATH\"\nexec \"$CK_REAL_CLAUSTRUM_BIN\" \"$@\"\n",
                            )
                            .ok(),
                        )?;
                        use std::os::unix::fs::PermissionsExt;
                        setup(
                            "make claustrum outage wrapper executable",
                            std::fs::set_permissions(
                                &wrapper,
                                std::fs::Permissions::from_mode(0o700),
                            )
                            .ok(),
                        )?;
                        wrapper
                    } else {
                        vault_binary.clone()
                    }
                }
                #[cfg(not(unix))]
                {
                    vault_binary.clone()
                }
            };
            config["modules"]["claustrum"] = json!({
                "program": vault_program.to_string_lossy(),
                "args": [],
                "reserved": true,
                "env": {
                    "CK_MASTER_KEY_PATH": key_path.to_string_lossy(),
                    "CK_REAL_CLAUSTRUM_BIN": vault_binary.to_string_lossy(),
                    "CK_TEST_VAULT_DISABLED": dir.join("vault-disabled").to_string_lossy(),
                    "CK_TEST_VAULT_PID_PATH": dir.join("vault.pid").to_string_lossy(),
                },
                "enabled": true,
            });
            let handles_path = dir.join("vault-handles.json");
            setup(
                "write seeded vault handles",
                std::fs::write(
                    &handles_path,
                    serde_json::to_string(&json!({ "handles": { &seed.credential_id: handle } }))
                        .unwrap(),
                )
                .ok(),
            )?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                setup(
                    "restrict vault handles to 0600",
                    std::fs::set_permissions(&handles_path, std::fs::Permissions::from_mode(0o600))
                        .ok(),
                )?;
            }
            config["modules"]["broca"]["env"]["BROCA_VAULT_HANDLES_PATH"] =
                json!(handles_path.to_string_lossy());
            config["modules"]["broca"]["env"]["BROCA_CREDENTIAL_VAULT_MODULE_ID"] =
                json!("claustrum");
        }
        setup(
            "write subc config",
            std::fs::write(&config_path, serde_json::to_string_pretty(&config).unwrap()).ok(),
        )?;

        let connection_file = runtime_dir.join("subc-connection.json");
        let child = broca_test_support::retry_executable_file_busy(|| {
            broca_test_support::tokio_rig_command(&subc_core, &dir, "daemon")?
                // The daemon derives its per-module capture directory from the DATA home
                // (`<data-home>/cortexkit/run/logs/<module_id>.stderr.log`), not from
                // XDG_RUNTIME_DIR. A rig that leaves it unset appends its own "broca" module's
                // stderr into the OPERATOR'S production capture file, where placement gates read
                // seal-line counts. Pinning it under the rig keeps the count about production.
                .env("XDG_CONFIG_HOME", &config_home)
                .env("XDG_RUNTIME_DIR", &runtime_dir)
                .env("XDG_DATA_HOME", runtime_dir.join("data-home"))
                // The broca module is given its own BROCA_STATE_ROOT in the config above; dropping
                // any inherited copy keeps every other module the daemon spawns off it as well.
                .env_remove("BROCA_STATE_ROOT")
                .env("SUBC_PORT", "0")
                .kill_on_drop(true)
                .spawn()
        })
        .expect("spawn subc-core");
        let child = broca_test_support::RigChild::new(child, &dir, "daemon");

        // Wait until the connection file is published (subc is up). Once the binaries are
        // present (subc_required passed), the daemon FAILING to come up is a real failure, not
        // a skip — fail loud in CI so a broken daemon can't show green.
        let deadline = Instant::now() + Duration::from_secs(10);
        while !connection_file.exists() {
            if Instant::now() >= deadline {
                let msg = "daemon never published its connection file";
                if subc_required() {
                    let why = required_because().unwrap_or("unknown");
                    panic!("single_turn_module_e2e: {msg} (bins present, REQUIRED by {why})");
                }
                eprintln!(
                    "SKIP: {msg}. This reports `ok` -- set BROCA_REQUIRE_SUBC=1 to make it a \
                     failure instead."
                );
                return None;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let mut daemon = Daemon {
            child: RefCell::new(child),
            dir,
            connection_file,
        };
        // Wait until the broca MODULE is actually LIVE (registered + bindable), not just
        // until subc is up. A supervised module spawns + HELLO-registers ASYNCHRONOUSLY after
        // the daemon binds, so a client that races in sees `state=running, live=false` and its
        // route.open is rejected `target_unavailable`. The orchestrator waits for readiness
        // before driving the client (the same discipline cortexkit-e2e uses for aft). A module
        // that never goes live is a real failure once the bins are present — fail loud in CI.
        if !daemon.wait_for_module_live("broca").await {
            let msg = daemon.readiness_diagnosis("broca");
            let msg = msg.as_str();
            if subc_required() {
                let why = required_because().unwrap_or("unknown");
                panic!("single_turn_module_e2e: {msg} (bins present, REQUIRED by {why})");
            }
            eprintln!(
                "SKIP: {msg}. This reports `ok` -- set BROCA_REQUIRE_SUBC=1 to make it a \
                 failure instead."
            );
            return None;
        }
        Some(daemon)
    }

    /// How long a supervised module gets to register and become bindable. Named because the
    /// diagnosis message quotes it: a reader who sees "never became live within 15s" can weigh
    /// that against what the host was doing, which a bare timeout denies them.
    const READINESS_TIMEOUT_SECS: u64 = 15;

    /// Name which process failed when a module never becomes live.
    ///
    /// The connection-file gate above proves the SUPERVISOR is alive; it proves nothing about
    /// the supervised child, and both read as "the daemon" to whoever is looking at the
    /// failure. A module dying at boot (an unwritable state root, say) leaves a live supervisor
    /// publishing its file, so the bare message sent an investigation at the harness while the
    /// module was the thing that died.
    fn readiness_diagnosis(&mut self, module_id: &str) -> String {
        match self.child.borrow_mut().try_wait() {
            // The supervisor itself exited: the harness or its config is the suspect, and the
            // module may never have been spawned at all.
            Ok(Some(status)) => format!(
                "the subc supervisor exited ({status}) before module '{module_id}' became live"
            ),
            // Supervisor alive, module absent. Four causes share this arm and it cannot
            // separate them, so it must not pretend to: the module failed to boot, booted and
            // failed to register, registered and never became bindable, OR WAS STILL STARTING
            // WHEN THE DEADLINE EXPIRED. The last one is not hypothetical -- on a shared host
            // under a fleet-wide compile load this arm fires against unmodified master, so a
            // message naming only the first three sends the reader to bisect a diff that is
            // not involved.
            Ok(None) => format!(
                "module '{module_id}' never became live within {}s while \
                 the subc supervisor stayed up (it failed to boot, failed to register, never \
                 became bindable, or was still starting when the deadline expired -- check host \
                 load before suspecting the diff)",
                Self::READINESS_TIMEOUT_SECS
            ),
            Err(e) => format!(
                "module '{module_id}' never became live and the supervisor's state is unreadable \
                 ({e})"
            ),
        }
    }

    /// Poll until `module_id`'s management surface is bindable (a `route.open` succeeds),
    /// proving the module finished registering. Opens a throwaway consumer connection, tries
    /// the bind on a dummy session, and closes it. Returns false if it never becomes live.
    async fn wait_for_module_live(&self, module_id: &str) -> bool {
        let probe_session = broca_protocol::BindIdentity::new(&self.dir, "probe", "readiness");
        let deadline = Instant::now() + Duration::from_secs(Self::READINESS_TIMEOUT_SECS);
        loop {
            if let Ok(conn) = broca_subc::SubcConnection::connect(&self.connection_file).await {
                if conn
                    .route_open_management(module_id, &probe_session)
                    .await
                    .is_ok()
                {
                    return true; // the module is live + bindable
                }
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn broca_health(&self) -> broca_subc::ModuleHealthSnapshot {
        let conn = broca_subc::SubcConnection::connect(&self.connection_file)
            .await
            .expect("connect health probe consumer");
        conn.supervisor_health_probe("broca")
            .await
            .expect("probe broca health")
    }

    fn ck_auth_command(&self) -> std::process::Command {
        let mut command =
            broca_test_support::ckdev_command(ck_auth_bin(), broca_test_support::scratch_root());
        command
            .arg("--data-dir")
            .arg(
                self.dir
                    .join("vault-data")
                    .join("cortexkit")
                    .join("claustrum"),
            )
            .arg("--key-path")
            .arg(self.dir.join("vault-master.key"))
            .arg("--subc")
            .arg(&self.connection_file);
        command
    }

    /// Return a seeded credential to active, the way a re-login does.
    ///
    /// Commits THROUGH the running vault module (`--subc`) rather than offline: the vault holds
    /// a single-writer lease while its daemon is up, so the offline path is refused outright.
    /// That is also how a real re-login writes, so the resume under test is reached the way
    /// production reaches it. A failed repair is asserted here rather than left to surface
    /// later as a resume that did not happen.
    fn repair_vault_credential(&self, credential_id: &str) {
        let out = broca_test_support::retry_executable_file_busy(|| {
            self.ck_auth_command()
                .args([
                    "put",
                    "--id",
                    credential_id,
                    "--payload",
                    "sk-repaired",
                    "--replace",
                ])
                .output()
        })
        .expect("run ck-auth put --replace");
        assert!(
            out.status.success(),
            "repairing the credential failed, so the resume leg cannot be attributed to the \
             engine: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// Wait until the running vault has applied the provider-auth-failure report.
    ///
    /// The cache eviction happens before that report is sent, so this vault-side state change is
    /// also a barrier proving the first run completed its post-run credential handling before the
    /// test removes vault connectivity.
    #[cfg(unix)]
    async fn wait_for_vault_needs_reauth(&self) -> bool {
        broca_test_support::wait_until("vault applies provider auth failure", || async {
            let output = broca_test_support::retry_executable_file_busy(|| {
                self.ck_auth_command().arg("status").output()
            });
            match output {
                Ok(output) => {
                    let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
                    text.push_str(&String::from_utf8_lossy(&output.stderr));
                    if output.status.success() && text.contains("needs_reauth") {
                        Ok(true)
                    } else {
                        Err(format!("auth status={}, output={text}", output.status))
                    }
                }
                Err(error) => Err(format!("auth status probe: {error}")),
            }
        })
        .await
    }

    #[cfg(unix)]
    async fn make_vault_unavailable(&self) -> bool {
        if std::fs::write(self.dir.join("vault-disabled"), b"disabled").is_err() {
            return false;
        }
        let pid = std::fs::read_to_string(self.dir.join("vault.pid"))
            .expect("the live vault wrapper must publish its PID")
            .trim()
            .parse::<i32>()
            .expect("vault PID must be numeric");
        assert!(pid > 1, "vault PID must identify a child process");
        // The wrapper execs the vault. Signal its published PID rather than
        // depending on a process listing's executable name and parent matching.
        unsafe { libc::kill(pid, libc::SIGKILL) == 0 }
    }

    /// The module's WAL directory, under the BROCA_STATE_ROOT the rig gives the module.
    fn module_wal_dir(&self) -> PathBuf {
        self.connection_file
            .parent()
            .unwrap()
            .join("broca-state")
            .join("wal")
    }
}

/// Unwrap a rig SETUP step, refusing to continue silently when the run is required.
///
/// A bare `?` on a fallible setup call ends the arm with no output at all, and -- worse -- it
/// exits before any guard is consulted, so `BROCA_REQUIRE_SUBC=1` cannot turn it into a
/// failure. That combination produces a green run on every platform that tested nothing and
/// said nothing, which is strictly worse than having no guard, because the guard's existence
/// is what makes the result feel accounted for.
///
/// Required runs panic naming the step and the trigger. Optional runs still return `None`, so
/// a developer without the sibling binaries keeps a clean skip.
fn setup<T>(what: &str, result: Option<T>) -> Option<T> {
    if result.is_none() {
        if let Some(why) = required_because() {
            panic!(
                "single_turn_module_e2e: rig setup failed at '{what}', and this run is REQUIRED \
                 by {why}. The arm would otherwise end here silently and report ok."
            );
        }
        eprintln!(
            "SKIP single_turn_module_e2e: rig setup failed at '{what}'. This reports `ok` -- set \
             BROCA_REQUIRE_SUBC=1 to make it a failure instead."
        );
    }
    result
}

fn resolve_required(name: &str, env_key: &str) -> Option<PathBuf> {
    let path = subc_bin(name, env_key);
    if path.exists() {
        return Some(path);
    }
    if subc_required() {
        panic!(
            "{name} is missing at {path:?}, but the real-subc module e2e is REQUIRED by {why}. \
             Build it: (cd ../subconscious && cargo build -p subc-core --bins).",
            why = required_because().unwrap_or("unknown")
        );
    }
    eprintln!(
        "SKIP single_turn_module_e2e: {name} missing at {path:?}. This reports `ok` -- set \
         BROCA_REQUIRE_SUBC=1 to make it a failure instead."
    );
    None
}

/// A captured broca-session run: its greppable stdout lines + exit code.
impl ClientRun {
    fn saw(&self, marker: &str) -> bool {
        self.lines.iter().any(|l| l.contains(marker))
    }
}

/// A per-session model/tool identity used by the mixed-family test. The model ids are catalog
/// ids, so the real module chooses the wire family instead of the test constructing providers.
///
/// Each session names its own tool module, which is what lets every session have a distinct
/// tool-result sentinel: the stub serves one fixed result per process. That is a real routing
/// path -- a caller pinning a tool to a module -- but it is not the deployed shape, where the
/// tool module is unset and every session shares the default. Sharing one module is the
/// stronger test of the route map and the dispatch fence, since separate modules keep sessions
/// apart even under a map keyed without a session identity. The sibling test below covers it,
/// trading distinct tool results for shared-module contention.
struct MixedSlot {
    label: &'static str,
    session: String,
    model: &'static str,
    aft_module: String,
    prompt: String,
    tool_result: String,
    final_text: String,
}

impl MixedSlot {
    fn new(label: &'static str, session_index: usize, model: &'static str) -> Self {
        MixedSlot {
            label,
            session: format!("mixed-{session_index}"),
            model,
            aft_module: format!("aft-{session_index}"),
            prompt: format!("Run the coordinator isolation probe for {label}-{session_index}."),
            tool_result: format!("TOOL-SENTINEL-{label}-{session_index}"),
            final_text: format!("FINAL-SENTINEL-{label}-{session_index}"),
        }
    }

    fn tools_json(&self) -> String {
        json!([{
            "name": "get_time",
            "description": "Get the current time in a timezone.",
            "input_schema": {
                "type": "object",
                "properties": { "timezone": { "type": "string" } }
            },
            "module": self.aft_module.as_str(),
        }])
        .to_string()
    }
}

/// Aimock routes the first and second model requests by the unique user prompt. This keeps one
/// server shared by every family while giving each session a distinct final response.
fn mixed_family_fixtures(slots: &[MixedSlot]) -> String {
    let fixtures = slots
        .iter()
        .flat_map(|slot| {
            [
                json!({
                    "match": { "userMessage": slot.prompt.as_str(), "hasToolResult": false },
                    // Hold the first model response briefly so the five admission barriers can
                    // all be observed before any tool turn and final response can finish. The
                    // test asserts only that barrier, never relative session timing.
                    "latency": 1000,
                    "response": {
                        "toolCalls": [{
                            "name": "get_time",
                            "arguments": { "timezone": slot.label }
                        }]
                    }
                }),
                json!({
                    "match": { "userMessage": slot.prompt.as_str(), "hasToolResult": true },
                    "response": { "content": slot.final_text.as_str() }
                }),
            ]
        })
        .collect::<Vec<_>>();
    json!({ "fixtures": fixtures }).to_string()
}

/// Observed client markers for the required admission barrier: no run may finish until every
/// client has reported an active send. This checks overlap without imposing timing/order between
/// the sessions themselves.
///
/// The expected count is supplied at construction rather than written in, because this assertion
/// weakens quietly if the two drift: with a count below the number of sessions, the barrier stops
/// flagging once that many are admitted, and the remaining sessions are free to finish early
/// without the probe noticing. Deriving it from the slot list keeps the two in step.
struct ConcurrencyProbe {
    expected: usize,
    admitted: AtomicUsize,
    finished: AtomicUsize,
    finished_before_all_admitted: AtomicBool,
}

impl ConcurrencyProbe {
    fn new(expected: usize) -> Self {
        Self {
            expected,
            admitted: AtomicUsize::new(0),
            finished: AtomicUsize::new(0),
            finished_before_all_admitted: AtomicBool::new(false),
        }
    }

    fn observe(&self, line: &str) {
        if line.contains("SEND state=active") {
            self.admitted.fetch_add(1, Ordering::SeqCst);
        }
        if line.contains("RUN_FINISHED") {
            if self.admitted.load(Ordering::SeqCst) < self.expected {
                self.finished_before_all_admitted
                    .store(true, Ordering::SeqCst);
            }
            self.finished.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// Run the broca-session client binary to completion against the daemon.
/// Poll a module's WAL directory until every marker appears in one file, or time out.
///
/// The durable record is the barrier rather than a wire line: a client can report a state the
/// WAL has not yet committed, and a test that proceeds on the report races the fsync it
/// actually depends on.
async fn wait_for_wal_marker(wal_dir: &Path, markers: &[&str]) -> bool {
    broca_test_support::wait_until("durable WAL markers", || async {
        let hit = std::fs::read_dir(wal_dir)
            .ok()
            .into_iter()
            .flatten()
            .filter_map(|e| std::fs::read(e.ok()?.path()).ok())
            .any(|bytes| {
                let text = String::from_utf8_lossy(&bytes);
                markers.iter().all(|m| text.contains(m))
            });
        hit.then_some(true)
            .ok_or_else(|| format!("wal_dir={}, missing markers={markers:?}", wal_dir.display()))
    })
    .await
}

async fn run_client(
    daemon: &Daemon,
    project_root: &Path,
    session: &str,
    args: &[&str],
) -> ClientRun {
    run_client_with_probe(daemon, project_root, session, args, None).await
}

/// Observe admission/finish markers as they arrive, rather than inferring concurrency
/// from how quickly client processes exit. All clients share the same deadline evidence.
async fn run_client_with_probe(
    daemon: &Daemon,
    project_root: &Path,
    session: &str,
    args: &[&str],
    probe: Option<std::sync::Arc<ConcurrencyProbe>>,
) -> ClientRun {
    let mut command = Command::new(broca_session_bin());
    command
        .arg("--subc")
        .arg(&daemon.connection_file)
        .arg("--project-root")
        .arg(project_root)
        .arg("--session")
        .arg(session)
        .args(args);
    session_client::run_with_deadline(
        &mut command,
        CLIENT_DEADLINE,
        |line| {
            if let Some(probe) = probe.as_ref() {
                probe.observe(line);
            }
        },
        || session_client::daemon_diagnostics(&daemon.dir, session, &mut daemon.child.borrow_mut()),
    )
    .await
    .unwrap_or_else(|diagnostics| panic!("client deadline expired\n{diagnostics}"))
}

async fn run_mixed_client(
    daemon: &Daemon,
    project_root: &Path,
    slot: &MixedSlot,
    probe: Arc<ConcurrencyProbe>,
) -> ClientRun {
    let tools = slot.tools_json();
    let args = [
        "run",
        "--prompt",
        slot.prompt.as_str(),
        "--model",
        slot.model,
        "--tools",
        tools.as_str(),
    ];
    run_client_with_probe(daemon, project_root, &slot.session, &args, Some(probe)).await
}

/// Like `run_mixed_client`, but the tool carries NO module hint, so dispatch falls to the
/// configured default and every session contends for the same target.
async fn run_shared_module_client(
    daemon: &Daemon,
    project_root: &Path,
    slot: &MixedSlot,
    probe: Arc<ConcurrencyProbe>,
) -> ClientRun {
    let tools = json!([{
        "name": "get_time",
        "description": "Get the current time in a timezone.",
        "input_schema": {
            "type": "object",
            "properties": { "timezone": { "type": "string" } }
        }
    }])
    .to_string();
    let args = [
        "run",
        "--prompt",
        slot.prompt.as_str(),
        "--model",
        slot.model,
        "--tools",
        tools.as_str(),
    ];
    run_client_with_probe(daemon, project_root, &slot.session, &args, Some(probe)).await
}

// ───────────────────────── the test ─────────────────────────

/// The model script: turn 1 calls `get_time`, turn 2 (after the tool result) answers.
const FIXTURES: &str = r#"{
  "fixtures": [
    { "match": { "hasToolResult": false },
      "response": { "toolCalls": [{ "name": "get_time", "arguments": { "timezone": "Europe/Madrid" } }] } },
    { "match": { "hasToolResult": true },
      "response": { "content": "The tool returned MADRID-12:00." } }
  ]
}"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_client_single_turn_through_the_real_module() {
    let Some(aimock) = AiMock::start("single-turn", FIXTURES).await else {
        return;
    };
    let Some(daemon) = Daemon::start("single-turn", &aimock.base_url).await else {
        return;
    };

    // A real project root for the session identity (subc canonicalizes it at route.open, so
    // it MUST exist on disk).
    let project_root = daemon.dir.join("project");
    std::fs::create_dir_all(&project_root).unwrap();
    let session = "s1";

    // The tool the turn may call (the model picks `get_time`).
    let tools = json!([{
        "name": "get_time",
        "description": "Get the current time in a timezone.",
        "input_schema": { "type": "object", "properties": { "timezone": { "type": "string" } } }
    }])
    .to_string();

    // Drive the COMBINED single-turn flow through the real module: subscribe → send → drain
    // the control stream to RUN_FINISHED.
    let run = run_client(
        &daemon,
        &project_root,
        session,
        &[
            "run",
            "--prompt",
            "What time is it in Madrid? Use the tool.",
            "--model",
            "mock/mock-model",
            "--tools",
            &tools,
        ],
    )
    .await;

    // ANTI-FALSE-GREEN 1: the WIRE events prove the full round-trip happened over the real
    // wire — the run started, the model called the tool, the tool result came back (through
    // real subc → fake-aft), and the run reached its terminal.
    assert!(run.saw("BOUND"), "client bound the management surface");
    assert!(
        run.saw("SEND state=active"),
        "the send started a run (not queued)"
    );
    assert!(run.saw("RUN_STARTED"), "the run's control stream started");
    assert!(
        run.saw("TOOL_CALL") && run.saw("name=get_time"),
        "the model called get_time over the wire"
    );
    assert!(
        run.saw("TOOL_RESULT") && run.saw("MADRID-12:00"),
        "the tool result (from fake-aft through real subc) streamed back"
    );
    assert!(
        run.saw("RUN_FINISHED") && run.saw("reason=Completed"),
        "the run reached a clean terminal"
    );
    assert_eq!(run.exit_code, 0, "a clean single-turn run exits 0");

    // ANTI-FALSE-GREEN 2: the run actually PERSISTED — the module wrote a durable WAL for the
    // session. A run that merely produced wire events but didn't persist (or never ran) would
    // leave no WAL.
    let wal_dir = daemon.module_wal_dir();
    assert!(
        wal_dir.exists(),
        "the module's WAL dir should exist after a run ({wal_dir:?})"
    );
    let wal_files: Vec<_> = std::fs::read_dir(&wal_dir)
        .map(|rd| rd.filter_map(|e| e.ok()).collect())
        .unwrap_or_default();
    assert!(
        !wal_files.is_empty(),
        "the module should have written a durable WAL file for the session ({wal_dir:?})"
    );
    #[cfg(unix)]
    {
        let state_root = wal_dir.parent().unwrap();
        let public = broca_test_support::public_tree_entries(state_root).unwrap();
        assert!(
            public.is_empty(),
            "state-root entries with group/world permissions: {public:?}"
        );
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn meta_muse_oauth_uses_responses_bearer_and_subscription_billing() {
    let server = ResponsesCapture::start().await;
    let Some(daemon) = Daemon::start_with_live_vault_credential_and_provider_override(
        "meta-muse-oauth",
        "oauth:meta:test",
        &server.base_url,
    )
    .await
    else {
        return;
    };

    let project_root = daemon.dir.join("meta-muse-project");
    std::fs::create_dir_all(&project_root).unwrap();
    let session = "meta-muse";
    let run = run_client(
        &daemon,
        &project_root,
        session,
        &[
            "run",
            "--prompt",
            "Reply with the test response.",
            "--model",
            "meta/muse-spark-1.1",
            "--tools",
            "[]",
            "--auth-method",
            "oauth",
            "--auth-account",
            "test",
        ],
    )
    .await;
    let captured = server.finish().await;

    assert!(run.saw("RUN_STARTED"), "the run started");
    assert!(
        run.saw("RUN_FINISHED") && run.saw("reason=Completed"),
        "the run completed against the loopback provider: {:?}",
        run.lines
    );
    assert_eq!(run.exit_code, 0, "the Responses turn exits successfully");

    let header_end = captured
        .windows(4)
        .position(|part| part == b"\r\n\r\n")
        .expect("captured HTTP request has headers")
        + 4;
    let headers = String::from_utf8_lossy(&captured[..header_end]);
    let request_line = headers.lines().next().expect("HTTP request line");
    assert_eq!(request_line, "POST /v1/responses HTTP/1.1");
    let authorization = headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("authorization")
            .then_some(value.trim())
    });
    assert_eq!(authorization, Some("Bearer sk-rig-only"));
    for forbidden in ["chatgpt-account-id", "session-id", "originator"] {
        assert!(
            !headers.lines().any(|line| {
                line.split_once(':')
                    .is_some_and(|(name, _)| name.eq_ignore_ascii_case(forbidden))
            }),
            "the first-party Responses request must not send {forbidden}: {headers}"
        );
    }
    let request_body: serde_json::Value =
        serde_json::from_slice(&captured[header_end..]).expect("Responses body is JSON");
    assert_eq!(request_body["model"], "muse-spark-1.1");
    assert!(
        request_body.get("reasoning").is_none(),
        "a Muse model without a variant does not add reasoning settings: {request_body}"
    );
    // Meta reuses a cached prefix only for a request that names a cache scope. A send without a
    // cache key gets its session id as the scope, frozen at admission.
    assert_eq!(
        request_body.get("prompt_cache_key"),
        Some(&serde_json::json!(session)),
        "the Meta request names the session as its prompt cache scope: {request_body}"
    );

    let mut records = std::fs::read_dir(daemon.module_wal_dir())
        .expect("module WAL directory exists")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "wal"))
        .find_map(|path| {
            let frames = broca_wal::replay_file(&path, broca_wal::DEFAULT_MAX_READ_BYTES).ok()?;
            frames
                .iter()
                .any(|frame| {
                    matches!(
                        &frame.record,
                        broca_wal::WalRecord::RunStarted { session: bind, .. }
                            if bind.session == session
                    )
                })
                .then_some(frames)
        })
        .expect("the completed Meta run has a readable WAL");
    let (run_id, credential_id, frozen_cache_key) = records
        .iter()
        .find_map(|frame| match &frame.record {
            broca_wal::WalRecord::RunStarted {
                run_id,
                session: bind,
                config,
                ..
            } if bind.session == session => Some((
                run_id.clone(),
                config
                    .auth_selection
                    .as_ref()
                    .map(|selection| selection.credential_id.clone()),
                config.frozen_render_config.get("prompt_cache_key").cloned(),
            )),
            _ => None,
        })
        .expect("RunStarted records the Meta credential selection");
    assert_eq!(credential_id.as_deref(), Some("oauth:meta:test"));
    assert_eq!(
        frozen_cache_key,
        Some(serde_json::json!(session)),
        "RunStarted freezes the key, so a resume renders the same scope"
    );

    let facts = broca_core::compute_segments_for_run(
        &records,
        &run_id,
        "meta-muse-e2e",
        &broca_module_serve::ChargeBasisTable::shipped_defaults(),
    );
    assert_eq!(facts.len(), 1, "one completed turn yields one billing fact");
    assert_eq!(facts[0].provider, "meta");
    assert_eq!(facts[0].auth_selection.as_deref(), Some("oauth:meta:test"));
    assert_eq!(
        facts[0].charge_basis,
        broca_core::ChargeBasis::SubscriptionIncluded
    );
    records.clear();
}

/// Five concurrent sessions exercise three catalog-selected families: Anthropic Messages,
/// OpenAI Chat, and Gemini. Duplicate slots within the first two families are intentional: a
/// family-scoped cache or map keyed without a run/session identity is only exposed by overlap
/// between siblings of the same family.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_mixed_family_sessions_through_the_real_module_are_isolated() {
    let slots = vec![
        MixedSlot::new("anthropic", 0, "anthropic/claude-opus-5"),
        MixedSlot::new("anthropic", 1, "anthropic/claude-opus-5"),
        MixedSlot::new("openai-chat", 2, "deepseek/deepseek-v4-flash"),
        MixedSlot::new("openai-chat", 3, "deepseek/deepseek-v4-flash"),
        MixedSlot::new("gemini", 4, "google/gemini-2.5-flash"),
    ];

    let sentinels = slots
        .iter()
        .flat_map(|slot| [&slot.tool_result, &slot.final_text])
        .collect::<Vec<_>>();
    let unique_sentinels = sentinels.iter().collect::<HashSet<_>>();
    assert_eq!(
        unique_sentinels.len(),
        sentinels.len(),
        "all tool-result and final-text sentinels must be unique so exclusion checks are non-vacuous"
    );

    let fixtures = mixed_family_fixtures(&slots);
    let Some(aimock) = AiMock::start("mixed-family-concurrency", &fixtures).await else {
        return;
    };
    let aft_modules = slots
        .iter()
        .map(|slot| (slot.aft_module.as_str(), slot.tool_result.as_str()))
        .collect::<Vec<_>>();
    let Some(daemon) =
        Daemon::start_with_aft_modules("mixed-family-concurrency", &aimock.base_url, &aft_modules)
            .await
    else {
        return;
    };

    let project_root = daemon.dir.join("mixed-project");
    std::fs::create_dir_all(&project_root).unwrap();
    // subc canonicalizes bind identities before handing them to the module, including macOS's
    // /private prefix. Use the same canonical path when deriving the expected WAL names.
    let project_root = std::fs::canonicalize(project_root).unwrap();
    let probe = Arc::new(ConcurrencyProbe::new(slots.len()));

    // `join!` polls every real client future together. The probe observes the clients' wire
    // markers and proves all five sends were admitted before any run reached its terminal.
    let (r0, r1, r2, r3, r4) = tokio::join!(
        run_mixed_client(&daemon, &project_root, &slots[0], Arc::clone(&probe)),
        run_mixed_client(&daemon, &project_root, &slots[1], Arc::clone(&probe)),
        run_mixed_client(&daemon, &project_root, &slots[2], Arc::clone(&probe)),
        run_mixed_client(&daemon, &project_root, &slots[3], Arc::clone(&probe)),
        run_mixed_client(&daemon, &project_root, &slots[4], Arc::clone(&probe)),
    );
    let runs = [r0, r1, r2, r3, r4];

    assert_eq!(
        probe.admitted.load(Ordering::SeqCst),
        slots.len(),
        "all five sessions reached the admitted/active barrier"
    );
    assert_eq!(
        probe.finished.load(Ordering::SeqCst),
        slots.len(),
        "all five sessions reached a terminal event"
    );
    assert!(
        !probe.finished_before_all_admitted.load(Ordering::SeqCst),
        "every session must be admitted before any session finishes"
    );

    for (index, (slot, run)) in slots.iter().zip(runs.iter()).enumerate() {
        assert_eq!(run.exit_code, 0, "slot {index} exited cleanly");
        assert!(
            run.saw("BOUND"),
            "{} bound the management surface",
            slot.session
        );
        assert!(
            run.saw("SEND state=active"),
            "{} was admitted rather than queued",
            slot.session
        );
        assert!(
            run.saw("RUN_STARTED"),
            "{} emitted RunStarted",
            slot.session
        );
        assert!(
            run.saw("TOOL_CALL") && run.saw("name=get_time"),
            "{} called the shared tool name",
            slot.session
        );
        assert!(
            run.saw(&slot.tool_result),
            "{} saw its own tool-result sentinel",
            slot.session
        );
        assert!(
            run.saw(&slot.final_text),
            "{} saw its own final-text sentinel",
            slot.session
        );
        assert!(
            run.saw("RUN_FINISHED") && run.saw("reason=Completed"),
            "{} completed cleanly",
            slot.session
        );

        // Check the entire captured stream, not just the typed tool-result line: a foreign
        // sentinel in final/display text is the same isolation failure as a foreign tool result.
        for (other_index, other) in slots.iter().enumerate() {
            if other_index == index {
                continue;
            }
            assert!(
                !run.saw(&other.tool_result),
                "{} saw foreign tool-result sentinel {}",
                slot.session,
                other.tool_result
            );
            assert!(
                !run.saw(&other.final_text),
                "{} saw foreign final-text sentinel {}",
                slot.session,
                other.final_text
            );
        }
    }

    // The module hashes the full bind identity into one WAL path per session. Read each WAL back
    // through the same store API used by the module and ensure durability did not merge payloads.
    let wal_dir = daemon.module_wal_dir();
    assert!(
        wal_dir.exists(),
        "the module WAL directory exists ({wal_dir:?})"
    );
    // Read whatever WALs the module actually wrote, rather than deriving each expected path by
    // re-hashing the bind identity here. The module canonicalizes the project root before
    // hashing it, and reproducing that faithfully means matching platform-specific path forms --
    // so a mismatch shows up as "this session has no WAL", which reads like lost durability
    // rather than like a test recomputing a name. Enumerating the directory asserts the
    // property directly: one WAL per session, each holding exactly one session's sentinels.
    let wal_files: Vec<PathBuf> = std::fs::read_dir(&wal_dir)
        .expect("read the module WAL directory")
        .filter_map(|entry| {
            let path = entry.expect("read WAL directory entry").path();
            path.extension()
                .is_some_and(|ext| ext == "wal")
                .then_some(path)
        })
        .collect();
    assert_eq!(
        wal_files.len(),
        slots.len(),
        "five sessions produced five WALs, found {wal_files:?}"
    );

    let contents: Vec<String> = wal_files
        .iter()
        .map(|path| {
            let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
            String::from_utf8_lossy(&bytes).into_owned()
        })
        .collect();

    for slot in slots.iter() {
        let holders = contents
            .iter()
            .filter(|text| text.contains(&slot.tool_result))
            .count();
        assert_eq!(
            holders, 1,
            "{}'s tool-result sentinel appears in exactly one WAL, found {holders}",
            slot.session
        );
    }

    // The isolation property itself: no single WAL may carry sentinels belonging to two
    // different sessions, whichever file each session turned out to own.
    for (index, text) in contents.iter().enumerate() {
        let owners: Vec<&str> = slots
            .iter()
            .filter(|slot| text.contains(&slot.tool_result) || text.contains(&slot.final_text))
            .map(|slot| slot.session.as_str())
            .collect();
        assert_eq!(
            owners.len(),
            1,
            "WAL {index} carries sentinels from {owners:?}, expected exactly one session"
        );
    }
}

/// The same overlap, but with every session dispatching to ONE tool module -- the deployed
/// shape, where no tool names a module and they all fall to the default.
///
/// This is the arrangement in which the route map and the dispatch fence actually contend: five
/// sessions resolving the same module id at the same time, so an entry keyed on the module
/// alone would be shared rather than merely coexisting. The sibling test above cannot show
/// that, because giving each session its own module keeps them apart however the key is built.
///
/// The trade is that one stub serves one fixed tool result, so tool-result sentinels cannot
/// distinguish sessions here. Isolation is asserted on the final text instead, which still
/// travels the full path: model response, through the loop, out over that session's stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_sessions_sharing_one_tool_module_are_isolated() {
    let slots = vec![
        MixedSlot::new("anthropic", 10, "anthropic/claude-opus-5"),
        MixedSlot::new("anthropic", 11, "anthropic/claude-opus-5"),
        MixedSlot::new("openai-chat", 12, "deepseek/deepseek-v4-flash"),
        MixedSlot::new("openai-chat", 13, "deepseek/deepseek-v4-flash"),
        MixedSlot::new("gemini", 14, "google/gemini-2.5-flash"),
    ];

    let finals = slots
        .iter()
        .map(|slot| slot.final_text.as_str())
        .collect::<HashSet<_>>();
    assert_eq!(
        finals.len(),
        slots.len(),
        "final-text sentinels must be unique or the exclusion checks prove nothing"
    );

    let fixtures = mixed_family_fixtures(&slots);
    let Some(aimock) = AiMock::start("shared-tool-module", &fixtures).await else {
        return;
    };
    // One tool module for every session: the rig points BROCA_TOOL_MODULE at it, and no tool
    // carries a module hint, so all five dispatches resolve the same target.
    let Some(daemon) = Daemon::start_with_aft_modules(
        "shared-tool-module",
        &aimock.base_url,
        &[("aft", "SHARED-TOOL-RESULT")],
    )
    .await
    else {
        return;
    };

    let project_root = daemon.dir.join("shared-project");
    std::fs::create_dir_all(&project_root).unwrap();
    let project_root = std::fs::canonicalize(project_root).unwrap();
    let probe = Arc::new(ConcurrencyProbe::new(slots.len()));

    let (r0, r1, r2, r3, r4) = tokio::join!(
        run_shared_module_client(&daemon, &project_root, &slots[0], Arc::clone(&probe)),
        run_shared_module_client(&daemon, &project_root, &slots[1], Arc::clone(&probe)),
        run_shared_module_client(&daemon, &project_root, &slots[2], Arc::clone(&probe)),
        run_shared_module_client(&daemon, &project_root, &slots[3], Arc::clone(&probe)),
        run_shared_module_client(&daemon, &project_root, &slots[4], Arc::clone(&probe)),
    );
    let runs = [r0, r1, r2, r3, r4];

    assert_eq!(
        probe.admitted.load(Ordering::SeqCst),
        slots.len(),
        "all five sessions reached the admitted barrier while sharing one tool module"
    );
    assert!(
        !probe.finished_before_all_admitted.load(Ordering::SeqCst),
        "every session must be admitted before any session finishes"
    );

    for (index, (slot, run)) in slots.iter().zip(runs.iter()).enumerate() {
        assert_eq!(run.exit_code, 0, "slot {index} exited cleanly");
        assert!(
            run.saw("TOOL_CALL") && run.saw("name=get_time"),
            "{} dispatched through the shared module",
            slot.session
        );
        assert!(
            run.saw("SHARED-TOOL-RESULT"),
            "{} received the shared module's result",
            slot.session
        );
        assert!(
            run.saw(&slot.final_text),
            "{} saw its own final text",
            slot.session
        );
        for (other_index, other) in slots.iter().enumerate() {
            if other_index == index {
                continue;
            }
            assert!(
                !run.saw(&other.final_text),
                "{} saw foreign final text {} while sharing a tool module",
                slot.session,
                other.final_text
            );
        }
    }
}

/// A model script for a run that needs no tools: it answers directly.
const NO_TOOL_FIXTURES: &str = r#"{
  "fixtures": [
    { "match": {},
      "response": { "content": "ONESHOT-ANSWER-NO-TOOLS." } }
  ]
}"#;

/// A TOOL-LESS send must complete even when the tool module rejects every bind. The
/// executor only warms the tool route for runs that carry tools, so a rejecting (or absent,
/// or misconfigured) tool module cannot stall a run that never needed it. Before this held,
/// an empty-tools send blocked behind a 30s warm-bind retry loop against the rejecting
/// module and the client timed out — the tool plane's availability must never gate a run
/// with no tools to dispatch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_tools_send_completes_when_tool_module_rejects_binds() {
    let Some(aimock) = AiMock::start("no-tools-reject", NO_TOOL_FIXTURES).await else {
        return;
    };
    let Some(daemon) = Daemon::start_with_aft_env(
        "no-tools-reject",
        &aimock.base_url,
        &[("FAKE_AFT_REJECT_ATTACH", "1")],
    )
    .await
    else {
        return;
    };

    let project_root = daemon.dir.join("project");
    std::fs::create_dir_all(&project_root).unwrap();

    let started = Instant::now();
    let run = run_client(
        &daemon,
        &project_root,
        "s-no-tools",
        &[
            "run",
            "--prompt",
            "Answer directly, no tools.",
            "--model",
            "mock/mock-model",
            "--tools",
            "[]",
        ],
    )
    .await;
    let elapsed = started.elapsed();

    assert!(
        run.saw("RUN_FINISHED") && run.saw("reason=Completed"),
        "a tool-less run completes even with a bind-rejecting tool module"
    );
    assert!(
        run.saw("ONESHOT-ANSWER-NO-TOOLS"),
        "the model text came back"
    );
    assert_eq!(run.exit_code, 0);
    // The stall this pins against was the full 30s warm-bind window; a healthy tool-less
    // run finishes in low single-digit seconds even on a loaded CI box.
    assert!(
        elapsed < Duration::from_secs(15),
        "tool-less send must not wait out the tool-module bind window (took {elapsed:?})"
    );
}

/// A missing credentials module degrades health without preventing broca from registering.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vault_dependency_is_resolved_after_registering() {
    let Some(daemon) = Daemon::start_with_broca_env(
        "vault-register-first",
        "",
        &[("BROCA_AUTH_SOURCE", "vault")],
    )
    .await
    else {
        return;
    };

    let health = daemon.broca_health().await;
    assert_eq!(
        health.status,
        subc_protocol::session::HealthStatus::Degraded
    );
    let detail = health
        .detail
        .as_deref()
        .expect("degraded health has detail");
    assert!(detail.contains("vault: credentials module not catalog-live"));
    assert!(
        detail.contains("no in-memory fusiform catalog")
            && detail.contains("no last-known-good catalog"),
        "a rig with neither fusiform nor an LKG reports both missing catalog sources: {detail}"
    );
    assert_eq!(
        health.metrics.unwrap()["vault"],
        serde_json::json!("disconnected")
    );

    let project_root = daemon.dir.join("vault-project");
    std::fs::create_dir_all(&project_root).unwrap();
    let run = timeout(
        Duration::from_secs(40),
        run_client(
            &daemon,
            &project_root,
            "vault-missing",
            &[
                "run",
                "--prompt",
                "This request needs provider authentication.",
                "--model",
                "openai/gpt-4o-mini",
                "--tools",
                "[]",
            ],
        ),
    )
    .await
    .expect("the bounded per-call vault readiness wait must terminate");

    assert_ne!(run.exit_code, 0);
    assert!(
        run.saw("transient"),
        "admission returns the typed transient error"
    );
    assert!(
        run.saw("credentials service unreachable"),
        "the admission error names the unavailable credential service"
    );
    assert!(
        !run.saw("SEND state=active"),
        "failed vault resolution does not admit a ghost run"
    );
}

/// A dead credential pauses the run durably, through the real vault, with no provider involved.
///
/// Every other case here drives aimock, and the mock path returns `AuthMode::None` before any
/// credential is consulted -- so nothing in this file reached the vault until now. This one
/// passes an empty provider base url to take the real credential path, and the pause happens
/// BEFORE the first provider call, which is what lets it run without a model at all.
///
/// The assertion is the DURABLE pause, not the wire line: a pause the client reports but the
/// WAL does not hold is not resumable, and resumability is the whole reason for pausing rather
/// than failing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_credential_pauses_the_run_durably() {
    let Some(daemon) = required_vault_daemon(
        Daemon::start_with_dead_vault_credential("vault-dead-cred", "apikey:openai").await,
    ) else {
        return;
    };

    let project_root = daemon.dir.join("vault-pause-project");
    std::fs::create_dir_all(&project_root).unwrap();

    let run = timeout(
        Duration::from_secs(60),
        run_client(
            &daemon,
            &project_root,
            "dead-cred",
            &[
                "run",
                "--prompt",
                "This needs a credential the vault has marked dead.",
                "--model",
                "openai/gpt-4o-mini",
                "--tools",
                "[]",
            ],
        ),
    )
    .await
    .expect("a paused run must terminate the client, not hang it");

    // Distinguish the seed being wrong from the engine being wrong. A revoked handle (the
    // mint-then-invalidate ordering) fails admission with "credential handle not found", which
    // is a rig defect wearing an engine defect's clothes.
    assert!(
        !run.saw("credential handle not found"),
        "the seed produced a REVOKED handle, not a dead credential with a live handle -- \
         check the invalidate/mint ordering in seed_dead_credential: {:?}",
        run.lines
    );
    assert!(
        run.saw("RUN_PAUSED"),
        "a dead credential pauses rather than failing: {:?}",
        run.lines
    );

    let wal_dir = daemon.module_wal_dir();
    let paused_on_disk = std::fs::read_dir(&wal_dir)
        .expect("module wal dir")
        .filter_map(|e| std::fs::read(e.ok()?.path()).ok())
        .any(|bytes| {
            let text = String::from_utf8_lossy(&bytes);
            text.contains("run_paused") && text.contains("auth_required")
        });
    assert!(
        paused_on_disk,
        "the pause must be durable in the WAL, not only reported on the wire"
    );
}

#[cfg(unix)]
const VAULT_REJECTION_FIXTURES: &str = r#"{
  "fixtures": [
    {
      "match": {},
      "response": {
        "error": {
          "message": "Invalid API key",
          "type": "invalid_request_error",
          "code": "invalid_api_key"
        },
        "status": 401
      }
    }
  ]
}"#;

/// A provider 401 evicts the CURRENT real-vault credential from the process cache.
///
/// After the first run, the vault is made unreachable while broca stays alive. With a correctly
/// evicted entry, a second fresh run has no last-known-good token and fails admission transiently.
/// If the rejected entry survived, connectivity fallback would serve it and aimock would produce
/// another durable auth pause. Those run outcomes expose cache state without inspecting logs.
///
/// This deliberately covers only the non-stale case: the failed run carries the version currently
/// served by the real vault. A stale race requires run A to retain version N while a rotation caches
/// N+1 before A's late 401, and that interleave cannot be scheduled deterministically from outside
/// the process. The version-mismatch arm therefore remains unit-covered.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_provider_401_evicts_the_current_real_vault_credential_from_process_cache() {
    let aimock = AiMock::start_on_port("vault-provider-401", VAULT_REJECTION_FIXTURES, 1337).await;
    if aimock.is_none() && vault_required() {
        panic!(
            "single_turn_module_e2e: aimock did not start on the atomic-chat catalog port, but \
             this run is REQUIRED by BROCA_REQUIRE_VAULT"
        );
    }
    let Some(_aimock) = aimock else {
        return;
    };
    let Some(daemon) = required_vault_daemon(
        Daemon::start_with_live_vault_credential("vault-provider-401", "apikey:atomic-chat").await,
    ) else {
        return;
    };

    let project_root = daemon.dir.join("vault-provider-401-project");
    std::fs::create_dir_all(&project_root).unwrap();
    let run_args = [
        "run",
        "--prompt",
        "Use the live vault credential that the provider will reject.",
        "--model",
        "atomic-chat/cache-eviction-probe",
        "--tools",
        "[]",
    ];

    let rejected = timeout(
        Duration::from_secs(60),
        run_client(
            &daemon,
            &project_root,
            "vault-provider-rejection",
            &run_args,
        ),
    )
    .await
    .expect("the provider-rejected run must terminate");
    assert!(
        rejected.saw("RUN_PAUSED") && rejected.saw("Invalid API key"),
        "the live vault token must reach aimock and receive its 401: {:?}",
        rejected.lines
    );
    assert!(
        daemon.wait_for_vault_needs_reauth().await,
        "the vault must apply the first run's provider-auth-failure report before the outage"
    );

    // Keep only claustrum unavailable across supervisor respawns. Broca must remain alive because
    // its process-local cache is the state this arm observes.
    assert!(
        daemon.make_vault_unavailable().await,
        "the rig must actually stop the supervised vault before testing cache fallback"
    );

    let uncached_result = timeout(
        Duration::from_secs(60),
        run_client(
            &daemon,
            &project_root,
            "vault-provider-rejection-after-outage",
            &run_args,
        ),
    )
    .await;
    let uncached = uncached_result.expect("vault-unavailable admission must terminate");

    assert_ne!(uncached.exit_code, 0);
    assert!(
        uncached.saw("transient") && uncached.saw("credentials service unreachable"),
        "evicting the current served version leaves no token for connectivity fallback: {:?}",
        uncached.lines
    );
    assert!(
        !uncached.saw("SEND state=active") && !uncached.saw("RUN_PAUSED"),
        "an evicted credential fails admission; a surviving cache entry would reach aimock and \
         pause again: {:?}",
        uncached.lines
    );
}

/// A repaired credential RESUMES the paused episode instead of starting a second one.
///
/// The pause test proves admission stops; nothing proved it can start again. The distinction is
/// invisible on the wire -- both a resume and a fresh run report `state=active` -- so the
/// assertion is on durable structure: a resumed lineage carries exactly ONE `run_started`,
/// because `ResumeAfterAuth` continues the paused episode under its original run id rather than
/// opening a new one. A second `run_started` would mean the repaired credential silently
/// abandoned the paused work and began again, losing whatever the first episode had committed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_repaired_credential_resumes_the_paused_episode() {
    let Some(daemon) = required_vault_daemon(
        Daemon::start_with_dead_vault_credential("vault-resume", "apikey:openai").await,
    ) else {
        return;
    };

    let project_root = daemon.dir.join("vault-resume-project");
    std::fs::create_dir_all(&project_root).unwrap();
    let send_args = [
        "send",
        "--prompt",
        "Pause, then resume once the credential is repaired.",
        "--model",
        "openai/gpt-4o-mini",
        "--tools",
        "[]",
        "--send-id",
        "resume-probe",
    ];

    let first = timeout(
        Duration::from_secs(60),
        run_client(&daemon, &project_root, "resume", &send_args),
    )
    .await
    .expect("the first send must terminate");
    assert!(
        !first.saw("credential handle not found"),
        "seed produced a revoked handle rather than a dead credential: {:?}",
        first.lines
    );

    // Wait for the pause to be DURABLE before repairing. Repairing while the run is still
    // admitting would leave the test unable to say which state it resumed from.
    let wal_dir = daemon.module_wal_dir();
    let paused = wait_for_wal_marker(&wal_dir, &["run_paused", "auth_required"]).await;
    assert!(
        paused,
        "the first send must pause durably before the repair"
    );

    daemon.repair_vault_credential("apikey:openai");

    let second = timeout(
        Duration::from_secs(60),
        run_client(&daemon, &project_root, "resume", &send_args),
    )
    .await
    .expect("the second send must terminate");
    assert!(
        !second.saw("credential needs re-authentication"),
        "the repaired credential must serve the resume: {:?}",
        second.lines
    );

    // The discriminating observation. `run_started` alone does NOT vary: a run that never
    // resumes still has exactly one, because the paused episode already wrote it. What
    // separates a resumed episode from a still-paused one is that the resumed run RE-ENTERS
    // the loop -- so the durable tell is a step opening after the pause.
    let stepped = wait_for_wal_marker(&wal_dir, &["step_started"]).await;
    assert!(
        stepped,
        "the repaired credential must resume the paused episode into a model step; no \
         step_started means the run stayed paused"
    );

    // And it must be the SAME episode. A second run_started would mean the repair abandoned
    // the paused work and began again, losing whatever the first episode had committed.
    let starts: usize = std::fs::read_dir(&wal_dir)
        .expect("module wal dir")
        .filter_map(|e| std::fs::read(e.ok()?.path()).ok())
        .map(|bytes| {
            String::from_utf8_lossy(&bytes)
                .matches("\"run_started\"")
                .count()
        })
        .sum();
    assert_eq!(
        starts, 1,
        "a resumed lineage carries one run_started; {starts} means the repair started a new \
         episode instead of continuing the paused one"
    );
}

/// Handles do not matter to the daemon: it reads by grant, so a credential invalidated after a
/// handle was minted (which revokes that handle) still pauses the run for re-authentication.
///
/// While broca read by handle, this seed order left it holding a revoked handle, and admission
/// failed with "credential handle not found" instead of pausing; this test pinned that. Now the
/// same seed must reach the resumable pause like any other dead credential, which is what shows
/// the daemon no longer consults a handle at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_revoked_handle_does_not_change_a_grant_read_of_a_dead_credential() {
    let Some(daemon) = Daemon::start_with_vault(
        "vault-wrong-order",
        "apikey:openai",
        VaultSeedOrder::MintThenInvalidate,
        None,
    )
    .await
    else {
        return;
    };

    let project_root = daemon.dir.join("vault-order-project");
    std::fs::create_dir_all(&project_root).unwrap();

    let run = timeout(
        Duration::from_secs(60),
        run_client(
            &daemon,
            &project_root,
            "wrong-order",
            &[
                "run",
                "--prompt",
                "The handle for this credential was revoked by the seed; the daemon reads by grant.",
                "--model",
                "openai/gpt-4o-mini",
                "--tools",
                "[]",
            ],
        ),
    )
    .await
    .expect("admission must terminate the client");

    assert!(
        !run.saw("credential handle not found") && !run.saw("not_found"),
        "a grant read never sees the revoked handle: {:?}",
        run.lines
    );
    assert!(
        run.saw("RUN_PAUSED"),
        "a dead credential read by grant reaches the resumable pause: {:?}",
        run.lines
    );
}

/// The WAL text of the one session named `session`, or empty when it wrote nothing.
fn session_wal_text(wal_dir: &Path, session: &str) -> String {
    let needle = format!("\"session\":\"{session}\"");
    std::fs::read_dir(wal_dir)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| std::fs::read(entry.ok()?.path()).ok())
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
        .find(|text| text.contains(&needle))
        .unwrap_or_default()
}

/// Poll `run.status` until the session reports idle. A client exits on RUN_FINISHED, a moment
/// before the coordinator retires the run; a send in that moment is QUEUED, and its admission
/// (and any refusal) happens at drain rather than in the send reply this test reads.
async fn wait_until_session_idle(daemon: &Daemon, project_root: &Path, session: &str) {
    broca_test_support::wait_until(&format!("session {session} becomes idle"), || async {
        let status = run_client(daemon, project_root, session, &["status"]).await;
        status
            .saw("STATUS Idle")
            .then_some(())
            .ok_or_else(|| format!("status lines={:?}", status.lines))
    })
    .await;
}

/// Context management is a per-session choice frozen at the session's first episode.
///
/// `{"transform": ...}` named the whole-array prompt transform, which broca no longer runs: a
/// send naming one is refused by name before anything is written, never run as `"none"`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn context_management_is_chosen_per_session_and_frozen_at_first_admission() {
    let Some(aimock) = AiMock::start("context-management", NO_TOOL_FIXTURES).await else {
        return;
    };
    let Some(daemon) = Daemon::start("context-management", &aimock.base_url).await else {
        return;
    };
    let project_root = daemon.dir.join("context-management-project");
    std::fs::create_dir_all(&project_root).unwrap();
    let turn = |session: &'static str, extra: &'static [&'static str]| {
        let daemon = &daemon;
        let project_root = &project_root;
        async move {
            let mut args = vec![
                "run",
                "--prompt",
                "Answer without tools.",
                "--model",
                "mock/mock-model",
                "--tools",
                "[]",
            ];
            args.extend_from_slice(extra);
            timeout(
                Duration::from_secs(30),
                run_client(daemon, project_root, session, &args),
            )
            .await
            .expect("every send must terminate the client")
        }
    };

    let none_first = turn("cm-none", &["--context-management", "none"]).await;
    assert!(
        none_first.saw("ONESHOT-ANSWER-NO-TOOLS"),
        "a none session renders straight to the provider: {:?}",
        none_first.lines
    );

    // A new session that names nothing takes the process default, which is "none" with no
    // broca.jsonc section.
    let defaulted = turn("cm-default", &[]).await;
    assert!(
        defaulted.saw("ONESHOT-ANSWER-NO-TOOLS"),
        "an unnamed session runs unmanaged: {:?}",
        defaulted.lines
    );

    // The none session's second send omits the field and inherits "none".
    wait_until_session_idle(&daemon, &project_root, "cm-none").await;
    let none_inherited = turn("cm-none", &[]).await;
    assert!(
        none_inherited.saw("ONESHOT-ANSWER-NO-TOOLS"),
        "an omitted field inherits the frozen none: {:?}",
        none_inherited.lines
    );

    // A different value on the bound session is refused, naming both, and nothing runs.
    wait_until_session_idle(&daemon, &project_root, "cm-none").await;
    let changed = turn(
        "cm-none",
        &["--context-management", r#"{"transform":"magic-context"}"#],
    )
    .await;
    assert!(
        changed.saw("context_management_changed")
            && changed.saw("fixed at \"none\"")
            && changed.saw(r#"asked for {"transform": "magic-context"}"#)
            && !changed.saw("SEND state=active"),
        "a changed value is refused at admission: {:?}",
        changed.lines
    );

    // "compaction" is refused at admission, never run as "none".
    let compaction = turn("cm-compaction", &["--context-management", "compaction"]).await;
    assert!(
        compaction.saw("context_management_unavailable")
            && compaction.saw("not available yet")
            && !compaction.saw("SEND state=active")
            && !compaction.saw("ONESHOT-ANSWER-NO-TOOLS"),
        "compaction is refused: {:?}",
        compaction.lines
    );

    // A new session naming a transform is refused by name, never run as "none".
    let transform = turn(
        "cm-transform",
        &["--context-management", r#"{"transform":"magic-context"}"#],
    )
    .await;
    assert!(
        transform.saw("context_management_unavailable")
            && transform.saw("no longer runs whole-array prompt transforms")
            && transform.saw("'magic-context'")
            && !transform.saw("SEND state=active")
            && !transform.saw("ONESHOT-ANSWER-NO-TOOLS"),
        "a transform is refused at admission: {:?}",
        transform.lines
    );

    // Durable proof: the none session froze "none" on BOTH admitted episodes (the inherited
    // one included) and the refused send wrote nothing; the refused sessions wrote no episode.
    let wal_dir = daemon.module_wal_dir();
    let none_wal = session_wal_text(&wal_dir, "cm-none");
    assert_eq!(none_wal.matches("\"type\":\"run_started\"").count(), 2);
    assert_eq!(
        none_wal.matches("\"context_management\":\"none\"").count(),
        2,
        "each admitted episode freezes the session's value"
    );
    assert!(session_wal_text(&wal_dir, "cm-default").contains(r#""context_management":"none""#));
    for refused in ["cm-compaction", "cm-transform"] {
        assert!(
            !session_wal_text(&wal_dir, refused).contains("\"type\":\"run_started\""),
            "{refused} was refused before RunStarted"
        );
    }
}

/// A child that exits at once, for standing in as a supervisor that died.
///
/// Spelled per-platform because the rest of this rig spawns real product binaries and never
/// needed a bare process before. `/usr/bin/true` does not exist on Windows, which CI caught.
fn exits_immediately() -> tokio::process::Command {
    if cfg!(windows) {
        let mut c = tokio::process::Command::new("cmd");
        c.args(["/C", "exit", "0"]);
        c
    } else {
        tokio::process::Command::new("/usr/bin/true")
    }
}

/// A child that outlives the assertion, for standing in as a live supervisor.
fn stays_running() -> tokio::process::Command {
    if cfg!(windows) {
        let mut c = tokio::process::Command::new("ping");
        c.args(["-n", "31", "127.0.0.1"]);
        c
    } else {
        let mut c = tokio::process::Command::new("/bin/sleep");
        c.arg("30");
        c
    }
}

/// The readiness diagnosis must SEPARATE a dead supervisor from a supervisor that is alive
/// without its module, because those send an investigator to opposite places.
///
/// The supervisor-exited arm is unreachable from the rig: the connection-file gate runs first,
/// so a supervisor that dies early fails there instead, and one that dies in the window between
/// publishing its file and the module registering is a race I cannot schedule. Driving the
/// method directly is what makes the arm provable at all -- otherwise it would ship as an
/// untested branch whose text nobody has ever read.
#[tokio::test]
async fn the_readiness_diagnosis_separates_a_dead_supervisor_from_a_missing_module() {
    let dir = broca_test_support::scratch_root().join(format!("broca-diag-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("rig dir");

    // A process that exits immediately, standing in for a supervisor that died.
    let mut dead = exits_immediately().spawn().expect("spawn");
    dead.wait().await.expect("reap");
    let mut daemon = Daemon {
        child: RefCell::new(broca_test_support::RigChild::new(dead, &dir, "daemon")),
        dir: dir.clone(),
        connection_file: dir.join("subc-connection.json"),
    };
    let msg = daemon.readiness_diagnosis("broca");
    assert!(
        msg.contains("supervisor exited"),
        "a reaped supervisor must be named as the failure, got: {msg}"
    );

    // A process still running, standing in for a live supervisor whose module never appeared.
    let alive = stays_running().kill_on_drop(true).spawn().expect("spawn");
    let mut daemon = Daemon {
        child: RefCell::new(broca_test_support::RigChild::new(alive, &dir, "daemon")),
        dir,
        connection_file: std::path::PathBuf::from("/nonexistent"),
    };
    let msg = daemon.readiness_diagnosis("broca");
    assert!(
        msg.contains("supervisor stayed up"),
        "a live supervisor must be reported as still up, got: {msg}"
    );
    // The load caveat rides on this arm specifically: it is the one that fires on an unmodified
    // tree when the host is busy, and without it the message reads as a verdict on the diff.
    assert!(
        msg.contains("still starting when the deadline expired"),
        "the timeout cause must stay named, got: {msg}"
    );
    daemon.child.get_mut().kill().await.ok();
}

// The shape you never probe is the shape you will assert is impossible. These strict,
// test-only mirrors audit live claustrum replies; production vault.rs remains forward-compatible.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct StrictVaultSuccess {
    payload: Vec<u8>,
    expires_at_ms: Option<i64>,
    record_version: u64,
    project_id: Option<String>,
    account_id: Option<String>,
    credential_id: Option<String>,
}

// Refusals deliberately omit identity/version metadata to hide whether an address exists
// but belongs to somebody else. Do not widen this mirror with the success-only fields.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictVaultRefusal {
    error: StrictVaultReadError,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictVaultReadError {
    code: String,
    // Production maps code to VaultError; the producer's class is redundant but audited here.
    class: String,
}

fn assert_vault_shape<T: serde::de::DeserializeOwned>(body: &serde_json::Value) -> T {
    serde_json::from_value(body.clone()).unwrap_or_else(|error| {
        panic!("the vault's reply carries a field this decoder does not read, or changed a known field: {error}; update broca-subc/src/vault.rs and its strict shape mirror")
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_vault_reply_shapes_match_the_decoder() {
    let Some(mut daemon) = required_vault_daemon(
        Daemon::start_with_vault(
            "vault-wire-shapes",
            "apikey:wire-probe",
            VaultSeedOrder::PutThenMint,
            None,
        )
        .await,
    ) else {
        return;
    };
    assert!(
        daemon.wait_for_module_live("claustrum").await,
        "vault must be ready for wire probes: {}; seeded by {:?}, served by {:?}. \
         If stderr reports different store/binary schemas, these artifacts are from different builds: {:?}",
        daemon.readiness_diagnosis("claustrum"),
        ck_auth_bin(),
        claustrum_bin(),
        std::fs::read_to_string(daemon.dir.join("runtime/data-home/cortexkit/run/logs/claustrum.stderr.log")),
    );
    let handles: serde_json::Value =
        serde_json::from_slice(&std::fs::read(daemon.dir.join("vault-handles.json")).unwrap())
            .unwrap();
    let handle = handles["handles"]["apikey:wire-probe"].as_str().unwrap();
    let conn = broca_subc::SubcConnection::connect(&daemon.connection_file)
        .await
        .unwrap();
    let session = broca_protocol::BindIdentity::new(&daemon.dir, "probe", "wire-shapes");
    let route = conn
        .route_open_management("claustrum", &session)
        .await
        .unwrap();
    for address in [
        handle,
        "ckh_0000000000000000000000000000000000000000000000000000000000000000",
    ] {
        let reply = timeout(
            Duration::from_secs(10),
            conn.route_request(
                &route,
                serde_json::to_vec(&json!({
                    "method": "credential.get", "params": {"handle": address}
                }))
                .unwrap(),
            ),
        )
        .await
        .expect("live credential.get probe must answer")
        .unwrap();
        let body = reply.get("result").expect("credential.get result envelope");
        assert!(
            body.as_object().is_some_and(|fields| !fields.is_empty()),
            "live reply must carry a non-empty field set"
        );
        if address == handle {
            assert_eq!(body["credential_id"], "apikey:wire-probe");
            assert!(body["payload"]
                .as_array()
                .is_some_and(|bytes| !bytes.is_empty()));
            assert!(body["record_version"].as_u64().is_some());
            let _: StrictVaultSuccess = assert_vault_shape(body);
            let mut future = body.clone();
            future["future_vault_field"] = json!(true);
            assert!(
                serde_json::from_value::<StrictVaultSuccess>(future).is_err(),
                "success mirror must reject unknown fields"
            );
        } else {
            assert_eq!(
                body["error"]["code"], "not_found",
                "probe must reach the non-resolving refusal shape"
            );
            let refusal: StrictVaultRefusal = assert_vault_shape(body);
            assert_eq!(refusal.error.code, "not_found");
            assert_eq!(refusal.error.class, "permanent");
            let mut future = body.clone();
            future["credential_id"] = json!("must-not-leak");
            assert!(
                serde_json::from_value::<StrictVaultRefusal>(future).is_err(),
                "refusal mirror must reject identity leakage"
            );
            let mut future_error = body.clone();
            future_error["error"]["future_vault_field"] = json!(true);
            assert!(
                serde_json::from_value::<StrictVaultRefusal>(future_error).is_err(),
                "refusal error mirror must reject unknown fields"
            );
        }
    }

    // credential.get_scoped, in the exact shape broca sends it. This connection belongs to the
    // test process, so it opens routes as a `Direct` principal that no grant can cover, and the
    // vault must refuse a credential that EXISTS with the same not_found it gives an unknown id.
    // That is the refusal broca's scoped-read message is written for, and the probe proves the
    // vault accepts these request bytes (a params mismatch would come back as an Error frame or
    // invalid_params, not not_found). The success reply is the same result type as
    // credential.get's, which the handle probe above pins; a test process cannot reach it here,
    // because a Reserved principal needs a launch nonce only the daemon's supervisor can issue.
    let reply = timeout(
        Duration::from_secs(10),
        conn.route_request(
            &route,
            serde_json::to_vec(&json!({
                "method": "credential.get_scoped",
                "params": {"credential_id": "apikey:wire-probe", "min_ttl_ms": 600_000}
            }))
            .unwrap(),
        ),
    )
    .await
    .expect("live credential.get_scoped probe must answer")
    .unwrap();
    let body = reply
        .get("result")
        .expect("credential.get_scoped result envelope");
    let refusal: StrictVaultRefusal = assert_vault_shape(body);
    assert_eq!(
        refusal.error.code, "not_found",
        "an ungranted principal must get not_found, whether or not the credential exists"
    );
    assert_eq!(refusal.error.class, "permanent");
}
