//! Broca reads its launch nonce through `subc_os::launch_nonce()` and nowhere else.
//!
//! The daemon hands each module it spawns a secret launch nonce, which the module presents in
//! HELLO and on every route it opens. The environment copy (`SUBC_LAUNCH_NONCE`) is readable by
//! any process of the same user, so the daemon now also hands it over as an inherited pipe named
//! by `SUBC_LAUNCH_NONCE_FD`. That pipe can be read once: the accessor reads it, closes it and
//! caches the answer for the life of the process. Broca reads the nonce at two different times,
//! HELLO at boot and every route open for as long as the process lives, so both must ask the
//! accessor, and neither may read the environment itself.
//!
//! THE ACCESSOR'S CACHE IS PROCESS-GLOBAL: the first call in a process decides the answer for
//! every later one. So each scenario below runs in a fresh copy of this test binary. The test
//! re-executes itself filtered to its own name, with the environment and descriptor that
//! scenario needs, and the child copy (recognising itself by `ROLE_ENV`) runs the assertions.
//! Descriptors are handed over with `subc_os::LaunchNonceHandoff`, the same code the daemon
//! spawns modules with, so the child sees exactly what a daemon-spawned broca sees.

#[cfg(unix)]
use std::ffi::OsString;
use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::Arc;

use async_trait::async_trait;
use broca_core::{FileLeaseStore, FileWalStore, OutputEvent, RunError, RunSummary};
use broca_module::executor::{RunExecutor, RunJob};
use broca_module::{DefaultOpener, LineageRegistry};
use subc_protocol::{FrameType, ModuleHelloBody};
use tokio::sync::mpsc;

/// Names the scenario a child copy of this binary is running. Absent in the parent.
const ROLE_ENV: &str = "BROCA_LAUNCH_NONCE_TEST_ROLE";

/// The nonce the tests hand over, and a different one for the environment copy, so a reader that
/// took the wrong source presents a value the assertion can tell apart.
// The pipe handover exists only on unix, so only the unix tests use this.
#[cfg(unix)]
const PIPE_NONCE: &str = "nonce-from-the-pipe-0123456789abcdef";
const ENV_NONCE: &str = "nonce-from-the-environment-fedcba9876543210";

/// True in the child copy running `test_name`.
fn is_child(test_name: &str) -> bool {
    std::env::var(ROLE_ENV).is_ok_and(|role| role == test_name)
}

/// A command that runs `test_name` alone in a fresh copy of this test binary, with every
/// launch-nonce variable the parent might have inherited removed, so the scenario starts from
/// only what `configure` gives it.
fn child_command(test_name: &str) -> Command {
    let mut command = Command::new(std::env::current_exe().expect("the test binary's own path"));
    command
        .args([test_name, "--exact", "--nocapture", "--test-threads=1"])
        .env(ROLE_ENV, test_name)
        .env_remove(subc_os::LAUNCH_NONCE_ENV)
        .env_remove(subc_os::LAUNCH_NONCE_FD_ENV)
        .env_remove(subc_protocol::SUBC_MODULE_ID_ENV);
    command
}

/// Run the child and fail with its output unless it passed. A child that ran no test at all
/// (a filter that matched nothing) also exits 0, so its output must name the test as passed.
fn assert_child_passed(test_name: &str, output: Output) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success() && stdout.contains(&format!("test {test_name} ... ok")),
        "the child copy running {test_name} failed ({:?}).\n--- stdout\n{stdout}\n--- stderr\n{stderr}",
        output.status
    );
}

/// A run executor that is never asked to run anything: these tests stop at HELLO.
struct NoRuns;

#[async_trait]
impl RunExecutor for NoRuns {
    async fn execute(
        &self,
        _job: RunJob,
        _out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        unreachable!("the launch-nonce tests start no runs")
    }
}

fn scratch(tag: &str) -> PathBuf {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-launch-nonce-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir
}

/// Drive broca's real serve path over an in-memory connection until it sends HELLO, and return
/// what the serve task produced: the HELLO body if it sent one, and its result if it ended.
///
/// The peer never acknowledges, so a serve path that sent HELLO keeps waiting and is dropped;
/// one that refused to send HELLO ends at once with its error.
async fn serve_until_hello(
    tag: &str,
) -> (
    Option<ModuleHelloBody>,
    Option<Result<broca_module_serve::ServeEnd, broca_module_serve::ServeIoError>>,
) {
    let dir = scratch(tag);
    let lease = Arc::new(FileLeaseStore::new(dir.join("leases")));
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let registry = Arc::new(LineageRegistry::new(Arc::new(DefaultOpener::new(
        lease,
        wal,
        Arc::new(NoRuns),
    ))));
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (mut client_read, _client_write) = tokio::io::split(client);
    let (server_read, server_write) = tokio::io::split(server);
    let mut serve = tokio::spawn(async move {
        broca_module_serve::serve_connection(
            server_read,
            server_write,
            "broca",
            registry.clone(),
            registry,
            broca_module_serve::serve::VaultGauge::not_in_use(),
        )
        .await
    });
    let deadline = std::time::Duration::from_secs(10);
    let outcome = tokio::select! {
        frame = subc_transport::read_frame(&mut client_read) => {
            let frame = frame.expect("read the first frame").map(|frame| {
                assert_eq!(frame.header.ty, FrameType::Hello, "broca's first frame is HELLO");
                serde_json::from_slice::<ModuleHelloBody>(&frame.body).expect("HELLO body decodes")
            });
            match frame {
                Some(hello) => (Some(hello), None),
                // The connection closed without a frame: the serve task ended first.
                None => (None, Some((&mut serve).await.expect("serve task"))),
            }
        }
        ended = &mut serve => (None, Some(ended.expect("serve task"))),
        () = tokio::time::sleep(deadline) => panic!("broca neither sent HELLO nor ended"),
    };
    serve.abort();
    let _ = std::fs::remove_dir_all(&dir);
    outcome
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build a runtime")
}

/// The inode of whatever this process has open at `fd`, or `None` if nothing is.
#[cfg(unix)]
fn inode_at(fd: i32) -> Option<u64> {
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: fstat writes one `struct stat` into a buffer of exactly that size, and writes
    // nothing when it fails; it only reads the descriptor number, never takes ownership.
    if unsafe { libc::fstat(fd, stat.as_mut_ptr()) } == -1 {
        return None;
    }
    // SAFETY: fstat succeeded, so it filled the buffer.
    // `ino_t` is not 64 bits on every Unix target, so the cast is only a no-op on some.
    #[allow(clippy::unnecessary_cast)]
    Some(unsafe { stat.assume_init() }.st_ino as u64)
}

/// The inode named in a `SUBC_LAUNCH_NONCE_FD` value (`<fd>:<inode>`).
#[cfg(unix)]
fn named_inode() -> u64 {
    let value = std::env::var(subc_os::LAUNCH_NONCE_FD_ENV).expect("the descriptor is named");
    value
        .split_once(':')
        .and_then(|(_, inode)| inode.parse().ok())
        .expect("the variable is <fd>:<inode>")
}

/// WITH ONLY THE DESCRIPTOR NAMED, HELLO CARRIES THE PIPE'S NONCE, AND A ROUTE OPENED AFTER HELLO
/// PRESENTS THE SAME VALUE WITHOUT READING THE DESCRIPTOR AGAIN.
///
/// No environment copy is set, so a HELLO that read the environment would carry nothing, and a
/// route identity that read it would be absent. The route identity is asked for after HELLO has
/// read and closed the pipe; a second read of descriptor 3 would find it closed (or reused) and
/// the identity would be missing, so a present identity with the pipe's value is the cached one.
///
/// Also checked here: the accessor leaves the environment exactly as it found it (a module that
/// removed the variables would break any reader in the process still on the environment copy),
/// and the provenance value broca reports for where it got its nonce says `fd`.
#[cfg(unix)]
#[test]
fn hello_and_a_later_route_open_both_present_the_pipe_nonce() {
    const NAME: &str = "hello_and_a_later_route_open_both_present_the_pipe_nonce";
    if is_child(NAME) {
        let environment_before: Vec<(OsString, OsString)> = std::env::vars_os().collect();
        let pipe_inode = named_inode();
        assert_eq!(
            inode_at(subc_os::LAUNCH_NONCE_FD),
            Some(pipe_inode),
            "the handoff put the pipe at descriptor 3 before anything read it"
        );

        let (hello, ended) = runtime().block_on(serve_until_hello("fd"));
        let hello = hello.unwrap_or_else(|| panic!("broca sent no HELLO: {ended:?}"));
        assert_eq!(hello.launch_nonce.as_deref(), Some(PIPE_NONCE));
        assert_ne!(
            inode_at(subc_os::LAUNCH_NONCE_FD),
            Some(pipe_inode),
            "HELLO's read closed the pipe, so a second reader could not have taken it"
        );

        let identity = broca_subc::connection::supervised_consumer_identity()
            .expect("a route opened after HELLO presents the supervised identity");
        assert_eq!(identity.module_id, "broca");
        assert_eq!(identity.launch_nonce, PIPE_NONCE);

        assert_eq!(
            broca_module_serve::manifest::launch_nonce_source(),
            Some(subc_protocol::manifest::LaunchNonceSource::Fd)
        );
        let environment_after: Vec<(OsString, OsString)> = std::env::vars_os().collect();
        assert_eq!(
            environment_before, environment_after,
            "reading the nonce must leave the process environment unchanged"
        );
        return;
    }

    let mut command = child_command(NAME);
    command.env(subc_protocol::SUBC_MODULE_ID_ENV, "broca");
    let handoff = subc_os::LaunchNonceHandoff::new(PIPE_NONCE).expect("make the nonce pipe");
    command.env(subc_os::LAUNCH_NONCE_FD_ENV, handoff.fd_env_value());
    handoff.install_last(&mut command);
    assert_child_passed(NAME, command.output().expect("run the child copy"));
}

/// With only the environment copy set, HELLO and route opens present it, and provenance reports
/// `env`.
///
/// The fleet's migration census (which reads each running module's HELLO provenance) counts a
/// module as switched to the pipe only when its provenance says `fd`, so a module reporting `fd`
/// for a nonce it took from the environment would be counted done while its nonce is still
/// visible to any same-user process listing environments with `ps eww`.
#[test]
fn the_environment_copy_is_used_and_reported_as_env_when_no_descriptor_is_named() {
    const NAME: &str =
        "the_environment_copy_is_used_and_reported_as_env_when_no_descriptor_is_named";
    if is_child(NAME) {
        let (hello, ended) = runtime().block_on(serve_until_hello("env"));
        let hello = hello.unwrap_or_else(|| panic!("broca sent no HELLO: {ended:?}"));
        assert_eq!(hello.launch_nonce.as_deref(), Some(ENV_NONCE));
        let identity = broca_subc::connection::supervised_consumer_identity()
            .expect("the route presents the supervised identity");
        assert_eq!(identity.launch_nonce, ENV_NONCE);
        assert_eq!(
            broca_module_serve::manifest::launch_nonce_source(),
            Some(subc_protocol::manifest::LaunchNonceSource::Env)
        );
        return;
    }

    let mut command = child_command(NAME);
    command
        .env(subc_protocol::SUBC_MODULE_ID_ENV, "broca")
        .env(subc_os::LAUNCH_NONCE_ENV, ENV_NONCE);
    assert_child_passed(NAME, command.output().expect("run the child copy"));
}

/// A NAMED BUT EMPTY DESCRIPTOR FAILS HELLO WITH THE ACCESSOR'S OWN MESSAGE AND NEVER FALLS BACK TO
/// THE ENVIRONMENT.
///
/// The environment copy is set to a different value, so a fallback anywhere would show: HELLO
/// would be sent carrying it, or the route identity would present it. The pipe is left as it was,
/// unread and open, because an empty pipe tells the reader nothing about whose it is.
#[cfg(unix)]
#[test]
fn a_named_empty_descriptor_fails_hello_and_never_falls_back_to_the_environment() {
    const NAME: &str =
        "a_named_empty_descriptor_fails_hello_and_never_falls_back_to_the_environment";
    if is_child(NAME) {
        let pipe_inode = named_inode();
        let (hello, ended) = runtime().block_on(serve_until_hello("empty"));
        assert!(
            hello.is_none(),
            "broca sent HELLO carrying {:?} from a named empty descriptor",
            hello.and_then(|hello| hello.launch_nonce)
        );
        let error = match ended {
            Some(Err(error)) => error.to_string(),
            other => panic!("the serve path must fail HELLO, got {other:?}"),
        };
        assert!(
            error.contains("the launch nonce pipe at descriptor 3 is empty"),
            "the refusal carries the accessor's own message: {error}"
        );
        assert_eq!(
            broca_subc::connection::supervised_consumer_identity(),
            None,
            "a route must present no identity rather than the environment copy"
        );
        assert_eq!(broca_module_serve::manifest::launch_nonce_source(), None);
        assert_eq!(
            inode_at(subc_os::LAUNCH_NONCE_FD),
            Some(pipe_inode),
            "the empty pipe is left open and untouched"
        );
        return;
    }

    let mut command = child_command(NAME);
    command
        .env(subc_protocol::SUBC_MODULE_ID_ENV, "broca")
        .env(subc_os::LAUNCH_NONCE_ENV, ENV_NONCE);
    let handoff = subc_os::LaunchNonceHandoff::new("").expect("make an empty pipe");
    command.env(subc_os::LAUNCH_NONCE_FD_ENV, handoff.fd_env_value());
    handoff.install_last(&mut command);
    assert_child_passed(NAME, command.output().expect("run the child copy"));
}

/// Run the real `ck-broca` binary as the daemon would, with `configure` deciding what launch
/// nonce it inherits, and return its output.
///
/// Every home it could write under is pinned into a scratch directory, so the log segment it
/// opens before refusing lands there and not in the operator's real broca log. The connection
/// file it is pointed at does not exist: a binary that got past the nonce check would fail on
/// that instead, with a different message.
#[cfg(unix)]
fn run_real_binary(tag: &str, configure: impl FnOnce(&mut Command)) -> Output {
    let dir = scratch(tag);
    let mut command = broca_test_support::ckdev_command(
        env!("CARGO_BIN_EXE_ck-broca"),
        broca_test_support::scratch_root(),
    );
    command
        .arg("--subc")
        .arg(dir.join("no-such-connection-file.json"))
        .env_remove(subc_os::LAUNCH_NONCE_ENV)
        .env_remove(subc_os::LAUNCH_NONCE_FD_ENV)
        .env(subc_protocol::SUBC_MODULE_ID_ENV, "broca")
        .env("HOME", dir.join("home"))
        .env("XDG_DATA_HOME", dir.join("data"))
        .env("XDG_CONFIG_HOME", dir.join("config"))
        .env("XDG_STATE_HOME", dir.join("state"))
        .env("XDG_RUNTIME_DIR", dir.join("runtime"))
        .env("BROCA_STATE_ROOT", dir.join("broca-state"));
    configure(&mut command);
    let output =
        broca_test_support::retry_executable_file_busy(|| command.output()).expect("run ck-broca");
    let _ = std::fs::remove_dir_all(&dir);
    output
}

/// The real binary refuses to start on a named but empty descriptor, with the accessor's own
/// message, even though the environment copy is set.
#[cfg(unix)]
#[test]
fn the_binary_refuses_to_start_on_a_named_empty_descriptor() {
    let output = run_real_binary("binary-empty", |command| {
        command.env(subc_os::LAUNCH_NONCE_ENV, ENV_NONCE);
        let handoff = subc_os::LaunchNonceHandoff::new("").expect("make an empty pipe");
        command.env(subc_os::LAUNCH_NONCE_FD_ENV, handoff.fd_env_value());
        handoff.install_last(command);
    });
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "ck-broca started: {stderr}");
    assert!(
        stderr.contains("launch nonce: the launch nonce pipe at descriptor 3 is empty"),
        "the refusal must carry the accessor's own message: {stderr}"
    );
}

/// The real binary refuses to start when the named descriptor is not open, which is what a
/// process a module spawned sees: it inherits the variable but not the pipe.
#[cfg(unix)]
#[test]
fn the_binary_refuses_to_start_on_a_named_descriptor_that_is_not_open() {
    // A high number rather than 3: whatever ran this test may have left its own descriptor 3
    // open and inheritable, and then the accessor would refuse it as the wrong file instead.
    let handoff = subc_os::LaunchNonceHandoff::new(PIPE_NONCE).expect("make the nonce pipe");
    let inode = handoff
        .fd_env_value()
        .split_once(':')
        .map(|(_, inode)| inode.to_string())
        .expect("the handoff names <fd>:<inode>");
    drop(handoff);
    let output = run_real_binary("binary-closed", |command| {
        command.env(subc_os::LAUNCH_NONCE_ENV, ENV_NONCE);
        command.env(subc_os::LAUNCH_NONCE_FD_ENV, format!("987:{inode}"));
    });
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "ck-broca started: {stderr}");
    assert!(
        stderr
            .contains("launch nonce: SUBC_LAUNCH_NONCE_FD names descriptor 987, which is not open"),
        "the refusal must carry the accessor's own message: {stderr}"
    );
}

/// Every non-comment line in `text` that names a launch-nonce variable, with its line number.
///
/// Comment lines are skipped because prose explaining the variables is not a read of them, and so
/// is a builder line that only removes a variable from a child process's environment
/// (`.env_remove(...)`), which is how test rigs keep a spawned daemon from inheriting it. Any
/// other mention counts, because every way of reading one of them (`env::var`, `var_os`,
/// `vars()` filtered by name, the constants that hold the names) has to name it.
fn nonce_variable_mentions(text: &str) -> Vec<(usize, String)> {
    const NAMES: [&str; 3] = [
        "SUBC_LAUNCH_NONCE",
        "LAUNCH_NONCE_ENV",
        "LAUNCH_NONCE_FD_ENV",
    ];
    text.lines()
        .enumerate()
        .filter(|(_, line)| !line.trim_start().starts_with("//"))
        .filter(|(_, line)| !line.trim_start().starts_with(".env_remove("))
        .filter(|(_, line)| NAMES.iter().any(|name| line.contains(name)))
        .map(|(index, line)| (index + 1, line.trim().to_string()))
        .collect()
}

fn rust_files_under(dir: &std::path::Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read {}: {e}", dir.display())) {
        let path = entry.expect("directory entry").path();
        if path.is_dir() {
            rust_files_under(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

/// NO SHIPPED BROCA CODE READS A LAUNCH-NONCE VARIABLE ITSELF.
///
/// Every reader must go through `subc_os::launch_nonce()`. A second, independent reader of the
/// descriptor would find it already closed, or read and close whatever socket reused its number;
/// a reader of the environment copy keeps working until the daemon stops setting it and then
/// silently loses broca's identity. The corpus is every `src/` tree in the workspace, which is
/// what ships; test directories are outside it, and so is this file.
#[test]
fn no_shipped_code_reads_a_launch_nonce_variable_directly() {
    // The detector must find a direct read, or an empty result means nothing.
    assert_eq!(
        nonce_variable_mentions(
            "// SUBC_LAUNCH_NONCE in prose\n.env_remove(\"SUBC_LAUNCH_NONCE\")\nlet n = std::env::var(\"SUBC_LAUNCH_NONCE\");\n"
        ),
        vec![(
            3,
            "let n = std::env::var(\"SUBC_LAUNCH_NONCE\");".to_string()
        )]
    );

    let crates = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut files = Vec::new();
    for entry in std::fs::read_dir(&crates).expect("read the crates directory") {
        let src = entry.expect("crate entry").path().join("src");
        if src.is_dir() {
            rust_files_under(&src, &mut files);
        }
    }

    // The corpus must contain the files that hold the accessor calls, or a clean result could
    // come from scanning the wrong place.
    let calls: Vec<String> = files
        .iter()
        .filter(|path| {
            std::fs::read_to_string(path).is_ok_and(|text| text.contains("subc_os::launch_nonce()"))
        })
        .map(|path| {
            // Compare as forward-slash paths, so the expected list below holds on Windows too,
            // where `display()` renders the separators as backslashes.
            path.strip_prefix(&crates)
                .unwrap_or(path)
                .components()
                .map(|part| part.as_os_str().to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join("/")
        })
        .collect();
    for expected in [
        "broca-module-serve/src/main.rs",
        "broca-module-serve/src/serve.rs",
        "broca-module-serve/src/manifest.rs",
        "broca-subc/src/connection.rs",
    ] {
        assert!(
            calls.iter().any(|path| path == expected),
            "expected an accessor call in {expected}; the scan found them in {calls:?}"
        );
    }

    let mut direct = Vec::new();
    for path in &files {
        let text = std::fs::read_to_string(path).expect("read a source file");
        for (line, code) in nonce_variable_mentions(&text) {
            direct.push(format!("{}:{line}: {code}", path.display()));
        }
    }
    assert!(
        direct.is_empty(),
        "shipped code names a launch-nonce variable instead of calling \
         subc_os::launch_nonce():\n{}",
        direct.join("\n")
    );
}
