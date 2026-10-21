//! The graceful-stop seal line, read back out of the log the real daemon made broca write.
//!
//! These four lines (`shutdown seal complete...`, `shutdown incomplete...`, and their
//! on-serve-return twins) are the ONLY evidence that the seal fired, and subc greps them at
//! every placement. A record with no component collapses to the bare module root, where
//! `CK_LOG=broca=error,broca.lifecycle=debug` silences it while every targeted record
//! survives -- so "it carries a component" has to be measured, not read off the source.
//!
//! It is measured through the real path rather than a synthetic `warn!`: spawn the actual
//! `ck-subc` daemon, let it spawn and register `ck-broca`, then kill the daemon. broca's
//! `serve` returns -- which IS the graceful-stop notice, the daemon never sends SIGTERM --
//! and the seal arm in `main` runs. Nothing in this test writes a log record itself; it only
//! reads the segment the module wrote under the rig's own data home.
//!
//! UNIX ONLY, because on Windows killing the daemon no longer lets a module seal at all. Since
//! subc c5e2fb60 (2026-09-25, issue #109) every module runs inside a job object with
//! JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, so when the daemon dies the kernel TerminateProcesses
//! broca before its `serve` can return, and no seal line is ever written. That is subc's
//! deliberate trade (containment on a crash over graceful teardown on a Windows stop, recorded
//! at the job field in subc-daemon/src/supervise.rs), not a broca defect. Measured: the first
//! Windows run after it logged broca's three boot records and no seal. REVISIT when subc ships
//! a Windows stop path that drains before exiting; this file should then run there again.
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::process::Child;
use tokio::time::{sleep, Instant};

/// A sibling-checkout subc binary, env-overridable, matching the other rigs here.
fn sibling_bin(name: &str, env_key: &str) -> PathBuf {
    broca_test_support::subc_bin(name, env_key)
}

/// Why a missing daemon binary may not quietly pass.
///
/// A skipped test reports `ok`, and this one would then say nothing about the property it
/// exists for. `scripts/rust-test-gate.sh` exports `BROCA_REQUIRE_SUBC`, so in any gated run
/// the absence is a failure that names itself; a bare local run may still skip, out loud.
fn require_subc() -> bool {
    std::env::var_os("BROCA_REQUIRE_SUBC").is_some()
}

struct Rig {
    child: Child,
    dir: PathBuf,
    connection_file: PathBuf,
    data_home: PathBuf,
}

impl Drop for Rig {
    fn drop(&mut self) {
        broca_test_support::kill_and_reap_tree(&mut self.child);
        broca_test_support::dump_rig_process_logs_on_failure(&self.dir, "daemon");
        broca_test_support::remove_rig_dir(&self.dir);
    }
}

impl Rig {
    /// `name` keeps each test's rig in its own directory. The directory used to be named for the
    /// process id alone, which is shared by every test in this file -- harmless while there was
    /// one test, and the moment a second arrived the two rigs removed each other's config and
    /// logs mid-run, so BOTH failed with "no seal record" while each passed alone.
    async fn start(name: &str) -> Option<Self> {
        let subc_core = sibling_bin("ck-subc", "SUBC_CORE_BIN");
        if !subc_core.exists() {
            assert!(
                !require_subc(),
                "seal_line_is_filterable_e2e: subc-core missing at {subc_core:?}, but this run \
                 is REQUIRED by BROCA_REQUIRE_SUBC"
            );
            println!(
                "SKIP seal_line_is_filterable_e2e: subc-core missing at {subc_core:?}. This \
                 reports `ok` -- set BROCA_REQUIRE_SUBC=1 to make it a failure instead."
            );
            return None;
        }
        let subc_core =
            broca_test_support::ckdev_binary(&subc_core, broca_test_support::scratch_root());

        let dir = broca_test_support::scratch_root()
            .join(format!("broca-seal-line-{name}-{}", std::process::id()));
        broca_test_support::remove_rig_dir(&dir);
        let config_home = dir.join("config");
        let runtime_dir = dir.join("runtime");
        // The module resolves its own log directory from the DATA home. Pinning it under the
        // rig is what keeps this test's records out of the operator's real broca log -- and is
        // also how the test finds them.
        let data_home = runtime_dir.join("data-home");
        let config_path = config_home.join("cortexkit").join("subc.jsonc");
        std::fs::create_dir_all(config_path.parent().unwrap()).expect("create subc config dir");
        std::fs::create_dir_all(&runtime_dir).expect("create runtime dir");

        let config = serde_json::json!({
            "version": 1,
            "modules": {
                "broca": {
                    "program": broca_test_support::ckdev_binary(
                        env!("CARGO_BIN_EXE_ck-broca"),
                        broca_test_support::scratch_root(),
                    ),
                    "args": [],
                    // A base_url override puts every run on the AuthMode::None path, so boot
                    // never opens the credential vault, and keeps the rig from needing a vault
                    // module to reach `serve`. The admission test below does start runs; its
                    // ack fires at the durable RunStarted barrier, before any provider call, so
                    // the unreachable URL fails those runs only after admission has been measured.
                    "env": {"BROCA_PROVIDER_BASE_URL": "http://127.0.0.1:1/unused"},
                    "enabled": true
                }
            }
        });
        std::fs::write(&config_path, serde_json::to_vec_pretty(&config).unwrap())
            .expect("write subc config");

        let connection_file = runtime_dir.join("subc-connection.json");
        let child = broca_test_support::tokio_rig_command(&subc_core, &dir, "daemon")
            .expect("create daemon log files")
            .env("XDG_CONFIG_HOME", &config_home)
            .env("XDG_RUNTIME_DIR", &runtime_dir)
            .env("XDG_DATA_HOME", &data_home)
            // broca's state root defaults to the data home pinned above, but an operator's
            // exported BROCA_STATE_ROOT would override that and send this rig's WAL into their
            // real state. The daemon passes its environment to the module, so drop it here.
            .env_remove("BROCA_STATE_ROOT")
            .env("SUBC_PORT", "0")
            .kill_on_drop(true)
            .spawn()
            .expect("spawn subc-core");

        let rig = Self {
            child,
            dir,
            connection_file,
            data_home,
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        while !rig.connection_file.exists() {
            assert!(
                Instant::now() < deadline,
                "daemon never published its connection file"
            );
            sleep(Duration::from_millis(25)).await;
        }
        assert!(
            rig.wait_for_module_live().await,
            "broca module never became live, so it never reached the seal path"
        );
        Some(rig)
    }

    async fn wait_for_module_live(&self) -> bool {
        let probe = broca_protocol::BindIdentity::new(&self.dir, "probe", "seal-line");
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline {
            if let Ok(conn) = broca_subc::SubcConnection::connect(&self.connection_file).await {
                if conn.route_open_management("broca", &probe).await.is_ok() {
                    return true;
                }
            }
            sleep(Duration::from_millis(50)).await;
        }
        false
    }

    fn logs_dir(&self) -> PathBuf {
        self.data_home.join("cortexkit").join("broca").join("logs")
    }
}

/// The module's own segment, found by listing rather than by rebuilding its name: the file is
/// named for a UTC day and this test must not re-derive that rule.
fn read_segment(logs_dir: &Path) -> String {
    let Ok(entries) = std::fs::read_dir(logs_dir) else {
        return String::new();
    };
    let mut text = String::new();
    for entry in entries.flatten() {
        if let Ok(contents) = std::fs::read_to_string(entry.path()) {
            text.push_str(&contents);
        }
    }
    text
}

/// The live `ck-broca` pids the rig's daemon is supervising, read from the process table.
#[cfg(unix)]
fn broca_children(daemon_pid: u32) -> Vec<u32> {
    let out = std::process::Command::new("pgrep")
        .args(["-P", &daemon_pid.to_string(), "-x", "ckdev-broca"])
        .output()
        .expect("run pgrep");
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|line| line.trim().parse().ok())
        .collect()
}

/// A SIGTERM SEALS, AND THEN THE PROCESS STILL DIES AS A SIGNALLED PROCESS -- SO IT IS RESPAWNED.
///
/// The supervisor reads an unrequested exit 0 as "stopped on purpose" and leaves the module down;
/// a death by signal is a crash, and is restarted. broca's SIGTERM arm used to seal and then
/// `exit(0)`, so an operator's `kill -TERM` left broca stopped until someone started it by hand.
///
/// BOTH HALVES ARE ASSERTED, because each half alone matches one of the two broken binaries: the
/// old one logs the seal line and is never respawned; a handler that re-raised WITHOUT sealing
/// would be respawned and log nothing. Only the fixed binary produces the line AND a new pid.
///
/// The line asserted is the SIGNAL ARM's, distinguished from its twin by the absence of "on serve
/// return" -- otherwise a seal reached some other way would satisfy it.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn a_sigterm_seals_and_the_module_is_respawned_rather_than_left_down() {
    let Some(rig) = Rig::start("sigterm").await else {
        return;
    };
    let daemon_pid = rig.child.id().expect("daemon pid");
    let first = broca_children(daemon_pid);
    assert_eq!(
        first.len(),
        1,
        "expected exactly one supervised ck-broca: {first:?}"
    );
    let old = first[0];

    let status = std::process::Command::new("kill")
        .args(["-TERM", &old.to_string()])
        .status()
        .expect("run kill");
    assert!(status.success(), "kill -TERM {old} failed");

    let logs_dir = rig.logs_dir();
    let deadline = Instant::now() + Duration::from_secs(40);
    let (mut sealed, mut respawned) = (None::<String>, None::<u32>);
    while Instant::now() < deadline && (sealed.is_none() || respawned.is_none()) {
        if sealed.is_none() {
            sealed = read_segment(&logs_dir)
                .lines()
                .find(|l| l.contains("shutdown seal complete") && !l.contains("on serve return"))
                .map(str::to_string);
        }
        if respawned.is_none() {
            respawned = broca_children(daemon_pid)
                .into_iter()
                .find(|pid| *pid != old);
        }
        sleep(Duration::from_millis(200)).await;
    }
    assert!(
        sealed.is_some(),
        "SIGTERM reached broca but the signal arm logged no seal. Segment:\n{}",
        read_segment(&logs_dir)
    );
    assert!(
        respawned.is_some(),
        "broca sealed on SIGTERM and was NOT respawned: it ended as a deliberate stop (exit 0) \
         rather than a signal death, so the supervisor left it down"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_shutdown_seal_line_carries_a_component_so_a_filter_can_reach_it() {
    let Some(mut rig) = Rig::start("component").await else {
        return;
    };

    // KILLING THE DAEMON IS THE TRIGGER. Its teardown never sends SIGTERM to a module -- the
    // socket closing is what makes broca's `serve` return, and the seal hangs off that return.
    rig.child.start_kill().expect("kill the daemon");

    let logs_dir = rig.logs_dir();
    let line = broca_test_support::wait_until("shutdown seal log record", || async {
        let segment = read_segment(&logs_dir);
        if let Some(line) = segment
            .lines()
            .find(|line| line.contains("shutdown seal") || line.contains("shutdown incomplete"))
        {
            return Ok(line.to_string());
        }
        Err(format!(
            "logs_dir={}, tail={}",
            logs_dir.display(),
            segment.lines().rev().take(8).collect::<Vec<_>>().join("\n")
        ))
    })
    .await;

    let record = cortexkit_log::parse_line(&line)
        .unwrap_or_else(|error| panic!("the seal record is not a readable line: {error}: {line}"));
    assert_eq!(
        record.logger, "broca.lifecycle",
        "the seal line must carry a component: on the bare root it cannot be raised by a \
         per-component filter, and this line is the only evidence the seal fired: {line}"
    );
    assert!(
        record.body.starts_with("shutdown"),
        "the message keeps its own opening words, with attribution in the logger name rather \
         than repeated as a prefix: {line}"
    );
}

/// Bind a fresh session the way `broca-session run` does (route.open, then subscribe on a second
/// route, then send) and time only the `session.send` round trip: the admission ack, not the run.
///
/// The send carries a tool schema, as `scripts/acceptance-turn.sh` does, so the tool-bearing
/// send path runs: it spawns a detached warm bind to the tool module (absent from this rig).
///
/// Unix-only because its one caller is: SIGTERM has no Windows equivalent, and an ungated helper
/// is dead code there, which `-D warnings` turns into a compile error on that leg alone.
#[cfg(unix)]
async fn time_first_send(
    rig: &Rig,
    session: &str,
) -> (
    Duration,
    Result<broca_module_serve::client::SendOutcome, broca_subc::connection::ConnError>,
) {
    use broca_module_serve::client::{SendSpec, SessionClient};
    let identity = broca_protocol::BindIdentity::new(&rig.dir, "probe", session);
    let conn = broca_subc::SubcConnection::connect(&rig.connection_file)
        .await
        .expect("connect to the rig daemon");
    // Retrying the bind absorbs the window where a respawned module is running but not yet
    // registered; that window is not what this measures.
    let client = SessionClient::bind_with_timeout(conn, "broca", identity, Duration::from_secs(20))
        .await
        .unwrap_or_else(|error| {
            let data_home = rig.dir.join("runtime/data-home/cortexkit");
            eprintln!(
                "{}",
                super::session_client::rig_log_diagnostics(&data_home, &data_home.join("broca"))
            );
            panic!("bind broca's management surface: {error:?}");
        });
    let _stream = client
        .subscribe(broca_module::serve::SubscribeFrom::Live)
        .await
        .expect("subscribe before sending, as broca-session run does");
    let tool = serde_json::from_value(serde_json::json!({
        "name": "read",
        "description": "Read a file from disk",
        "input_schema": {
            "type": "object",
            "properties": {"filePath": {"type": "string"}},
            "required": ["filePath"]
        }
    }))
    .expect("tool schema");
    let started = Instant::now();
    let outcome = client
        .send(SendSpec {
            prompt: "hi",
            system: None,
            provider: "mock",
            model: "mock-model",
            variant: None,
            service_tier: None,
            tools: Some(vec![tool]),
            send_id: None,
            max_output_tokens: None,
            cache: None,
            budget: None,
            auth: None,
            one_shot: None,
            context_management: None,
        })
        .await;
    (started.elapsed(), outcome)
}

/// THE FIRST SEND AFTER A SIGTERM RESPAWN IS ADMITTED, sent the moment the new process exists.
///
/// Written after one production `kill -TERM`: the respawned module registered 0.16 s later, and
/// the first `session.send` ~15 s after that timed out on the client with no run-index row. This
/// rig has not reproduced that (every sweep of immediate/1/5/15 s delays admitted in well under
/// a second, same as first boot), so this is a guard on the path rather than a reproduction.
///
/// THE BOUND IS THE CLIENT'S OWN DEADLINE. `send` fails with a transport timeout after
/// `broca_subc::connection::RESPONSE_TIMEOUT` (10 s), which is exactly the production failure,
/// so "returned Active" already means "admitted inside 10 s". No tighter bound is asserted: the
/// measured admissions are tens of milliseconds, and a loaded CI host must not turn a slow
/// machine into a red test. The first-boot send is the control that makes a respawn-only
/// failure distinguishable from a rig that cannot admit at all.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn the_first_send_after_a_sigterm_respawn_is_admitted() {
    let Some(rig) = Rig::start("respawn-admission").await else {
        return;
    };
    let (control_took, control) = time_first_send(&rig, "before-respawn").await;
    assert!(
        matches!(
            control,
            Ok(broca_module_serve::client::SendOutcome::Active { .. })
        ),
        "the control send on the first boot was not admitted ({control_took:?}): {control:?}"
    );

    let daemon_pid = rig.child.id().expect("daemon pid");
    let first = broca_children(daemon_pid);
    assert_eq!(
        first.len(),
        1,
        "expected one supervised ck-broca: {first:?}"
    );
    let old = first[0];
    let status = std::process::Command::new("kill")
        .args(["-TERM", &old.to_string()])
        .status()
        .expect("run kill");
    assert!(status.success(), "kill -TERM {old} failed");
    let deadline = Instant::now() + Duration::from_secs(40);
    while !broca_children(daemon_pid).iter().any(|pid| *pid != old) {
        assert!(
            Instant::now() < deadline,
            "broca was not respawned after SIGTERM"
        );
        sleep(Duration::from_millis(20)).await;
    }

    let (took, outcome) = time_first_send(&rig, "after-respawn").await;
    println!("admission: first boot {control_took:?}, after SIGTERM respawn {took:?}");
    assert!(
        matches!(
            outcome,
            Ok(broca_module_serve::client::SendOutcome::Active { .. })
        ),
        "the first send after a SIGTERM respawn was not admitted ({took:?}; first-boot control \
         took {control_took:?}): {outcome:?}"
    );
}
