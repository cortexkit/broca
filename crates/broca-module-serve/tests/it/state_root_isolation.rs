//! broca's state root at boot, and the isolation every test that spawns broca must keep from it.
//!
//! Spawn wrappers are exercised against hostile inherited roots. The child must create its
//! state files only beneath the selected scratch root, never the operator's data home.

use std::path::{Path, PathBuf};
use std::process::Output;

/// A scratch layout mirroring a real machine: a runtime directory holding the (absent)
/// connection file, and a separate data home.
struct Machine(PathBuf);

impl Machine {
    fn new(tag: &str) -> Self {
        let root = broca_test_support::scratch_root().join(format!(
            "broca-state-root-boot-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        for dir in ["runtime", "data", "config", "cwd"] {
            std::fs::create_dir_all(root.join(dir)).expect("create scratch layout");
        }
        Self(root)
    }

    fn connection_file(&self) -> PathBuf {
        self.0.join("runtime").join("subc-connection.json")
    }

    /// Where the old default put state: beside the connection file.
    fn legacy_root(&self) -> PathBuf {
        self.0.join("runtime").join("broca-state")
    }

    fn new_root(&self) -> PathBuf {
        self.0.join("data").join("cortexkit").join("broca")
    }

    /// Boot the real binary against a connection file that does not exist. Every home it could
    /// write under is pinned into the scratch layout, and an inherited BROCA_STATE_ROOT is
    /// dropped so the default rule is what gets exercised.
    fn boot(&self) -> Output {
        broca_test_support::retry_executable_file_busy(|| {
            broca_test_support::ckdev_command(
                env!("CARGO_BIN_EXE_ck-broca"),
                broca_test_support::scratch_root(),
            )
            .arg("--subc")
            .arg(self.connection_file())
            .env("XDG_DATA_HOME", self.0.join("data"))
            .env("XDG_CONFIG_HOME", self.0.join("config"))
            .env_remove("BROCA_STATE_ROOT")
            .env_remove("SUBC_LAUNCH_NONCE")
            .env_remove("SUBC_LAUNCH_NONCE_FD")
            .env("SUBC_MODULE_ID", "broca")
            .output()
        })
        .expect("run ck-broca")
    }
}

impl Drop for Machine {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A machine that ran on the old default keeps its WAL beside the connection file. Starting at
/// the new default would silently begin an empty history, so the binary must refuse, naming
/// both paths, before it connects to anything.
#[test]
fn boot_refuses_while_the_old_default_holds_a_wal() {
    let machine = Machine::new("stranded");
    std::fs::create_dir_all(machine.legacy_root().join("wal")).unwrap();

    let output = machine.boot();
    // `main` returns the error, and the runtime prints it with `Debug`, which escapes each
    // backslash. On Windows every path separator would appear doubled, so undo that before
    // looking for paths in the message.
    let stderr = String::from_utf8_lossy(&output.stderr).replace("\\\\", "\\");
    assert!(!output.status.success(), "{stderr}");
    assert!(stderr.contains("refusing to start"), "{stderr}");
    assert!(
        stderr.contains(&machine.legacy_root().display().to_string())
            && stderr.contains(&machine.new_root().display().to_string()),
        "the refusal must name the old and the new location: {stderr}"
    );
    assert!(
        !machine.new_root().join("wal").exists(),
        "nothing may be written at the new default while the old one holds state"
    );
}

/// A fresh machine, with state in neither place, gets past the state root: the boot then fails
/// only because the connection file does not exist.
#[test]
fn a_fresh_machine_boots_past_the_state_root() {
    let machine = Machine::new("fresh");
    let output = machine.boot();
    // `main` returns the error, and the runtime prints it with `Debug`, which escapes each
    // backslash. On Windows every path separator would appear doubled, so undo that before
    // looking for paths in the message.
    let stderr = String::from_utf8_lossy(&output.stderr).replace("\\\\", "\\");
    assert!(!output.status.success(), "{stderr}");
    assert!(
        stderr.contains("tool-plane consumer connect"),
        "a fresh machine must reach the subc connection, not stop at the state root: {stderr}"
    );
    assert!(!stderr.contains("state root"), "{stderr}");
}

/// With no BROCA_STATE_ROOT and no home at all, the binary refuses by name rather than putting
/// the WAL under a path relative to wherever it was started.
#[test]
fn boot_without_any_home_refuses_by_name() {
    let machine = Machine::new("homeless");
    let output = broca_test_support::retry_executable_file_busy(|| {
        broca_test_support::ckdev_command(
            env!("CARGO_BIN_EXE_ck-broca"),
            broca_test_support::scratch_root(),
        )
        .arg("--subc")
        .arg(machine.connection_file())
        // No HOME, no XDG_DATA_HOME, no BROCA_STATE_ROOT. Only the config home is given, so the
        // config loaders that run first resolve into scratch; anything else that resolves a
        // relative path lands in the scratch working directory.
        .env_clear()
        .env("XDG_CONFIG_HOME", machine.0.join("config"))
        .env("SUBC_MODULE_ID", "broca")
        .current_dir(machine.0.join("cwd"))
        .output()
    })
    .expect("run ck-broca");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    assert!(
        stderr.contains("cannot resolve broca's state root") && stderr.contains("BROCA_STATE_ROOT"),
        "{stderr}"
    );
    assert!(
        !machine.0.join("cwd").join("broca-state").exists() && !machine.legacy_root().exists(),
        "no relative or connection-relative state root may be created"
    );
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name == "target") {
                continue;
            }
            rust_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

/// Only the shared resolver in `state_root.rs` may read BROCA_STATE_ROOT or spell the old
/// connection-relative `broca-state` directory. A second resolver in any binary is how the CLI
/// and the daemon came to disagree about where state lives, and a revived `broca-state` fallback
/// would put the WAL back among runtime files that cleanup removes.
#[test]
fn only_the_shared_resolver_names_the_state_root() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let resolver = Path::new("crates/broca-module-serve/src/state_root.rs");
    let mut files = Vec::new();
    rust_files(&workspace.join("crates"), &mut files);
    let mut offenders = Vec::new();
    let mut scanned = 0;
    for file in &files {
        let relative = file.strip_prefix(&workspace).unwrap_or(file);
        // Production sources only: tests name the variable to pin it.
        let in_src = relative.components().any(|c| c.as_os_str() == "src");
        if !in_src || relative == resolver {
            continue;
        }
        scanned += 1;
        let source = std::fs::read_to_string(file).unwrap_or_default();
        for (number, line) in source.lines().enumerate() {
            if line.contains("\"BROCA_STATE_ROOT\"") || line.contains("\"broca-state\"") {
                offenders.push(format!(
                    "{}:{}: {}",
                    relative.display(),
                    number + 1,
                    line.trim()
                ));
            }
        }
    }
    assert!(scanned > 50, "only {scanned} source files scanned");
    assert!(
        offenders.is_empty(),
        "resolve the state root through broca_module_serve::state_root instead:\n  {}",
        offenders.join("\n  ")
    );
}

/// Boot a child through each command builder and observe its writes beneath the selected root.
#[tokio::test]
async fn every_broca_spawn_site_pins_its_state_root() {
    let wrappers = [
        "command",
        "ckdev_command",
        "rig_command",
        "tokio_rig_command",
    ];
    let mut exercised = std::collections::BTreeSet::new();
    for wrapper in wrappers {
        for explicit in [false, true] {
            let machine = Machine::new(&format!("{wrapper}-{explicit}"));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let key = vec![0x51; 32];
            let daemon_id = [0x15; 16];
            subc_transport::connection_file::write_atomic(
                machine.connection_file(),
                &subc_transport::connection_file::ConnectionInfo {
                    schema: subc_transport::connection_file::SCHEMA_VERSION,
                    wire_version: None,
                    endpoints: vec![subc_transport::connection_file::Endpoint {
                        host: "127.0.0.1".into(),
                        port: listener.local_addr().unwrap().port(),
                    }],
                    key: key.clone(),
                    daemon_id,
                    pid: std::process::id(),
                    daemon_ver: "isolation".into(),
                },
            )
            .unwrap();
            // Authentication lets boot reach its storage initialization. The stand-in
            // offers no modules or providers, so it cannot authorize any application work.
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                subc_transport::authenticate_server(
                    &mut stream,
                    &key,
                    &daemon_id,
                    "isolation",
                    broca_test_support::STAND_IN_AUTH_DEADLINE,
                )
                .await
                .unwrap();
                while let Ok(Some(frame)) = subc_transport::read_frame(&mut stream).await {
                    let (ty, body) = match frame.header.ty {
                        subc_protocol::FrameType::Request => (subc_protocol::FrameType::Error,
                            br#"{"code":"unknown_method","message":"no services in the isolation stand-in"}"#.to_vec()),
                        subc_protocol::FrameType::Ping => (subc_protocol::FrameType::Pong, vec![]),
                        _ => continue,
                    };
                    let reply = subc_protocol::Frame::build(
                        ty,
                        subc_protocol::Flags::new(false, subc_protocol::Priority::Passive, false),
                        frame.header.channel,
                        frame.header.epoch,
                        frame.header.corr,
                        body,
                    )
                    .unwrap();
                    if subc_transport::write_frame(&mut stream, &reply)
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            });
            let binary =
                broca_test_support::ckdev_binary(env!("CARGO_BIN_EXE_ck-broca"), &machine.0);
            let mut command: tokio::process::Command = match wrapper {
                "command" => std::process::Command::new(&binary).into(),
                "ckdev_command" => broca_test_support::ckdev_command(&binary, &machine.0).into(),
                "rig_command" => broca_test_support::rig_command(&binary, &machine.0, "isolation")
                    .unwrap()
                    .into(),
                "tokio_rig_command" => {
                    broca_test_support::tokio_rig_command(&binary, &machine.0, "isolation").unwrap()
                }
                _ => unreachable!(),
            };
            let inherited = machine.0.join("inherited-state");
            let home = machine.0.join("hostile-home");
            command
                .arg("--subc")
                .arg(machine.connection_file())
                .env("HOME", &home)
                .env("USERPROFILE", &home)
                .env("XDG_DATA_HOME", machine.0.join("data"))
                .env("LOCALAPPDATA", machine.0.join("data"))
                .env("XDG_CONFIG_HOME", machine.0.join("config"))
                .env("APPDATA", machine.0.join("config"))
                .env_remove("BROCA_CONFIG_PATH")
                .env_remove("SUBC_LAUNCH_NONCE")
                .env_remove("SUBC_LAUNCH_NONCE_FD")
                .env("SUBC_MODULE_ID", "broca")
                .env("BROCA_STATE_ROOT", &inherited)
                .stdin(std::process::Stdio::null())
                .stdout(std::fs::File::create(machine.0.join("boot.stdout.log")).unwrap())
                .stderr(std::fs::File::create(machine.0.join("boot.stderr.log")).unwrap())
                .kill_on_drop(true)
                .current_dir(machine.0.join("cwd"));
            if explicit {
                command.env("BROCA_STATE_ROOT", machine.new_root());
            } else {
                command.env_remove("BROCA_STATE_ROOT");
            }
            let mut child = command.spawn().unwrap();
            broca_test_support::wait_until("isolated child storage initialization", || async {
                if machine
                    .new_root()
                    .join("media-uploads/uploads.lock")
                    .is_file()
                {
                    Ok(())
                } else {
                    Err(std::fs::read_to_string(machine.0.join("boot.stderr.log"))
                        .unwrap_or_default())
                }
            })
            .await;
            for directory in ["leases", "wal-durable", "wal-archive-durable"] {
                assert!(
                    machine.new_root().join(directory).is_dir(),
                    "{wrapper}: missing {directory}"
                );
            }
            let logs = machine.new_root().join("logs");
            let names: Vec<_> = std::fs::read_dir(&logs)
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect();
            assert!(
                names
                    .iter()
                    .any(|name| name.to_string_lossy().starts_with("stall-")),
                "{wrapper} did not write its state-root log: {names:?}"
            );
            assert!(
                !inherited.exists(),
                "{wrapper} used the inherited state root"
            );
            assert!(!home.exists(), "{wrapper} wrote under the default home");
            assert!(
                !machine.legacy_root().exists(),
                "{wrapper} used the connection-relative root"
            );
            assert_eq!(std::fs::read_dir(machine.0.join("cwd")).unwrap().count(), 0);
            child.kill().await.unwrap();
            server.abort();
            exercised.insert((wrapper, explicit));
        }
    }
    assert_eq!(
        exercised.len(),
        8,
        "all four wrappers must run both root-selection policies"
    );
}
