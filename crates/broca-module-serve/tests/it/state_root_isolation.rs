//! broca's state root at boot, and the isolation every test that spawns broca must keep from it.
//!
//! The default state root is the operator's REAL data home (`~/.local/share/cortexkit/broca`),
//! where the production WAL and the run index astrocyte reads both live. A test that spawns
//! `ck-broca` or `broca-run` -- directly, or through a subc daemon that passes its environment
//! on to the module -- without pinning where that state goes would write into production. The
//! guard at the bottom of this file reads every such spawn site in the workspace and fails on
//! any that does not pin it.

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

// ------------------------------------------------------------------------------------------
// The isolation guard: every test that spawns broca must pin where its state goes, so no test
// run can write into the operator's real state root.

/// Text that marks a source file as one that runs broca's binaries.
const BROCA_BINARY_MARKERS: &[&str] = &[
    "CARGO_BIN_EXE_ck-broca",
    "CARGO_BIN_EXE_broca-run",
    "\"ck-broca\"",
    "\"broca-run\"",
];

/// Programs spawned from those files that are NOT broca and open no state root. Every entry is
/// the program argument with whitespace removed. A new program in such a file is treated as a
/// broca spawn until it is listed here, so the guard cannot be passed by a spawn nobody looked
/// at.
const NOT_BROCA: &[(&str, &str)] = &[
    ("program", "the aimock provider server"),
    (
        "ck_import_bin()",
        "the import CLI operating on the rig's isolated project and daemon",
    ),
    (
        "ck_auth",
        "the vault's credential CLI, pointed at a scratch --data-dir",
    ),
    (
        "&ck_auth",
        "the vault's credential CLI, pointed at a scratch --data-dir",
    ),
    (
        "ck_auth_bin()",
        "the vault's credential CLI, pointed at a scratch --data-dir",
    ),
    (
        "broca_session_bin()",
        "a subc client of an already-running module",
    ),
    (
        "env!(\"CARGO_BIN_EXE_broca-session\")",
        "a subc client of an already-running module",
    ),
    (
        "env!(\"CARGO_BIN_EXE_ck-import\")",
        "a subc client of an already-running module",
    ),
    (
        "std::env::current_exe().expect(\"thetestbinary'sownpath\")",
        "the test binary re-running one of its own tests",
    ),
];

/// One `Command::new` site that the guard checked.
#[derive(Debug)]
struct Site {
    file: PathBuf,
    line: usize,
    program: String,
    isolated: bool,
}

fn strip_whitespace(text: &str) -> String {
    text.chars().filter(|c| !c.is_whitespace()).collect()
}

/// The builder text from a spawn site to the call that runs it. `.env` calls made on a named
/// command before it runs fall inside this window too.
fn spawn_window(source: &str) -> &str {
    let end = [".spawn()", ".output()", ".status()"]
        .iter()
        .filter_map(|call| source.find(call))
        .min()
        .unwrap_or(source.len());
    &source[..end]
}

/// Whether a spawn window keeps broca off the operator's data home: an explicit state root, an
/// emptied environment, or a pinned data home with any inherited state root removed. The last
/// needs both halves, because an exported BROCA_STATE_ROOT overrides the data home.
fn window_isolates(window: &str) -> bool {
    let window = strip_whitespace(window);
    window.contains(".env(\"BROCA_STATE_ROOT\",")
        || window.contains(".env_clear()")
        || (window.contains(".env(\"XDG_DATA_HOME\",")
            && window.contains(".env_remove(\"BROCA_STATE_ROOT\")"))
}

/// The argument of the call whose opening parenthesis ends `source[..open]`.
fn call_argument(source: &str, open: usize) -> &str {
    let mut depth = 1usize;
    for (offset, ch) in source[open..].char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return &source[open..open + offset];
                }
            }
            _ => {}
        }
    }
    &source[open..]
}

/// The first argument of an argument list, cut at the first comma outside any brackets.
fn first_argument(arguments: &str) -> &str {
    let mut depth = 0usize;
    for (offset, ch) in arguments.char_indices() {
        match ch {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => return &arguments[..offset],
            _ => {}
        }
    }
    arguments
}

/// Every broca spawn site in `source`, checked. A site is either a `Command` constructor or
/// the test-support ckdev command builder, which is how most rigs now launch broca; missing
/// the second form would leave those spawns unchecked. Programs named by a
/// string literal without "broca" in it (pgrep, kill, sleep) and the programs in
/// [`NOT_BROCA`] are skipped.
fn spawn_sites(file: &Path, source: &str) -> Vec<Site> {
    if !BROCA_BINARY_MARKERS.iter().any(|m| source.contains(m)) {
        return Vec::new();
    }
    let needles = [
        ["Command", "::new("].concat(),
        ["ckdev_", "command("].concat(),
    ];
    let mut opens: Vec<usize> = needles
        .iter()
        .flat_map(|needle| {
            source
                .match_indices(needle.as_str())
                .map(move |(at, _)| at + needle.len())
        })
        .collect();
    opens.sort_unstable();
    let mut sites = Vec::new();
    for open in opens {
        // The builder's own definition is not a spawn.
        if source[..open]
            .strip_suffix(needles[1].as_str())
            .is_some_and(|before| before.ends_with("fn "))
        {
            continue;
        }
        let program = strip_whitespace(first_argument(call_argument(source, open)));
        if program.starts_with('"') && !program.contains("broca") {
            continue;
        }
        if NOT_BROCA.iter().any(|(name, _)| *name == program) {
            continue;
        }
        sites.push(Site {
            file: file.to_path_buf(),
            line: source[..open].lines().count(),
            isolated: window_isolates(spawn_window(&source[open..])),
            program,
        });
    }
    sites
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

/// The checker must be able to fail: an unpinned spawn, and one that pins the data home but
/// lets an inherited BROCA_STATE_ROOT through, are both flagged, and the pinned forms pass.
#[test]
fn the_isolation_checker_flags_an_unpinned_spawn() {
    let new = ["Command", "::new("].concat();
    let case = |body: &str| {
        let source = format!(
            "// CARGO_BIN_EXE_ck-broca\nlet out = {new}env!(\"CARGO_BIN_EXE_ck-broca\"))\n{body}\n    .output();\n"
        );
        let sites = spawn_sites(Path::new("synthetic.rs"), &source);
        assert_eq!(sites.len(), 1, "{sites:?}");
        sites[0].isolated
    };
    assert!(
        !case("    .arg(\"--subc\")"),
        "an unpinned spawn must be flagged"
    );
    assert!(
        !case("    .env(\"XDG_DATA_HOME\", &d)"),
        "a pinned data home with an inheritable BROCA_STATE_ROOT must be flagged"
    );
    assert!(case("    .env(\"BROCA_STATE_ROOT\", &d)"));
    assert!(case(
        "    .env(\"XDG_DATA_HOME\", &d)\n    .env_remove(\"BROCA_STATE_ROOT\")"
    ));
    assert!(case("    .env_clear()"));
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

/// Every spawn of broca in the workspace keeps it off the operator's real data home.
#[test]
fn every_broca_spawn_site_pins_its_state_root() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut files = Vec::new();
    rust_files(&workspace.join("crates"), &mut files);
    let sites: Vec<Site> = files
        .iter()
        .flat_map(|file| {
            let source = std::fs::read_to_string(file).unwrap_or_default();
            let relative = file.strip_prefix(&workspace).unwrap_or(file).to_path_buf();
            spawn_sites(&relative, &source)
        })
        .collect();

    for site in &sites {
        eprintln!(
            "checked {}:{} spawns {}",
            site.file.display(),
            site.line,
            site.program
        );
    }
    let unpinned: Vec<String> = sites
        .iter()
        .filter(|site| !site.isolated)
        .map(|site| {
            format!(
                "{}:{} spawns {}",
                site.file.display(),
                site.line,
                site.program
            )
        })
        .collect();
    assert!(
        unpinned.is_empty(),
        "these spawns of broca could write into the operator's real state root. Each must set \
         BROCA_STATE_ROOT, or pin XDG_DATA_HOME AND env_remove(\"BROCA_STATE_ROOT\"), or \
         env_clear(). If the program is not broca at all, add it to NOT_BROCA with the \
         reason:\n  {}",
        unpinned.join("\n  ")
    );

    // NON-VACUITY: the scan must actually have reached the rigs known to spawn broca, or a
    // broken walk or marker list would pass this with nothing checked.
    for expected in [
        "crates/broca-module-serve/tests/it/provider_override_boot.rs",
        "crates/broca-module-serve/tests/it/wal_known_features.rs",
        "crates/broca-module-serve/tests/launch_nonce.rs",
        "crates/broca-module-serve/tests/it/launch_nonce_e2e.rs",
        "crates/broca-module-serve/tests/it/seal_line_is_filterable_e2e.rs",
        "crates/broca-module-serve/tests/it/single_turn_module_e2e.rs",
        "crates/broca-module-serve/tests/it/captured_replay_e2e.rs",
        "crates/broca-module-serve/tests/it/state_root_isolation.rs",
        "crates/ck-import/tests/it/real_module_e2e.rs",
    ] {
        assert!(
            sites.iter().any(|site| site.file == Path::new(expected)),
            "the guard checked no spawn site in {expected}; its scan has stopped reaching the \
             rigs it exists for"
        );
    }
}
