//! Where broca keeps its durable state -- the WAL, the leases and `run-index.db` -- resolved
//! ONCE, for every binary that needs it.
//!
//! THE RULE, in order:
//!
//! 1. `BROCA_STATE_ROOT`, when set and non-empty, used exactly as given.
//! 2. `<data home>/cortexkit/broca`, where the data home is `XDG_DATA_HOME`, else (on Windows)
//!    `APPDATA` or `USERPROFILE\AppData\Roaming`, else `HOME/.local/share`. These are the same
//!    rungs, in the same order, as `default_data_home` in the subc daemon that supervises broca,
//!    and its mirror
//!    `cortexkit_store_types::resolve_data_home`, which every CortexKit store and broca's own log
//!    directory already resolve through. That crate ships a golden file of environment cases
//!    and the data home each must produce; the parity test below runs this ladder over it.
//! 3. Nothing. When neither the variable nor a home directory is available, broca refuses to
//!    start and says so by name. It never falls back to a relative path: that would place the WAL
//!    under whatever directory the process happened to start in, and a later start from another
//!    directory would find an empty history and report success.
//!
//! THE LOCATION IS A CONTRACT WITH ANOTHER MODULE. Astrocyte reads broca's usage straight from
//! `<data home>/cortexkit/broca/run-index.db`, and it cannot tell a missing index from "broca is
//! not installed". A state root anywhere else makes broca's usage vanish from astrocyte without
//! an error on either side, so the default must be exactly this path.
//!
//! WHY THE DEFAULT MOVED. The default used to be a `broca-state` directory beside the subc
//! connection file, which is the daemon's RUNTIME directory: connection file, start lock and
//! other genuinely ephemeral files that cleanup removes. The WAL is the system of record and
//! nothing can regenerate it, so it does not belong there. Production never used that default
//! (the deployment sets `BROCA_STATE_ROOT`), but an unset variable silently took it.
//!
//! STATE IS NEVER MOVED SILENTLY. A machine that did run on the old default has its WAL and run
//! index in `<connection-file dir>/broca-state`. Starting at the new default there would begin an
//! empty history and leave the real one orphaned, so [`refuse_stranded_legacy_state`] refuses to
//! start instead and names both paths.

use std::ffi::OsString;
use std::fmt;
use std::path::{Path, PathBuf};

/// The variable that selects the state root explicitly.
pub const STATE_ROOT_ENV: &str = "BROCA_STATE_ROOT";

/// The directory name the OLD default placed beside the subc connection file. Kept only so a
/// machine still holding state there is refused rather than silently abandoned.
const LEGACY_DIR_NAME: &str = "broca-state";

/// The entries whose presence means a directory holds broca state that cannot be regenerated.
const STATE_MARKERS: [&str; 2] = ["wal", "run-index.db"];

/// How the state root was chosen. Only a derived root is checked for state stranded at the old
/// default: an explicit `BROCA_STATE_ROOT` resolves exactly as it always did, so nothing moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateRootOrigin {
    /// `BROCA_STATE_ROOT` named it.
    Explicit,
    /// Derived from the data home.
    DataHome,
}

/// A resolved state root and how it was chosen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateRoot {
    pub path: PathBuf,
    pub origin: StateRootOrigin,
}

/// Why broca will not start with any state root.
#[derive(Debug)]
pub enum StateRootError {
    /// Neither `BROCA_STATE_ROOT` nor any data-home rung is set.
    Unresolvable,
    /// The data-home rung that won is a relative path.
    RelativeDataHome { var: &'static str, value: PathBuf },
    /// The new default holds no state, but the old default does.
    StrandedLegacyState { resolved: PathBuf, legacy: PathBuf },
    /// Whether a directory holds state could not be determined.
    Probe {
        path: PathBuf,
        error: std::io::Error,
    },
}

impl fmt::Display for StateRootError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unresolvable => write!(
                f,
                "cannot resolve broca's state root: {STATE_ROOT_ENV} is not set and there is no \
                 data home (XDG_DATA_HOME and HOME are both unset). Refusing to start rather than \
                 placing the WAL under a relative path. Set {STATE_ROOT_ENV} to the directory that \
                 holds broca's WAL and run index."
            ),
            Self::RelativeDataHome { var, value } => write!(
                f,
                "cannot resolve broca's state root: {var} is the relative path {}, which would \
                 place the WAL under whatever directory this process started in. Set {var} to an \
                 absolute path, or set {STATE_ROOT_ENV}.",
                value.display()
            ),
            Self::StrandedLegacyState { resolved, legacy } => write!(
                f,
                "refusing to start: broca's state root is now {resolved}, which holds no WAL or \
                 run index, but the old default location {legacy} does. Starting here would begin \
                 an empty history and leave the existing one behind. Either set \
                 {STATE_ROOT_ENV}={legacy} to keep using it, or stop broca and move {legacy} to \
                 {resolved}.",
                resolved = resolved.display(),
                legacy = legacy.display(),
            ),
            Self::Probe { path, error } => write!(
                f,
                "cannot tell whether {} holds broca state: {error}. Refusing to start rather than \
                 guess.",
                path.display()
            ),
        }
    }
}

impl std::error::Error for StateRootError {}

/// Read one environment variable, treating an EMPTY value as unset, the same way the subc daemon
/// does: an exported-but-empty variable is the shell's normal way to say "not set".
fn non_empty(value: Option<OsString>) -> Option<OsString> {
    value.filter(|v| !v.is_empty())
}

/// The platform data home and the variable that supplied it, or `None` when no rung is set.
///
/// The value is returned RAW, relative or not, so the parity test can compare it with the golden
/// cases; [`resolve_state_root_with`] is where a relative value is refused. One golden case is
/// deliberately not followed: with no variable set at all the golden expects the relative
/// `.local/share`, while here that case is `None` and broca refuses to start.
fn data_home_with(env: &impl Fn(&str) -> Option<OsString>) -> Option<(PathBuf, &'static str)> {
    if let Some(xdg) = non_empty(env("XDG_DATA_HOME")) {
        return Some((PathBuf::from(xdg), "XDG_DATA_HOME"));
    }
    #[cfg(windows)]
    {
        if let Some(app_data) = non_empty(env("APPDATA")) {
            return Some((PathBuf::from(app_data), "APPDATA"));
        }
        if let Some(profile) = non_empty(env("USERPROFILE")) {
            return Some((
                PathBuf::from(profile).join("AppData").join("Roaming"),
                "USERPROFILE",
            ));
        }
    }
    non_empty(env("HOME")).map(|home| (PathBuf::from(home).join(".local").join("share"), "HOME"))
}

/// Resolve the state root from an environment lookup, without touching the filesystem.
///
/// The lookup is a parameter so every rung is testable with an injected map; mutating the real
/// process environment would race the other tests running in parallel.
pub fn resolve_state_root_with(
    env: impl Fn(&str) -> Option<OsString>,
) -> Result<StateRoot, StateRootError> {
    if let Some(explicit) = non_empty(env(STATE_ROOT_ENV)) {
        return Ok(StateRoot {
            path: PathBuf::from(explicit),
            origin: StateRootOrigin::Explicit,
        });
    }
    let (data_home, var) = data_home_with(&env).ok_or(StateRootError::Unresolvable)?;
    if !data_home.is_absolute() {
        return Err(StateRootError::RelativeDataHome {
            var,
            value: data_home,
        });
    }
    Ok(StateRoot {
        path: data_home.join("cortexkit").join("broca"),
        origin: StateRootOrigin::DataHome,
    })
}

/// [`resolve_state_root_with`] against the real process environment.
pub fn resolve_state_root() -> Result<StateRoot, StateRootError> {
    resolve_state_root_with(|key| std::env::var_os(key))
}

/// Where the OLD default put state: a `broca-state` directory beside the connection file.
/// Derived exactly as that default was, so a machine that used it is recognised.
pub fn legacy_state_root(connection_file: &Path) -> PathBuf {
    connection_file
        .parent()
        .map(|dir| dir.join(LEGACY_DIR_NAME))
        .unwrap_or_else(|| PathBuf::from(LEGACY_DIR_NAME))
}

/// True when `dir` holds a WAL directory or a run index.
fn holds_state(dir: &Path) -> Result<bool, StateRootError> {
    for marker in STATE_MARKERS {
        let path = dir.join(marker);
        match path.try_exists() {
            Ok(true) => return Ok(true),
            Ok(false) => {}
            Err(error) => return Err(StateRootError::Probe { path, error }),
        }
    }
    Ok(false)
}

/// Refuse to start when a derived state root is empty but the old default still holds state.
///
/// A fresh machine, with state in neither place, passes and starts at the new default. So does a
/// machine whose new root already holds state, and any explicit `BROCA_STATE_ROOT`.
pub fn refuse_stranded_legacy_state(
    root: &StateRoot,
    connection_file: &Path,
) -> Result<(), StateRootError> {
    if root.origin == StateRootOrigin::Explicit || holds_state(&root.path)? {
        return Ok(());
    }
    let legacy = legacy_state_root(connection_file);
    if holds_state(&legacy)? {
        return Err(StateRootError::StrandedLegacyState {
            resolved: root.path.clone(),
            legacy,
        });
    }
    Ok(())
}

/// The state root a launching binary must use: resolved, then checked for stranded state at the
/// old default. Both `ck-broca` and `broca-run` call this, so they cannot disagree about where
/// the shared state lives.
pub fn resolve_for_launch_with(
    env: impl Fn(&str) -> Option<OsString>,
    connection_file: &Path,
) -> Result<StateRoot, StateRootError> {
    let root = resolve_state_root_with(env)?;
    refuse_stranded_legacy_state(&root, connection_file)?;
    Ok(root)
}

/// [`resolve_for_launch_with`] against the real process environment.
pub fn resolve_for_launch(connection_file: &Path) -> Result<StateRoot, StateRootError> {
    resolve_for_launch_with(|key| std::env::var_os(key), connection_file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<OsString> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        move |key: &str| map.get(key).map(OsString::from)
    }

    /// A fresh scratch directory, removed on drop.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(tag: &str) -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let root = broca_test_support::scratch_root().join(format!(
                "broca-state-root-{tag}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = std::fs::remove_dir_all(&root);
            std::fs::create_dir_all(&root).expect("create scratch dir");
            Self(root)
        }

        /// A connection file in `runtime/`, so the old default is `runtime/broca-state`.
        fn connection_file(&self) -> PathBuf {
            self.0.join("runtime").join("subc-connection.json")
        }

        fn data_home(&self) -> PathBuf {
            self.0.join("data")
        }

        fn env(&self) -> impl Fn(&str) -> Option<OsString> {
            let data_home = self.data_home().into_os_string();
            move |key: &str| (key == "XDG_DATA_HOME").then(|| data_home.clone())
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// An absolute path on the platform running the test. The resolver refuses a relative data
    /// home, and `/xdg` is relative on Windows (no drive), so fixtures are built from this.
    fn abs(tail: &str) -> PathBuf {
        #[cfg(windows)]
        let root = PathBuf::from(r"C:\");
        #[cfg(not(windows))]
        let root = PathBuf::from("/");
        tail.split('/').fold(root, |path, part| path.join(part))
    }

    fn s(path: &std::path::Path) -> String {
        path.to_str().expect("fixture paths are UTF-8").to_owned()
    }

    #[test]
    fn the_variable_wins_over_every_data_home_rung() {
        let (srv, xdg, home) = (s(&abs("srv/broca")), s(&abs("xdg")), s(&abs("home/u")));
        let env = env_of(&[
            (STATE_ROOT_ENV, srv.as_str()),
            ("XDG_DATA_HOME", xdg.as_str()),
            ("HOME", home.as_str()),
        ]);
        assert_eq!(
            resolve_state_root_with(env).unwrap(),
            StateRoot {
                path: abs("srv/broca"),
                origin: StateRootOrigin::Explicit
            }
        );
    }

    #[test]
    fn xdg_data_home_wins_over_home_when_the_variable_is_unset() {
        let (xdg, home) = (s(&abs("xdg")), s(&abs("home/u")));
        let env = env_of(&[("XDG_DATA_HOME", xdg.as_str()), ("HOME", home.as_str())]);
        assert_eq!(
            resolve_state_root_with(env).unwrap(),
            StateRoot {
                path: abs("xdg/cortexkit/broca"),
                origin: StateRootOrigin::DataHome
            }
        );
    }

    /// With only HOME set -- the usual host -- the root is exactly where astrocyte reads
    /// `run-index.db`.
    #[test]
    fn home_is_the_last_rung() {
        let home = s(&abs("home/u"));
        let env = env_of(&[("HOME", home.as_str())]);
        assert_eq!(
            resolve_state_root_with(env).unwrap(),
            StateRoot {
                path: abs("home/u/.local/share/cortexkit/broca"),
                origin: StateRootOrigin::DataHome
            }
        );
    }

    /// An exported-but-empty variable means "not set" and falls through to the next rung,
    /// rather than resolving the WAL to the filesystem root or the current directory.
    #[test]
    fn empty_values_fall_through_to_the_next_rung() {
        let home = s(&abs("home/u"));
        let env = env_of(&[
            (STATE_ROOT_ENV, ""),
            ("XDG_DATA_HOME", ""),
            ("HOME", home.as_str()),
        ]);
        assert_eq!(
            resolve_state_root_with(env).unwrap().path,
            abs("home/u/.local/share/cortexkit/broca")
        );
    }

    #[test]
    fn no_variable_and_no_home_refuses_by_name() {
        let error = resolve_state_root_with(env_of(&[])).unwrap_err();
        assert!(matches!(error, StateRootError::Unresolvable), "{error:?}");
        let message = error.to_string();
        for name in [STATE_ROOT_ENV, "XDG_DATA_HOME", "HOME"] {
            assert!(message.contains(name), "{message}");
        }
    }

    /// A relative data home would put the WAL wherever the process happened to start.
    #[test]
    fn a_relative_data_home_refuses_rather_than_resolving_against_the_cwd() {
        let error =
            resolve_state_root_with(env_of(&[("XDG_DATA_HOME", "relative/path")])).unwrap_err();
        assert!(
            matches!(
                &error,
                StateRootError::RelativeDataHome { var: "XDG_DATA_HOME", value }
                    if value == Path::new("relative/path")
            ),
            "{error:?}"
        );
    }

    /// The data-home rungs match the golden shipped by cortexkit-store-types 0.2.2 at
    /// `tests/golden/data_home_resolution.json`, vendored byte for byte into this crate.
    /// Refresh the fixture and its SHA-256 when that dependency is bumped, so broca's
    /// state directory stays aligned with the supervisor's resolver.
    #[test]
    fn data_home_rungs_match_the_shared_supervisor_golden() {
        use sha2::{Digest, Sha256};
        let bytes = include_bytes!(
            "../tests/fixtures/cortexkit-store-types-0.2.2/data_home_resolution.json"
        );
        assert_eq!(
            format!("{:x}", Sha256::digest(bytes)),
            "0390a2734f3e4ca9f853f23839e017b774b0319b9f47a39fa7dd44e47d436d09",
            "refresh the cortexkit-store-types fixture and its reviewed SHA-256 together"
        );
        let golden: serde_json::Value =
            serde_json::from_slice(bytes).expect("the shared data-home golden is valid JSON");
        let mut checked = 0;
        for case in golden["cases"].as_array().expect("golden has cases") {
            let platform = case["platform"].as_str().unwrap_or("any");
            let applies = match platform {
                "any" => true,
                "unix" => cfg!(unix),
                "windows" => cfg!(windows),
                _ => false,
            };
            if !applies {
                continue;
            }
            let expect = case["expect"]
                .as_str()
                .expect("every golden case has an expect");
            let pairs: Vec<(String, String)> = case["env"]
                .as_object()
                .expect("golden env is an object")
                .iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                .collect();
            let env = move |key: &str| {
                pairs
                    .iter()
                    .find(|(k, _)| k == key)
                    .map(|(_, v)| OsString::from(v))
            };
            let name = case["name"].as_str().unwrap_or("?");
            match data_home_with(&env) {
                Some((path, _)) => assert_eq!(path, PathBuf::from(expect), "golden row {name}"),
                // The one deliberate difference: with no variable set, broca refuses instead of
                // using the relative `.local/share`.
                None => assert_eq!(
                    expect,
                    Path::new(".local").join("share").to_string_lossy(),
                    "golden row {name}: only the all-unset row may resolve to nothing"
                ),
            }
            checked += 1;
        }
        assert!(checked >= 5, "only {checked} golden rows applied");
    }

    #[test]
    fn old_default_state_refuses_to_start_and_names_both_paths() {
        let scratch = Scratch::new("stranded");
        let legacy = legacy_state_root(&scratch.connection_file());
        std::fs::create_dir_all(legacy.join("wal")).unwrap();

        let error = resolve_for_launch_with(scratch.env(), &scratch.connection_file())
            .expect_err("state at the old default must refuse a start at the new one");
        let resolved = scratch.data_home().join("cortexkit").join("broca");
        assert!(
            matches!(
                &error,
                StateRootError::StrandedLegacyState { resolved: r, legacy: l }
                    if *r == resolved && *l == legacy
            ),
            "{error:?}"
        );
        let message = error.to_string();
        assert!(
            message.contains(&resolved.display().to_string()),
            "{message}"
        );
        assert!(message.contains(&legacy.display().to_string()), "{message}");
        assert!(message.contains(STATE_ROOT_ENV), "{message}");
    }

    /// A run index alone is state too: it is what astrocyte reads.
    #[test]
    fn an_old_run_index_without_a_wal_also_refuses() {
        let scratch = Scratch::new("stranded-index");
        let legacy = legacy_state_root(&scratch.connection_file());
        std::fs::create_dir_all(&legacy).unwrap();
        std::fs::write(legacy.join("run-index.db"), b"").unwrap();
        assert!(matches!(
            resolve_for_launch_with(scratch.env(), &scratch.connection_file()),
            Err(StateRootError::StrandedLegacyState { .. })
        ));
    }

    #[test]
    fn a_fresh_machine_starts_at_the_new_default() {
        let scratch = Scratch::new("fresh");
        let root = resolve_for_launch_with(scratch.env(), &scratch.connection_file())
            .expect("no state anywhere is a normal first start");
        assert_eq!(
            root.path,
            scratch.data_home().join("cortexkit").join("broca")
        );
        assert_eq!(root.origin, StateRootOrigin::DataHome);
    }

    /// Once the new root holds state, a leftover copy at the old default is not a reason to stop.
    #[test]
    fn state_at_the_new_default_wins_over_a_leftover_old_copy() {
        let scratch = Scratch::new("both");
        std::fs::create_dir_all(legacy_state_root(&scratch.connection_file()).join("wal")).unwrap();
        std::fs::create_dir_all(scratch.data_home().join("cortexkit/broca/wal")).unwrap();
        assert!(resolve_for_launch_with(scratch.env(), &scratch.connection_file()).is_ok());
    }

    /// An explicit root resolves exactly as it always did, so no state moved and there is
    /// nothing to refuse.
    #[test]
    fn an_explicit_root_is_not_checked_against_the_old_default() {
        let scratch = Scratch::new("explicit");
        std::fs::create_dir_all(legacy_state_root(&scratch.connection_file()).join("wal")).unwrap();
        let explicit = scratch.0.join("explicit").into_os_string();
        let env = move |key: &str| (key == STATE_ROOT_ENV).then(|| explicit.clone());
        let root = resolve_for_launch_with(env, &scratch.connection_file()).unwrap();
        assert_eq!(root.origin, StateRootOrigin::Explicit);
    }
}
