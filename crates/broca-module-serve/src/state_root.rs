//! Where broca keeps its durable state -- the session store, the leases and `run-index.db` --
//! resolved ONCE, for every binary that needs it.
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
//!    start and says so by name. It never falls back to a relative path: that would place the
//!    sessions under whatever directory the process happened to start in, and a later start from
//!    another directory would find an empty history and report success.
//!
//! THE LOCATION IS A CONTRACT WITH ANOTHER MODULE. Astrocyte reads broca's usage straight from
//! `<data home>/cortexkit/broca/run-index.db`, and it cannot tell a missing index from "broca is
//! not installed". A state root anywhere else makes broca's usage vanish from astrocyte without
//! an error on either side, so the default must be exactly this path.

use std::ffi::OsString;
use std::fmt;
use std::path::PathBuf;

/// The variable that selects the state root explicitly.
pub const STATE_ROOT_ENV: &str = "BROCA_STATE_ROOT";

/// Why broca will not start with any state root.
#[derive(Debug)]
pub enum StateRootError {
    /// Neither `BROCA_STATE_ROOT` nor any data-home rung is set.
    Unresolvable,
    /// The data-home rung that won is a relative path.
    RelativeDataHome { var: &'static str, value: PathBuf },
}

impl fmt::Display for StateRootError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unresolvable => write!(
                f,
                "cannot resolve broca's state root: {STATE_ROOT_ENV} is not set and there is no \
                 data home (XDG_DATA_HOME and HOME are both unset). Refusing to start rather than \
                 placing broca's state under a relative path. Set {STATE_ROOT_ENV} to the \
                 directory that holds broca's session store and run index."
            ),
            Self::RelativeDataHome { var, value } => write!(
                f,
                "cannot resolve broca's state root: {var} is the relative path {}, which would \
                 place broca's state under whatever directory this process started in. Set {var} to an \
                 absolute path, or set {STATE_ROOT_ENV}.",
                value.display()
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
) -> Result<PathBuf, StateRootError> {
    if let Some(explicit) = non_empty(env(STATE_ROOT_ENV)) {
        return Ok(PathBuf::from(explicit));
    }
    let (data_home, var) = data_home_with(&env).ok_or(StateRootError::Unresolvable)?;
    if !data_home.is_absolute() {
        return Err(StateRootError::RelativeDataHome {
            var,
            value: data_home,
        });
    }
    Ok(data_home.join("cortexkit").join("broca"))
}

/// [`resolve_state_root_with`] against the real process environment.
pub fn resolve_state_root() -> Result<PathBuf, StateRootError> {
    resolve_state_root_with(|key| std::env::var_os(key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::Path;

    fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<OsString> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        move |key: &str| map.get(key).map(OsString::from)
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
        assert_eq!(resolve_state_root_with(env).unwrap(), abs("srv/broca"));
    }

    #[test]
    fn xdg_data_home_wins_over_home_when_the_variable_is_unset() {
        let (xdg, home) = (s(&abs("xdg")), s(&abs("home/u")));
        let env = env_of(&[("XDG_DATA_HOME", xdg.as_str()), ("HOME", home.as_str())]);
        assert_eq!(
            resolve_state_root_with(env).unwrap(),
            abs("xdg/cortexkit/broca")
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
            abs("home/u/.local/share/cortexkit/broca")
        );
    }

    /// An exported-but-empty variable means "not set" and falls through to the next rung,
    /// rather than resolving the state root to the filesystem root or the current directory.
    #[test]
    fn empty_values_fall_through_to_the_next_rung() {
        let home = s(&abs("home/u"));
        let env = env_of(&[
            (STATE_ROOT_ENV, ""),
            ("XDG_DATA_HOME", ""),
            ("HOME", home.as_str()),
        ]);
        assert_eq!(
            resolve_state_root_with(env).unwrap(),
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

    /// A relative data home would put the state root wherever the process happened to start.
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
}
