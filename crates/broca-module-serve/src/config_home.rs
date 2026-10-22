//! Where broca's operator-edited config files live — resolved ONCE, for every file.
//!
//! Cache, billing, auth-method and vault-handle loaders share this resolver so a new home
//! variable cannot move only some of their files. Ignoring a configured home can silently
//! select shipped defaults instead of an operator's overrides.
//!
//! The authority is the subc supervisor's `default_config_home`, last checked against
//! subconscious f4e71180 (`crates/subc-daemon/src/daemon_config.rs`). Its golden table is
//! vendored under `tests/fixtures/subconscious-f4e71180`. Matching its home-variable precedence
//! keeps broca's operator config in the supervisor's configured tree. If the supervisor exposes
//! a callable resolver, use it here instead of maintaining a mirror.

use std::ffi::OsString;
use std::path::PathBuf;

/// Why a config path could not be produced, kept DISTINCT from "the file is not there".
///
/// The two used to be one `None`, and they mean opposite things to an operator: a missing file
/// is a deliberate "no overrides", while an unresolvable HOME is broca unable to look at all.
/// Under launchd today HOME is always set, so the second case is unreachable on this host — but
/// it is one supervisor change away (a system-domain daemon, a container), and the failure it
/// would produce is every override silently ignored while health stays green.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigHome {
    /// A resolved config directory or file. The loader decides whether absence is allowed.
    Resolved {
        path: PathBuf,
        /// True only when the file-specific override named this path. Selecting a config
        /// directory through the config-home ladder does not make its files mandatory.
        explicit: bool,
    },
    /// No platform config-home variable was set, so no path can be formed.
    Unresolvable,
}

/// Read one environment variable, treating an EMPTY value as unset.
///
/// An exported-but-empty variable is the shell's normal way to say "not set", and honouring it
/// as a path root would resolve config to `/.config/...`. The daemon's own resolver makes the
/// same choice.
fn non_empty(value: Option<OsString>) -> Option<OsString> {
    value.filter(|v| !v.is_empty())
}

/// The config directory ladder, in the supervisor's order: `XDG_CONFIG_HOME`, then on Windows
/// `APPDATA` or `USERPROFILE/AppData/Roaming`, then `HOME/.config`.
///
/// Configured relative paths are honoured, as in the supervisor. The one deliberate difference
/// is the all-unset case: broca reports `Unresolvable` rather than guessing a relative `.config`.
/// This ladder is separate from the data-home ladder, which uses `XDG_DATA_HOME` and `.local/share`.
///
/// An injected lookup lets tests exercise every rung without changing the process environment.
pub fn config_home_with(env: impl Fn(&str) -> Option<OsString>) -> ConfigHome {
    if let Some(xdg) = non_empty(env("XDG_CONFIG_HOME")) {
        return ConfigHome::Resolved {
            path: PathBuf::from(xdg).join("cortexkit/broca"),
            explicit: false,
        };
    }
    #[cfg(windows)]
    {
        if let Some(app_data) = non_empty(env("APPDATA")) {
            return ConfigHome::Resolved {
                path: PathBuf::from(app_data).join("cortexkit").join("broca"),
                explicit: false,
            };
        }
        if let Some(profile) = non_empty(env("USERPROFILE")) {
            return ConfigHome::Resolved {
                path: PathBuf::from(profile)
                    .join("AppData")
                    .join("Roaming")
                    .join("cortexkit")
                    .join("broca"),
                explicit: false,
            };
        }
    }
    if let Some(home) = non_empty(env("HOME")) {
        return ConfigHome::Resolved {
            path: PathBuf::from(home).join(".config/cortexkit/broca"),
            explicit: false,
        };
    }
    ConfigHome::Unresolvable
}

/// `config_home_with` against the real process environment.
pub fn config_home() -> ConfigHome {
    config_home_with(|key| std::env::var_os(key))
}

/// The path to one config file, or `Unresolvable` if no config home can be formed.
///
/// `explicit_env` is the file's own override (`BROCA_CONFIG_PATH` and friends) and stays
/// EXCLUSIVE: a file override never falls back to a discovered path. Its provenance travels
/// with the path so loaders can distinguish an absent optional file from a broken override.
/// Auth-method and vault-handle loaders reject missing overrides. The `broca.jsonc` loaders
/// keep shipped defaults for a missing file; boot refuses a missing named `broca.jsonc` first,
/// through [`require_explicit_file`].
pub fn config_file_with(
    env: impl Fn(&str) -> Option<OsString>,
    explicit_env: &str,
    file_name: &str,
) -> ConfigHome {
    if let Some(explicit) = non_empty(env(explicit_env)) {
        return ConfigHome::Resolved {
            path: PathBuf::from(explicit),
            explicit: true,
        };
    }
    match config_home_with(env) {
        ConfigHome::Resolved { path, .. } => ConfigHome::Resolved {
            path: path.join(file_name),
            explicit: false,
        },
        ConfigHome::Unresolvable => ConfigHome::Unresolvable,
    }
}

/// `config_file_with` against the real process environment.
pub fn config_file(explicit_env: &str, file_name: &str) -> ConfigHome {
    config_file_with(|key| std::env::var_os(key), explicit_env, file_name)
}

/// Refuse a file override that names no file.
///
/// Every loader of a shared file such as `broca.jsonc` treats a missing file as "no
/// overrides" and keeps shipped defaults, which is right for the discovered default path.
/// For a path the operator named, absence is almost always a typo, and taking defaults would
/// silently drop every override in the file (account selection, credential aliases, provider
/// endpoints, archiving) while health stays green. Boot checks the named file once, before
/// any loader runs, so the refusal names the variable and the path.
pub fn require_explicit_file_with(
    env: impl Fn(&str) -> Option<OsString>,
    explicit_env: &str,
    file_name: &str,
) -> Result<(), String> {
    let ConfigHome::Resolved {
        path,
        explicit: true,
    } = config_file_with(env, explicit_env, file_name)
    else {
        return Ok(());
    };
    match std::fs::metadata(&path) {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Err(format!(
            "{explicit_env} names {} but no file is there; refusing to start with shipped \
             defaults in place of that file's overrides",
            path.display()
        )),
        Err(error) => Err(format!(
            "{explicit_env} names {}, which cannot be checked: {error}",
            path.display()
        )),
    }
}

/// `require_explicit_file_with` against the real process environment.
pub fn require_explicit_file(explicit_env: &str, file_name: &str) -> Result<(), String> {
    require_explicit_file_with(|key| std::env::var_os(key), explicit_env, file_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// A named file that is not there refuses, naming the variable and the path; a named file
    /// that exists, and a discovered default path that does not, are both accepted.
    #[test]
    fn a_named_config_file_must_exist() {
        let dir =
            std::env::temp_dir().join(format!("broca-explicit-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let present = dir.join("broca.jsonc");
        std::fs::write(&present, b"{}").unwrap();
        let missing = dir.join("brcoa.jsonc");
        let named = |path: &std::path::Path| {
            let path = path.to_owned();
            move |key: &str| (key == "BROCA_CONFIG_PATH").then(|| path.clone().into_os_string())
        };
        let error = require_explicit_file_with(named(&missing), "BROCA_CONFIG_PATH", "broca.jsonc")
            .expect_err("a named file that is not there must refuse");
        assert!(error.contains("BROCA_CONFIG_PATH"), "{error}");
        assert!(error.contains(&missing.display().to_string()), "{error}");
        require_explicit_file_with(named(&present), "BROCA_CONFIG_PATH", "broca.jsonc").unwrap();
        let home = dir.join("no-config-here");
        require_explicit_file_with(
            |key| (key == "HOME").then(|| home.clone().into_os_string()),
            "BROCA_CONFIG_PATH",
            "broca.jsonc",
        )
        .expect("a discovered default path may be absent");
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<OsString> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        move |key: &str| map.get(key).map(OsString::from)
    }

    #[test]
    fn config_home_rungs_match_the_shared_supervisor_golden() {
        use sha2::{Digest, Sha256};
        let bytes =
            include_bytes!("../tests/fixtures/subconscious-f4e71180/config_home_resolution.json");
        assert_eq!(
            format!("{:x}", Sha256::digest(bytes)),
            "4b28353a4f477c4ae64ba9cad981ae11583bf417267bc25626c4c5a383b3fc9d",
            "refresh the supervisor fixture and its reviewed SHA-256 together"
        );
        let golden: serde_json::Value = serde_json::from_slice(bytes).unwrap();
        let mut checked = 0;
        for case in golden["cases"].as_array().unwrap() {
            let platform = case["platform"].as_str().unwrap();
            if !(platform == "any"
                || platform == "windows" && cfg!(windows)
                || platform == "unix" && cfg!(unix))
            {
                continue;
            }
            // A lookup into just this row clears all inherited home variables without racing
            // other tests by changing the process environment.
            let env = |key: &str| case["env"][key].as_str().map(OsString::from);
            let name = case["name"].as_str().unwrap();
            let expect = PathBuf::from(case["expect"].as_str().unwrap());
            let expected = if matches!(
                name,
                "all_unset_falls_to_relative" | "windows_all_unset_falls_to_relative"
            ) {
                // In the all-unset case, broca refuses instead of using subc's relative .config.
                assert_eq!(expect, PathBuf::from(".config"));
                ConfigHome::Unresolvable
            } else {
                ConfigHome::Resolved {
                    path: expect.join("cortexkit").join("broca"),
                    explicit: false,
                }
            };
            assert_eq!(config_home_with(env), expected, "{name}");
            checked += 1;
        }
        assert_eq!(checked, if cfg!(windows) { 8 } else { 6 });
    }

    /// THE DEFECT THIS MODULE EXISTS FOR: the daemon honours XDG_CONFIG_HOME and broca did not.
    #[test]
    fn xdg_config_home_wins_over_home() {
        let env = env_of(&[("XDG_CONFIG_HOME", "/xdg"), ("HOME", "/home/u")]);
        assert_eq!(
            config_home_with(env),
            ConfigHome::Resolved {
                path: PathBuf::from("/xdg/cortexkit/broca"),
                explicit: false
            },
            "an operator who exports XDG_CONFIG_HOME moves the daemon's config; broca must follow"
        );
    }

    /// The rung every read takes on a default host, and the one a real-environment test cannot
    /// reach without mutating the process.
    #[test]
    fn home_is_the_fallback_when_xdg_is_absent() {
        let env = env_of(&[("HOME", "/home/u")]);
        assert_eq!(
            config_home_with(env),
            ConfigHome::Resolved {
                path: PathBuf::from("/home/u/.config/cortexkit/broca"),
                explicit: false
            },
        );
    }

    /// An exported-but-empty XDG_CONFIG_HOME must not resolve config to `/cortexkit/broca`.
    #[test]
    fn an_empty_value_is_treated_as_unset() {
        let env = env_of(&[("XDG_CONFIG_HOME", ""), ("HOME", "/home/u")]);
        assert_eq!(
            config_home_with(env),
            ConfigHome::Resolved {
                path: PathBuf::from("/home/u/.config/cortexkit/broca"),
                explicit: false
            },
        );
    }

    /// UNRESOLVABLE IS NOT "NO OVERRIDES". Both used to be `None`, and they mean opposite
    /// things: one is an operator's deliberate silence, the other is broca unable to look.
    #[test]
    fn no_home_and_no_xdg_is_unresolvable_rather_than_a_path() {
        assert_eq!(config_home_with(env_of(&[])), ConfigHome::Unresolvable);
    }

    /// The file-level override is EXCLUSIVE: set-and-wrong must win and then fail, never fall
    /// back to a discoverable path. Asserted with a resolvable HOME present, so the test fails
    /// if the explicit value is treated as a first preference rather than an exclusive one.
    #[test]
    fn an_explicit_path_is_exclusive_and_does_not_fall_back() {
        let env = env_of(&[
            ("BROCA_CONFIG_PATH", "/tmp/rig/broca.jsonc"),
            ("XDG_CONFIG_HOME", "/xdg"),
            ("HOME", "/home/u"),
        ]);
        assert_eq!(
            config_file_with(env, "BROCA_CONFIG_PATH", "broca.jsonc"),
            ConfigHome::Resolved {
                path: PathBuf::from("/tmp/rig/broca.jsonc"),
                explicit: true
            },
        );
    }

    /// Every config file resolves through ONE ladder. This is the regression guard for the
    /// three-copies state: a rung added here reaches all of them, and a file that grows its own
    /// resolver fails this by diverging from its siblings.
    #[test]
    fn every_config_file_shares_one_directory() {
        let env = env_of(&[("XDG_CONFIG_HOME", "/xdg")]);
        for (var, file) in [
            ("BROCA_CONFIG_PATH", "broca.jsonc"),
            ("BROCA_AUTH_METHODS_PATH", "auth-methods.json"),
            ("BROCA_VAULT_HANDLES_PATH", "vault-handles.json"),
        ] {
            assert_eq!(
                config_file_with(&env, var, file),
                ConfigHome::Resolved {
                    path: PathBuf::from(format!("/xdg/cortexkit/broca/{file}")),
                    explicit: false
                },
                "{file} must resolve under the same config home as its siblings"
            );
        }
    }
}

#[cfg(test)]
mod config_loading {
    use super::*;
    use crate::auth_routing::{AuthMethod, MethodConfig, METHODS_PATH_ENV};
    use crate::billing_routing::ChargeBasisTable;
    use crate::cache_routing::{CacheTtlTable, CONFIG_PATH_ENV};
    use crate::vault_config::{VaultHandles, HANDLES_PATH_ENV};
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let root = broca_test_support::scratch_root().join(format!(
                "broca-config-loading-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&root).expect("fresh test directory");
            Self(root)
        }

        fn path(&self, file: &str) -> PathBuf {
            self.0.join("cortexkit/broca").join(file)
        }

        fn resolve(&self, var: &str, file: &str, explicit: bool) -> ConfigHome {
            config_file_with(
                |key| {
                    if explicit && key == var {
                        Some(self.path(file).into_os_string())
                    } else if key == "XDG_CONFIG_HOME" {
                        Some(self.0.clone().into_os_string())
                    } else {
                        None
                    }
                },
                var,
                file,
            )
        }

        fn write(&self, file: &str, contents: &str) {
            use std::io::Write;
            let path = self.path(file);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            options
                .open(path)
                .unwrap()
                .write_all(contents.as_bytes())
                .unwrap();
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }

    #[test]
    fn auth_explicit_missing_names_override_and_path() {
        let fixture = Fixture::new();
        let error = MethodConfig::load_resolved(fixture.resolve(
            METHODS_PATH_ENV,
            "auth-methods.json",
            true,
        ))
        .unwrap_err();
        assert!(error.contains(METHODS_PATH_ENV), "{error}");
        assert!(
            error.contains(&fixture.path("auth-methods.json").display().to_string()),
            "{error}"
        );
    }

    #[test]
    fn auth_discovered_missing_keeps_empty_map() {
        let fixture = Fixture::new();
        let config = MethodConfig::load_resolved(fixture.resolve(
            METHODS_PATH_ENV,
            "auth-methods.json",
            false,
        ))
        .unwrap();
        assert_eq!(config.method_for("anthropic"), None);
    }

    #[test]
    fn auth_explicit_present_loads_configured_method() {
        let fixture = Fixture::new();
        fixture.write("auth-methods.json", r#"{"defaults":{"anthropic":"oauth"}}"#);
        let config = MethodConfig::load_resolved(fixture.resolve(
            METHODS_PATH_ENV,
            "auth-methods.json",
            true,
        ))
        .unwrap();
        assert_eq!(config.method_for("anthropic"), Some(AuthMethod::Oauth));
    }

    #[test]
    fn vault_missing_is_fatal_for_both_origins() {
        let fixture = Fixture::new();
        for explicit in [false, true] {
            assert!(
                VaultHandles::load_resolved(fixture.resolve(
                    HANDLES_PATH_ENV,
                    "vault-handles.json",
                    explicit
                ))
                .is_err(),
                "explicit={explicit}"
            );
        }
    }

    #[test]
    fn vault_explicit_missing_names_override_and_path() {
        let fixture = Fixture::new();
        let error = VaultHandles::load_resolved(fixture.resolve(
            HANDLES_PATH_ENV,
            "vault-handles.json",
            true,
        ))
        .err()
        .expect("missing explicit handle file must fail")
        .to_string();
        assert!(error.contains(HANDLES_PATH_ENV), "{error}");
        assert!(
            error.contains(&fixture.path("vault-handles.json").display().to_string()),
            "{error}"
        );
    }

    #[test]
    fn vault_parse_diagnostic_never_echoes_handle_contents() {
        let fixture = Fixture::new();
        let secret = "ckh_secret_config_loading_sentinel";
        fixture.write(
            "vault-handles.json",
            &format!(r#"{{"handles":{{}},"{secret}":1}}"#),
        );
        for explicit in [false, true] {
            let error = VaultHandles::load_resolved(fixture.resolve(
                HANDLES_PATH_ENV,
                "vault-handles.json",
                explicit,
            ))
            .err()
            .expect("unknown field must fail parsing")
            .to_string();
            assert!(error.contains("parse vault handle file"), "{error}");
            assert!(!error.contains(secret), "parse diagnostic leaked a handle");
        }
    }

    #[test]
    fn cache_missing_keeps_shipped_defaults_for_both_origins() {
        let fixture = Fixture::new();
        for explicit in [false, true] {
            let table = CacheTtlTable::load_resolved(fixture.resolve(
                CONFIG_PATH_ENV,
                "broca.jsonc",
                explicit,
            ))
            .unwrap();
            assert_eq!(
                table,
                CacheTtlTable::shipped_defaults(),
                "explicit={explicit}"
            );
        }
    }

    #[test]
    fn billing_missing_keeps_shipped_defaults_for_both_origins() {
        let fixture = Fixture::new();
        for explicit in [false, true] {
            let table = ChargeBasisTable::load_resolved(fixture.resolve(
                CONFIG_PATH_ENV,
                "broca.jsonc",
                explicit,
            ))
            .unwrap();
            assert_eq!(
                table,
                ChargeBasisTable::shipped_defaults(),
                "explicit={explicit}"
            );
        }
    }
}
