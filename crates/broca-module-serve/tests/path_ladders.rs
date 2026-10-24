//! Native path parity without mutating the test process's environment.

use broca_module_serve::config_home::{config_file_with, config_home_with, ConfigHome};
use broca_module_serve::state_root::{resolve_state_root_with, StateRootError, StateRootOrigin};
use serde_json::Value;
use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Command;

fn cases() -> Vec<Value> {
    serde_json::from_str::<Value>(include_str!("fixtures/path_ladders.json")).unwrap()["cases"]
        .as_array()
        .unwrap()
        .clone()
}

fn applies(case: &Value) -> bool {
    match case["platform"].as_str().unwrap() {
        "any" => true,
        "unix" => cfg!(unix),
        "windows" => cfg!(windows),
        other => panic!("unknown golden platform {other}"),
    }
}

#[test]
fn state_root_matches_shared_path_golden() {
    let mut checked = 0;
    for case in cases().iter().filter(|case| applies(case)) {
        let name = case["name"].as_str().unwrap();
        let env = |key: &str| case["env"][key].as_str().map(OsString::from);
        let result = resolve_state_root_with(env);
        if let Some(path) = case["state"].as_str() {
            let resolved = result.unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(resolved.path, PathBuf::from(path), "{name}");
            let explicit = case["env"]["BROCA_STATE_ROOT"]
                .as_str()
                .is_some_and(|value| !value.is_empty());
            assert_eq!(
                resolved.origin,
                if explicit {
                    StateRootOrigin::Explicit
                } else {
                    StateRootOrigin::DataHome
                },
                "{name}"
            );
        } else {
            let error = result.expect_err(name);
            match (case["state_error"].as_str().unwrap(), error) {
                ("unresolvable", StateRootError::Unresolvable) => {}
                (expected, StateRootError::RelativeDataHome { var, .. }) => {
                    assert_eq!(var, expected, "{name}");
                }
                (expected, actual) => panic!("{name}: expected {expected}, got {actual:?}"),
            }
        }
        checked += 1;
    }
    assert_eq!(checked, if cfg!(windows) { 15 } else { 5 });
}

#[test]
fn config_home_matches_shared_path_golden() {
    let mut checked = 0;
    for case in cases().iter().filter(|case| applies(case)) {
        let name = case["name"].as_str().unwrap();
        let env = |key: &str| case["env"][key].as_str().map(OsString::from);
        let expected = match case["config"].as_str() {
            Some(path) => ConfigHome::Resolved {
                path: PathBuf::from(path),
                explicit: false,
            },
            None => ConfigHome::Unresolvable,
        };
        assert_eq!(config_home_with(env), expected, "{name}");
        for (var, file) in [
            ("BROCA_CONFIG_PATH", "broca.jsonc"),
            ("BROCA_AUTH_METHODS_PATH", "auth-methods.json"),
        ] {
            let file_expected = match &expected {
                ConfigHome::Resolved { path, .. } => ConfigHome::Resolved {
                    path: path.join(file),
                    explicit: false,
                },
                ConfigHome::Unresolvable => ConfigHome::Unresolvable,
            };
            assert_eq!(
                config_file_with(env, var, file),
                file_expected,
                "{name}: {file}"
            );
            assert_eq!(
                config_file_with(
                    |key| {
                        if key == var {
                            Some(OsString::from("explicit-relative.json"))
                        } else {
                            env(key)
                        }
                    },
                    var,
                    file
                ),
                ConfigHome::Resolved {
                    path: PathBuf::from("explicit-relative.json"),
                    explicit: true
                },
                "{name}: exclusive {var}"
            );
        }
        checked += 1;
    }
    assert_eq!(checked, if cfg!(windows) { 15 } else { 5 });
}

#[test]
fn python_reads_the_same_path_goldens() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let script = root.join("tests/path_ladders.py");
    let python = if Command::new("python3").arg("--version").output().is_ok() {
        "python3"
    } else {
        "python"
    };
    let output = Command::new(python)
        .arg("-B")
        .arg(script)
        .arg("-v")
        .output()
        .expect("Python is required to verify the script-side path ladders");
    assert!(
        output.status.success(),
        "Python path golden failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
