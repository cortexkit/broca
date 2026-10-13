//! Usage errors and fast probes must not create daemon state or production log segments.
use std::path::PathBuf;
use std::process::Output;

struct Scratch {
    root: PathBuf,
}

impl Scratch {
    fn new(name: &str) -> Self {
        let root = broca_test_support::scratch_root()
            .join(format!("broca-early-args-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let config_dir = root.join("config/cortexkit/broca");
        std::fs::create_dir_all(&config_dir).unwrap();
        // Loading this config emits a warning, exposing accidental config/logger startup.
        std::fs::write(
            config_dir.join("broca.jsonc"),
            r#"{"wal_archive_idle_retention_days": 7}"#,
        )
        .unwrap();
        Self { root }
    }

    fn spawn(&self, args: &[&str]) -> Output {
        broca_test_support::retry_executable_file_busy(|| {
            broca_test_support::ckdev_command(
                env!("CARGO_BIN_EXE_ck-broca"),
                broca_test_support::scratch_root(),
            )
            .args(args)
            .env("XDG_DATA_HOME", self.root.join("data"))
            .env("XDG_CONFIG_HOME", self.root.join("config"))
            .env("BROCA_STATE_ROOT", self.root.join("state"))
            .env_remove("BROCA_CONFIG_PATH")
            .env_remove("SUBC_LAUNCH_NONCE_FD")
            .output()
        })
        .unwrap()
    }

    fn assert_untouched(&self) {
        assert!(
            !self.root.join("data/cortexkit/broca/logs").exists(),
            "usage errors and probes must not initialise the logger"
        );
        assert!(!self.root.join("state").exists(), "state root created");
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[test]
fn unknown_flags_refuse_before_logger_or_state_creation() {
    let scratch = Scratch::new("unknown");
    for flag in ["--ck-domain", "--no-such-flag"] {
        let output = scratch.spawn(&[flag]);
        assert_eq!(output.status.code(), Some(2));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(flag), "{stderr}");
        assert!(stderr.contains("valid arguments:"), "{stderr}");
        scratch.assert_untouched();
    }
}

#[test]
fn missing_subc_refuses_before_logger_or_state_creation() {
    let scratch = Scratch::new("missing");
    for args in [vec![], vec!["--subc"], vec!["--subc", "--no-such-flag"]] {
        let output = scratch.spawn(&args);
        assert_eq!(output.status.code(), Some(2));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("--subc"), "{stderr}");
        assert!(stderr.contains("valid arguments:"), "{stderr}");
        scratch.assert_untouched();
    }
}

#[test]
fn fast_probes_remain_side_effect_free() {
    let scratch = Scratch::new("probes");
    for flag in ["--version", "-V", "--wal-known-features"] {
        let output = scratch.spawn(&[flag]);
        assert!(output.status.success(), "{output:?}");
        let stdout = String::from_utf8(output.stdout).unwrap();
        if flag == "--wal-known-features" {
            let mut names = broca_wal::KNOWN_FEATURES.to_vec();
            names.sort_unstable();
            let expected: String = names.iter().map(|name| format!("{name}\n")).collect();
            assert_eq!(stdout, expected);
        } else {
            assert_eq!(stdout, format!("ck-broca {}\n", env!("CARGO_PKG_VERSION")));
        }
        scratch.assert_untouched();
    }
}
