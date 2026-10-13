//! What a production warning actually looks like once it has been through the fleet logger.
//!
//! The messages used to carry a hand-written `broca:` prefix because the daemon log is shared
//! and an unattributed line gets read as whoever's the reader assumes. The logger supplies
//! that attribution itself, as the record's logger name, so the prefix came out of the text --
//! and "the logger supplies it" is an assumption until something reads a rendered line back.
//! This test reads one.
//!
//! It goes through a REAL call site rather than emitting a record of its own. A synthetic
//! `warn!` here would prove the crate renders what it is given and nothing about what broca's
//! own sites declare: the `target:` on the site is the only thing that decides the component,
//! and a site that lost it, or named the wrong one, would still pass a synthetic check.

use std::path::{Path, PathBuf};

/// The site under test: loading an archive config whose retention is under the safe floor
/// warns, on `broca.archive`, before returning the config.
const BELOW_FLOOR_CONFIG: &str = r#"{"wal_archive_idle_retention_days": 7}"#;

fn rig_dir() -> PathBuf {
    let dir = broca_test_support::scratch_root()
        .join(format!("broca-fleet-log-shape-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create the rig directory");
    dir
}

fn write_config(dir: &Path) -> PathBuf {
    let path = dir.join("broca.jsonc");
    std::fs::write(&path, BELOW_FLOOR_CONFIG).expect("write the archive config");
    path
}

#[test]
fn a_production_warning_carries_the_module_id_and_the_component_it_declared() {
    let dir = rig_dir();
    let logs = dir.join("logs");

    let mut config = cortexkit_log::Config::in_dir("broca", &logs);
    // Pin the filter instead of inheriting `CK_LOG` from whoever runs the suite. This test is
    // about the SHAPE of a record, so a host that had exported a stricter spec would drop the
    // record and fail it for a reason that has nothing to do with rendering.
    config.spec = Some("info".to_string());
    let handle = cortexkit_log::init(config).expect("no other subscriber in this test process");

    let path = write_config(&dir);
    broca_module_serve::wal_archive::WalArchiveConfig::load(&path)
        .expect("a retention below the floor is a warning, not a refusal");

    let rendered = std::fs::read_to_string(handle.path()).expect("read the log segment");
    let line = rendered
        .lines()
        .find(|line| line.contains("is below 14 days"))
        .unwrap_or_else(|| {
            panic!("the archive retention warning never reached the segment:\n{rendered}")
        });

    let record = cortexkit_log::parse_line(line).unwrap_or_else(|error| {
        panic!("the record is not a readable fleet log line: {error}: {line}")
    });

    assert_eq!(
        record.module_id, "broca",
        "the record must name the module that wrote it, so a shared log stays attributable: \
         {line}"
    );
    assert_eq!(
        record.logger, "broca.archive",
        "the site declared the archive component and the rendered logger must show it, or \
         nothing can be filtered by component: {line}"
    );
    assert_eq!(
        record.level,
        cortexkit_log::ParsedLevel::Warn,
        "a site that printed unconditionally before must still be at or above the default \
         root level: {line}"
    );
    assert!(
        record
            .body
            .starts_with("wal_archive_idle_retention_days=7 is below 14 days"),
        "the message begins with its own words: the attribution now lives in the logger name, \
         and a surviving `broca:` prefix would repeat it: {line}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
