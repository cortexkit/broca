//! Session logs the operator scripts under `scripts/` are tested against, written by the real
//! runner so the scripts meet the exact frames broca writes.
//!
//! Each fixture is a resume close-out: two parallel tool calls whose intents were durable when
//! the process died, then the resume that pairs each with an `outcome_unknown` result and seals
//! the episode Interrupted.
//!
//! - `resume-close-out-sealed.wal`: the whole close-out. The run is sealed, so no script may
//!   count it as live.
//! - `resume-close-out-unsealed.wal`: cut just after the `resume_started` record. The episode
//!   still has no terminal, so it is live, and the resume record must not make it look sealed.
//!
//! The committed files carry wall-clock timestamps, so this test compares the sequence of
//! record kinds, not bytes. Set `BROCA_REGEN_SCRIPT_FIXTURES=1` to rewrite them.

use crate::scenarios;

use std::path::PathBuf;

use broca_crash_harness::{KillPoint, KillTrigger, LogImage, RunnerHandle};
use broca_wal::WalRecord;

fn kinds(log: &LogImage) -> Vec<String> {
    log.records()
        .iter()
        .map(|record| {
            serde_json::to_value(record).unwrap()["type"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect()
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../scripts/fixtures")
}

#[tokio::test]
async fn the_script_fixtures_are_what_the_runner_writes_for_a_resume_close_out() {
    let handle = RunnerHandle::new(scenarios::TWO_EDITS).await;
    let after_intents = handle
        .reference
        .log
        .records()
        .iter()
        .rposition(|record| matches!(record, WalRecord::ToolDispatchIntent { .. }))
        .unwrap()
        + 1;
    let kill = handle
        .kill_at(
            KillPoint::RecordBoundary {
                records: after_intents,
                torn: false,
            },
            KillTrigger::Truncate,
        )
        .await;
    let resumed = handle.resume(&kill).await;
    handle.check(&kill, &resumed);
    let sealed = resumed.log.bytes.clone();
    let unsealed = resumed.log.bytes[..resumed.log.end_of_records(after_intents + 1)].to_vec();
    let unsealed_log = LogImage::parse(unsealed.clone());
    assert!(matches!(
        unsealed_log.records().last(),
        Some(WalRecord::ResumeStarted { .. })
    ));

    let dir = fixture_dir();
    let fixtures = [
        ("resume-close-out-sealed.wal", sealed),
        ("resume-close-out-unsealed.wal", unsealed),
    ];
    if std::env::var_os("BROCA_REGEN_SCRIPT_FIXTURES").is_some() {
        std::fs::create_dir_all(&dir).unwrap();
        for (name, bytes) in &fixtures {
            std::fs::write(dir.join(name), bytes).unwrap();
        }
    }
    for (name, bytes) in &fixtures {
        let committed = LogImage::read(&dir.join(name));
        assert_eq!(committed.torn_tail, 0, "{name} is torn");
        assert_eq!(
            kinds(&committed),
            kinds(&LogImage::parse(bytes.clone())),
            "{name} no longer matches what the runner writes; regenerate it with \
             BROCA_REGEN_SCRIPT_FIXTURES=1 and rerun the script suites"
        );
    }
}
