use std::path::PathBuf;

use broca_session_store::backend::SessionAddr;
use broca_session_store::codec::Id128;
use broca_session_store::session::restore::{LogCut, RestoreError, Restored};
use broca_wal::StorageClass;

use super::Report;

fn report() -> Report {
    let mut report = Report::new(StorageClass::Durable, &SessionAddr("ab12".into()));
    report.session(&broca_protocol::BindIdentity::new("/p", "opencode2", "ses_head"));
    report
}

fn cut() -> LogCut {
    LogCut {
        log: Id128([0x11; 16]),
        record: 9,
        lacking: "medium 1111/a/x".into(),
        first_cut: 8,
        at: 100,
        was: 160,
        sidecar: PathBuf::from("/s/logs/ab12/11.restore-cut-42"),
    }
}

/// Each outcome reads as one line naming the session, what restore did and why, with the facts
/// behind it beside the line.
#[test]
fn each_outcome_is_reported_as_one_line_with_its_facts() {
    let kept = report().restored(Restored::Kept { version: 7 });
    assert_eq!(kept["outcome"], "kept");
    assert_eq!(kept["version"], 7);
    assert_eq!(
        kept["report"],
        "durable ab12 (ses_head): kept at version 7, everything it names is there"
    );
    assert_eq!(kept["session"]["harness"], "opencode2");

    let rolled = report().restored(Restored::Repaired {
        from: 9,
        to: 6,
        written: Some(10),
        cut: Some(cut()),
    });
    assert_eq!(rolled["outcome"], "rolled_back");
    assert_eq!(
        (&rolled["from_version"], &rolled["to_version"], &rolled["written_version"]),
        (&serde_json::json!(9), &serde_json::json!(6), &serde_json::json!(10))
    );
    assert_eq!(
        rolled["report"],
        "durable ab12 (ses_head): rolled back from version 9 to version 6, written as version 10; \
         log 11111111111111111111111111111111 cut at record 8, since record 9 names medium \
         1111/a/x, which the backup lacks; the 60 bytes cut are kept in \
         /s/logs/ab12/11.restore-cut-42"
    );

    let cut_only = report().restored(Restored::Repaired {
        from: 9,
        to: 9,
        written: None,
        cut: Some(cut()),
    });
    assert_eq!(cut_only["outcome"], "log_cut");
    assert_eq!(cut_only["cut"]["record"], 9);
    assert_eq!(cut_only["cut"]["sidecar"], "/s/logs/ab12/11.restore-cut-42");
    assert!(cut_only.get("written_version").is_none());

    let lost = report().restored(Restored::Unrecoverable {
        lacking: vec![(9, "log 22".into()), (8, "sealed segment 33/l".into())],
    });
    assert_eq!(lost["outcome"], "unrecoverable");
    assert_eq!(
        lost["report"],
        "durable ab12 (ses_head): unrecoverable: version 9 lacks log 22; version 8 lacks \
         sealed segment 33/l; nothing changed"
    );
    assert_eq!(lost["lacking"][1]["what"], "sealed segment 33/l");

    let refused = report().restored(Restored::Refused {
        reason: "a recovery is in progress".into(),
    });
    assert_eq!(refused["outcome"], "refused");
    assert_eq!(
        refused["report"],
        "durable ab12 (ses_head): not restored, a recovery is in progress; nothing changed"
    );

    assert_eq!(
        report().error(RestoreError::KeyUnavailable)["outcome"],
        "key_unavailable"
    );
    assert_eq!(report().busy("run_active")["outcome"], "busy");
}
