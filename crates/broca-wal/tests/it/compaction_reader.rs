use std::{path::Path, process::Command};

use broca_protocol::content::Message;
use broca_wal::{
    archive::ArchiveWriter, compaction_replacement_sha256, CompactionCursor, CompactionDisposition,
    CompactionRange, CompactionStabilityRank, CompactionWait, CompactionWaitExit, FileWal,
    OpaqueJson, RejectedCompaction, Wal, WalRecord,
};
use serde_json::{json, Value};

const RECORDS: &str = include_str!("../fixtures/compaction_records.jsonl");

#[test]
fn compaction_record_bytes_and_inline_hashes_are_pinned() {
    for bytes in RECORDS.lines() {
        let record: WalRecord = serde_json::from_str(bytes).unwrap();
        assert_ne!(record, WalRecord::Unknown);
        assert_eq!(serde_json::to_string(&record).unwrap(), bytes);
        if let WalRecord::CompactionMessage {
            replacement,
            sha256,
            ..
        } = record
        {
            assert_eq!(compaction_replacement_sha256(&replacement).unwrap(), sha256);
        }
    }
    assert_eq!(
        compaction_replacement_sha256(&[]).unwrap(),
        "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"
    );
}

#[test]
fn compaction_closed_values_and_unsigned_metadata_refuse_unknowns() {
    for disposition in [
        "noop",
        "wait",
        "refuse",
        "superseded_request",
        "late",
        "range_inverted",
        "stale_version",
        "range_beyond_newest",
        "structural",
        "failed_call",
        "timeout",
    ] {
        let value = Value::from(disposition);
        let decoded: CompactionDisposition = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), value);
    }
    for exit in [
        "ready",
        "bound",
        "cap",
        "exceeded",
        "cancel",
        "stopped",
        "interrupted",
    ] {
        let value = Value::from(exit);
        let decoded: CompactionWaitExit = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), value);
    }
    for (index, field) in [(1, "kind"), (3, "disposition"), (6, "exited")] {
        let mut value: Value = serde_json::from_str(RECORDS.lines().nth(index).unwrap()).unwrap();
        value[field] = "unknown".into();
        assert!(
            serde_json::from_value::<WalRecord>(value).is_err(),
            "{field}"
        );
    }
    assert!(serde_json::from_value::<CompactionRange>(json!({"from":-1,"to":0})).is_err());
    assert!(
        serde_json::from_value::<CompactionStabilityRank>(json!({"index":0,"rank":-1})).is_err()
    );
    assert!(serde_json::from_value::<CompactionCursor>(json!({"after_ordinal":0})).is_err());
    assert!(serde_json::from_value::<CompactionWait>(json!({"reason":"wait"})).is_err());
}

/// The storage mirrors decode the actual frozen provider fields, not a fixture derived
/// from the mirrors. Case names from commons remain in failure messages.
#[test]
fn compaction_storage_mirrors_decode_frozen_provider_vectors() {
    let setup: Value = serde_json::from_str(include_str!(
        "../fixtures/compaction-provider-v1/setup.json"
    ))
    .unwrap();
    let answers: Value = serde_json::from_str(include_str!(
        "../fixtures/compaction-provider-v1/answers.json"
    ))
    .unwrap();
    let status: Value = serde_json::from_str(include_str!(
        "../fixtures/compaction-provider-v1/status.json"
    ))
    .unwrap();
    let mut messages = 0;
    let mut ranks = 0;
    let mut conditions = 0;
    let mut waits = 0;
    let mut cursors = 0;
    let mut absent_cursors = 0;
    for case in setup["answers"]
        .as_array()
        .unwrap()
        .iter()
        .chain(answers["answers"].as_array().unwrap())
    {
        let name = case["name"].as_str().unwrap();
        let answer = &case["answer"];
        let message = answer.get("initial").or_else(|| answer.get("compaction"));
        if let Some(message) = message {
            let rejected: RejectedCompaction = serde_json::from_value(message.clone())
                .unwrap_or_else(|error| panic!("{name}: replacement identity: {error}"));
            assert_eq!(
                serde_json::to_value(rejected.range).unwrap(),
                message["range"],
                "{name}"
            );
            let range: CompactionRange = serde_json::from_value(message["range"].clone())
                .unwrap_or_else(|error| panic!("{name}: range: {error}"));
            assert_eq!(
                serde_json::to_value(range).unwrap(),
                message["range"],
                "{name}"
            );
            let version: u64 = serde_json::from_value(message["version"].clone()).unwrap();
            let id: String = serde_json::from_value(message["compaction_id"].clone()).unwrap();
            assert_eq!(Value::from(version), message["version"], "{name}");
            assert_eq!(Value::from(id), message["compaction_id"], "{name}");
            let replacement: Vec<Message> = serde_json::from_value(message["replacement"].clone())
                .unwrap_or_else(|error| panic!("{name}: replacement: {error}"));
            assert_eq!(
                serde_json::to_value(replacement).unwrap(),
                message["replacement"],
                "{name}"
            );
            messages += 1;
        }
        if let Some(value) = answer.get("stability") {
            let stability: Vec<CompactionStabilityRank> = serde_json::from_value(value.clone())
                .unwrap_or_else(|error| panic!("{name}: stability: {error}"));
            assert_eq!(serde_json::to_value(stability).unwrap(), *value, "{name}");
            ranks += 1;
        }
        if let Some(value) = answer.get("call_when") {
            let text = serde_json::to_string(value).unwrap();
            let opaque = OpaqueJson::from_json_text(&text).unwrap();
            let decoded: OpaqueJson =
                serde_json::from_str(&serde_json::to_string(&opaque).unwrap()).unwrap();
            assert_eq!(decoded.as_json_text(), text, "{name}");
            assert_eq!(
                serde_json::from_str::<Value>(decoded.as_json_text()).unwrap(),
                *value,
                "{name}"
            );
            conditions += 1;
        }
        if answer["answer"] == "wait" {
            let value = json!({"reason": answer["reason"], "bound_ms": answer["bound_ms"]});
            let wait: CompactionWait = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(serde_json::to_value(wait).unwrap(), value, "{name}");
            waits += 1;
        }
    }
    for case in status["requests"].as_array().unwrap() {
        let request = &case["request"];
        let mut value = json!({"lineage_id": request["lineage_id"]});
        if let Some(after) = request.get("after_ordinal") {
            value["after_ordinal"] = after.clone();
        } else {
            absent_cursors += 1;
        }
        let cursor: CompactionCursor = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(
            serde_json::to_value(cursor).unwrap(),
            value,
            "{}",
            case["name"]
        );
        cursors += 1;
    }
    assert!(messages >= 5 && ranks > 0 && conditions > 0 && waits > 0 && cursors > 0 && absent_cursors > 0,
        "vectors must exercise each mirror: {messages}/{ranks}/{conditions}/{waits}/{cursors}/{absent_cursors}");
}

#[test]
fn setup_call_when_preserves_unknown_conditions_and_verbatim_json_text() {
    let bytes = RECORDS.lines().nth(1).unwrap();
    let record: WalRecord = serde_json::from_str(bytes).unwrap();
    let WalRecord::CompactionMessage {
        call_when: Some(ref call_when),
        request_id: None,
        cursor: None,
        ..
    } = record
    else {
        panic!("setup freezes declarations and has no step fence/cursor");
    };
    assert_eq!(
        call_when.as_json_text(),
        r#"{"default":0.80,"every_n_steps":1e1}"#
    );
    assert_eq!(serde_json::to_string(&record).unwrap(), bytes);
}

fn verify(root: &Path, mode: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_wal-verify"))
        .arg(mode)
        .arg(root)
        .arg(root.join("baseline"))
        .output()
        .unwrap()
}

fn archive(root: &Path, live: &Path) {
    std::fs::create_dir_all(root.join("wal-archive")).unwrap();
    let mut writer = ArchiveWriter::create(root.join("wal-archive/fold-1.ark"))
        .unwrap()
        .without_feature_floor();
    writer
        .append("0123456789abcdef", &std::fs::read(live).unwrap())
        .unwrap();
    writer.seal().unwrap();
    std::fs::remove_file(live).unwrap();
}

#[test]
fn wal_verify_accepts_compaction_records_live_and_archived() {
    let root = broca_test_support::scratch_root()
        .join(format!("compaction-verify-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("wal")).unwrap();
    let live = root.join("wal/0123456789abcdef.wal");
    let mut wal = FileWal::open(&live)
        .unwrap()
        .with_feature_floor(root.join(broca_wal::FEATURE_FLOOR_FILENAME));
    for json in RECORDS.lines() {
        let record: WalRecord = serde_json::from_str(json).unwrap();
        wal.append(1, &record, true).unwrap();
    }
    drop(wal);
    let floor = broca_wal::read_floor(&root.join(broca_wal::FEATURE_FLOOR_FILENAME)).unwrap();
    assert!(floor.contains("compaction/v1") && floor.contains("compaction-wait/v1"));
    let output = verify(&root, "baseline");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("files 1 frames 7 bytes"));
    archive(&root, &live);
    let output = verify(&root, "check");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("all 1 baseline WAL identities present")
    );
    std::fs::remove_dir_all(root).unwrap();
}

/// Both frame digests remain valid: only the stored inline-content digest is false.
#[test]
fn wal_verify_rejects_mismatched_inline_replacements_live_and_archived() {
    let root = broca_test_support::scratch_root()
        .join(format!("compaction-bad-hash-{}", std::process::id()));
    for index in [1, 2] {
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("wal")).unwrap();
        let live = root.join("wal/0123456789abcdef.wal");
        let mut record: WalRecord =
            serde_json::from_str(RECORDS.lines().nth(index).unwrap()).unwrap();
        let WalRecord::CompactionMessage { sha256, .. } = &mut record else {
            unreachable!()
        };
        *sha256 = "0".repeat(64);
        std::fs::write(&live, broca_wal::framing::encode(1, 1, &record).unwrap()).unwrap();
        assert_eq!(
            broca_wal::replay_file(&live, 1024 * 1024).unwrap().len(),
            1,
            "a valid envelope is necessary to reach the inline digest check"
        );
        for archived in [false, true] {
            if archived {
                archive(&root, &live);
            }
            let output = verify(&root, "baseline");
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert_eq!(output.status.code(), Some(2), "{stderr}");
            assert!(
                stderr.contains("0123456789abcdef.wal")
                    && stderr.contains("seq=1")
                    && stderr.contains("inline replacement sha256 mismatch"),
                "{stderr}"
            );
            assert!(
                !root.join("baseline").exists(),
                "bad content must never establish a baseline"
            );
        }
    }
    std::fs::remove_dir_all(root).unwrap();
}
