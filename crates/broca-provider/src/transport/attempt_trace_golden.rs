use super::{Case, Event};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::OnceLock};

const PARENT: &str = "8032fae541e7b77fdc454cf8d37a024528e6a219";

fn events(actual: &[Event]) -> Value {
    Value::Array(actual.iter().map(|event| match event {
        Event::Send { ordinal, index, websocket } => {
            json!({"send": {"ordinal": ordinal, "index": index, "websocket": websocket}})
        }
        Event::Wait(delay) => json!({"wait_ns": delay.as_nanos()}),
        Event::EscapeClient => json!("escape_client"),
        Event::SwitchToHttps => json!("switch_to_https"),
        Event::Attempt { ordinal, outcome } => {
            json!({"attempt": {"ordinal": ordinal, "outcome": outcome}})
        }
        Event::Refresh { rejected, credential } => {
            json!({"refresh": {"rejected": rejected, "credential": credential}})
        }
        Event::Decision { action, reason } => {
            json!({"decision": {"action": action, "reason": reason}})
        }
        Event::End { retries_used, quiet, failed } => {
            json!({"end": {"retries_used": retries_used, "quiet": quiet, "failed": failed}})
        }
    }).collect())
}

pub(super) fn assert_or_capture(case: &Case, actual: &[Event]) {
    let description = format!("{case:?}");
    let key = format!("{:x}", Sha256::digest(description.as_bytes()));
    let observed = json!({"case": description, "events": events(actual)});
    // Capture ACTUAL driver events, never expected() output. Restore these files under
    // crates/broca-provider/src from revision 8032fae54 before capturing: attempt_policy.rs,
    // lib.rs, transport.rs, transport_codex_retry.rs, transport_websocket.rs. Leave all
    // scenarios and the transport harness unchanged, then run:
    // mkdir -p target/scratch && BROCA_PARENT_ATTEMPT_CAPTURE=target/scratch/parent-attempt-traces \
    //   scripts/linux-test.sh 'package(=broca-provider) & test(attempt_trace_)'
    // Merge the per-case JSON files, sorted by their SHA-256 filename, into the traces map in
    // attempt_trace_goldens.json. assert_trace compares actual to expected(case) first, so
    // attempt_trace_negative_control_rejects_a_changed_requested_delay panics before its
    // altered trace can overwrite a real parent trace.
    if let Some(directory) = std::env::var_os("BROCA_PARENT_ATTEMPT_CAPTURE") {
        let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(directory);
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            directory.join(format!("{key}.json")),
            serde_json::to_vec_pretty(&observed).unwrap(),
        ).unwrap();
        return;
    }
    static GOLDENS: OnceLock<Value> = OnceLock::new();
    let goldens = GOLDENS.get_or_init(|| {
        serde_json::from_str(include_str!("attempt_trace_goldens.json")).unwrap()
    });
    assert_eq!(goldens["parent"], PARENT);
    let frozen = goldens["traces"].get(&key).unwrap_or_else(|| {
        panic!("missing frozen parent trace for {key}: {case:?}")
    });
    assert_eq!(&observed, frozen, "wire/parent trace mismatch: {case:?}");
}
