//! Pinned-bytes fixtures for every record and field gated under `deferred-call/v1`, and for the
//! one informational record beside them, `question_withdrawal_acknowledged`.
//!
//! Each fixture is the exact record JSON a writer produces, framed at seq 1 and fence 1. The
//! digest is a literal computed outside this crate (SHA-256 over the version byte, seq and fence
//! as u64 LE, and the payload), so a change to the framing or to a record's bytes cannot move the
//! expectation together with the code.
//!
//! A reader that knows `deferred-call/v1` decodes a gated record with serde, which would skip a
//! field its types lack WITHOUT ANY ERROR. So decoding alone proves nothing about the fields:
//! these tests re-encode every decoded fixture and require the original bytes back, and name the
//! first field that did not survive. [`GATED_FIELDS`] lists every gated record and field, so a
//! field without a fixture that carries it fails too.

use super::*;
use crate::record::{
    DeferredDecision, DeferredExecutor, LateDecision, LateExecution, LateOutcome, OnExpiry,
    ProviderHeld, WithdrawElicitation, WithdrawalAnswer,
};
use serde_json::Value;

/// One pinned record: its name, the features its frame requires (sorted, as written), the record
/// JSON, and the frame digest computed outside this crate.
struct Fixture {
    name: &'static str,
    requires: &'static [&'static str],
    record: &'static str,
    digest: &'static str,
}

const FIXTURES: &[Fixture] = &[
    Fixture {
        name: "tool_call_deferred_agent_scope",
        requires: &["deferred-call/v1"],
        record: r#"{"type":"tool_call_deferred","batch_id":"r-b1","call_id":"t1","executor":"broca","module":"aft","tool":"edit","schema_pin":{"schema_digest":"sha256:5f2c","semantics":2},"args_jcs":"{\"f\":\"a\"}","args_hash":"910b42ae2f4a973d3af5f056c18c62742efb4f7c4af344444e47e071411d6e57","scope":{"owner":{"kind":"reserved","module_id":"prefrontal-core"},"ref":"head-1","scope_epoch":3},"ask":{"prompt":"Edit a?","options":["run","skip"],"expires_at_ms":1790000723000,"on_expiry":"deny","material_damage":true,"late_execution":"execute"}}"#,
        digest: "afcf82fa0427219fa8dec52b25cea4d9657b21b901cd0a4b18fab47a744cd8b1",
    },
    Fixture {
        name: "tool_call_deferred_automation_scope",
        requires: &["deferred-call/v1"],
        record: r#"{"type":"tool_call_deferred","batch_id":"r-b2","call_id":"t2","executor":"broca","tool":"publish","args_jcs":"{\"f\":\"a\"}","args_hash":"910b42ae2f4a973d3af5f056c18c62742efb4f7c4af344444e47e071411d6e57","scope":{"owner":{"kind":"reserved","module_id":"prefrontal-core"},"ref":"head-1","scope_epoch":3,"flow_id":"flow-1","sender":{"kind":"reserved","module_id":"basal"}},"ask":{"prompt":"Publish?","expires_at_ms":1790000724000,"on_expiry":"deny","material_damage":false,"late_execution":"notify_only"}}"#,
        digest: "f150f09ebe054a2c14da32bc1a96046ec3ae36dc3a636a2dff1e2a9453014b22",
    },
    Fixture {
        name: "approval_recorded_approved",
        requires: &["deferred-call/v1"],
        record: r#"{"type":"approval_recorded","call_key":"5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:7","call_id":"t1","elicitation_id":"el-1","decision":"approved","approval_digest":"sha256:ab12"}"#,
        digest: "c384b429ef1cd84de796c257382eaddd58d86a902b77583cbe5f6b14d8fce6f1",
    },
    Fixture {
        name: "approval_recorded_declined",
        requires: &["deferred-call/v1"],
        record: r#"{"type":"approval_recorded","call_key":"5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:7","call_id":"t1","elicitation_id":"el-1","decision":"declined","closing_result":{"tool_call_id":"t1","output":{"kind":"error","message":"pre_tool_declined: the call was declined"},"is_error":true}}"#,
        digest: "47466a3f1ab35ffc4e34f5a00ed4202c6378b97601131be31dbdd41557b88cd1",
    },
    Fixture {
        name: "approval_recorded_expired",
        requires: &["deferred-call/v1"],
        record: r#"{"type":"approval_recorded","call_key":"5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:7","call_id":"t1","elicitation_id":"el-1","decision":"expired","closing_result":{"tool_call_id":"t1","output":{"kind":"error","message":"pre_tool_expired: the approval question expired"},"is_error":true}}"#,
        digest: "f566c12face35d7ac162cc87d440f5cf282c69ec94284246853bbe0999c4f201",
    },
    Fixture {
        name: "approval_recorded_unavailable_unfiled",
        requires: &["deferred-call/v1"],
        record: r#"{"type":"approval_recorded","call_key":"5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:7","call_id":"t1","decision":"unavailable","closing_result":{"tool_call_id":"t1","output":{"kind":"error","message":"pre_tool_unavailable: the approval question could not be filed"},"is_error":true}}"#,
        digest: "383e45cdc02c06d168bcc7d47f2a14358b47bfd64b66af024aa4bc4da86287a4",
    },
    Fixture {
        name: "awaiting_approval",
        requires: &["deferred-call/v1"],
        record: r#"{"type":"awaiting_approval","batch_id":"r-b1","pending":[{"call_key":"5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:7","call_id":"t1","elicitation_id":"el-1","expires_at_ms":1790000723000},{"call_key":"5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:8","call_id":"t2","expires_at_ms":1790000724000}]}"#,
        digest: "ba4fbc372849de2dc0c714294190ec1f5f7630dc96d30eff61411b73b3aba5ab",
    },
    Fixture {
        name: "tool_dispatch_intent_approved",
        requires: &["deferred-call/v1"],
        record: r#"{"type":"tool_dispatch_intent","batch_id":"r-b1","tool_call_id":"t1","tool_name":"edit","args":{"f":"a"},"approval_digest":"sha256:ab12","call_key":"5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:7"}"#,
        digest: "217268d95384cac5d94cf62ddb3490dad263a5f42939d82774bc24ed7ba6a2dd",
    },
    Fixture {
        name: "tool_dispatch_intent_approved_with_module",
        requires: &["deferred-call/v1", "dispatch-module/v1"],
        record: r#"{"type":"tool_dispatch_intent","batch_id":"r-b1","tool_call_id":"t1","tool_name":"edit","args":{"f":"a"},"module_id":"aft","schema_pin":{"schema_digest":"sha256:5f2c","semantics":2},"approval_digest":"sha256:ab12","call_key":"5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:7"}"#,
        digest: "eebe31a96f6e903cdf28deff9196f5804f4350b21138b68b986d7938fb0c80f0",
    },
    Fixture {
        name: "tool_result_withdraw_elicitation",
        requires: &["deferred-call/v1"],
        record: r#"{"type":"tool_result","batch_id":"r-b1","result":{"tool_call_id":"t1","output":{"kind":"error","message":"tool not run: batch interrupted"},"is_error":true},"outcome":"interrupted","withdraw_elicitation":{"call_key":"5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:7","elicitation_id":"el-1"}}"#,
        digest: "9fe20a592bdb1c8398b8cab695c66250e854faac6fb12c98d2b1dec4eea91200",
    },
    Fixture {
        name: "tool_result_withdraw_elicitation_unfiled",
        requires: &["deferred-call/v1"],
        record: r#"{"type":"tool_result","batch_id":"r-b1","result":{"tool_call_id":"t1","output":{"kind":"error","message":"tool not run: batch interrupted"},"is_error":true},"outcome":"interrupted","withdraw_elicitation":{"call_key":"5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:7"}}"#,
        digest: "10e3956c5e2becd505d8c5a6fa7a806fba0626dd764f127c89e18985a5476a35",
    },
    Fixture {
        name: "tool_result_provider_held_decision_pending",
        requires: &["deferred-call/v1"],
        record: r#"{"type":"tool_result","batch_id":"r-b1","result":{"tool_call_id":"t1","output":{"kind":"text","text":"decision pending: el-2"},"is_error":false},"outcome":"completed","provider_held":{"call_key":"5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:9","module":"plexus","elicitation_id":"el-2"}}"#,
        digest: "0e5799489eca5c84c60d55b2b6bc63ba43c7cf40ff31774c6d324cb7e84731ca",
    },
    Fixture {
        name: "tool_result_provider_held_in_flight",
        requires: &["deferred-call/v1"],
        record: r#"{"type":"tool_result","batch_id":"r-b1","result":{"tool_call_id":"t1","output":{"kind":"error","message":"tool outcome unknown (interrupted for review): ask in flight"},"is_error":true},"outcome":"unknown","provider_held":{"call_key":"5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:9","module":"plexus"}}"#,
        digest: "24c7277496f42f95d3cf45a56a474aa82e1fcb7c29591ef16065f1e7d61517dc",
    },
    Fixture {
        name: "late_approval_recorded_approved",
        requires: &["deferred-call/v1"],
        record: r#"{"type":"late_approval_recorded","call_key":"5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:7","elicitation_id":"el-1","decision":"approved","approval_digest":"sha256:ab12","late_execution":"execute"}"#,
        digest: "c05871ae53cfb138b07a5b3c2982cd033790004c95919852d3e6e56d39f08e47",
    },
    Fixture {
        name: "late_approval_recorded_declined",
        requires: &["deferred-call/v1"],
        record: r#"{"type":"late_approval_recorded","call_key":"5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:7","elicitation_id":"el-1","decision":"declined"}"#,
        digest: "475531163c25c0f39ba964e6b015d709b91766ea2828bdbf505469a1be9418d7",
    },
    Fixture {
        name: "late_approval_recorded_none",
        requires: &["deferred-call/v1"],
        record: r#"{"type":"late_approval_recorded","call_key":"5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:7","elicitation_id":"el-1","decision":"none"}"#,
        digest: "20d1710c2af1f3d9287fedae5b82c06741be7f003d8eb30ebc52ac8f2306d749",
    },
    Fixture {
        name: "late_dispatch_intent",
        requires: &["deferred-call/v1"],
        record: r#"{"type":"late_dispatch_intent","call_key":"5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:7","approval_digest":"sha256:ab12","batch_id":"late:5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:7"}"#,
        digest: "9e3679d3e55214fe9b3e34f9e5821f64df32f4d8e794f9b488bfaed6db0664d8",
    },
    Fixture {
        name: "late_result_completed",
        requires: &["deferred-call/v1"],
        record: r#"{"type":"late_result","call_key":"5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:7","outcome":"completed","result":{"tool_call_id":"t1","output":{"kind":"text","text":"done"},"is_error":false}}"#,
        digest: "b2270506ad50726001bbb0ee8548580952e9b0131d4ef48aef26caf18293a32d",
    },
    Fixture {
        name: "late_result_not_sent",
        requires: &["deferred-call/v1"],
        record: r#"{"type":"late_result","call_key":"5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:7","outcome":"not_sent","result":{"tool_call_id":"t1","output":{"kind":"error","message":"pre_tool_denied: refused on second validation"},"is_error":true}}"#,
        digest: "b69a4551fd8285c5fb611c7801b05a084aa468434e3027713e1a5bda9681fa5f",
    },
    Fixture {
        name: "late_result_unknown",
        requires: &["deferred-call/v1"],
        record: r#"{"type":"late_result","call_key":"5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:7","outcome":"unknown","result":{"tool_call_id":"t1","output":{"kind":"error","message":"tool outcome unknown (interrupted for review): late dispatch"},"is_error":true}}"#,
        digest: "917ea3d5a9c22337ef79fb6823d766946c5d5527c134d4579f2c681085bcb20a",
    },
    Fixture {
        name: "question_withdrawal_acknowledged",
        requires: &[],
        record: r#"{"type":"question_withdrawal_acknowledged","call_key":"5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:7","elicitation_id":"el-1","answer":"acknowledged"}"#,
        digest: "5699dbbae201bdb707d2cb867c44b1bce9153a7cab81864994ba4f7fb7586932",
    },
    Fixture {
        name: "question_withdrawal_not_sent",
        requires: &[],
        record: r#"{"type":"question_withdrawal_acknowledged","call_key":"5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:7","answer":"not_sent","reason":"already_answered"}"#,
        digest: "6a4a0a735e3f67e5f3f89a5120c6e352533d8a649b7d2fab611707c006964230",
    },
];

/// Every record and field gated under `deferred-call/v1`, as `(record type, field path)`. An empty
/// path is the whole record kind. Nested paths are dot-separated. Every leaf field of each gated
/// record and field is listed, including the optional ones, so each is carried by at least one
/// fixture.
const GATED_FIELDS: &[(&str, &str)] = &[
    ("tool_call_deferred", ""),
    ("tool_call_deferred", "batch_id"),
    ("tool_call_deferred", "call_id"),
    ("tool_call_deferred", "executor"),
    ("tool_call_deferred", "module"),
    ("tool_call_deferred", "tool"),
    ("tool_call_deferred", "schema_pin.schema_digest"),
    ("tool_call_deferred", "schema_pin.semantics"),
    ("tool_call_deferred", "args_jcs"),
    ("tool_call_deferred", "args_hash"),
    ("tool_call_deferred", "scope.owner"),
    ("tool_call_deferred", "scope.ref"),
    ("tool_call_deferred", "scope.scope_epoch"),
    ("tool_call_deferred", "scope.flow_id"),
    ("tool_call_deferred", "scope.sender"),
    ("tool_call_deferred", "ask.prompt"),
    ("tool_call_deferred", "ask.options"),
    ("tool_call_deferred", "ask.expires_at_ms"),
    ("tool_call_deferred", "ask.on_expiry"),
    ("tool_call_deferred", "ask.material_damage"),
    ("tool_call_deferred", "ask.late_execution"),
    ("approval_recorded", ""),
    ("approval_recorded", "call_key"),
    ("approval_recorded", "call_id"),
    ("approval_recorded", "elicitation_id"),
    ("approval_recorded", "decision"),
    ("approval_recorded", "approval_digest"),
    ("approval_recorded", "closing_result.tool_call_id"),
    ("approval_recorded", "closing_result.output"),
    ("approval_recorded", "closing_result.is_error"),
    ("awaiting_approval", ""),
    ("awaiting_approval", "batch_id"),
    ("awaiting_approval", "pending"),
    ("tool_dispatch_intent", "approval_digest"),
    ("tool_dispatch_intent", "call_key"),
    ("tool_result", "withdraw_elicitation.call_key"),
    ("tool_result", "withdraw_elicitation.elicitation_id"),
    ("tool_result", "provider_held.call_key"),
    ("tool_result", "provider_held.module"),
    ("tool_result", "provider_held.elicitation_id"),
    ("late_approval_recorded", ""),
    ("late_approval_recorded", "call_key"),
    ("late_approval_recorded", "elicitation_id"),
    ("late_approval_recorded", "decision"),
    ("late_approval_recorded", "approval_digest"),
    ("late_approval_recorded", "late_execution"),
    ("late_dispatch_intent", ""),
    ("late_dispatch_intent", "call_key"),
    ("late_dispatch_intent", "approval_digest"),
    ("late_dispatch_intent", "batch_id"),
    ("late_result", ""),
    ("late_result", "call_key"),
    ("late_result", "outcome"),
    ("late_result", "result.tool_call_id"),
    ("late_result", "result.output"),
    ("late_result", "result.is_error"),
];

/// The fields of each `awaiting_approval` pending entry, checked inside the list.
const PENDING_ENTRY_FIELDS: &[&str] = &["call_key", "call_id", "elicitation_id", "expires_at_ms"];

/// The shipped reader, whose replay, projection and session reads interpret deferred calls.
fn deferred_call_reader() -> Vec<&'static str> {
    KNOWN_FEATURES.to_vec()
}

/// A reader released before `deferred-call/v1`: the shipped table without that name.
fn pre_deferred_call_reader() -> Vec<&'static str> {
    KNOWN_FEATURES
        .iter()
        .copied()
        .filter(|name| *name != DEFERRED_CALL_FEATURE)
        .collect()
}

fn hex_digest(hex: &str) -> [u8; 32] {
    assert_eq!(hex.len(), 64, "a SHA-256 digest is 64 hex digits: {hex}");
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).unwrap();
    }
    out
}

/// The exact frame a writer produces for `fixture`, built from the literals alone: the payload
/// is the bare record for an ungated record, else the gated envelope.
fn pinned_frame(fixture: &Fixture) -> Vec<u8> {
    let (ver, payload) = if fixture.requires.is_empty() {
        (FRAMING_VERSION, fixture.record.to_string())
    } else {
        let names: Vec<String> = fixture
            .requires
            .iter()
            .map(|name| format!("\"{name}\""))
            .collect();
        (
            GATED_FRAMING_VERSION,
            format!(
                r#"{{"requires":[{}],"record":{}}}"#,
                names.join(","),
                fixture.record
            ),
        )
    };
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.push(ver);
    out.extend_from_slice(&1u64.to_le_bytes());
    out.extend_from_slice(&1u64.to_le_bytes());
    out.extend_from_slice(&hex_digest(fixture.digest));
    out.extend_from_slice(payload.as_bytes());
    out
}

fn decode_with(fixture: &Fixture, known: &[&str]) -> WalRecord {
    match decode_next_with(&pinned_frame(fixture), known) {
        DecodeStep::Record { framed, consumed } => {
            assert_eq!(consumed, pinned_frame(fixture).len(), "{}", fixture.name);
            assert_eq!((framed.seq, framed.fence), (1, 1), "{}", fixture.name);
            framed.record
        }
        DecodeStep::Corrupt(error) => panic!("{}: refused as corruption: {error}", fixture.name),
        DecodeStep::NewerWriter { missing, .. } => {
            panic!(
                "{}: refused as a newer writer missing {missing:?}",
                fixture.name
            )
        }
        _ => panic!("{}: expected a record", fixture.name),
    }
}

/// The first path present in `expected` that is absent from, or different in, `actual`.
fn first_lost_field(expected: &Value, actual: &Value, path: &str) -> Option<String> {
    match (expected, actual) {
        (Value::Object(want), Value::Object(got)) => want.iter().find_map(|(key, value)| {
            let here = if path.is_empty() {
                key.clone()
            } else {
                format!("{path}.{key}")
            };
            match got.get(key) {
                None => Some(here),
                Some(found) => first_lost_field(value, found, &here),
            }
        }),
        (Value::Array(want), Value::Array(got)) if want.len() == got.len() => want
            .iter()
            .zip(got)
            .enumerate()
            .find_map(|(i, (w, g))| first_lost_field(w, g, &format!("{path}[{i}]"))),
        _ if expected == actual => None,
        _ => Some(path.to_string()),
    }
}

fn field<'a>(record: &'a Value, path: &str) -> Option<&'a Value> {
    if path.is_empty() {
        return Some(record);
    }
    path.split('.')
        .try_fold(record, |value, key| value.get(key))
        .filter(|value| !value.is_null())
}

/// Every fixture decodes under a `deferred-call/v1` reader as the record kind it names, and
/// re-encodes to exactly its pinned frame: same header, same digest, same payload bytes, and the
/// same `requires` list, which the record derives on its own.
#[test]
fn every_deferred_call_fixture_decodes_and_re_encodes_to_its_pinned_frame() {
    let _table = lock_feature_table_for_test();
    for fixture in FIXTURES {
        let record = decode_with(fixture, &deferred_call_reader());
        assert_ne!(record, WalRecord::Unknown, "{}", fixture.name);
        assert_eq!(
            record.required_features(),
            fixture.requires,
            "{}: the record must derive its own feature list",
            fixture.name
        );
        assert_eq!(
            encode(1, 1, &record).unwrap(),
            pinned_frame(fixture),
            "{}: re-encoding must give back the pinned frame",
            fixture.name
        );
    }
}

/// THE NO-SILENT-SKIP PROOF. A `deferred-call/v1` reader decodes every gated field: each fixture's
/// record, decoded and re-serialized, still holds every field of the original with the same value
/// (a field the reader's types lacked would be dropped by serde with no error, and is named
/// here), and every gated field in [`GATED_FIELDS`] is carried by at least one fixture, so no
/// field escapes the check by having no fixture.
#[test]
fn a_deferred_call_reader_decodes_every_gated_field() {
    let _table = lock_feature_table_for_test();
    let decoded: Vec<(Value, Value)> = FIXTURES
        .iter()
        .map(|fixture| {
            let record = decode_with(fixture, &deferred_call_reader());
            let original: Value = serde_json::from_str(fixture.record).unwrap();
            let read_back = serde_json::to_value(&record).unwrap();
            if let Some(lost) = first_lost_field(&original, &read_back, "") {
                panic!(
                    "{}: field `{lost}` was not decoded by a deferred-call/v1 reader",
                    fixture.name
                );
            }
            (original, read_back)
        })
        .collect();

    for (kind, path) in GATED_FIELDS {
        let carriers: Vec<&Value> = decoded
            .iter()
            .filter(|(original, _)| original["type"] == *kind)
            .filter(|(original, _)| field(original, path).is_some())
            .map(|(_, read_back)| read_back)
            .collect();
        assert!(
            !carriers.is_empty(),
            "no fixture carries the gated field {kind}.{path}"
        );
        for read_back in carriers {
            assert!(
                field(read_back, path).is_some(),
                "{kind}.{path} is missing after a deferred-call/v1 reader decoded it"
            );
        }
    }
    for entry_field in PENDING_ENTRY_FIELDS {
        assert!(
            decoded.iter().any(|(original, read_back)| {
                original["type"] == "awaiting_approval"
                    && original["pending"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .zip(read_back["pending"].as_array().unwrap())
                        .any(|(want, got)| {
                            !want[entry_field].is_null() && want[entry_field] == got[entry_field]
                        })
            }),
            "no awaiting_approval fixture carries and decodes pending[].{entry_field}"
        );
    }
}

/// The typed values a later reader acts on come back as written, not merely as JSON that
/// re-serializes: the key, the decision, the closing result, the approval digest and the
/// withdrawal obligations.
#[test]
fn gated_fields_decode_into_their_typed_values() {
    let _table = lock_feature_table_for_test();
    let by_name = |name: &str| {
        let fixture = FIXTURES.iter().find(|f| f.name == name).unwrap();
        decode_with(fixture, &deferred_call_reader())
    };
    let key = "5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:7";

    let WalRecord::ToolCallDeferred {
        executor,
        module,
        schema_pin,
        args_jcs,
        args_hash,
        scope,
        ask,
        ..
    } = by_name("tool_call_deferred_agent_scope")
    else {
        panic!("expected ToolCallDeferred");
    };
    assert_eq!(executor, DeferredExecutor::Broca);
    assert_eq!(module.as_deref(), Some("aft"));
    assert_eq!(schema_pin.map(|pin| pin.semantics), Some(2));
    assert_eq!(args_jcs, r#"{"f":"a"}"#);
    assert_eq!(
        args_hash, "910b42ae2f4a973d3af5f056c18c62742efb4f7c4af344444e47e071411d6e57",
        "sha256 of the argument bytes, computed outside this crate"
    );
    assert_eq!((scope.scope_ref.as_str(), scope.scope_epoch), ("head-1", 3));
    assert_eq!(scope.flow_id, None);
    assert_eq!(ask.expires_at_ms, 1_790_000_723_000);
    assert_eq!(ask.options, ["run", "skip"]);
    assert_eq!(ask.on_expiry, OnExpiry::Deny);
    assert!(ask.material_damage);
    assert_eq!(ask.late_execution, LateExecution::Execute);

    let WalRecord::ToolCallDeferred { scope, ask, .. } =
        by_name("tool_call_deferred_automation_scope")
    else {
        panic!("expected ToolCallDeferred");
    };
    assert_eq!(scope.flow_id.as_deref(), Some("flow-1"));
    assert!(scope.sender.is_some());
    assert_eq!(ask.late_execution, LateExecution::NotifyOnly);

    let WalRecord::ApprovalRecorded {
        call_key,
        decision,
        approval_digest,
        closing_result,
        ..
    } = by_name("approval_recorded_approved")
    else {
        panic!("expected ApprovalRecorded");
    };
    assert_eq!(call_key, key);
    assert_eq!(decision, DeferredDecision::Approved);
    assert_eq!(approval_digest.as_deref(), Some("sha256:ab12"));
    assert!(closing_result.is_none());

    for (name, want) in [
        ("approval_recorded_declined", DeferredDecision::Declined),
        ("approval_recorded_expired", DeferredDecision::Expired),
        (
            "approval_recorded_unavailable_unfiled",
            DeferredDecision::Unavailable,
        ),
    ] {
        let WalRecord::ApprovalRecorded {
            decision,
            closing_result,
            elicitation_id,
            ..
        } = by_name(name)
        else {
            panic!("expected ApprovalRecorded");
        };
        assert_eq!(decision, want, "{name}");
        assert!(
            closing_result.is_some_and(|result| result.is_error),
            "{name}"
        );
        assert_eq!(
            elicitation_id.is_none(),
            want == DeferredDecision::Unavailable,
            "{name}"
        );
    }

    let WalRecord::ToolDispatchIntent {
        approval_digest,
        call_key,
        module_id,
        ..
    } = by_name("tool_dispatch_intent_approved_with_module")
    else {
        panic!("expected ToolDispatchIntent");
    };
    assert_eq!(approval_digest.as_deref(), Some("sha256:ab12"));
    assert_eq!(call_key.as_deref(), Some(key));
    assert_eq!(module_id.as_deref(), Some("aft"));

    let WalRecord::ToolResult {
        withdraw_elicitation,
        provider_held,
        ..
    } = by_name("tool_result_withdraw_elicitation")
    else {
        panic!("expected ToolResult");
    };
    assert_eq!(
        withdraw_elicitation,
        Some(WithdrawElicitation {
            call_key: key.into(),
            elicitation_id: Some("el-1".into())
        })
    );
    assert_eq!(provider_held, None);

    let WalRecord::ToolResult { provider_held, .. } =
        by_name("tool_result_provider_held_decision_pending")
    else {
        panic!("expected ToolResult");
    };
    assert_eq!(
        provider_held,
        Some(ProviderHeld {
            call_key: "5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:9".into(),
            module: "plexus".into(),
            elicitation_id: Some("el-2".into())
        })
    );

    let WalRecord::LateApprovalRecorded {
        decision,
        late_execution,
        ..
    } = by_name("late_approval_recorded_approved")
    else {
        panic!("expected LateApprovalRecorded");
    };
    assert_eq!(decision, LateDecision::Approved);
    assert_eq!(late_execution, Some(LateExecution::Execute));

    let WalRecord::LateResult { outcome, .. } = by_name("late_result_not_sent") else {
        panic!("expected LateResult");
    };
    assert_eq!(outcome, LateOutcome::NotSent);
}

/// A build released before `deferred-call/v1` refuses every gated fixture as a newer writer
/// naming exactly that feature. It never reads the record with the gated field ignored.
#[test]
fn a_reader_without_deferred_call_v1_refuses_every_gated_fixture_as_newer_writer() {
    let _table = lock_feature_table_for_test();
    let older = pre_deferred_call_reader();
    for fixture in FIXTURES.iter().filter(|f| !f.requires.is_empty()) {
        match decode_next_with(&pinned_frame(fixture), &older) {
            DecodeStep::NewerWriter { ver, missing } => {
                assert_eq!(ver, GATED_FRAMING_VERSION, "{}", fixture.name);
                assert_eq!(missing, [DEFERRED_CALL_FEATURE], "{}", fixture.name);
            }
            DecodeStep::Corrupt(error) => {
                panic!("{}: refused as corruption: {error}", fixture.name)
            }
            _ => panic!("{}: expected NewerWriter", fixture.name),
        }
    }
}

/// Every gated record or field lands in a gated frame even through plain [`encode`] with no
/// requirement passed: the record adds `deferred-call/v1` on its own, so no writer can put a
/// must-understand value where an older build reads it unchecked.
#[test]
fn a_gated_record_is_never_written_in_an_ordinary_frame() {
    let _table = lock_feature_table_for_test();
    for fixture in FIXTURES.iter().filter(|f| !f.requires.is_empty()) {
        let record: WalRecord = serde_json::from_str(fixture.record).unwrap();
        assert!(
            record.required_features().contains(&DEFERRED_CALL_FEATURE),
            "{}",
            fixture.name
        );
        let bytes = encode_with_requires(1, 1, &[], &record).unwrap();
        assert_eq!(bytes[4], GATED_FRAMING_VERSION, "{}", fixture.name);
    }
}

/// `question_withdrawal_acknowledged` is informational: an ordinary version-1 frame whose payload
/// is the bare record, no required feature (so it never extends a frame's `requires`), readable
/// by a reader that knows no feature at all, and skipped by every session reader.
#[test]
fn question_withdrawal_acknowledged_is_ungated_ver_1_and_informational() {
    let _table = lock_feature_table_for_test();
    let ungated: Vec<&Fixture> = FIXTURES.iter().filter(|f| f.requires.is_empty()).collect();
    assert_eq!(ungated.len(), 2);
    for fixture in ungated {
        let record = decode_with(fixture, &[]);
        assert!(
            matches!(record, WalRecord::QuestionWithdrawalAcknowledged { .. }),
            "{}",
            fixture.name
        );
        assert!(record.required_features().is_empty(), "{}", fixture.name);
        assert!(record.is_informational(), "{}", fixture.name);
        let bytes = encode(1, 1, &record).unwrap();
        assert_eq!(bytes[4], FRAMING_VERSION, "{}", fixture.name);
        assert_eq!(&bytes[HEADER_LEN..], fixture.record.as_bytes());
        assert_eq!(bytes, pinned_frame(fixture), "{}", fixture.name);
    }
    let WalRecord::QuestionWithdrawalAcknowledged { answer, reason, .. } = decode_with(
        FIXTURES
            .iter()
            .find(|f| f.name == "question_withdrawal_not_sent")
            .unwrap(),
        &[],
    ) else {
        panic!("expected QuestionWithdrawalAcknowledged");
    };
    assert_eq!(answer, WithdrawalAnswer::NotSent);
    assert_eq!(reason.as_deref(), Some("already_answered"));
}

/// An executor value this build does not know (one reserved for calls a tool provider executes,
/// which share the feature) decodes, is kept verbatim and re-serializes to the stored string,
/// instead of failing the whole WAL as corrupt.
#[test]
fn an_unknown_executor_decodes_and_keeps_its_string() {
    let _table = lock_feature_table_for_test();
    let base = FIXTURES[0].record;
    let provider = base.replacen(r#""executor":"broca""#, r#""executor":"provider""#, 1);
    assert_ne!(provider, base);
    let record: WalRecord = serde_json::from_str(&provider).unwrap();
    let WalRecord::ToolCallDeferred { executor, .. } = &record else {
        panic!("expected ToolCallDeferred");
    };
    assert_eq!(*executor, DeferredExecutor::Other("provider".into()));
    assert_eq!(serde_json::to_string(&record).unwrap(), provider);
}

/// The production decoder, not a test-expanded capability table, must decode every gated
/// fixture without dropping any field. Re-encoding catches serde's otherwise silent omissions.
#[test]
fn this_build_decodes_every_deferred_call_frame_and_gated_field() {
    let _table = lock_feature_table_for_test();
    assert_eq!(DEFERRED_CALL_FEATURE, "deferred-call/v1");
    assert!(is_feature_name(DEFERRED_CALL_FEATURE));
    assert!(KNOWN_FEATURES.contains(&DEFERRED_CALL_FEATURE));
    for fixture in FIXTURES.iter().filter(|f| !f.requires.is_empty()) {
        match decode_next(&pinned_frame(fixture)) {
            DecodeStep::Record { framed, consumed } => {
                assert_eq!(consumed, pinned_frame(fixture).len(), "{}", fixture.name);
                assert_eq!(
                    encode(framed.seq, framed.fence, &framed.record).unwrap(),
                    pinned_frame(fixture),
                    "{}: every gated field must survive",
                    fixture.name
                );
            }
            _ => panic!(
                "{}: this build must decode a deferred-call/v1 frame",
                fixture.name
            ),
        }
    }
}

/// A record carrying deferred-call fields beside step-transform hook output, or beside a
/// dispatch module, requires the union of the features, in the sorted order the framing
/// writes, and a reader knowing all of them reads it back byte for byte. A reader missing any
/// one of them refuses it.
#[test]
fn deferred_call_fields_combine_with_the_other_gated_fields() {
    let _table = lock_feature_table_for_test();
    let fixture = |name: &str| FIXTURES.iter().find(|f| f.name == name).unwrap().record;
    let cases = [
        (
            fixture("tool_dispatch_intent_approved").replacen(
                r#","approval_digest""#,
                r#","hooks":{"ops":[]},"approval_digest""#,
                1,
            ),
            vec![DEFERRED_CALL_FEATURE, STEP_TRANSFORMS_FEATURE],
        ),
        (
            fixture("tool_dispatch_intent_approved_with_module").replacen(
                r#","approval_digest""#,
                r#","hooks":{"ops":[]},"approval_digest""#,
                1,
            ),
            vec![
                DEFERRED_CALL_FEATURE,
                DISPATCH_MODULE_FEATURE,
                STEP_TRANSFORMS_FEATURE,
            ],
        ),
        (
            fixture("tool_result_withdraw_elicitation").replacen(
                r#","withdraw_elicitation""#,
                r#","hooks":{"ops":[]},"withdraw_elicitation""#,
                1,
            ),
            vec![DEFERRED_CALL_FEATURE, STEP_TRANSFORMS_FEATURE],
        ),
        (
            fixture("tool_result_provider_held_in_flight").replacen(
                r#","provider_held""#,
                r#","hooks":{"ops":[]},"provider_held""#,
                1,
            ),
            vec![DEFERRED_CALL_FEATURE, STEP_TRANSFORMS_FEATURE],
        ),
    ];
    for (json, requires) in cases {
        assert!(json.contains(r#""hooks":{"ops":[]}"#), "{json}");
        let record: WalRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(record.required_features(), requires.as_slice(), "{json}");
        assert_eq!(serde_json::to_string(&record).unwrap(), json);
        let bytes = encode(1, 1, &record).unwrap();
        let mut known = deferred_call_reader();
        known.extend(requires.iter().copied());
        match decode_next_with(&bytes, &known) {
            DecodeStep::Record { framed, .. } => assert_eq!(framed.record, record, "{json}"),
            _ => panic!("a reader knowing {requires:?} must read {json}"),
        }
        for missing in &requires {
            let without: Vec<&str> = known.iter().copied().filter(|n| n != missing).collect();
            assert!(
                matches!(
                    decode_next_with(&bytes, &without),
                    DecodeStep::NewerWriter { missing: ref m, .. } if m == &[missing.to_string()]
                ),
                "a reader without {missing} must refuse {json}"
            );
        }
    }
}
