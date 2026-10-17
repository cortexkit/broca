//! The `llm-runner/v1` role's ops as broca serves them: `role.describe`, `session.head`,
//! `run.result` and `session.baseline`, the per-call dispatch facts on a `session.read` page,
//! and the refusal codes and detail those ops answer with.
//!
//! The role defines these shapes (commons `cortexkit-role-llm-runner`). Most are written here as
//! broca's own types so every byte broca answers is pinned in this crate. The `role.describe`
//! answer and the steer delivery receipt are the role's own types instead, because what they
//! say is defined by the role rather than by broca; golden files pin their bytes all the same. A
//! consumer decodes all of them leniently, so every member that may be absent is skipped when
//! it is.

use broca_protocol::caller::SharedProviderError;
use serde::{Deserialize, Serialize};

use crate::CursorWire;

/// The role this module serves, as its manifest and `role.describe` spell it.
pub const LLM_RUNNER_V1: &str = "llm-runner/v1";

pub const OP_ROLE_DESCRIBE: &str = "role.describe";
pub const OP_SESSION_HEAD: &str = "session.head";
pub const OP_RUN_RESULT: &str = "run.result";
pub const OP_SESSION_BASELINE: &str = "session.baseline";

pub use broca_protocol::READ_REPLY_MAX_BYTES;
/// The `session.read` byte cap a page stops at when the request names none.
pub const READ_DEFAULT_MAX_BYTES: u64 = READ_REPLY_MAX_BYTES;
/// The largest `session.read` byte cap broca honours; a larger request is capped to it.
pub const READ_MAXIMUM_MAX_BYTES: u64 = READ_REPLY_MAX_BYTES;

/// A request member is malformed, unknown, or combined with one it excludes. `detail.field`
/// names it.
pub const INVALID_PARAMS: &str = "invalid_params";
/// A read names a lineage that is not the session's current one. The caller re-reads from the
/// tail.
pub const LINEAGE_CHANGED: &str = "lineage_changed";
/// `after_mid` names no message of the lineage.
pub const UNKNOWN_MID: &str = "unknown_mid";
/// `run.result` names no run of the session.
pub const UNKNOWN_RUN: &str = "unknown_run";
/// A send whose delivery mode the runner does not declare, with `detail.delivery` naming it.
/// broca declares every mode the role defines (`queue`, `steer` and `interrupt`), so it never
/// answers this code; it is listed so the code set matches the role's.
pub const DELIVERY_UNSUPPORTED: &str = "delivery_unsupported";

/// The `detail` of an `invalid_params` or `send_id_reuse` refusal: the request member at fault.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldDetail {
    pub field: String,
}

/// Which request member a decode of `params` as `T` failed on, when that can be told: the one
/// member whose removal lets the rest decode (an unknown member of a strict request, or a
/// malformed value), else the member serde names as missing or unknown.
pub fn failing_field<T: serde::de::DeserializeOwned>(
    params: &serde_json::Value,
    error: &serde_json::Error,
) -> Option<String> {
    if let Some(object) = params.as_object() {
        for key in object.keys() {
            let mut rest = object.clone();
            rest.remove(key);
            if serde_json::from_value::<T>(serde_json::Value::Object(rest)).is_ok() {
                return Some(key.clone());
            }
        }
    }
    let message = error.to_string();
    ["missing field `", "unknown field `"]
        .iter()
        .find_map(|prefix| {
            let start = message.find(prefix)? + prefix.len();
            let end = message[start..].find('`')?;
            Some(message[start..start + end].to_owned())
        })
}

/// One tool call of a `session.read` message: what broca recorded about its dispatch.
///
/// `tool_call_id` is the model's id, for display only: a model may use one id in several steps.
/// `call_key` is the call's own key, absent until the call has a durable dispatch intent;
/// `dispatched_to` is the module the call was sent to, absent for a call never sent and for a
/// call whose intent was written before broca recorded its module. `indeterminate` is true while
/// the call has a durable dispatch intent and no result: it may or may not have run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionReadToolCall {
    pub tool_call_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatched_to: Option<String>,
    #[serde(default)]
    pub indeterminate: bool,
}

/// `session.head` params: none. Strict, so a caller expecting a member that does not exist
/// learns so.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeadParams {}

/// The most recent run of a session, as `session.head` reports it. It describes that run,
/// never the transcript.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LastRunState {
    pub run_id: String,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// `session.head`: a session's head without message bodies. A session never written has no
/// lineage yet and answers every member absent; an answer with `lineage_id` carries
/// `updated_at`, the time of the session's newest durable write in milliseconds since the Unix
/// epoch.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeadResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage_id: Option<String>,
    /// The ordinal of the newest message; absent on an empty transcript.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_ordinal: Option<u64>,
    /// The last durable control unit, the same cursor a `session.read` page carries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<CursorWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_state: Option<LastRunState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<u64>,
    /// The session's prompt-cache clock, present once this broca process has sent a request of
    /// the session since the process started. Absent otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<crate::HeadCacheWire>,
    /// The newest title any send to the session recorded (on a run start, a queued send or a
    /// steer, whichever the WAL holds last). Absent, never empty, when no send carried one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

/// `run.result` params. Strict: the run is the question.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunResultParams {
    pub run_id: String,
}

/// The final assistant message of a completed run: its text parts joined in order with nothing
/// inserted, reasoning excluded. A final message with no text part answers `text: ""`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalMessage {
    pub ordinal: u64,
    pub mid: String,
    pub text: String,
}

/// `run.result`: a run's state and, once it completed, its final message. `state` is one of the
/// role's states (`active`, `paused`, `completed`, `max_steps`, `cancelled`, `error`,
/// `interrupted`) or a state of broca's own (`transform_unavailable`), which a consumer decodes
/// as an open string. A run that has not ended carries no `final_message`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunResultResponse {
    pub run_id: String,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<SharedProviderError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_message: Option<FinalMessage>,
}

/// `role.describe`: what this build serves of the `llm-runner/v1` role. It describes the build,
/// never a session, so it is the same on every route. The answer is the role's own type, so
/// a member the role adds reaches broca's answer as the role defines it; its bytes are pinned in
/// `tests/golden/role_describe.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RoleDescribeResponse {
    #[serde(flatten)]
    pub role: cortexkit_role_llm_runner::describe::RoleDescribe,
    pub session_class: SessionClassCapability,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionClassCapability {
    pub backup_classes: Vec<String>,
}
impl std::ops::Deref for RoleDescribeResponse {
    type Target = cortexkit_role_llm_runner::describe::RoleDescribe;
    fn deref(&self) -> &Self::Target {
        &self.role
    }
}

/// Discovery for a build that can read compaction WAL records but cannot yet write them.
/// The serving adapter must opt in after installing the provider adapter and ready/model ops.
pub fn broca_role_describe(implementation_version: &str) -> RoleDescribeResponse {
    broca_role_describe_with_writers(implementation_version, false, false, false)
}

/// Discovery follows all installed writers through one capability calculation. Decoding
/// compaction, retention or hook records alone does not promise to execute those writers.
pub fn broca_role_describe_with_writers(
    implementation_version: &str,
    compaction_writer: bool,
    retention_writer: bool,
    hook_writer: bool,
) -> RoleDescribeResponse {
    use cortexkit_role_llm_runner::describe::{steer_receipt, Major};
    let mut ops = [
        OP_ROLE_DESCRIBE,
        OP_SESSION_BASELINE,
        "session.read",
        OP_SESSION_HEAD,
        OP_RUN_RESULT,
        "run.status",
        "run.cancel",
        "session.retract",
        "session.subscribe",
        "session.send",
    ]
    .map(String::from)
    .to_vec();
    let mut capabilities = [
        "transcript_reads",
        "run_ops",
        cortexkit_role_llm_runner::capabilities::RUN_STATUS,
        cortexkit_role_llm_runner::capabilities::RUN_CANCEL,
        cortexkit_role_llm_runner::capabilities::RETRACT,
        "dispatch_attribution",
        "streaming",
        "steer",
        "queue",
        "interrupt",
        "plans",
        "fetch-plan-optional-tools/v1",
        "session_class",
    ]
    .map(String::from)
    .to_vec();
    if compaction_writer {
        ops.push(crate::OP_COMPACTION_READY.into());
        capabilities.extend(["compaction".into(), "model_view".into()]);
    }
    if hook_writer {
        capabilities.push(cortexkit_role_llm_runner::capabilities::ORDERED_HOOK_PHASES.into());
    }
    if retention_writer {
        capabilities.push(cortexkit_role_llm_runner::capabilities::RETENTION.into());
    }
    let mut describe = cortexkit_role_llm_runner::describe::RoleDescribe::new(
        vec![Major {
            version: LLM_RUNNER_V1.into(),
            ops,
            stability: "alpha".into(),
        }],
        implementation_version,
    )
    .with_capabilities(capabilities)
    // A session's own capabilities come back on the reply to the send that admits (first
    // writes) the session, not from a separate `session.baseline` call.
    .with_session_capabilities_from("admission")
    .with_max_bytes(READ_DEFAULT_MAX_BYTES, READ_MAXIMUM_MAX_BYTES)
    // Once a steer's `Steered` record is durable broca delivers it: the running turn renders
    // it at its next step boundary (taking one more step if the turn would otherwise end), and
    // a turn that ends first leaves it in the transcript for the next episode. So a steer's
    // `delivered` receipt is either absent (not rendered yet) or final, never `pending` or
    // `unknown`.
    .with_steer_receipt(steer_receipt::GUARANTEED);
    if retention_writer {
        describe.retention = Some(cortexkit_role_llm_runner::describe::Retention::new(
            2_592_000, 3_600_000,
        ));
    }
    RoleDescribeResponse {
        role: describe,
        session_class: SessionClassCapability {
            backup_classes: vec!["head".into()],
        },
    }
}

/// `session.baseline` params: none. Strict.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BaselineParams {}

/// What a session froze when it was admitted, and what has changed since.
///
/// A session admitted without a fetch plan has no fetched items and no composition: `items` is
/// empty and `composition_digest` absent, never a value computed some other way. One admitted
/// with a plan reports, from its frozen manifest, one item per provider that answered
/// (`{provider, item_digest}`, the provider's catalog digest), the optional items that did not
/// answer, and the admitted composition digest (excluding absent optional tool providers).
/// `tool_names` are the model-facing names of the
/// tools the session runs with. No mid-session change exists yet, so both generations are 0.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaselineWire {
    /// Each fetched plan item's frozen digests; always empty for a session admitted without a
    /// fetch plan.
    pub items: Vec<serde_json::Value>,
    /// Optional items recorded absent at admission: `{provider, kind, reason, provider_code?}`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub absent_items: Vec<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub composition_digest: Option<String>,
    pub tool_names: Vec<String>,
    pub accepted_generation: u64,
    pub applied_generation: u64,
    /// What the session's frozen config declares about its prompt cache. Absent when unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<crate::CacheDeclarationWire>,
}

impl BaselineWire {
    /// The baseline of a session admitted without a fetch plan, running with `tool_names`.
    pub fn plan_less(tool_names: Vec<String>) -> Self {
        Self {
            items: Vec::new(),
            absent_items: Vec::new(),
            composition_digest: None,
            tool_names,
            accepted_generation: 0,
            applied_generation: 0,
            cache: None,
        }
    }

    /// The baseline of a session admitted with a fetch plan.
    pub fn planned(
        items: Vec<serde_json::Value>,
        absent_items: Vec<serde_json::Value>,
        composition_digest: String,
        tool_names: Vec<String>,
    ) -> Self {
        Self {
            items,
            absent_items,
            composition_digest: Some(composition_digest),
            tool_names,
            accepted_generation: 0,
            applied_generation: 0,
            cache: None,
        }
    }
}

/// `session.baseline`: `not_yet` before the session's first send was admitted, else `ready` with
/// the baseline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaselineReply {
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline: Option<BaselineWire>,
}

impl BaselineReply {
    pub fn not_yet() -> Self {
        Self {
            state: "not_yet".into(),
            baseline: None,
        }
    }

    pub fn ready(baseline: BaselineWire) -> Self {
        Self {
            state: "ready".into(),
            baseline: Some(baseline),
        }
    }
}

/// The reply to `session.send`: its state, and on the send that admitted the session (and every
/// retry of it under the same `send_id`) the session's baseline, read from the record that send
/// wrote.
///
/// A retry of a steer also carries `delivered` once a model step has rendered the steered
/// message: `step` with the run it joined when that run rendered it, `turn` with a later run
/// when the joined run ended first and the next episode carried it. Absent means not rendered
/// yet, which on broca still means it will be (`role.describe` declares `steer_receipt:
/// guaranteed`). Every other reply carries none, and its bytes are what they were before the
/// member existed.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SendReplyWire {
    #[serde(flatten)]
    pub result: crate::SendResult,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub baseline: Option<BaselineWire>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivered: Option<cortexkit_role_llm_runner::send::Delivered>,
}

/// The bytes `session.head`, `session.baseline` and the admitting `session.send` reply answered
/// with before they could carry prompt-cache facts. A reply that has no cache facts to report
/// must keep exactly these bytes.
#[cfg(test)]
mod pinned_reply_bytes {
    use super::*;
    use crate::scope_refusal_golden::assert_golden;
    use sha2::{Digest, Sha256};

    fn golden(file: &str) -> Vec<u8> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/golden")
            .join(file);
        std::fs::read(&path)
            .unwrap_or_else(|error| panic!("read golden {}: {error}", path.display()))
    }

    #[test]
    fn session_head_bytes_are_pinned() {
        assert_golden(
            HeadResponse {
                title: None,
                lineage_id: Some("lin-1".into()),
                last_ordinal: Some(3),
                head: Some(CursorWire {
                    wal_seq: 12,
                    sub_index: 0,
                }),
                last_run_state: Some(LastRunState {
                    run_id: "run-1".into(),
                    state: "completed".into(),
                    reason: None,
                }),
                updated_at: Some(1_790_000_000_000),
                cache: None,
            },
            "session_head.json",
            SESSION_HEAD_SHA256,
        );
    }

    /// `session.head` for a session whose newest send carried a title. A session with no title
    /// keeps exactly the bytes of `session_head.json`.
    #[test]
    fn session_head_with_a_title_bytes_are_pinned() {
        assert_golden(
            HeadResponse {
                title: Some("Fix the login bug".into()),
                lineage_id: Some("lin-1".into()),
                last_ordinal: Some(3),
                head: Some(CursorWire {
                    wal_seq: 12,
                    sub_index: 0,
                }),
                last_run_state: Some(LastRunState {
                    run_id: "run-1".into(),
                    state: "completed".into(),
                    reason: None,
                }),
                updated_at: Some(1_790_000_000_000),
                cache: None,
            },
            "session_head.title.json",
            SESSION_HEAD_TITLE_SHA256,
        );
    }

    #[test]
    fn session_baseline_bytes_are_pinned() {
        assert_golden(
            BaselineReply::ready(BaselineWire::plan_less(vec!["read".into()])),
            "session_baseline.ready.json",
            SESSION_BASELINE_SHA256,
        );
    }

    /// The reply to the send that admitted a session carries its baseline. The reply type is
    /// serialize-only, so its bytes are compared directly rather than round-tripped.
    #[test]
    fn admission_reply_bytes_are_pinned() {
        let reply = SendReplyWire {
            result: crate::SendResult::Active {
                run_id: "run-1".into(),
            },
            baseline: Some(BaselineWire::plan_less(vec!["read".into()])),
            delivered: None,
        };
        let file = golden("send_reply.admission.json");
        assert_eq!(
            String::from_utf8(serde_json::to_vec(&reply).unwrap()).unwrap(),
            String::from_utf8(file.clone()).unwrap()
        );
        assert_eq!(
            format!("{:x}", Sha256::digest(&file)),
            ADMISSION_REPLY_SHA256,
            "golden file send_reply.admission.json changed"
        );
    }

    /// Compare a serialize-only reply's bytes with its golden file, and the file with its pinned
    /// digest, so regenerating the file to match a changed type fails until the digest is changed
    /// on purpose.
    fn assert_reply_bytes(reply: &SendReplyWire, file: &str, sha256: &str) {
        let golden = golden(file);
        assert_eq!(
            String::from_utf8(serde_json::to_vec(reply).unwrap()).unwrap(),
            String::from_utf8(golden.clone()).unwrap(),
            "serialized reply differs from {file}"
        );
        assert_eq!(
            format!("{:x}", Sha256::digest(&golden)),
            sha256,
            "golden file {file} changed"
        );
    }

    /// The `session.send` replies that carry no baseline: every state a send answers with. A
    /// member added to the reply later must leave these bytes unchanged whenever it is absent.
    #[test]
    fn send_reply_bytes_without_a_baseline_are_pinned() {
        let cases = [
            (
                crate::SendResult::Active {
                    run_id: "run-1".into(),
                },
                "send_reply.unreceipted.active.json",
                SEND_REPLY_ACTIVE_SHA256,
            ),
            (
                crate::SendResult::Finished {
                    run_id: "run-1".into(),
                    reason: broca_protocol::RunFinishReason::Completed,
                },
                "send_reply.unreceipted.finished.json",
                SEND_REPLY_FINISHED_SHA256,
            ),
            (
                crate::SendResult::Pending {
                    submission_id: "sub-run-1".into(),
                },
                "send_reply.unreceipted.pending.json",
                SEND_REPLY_PENDING_SHA256,
            ),
        ];
        for (result, file, sha256) in cases {
            let reply = SendReplyWire {
                result,
                baseline: None,
                delivered: None,
            };
            assert_reply_bytes(&reply, file, sha256);
        }
    }

    /// A steer retry's reply with its delivery receipt: `step` names the run the steer joined,
    /// `turn` the later run whose episode carried it after the joined run ended first.
    #[test]
    fn send_reply_bytes_with_a_delivery_receipt_are_pinned() {
        use cortexkit_role_llm_runner::send::{delivered_as, Delivered};
        let step = SendReplyWire {
            result: crate::SendResult::Active {
                run_id: "run-1".into(),
            },
            baseline: None,
            delivered: Some(Delivered::new(delivered_as::STEP).with_ref("run-1")),
        };
        assert_reply_bytes(
            &step,
            "send_reply.delivered_step.json",
            DELIVERED_STEP_SHA256,
        );
        let turn = SendReplyWire {
            result: crate::SendResult::Finished {
                run_id: "run-1".into(),
                reason: broca_protocol::RunFinishReason::Cancelled,
            },
            baseline: None,
            delivered: Some(Delivered::new(delivered_as::TURN).with_ref("run-2")),
        };
        assert_reply_bytes(
            &turn,
            "send_reply.delivered_turn.json",
            DELIVERED_TURN_SHA256,
        );
    }

    /// The cortexkit-role-llm-runner vectors from commons revision
    /// `067701f1ab61cd2c81aa58fb66b0cb65fbaef9e7`, vendored byte for byte from
    /// `test-vectors/llm-runner-v1`. Refresh these copies and their SHA-256 pins
    /// when the workspace's role dependency revision is bumped.
    fn role_vectors(file: &str) -> serde_json::Value {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/commons-067701f1ab61cd2c81aa58fb66b0cb65fbaef9e7/llm-runner-v1")
            .join(file);
        let bytes = std::fs::read(&path)
            .unwrap_or_else(|error| panic!("read vectors {}: {error}", path.display()));
        let sha256 = match file {
            "send.json" => "e9a50df9e8806625d8e6db2681e02515b1726ce6bc3164d38e12ae8e58f0c318",
            "role-describe.json" => {
                "36fbcbb22cc55792edd1785b4adbf747f30f99fd189ee9bd46dbacd6d528fb28"
            }
            _ => panic!("no reviewed SHA-256 for role vector {file}"),
        };
        assert_eq!(
            format!("{:x}", Sha256::digest(&bytes)),
            sha256,
            "refresh the commons role vectors and their reviewed SHA-256 pins together"
        );
        serde_json::from_slice(&bytes).unwrap()
    }

    /// Every `session.send` reply vector broca can answer (no baseline, and no receipt state a
    /// guaranteed runner never gives) encodes from broca's reply type to exactly the vector, and
    /// decodes with the role's `SendReply` to the same value the vector does. That covers the
    /// `step` and `turn` receipts with a `ref`, the only receipts broca answers.
    #[test]
    fn send_reply_vectors_broca_can_answer_encode_as_the_role_decodes_them() {
        use cortexkit_role_llm_runner::send::{delivered_as, Delivered, SendReply};
        let vectors = role_vectors("send.json");
        let mut receipts = Vec::new();
        for case in vectors["replies"].as_array().unwrap() {
            let name = case["name"].as_str().unwrap();
            let vector = &case["reply"];
            let role: SendReply = serde_json::from_value(vector.clone()).unwrap();
            if role.baseline.is_some() {
                continue;
            }
            let delivered = match &role.delivered {
                None => None,
                Some(d) if d.r#ref.is_some() && d.is_delivered() => Some(d.clone()),
                // `pending` and `unknown` are never answered by a guaranteed runner, and broca
                // always names the run a delivered steer reached.
                Some(_) => continue,
            };
            let result: crate::SendResult = serde_json::from_value(vector.clone())
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            let reply = SendReplyWire {
                result,
                baseline: None,
                delivered: delivered.clone(),
            };
            let encoded = serde_json::to_value(&reply).unwrap();
            assert_eq!(&encoded, vector, "{name}");
            assert_eq!(
                serde_json::from_value::<SendReply>(encoded).unwrap(),
                role,
                "{name}"
            );
            if let Some(delivered) = delivered {
                receipts.push(delivered);
            }
        }
        // The vectors that make this test worth running: a delivered receipt of each kind broca
        // answers.
        for kind in [delivered_as::STEP, delivered_as::TURN] {
            assert!(
                receipts
                    .iter()
                    .any(|d: &Delivered| d.r#as == kind && d.r#ref.is_some()),
                "no {kind} vector with a ref"
            );
        }
    }

    /// broca's `role.describe` passes the role's consumer check, and states its steer receipt
    /// policy exactly as the role's canonical `guaranteed` vector spells it.
    #[test]
    fn role_describe_declares_a_guaranteed_steer_receipt_as_the_vectors_spell_it() {
        use cortexkit_role_llm_runner::describe::{check_describe, steer_receipt};
        let encoded = serde_json::to_value(broca_role_describe("0.0.0-golden")).unwrap();
        let checked = check_describe(&encoded).unwrap_or_else(|p| panic!("{encoded}: {p:?}"));
        assert_eq!(checked.steer_receipt(), steer_receipt::GUARANTEED);
        let vectors = role_vectors("role-describe.json");
        let guaranteed = vectors["canonical"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == "steer receipt guaranteed")
            .expect("the guaranteed steer receipt vector");
        assert_eq!(
            encoded["steer_receipt"],
            guaranteed["answer"]["steer_receipt"]
        );
    }

    /// `role.describe` as this build answers it. The build identity is fixed here so a version
    /// bump does not move the pinned bytes.
    #[test]
    fn role_describe_bytes_are_pinned() {
        assert_golden(
            broca_role_describe_with_writers("0.0.0-writer-golden", true, true, true),
            "role_describe.json",
            ROLE_DESCRIBE_SHA256,
        );
    }

    #[test]
    fn reader_release_does_not_advertise_writer_groups() {
        assert_golden(
            broca_role_describe("0.0.0-reader-golden"),
            "role_describe_reader.json",
            "cac93c1c77aee1a10a6aff0f8e45e97a73dd82e3dd98eccb3c7b521838eb8819",
        );
    }

    #[test]
    fn retention_declaration_follows_only_its_writer_switch() {
        use cortexkit_role_llm_runner::describe::check_describe;
        for compaction_writer in [false, true] {
            for retention_writer in [false, true] {
                let encoded = serde_json::to_value(broca_role_describe_with_writers(
                    "test",
                    compaction_writer,
                    retention_writer,
                    false,
                ))
                .unwrap();
                check_describe(&encoded).unwrap_or_else(|error| panic!("{error:?}"));
                assert_eq!(
                    encoded["capabilities"]
                        .as_array()
                        .unwrap()
                        .contains(&serde_json::json!("retention")),
                    retention_writer,
                    "the retention group and its limits must be declared together"
                );
                assert!(
                    encoded["majors"][0]["ops"]
                        .as_array()
                        .unwrap()
                        .contains(&serde_json::json!("run.status")),
                    "run status is served regardless of retention support"
                );
                if retention_writer {
                    assert_eq!(
                        encoded["retention"],
                        serde_json::json!({"max_seconds": 2592000, "delete_within_ms": 3600000})
                    );
                } else {
                    assert!(encoded.get("retention").is_none());
                }
            }
        }
    }

    #[test]
    fn writer_declares_complete_compaction_groups() {
        use cortexkit_role_llm_runner::describe::check_describe;
        let encoded =
            serde_json::to_value(broca_role_describe_with_writers("test", true, false, false))
                .unwrap();
        check_describe(&encoded).unwrap_or_else(|error| panic!("{error:?}"));
        let capabilities = encoded["capabilities"].as_array().unwrap();
        assert!(capabilities.contains(&serde_json::json!("compaction")));
        assert!(capabilities.contains(&serde_json::json!("model_view")));
        assert!(encoded["majors"][0]["ops"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("compaction.ready")));
    }

    #[test]
    fn ordered_hook_phases_follow_only_the_installed_hook_writer() {
        use cortexkit_role_llm_runner::{
            capabilities::ORDERED_HOOK_PHASES, describe::check_describe,
        };
        for compaction in [false, true] {
            for retention in [false, true] {
                for hooks in [false, true] {
                    let encoded = serde_json::to_value(broca_role_describe_with_writers(
                        "test", compaction, retention, hooks,
                    ))
                    .unwrap();
                    check_describe(&encoded).unwrap_or_else(|error| panic!("{error:?}"));
                    let caps = encoded["capabilities"].as_array().unwrap();
                    assert_eq!(
                        caps.contains(&serde_json::json!(ORDERED_HOOK_PHASES)),
                        hooks
                    );
                    assert_eq!(caps.contains(&serde_json::json!("compaction")), compaction);
                    assert_eq!(caps.contains(&serde_json::json!("model_view")), compaction);
                    assert_eq!(
                        encoded["majors"][0]["ops"]
                            .as_array()
                            .unwrap()
                            .contains(&serde_json::json!(crate::OP_COMPACTION_READY)),
                        compaction
                    );
                    assert_eq!(encoded.get("retention").is_some(), retention);
                }
            }
        }
    }

    /// `session.head` with the session's cache clock, and `session.baseline` with its cache
    /// declaration: the shapes a caller schedules warms from.
    #[test]
    fn cache_facts_on_head_and_baseline_are_pinned() {
        assert_golden(
            HeadResponse {
                title: None,
                lineage_id: Some("lin-1".into()),
                last_ordinal: Some(3),
                head: Some(CursorWire {
                    wal_seq: 12,
                    sub_index: 0,
                }),
                last_run_state: Some(LastRunState {
                    run_id: "run-1".into(),
                    state: "completed".into(),
                    reason: None,
                }),
                updated_at: Some(1_790_000_000_000),
                cache: Some(crate::HeadCacheWire {
                    last_sent_at_ms: 1_790_000_000_000,
                    ttl_ms: Some(3_600_000),
                    expires_at_ms: Some(1_790_003_600_000),
                    prefix_tokens: None,
                    expiry_basis: crate::EXPIRY_BASIS_ESTIMATED.into(),
                }),
            },
            "session_head.cache.json",
            SESSION_HEAD_CACHE_SHA256,
        );
        let mut baseline = BaselineWire::plan_less(vec!["read".into()]);
        baseline.cache = Some(crate::CacheDeclarationWire {
            ttl_ms: Some(3_600_000),
            ttl_options_ms: vec![300_000, 3_600_000],
            read_refreshes_ttl: Some(true),
            warm: Some(crate::WARM_SUPPORTED.into()),
            basis: Some("requested".into()),
        });
        assert_golden(
            BaselineReply::ready(baseline),
            "session_baseline.cache.json",
            SESSION_BASELINE_CACHE_SHA256,
        );
    }

    const SESSION_HEAD_CACHE_SHA256: &str =
        "cabd121e8604ed081860ea7ed7f5f35879797212a069ddda2f612cefc621d740";
    const SESSION_BASELINE_CACHE_SHA256: &str =
        "438ccb57deadbc8e53c5450e0d3c5189954093f7f855695a9df9a18f92981348";
    const SESSION_HEAD_TITLE_SHA256: &str =
        "96eeaa6374e9f8820c01ea8d4e7366dea00b1a8dc190af701fbf8c86e4c4b37b";
    const SESSION_HEAD_SHA256: &str =
        "aec01aab3542523cb8dacf5407a659a9b822e836cac01f00125caa253614467a";
    const SESSION_BASELINE_SHA256: &str =
        "3d57d32b90a4c32116cb283c0fe5b1c602a65be2477fb2378fea4bb0aff81733";
    const ADMISSION_REPLY_SHA256: &str =
        "25bcbeb01466096e24e0209e0b0afeae89517f554e47ab3c8939eac2a1ae0ec7";
    const SEND_REPLY_ACTIVE_SHA256: &str =
        "dc7a5ff7296e2da72633c6caaa9ed16fb23352c4b9cfedfa32c393d6a8943c60";
    const SEND_REPLY_FINISHED_SHA256: &str =
        "57d5acee4a551062183fc09c5e09292df58ab05ee4bdb8fbbc9b7412fa8afc86";
    const SEND_REPLY_PENDING_SHA256: &str =
        "64757351eb175035f70b80cb349f0ecc17f83549bd912d17a739a5351f628772";
    const ROLE_DESCRIBE_SHA256: &str =
        "de8d616194d50c942361192348942203270518677a53165f50f4051c57ae928a";
    const DELIVERED_STEP_SHA256: &str =
        "8dac9b0a27f22fd515c117b54d844f88b2a51947de5e4b7ac1b9eab626237aed";
    const DELIVERED_TURN_SHA256: &str =
        "e78ee6901194a60f3081074ad295982e08d64b11b574e7c837bc7dd4a15846c2";
}
