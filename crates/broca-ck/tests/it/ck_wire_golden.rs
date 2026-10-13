//! The CK-Message wire golden — a canonical serialized `CkMessage` array fixture.
//!
//! This is the CROSS-LANGUAGE VECTOR for the transform wire: it pins the exact JSON shape
//! `broca-ck` emits (internally-tagged `ContentKind`, the `ToolResultOutput` shape, `origin`,
//! provider-extras) so a downstream parser — magic-context's ingress — can pin its own golden
//! against THIS file. That kills a wire-shape mismatch before the socket round-trip instead
//! of during it (the one genuine mismatch risk in the producer↔transform integration).
//!
//! The fixture is produced by `lift`ing a representative canonical prompt that exercises every
//! content kind the OWNED producer leg emits: text, signed + redacted reasoning, a tool call,
//! and a tool-result turn covering the text / json / denied / error outputs, plus an assistant
//! `origin`. The fixture then appends one realistic `Opaque` reasoning-arc vector too, because
//! the cross-language CK wire contract must pin the serialized `OpaqueArcKind` vocabulary even
//! when the owned producer leg does not emit that block on its own.
//!
//! Regenerate after an INTENTIONAL CK shape change: `BROCA_UPDATE_GOLDEN=1 cargo test -p
//! broca-ck --test it ck_wire_golden::`. A change here is a wire-contract change — coordinate the
//! matching update with magic-context's ingress golden.

use broca_ck::{
    lift, CkMessage, ContentBlock as CkBlock, ContentKind, OpaqueArc, OpaqueArcKind, OpaqueArcRole,
    OpaqueBlock, OpaqueSource, ResultBlock, ResultBlockKind, Role as CkRole, ToolResultOutput,
    ToolResultOutputKind,
};
use broca_protocol::content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult};
use broca_protocol::request::ModelHandle;
use broca_protocol::ToolCallId;

fn fixture_path() -> &'static str {
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/ck_wire_golden.json"
    )
}

fn fixture_text() -> String {
    std::fs::read_to_string(fixture_path()).expect("read CK wire golden fixture")
}

/// A representative prompt exercising every owned-producer content kind.
fn corpus() -> Vec<Message> {
    vec![
        Message::new(
            Role::System,
            vec![ContentBlock::Text {
                text: "You are a helpful assistant.".into(),
            }],
        ),
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "Do the thing.".into(),
            }],
        ),
        Message::assistant_from(
            vec![
                ContentBlock::Reasoning {
                    text: "let me think".into(),
                    signature: Some("sig-abc".into()),
                },
                ContentBlock::RedactedReasoning {
                    data: "encrypted-blob".into(),
                    provider_format: None,
                },
                ContentBlock::Text {
                    text: "calling tools".into(),
                },
                ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId("call_text".into()),
                    tool_name: "read".into(),
                    input: serde_json::json!({"path": "a.txt"}),
                }),
                ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId("call_json".into()),
                    tool_name: "inspect".into(),
                    input: serde_json::json!({"target": "b"}),
                }),
                ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId("call_denied".into()),
                    tool_name: "delete".into(),
                    input: serde_json::json!({"path": "c"}),
                }),
                ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId("call_err".into()),
                    tool_name: "write".into(),
                    input: serde_json::json!({"path": "d"}),
                }),
            ],
            ModelHandle::new("anthropic", "claude-opus-4-8"),
        ),
        Message::new(
            Role::Tool,
            vec![
                ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId("call_text".into()),
                    output: ToolOutput::Text {
                        text: "file contents".into(),
                    },
                    is_error: false,
                }),
                ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId("call_json".into()),
                    output: ToolOutput::Json {
                        value: serde_json::json!({"ok": true, "n": 42}),
                    },
                    is_error: false,
                }),
                ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId("call_denied".into()),
                    output: ToolOutput::Denied {
                        reason: "policy forbids delete".into(),
                    },
                    is_error: true,
                }),
                ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId("call_err".into()),
                    output: ToolOutput::Error {
                        message: "disk full".into(),
                    },
                    is_error: true,
                }),
            ],
        ),
    ]
}

fn reasoning_arc_vector() -> CkMessage {
    CkMessage {
        role: CkRole::Assistant,
        content: vec![CkBlock::bare(ContentKind::Opaque(OpaqueBlock {
            source: OpaqueSource::Wire {
                wire: "openai_responses".into(),
            },
            kind: "reasoning".into(),
            raw: br#"{"type":"reasoning","id":"rs_example","summary":[],"encrypted_content":"enc_example"}"#
                .to_vec(),
            arc: Some(OpaqueArc {
                kind: OpaqueArcKind::Reasoning,
                id: "rs_example".into(),
                role: OpaqueArcRole::Request,
            }),
        }))],
        origin: None,
        provider_extras: Default::default(),
        meta: Default::default(),
    }
}

/// Array-valued error tool results have no owned form (`lift` never produces them), so the
/// pair is hand-built the way the Anthropic decoder emits it on the MITM leg. The assistant
/// partner keeps the arc paired: a projector that enforces call/result pairing must accept
/// the corpus.
fn error_content_call_vector() -> CkMessage {
    CkMessage {
        role: CkRole::Assistant,
        content: vec![CkBlock::bare(ContentKind::ToolCall {
            id: "toolu_err_content".into(),
            name: "run_build".into(),
            input: serde_json::json!({"target": "release"}),
            thought_signature: None,
            provider_executed: false,
        })],
        origin: None,
        provider_extras: Default::default(),
        meta: Default::default(),
    }
}

fn error_content_vector() -> CkMessage {
    CkMessage {
        role: CkRole::Tool,
        content: vec![CkBlock::bare(ContentKind::ToolResult {
            id: "toolu_err_content".into(),
            tool_name: "run_build".into(),
            output: ToolResultOutput::bare(ToolResultOutputKind::ErrorContent {
                blocks: vec![ResultBlock {
                    kind: ResultBlockKind::Text {
                        text: "build failed: exit 1".into(),
                    },
                    provider_extras: Default::default(),
                }],
            }),
            provider_executed: false,
        })],
        origin: None,
        provider_extras: Default::default(),
        meta: Default::default(),
    }
}

#[test]
fn ck_wire_serialization_matches_golden() {
    // A fixed api-family so the fixture is stable (render never reads `api`).
    let mut ck = lift(&corpus(), &|_| "anthropic".into());
    ck.push(reasoning_arc_vector());
    ck.push(error_content_call_vector());
    ck.push(error_content_vector());
    let rendered = serde_json::to_string_pretty(&ck).expect("serialize CK array");

    if std::env::var("BROCA_UPDATE_GOLDEN").is_ok() {
        // Refuse to write a fixture the pinned constant does not describe. The pin is what
        // keeps the CONSUMER repos honest, so a regeneration that leaves it stale would ship a
        // wire-contract change under a hash that still names the old bytes. Failing here makes
        // the same-commit rule an exit code rather than a comment someone has to remember,
        // and the message carries the value to paste so the loop is one step, not a hunt.
        let pending = format!("{rendered}\n");
        let pending_sha = sha256_hex(&pending);
        assert_eq!(
            pending_sha,
            GOLDEN_SHA256,
            "refusing to regenerate: GOLDEN_SHA256 still names the OLD fixture bytes. A stale \
             constant would let a wire-contract change propagate to the repos that vendor this \
             fixture and pin the same hash. Set GOLDEN_SHA256 to {pending_sha} in this same \
             commit, re-run with BROCA_UPDATE_GOLDEN=1, then re-vendor the fixture into each \
             copy below.\n{}",
            sibling_vendored_copies()
        );
        std::fs::write(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/ck_wire_golden.json"
            ),
            pending,
        )
        .unwrap();
        return;
    }

    assert_eq!(
        rendered,
        fixture_text().trim_end_matches('\n'),
        "CK wire serialization changed. This is a WIRE-CONTRACT change: if INTENTIONAL, re-run \
         with BROCA_UPDATE_GOLDEN=1 AND coordinate the matching update to magic-context's \
         ingress golden (this fixture is the cross-language vector their parser pins against)."
    );
}

#[test]
fn golden_reasoning_vector_round_trips_as_ck_message() {
    let golden: serde_json::Value =
        serde_json::from_str(&fixture_text()).expect("parse CK golden JSON");
    let vector = golden
        .as_array()
        .and_then(|items| items.last())
        .cloned()
        .expect("golden keeps a reasoning vector at the tail");
    let parsed: CkMessage =
        serde_json::from_value(vector.clone()).expect("deserialize reasoning vector");
    let rerendered = serde_json::to_value(parsed).expect("re-serialize reasoning vector");
    assert_eq!(rerendered, vector);
}

/// The pinned SHA-256 of the fixture FILE BYTES (`shasum -a 256` over
/// `tests/fixtures/ck_wire_golden.json`, LF line endings — enforced by `.gitattributes`).
///
/// Magic-context vendors this fixture as an independent byte-identical copy at
/// `crates/mc-module/testdata/ck_wire_golden.json` and pins the same constant over it, so a
/// one-sided regeneration fails in whichever repo did not move. The serialization test above
/// keeps THIS repo's code and fixture in sync; the sha pin is what keeps the two REPOS in
/// sync, so the cross-repo wire contract cannot drift silently.
///
/// Thalamus depends on `broca-ck` by pinned git rev rather than vendoring this fixture, so a
/// regeneration cannot reach it until someone advances that rev deliberately.
///
/// Keep this consumer list checked rather than remembered: it named a repo that has never
/// held a copy, and the error messages below repeated the claim, so a reader coordinating a
/// regeneration would have gone looking for a file that was not there.
///
/// `sibling_vendored_copies` below is the answer to that: on a regeneration it reports the
/// copies it can SEE on disk rather than the ones this comment remembers, so the list in the
/// failure message cannot rot the way this sentence can. It is a best-effort observation, not
/// a proof of the consumer set -- a sibling that is absent from a given checkout is reported
/// as unseen rather than as absent, because those are different facts and only one of them is
/// knowable from here.
///
/// On an intentional CK shape change: regenerate the fixture (`BROCA_UPDATE_GOLDEN=1`, which
/// refuses unless this constant already names the new bytes), then re-vendor the fixture and
/// update the matching constant in magic-context.
const GOLDEN_SHA256: &str = "0a63fc82b79c169402a8ed02a254c954360bd374f04fc44ba75c401219c4a6b0";

fn sha256_hex(text: &str) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

#[test]
fn ck_wire_golden_fixture_sha_is_pinned() {
    let actual = sha256_hex(&fixture_text());
    assert_eq!(
        actual, GOLDEN_SHA256,
        "ck_wire_golden.json changed without updating the pinned sha. This fixture is a \
         cross-repo wire contract: magic-context vendors a byte-identical copy at \
         crates/mc-module/testdata/ck_wire_golden.json pinned to the same hash. Update the \
         constant here AND re-vendor the fixture there."
    );
}

/// Report each known vendored copy of this fixture as it is on disk right now, for the
/// regeneration failure message.
///
/// The paths are still written down, so this does not remove the need to add a row when a new
/// consumer starts vendoring. What it removes is the WORSE failure: a list that keeps naming a
/// copy after it stops existing, which is what sent a reader looking for an ai-proxy file that
/// was never there. A path that has gone away now reports as unseen, and a hash that has
/// drifted reports its own value, so the message describes the tree rather than the memory.
fn sibling_vendored_copies() -> String {
    const VENDORED: &[(&str, &str)] = &[(
        "magic-context",
        "../magic-context/crates/mc-module/testdata/ck_wire_golden.json",
    )];

    let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut lines = vec!["vendored copies, as seen from this checkout:".to_string()];
    for (name, relative) in VENDORED {
        let path = repo_root.join(relative);
        let line = match std::fs::read_to_string(&path) {
            Ok(text) => {
                let sha = sha256_hex(&text);
                let state = if sha == GOLDEN_SHA256 {
                    "matches the current pin"
                } else {
                    "DIFFERS from the current pin"
                };
                format!("  {name}: {relative} -- {state} ({sha})")
            }
            // Absent from THIS checkout is not the same fact as absent from the fleet: a
            // sibling may simply not be cloned here. Say which one was observed.
            Err(error) => format!("  {name}: {relative} -- not readable here ({error})"),
        };
        lines.push(line);
    }
    lines.push(
        "  thalamus: depends on broca-ck by pinned git rev and vendors no copy; it moves only \
         when that rev moves."
            .to_string(),
    );
    lines.join("\n")
}
