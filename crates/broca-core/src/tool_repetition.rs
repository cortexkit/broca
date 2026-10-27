//! Detection of a model repeating tool calls without making progress.
//!
//! This module is pure: it observes calls and their outcomes and returns a verdict. Whether a
//! verdict becomes a warning in a tool result, an injected user turn, or nothing at all is the
//! run loop's decision, not this module's.
//!
//! The shape follows hermes-agent's `tool_guardrails`, which is the only surveyed implementation
//! that gates on OUTCOME rather than on repetition alone. That distinction is what makes the
//! signal usable: a repeated call is not evidence of a stuck model, but a repeated call that keeps
//! failing, or that keeps returning bytes the model has already seen, is.
//!
//! Nothing here terminates a run. The step cap remains the only terminator.

use broca_protocol::{canonical_json_bytes, ContentBlock, Message, Role, ToolCall, ToolOutput};
use std::{borrow::Cow, collections::HashMap};

/// The bytes of a tool result as the repetition check sees them.
///
/// Textual outputs borrow their existing storage. Structured output owns its canonical JSON because
/// serialization must materialize bytes. Canonical serialization keeps object key order from
/// changing the byte comparison of equivalent results.
pub(crate) fn result_bytes(output: &ToolOutput) -> Cow<'_, [u8]> {
    match output {
        ToolOutput::Parts { parts } => {
            let identity: Vec<_> = parts
                .iter()
                .map(|part| match part {
                    broca_protocol::ToolOutputPart::Text { text } => {
                        serde_json::json!({"text":text})
                    }
                    broca_protocol::ToolOutputPart::Media { media } => {
                        serde_json::json!({"sha256":media.sha256})
                    }
                })
                .collect();
            Cow::Owned(canonical_json_bytes(&serde_json::Value::Array(identity)))
        }
        ToolOutput::Text { text } => Cow::Borrowed(text.as_bytes()),
        ToolOutput::Json { value } => Cow::Owned(canonical_json_bytes(value)),
        ToolOutput::Denied { reason } => Cow::Borrowed(reason.as_bytes()),
        ToolOutput::Error { message } => Cow::Borrowed(message.as_bytes()),
    }
}

/// A small platform-stable FNV-1a 64-bit digest.
///
/// Deliberately local rather than shared with `broca-provider`'s copy: that one sits on the
/// render-edge tool-id alias path, where the digest is part of the byte-stable request identity,
/// and hoisting it into a common crate to save a dozen lines would put a cache-stability surface in
/// reach of an unrelated change. These digests are in-memory only and never reach the wire, so the
/// two have no reason to move together.
fn digest(bytes: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// How many identical failing calls before the model is told it is looping.
const EXACT_FAILURE_WARN_AFTER: u32 = 2;
/// How many failures of one tool, regardless of arguments, before the same.
const SAME_TOOL_FAILURE_WARN_AFTER: u32 = 3;
/// How many identical successful results from a read-only tool before the same.
const NO_PROGRESS_WARN_AFTER: u32 = 2;

/// A canonical identity for "this exact call". Arguments ride through the same key-sorting
/// canonicaliser the render path uses, so argument key order cannot produce two identities for one
/// call -- which would silently defeat the counter.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CallSignature {
    tool_name: String,
    args_digest: u64,
}

impl CallSignature {
    pub fn of(call: &ToolCall) -> Self {
        let canonical = canonical_json_bytes(&call.input);
        Self {
            tool_name: call.tool_name.clone(),
            args_digest: digest(&canonical),
        }
    }
}

/// What the observer concluded about a call that just completed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepetitionVerdict {
    /// Nothing worth saying.
    Quiet,
    /// The same call, with the same arguments, has now failed this many times.
    RepeatedFailure { tool_name: String, count: u32 },
    /// This tool has failed this many times in a row, with any arguments.
    RepeatedToolFailure { tool_name: String, count: u32 },
    /// A read-only tool returned a byte-identical result this many times running.
    NoProgress { tool_name: String, count: u32 },
}

impl RepetitionVerdict {
    /// The guidance shown to the model. Phrased as an observation plus a suggested change of
    /// approach: the model can only act on this if it is told what to do differently, and a bare
    /// "you are looping" invites it to try the same call once more to check.
    pub fn guidance(&self) -> Option<String> {
        match self {
            Self::Quiet => None,
            Self::RepeatedFailure { tool_name, count } => Some(format!(
                "{tool_name} has now failed {count} times with identical arguments. Read the error \
                 and change the arguments or the approach; repeating this call unchanged will \
                 fail the same way."
            )),
            Self::RepeatedToolFailure { tool_name, count } => Some(format!(
                "{tool_name} has failed {count} times in a row. Something about this tool or its \
                 target is not working right now; try a different route to the same goal."
            )),
            Self::NoProgress { tool_name, count } => Some(format!(
                "{tool_name} returned exactly the same result {count} times. The information is \
                 already available above; use it or change the query rather than asking again."
            )),
        }
    }
}

/// Whether repeating a tool can be evidence of no progress.
///
/// Only read-only tools qualify. A mutating tool called twice with the same arguments and the same
/// result may be doing exactly what was asked -- writing the same file twice is repetition, not a
/// loop -- so counting it would fire on correct behaviour. Unknown tools are treated as mutating,
/// which is the safe direction: an unrecognised tool produces no warning rather than a wrong one.
fn is_read_only(tool_name: &str) -> bool {
    const READ_ONLY: &[&str] = &[
        "read",
        "grep",
        "glob",
        "list",
        "search",
        "aft_search",
        "aft_zoom",
        "aft_outline",
        "aft_callgraph",
        "webfetch",
        "websearch",
    ];
    READ_ONLY.contains(&tool_name)
}

/// Per-episode tool-call history.
///
/// Scoped to one episode deliberately: a new turn is a new intent, and carrying counts across turns
/// would warn about a call the model has legitimately returned to after doing something else.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RepetitionObserver {
    exact_failures: HashMap<CallSignature, u32>,
    tool_failures: HashMap<String, u32>,
    /// Signature -> (digest of the last result, how many times that digest has repeated).
    no_progress: HashMap<CallSignature, (u64, u32)>,
}

impl RepetitionObserver {
    pub fn new() -> Self {
        Self::default()
    }

    /// Rebuild the observer by folding a durable prompt.
    ///
    /// This is what lets a warning be written into a `ToolResult` at all. That result is durable
    /// state which enters the next prompt, so a resumed run must produce the identical bytes -- and
    /// it can only do that if the counters are a function of the durable history rather than of
    /// whatever an in-memory observer happened to have seen before the crash.
    ///
    /// Everything needed is already in the prompt: an assistant turn carries `ToolCall` blocks with
    /// the name and arguments, and the following tool turn carries `ToolResult` blocks with the
    /// output and error flag. So this needs no new WAL record and no new durable state -- the
    /// history was always being written, it simply was not being read this way.
    ///
    /// Calls and results are paired in source order within each adjacent assistant/tool turn, which
    /// is the same grouping the run loop uses when restoring model-visible guidance. Provider IDs
    /// are only step-unique, so an episode-wide ID lookup would attach a later step's result to an
    /// earlier call. A call without a paired result is skipped because its outcome is unknown.
    pub fn from_prompt(prompt: &[Message]) -> Self {
        let mut fold = RepetitionFold::default();
        for message in prompt {
            fold.push(message);
        }
        fold.into_observer()
    }

    /// Record one completed call and report what it means.
    ///
    /// `result_bytes` is the tool's output as it will be shown to the model, and `failed` is the
    /// dispatch's own verdict rather than anything inferred from the text -- classification belongs
    /// to the layer that made the call, and sniffing an error out of result bytes would misread any
    /// tool that legitimately returns the word "error".
    pub fn observe(
        &mut self,
        call: &ToolCall,
        result_bytes: &[u8],
        failed: bool,
    ) -> RepetitionVerdict {
        let signature = CallSignature::of(call);

        if failed {
            // A failure invalidates any no-progress streak: the tool is not returning a stable
            // answer, it is erroring, and that is a different conversation with the model.
            self.no_progress.remove(&signature);

            let exact = self.exact_failures.entry(signature).or_insert(0);
            *exact += 1;
            let exact = *exact;

            let same_tool = self
                .tool_failures
                .entry(call.tool_name.clone())
                .or_insert(0);
            *same_tool += 1;
            let same_tool = *same_tool;

            // Exact-repeat first: it is the more specific diagnosis, and telling the model "this
            // call keeps failing" is more actionable than "this tool keeps failing".
            if exact >= EXACT_FAILURE_WARN_AFTER {
                return RepetitionVerdict::RepeatedFailure {
                    tool_name: call.tool_name.clone(),
                    count: exact,
                };
            }
            if same_tool >= SAME_TOOL_FAILURE_WARN_AFTER {
                return RepetitionVerdict::RepeatedToolFailure {
                    tool_name: call.tool_name.clone(),
                    count: same_tool,
                };
            }
            return RepetitionVerdict::Quiet;
        }

        // A success clears the failure history for this call and this tool. A tool that failed
        // three times and then worked is not looping, and carrying the old count would warn about
        // a problem the model has already solved.
        self.exact_failures.remove(&signature);
        self.tool_failures.remove(&call.tool_name);

        if !is_read_only(&call.tool_name) {
            self.no_progress.remove(&signature);
            return RepetitionVerdict::Quiet;
        }

        let result_digest = digest(result_bytes);
        let entry = self
            .no_progress
            .entry(signature)
            .or_insert((result_digest, 0));
        if entry.0 == result_digest {
            entry.1 += 1;
        } else {
            // A different result means the call IS making progress -- polling a file that another
            // process is writing lands here, and resetting is what keeps that legitimate pattern
            // from being reported as a loop.
            *entry = (result_digest, 1);
        }
        let count = entry.1;

        if count >= NO_PROGRESS_WARN_AFTER {
            return RepetitionVerdict::NoProgress {
                tool_name: call.tool_name.clone(),
                count,
            };
        }
        RepetitionVerdict::Quiet
    }
}

/// The episode fold shared by whole-prompt restoration and streamed store reads.
/// Only the immediately preceding assistant's calls survive a page boundary; no message rows do.
#[derive(Default)]
pub(crate) struct RepetitionFold {
    observer: RepetitionObserver,
    pending_calls: Option<Vec<ToolCall>>,
}

impl RepetitionFold {
    /// Observe raw outputs before a caller decorates its model-visible copies. Block indexes
    /// identify results within this message, not by provider IDs (which may repeat across steps).
    pub(crate) fn push(&mut self, message: &Message) -> Vec<(usize, RepetitionVerdict)> {
        let previous = self.pending_calls.take();
        let mut verdicts = Vec::new();
        if message.role == Role::Tool {
            if let Some(calls) = previous {
                let mut calls = calls.iter();
                for (index, block) in message.content.iter().enumerate() {
                    if let ContentBlock::ToolResult(result) = block {
                        if let Some(call) = calls.next() {
                            let bytes = result_bytes(&result.output);
                            verdicts.push((
                                index,
                                self.observer.observe(call, bytes.as_ref(), result.is_error),
                            ));
                        }
                    }
                }
            }
        } else if message.role == Role::Assistant {
            self.pending_calls = Some(
                message
                    .content
                    .iter()
                    .filter_map(|block| match block {
                        ContentBlock::ToolCall(call) => Some(call.clone()),
                        _ => None,
                    })
                    .collect(),
            );
        }
        verdicts
    }

    pub(crate) fn into_observer(self) -> RepetitionObserver {
        self.observer
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn call(name: &str, input: serde_json::Value) -> ToolCall {
        ToolCall {
            tool_call_id: broca_protocol::ToolCallId(format!("call-{name}")),
            tool_name: name.to_string(),
            input,
            thought_signature: None,
        }
    }

    fn legacy_result_bytes(output: &ToolOutput) -> Vec<u8> {
        match output {
            // Multipart output had no representation in the legacy model.
            ToolOutput::Parts { .. } => Vec::new(),
            ToolOutput::Text { text } => text.as_bytes().to_vec(),
            ToolOutput::Json { value } => canonical_json_bytes(value),
            ToolOutput::Denied { reason } => reason.as_bytes().to_vec(),
            ToolOutput::Error { message } => message.as_bytes().to_vec(),
        }
    }

    #[test]
    fn production_sized_text_result_bytes_allocates_nothing() {
        const PRODUCTION_PAYLOAD_BYTES: usize = 100_353;
        let output = ToolOutput::Text {
            text: "x".repeat(PRODUCTION_PAYLOAD_BYTES),
        };

        let source = match &output {
            ToolOutput::Text { text } => text.as_bytes(),
            _ => unreachable!(),
        };
        let bytes = result_bytes(&output);

        assert!(
            matches!(bytes, Cow::Borrowed(_)),
            "the zero-allocation path must return a borrowed slice"
        );
        assert_eq!(bytes.as_ptr(), source.as_ptr());
        assert_eq!(bytes.len(), PRODUCTION_PAYLOAD_BYTES);
    }

    #[test]
    fn multipart_repetition_identity_is_ordered_text_and_media_digest() {
        let media = broca_protocol::MediaRef {
            kind: broca_protocol::MediaKind::Image,
            mime: "image/png".into(),
            byte_size: 42,
            sha256: "a".repeat(64),
            width: Some(3),
            height: Some(2),
        };
        let parts = vec![
            broca_protocol::ToolOutputPart::Text {
                text: "before".into(),
            },
            broca_protocol::ToolOutputPart::Media {
                media: media.clone(),
            },
        ];
        let output = ToolOutput::Parts {
            parts: parts.clone(),
        };
        assert_eq!(
            result_bytes(&output).as_ref(),
            format!(
                "[{{\"text\":\"before\"}},{{\"sha256\":\"{}\"}}]",
                "a".repeat(64)
            )
            .as_bytes()
        );
        let mut metadata_only = media;
        metadata_only.mime = "image/jpeg".into();
        let same_bytes = ToolOutput::Parts {
            parts: vec![
                parts[0].clone(),
                broca_protocol::ToolOutputPart::Media {
                    media: metadata_only,
                },
            ],
        };
        assert_eq!(result_bytes(&output), result_bytes(&same_bytes));
        let mut reversed = parts;
        reversed.reverse();
        assert_ne!(
            result_bytes(&output),
            result_bytes(&ToolOutput::Parts { parts: reversed })
        );
    }

    #[test]
    fn result_bytes_preserve_legacy_digests() {
        let outputs = [
            ToolOutput::Text {
                text: "text payload".into(),
            },
            ToolOutput::Json {
                value: json!({"z": 1, "a": [true, false]}),
            },
            ToolOutput::Denied {
                reason: "policy denied".into(),
            },
            ToolOutput::Error {
                message: "tool failed".into(),
            },
        ];

        for output in &outputs {
            let legacy = legacy_result_bytes(output);
            let current = result_bytes(output);
            assert_eq!(
                digest(current.as_ref()),
                digest(&legacy),
                "result digest changed for {output:?}"
            );
        }
    }

    #[test]
    fn identical_failures_warn_on_the_second() {
        let mut observer = RepetitionObserver::new();
        let c = call("read", json!({"filePath": "a.rs"}));
        assert_eq!(
            observer.observe(&c, b"boom", true),
            RepetitionVerdict::Quiet
        );
        assert!(matches!(
            observer.observe(&c, b"boom", true),
            RepetitionVerdict::RepeatedFailure { count: 2, .. }
        ));
    }

    /// The rule that makes the counter honest: one success means the model is not stuck, so the
    /// history must not survive it. Without this a tool that fails twice, works, then fails once
    /// would warn on that single later failure.
    #[test]
    fn a_success_clears_the_failure_history() {
        let mut observer = RepetitionObserver::new();
        let c = call("read", json!({"filePath": "a.rs"}));
        observer.observe(&c, b"boom", true);
        observer.observe(&c, b"fine", false);
        assert_eq!(
            observer.observe(&c, b"boom", true),
            RepetitionVerdict::Quiet,
            "the failure before the success must not count toward the next one"
        );
    }

    /// The polling counter-example, which is the whole reason outcome alone is not enough: the same
    /// read against a file being written returns different bytes each time, and that is progress.
    #[test]
    fn a_changing_result_is_progress_not_repetition() {
        let mut observer = RepetitionObserver::new();
        let c = call("read", json!({"filePath": "growing.log"}));
        for i in 0..6 {
            let body = format!("line {i}");
            assert_eq!(
                observer.observe(&c, body.as_bytes(), false),
                RepetitionVerdict::Quiet,
                "a read whose contents keep changing is making progress"
            );
        }
    }

    #[test]
    fn an_unchanging_read_warns() {
        let mut observer = RepetitionObserver::new();
        let c = call("read", json!({"filePath": "static.rs"}));
        assert_eq!(
            observer.observe(&c, b"same", false),
            RepetitionVerdict::Quiet
        );
        assert!(matches!(
            observer.observe(&c, b"same", false),
            RepetitionVerdict::NoProgress { count: 2, .. }
        ));
    }

    /// Writing the same file twice is repetition and not a loop, so a mutating tool must never
    /// produce a no-progress verdict however identical its results.
    #[test]
    fn a_mutating_tool_never_reports_no_progress() {
        let mut observer = RepetitionObserver::new();
        let c = call("write", json!({"filePath": "out.rs", "content": "x"}));
        for _ in 0..5 {
            assert_eq!(
                observer.observe(&c, b"written", false),
                RepetitionVerdict::Quiet
            );
        }
    }

    #[test]
    fn one_tool_failing_on_varied_arguments_warns() {
        let mut observer = RepetitionObserver::new();
        for i in 0..3 {
            let c = call("read", json!({ "filePath": format!("f{i}.rs") }));
            let verdict = observer.observe(&c, b"missing", true);
            if i < 2 {
                assert_eq!(verdict, RepetitionVerdict::Quiet);
            } else {
                assert!(matches!(
                    verdict,
                    RepetitionVerdict::RepeatedToolFailure { count: 3, .. }
                ));
            }
        }
    }

    /// Argument key order must not split one call into two signatures, or the counter silently
    /// never fires on a model that varies key order between calls.
    ///
    /// BE CLEAR ABOUT WHAT HOLDS THIS UP: not the canonicaliser. `serde_json::Value` stores
    /// objects in a sorted map, so both spellings are already identical by the time any code here
    /// runs -- this test passes even with the canonicalisation removed entirely, which I verified
    /// rather than assumed. What it actually pins is the DEPENDENCY ASSUMPTION: serde_json's
    /// `preserve_order` feature swaps that sorted map for an insertion-ordered one, and cargo
    /// feature unification means any crate in the tree turning it on would change it here. That is
    /// a real regression this catches and a plausible one, since the flag is enabled for exactly
    /// the round-trip-fidelity reasons another part of a workspace tends to want.
    ///
    /// Written from JSON TEXT rather than `json!` literals so a feature flip is observable at all:
    /// the two spellings must differ somewhere before parsing, or there is nothing for the flag to
    /// preserve.
    #[test]
    fn argument_key_order_does_not_split_the_signature() {
        let ab: serde_json::Value = serde_json::from_str(r#"{"a":1,"b":2}"#).unwrap();
        let ba: serde_json::Value = serde_json::from_str(r#"{"b":2,"a":1}"#).unwrap();
        assert_eq!(
            CallSignature::of(&call("read", ab)),
            CallSignature::of(&call("read", ba)),
            "the same call written with its keys in a different order is the same call"
        );
    }
}

#[cfg(test)]
mod prompt_fold_tests {
    use super::*;
    use broca_protocol::{Message, Role, ToolCallId, ToolResult};
    use serde_json::json;

    fn call(id: &str, name: &str, input: serde_json::Value) -> ContentBlock {
        ContentBlock::ToolCall(ToolCall {
            tool_call_id: ToolCallId(id.to_string()),
            tool_name: name.to_string(),
            input,
            thought_signature: None,
        })
    }

    fn result(id: &str, text: &str, is_error: bool) -> ContentBlock {
        ContentBlock::ToolResult(ToolResult {
            tool_call_id: ToolCallId(id.to_string()),
            output: ToolOutput::Text {
                text: text.to_string(),
            },
            is_error,
        })
    }

    fn turn(role: Role, content: Vec<ContentBlock>) -> Message {
        Message {
            role,
            content,
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
        }
    }

    /// The property the whole design rests on: a warning written into a durable ToolResult must
    /// come out the same after a crash, and it only can if the counters are a function of the
    /// prompt. Folding the durable history must therefore land in the same state as having watched
    /// the calls live.
    #[test]
    fn folding_a_prompt_matches_having_watched_it_live() {
        let c = ToolCall {
            tool_call_id: ToolCallId("t1".into()),
            tool_name: "read".into(),
            input: json!({"filePath": "a.rs"}),
            thought_signature: None,
        };
        let mut live = RepetitionObserver::new();
        live.observe(&c, b"boom", true);
        let live_verdict = live.observe(&c, b"boom", true);

        let prompt = vec![
            turn(
                Role::Assistant,
                vec![call("t1", "read", json!({"filePath": "a.rs"}))],
            ),
            turn(Role::Tool, vec![result("t1", "boom", true)]),
        ];
        let mut folded = RepetitionObserver::from_prompt(&prompt);
        let folded_verdict = folded.observe(&c, b"boom", true);

        assert_eq!(
            folded_verdict, live_verdict,
            "a resumed run must reach the same verdict as one that never crashed"
        );
    }

    /// An unanswered call is an in-flight or interrupted dispatch whose outcome nobody knows.
    /// Counting it either way would invent history the live path never saw, and the direction that
    /// matters is that it must not be counted as a failure.
    #[test]
    fn a_call_with_no_result_is_not_folded() {
        let prompt = vec![
            turn(
                Role::Assistant,
                vec![call("t1", "read", json!({"filePath": "a.rs"}))],
            ),
            turn(Role::Tool, vec![result("t1", "boom", true)]),
            // second call was dispatched but its result never landed
            turn(
                Role::Assistant,
                vec![call("t2", "read", json!({"filePath": "a.rs"}))],
            ),
        ];
        let observer = RepetitionObserver::from_prompt(&prompt);
        let signature = CallSignature::of(&ToolCall {
            tool_call_id: ToolCallId("t1".into()),
            tool_name: "read".into(),
            input: json!({"filePath": "a.rs"}),
            thought_signature: None,
        });
        assert_eq!(
            observer.exact_failures.get(&signature).copied(),
            Some(1),
            "only the answered call counts; the dangling one must not add a second failure"
        );
    }

    /// Google-family normalizers mint `call_0` again at the first call of every model step. A
    /// resumed observer must therefore follow turn order rather than treating that step-local id as
    /// an episode-wide identity. The live observer is the oracle for the bytes an uninterrupted run
    /// would show the model.
    #[test]
    fn folding_reused_step_local_ids_matches_live_verdict_sequence() {
        let first = ToolCall {
            tool_call_id: ToolCallId("call_0".into()),
            tool_name: "read".into(),
            input: json!({"filePath": "A"}),
            thought_signature: None,
        };
        let second = ToolCall {
            tool_call_id: ToolCallId("call_0".into()),
            tool_name: "read".into(),
            input: json!({"filePath": "B"}),
            thought_signature: None,
        };
        let continuation = [
            ToolCall {
                tool_call_id: ToolCallId("call_0".into()),
                tool_name: "read".into(),
                input: json!({"filePath": "C"}),
                thought_signature: None,
            },
            ToolCall {
                tool_call_id: ToolCallId("call_0".into()),
                tool_name: "read".into(),
                input: json!({"filePath": "D"}),
                thought_signature: None,
            },
        ];

        let mut live = RepetitionObserver::new();
        assert_eq!(
            live.observe(&first, b"A contents", false),
            RepetitionVerdict::Quiet
        );
        assert_eq!(
            live.observe(&second, b"B missing", true),
            RepetitionVerdict::Quiet
        );

        let prompt = vec![
            turn(
                Role::Assistant,
                vec![call("call_0", "read", json!({"filePath": "A"}))],
            ),
            turn(Role::Tool, vec![result("call_0", "A contents", false)]),
            turn(
                Role::Assistant,
                vec![call("call_0", "read", json!({"filePath": "B"}))],
            ),
            turn(Role::Tool, vec![result("call_0", "B missing", true)]),
        ];
        let mut folded = RepetitionObserver::from_prompt(&prompt);

        let live_verdicts: Vec<_> = continuation
            .iter()
            .map(|call| live.observe(call, b"missing", true))
            .collect();
        let folded_verdicts: Vec<_> = continuation
            .iter()
            .map(|call| folded.observe(call, b"missing", true))
            .collect();

        assert_eq!(
            folded_verdicts, live_verdicts,
            "a resumed run must emit the same guidance sequence as an uninterrupted run"
        );
        assert_eq!(
            live_verdicts,
            vec![
                RepetitionVerdict::Quiet,
                RepetitionVerdict::RepeatedToolFailure {
                    tool_name: "read".into(),
                    count: 3,
                },
            ]
        );
    }

    #[test]
    fn an_empty_prompt_folds_to_a_quiet_observer() {
        let observer = RepetitionObserver::from_prompt(&[]);
        assert!(observer.exact_failures.is_empty());
        assert!(observer.no_progress.is_empty());
    }
}
