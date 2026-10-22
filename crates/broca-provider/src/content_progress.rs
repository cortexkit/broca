//! Observational wire classification, independent of the record counters that reset the
//! transport's existing idle and content timeouts. Recording telemetry never resets a timeout.

use std::collections::BTreeMap;

use broca_protocol::provider::{ContentProgress, EventTypeTally};
use serde_json::Value;

#[derive(Clone, Copy, Default)]
pub(crate) enum ContentFamily {
    #[default]
    Unknown,
    Anthropic,
    Responses,
    Chat,
    Gemini,
    CodeAssist,
    Bedrock,
}

#[derive(Default)]
pub(crate) struct ContentObserver {
    pub(crate) family: ContentFamily,
    progress: ContentProgress,
    // Exact counts are retained until the attempt ends so a late frequent type can enter the
    // top sixteen. Storage is bounded by the transport's existing per-attempt byte limit.
    types: BTreeMap<String, EventTypeTally>,
    non_content_bytes_unknown: bool,
    suffix_bytes_unknown: bool,
    /// How many payloads arrived as text and were parsed here. A caller that already holds the
    /// parsed JSON uses `observe_value`, and tests assert it never pays for this second parse.
    #[cfg(test)]
    pub(crate) text_payloads_parsed: usize,
}

fn nonempty(value: Option<&Value>) -> bool {
    value.and_then(Value::as_str).is_some_and(|s| !s.is_empty())
}

fn classify(family: ContentFamily, name: &str, value: &Value) -> (bool, bool) {
    use ContentFamily::*;
    match family {
        Anthropic => match name {
            "content_block_delta" => match value["delta"]["type"].as_str() {
                Some("text_delta") => (nonempty(value["delta"].get("text")), true),
                Some("thinking_delta") => (nonempty(value["delta"].get("thinking")), true),
                Some("input_json_delta") => (nonempty(value["delta"].get("partial_json")), true),
                Some("refusal_delta") => (nonempty(value["delta"].get("refusal")), true),
                Some("signature_delta") => (false, true),
                _ => (false, false),
            },
            "message_start"
            | "message_delta"
            | "message_stop"
            | "content_block_start"
            | "content_block_stop"
            | "ping"
            | "error" => (false, true),
            _ => (false, false),
        },
        Responses => match name {
            "response.output_text.delta"
            | "response.refusal.delta"
            | "response.function_call_arguments.delta"
            | "response.reasoning_text.delta"
            | "response.reasoning_summary_text.delta"
            | "response.reasoning.delta"
            | "response.reasoning_summary.delta" => (nonempty(value.get("delta")), true),
            "response.created"
            | "response.in_progress"
            | "response.queued"
            | "response.completed"
            | "response.incomplete"
            | "response.failed"
            | "response.output_item.added"
            | "response.output_item.done"
            | "response.content_part.added"
            | "response.content_part.done"
            | "response.output_text.done"
            | "response.output_text.annotation.added"
            | "response.refusal.done"
            | "response.function_call_arguments.done"
            | "response.reasoning_text.done"
            | "response.reasoning_summary_text.done"
            | "response.reasoning_summary_part.added"
            | "response.reasoning_summary_part.done"
            | "response.reasoning.done"
            | "response.reasoning_summary.done"
            | "rate_limits.updated"
            | "ping"
            | "error" => (false, true),
            _ => (false, false),
        },
        Chat => match name {
            "chat.completion.chunk" | "message" => {
                let content = value["choices"].as_array().is_some_and(|choices| {
                    choices.iter().any(|choice| {
                        let delta = &choice["delta"];
                        ["content", "reasoning_content", "reasoning", "refusal"]
                            .iter()
                            .any(|key| nonempty(delta.get(key)))
                            || delta["tool_calls"].as_array().is_some_and(|calls| {
                                calls
                                    .iter()
                                    .any(|call| nonempty(call["function"].get("arguments")))
                            })
                            || nonempty(delta["function_call"].get("arguments"))
                    })
                });
                (content, value.get("choices").is_some())
            }
            "ping" | "error" => (false, true),
            _ => (false, false),
        },
        Gemini | CodeAssist => match name {
            "message" => {
                let body = if matches!(family, CodeAssist) {
                    &value["response"]
                } else {
                    value
                };
                let content = body["candidates"].as_array().is_some_and(|candidates| {
                    candidates.iter().any(|candidate| {
                        candidate["content"]["parts"]
                            .as_array()
                            .is_some_and(|parts| {
                                parts.iter().any(|part| {
                                    nonempty(part.get("text")) || part.get("functionCall").is_some()
                                })
                            })
                    })
                });
                (
                    content,
                    body.get("candidates").is_some()
                        || body.get("usageMetadata").is_some()
                        || body.get("promptFeedback").is_some(),
                )
            }
            "ping" | "error" => (false, true),
            _ => (false, false),
        },
        Bedrock => match name {
            "contentBlockDelta" => {
                let delta = &value["delta"];
                let known = ["text", "toolUse", "reasoningContent"]
                    .iter()
                    .any(|key| delta.get(key).is_some());
                (
                    nonempty(delta.get("text"))
                        || nonempty(delta["toolUse"].get("input"))
                        || nonempty(delta["reasoningContent"].get("text")),
                    known,
                )
            }
            "messageStart" | "messageStop" | "contentBlockStart" | "contentBlockStop"
            | "metadata" | "error" => (false, true),
            _ => (false, false),
        },
        Unknown => (false, false),
    }
}

impl ContentObserver {
    pub(crate) fn new(family: ContentFamily) -> Self {
        Self {
            family,
            ..Default::default()
        }
    }
    pub(crate) fn observe(&mut self, name: &str, payload: Option<&str>, bytes: usize) {
        self.observe_bytes(name, payload, Some(bytes as u64));
    }

    /// `observe` for a payload the caller has already parsed, so the hot path does not
    /// serialize it back to text only for this observer to parse it again.
    pub(crate) fn observe_value(&mut self, name: &str, payload: &Value, bytes: usize) {
        self.record(name, payload, Some(bytes as u64), None);
    }

    pub(crate) fn observe_unmeasured_keepalive(&mut self) {
        self.observe_bytes("keepalive", None, None);
    }

    fn observe_bytes(&mut self, name: &str, payload: Option<&str>, bytes: Option<u64>) {
        self.observe_bytes_with_time(name, payload, bytes, None);
    }

    #[cfg(test)]
    fn observe_at(&mut self, name: &str, payload: Option<&str>, bytes: Option<u64>, now_ms: u64) {
        self.observe_bytes_with_time(name, payload, bytes, Some(now_ms));
    }

    fn observe_bytes_with_time(
        &mut self,
        name: &str,
        payload: Option<&str>,
        bytes: Option<u64>,
        now_ms: Option<u64>,
    ) {
        let parsed = match (name, payload) {
            ("keepalive" | "empty" | "[DONE]", _) | (_, None) => Value::Null,
            (_, Some(payload)) => {
                #[cfg(test)]
                {
                    self.text_payloads_parsed += 1;
                }
                serde_json::from_str::<Value>(payload).unwrap_or(Value::Null)
            }
        };
        self.record(name, &parsed, bytes, now_ms);
    }

    fn record(&mut self, name: &str, payload: &Value, bytes: Option<u64>, now_ms: Option<u64>) {
        let (content, known) = match name {
            "keepalive" | "empty" | "[DONE]" => (false, true),
            _ => classify(self.family, name, payload),
        };
        let key = if name.len() <= 128 { name } else { "unknown" };
        let tally = self
            .types
            .entry(key.to_owned())
            .or_insert_with(|| EventTypeTally {
                event_type: key.to_owned(),
                count: 0,
                bytes: Some(0),
                unknown: None,
            });
        tally.count += 1;
        tally.bytes = tally.bytes.zip(bytes).map(|(total, bytes)| total + bytes);
        if !known {
            tally.unknown = Some(true);
        }
        if content {
            let now = now_ms.unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64
            });
            if let Some(previous) = self.progress.last_content_at_ms {
                let gap = now.saturating_sub(previous);
                if self.progress.max_content_gap_ms.is_none_or(|max| gap > max) {
                    self.progress.max_content_gap_ms = Some(gap);
                    self.progress.max_content_gap_ended_at_ms = Some(now);
                }
            }
            self.progress.first_content_at_ms.get_or_insert(now);
            self.progress.last_content_at_ms = Some(now);
            if let Some(bytes) = bytes {
                *self.progress.content_bytes.get_or_insert(0) += bytes;
            }
            self.progress.non_content_bytes_after_last_content = Some(0);
            self.progress.events_after_last_content = Some(0);
            self.suffix_bytes_unknown = false;
        } else {
            if let Some(bytes) = bytes {
                *self.progress.non_content_bytes.get_or_insert(0) += bytes;
            } else {
                self.non_content_bytes_unknown = true;
            }
            if self.progress.last_content_at_ms.is_some() {
                if let Some(bytes) = bytes {
                    *self
                        .progress
                        .non_content_bytes_after_last_content
                        .get_or_insert(0) += bytes;
                } else {
                    self.suffix_bytes_unknown = true;
                }
                *self.progress.events_after_last_content.get_or_insert(0) += 1;
            }
        }
    }

    pub(crate) fn snapshot(&self) -> ContentProgress {
        let mut progress = self.progress.clone();
        if self.non_content_bytes_unknown {
            progress.non_content_bytes = None;
        }
        if self.suffix_bytes_unknown {
            progress.non_content_bytes_after_last_content = None;
        }
        let mut types: Vec<_> = self.types.values().cloned().collect();
        types.sort_by(|a, b| b.count.cmp(&a.count).then(a.event_type.cmp(&b.event_type)));
        if types.len() > 16 {
            let rest = types.split_off(16);
            types.push(EventTypeTally {
                event_type: "other".into(),
                count: rest.iter().map(|t| t.count).sum(),
                bytes: rest.iter().map(|t| t.bytes).sum(),
                unknown: rest.iter().any(|t| t.unknown == Some(true)).then_some(true),
            });
        }
        progress.event_types = (!types.is_empty()).then_some(types);
        progress
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn wire_family_content_classification() {
        use ContentFamily::*;
        let cases = [
            (
                Anthropic,
                "content_block_delta",
                json!({"delta":{"type":"text_delta","text":"x"}}),
            ),
            (
                Anthropic,
                "content_block_delta",
                json!({"delta":{"type":"thinking_delta","thinking":"x"}}),
            ),
            (
                Anthropic,
                "content_block_delta",
                json!({"delta":{"type":"input_json_delta","partial_json":"{}"}}),
            ),
            (
                Anthropic,
                "content_block_delta",
                json!({"delta":{"type":"refusal_delta","refusal":"no"}}),
            ),
            (
                Responses,
                "response.output_text.delta",
                json!({"delta":"x"}),
            ),
            (
                Responses,
                "response.reasoning_text.delta",
                json!({"delta":"x"}),
            ),
            (
                Responses,
                "response.reasoning_summary_text.delta",
                json!({"delta":"x"}),
            ),
            (
                Responses,
                "response.function_call_arguments.delta",
                json!({"delta":"{}"}),
            ),
            (Responses, "response.refusal.delta", json!({"delta":"no"})),
            (
                Chat,
                "message",
                json!({"choices":[{"delta":{"content":"x"}}]}),
            ),
            (
                Chat,
                "message",
                json!({"choices":[{"delta":{"reasoning_content":"x"}}]}),
            ),
            (
                Chat,
                "message",
                json!({"choices":[{"delta":{"refusal":"no"}}]}),
            ),
            (
                Chat,
                "message",
                json!({"choices":[{"delta":{"tool_calls":[{"function":{"arguments":"{}"}}]}}]}),
            ),
            (
                Gemini,
                "message",
                json!({"candidates":[{"content":{"parts":[{"text":"x"}]}}]}),
            ),
            (
                Gemini,
                "message",
                json!({"candidates":[{"content":{"parts":[{"text":"x","thought":true}]}}]}),
            ),
            (
                Gemini,
                "message",
                json!({"candidates":[{"content":{"parts":[{"functionCall":{"name":"f","args":{}}}]}}]}),
            ),
            (
                CodeAssist,
                "message",
                json!({"response":{"candidates":[{"content":{"parts":[{"text":"x"}]}}]}}),
            ),
            (Bedrock, "contentBlockDelta", json!({"delta":{"text":"x"}})),
            (
                Bedrock,
                "contentBlockDelta",
                json!({"delta":{"toolUse":{"input":"{}"}}}),
            ),
            (
                Bedrock,
                "contentBlockDelta",
                json!({"delta":{"reasoningContent":{"text":"x"}}}),
            ),
        ];
        for (family, name, value) in cases {
            assert_eq!(
                classify(family, name, &value),
                (true, true),
                "{name}: {value}"
            );
        }
        let metadata = [
            (
                Anthropic,
                "content_block_delta",
                json!({"delta":{"type":"signature_delta","signature":"x"}}),
            ),
            (
                Anthropic,
                "content_block_start",
                json!({"content_block":{"text":"not a delta"}}),
            ),
            (
                Responses,
                "response.in_progress",
                json!({"delta":"not a delta"}),
            ),
            (
                Responses,
                "response.output_item.done",
                json!({"item":{"text":"not a delta"}}),
            ),
            (
                Chat,
                "message",
                json!({"choices":[{"delta":{"role":"assistant"}}]}),
            ),
            (
                Gemini,
                "message",
                json!({"usageMetadata":{"totalTokenCount":1}}),
            ),
            (
                CodeAssist,
                "message",
                json!({"response":{"usageMetadata":{}}}),
            ),
            (Bedrock, "metadata", json!({"usage":{}})),
        ];
        for (family, name, value) in metadata {
            assert_eq!(classify(family, name, &value), (false, true), "{name}");
        }
        for family in [Anthropic, Responses, Chat, Gemini, CodeAssist, Bedrock] {
            assert_eq!(
                classify(family, "future.delta", &json!({"delta":"content-looking"})),
                (false, false)
            );
        }
    }

    #[test]
    fn metadata_only_stream_records_no_content() {
        let mut decoder = crate::sse::SseDecoder::for_content_family(ContentFamily::Responses);
        let raw = b": keepalive\n\ndata: {\"type\":\"response.in_progress\"}\n\n";
        decoder.push(raw).unwrap();
        let progress = decoder.content_progress();
        let value = serde_json::to_value(&progress).unwrap();
        for field in [
            "first_content_at_ms",
            "last_content_at_ms",
            "content_bytes",
            "non_content_bytes_after_last_content",
            "events_after_last_content",
        ] {
            assert!(value.get(field).is_none(), "{field}");
        }
        assert_eq!(
            progress.non_content_bytes,
            Some((b": keepalive".len() + br#"{"type":"response.in_progress"}"#.len()) as u64)
        );
        assert_eq!(decoder.data_records(), 1);
        assert_eq!(decoder.liveness_records(), 2);
        assert_eq!(
            progress
                .event_types
                .unwrap()
                .iter()
                .map(|t| t.count)
                .sum::<u64>(),
            2
        );
    }

    #[test]
    fn google_normalizers_report_real_content_progress() {
        use crate::families::{
            gemini_frozen_config, google_codeassist_frozen_config, Gemini, GoogleCodeAssist,
        };
        use crate::wire::WireFamily;
        let body = json!({"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"hello","thought":true}]}}]});
        for (mut normalizer, value) in [
            (
                Gemini.normalizer(&gemini_frozen_config("m"), broca_protocol::StepId(1)),
                body.clone(),
            ),
            (
                GoogleCodeAssist.normalizer(
                    &google_codeassist_frozen_config("m"),
                    broca_protocol::StepId(1),
                ),
                json!({"response":body}),
            ),
        ] {
            normalizer
                .push(format!("data: {value}\n\n").as_bytes())
                .unwrap();
            assert!(normalizer
                .content_progress()
                .content_bytes
                .is_some_and(|n| n > 0));
            assert!(normalizer.content_progress().last_content_at_ms.is_some());
        }
    }

    #[test]
    fn websocket_keepalive_does_not_measure_synthetic_sse_bytes() {
        let mut decoder = crate::sse::SseDecoder::for_content_family(ContentFamily::Responses);
        decoder.websocket_telemetry();
        decoder
            .push(b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n")
            .unwrap();
        decoder.push(b": keepalive\n\n").unwrap();
        let progress = decoder.content_progress();
        assert_eq!(progress.non_content_bytes, None);
        assert_eq!(progress.non_content_bytes_after_last_content, None);
        assert_eq!(progress.events_after_last_content, Some(1));
        let keepalive = progress
            .event_types
            .unwrap()
            .into_iter()
            .find(|t| t.event_type == "keepalive")
            .unwrap();
        assert_eq!(keepalive.count, 1);
        assert_eq!(keepalive.bytes, None);
        assert!(progress.content_bytes.is_some());
        assert_eq!(decoder.liveness_records(), 2);
    }

    #[test]
    fn zero_content_events_leave_gap_measurements_absent() {
        let mut observer = ContentObserver::new(ContentFamily::Responses);
        observer.observe_at("keepalive", None, None, 100);
        observer.observe_at("response.in_progress", Some("{}"), Some(2), 200);
        let progress = observer.snapshot();
        assert_eq!(progress.first_content_at_ms, None);
        assert_eq!(progress.last_content_at_ms, None);
        assert_eq!(progress.max_content_gap_ms, None);
        assert_eq!(progress.max_content_gap_ended_at_ms, None);
    }

    #[test]
    fn one_content_event_leaves_gap_measurements_absent() {
        let mut observer = ContentObserver::new(ContentFamily::Responses);
        observer.observe_at(
            "response.output_text.delta",
            Some(r#"{"delta":"x"}"#),
            Some(10),
            100,
        );
        let progress = observer.snapshot();
        assert_eq!(progress.first_content_at_ms, Some(100));
        assert_eq!(progress.last_content_at_ms, Some(100));
        assert_eq!(progress.max_content_gap_ms, None);
        assert_eq!(progress.max_content_gap_ended_at_ms, None);
    }

    #[test]
    fn several_content_events_keep_the_longest_gap_and_its_end() {
        let mut observer = ContentObserver::new(ContentFamily::Responses);
        for now_ms in [100, 150, 650, 700] {
            observer.observe_at(
                "response.output_text.delta",
                Some(r#"{"delta":"x"}"#),
                Some(10),
                now_ms,
            );
        }
        let progress = observer.snapshot();
        assert_eq!(progress.max_content_gap_ms, Some(500));
        assert_eq!(progress.max_content_gap_ended_at_ms, Some(650));
        assert_eq!(progress.last_content_at_ms, Some(700));
    }

    #[test]
    fn interleaved_keepalives_do_not_extend_content_gaps() {
        let mut observer = ContentObserver::new(ContentFamily::Responses);
        observer.observe_at(
            "response.output_text.delta",
            Some(r#"{"delta":"x"}"#),
            Some(10),
            100,
        );
        observer.observe_at("keepalive", None, None, 10_000);
        observer.observe_at("response.in_progress", Some("{}"), Some(2), 20_000);
        observer.observe_at(
            "response.output_text.delta",
            Some(r#"{"delta":"y"}"#),
            Some(10),
            150,
        );
        let progress = observer.snapshot();
        assert_eq!(progress.max_content_gap_ms, Some(50));
        assert_eq!(progress.max_content_gap_ended_at_ms, Some(150));
    }

    #[test]
    fn a_measured_zero_gap_is_distinct_from_an_unobserved_gap() {
        let mut observer = ContentObserver::new(ContentFamily::Responses);
        for _ in 0..2 {
            observer.observe_at(
                "response.output_text.delta",
                Some(r#"{"delta":"x"}"#),
                Some(10),
                100,
            );
        }
        let progress = observer.snapshot();
        assert_eq!(progress.max_content_gap_ms, Some(0));
        assert_eq!(progress.max_content_gap_ended_at_ms, Some(100));
    }

    #[test]
    fn progress_tally_is_top_sixteen_and_suffix_resets() {
        let mut observer = ContentObserver::new(ContentFamily::Responses);
        observer.observe("response.output_text.delta", Some(r#"{"delta":"é"}"#), 30);
        observer.observe("response.in_progress", Some("{}"), 40);
        let first = observer.snapshot();
        assert_eq!(first.non_content_bytes_after_last_content, Some(40));
        assert_eq!(first.events_after_last_content, Some(1));
        observer.observe("response.output_text.delta", Some(r#"{"delta":"x"}"#), 31);
        assert_eq!(observer.snapshot().events_after_last_content, Some(0));
        for i in 0..20 {
            observer.observe(&format!("future.{i:02}"), Some("{}"), 10);
        }
        for _ in 0..10 {
            observer.observe("late.frequent", Some("{}"), 20);
        }
        let progress = observer.snapshot();
        assert_eq!(progress.content_bytes, Some(61));
        assert!(progress.first_content_at_ms <= progress.last_content_at_ms);
        assert_eq!(progress.non_content_bytes_after_last_content, Some(400));
        assert_eq!(progress.events_after_last_content, Some(30));
        let types = progress.event_types.unwrap();
        assert_eq!(types.len(), 17);
        assert_eq!(types[0].event_type, "late.frequent");
        assert_eq!(types[0].unknown, Some(true));
        assert_eq!(types.last().unwrap().event_type, "other");
        assert_eq!(types.iter().map(|t| t.count).sum::<u64>(), 33);
        assert_eq!(
            types.iter().map(|t| t.bytes).sum::<Option<u64>>(),
            Some(501)
        );
    }
}
