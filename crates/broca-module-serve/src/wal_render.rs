//! Shared offline render path for WAL-based probes. No endpoint or credentials are opened.
use std::sync::Arc;

use broca_core::run::restore_repetition_guidance;
use broca_protocol::{CallOptions, StepContext};
use broca_provider::wire::FrozenRenderConfig;
use broca_wal::{RunConfig, WalRecord};

pub fn render_step(
    prefix: &[WalRecord],
    config: &RunConfig,
    run_id: &str,
    step: u64,
) -> Result<(FrozenRenderConfig, CallOptions, Vec<u8>), String> {
    let (frozen, mut opts, state) = boundary_options(prefix, config, run_id, step)?;
    let family = broca_provider::registry::wire_family(frozen.family_id)
        .ok_or("no wire family implementation for the frozen family id")?;
    if let Some(state) = state.state() {
        opts = broca_core::compaction::model_options(&opts, state).map_err(|e| e.to_string())?;
    }
    let body = family.render(&frozen, &opts);
    Ok((frozen, opts, body))
}

pub fn boundary_options(
    prefix: &[WalRecord],
    config: &RunConfig,
    run_id: &str,
    step: u64,
) -> Result<
    (
        FrozenRenderConfig,
        CallOptions,
        broca_core::compaction::CompactionCheckpoint,
    ),
    String,
> {
    let mut prior =
        broca_core::replay::replay(prefix).map_err(|e| format!("replay WAL prefix: {e}"))?;
    // Replay retains raw tool outputs. Before sending them to the model, the live runner
    // adds warnings about repeated failures or identical read results. Reconstruct those
    // warnings from each episode's calls and results, resetting counters for each new run.
    for span in prior
        .episode_step_spans
        .clone()
        .ok_or("missing episode spans")?
    {
        restore_repetition_guidance(&mut prior.prompt[span]);
    }
    // A run frozen with `render_tags` sent tagged user and tool-result text with its tag prefix.
    if config.render_tags {
        broca_protocol::tags::render_tags_into(
            &mut prior.prompt,
            broca_protocol::tags::TagRendering::for_render_config(&config.frozen_render_config),
        );
    } else {
        // Model text a run under the computed rules marked for cleaning went out cleaned on
        // every request since; a structured-output run's final text went out unchanged.
        broca_protocol::tags::clean_computed_into(&mut prior.prompt);
    }
    let frozen: FrozenRenderConfig = serde_json::from_value(config.frozen_render_config.clone())
        .map_err(|e| format!("decode frozen_render_config: {e}"))?;
    // Copy the runner's frozen tools, tool choice, response format, generation settings and
    // provider options. The replayed prompt and step identity are the per-step inputs.
    let mut opts = CallOptions {
        declared_markers: Vec::new(),
        ephemeral_session: false,
        marker_order_version: prior.marker_order_version,
        render_episodes: Arc::new(prior.render_episodes.unwrap_or_default().into()),
        prompt: prior.prompt,
        tools: Arc::clone(&config.tools),
        tool_choice: Arc::new(config.tool_choice.clone()),
        response_format: Arc::new(config.response_format.clone()),
        generation: Arc::new(config.generation.clone()),
        provider_options: Arc::new(config.provider_options.clone()),
        step_context: Some(StepContext {
            session_address: None,
            credential_id: None,
            account_ref: None,
            run_id: run_id.to_owned(),
            step_index: step,
        }),
        target_model: Some(config.model.clone()),
    };
    if prior.compaction.state().is_some() {
        broca_protocol::content::stamp_ordinals(&mut opts.prompt);
    }
    Ok((frozen, opts, prior.compaction))
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::{
        ContentBlock, Message, Role, ToolCall, ToolCallId, ToolOutput, ToolResult,
    };
    use serde_json::json;

    fn turn(id: &str, input: serde_json::Value, output: ToolOutput, failed: bool) -> Vec<Message> {
        vec![
            Message {
                role: Role::Assistant,
                content: vec![ContentBlock::ToolCall(ToolCall {
                    tool_call_id: ToolCallId(id.into()),
                    tool_name: "read".into(),
                    input,
                    thought_signature: None,
                })],
                origin: None,
                ordinal: None,
                tags: Default::default(),
                cache_prefix_blocks: None,
            },
            Message {
                role: Role::Tool,
                content: vec![ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId(id.into()),
                    output,
                    is_error: failed,
                })],
                origin: None,
                ordinal: None,
                tags: Default::default(),
                cache_prefix_blocks: None,
            },
        ]
    }

    #[test]
    fn restores_exact_failure_guidance_for_every_output_shape() {
        for output in [
            ToolOutput::Text { text: "raw".into() },
            ToolOutput::error("raw"),
            ToolOutput::Denied {
                reason: "raw".into(),
            },
            ToolOutput::Json {
                value: json!({"raw": 1}),
            },
        ] {
            let mut prompt = turn("a", json!({}), output.clone(), true);
            prompt.extend(turn("b", json!({}), output.clone(), true));
            let raw = prompt.clone();
            restore_repetition_guidance(&mut prompt);
            assert_eq!(prompt[..3], raw[..3]);
            let suffix = "\n\nTool repetition guidance: read has now failed 2 times with identical arguments. Read the error and change the arguments or the approach; repeating this call unchanged will fail the same way.";
            let expected = match output {
                ToolOutput::Parts { mut parts } => {
                    parts.push(broca_protocol::ToolOutputPart::Text {
                        text: suffix.into(),
                    });
                    ToolOutput::Parts { parts }
                }
                ToolOutput::Text { text } => ToolOutput::Text {
                    text: text + suffix,
                },
                ToolOutput::Error {
                    message,
                    module,
                    code,
                    detail,
                    nothing_sent,
                    retry,
                } => ToolOutput::Error {
                    message: message + suffix,
                    module,
                    code,
                    detail,
                    nothing_sent,
                    retry,
                },
                ToolOutput::Denied { reason } => ToolOutput::Denied {
                    reason: reason + suffix,
                },
                ToolOutput::Json { value } => ToolOutput::Text {
                    text: format!("{value}{suffix}"),
                },
            };
            assert_eq!(
                prompt[3].content,
                vec![ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId("b".into()),
                    output: expected,
                    is_error: true,
                })]
            );
        }
    }

    #[test]
    fn restores_same_tool_failure_guidance_in_source_order() {
        let mut prompt = Vec::new();
        for n in 0..4 {
            prompt.extend(turn(
                &format!("{n}"),
                json!({"n": n}),
                ToolOutput::error("raw"),
                true,
            ));
        }
        restore_repetition_guidance(&mut prompt);
        for (index, count) in [(5, 3), (7, 4)] {
            let ContentBlock::ToolResult(result) = &prompt[index].content[0] else {
                panic!()
            };
            assert_eq!(result.output, ToolOutput::error(format!("raw\n\nTool repetition guidance: read has failed {count} times in a row. Something about this tool or its target is not working right now; try a different route to the same goal.")));
        }
    }

    #[test]
    fn no_progress_uses_raw_json_and_episode_observers_reset() {
        let raw = ToolOutput::Json {
            value: json!({"a": 1, "b": 2}),
        };
        let mut prompt = turn("a", json!({}), raw.clone(), false);
        prompt.extend(turn("b", json!({}), raw.clone(), false));
        prompt.extend(turn("c", json!({}), raw.clone(), false));
        restore_repetition_guidance(&mut prompt);
        for (index, count) in [(3, 2), (5, 3)] {
            let ContentBlock::ToolResult(result) = &prompt[index].content[0] else {
                panic!()
            };
            assert_eq!(result.output, ToolOutput::Text { text: format!("{{\"a\":1,\"b\":2}}\n\nTool repetition guidance: read returned exactly the same result {count} times. The information is already available above; use it or change the query rather than asking again.") });
        }
        let mut next_episode = turn("d", json!({}), raw, false);
        let expected = next_episode.clone();
        restore_repetition_guidance(&mut next_episode);
        assert_eq!(next_episode, expected);
    }
}

#[cfg(test)]
mod replay_tests {
    use super::*;
    use broca_protocol::{ContentBlock, ToolOutput};
    use serde_json::json;

    #[test]
    fn render_applies_compaction_and_strips_pre_view_thinking_from_wal() {
        let frozen = broca_provider::families::anthropic_frozen_config("claude-opus-5-5");
        let config: RunConfig = serde_json::from_value(json!({
            "model": {"provider_module_id":"anthropic","model_id":"claude-opus-5-5"},
            "tools": [], "tool_choice":{"type":"auto"}, "generation": {},
            "response_format":{"type":"text"}, "context_epoch":0,
            "renderer_version":frozen.renderer_version, "frozen_render_config":frozen,
        }))
        .unwrap();
        let records: Vec<WalRecord> = serde_json::from_value(json!([
            {"type":"run_started", "run_id":"r", "session":{"project_root":"/test","harness":"test","session":"test"},
             "config": config, "input":[
                 {"role":"user", "content":[{"type":"text", "text":"original prefix"}]},
                 {"role":"assistant", "origin":{"provider_module_id":"anthropic","model_id":"claude-opus-5-5"},
                  "content":[{"type":"reasoning","text":"old thinking","signature":"old-sig"},
                             {"type":"text","text":"kept answer"}]}
             ]},
            {"type":"compaction_message", "provider":"compactor", "kind":"setup",
             "compaction_id":"view", "version":1, "range":{"from":0,"to":1}, "sha256":"fixture",
             "replacement":[{"role":"user", "content":[{"type":"text", "text":"summary"}]}]}
        ])).unwrap();
        let stored = serde_json::to_vec(&records).unwrap();
        let (_, opts, bytes) = render_step(&records, &config, "r", 1).unwrap();
        assert_eq!(opts.render_episodes.compaction_prefix_len, Some(2));
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            body["messages"],
            json!([
                {"role":"user", "content":[{"type":"text","text":"summary"}]},
                {"role":"assistant", "content":[{"type":"text","text":"kept answer"}]}
            ])
        );
        assert_eq!(serde_json::to_vec(&records).unwrap(), stored);
    }

    #[test]
    fn render_restores_prior_episode_guidance_without_leaking_counts() {
        let frozen = broca_provider::families::openai_responses_frozen_config("test-model");
        let config: RunConfig = serde_json::from_value(json!({
            "model": {"provider_module_id": "test", "model_id": "test-model"},
            "tools": [], "tool_choice": {"type": "auto"}, "generation": {},
            "response_format": {"type": "text"}, "context_epoch": 0,
            "renderer_version": frozen.renderer_version,
            "frozen_render_config": frozen
        }))
        .unwrap();
        let start = |id: &str| {
            serde_json::from_value::<WalRecord>(json!({
                "type": "run_started", "run_id": id,
                "session": {"project_root": "/test", "harness": "test", "session": "test"},
                "config": config, "input": []
            }))
            .unwrap()
        };
        let mut records = vec![start("first")];
        for step in 1..=2 {
            let batch = format!("first-b{step}");
            for value in [
                json!({"type": "model_step_finished", "step_id": step, "usage": {},
                "assistant_message": {"message_id": format!("m{step}"), "content": [
                    {"type": "tool_call", "tool_call_id": format!("c{step}"), "tool_name": "read", "input": {}}
                ]}}),
                json!({"type": "tool_batch_started", "step_id": step, "batch_id": batch, "planned_calls": [format!("c{step}")]}),
                json!({"type": "tool_result", "batch_id": batch, "outcome": "completed", "result": {
                    "tool_call_id": format!("c{step}"), "output": {"kind": "error", "message": "raw"}, "is_error": true
                }}),
                json!({"type": "tool_batch_finished", "batch_id": batch}),
                json!({"type": "turn_finished", "step_id": step}),
            ] {
                records.push(serde_json::from_value(value).unwrap());
            }
        }
        records.push(
            serde_json::from_value(
                json!({"type": "run_finished", "reason": "completed", "usage": {}}),
            )
            .unwrap(),
        );
        records.push(start("second"));
        // A reused call id in a new episode must not inherit earlier failure counts
        // or acquire a repetition warning on its first failure.
        let next_turn = records[1..6].to_vec();
        records.extend(next_turn);
        let raw = broca_core::replay::replay(&records).unwrap();
        let (_, opts, actual) = render_step(&records, &config, "second", 2).unwrap();
        let mut expected = opts.clone();
        expected.prompt = raw.prompt;
        let ContentBlock::ToolResult(result) = &mut expected.prompt[3].content[0] else {
            panic!()
        };
        result.output = ToolOutput::error("raw\n\nTool repetition guidance: read has now failed 2 times with identical arguments. Read the error and change the arguments or the approach; repeating this call unchanged will fail the same way.");
        assert_eq!(opts.prompt, expected.prompt);
        assert_eq!(
            actual,
            broca_provider::registry::wire_family(frozen.family_id)
                .unwrap()
                .render(&frozen, &expected)
        );
        assert_eq!(records.iter().filter(|r| matches!(r, WalRecord::ToolResult { result, .. } if result.output == ToolOutput::error("raw"))).count(), 3);
    }
}

#[cfg(test)]
mod title_render_tests {
    use super::*;
    use serde_json::json;

    /// The frozen render config of every wire family, so a property checked here holds for each
    /// family broca can send to.
    fn every_family() -> Vec<FrozenRenderConfig> {
        use broca_provider::families::*;
        vec![
            anthropic_frozen_config("m"),
            anthropic_apikey_frozen_config("m"),
            openai_chat_frozen_config("m"),
            openai_responses_frozen_config("m"),
            openai_responses_chatgpt_frozen_config("m"),
            gemini_frozen_config("m"),
            // Code Assist renders only with the project its credential is bound to.
            {
                let mut frozen = google_codeassist_frozen_config("m");
                frozen.project_id = Some("project-1".into());
                frozen
            },
            bedrock_frozen_config("m"),
        ]
    }

    /// A session's WAL with a title on every record a send writes, or on none: a first run, a
    /// queued send that the second run drains, and a steer into the second run that its first
    /// step renders.
    fn records(config: &RunConfig, title: Option<&str>) -> Vec<WalRecord> {
        let titled = |mut value: serde_json::Value| {
            if let Some(title) = title {
                value["title"] = json!(title);
            }
            serde_json::from_value::<WalRecord>(value).unwrap()
        };
        let user =
            |text: &str| json!([{"role": "user", "content": [{"type": "text", "text": text}]}]);
        let session = json!({"project_root": "/test", "harness": "test", "session": "test"});
        vec![
            titled(json!({"type": "run_started", "run_id": "r1", "session": session,
                "config": config, "input": user("first task")})),
            serde_json::from_value(json!({"type": "model_step_finished", "step_id": 1, "usage": {},
                "assistant_message": {"message_id": "m1", "content": [{"type": "text", "text": "first answer"}]}}))
            .unwrap(),
            serde_json::from_value(json!({"type": "turn_finished", "step_id": 1})).unwrap(),
            titled(json!({"type": "queued", "submission_id": "sub-r2", "request": {
                "model": {"provider_module_id": "test", "model_id": "m"}, "input": user("queued task"),
                "policy_allowed": [], "tool_choice": {"type": "auto"}, "generation": {}, "stop_when": []}})),
            serde_json::from_value(json!({"type": "run_finished", "reason": "completed", "usage": {}}))
                .unwrap(),
            titled(json!({"type": "run_started", "run_id": "r2", "session": session,
                "config": config, "input": user("queued task"), "submission_id": "sub-r2"})),
            titled(json!({"type": "steered", "delivery": "steer", "input": user("steered words")})),
            serde_json::from_value(json!({"type": "step_started", "step_id": 1})).unwrap(),
        ]
    }

    /// A title is never rendered: for every wire family, the request body broca renders from a
    /// session whose run starts, queued send and steer all carry titles is byte-identical to the
    /// body rendered from the same session without them, and the title appears nowhere in it.
    /// The rendered body does carry the queued and steered messages, so the comparison covers
    /// the records that hold a title rather than a prefix that never reaches them.
    #[test]
    fn titles_leave_every_familys_rendered_request_bytes_unchanged() {
        const TITLE: &str = "Fix the login bug";
        for frozen in every_family() {
            let config: RunConfig = serde_json::from_value(json!({
                "model": {"provider_module_id": "test", "model_id": "m"},
                "tools": [], "tool_choice": {"type": "auto"}, "generation": {},
                "response_format": {"type": "text"}, "context_epoch": 0,
                "renderer_version": frozen.renderer_version,
                "frozen_render_config": frozen
            }))
            .unwrap();
            let titled = records(&config, Some(TITLE));
            let untitled = records(&config, None);
            assert_eq!(
                titled
                    .iter()
                    .filter(|record| record.title().is_some())
                    .count(),
                4,
                "the titled session carries a title on every record a send writes"
            );
            let (_, _, titled_body) = render_step(&titled, &config, "r2", 1).unwrap();
            let (_, _, untitled_body) = render_step(&untitled, &config, "r2", 1).unwrap();
            let family = format!("{:?}", frozen.family_id);
            assert_eq!(
                titled_body, untitled_body,
                "{family}: a title changed the request"
            );
            let body = String::from_utf8_lossy(&titled_body);
            assert!(
                !body.contains(TITLE),
                "{family}: the title was rendered: {body}"
            );
            for text in ["first task", "queued task", "steered words"] {
                assert!(
                    body.contains(text),
                    "{family}: `{text}` missing from {body}"
                );
            }
        }
    }
}
