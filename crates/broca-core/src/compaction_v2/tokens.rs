//! Read-time counts of full, uncompacted entry content. Nothing in this cache is durable.

use std::collections::HashMap;

use broca_protocol::ContentBlock;
use cortexkit_role_compaction::{
    notice::NoticeRequest,
    part::{AttachmentItem, FileMedia, PartBody, ToolPart, ToolStatus},
    read::{Entry, ReadPage, Role},
    tokens::{PartTokens, ReasoningTokens, SurfaceTokens},
};
use cortexkit_tokens::{count, family_for_model, image, reasoning, Family, FamilyChoice};

#[derive(Default)]
pub struct Counts {
    tokenizer_time: std::time::Duration,
    facts: Option<Facts>,
    parts: HashMap<(String, String, String), Cached>,
}

struct Facts {
    key: String,
    id: String,
    family: FamilyChoice,
    image: image::Rule,
    system: Option<SurfaceTokens>,
    tools_count: Option<SurfaceTokens>,
}

struct Cached {
    digest: String,
    tag: Option<u64>,
    input: Option<u64>,
    tokens: Option<PartTokens>,
}

impl Counts {
    pub fn tokenizer_time(&self) -> std::time::Duration {
        self.tokenizer_time
    }
    pub fn family_id(&self) -> Option<&str> {
        self.facts.as_ref().map(|facts| facts.id.as_str())
    }
    /// Freeze count inputs once for an unchanged run configuration. A new model or render
    /// surface invalidates derived counts, even when it happens to use the same tokenizer.
    pub fn configure(
        &mut self,
        frozen: &super::host::Frozen<'_>,
        manifest: Option<&broca_wal::PlanManifest>,
    ) {
        let mut system = manifest
            .map(super::systems::from_manifest)
            .unwrap_or_else(|| frozen.global_system.unwrap_or_default().to_vec());
        let key = serde_json::to_string(&serde_json::json!({
            "model": frozen.model, "render": frozen.frozen_render_config,
            "tools": frozen.tools, "system": system,
        }))
        .expect("frozen count inputs serialize");
        if self.facts.as_ref().is_some_and(|facts| facts.key == key) {
            return;
        }
        let family = family_for_model(&frozen.model.model_id);
        let structured =
            broca_protocol::structured_output::FrozenStructuredOutput::from_frozen_render_config(
                Some(frozen.frozen_render_config),
            );
        let mut definitions = frozen.tools.to_vec();
        if let Some(structured) = structured {
            use broca_protocol::structured_output::{Mode, SchemaDelivery};
            match structured.mode {
                Mode::SubmitTool => definitions.push(
                    broca_protocol::structured_output::submit_tool(structured.schema()),
                ),
                Mode::FinalText if structured.delivery == SchemaDelivery::Unconstrained => {
                    let mut text = broca_protocol::structured_output::instructions(&structured);
                    if let Some(message) = system.first_mut() {
                        if structured.instructions_paragraph_break {
                            text.insert_str(0, "\n\n");
                        }
                        message
                            .content
                            .push(ContentBlock::Text { text: text.into() });
                    } else {
                        system.push(broca_protocol::Message::new(
                            broca_protocol::Role::System,
                            vec![ContentBlock::Text { text: text.into() }],
                        ));
                    }
                }
                _ => {}
            }
        }
        let system_count = system
            .iter()
            .flat_map(|message| &message.content)
            .try_fold(0u64, |sum, block| match block {
                ContentBlock::Text { text } => {
                    timed_count(family.family, text, &mut self.tokenizer_time)
                        .map(|n| sum.saturating_add(n))
                }
                _ => None,
            })
            .map(|n| SurfaceTokens::new(n, family.exact));
        let tools_count = count_definitions(
            family.family,
            &definitions,
            frozen.frozen_render_config,
            &mut self.tokenizer_time,
        );
        let api = match frozen
            .frozen_render_config
            .get("family_id")
            .and_then(|v| v.as_str())
        {
            Some("openai_chat" | "openai_responses" | "openai_responses_chatgpt") => {
                image::Api::OpenAi
            }
            Some("gemini" | "google_codeassist") => image::Api::Google,
            _ => image::Api::Other,
        };
        self.facts = Some(Facts {
            key,
            id: token_family_id(&frozen.model.model_id, api),
            family,
            image: image::rule_for(&frozen.model.model_id, api),
            system: system_count,
            tools_count,
        });
        self.parts.clear();
    }

    /// The common annotation pass for both storages and notices. Call before compacting, while
    /// the text and results are still present; compacting afterwards preserves these counts.
    pub fn annotate(&mut self, entries: &mut [Entry]) {
        let Some(facts) = &self.facts else { return };
        for entry in entries {
            let mut visible = Some(0u64);
            for index in 0..entry.parts.len() {
                let part = &entry.parts[index];
                let key = (entry.mid.clone(), part.id.clone(), facts.id.clone());
                let old = self.parts.get(&key);
                let cached = old.filter(|old| old.digest == part.digest && old.tag == part.tag);
                let (tokens, input) = if let Some(old) = cached {
                    (old.tokens, old.input)
                } else {
                    let input = match &part.body {
                        PartBody::Tool(tool) => old.and_then(|old| old.input).or_else(|| {
                            tool_input_count(facts.family.family, tool, &mut self.tokenizer_time)
                        }),
                        _ => None,
                    };
                    let tokens = match &part.body {
                        PartBody::Text { .. } => super::render::counted_content(entry, part)
                            .and_then(|text| {
                                timed_count(facts.family.family, &text, &mut self.tokenizer_time)
                            })
                            .map(PartTokens::count),
                        PartBody::Tool(tool) if tool.status != ToolStatus::Running => {
                            let text = super::render::counted_content(entry, part);
                            let output = text.and_then(|text| {
                                timed_count(facts.family.family, &text, &mut self.tokenizer_time)
                            });
                            let output = output.and_then(|n| {
                                tool.attachments().iter().try_fold(n, |sum, attachment| {
                                    match &attachment.item {
                                        AttachmentItem::File(file) => {
                                            Some(sum.saturating_add(file_count(file, facts.image)?))
                                        }
                                        _ => None,
                                    }
                                })
                            });
                            input
                                .zip(output)
                                .map(|(input, output)| PartTokens::tool(input, output))
                        }
                        PartBody::File(file) => {
                            file_count(file, facts.image).map(PartTokens::count)
                        }
                        _ => None,
                    };
                    self.parts.insert(
                        key,
                        Cached {
                            digest: part.digest.to_string(),
                            tag: part.tag,
                            input,
                            tokens,
                        },
                    );
                    (tokens, input)
                };
                let generated = match &part.body {
                    PartBody::Reasoning { .. } => Some(0),
                    PartBody::Tool(_) => input,
                    _ => match tokens {
                        Some(PartTokens::Count(n)) => Some(n.count),
                        _ => None,
                    },
                };
                visible = visible.zip(generated).map(|(sum, n)| sum.saturating_add(n));
                entry.parts[index].tokens = tokens;
            }
            entry.reasoning_tokens = if entry.role == Role::Assistant {
                entry.usage.and_then(|usage| match usage.reasoning_tokens {
                    Some(n) => Some(ReasoningTokens::new(n, false)),
                    None => Some(ReasoningTokens::new(
                        reasoning::generated(
                            reasoning::StepUsage {
                                output: usage.output_tokens?,
                                reasoning: None,
                            },
                            visible?,
                        ),
                        false,
                    )),
                })
            } else {
                None
            };
        }
    }

    pub fn page(&mut self, page: &mut ReadPage) {
        self.annotate(&mut page.entries);
        let has_count = has_counts(&page.entries);
        page.token_family = has_count.then(|| {
            self.facts
                .as_ref()
                .expect("counts have a family")
                .id
                .clone()
        });
        page.token_family_exact = has_count.then(|| {
            self.facts
                .as_ref()
                .expect("counts have a family")
                .family
                .exact
        });
    }

    pub fn finish_page(
        &mut self,
        mut full: ReadPage,
        request: &cortexkit_role_compaction::read::ReadRequest,
    ) -> ReadPage {
        self.annotate(&mut full.entries);
        let mut page = super::views::page(&full.entries, request, full.head);
        if page.next_after.is_none() {
            page.next_after = full.next_after;
        }
        let has_count = has_counts(&page.entries);
        page.token_family = has_count.then(|| {
            self.facts
                .as_ref()
                .expect("counts have a family")
                .id
                .clone()
        });
        page.token_family_exact = has_count.then(|| {
            self.facts
                .as_ref()
                .expect("counts have a family")
                .family
                .exact
        });
        page
    }

    pub fn notice(&self, notice: &mut NoticeRequest, notes_declared: bool) {
        notice.token_family = None;
        notice.token_family_exact = None;
        if let Some(facts) = &self.facts {
            notice.conditions.system_tokens = facts.system;
            notice.conditions.tools_tokens = facts.tools_count;
            // Broca has no pending non-entry plan/tool notes: a refresh replaces the frozen
            // surface. Its reminders and delivered notes are transcript parts, counted there,
            // not persistent runner notes. Their separate surface is therefore known empty.
            notice.conditions.notes_tokens =
                notes_declared.then(|| SurfaceTokens::new(0, facts.family.exact));
            if has_counts(&notice.entries)
                || facts.system.is_some()
                || facts.tools_count.is_some()
                || notice.conditions.notes_tokens.is_some()
            {
                notice.token_family = Some(facts.id.clone());
                notice.token_family_exact = Some(facts.family.exact);
            }
        }
    }
}

/// Text tokenizer revisions alone do not identify image billing. The suffix makes a change
/// of image rule a family change too, so readers cannot keep incompatible image counts.
pub fn token_family_id(model: &str, api: image::Api) -> String {
    let family = family_for_model(model).family;
    let (default_model, default_api) = match family {
        Family::O200k | Family::Cl100k => ("gpt-4o", image::Api::OpenAi),
        Family::ClaudeV3 => ("claude-opus-4-6", image::Api::Other),
        Family::ClaudeV47 | Family::ClaudeV5 | Family::ClaudeV5Sonnet => {
            ("claude-opus-5", image::Api::Other)
        }
        _ => ("", image::Api::Other),
    };
    let rule = image::rule_for(model, api);
    let mut id = family.token_family();
    if rule != image::rule_for(default_model, default_api) {
        let label = match rule {
            image::Rule::AnthropicPatch {
                max_edge,
                max_tokens,
            } => format!("anthropic-{max_edge}-{max_tokens}"),
            image::Rule::Fixed { tokens } => format!("fixed-{tokens}"),
            image::Rule::DeepSeekPatch {
                patch_size,
                downsample,
                max_tokens,
                min_pixels,
            } => format!("deepseek-{patch_size}-{downsample}-{max_tokens}-{min_pixels}"),
            image::Rule::OpenAiPatch {
                multiplier,
                low,
                high,
                original,
                auto_original,
            } => {
                let sizing = |size: image::PatchSizing| {
                    format!(
                        "{}-{}",
                        size.max_edge,
                        size.patch_budget
                            .map_or_else(|| "none".into(), |budget| budget.to_string())
                    )
                };
                format!(
                    "openai-{multiplier}-{}-{}-{}-{auto_original}",
                    sizing(low),
                    sizing(high),
                    sizing(original)
                )
            }
        };
        id.push_str("+img:");
        id.push_str(&label);
    }
    id
}

fn has_counts(entries: &[Entry]) -> bool {
    entries.iter().any(|entry| {
        entry.reasoning_tokens.is_some() || entry.parts.iter().any(|part| part.tokens.is_some())
    })
}

/// The same definition counter that supplies compaction's `tools_tokens`, applied to any
/// slice of definitions. Counts remain estimates because provider-specific schema wrapping
/// and dispatch metadata are outside these inputs.
pub fn tool_definition_tokens(
    model_id: &str,
    tools: &[broca_protocol::Tool],
    render: &serde_json::Value,
) -> Option<SurfaceTokens> {
    count_definitions(
        family_for_model(model_id).family,
        tools,
        render,
        &mut Default::default(),
    )
}

fn count_definitions(
    family: Family,
    definitions: &[broca_protocol::Tool],
    render: &serde_json::Value,
    elapsed: &mut std::time::Duration,
) -> Option<SurfaceTokens> {
    let operator_aliases = render
        .get("tool_name_aliases")
        .and_then(serde_json::Value::as_object);
    let tools: Vec<_> = definitions
        .iter()
        .map(|tool| {
            let name = operator_aliases
                .and_then(|aliases| aliases.get(&tool.name))
                .and_then(serde_json::Value::as_str)
                .unwrap_or(&tool.name);
            serde_json::json!({
                "name": broca_protocol::tool_name_alias::normalize(name),
                "description": tool.description, "input_schema": tool.input_schema,
            })
        })
        .collect();
    timed_count(
        family,
        &serde_json::to_string(&tools).expect("tools serialize"),
        elapsed,
    )
    .map(|n| SurfaceTokens::new(n, false))
}

fn timed_count(family: Family, text: &str, elapsed: &mut std::time::Duration) -> Option<u64> {
    let start = std::time::Instant::now();
    let tokens = count(family, text);
    *elapsed += start.elapsed();
    tokens
}

/// Count the provider's context-free name rewrite plus its JSON argument serialization.
/// Request-local collision suffixes and operator-selected aliases can differ by a few tokens
/// from what the model saw. Stability and bounded read cost require the name to depend on this
/// part alone, rather than rebuilding a namespace from the current request's other calls.
fn tool_input_count(
    family: Family,
    tool: &ToolPart,
    elapsed: &mut std::time::Duration,
) -> Option<u64> {
    // The Chat/Responses renderers use this same argument serializer.
    let arguments = serde_json::to_string(&tool.input).ok()?;
    let name = broca_protocol::tool_name_alias::normalize(&tool.name);
    timed_count(family, &format!("{name}{arguments}"), elapsed)
}

pub(super) fn strip_counts(entries: &mut [Entry]) {
    for entry in entries {
        entry.reasoning_tokens = None;
        for part in &mut entry.parts {
            part.tokens = None;
        }
    }
}

fn file_count(file: &FileMedia, rule: image::Rule) -> Option<u64> {
    if !file.mime.starts_with("image/") {
        return None;
    }
    Some(image::tokens(
        rule,
        image::Size {
            width: u32::try_from(file.width?).ok()?,
            height: u32::try_from(file.height?).ok()?,
        },
        None,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::Message;
    use cortexkit_role_compaction::{
        part::{Delivered, Part, ToolPart, Transport},
        read::{Head, ReadRequest},
    };
    use serde_json::json;

    fn configure(counts: &mut Counts, model: &str) {
        configure_render(counts, model, json!({"family_id": "openai_chat"}));
    }

    fn configure_render(counts: &mut Counts, model: &str, config: serde_json::Value) {
        let model = broca_protocol::request::ModelHandle::new("stub", model);
        let globals = vec![Message::new(
            broca_protocol::Role::System,
            vec![ContentBlock::Text {
                text: "system instructions".into(),
            }],
        )];
        counts.configure(
            &super::super::host::Frozen {
                model: &model,
                variant: None,
                service_tier: None,
                frozen_render_config: &config,
                tools: &[],
                global_system: Some(&globals),
                auth_selection: None,
                context_limit: Some(200_000),
                max_output_tokens: Some(1000),
            },
            None,
        );
    }

    fn entry(mid: &str, role: Role, body: PartBody) -> Entry {
        let mut part = Part::new("0", body);
        part.tag = Some(7);
        Entry::new(1, mid, role, 100, vec![part])
    }

    fn text() -> Entry {
        entry(
            "text",
            Role::Assistant,
            PartBody::Text {
                text: "hello world".into(),
            },
        )
    }

    fn page(entries: Vec<Entry>) -> ReadPage {
        ReadPage::new(entries, Head::new())
    }

    fn expected(text: &str) -> u64 {
        count(Family::O200k, text).unwrap()
    }

    #[test]
    fn per_definition_tokens_use_the_compaction_definition_counter() {
        let mut counts = Counts::default();
        let model = broca_protocol::ModelHandle::new("fixture", "gpt-4o");
        let tools = vec![serde_json::from_value(json!({
            "name":"read", "description":"Read a file", "input_schema":{"type":"object"},
            "module":"fixture",
        }))
        .unwrap()];
        let render = json!({});
        counts.configure(
            &super::super::host::Frozen {
                model: &model,
                variant: None,
                service_tier: None,
                frozen_render_config: &render,
                tools: &tools,
                global_system: None,
                auth_selection: None,
                context_limit: Some(200_000),
                max_output_tokens: Some(1000),
            },
            None,
        );
        let single = tool_definition_tokens("gpt-4o", &tools, &render).unwrap();
        assert_eq!(single, counts.facts.as_ref().unwrap().tools_count.unwrap());
        assert_eq!(
            single.count,
            expected(
                r#"[{"description":"Read a file","input_schema":{"type":"object"},"name":"read"}]"#
            )
        );
        assert!(!single.exact);
    }

    #[test]
    fn projected_parts_count_render_inputs_and_running_tool_inputs_are_stable() {
        let mut counts = Counts::default();
        configure(&mut counts, "gpt-4o");
        let mut tool = ToolPart::new("call", "bad:name", json!({"a": 1}), ToolStatus::Running);
        let mut running = page(vec![
            text(),
            entry("tool", Role::Assistant, PartBody::Tool(tool.clone())),
        ]);
        counts.page(&mut running);
        assert_eq!(running.token_family.as_deref(), Some("o200k/1"));
        assert_eq!(running.token_family_exact, Some(false));
        assert_eq!(
            running.entries[0].parts[0].tokens,
            Some(PartTokens::count(expected("§7§ hello world")))
        );
        assert_eq!(running.entries[1].parts[0].tokens, None);
        let cached_input = counts
            .parts
            .get(&("tool".into(), "0".into(), "o200k/1".into()))
            .unwrap()
            .input
            .unwrap();
        assert_eq!(cached_input, expected("bad_name_pO2PUf{\"a\":1}"));
        tool.status = ToolStatus::Completed;
        tool.output = Some("result".into());
        tool.set_delivered(vec![Delivered::new("mc", "notice text")]);
        let mut completed = page(vec![entry("tool", Role::Assistant, PartBody::Tool(tool))]);
        let digest = completed.entries[0].digest.clone();
        counts.page(&mut completed);
        assert_eq!(
            completed.entries[0].parts[0].tokens,
            Some(PartTokens::tool(
                cached_input,
                expected("§7§ result\n\nnotice text")
            ))
        );
        assert_eq!(
            completed.entries[0].digest, digest,
            "counts do not affect transcript checksums"
        );
        let first = completed.clone();
        counts.page(&mut completed);
        assert_eq!(completed, first);
    }

    #[test]
    fn images_use_known_dimensions_and_missing_media_stays_absent() {
        let mut counts = Counts::default();
        configure(&mut counts, "gpt-4o");
        let mut image = FileMedia::new("image/png", Transport::Reference);
        image.width = Some(640);
        image.height = Some(480);
        let mut read = page(vec![entry(
            "image",
            Role::User,
            PartBody::File(image.clone()),
        )]);
        counts.page(&mut read);
        assert_eq!(
            read.entries[0].parts[0].tokens,
            Some(PartTokens::count(image::tokens(
                image::rule_for("gpt-4o", image::Api::OpenAi),
                image::Size {
                    width: 640,
                    height: 480
                },
                None
            )))
        );
        image.width = None;
        let mut unknown = page(vec![entry("unknown", Role::User, PartBody::File(image))]);
        counts.page(&mut unknown);
        assert!(unknown.entries[0].parts[0].tokens.is_none());
        assert!(unknown.token_family.is_none());
        assert!(unknown.token_family_exact.is_none());
    }

    #[test]
    fn reasoning_uses_reported_counts_then_complete_visible_accounting() {
        let mut counts = Counts::default();
        configure(&mut counts, "gpt-4o");
        let mut answer = text();
        let mut usage = cortexkit_role_compaction::read::StepUsage::default();
        usage.reasoning_tokens = Some(19);
        answer.usage = Some(usage);
        counts.annotate(std::slice::from_mut(&mut answer));
        assert_eq!(
            answer.reasoning_tokens,
            Some(ReasoningTokens::new(19, false))
        );
        usage.reasoning_tokens = None;
        usage.output_tokens = Some(50);
        answer.usage = Some(usage);
        counts.annotate(std::slice::from_mut(&mut answer));
        assert_eq!(
            answer.reasoning_tokens,
            Some(ReasoningTokens::new(
                50 - expected("§7§ hello world"),
                false
            ))
        );
        answer.parts.push(Part::new(
            "1",
            PartBody::File(FileMedia::new("application/pdf", Transport::Reference)),
        ));
        counts.annotate(std::slice::from_mut(&mut answer));
        assert!(
            answer.reasoning_tokens.is_none(),
            "unknown visible operands are not zero"
        );
        answer.parts.pop();
        answer.usage = Some(cortexkit_role_compaction::read::StepUsage::default());
        counts.annotate(std::slice::from_mut(&mut answer));
        assert!(answer.reasoning_tokens.is_none());
    }

    #[test]
    fn frozen_structured_output_counts_its_shared_tool_and_schema_instructions() {
        let mut counts = Counts::default();
        let mut render = json!({"family_id": "openai_chat", "structured_output": {
            "mode": "submit_tool", "name": "result", "schema_jcs": "{\"type\":\"object\"}",
            "enforcement": "best_effort", "delivery": "unconstrained", "instructions_paragraph_break": true,
        }});
        configure_render(&mut counts, "gpt-4o", render.clone());
        let definitions = json!([{
            "name": "submit_result", "input_schema": {"type": "object"},
            "description": broca_protocol::structured_output::SUBMIT_RESULT_DESCRIPTION,
        }]);
        assert_eq!(
            counts.facts.as_ref().unwrap().tools_count.unwrap().count,
            expected(&definitions.to_string())
        );
        render["structured_output"]["mode"] = json!("final_text");
        configure_render(&mut counts, "gpt-4o", render);
        let schema_text = "\n\nYour final answer must be a single JSON value, with no other text before or after it, that validates against this JSON Schema (draft 2020-12) named \"result\":\n{\"type\":\"object\"}";
        assert_eq!(
            counts.facts.as_ref().unwrap().system.unwrap().count,
            expected("system instructions") + expected(schema_text)
        );
    }

    #[test]
    fn compact_pages_preserve_counts_and_family_switches_recount() {
        use cortexkit_role_compaction::read::{Detail, Select};
        let mut counts = Counts::default();
        configure(&mut counts, "gpt-4o");
        let request = ReadRequest::new(
            Select::After {
                after_ordinal: None,
                limit: None,
            },
            Detail::Compact,
        );
        let compact = counts.finish_page(page(vec![text()]), &request);
        assert!(compact.entries[0].parts[0].tokens.is_some());
        assert_eq!(compact.token_family.as_deref(), Some("o200k/1"));
        configure(&mut counts, "claude-opus-5");
        assert!(
            counts.parts.is_empty(),
            "model changes invalidate the count cache"
        );
        let mut next = page(vec![text()]);
        counts.page(&mut next);
        let family = family_for_model("claude-opus-5");
        assert_eq!(next.token_family, Some(family.family.token_family()));
        assert_eq!(
            next.entries[0].parts[0].tokens,
            count(family.family, "§7§ hello world").map(PartTokens::count)
        );
        let mut unknown = page(vec![entry(
            "reasoning",
            Role::Assistant,
            PartBody::Reasoning {
                text: Some("thought".into()),
                text_bytes: Some(7),
            },
        )]);
        counts.page(&mut unknown);
        assert!(unknown.entries[0].parts[0].tokens.is_none());
        assert!(unknown.token_family.is_none());
    }

    #[test]
    fn image_rule_changes_change_the_opaque_family_even_with_the_same_text_tokenizer() {
        assert_eq!(
            family_for_model("gpt-4o").family,
            family_for_model("gpt-5.2").family
        );
        assert_ne!(
            token_family_id("gpt-4o", image::Api::OpenAi),
            token_family_id("gpt-5.2", image::Api::OpenAi)
        );
        assert!(token_family_id("gemini-3-pro", image::Api::Google).contains("+img:fixed-"));
        assert_eq!(
            token_family_id("claude-opus-5", image::Api::Other),
            "claude-v5/1"
        );
        let mut counts = Counts::default();
        configure(&mut counts, "gpt-4o");
        let mut file = FileMedia::new("image/png", Transport::Reference);
        file.width = Some(4000);
        file.height = Some(3000);
        let original = entry("image", Role::User, PartBody::File(file));
        let mut first = page(vec![original.clone()]);
        counts.page(&mut first);
        configure(&mut counts, "gpt-5.2");
        let mut next = page(vec![original]);
        counts.page(&mut next);
        assert_ne!(next.token_family, first.token_family);
        assert_eq!(
            next.entries[0].parts[0].tokens,
            Some(PartTokens::count(image::tokens(
                image::rule_for("gpt-5.2", image::Api::OpenAi),
                image::Size {
                    width: 4000,
                    height: 3000
                },
                None
            )))
        );
    }

    #[test]
    fn tool_input_count_does_not_change_when_a_colliding_name_enters_the_request() {
        let mut counts = Counts::default();
        configure(&mut counts, "gpt-4o");
        let tool = ToolPart::new("call", "bad:name", json!({}), ToolStatus::Running);
        let original = entry("call", Role::Assistant, PartBody::Tool(tool));
        counts.annotate(&mut [original.clone()]);
        let key = ("call".into(), "0".into(), "o200k/1".into());
        let first = counts.parts.get(&key).unwrap().input;
        let collision = entry(
            "other",
            Role::Assistant,
            PartBody::Tool(ToolPart::new(
                "other",
                "bad_name_pO2PUf",
                json!({}),
                ToolStatus::Running,
            )),
        );
        counts.annotate(&mut [original, collision]);
        assert_eq!(counts.parts.get(&key).unwrap().input, first);
        assert_eq!(first, Some(expected("bad_name_pO2PUf{}")));
    }

    #[derive(Default)]
    struct Plane(std::sync::Mutex<Vec<(String, serde_json::Value)>>, bool);

    #[async_trait::async_trait]
    impl super::super::runtime::Plane for Plane {
        async fn provides_notes_tokens(&self) -> bool {
            self.1
        }
        async fn call(
            &self,
            method: &str,
            params: serde_json::Value,
        ) -> Result<Box<serde_json::value::RawValue>, super::super::runtime::CallError> {
            self.0.lock().unwrap().push((method.into(), params));
            let answer = match method {
                "compaction.setup" => "ready",
                "compaction.notice" => "received",
                _ => "nothing",
            };
            Ok(serde_json::value::to_raw_value(&json!({"answer": answer})).unwrap())
        }
    }

    #[tokio::test]
    async fn every_notice_carries_cached_surface_counts_but_rides_and_setup_do_not() {
        use cortexkit_role_compaction::{
            ride::{RideCause, RideRequest},
            setup::ModelRef,
        };
        let runtime = super::super::runtime::Runtime::default();
        configure(&mut runtime.tokens.lock().unwrap(), "gpt-4o");
        let plane = Plane::default();
        let mut conditions = super::super::conditions::conditions(
            ModelRef::new("stub", "gpt-4o"),
            200_000,
            1000,
            false,
        );
        let setup = super::super::conditions::setup_request("default", &conditions, 100, false);
        runtime.observe(&[text()]);
        runtime.flush(&plane, &setup, &conditions, None).await;
        let mut second = text();
        second.ordinal = 2;
        second.mid = "second".into();
        runtime.observe(&[second]);
        runtime.flush(&plane, &setup, &conditions, None).await;
        // Even a caller's populated condition counts cannot leak onto the ride wire.
        conditions.system_tokens = Some(SurfaceTokens::new(999, true));
        runtime
            .ride(
                &plane,
                &setup,
                &RideRequest::new(vec![RideCause::ModelChange], 1000, conditions),
            )
            .await;
        let calls = plane.0.lock().unwrap();
        let notices: Vec<_> = calls
            .iter()
            .filter(|(method, _)| method == "compaction.notice")
            .map(|(_, value)| value)
            .collect();
        assert_eq!(notices.len(), 2);
        for notice in notices {
            assert_eq!(notice["token_family"], "o200k/1");
            assert_eq!(
                notice["conditions"]["system_tokens"]["count"],
                expected("system instructions")
            );
            assert_eq!(notice["conditions"]["system_tokens"]["exact"], false);
            assert_eq!(notice["conditions"]["tools_tokens"]["exact"], false);
            assert!(notice["conditions"].get("notes_tokens").is_none());
            assert!(notice["entries"][0]["parts"][0]["tokens"].is_object());
        }
        let ride = &calls
            .iter()
            .find(|(method, _)| method == "compaction.ride")
            .unwrap()
            .1;
        assert!(ride["conditions"].get("system_tokens").is_none());
        assert!(ride["conditions"].get("tools_tokens").is_none());
        assert!(ride["conditions"].get("notes_tokens").is_none());
        let setup = &calls
            .iter()
            .find(|(method, _)| method == "compaction.setup")
            .unwrap()
            .1;
        assert!(setup.get("system_tokens").is_none());
        assert!(setup.get("tools_tokens").is_none());
    }

    #[tokio::test]
    async fn declaring_provider_gets_known_empty_notes_without_counting_transcript_reminders_twice()
    {
        use cortexkit_role_compaction::setup::ModelRef;
        let runtime = super::super::runtime::Runtime::default();
        configure(&mut runtime.tokens.lock().unwrap(), "claude-opus-5");
        let plane = Plane(Default::default(), true);
        let conditions = super::super::conditions::conditions(
            ModelRef::new("stub", "claude-opus-5"),
            200_000,
            1000,
            false,
        );
        let setup = super::super::conditions::setup_request("default", &conditions, 100, false);
        runtime.observe(&[entry(
            "reminder",
            Role::User,
            PartBody::Text {
                text: "one-request reminder".into(),
            },
        )]);
        runtime.flush(&plane, &setup, &conditions, None).await;
        let calls = plane.0.lock().unwrap();
        let notice = &calls
            .iter()
            .find(|(method, _)| method == "compaction.notice")
            .unwrap()
            .1;
        assert_eq!(
            notice["conditions"]["notes_tokens"],
            json!({"count": 0, "exact": true})
        );
        assert_eq!(
            notice["conditions"]["system_tokens"]["count"],
            count(Family::ClaudeV5, "system instructions").unwrap()
        );
        assert!(notice["entries"][0]["parts"][0]["tokens"].is_object());
        assert_eq!(notice["token_family"], "claude-v5/1");
    }

    #[tokio::test]
    async fn a_family_switch_refreshes_counts_already_pending_in_a_notice() {
        use cortexkit_role_compaction::setup::ModelRef;
        let runtime = super::super::runtime::Runtime::default();
        configure(&mut runtime.tokens.lock().unwrap(), "gpt-4o");
        let original = text();
        runtime.observe(std::slice::from_ref(&original));
        configure(&mut runtime.tokens.lock().unwrap(), "claude-opus-5");
        runtime.observe(std::slice::from_ref(&original));
        let conditions = super::super::conditions::conditions(
            ModelRef::new("stub", "claude-opus-5"),
            200_000,
            1000,
            false,
        );
        let setup = super::super::conditions::setup_request("default", &conditions, 100, false);
        let plane = Plane::default();
        runtime.flush(&plane, &setup, &conditions, None).await;
        let calls = plane.0.lock().unwrap();
        let notice = &calls
            .iter()
            .find(|(method, _)| method == "compaction.notice")
            .unwrap()
            .1;
        assert_eq!(notice["token_family"], "claude-v5/1");
        assert_eq!(
            notice["entries"][0]["parts"][0]["tokens"]["count"],
            count(Family::ClaudeV5, "§7§ hello world").unwrap()
        );
    }
}
