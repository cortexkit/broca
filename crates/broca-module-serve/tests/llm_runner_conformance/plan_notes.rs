//! A Messages fixture for the plan-note cases; ordinary scripted Chat has no note turn.

use super::*;
use cortexkit_role_llm_runner_conformance::{ModelRequestObservation, PlanNotesFixture};

const PRESET: &str = "plan-notes";
pub(super) const MODEL_PREFIX: &str = "conformance-plan-notes-";
const CASE_MODELS: [(&str, &str); 7] = [
    (
        "plan_notes_pending_until_reconcile",
        "conformance-plan-notes-reconcile",
    ),
    (
        "plan_notes_not_stored_entries",
        "conformance-plan-notes-not-stored",
    ),
    (
        "plan_notes_removed_tool_refused",
        "conformance-plan-notes-tool-refused",
    ),
    (
        "plan_notes_system_note_not_full_text",
        "conformance-plan-notes-short-note",
    ),
    (
        "plan_notes_restore_after_delivery",
        "conformance-plan-notes-restore-delivered",
    ),
    (
        "plan_notes_restore_before_delivery",
        "conformance-plan-notes-restore-undelivered",
    ),
    (
        "plan_notes_restore_requires_system_note",
        "conformance-plan-notes-restore-missing-note",
    ),
];
const ORIGINAL: &str = "Review reports using the original rubric.";
const REPLACEMENT: &str = "Review reports using the replacement rubric and include a summary.";
const NOTE: &str = "Switch to the replacement rubric.";
const CORRECTION: &str = "Use the original rubric again.";

/// The kit mints one session for each case. Identify it by its test model rather than by
/// a marker in the system text whose replacement the cases are testing.
pub(super) fn model_for_session(session: &str) -> Option<&'static str> {
    let (case, _) = session.strip_prefix("cf-")?.rsplit_once('-')?;
    CASE_MODELS
        .iter()
        .find_map(|&(name, model)| (name == case).then_some(model))
}

pub(super) fn catalog_rows() -> impl Iterator<Item = (String, Value)> {
    let mut row = output_refs::catalog_row();
    row["capability.mid_conversation_system"] = json!(true);
    CASE_MODELS
        .into_iter()
        .map(move |(_, model)| (format!("{}/{model}", output_refs::PROVIDER), row.clone()))
}

pub(super) fn session_for_model<'a>(
    model: &str,
    mut sessions: impl Iterator<Item = &'a String>,
) -> Option<String> {
    sessions
        .find(|session| model_for_session(session) == Some(model))
        .cloned()
}

fn plan(replacement: bool) -> Map<String, Value> {
    let tools = if replacement {
        json!([])
    } else {
        json!([{"name":SCRIPTED_TOOL,"capabilities":[]}])
    };
    let composition = json!({"providers":[{"provider":TOOL_MODULE,"tools":tools}],
        "step_transform_items":[]});
    let tool_items = if replacement {
        json!([])
    } else {
        json!([{"provider":TOOL_MODULE,"preset":"conformance","params":{}}])
    };
    // Anthropic defaults to five-minute cache markers. The suite advances the daemon's
    // clock, but model requests use wall time; don't let that difference expire the prefix.
    json!({
        "tool_items":tool_items,
        "system_text_items":[{"provider":TOOL_MODULE,"preset":PRESET,"params":{"replacement":replacement}}],
        "step_transform_items":[],
        "cache":{"ttl":"none"},
        "composition_digest":cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),
        "composition":composition,
    })
    .as_object()
    .unwrap()
    .clone()
}

pub(super) fn fixture() -> PlanNotesFixture {
    PlanNotesFixture::new(plan(true), NOTE, REPLACEMENT, SCRIPTED_TOOL)
        .with_restoration(plan(false), CORRECTION)
}

pub(super) fn send_fields(model: &str, first: bool) -> Map<String, Value> {
    let mut fields = Map::new();
    fields.insert(
        "model".into(),
        json!({"provider":output_refs::PROVIDER,"model":model}),
    );
    if first {
        fields.insert("session_class".into(), json!("head"));
        fields.insert("plan".into(), plan(false).into());
    }
    fields
}

pub(super) fn system_text(item: &Value) -> Option<&'static str> {
    if item["preset"] != PRESET {
        return None;
    }
    Some(match item["params"]["replacement"].as_bool()? {
        true => REPLACEMENT,
        false => ORIGINAL,
    })
}

pub(super) fn observe(request: &Value) -> ModelRequestObservation {
    fn texts(content: &Value) -> impl Iterator<Item = String> + '_ {
        // Messages renders a turn-system message's content as a string, while
        // ordinary conversational messages and the top-level system use blocks.
        content
            .as_str()
            .into_iter()
            .chain(
                content
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|part| part["text"].as_str()),
            )
            .map(str::to_owned)
    }

    let tools = request["tools"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|tool| tool["name"].as_str().map(str::to_owned))
        .collect();
    let mut observation = ModelRequestObservation::new(tools);
    let mut system_text: Vec<_> = texts(&request["system"]).collect();
    let mut rendered = Vec::new();
    for message in request["messages"].as_array().into_iter().flatten() {
        let parts = texts(&message["content"]);
        if message["role"] == "system" {
            system_text.extend(parts);
        } else {
            rendered.extend(parts);
        }
    }
    observation.system_text = Some(system_text);
    observation.with_rendered(rendered)
}

#[test]
fn observes_both_plan_notes_from_the_real_messages_renderer() {
    use broca_protocol::{CallOptions, ContentBlock, Message, Role, Tool};
    use broca_provider::{
        families::{anthropic_frozen_config, AnthropicMessages},
        policy::SystemPromptPolicy,
        wire::WireFamily,
    };

    let model = model_for_session("cf-plan_notes_system_note_not_full_text-457").unwrap();
    let mut frozen = anthropic_frozen_config(model);
    frozen.anthropic_turn_system = true;
    frozen.system_prompt = SystemPromptPolicy::TopLevelField;
    let text = |ordinal, role, text: &str| {
        let mut message = Message::new(role, vec![ContentBlock::Text { text: text.into() }]);
        message.ordinal = Some(ordinal);
        message
    };
    let mut opts = CallOptions::new(vec![
        text(0, Role::System, ORIGINAL),
        text(1, Role::User, "start"),
        text(2, Role::Assistant, "first answer"),
        text(3, Role::User, "deliver notes"),
    ]);
    let tool: Tool = serde_json::from_value(json!({
        "name": SCRIPTED_TOOL,
        "description": "Answers each call with its scripted result.",
        "input_schema": {"type":"object"},
    }))
    .unwrap();
    opts.tools = Arc::new(vec![tool]);
    let removal = broca_core::plan_notes::tool_removal(SCRIPTED_TOOL);
    use broca_core::plan_notes::{view, Note::LegacyText};
    let opts = view(&opts, [(4, LegacyText(NOTE)), (4, LegacyText(&removal))]).unwrap();
    let request: Value = serde_json::from_slice(&AnthropicMessages.render(&frozen, &opts)).unwrap();

    let messages = request["messages"].as_array().unwrap();
    assert_eq!(messages[2]["content"][0]["text"], "deliver notes");
    let note = format!("<context-note>{NOTE}</context-note>");
    assert_eq!(messages[3], json!({"role":"user", "content":note}));
    assert_eq!(messages[4], json!({"role":"system", "content":removal}));

    let observed = observe(&request);
    assert_eq!(observed.tools, [SCRIPTED_TOOL]);
    assert_eq!(
        observed.rendered,
        ["start", "first answer", "deliver notes", &note]
    );
    assert_eq!(observed.system_text.unwrap(), [ORIGINAL, &removal]);
}

#[test]
fn plan_note_scripts_are_identified_without_a_prompt_marker() {
    let sessions = [
        "cf-plan_notes_restore_before_delivery-434".to_owned(),
        "cf-plan_notes_restore_after_delivery-433".to_owned(),
    ];
    let fields = send_fields(model_for_session(&sessions[1]).unwrap(), true);
    let model = fields["model"]["model"].as_str().unwrap();
    assert_eq!(fields["plan"]["cache"], fixture().plan["cache"]);
    let later = send_fields(model, false);
    assert_eq!(later["model"], fields["model"]);
    assert!(!later.contains_key("plan"));
    assert_eq!(
        session_for_model(model, sessions.iter()).as_deref(),
        Some(sessions[1].as_str())
    );
    assert!(catalog_rows().any(|(key, _)| key == format!("anthropic/{model}")));
    assert_eq!(session_for_model("mock-model", sessions.iter()), None);
}
