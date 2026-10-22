//! The crash-cut scenarios, as scripted runs the harness can kill anywhere.
//!
//! The edit, identity-embedding, budget-nudge and repetition scenarios use the requests,
//! scripts and tools of the every-cut tests broca-core's `tests/it/resume_exec.rs` used to run;
//! the text-only max-steps scenario is the run from `tests/it/terminal_reason_crash_cut.rs`. The
//! session's project root is a fixed path rather than a scratch directory, because every kill
//! copies the same log into a fresh state root and the log's path is derived from the session.

#![allow(dead_code)]

use broca_core::{RunControl, RunRequest, StubReply, StubToolPlane};
use broca_crash_harness::{Render, Scenario};
use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput},
    provider::{NormalizedEvent, ProviderControl},
    request::{
        BudgetConfig, BudgetNudge, ExecutionMode, ModelHandle, StopCondition, Tool, ToolChoice,
    },
    BindIdentity, FinishReason, GenerationConfig, MessageId, RunId, ToolCallId, Usage,
};
use serde_json::json;

pub const PROJECT_ROOT: &str = "/crash-harness/project";

pub fn session() -> BindIdentity {
    BindIdentity::new(PROJECT_ROOT, "m9", "s1")
}

fn assistant(
    message_id: &str,
    content: Vec<ContentBlock>,
    finish: FinishReason,
) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(message_id.into()),
                content,
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: finish,
            retries_used: 0,
        }),
    ]
}

pub fn tool_call(id: &str, name: &str, input: serde_json::Value) -> ContentBlock {
    ContentBlock::ToolCall(ToolCall {
        thought_signature: None,
        tool_call_id: ToolCallId(id.into()),
        tool_name: name.into(),
        input,
    })
}

pub fn text(text: &str) -> ContentBlock {
    ContentBlock::Text { text: text.into() }
}

/// Step 1 emits two parallel `edit` calls, step 2 is the text reply.
pub fn two_edits_scripts() -> Vec<Vec<NormalizedEvent>> {
    vec![
        vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: MessageId("m1".into()),
                    content: vec![
                        tool_call("t1", "edit", json!({"f": "a"})),
                        tool_call("t2", "edit", json!({"f": "b"})),
                    ],
                },
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage {
                    input_tokens: Some(100),
                    output_tokens: Some(10),
                    ..Default::default()
                },
                finish_reason: FinishReason::ToolCalls,
                retries_used: 0,
            }),
        ],
        vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: MessageId("m2".into()),
                    content: vec![text("done")],
                },
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage {
                    input_tokens: Some(50),
                    output_tokens: Some(4),
                    ..Default::default()
                },
                finish_reason: FinishReason::Stop,
                retries_used: 0,
            }),
        ],
    ]
}

/// Step 1 emits one `edit` call, step 2 is the text reply.
pub fn one_edit_scripts() -> Vec<Vec<NormalizedEvent>> {
    vec![
        assistant(
            "one-1",
            vec![tool_call("t1", "edit", json!({"f": "a"}))],
            FinishReason::ToolCalls,
        ),
        assistant("one-2", vec![text("done")], FinishReason::Stop),
    ]
}

pub fn repetition_scripts() -> Vec<Vec<NormalizedEvent>> {
    vec![
        assistant(
            "repeat-1",
            vec![tool_call(
                "repeat-1",
                "read",
                json!({"filePath": "missing.rs"}),
            )],
            FinishReason::ToolCalls,
        ),
        assistant(
            "repeat-2",
            vec![tool_call(
                "repeat-2",
                "read",
                json!({"filePath": "missing.rs"}),
            )],
            FinishReason::ToolCalls,
        ),
        assistant(
            "repeat-done",
            vec![text("using another approach")],
            FinishReason::Stop,
        ),
    ]
}

pub fn one_text_step_scripts() -> Vec<Vec<NormalizedEvent>> {
    vec![assistant("m1", vec![text("done")], FinishReason::Stop)]
}

pub fn edit_request() -> RunRequest {
    RunRequest {
        directives: None,
        session_class: None,
        retention: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        scope: None,
        project_id: None,
        principal: None,
        send_id: None,
        request_identity_digest: None,
        run_id: RunId("r".into()),
        session: session(),
        model: ModelHandle::new("stub", "m"),
        input: vec![Message::new(Role::User, vec![text("edit a and b")])],
        tools: vec![Tool {
            name: "edit".into(),
            description: "edit a file".into(),
            input_schema: json!({"type": "object"}),
            module: None,
            execution_mode: ExecutionMode::Parallel,
            reply: None,
        }],
        policy_allowed: ["edit".to_string()].into_iter().collect(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![StopCondition::StepCount { n: 5 }],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
        structured_output: None,
    }
}

pub fn budget_request() -> RunRequest {
    let mut request = edit_request();
    request.budget = Some(BudgetConfig {
        max_input_tokens: None,
        nudges: vec![BudgetNudge {
            at_tool_calls: 2,
            text: "Check the work, then continue exactly once.".into(),
        }],
        finalize_at_tool_calls: 99,
        finalize_text: "finalize".into(),
    });
    request
}

pub fn input_cap_request() -> RunRequest {
    let mut request = edit_request();
    request.budget = Some(BudgetConfig {
        max_input_tokens: Some(150),
        ..Default::default()
    });
    request
}

pub fn input_cap_tool_request() -> RunRequest {
    let mut request = input_cap_request();
    request.budget.as_mut().unwrap().max_input_tokens = Some(100);
    request
}

pub fn repetition_request() -> RunRequest {
    let mut request = edit_request();
    request.tools = vec![Tool {
        name: "read".into(),
        description: "read a file".into(),
        input_schema: json!({"type": "object"}),
        module: None,
        execution_mode: ExecutionMode::Sequential,
        reply: None,
    }];
    request.policy_allowed = ["read".to_string()].into_iter().collect();
    request
}

pub fn max_steps_request() -> RunRequest {
    let mut request = edit_request();
    request.run_id = RunId("r1".into());
    request.input = vec![Message::new(Role::User, vec![text("Say done.")])];
    request.tools = vec![];
    request.policy_allowed = Default::default();
    request.stop_when = vec![StopCondition::StepCount { n: 1 }];
    request
}

pub fn edit_plane() -> StubToolPlane {
    StubToolPlane::new().with_tool("edit", |c| {
        StubReply::Output(ToolOutput::Text {
            text: format!("edited-{}", c.tool_call_id.0),
        })
    })
}

pub fn repetition_plane() -> StubToolPlane {
    StubToolPlane::new().with_tool("read", |_| StubReply::ExecError("file not found".into()))
}

pub fn empty_plane() -> StubToolPlane {
    StubToolPlane::new()
}

fn default_control() -> RunControl {
    RunControl::default()
}

fn inherited_control() -> RunControl {
    RunControl {
        tools_were_omitted: true,
        ..Default::default()
    }
}

/// Two parallel edits under an inherited (omitted) tool surface.
pub const INHERITED_TOOL_SURFACE: Scenario = Scenario {
    name: "inherited_tool_surface",
    request: edit_request,
    control: inherited_control,
    scripts: two_edits_scripts,
    render: Render::Canonical,
    plane: edit_plane,
    expected: RunFinishReason::Completed,
};

/// Two parallel edits.
pub const TWO_EDITS: Scenario = Scenario {
    name: "two_edits",
    request: edit_request,
    control: default_control,
    scripts: two_edits_scripts,
    render: Render::Canonical,
    plane: edit_plane,
    expected: RunFinishReason::Completed,
};

/// One edit, then the text reply: small enough to write every cut's outcome down by hand.
pub const ONE_EDIT: Scenario = Scenario {
    name: "one_edit",
    request: edit_request,
    control: default_control,
    scripts: one_edit_scripts,
    render: Render::Canonical,
    plane: edit_plane,
    expected: RunFinishReason::Completed,
};

/// Two parallel edits rendered with an embedded run id and step index.
pub const ANTIGRAVITY_IDENTITY: Scenario = Scenario {
    name: "antigravity_identity",
    request: edit_request,
    control: default_control,
    scripts: two_edits_scripts,
    render: Render::IdentityEmbedding,
    plane: edit_plane,
    expected: RunFinishReason::Completed,
};

/// Two parallel edits whose second call trips a budget nudge.
pub const BUDGET_NUDGE: Scenario = Scenario {
    name: "budget_nudge",
    request: budget_request,
    control: default_control,
    scripts: two_edits_scripts,
    render: Render::Canonical,
    plane: edit_plane,
    expected: RunFinishReason::Completed,
};

/// Two model steps consume 100 then 50 input tokens. Truncating the WAL before or after
/// either usage record must still yield a consumed sum of 150 after recovery.
pub const INPUT_TOKEN_CAP: Scenario = Scenario {
    name: "input_token_cap",
    request: input_cap_request,
    control: default_control,
    scripts: two_edits_scripts,
    render: Render::Canonical,
    plane: edit_plane,
    expected: RunFinishReason::BudgetExhausted,
};

/// The first model step reaches 100 input tokens and emits two tool calls. WAL truncation
/// during their synthetic error results must not dispatch either call or lose the budget terminal.
pub const INPUT_TOKEN_CAP_TOOLS: Scenario = Scenario {
    name: "input_token_cap_tools",
    request: input_cap_tool_request,
    control: default_control,
    scripts: two_edits_scripts,
    render: Render::Canonical,
    plane: edit_plane,
    expected: RunFinishReason::BudgetExhausted,
};

/// The same failing read twice, so the third request carries repetition guidance.
pub const REPETITION_GUIDANCE: Scenario = Scenario {
    name: "repetition_guidance",
    request: repetition_request,
    control: default_control,
    scripts: repetition_scripts,
    render: Render::Canonical,
    plane: repetition_plane,
    expected: RunFinishReason::Completed,
};

/// One text step under a one-step limit. The step requested no tools, so the run ended on its
/// own and is `completed`, even though the step was also the limit.
pub const TEXT_ONLY_MAX_STEPS: Scenario = Scenario {
    name: "text_only_max_steps",
    request: max_steps_request,
    control: default_control,
    scripts: one_text_step_scripts,
    render: Render::Canonical,
    plane: empty_plane,
    expected: RunFinishReason::Completed,
};

pub const ALL: &[Scenario] = &[
    INHERITED_TOOL_SURFACE,
    TWO_EDITS,
    ANTIGRAVITY_IDENTITY,
    BUDGET_NUDGE,
    INPUT_TOKEN_CAP,
    INPUT_TOKEN_CAP_TOOLS,
    REPETITION_GUIDANCE,
    TEXT_ONLY_MAX_STEPS,
];

/// The system text of [`planned_manifest`], as its provider answered it.
pub const PLANNED_TEXT: &str = "Edit carefully.";

/// The frozen manifest of a session admitted with a one-provider fetch plan: `edit`, served by
/// `aft`, and `aft`'s system text. Its plan and digests are fixed values; the run loop records
/// the manifest and restores its provider presets on resume.
pub fn planned_manifest() -> broca_wal::PlanManifest {
    let tools = std::sync::Arc::new(vec![Tool {
        name: "edit".into(),
        description: "edit a file".into(),
        input_schema: json!({"type": "object"}),
        module: Some("aft".into()),
        execution_mode: ExecutionMode::Parallel,
        reply: None,
    }]);
    // SHA-256 of `{}` for the illustrative catalog and composition.
    let empty_object = "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a";
    // A non-confining preset: these sessions run unscoped, and an episode whose plan asks for
    // the `worker` preset refuses to start without a scope.
    broca_wal::PlanManifest {
        declarations: Vec::new(),
        plan: broca_wal::OpaqueJson::from_json_text(
            r#"{"tool_items":[{"provider":"aft","preset":"reader"}]}"#,
        )
        .unwrap(),
        plan_identity: "a0dfbc0f5d00737f5634ed960e392e853478c1eb05570968e18e24ec00988648".into(),
        composition_digest: empty_object.into(),
        fetches: vec![broca_wal::ProviderFetch {
            provider: "aft".into(),
            outcome: broca_wal::FetchOutcome::Answered {
                catalog_digest: empty_object.into(),
                tools: tools.clone(),
                text: Some(PLANNED_TEXT.into()),
                result_ops: None,
                reply: None,
            },
        }],
        tools,
        system_text: PLANNED_TEXT.into(),
        system_digest: "7ba8844a3d30a8563b097fea879ab4d8de242c1ed37af7562e976881960afdcf".into(),
        join_version: 1,
        role_versions: [("tool-provider".to_owned(), "v1".to_owned())]
            .into_iter()
            .collect(),
    }
}

/// The request a session admitted with [`planned_manifest`] sends its first episode: the
/// manifest's tools, and its system text before the prompt.
pub fn planned_request() -> RunRequest {
    let mut request = edit_request();
    let manifest = planned_manifest();
    request.tools = manifest.tools.as_ref().clone();
    request
        .input
        .insert(0, Message::new(Role::System, vec![text(PLANNED_TEXT)]));
    request
}

fn planned_control() -> RunControl {
    RunControl {
        manifest: Some(std::sync::Arc::new(planned_manifest())),
        ..Default::default()
    }
}

/// Two parallel edits in a session admitted with a fetch plan: its first `RunStarted` freezes
/// the plan manifest.
pub const PLANNED_EDITS: Scenario = Scenario {
    name: "planned_edits",
    request: planned_request,
    control: planned_control,
    scripts: two_edits_scripts,
    render: Render::Canonical,
    plane: edit_plane,
    expected: RunFinishReason::Completed,
};

/// One edit, run with `RunnerHandle::stopping`: the module's stop request arrives while the edit
/// runs, so the edit finishes, no second step starts, and the run ends Interrupted.
pub const STOPPED_ONE_EDIT: Scenario = Scenario {
    name: "stopped_one_edit",
    request: edit_request,
    control: default_control,
    scripts: one_edit_scripts,
    render: Render::Canonical,
    plane: edit_plane,
    expected: RunFinishReason::Interrupted,
};

/// Two parallel edits, run with `RunnerHandle::stopping`: the stop request arrives while the
/// first edit runs, so the second is never sent and the run ends Interrupted.
pub const STOPPED_TWO_EDITS: Scenario = Scenario {
    name: "stopped_two_edits",
    request: edit_request,
    control: default_control,
    scripts: two_edits_scripts,
    render: Render::Canonical,
    plane: edit_plane,
    expected: RunFinishReason::Interrupted,
};

/// Find a scenario by name, for a spawned runner process.
pub fn by_name(name: &str) -> Option<Scenario> {
    ALL.iter()
        .copied()
        .chain([ONE_EDIT, PLANNED_EDITS])
        .find(|scenario| scenario.name == name)
}
