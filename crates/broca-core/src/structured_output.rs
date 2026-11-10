//! Structured run results and the assistant text or submit call that produces them.

use broca_protocol::structured_output::FrozenStructuredOutput;
use broca_protocol::{AssistantMessage, ContentBlock, ToolCall, ToolResult};

/// The text parts of `message` joined exactly as `run.result` joins them: in order, with
/// nothing inserted. This is the answer a `final_text` run validates.
pub fn joined_text(message: &AssistantMessage) -> String {
    message
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } => {
                Some(text.as_str())
            }
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(cortexkit_role_llm_runner::run::TEXT_SEPARATOR)
}

/// The `submit_result` call that ends a `submit_tool` run at this step: the first one the
/// model emitted that wasn't refused for unparseable arguments. `None` for a `final_text` run or
/// a step without one.
pub fn counted_submit(
    frozen: &FrozenStructuredOutput,
    message: &AssistantMessage,
    rejected: &[ToolResult],
) -> Option<ToolCall> {
    if frozen.mode != broca_protocol::structured_output::Mode::SubmitTool {
        return None;
    }
    message
        .tool_calls()
        .find(|call| {
            call.tool_name == broca_protocol::structured_output::SUBMIT_RESULT_TOOL
                && !rejected
                    .iter()
                    .any(|result| result.tool_call_id == call.tool_call_id)
        })
        .cloned()
}

/// What an ended run admitted with structured output produced, as `run.status` and `run.result`
/// serve it. A completed run recorded its result in the commit that ended it (`recorded`). Any
/// other end produced no value, which the frozen settings report as absent. `None` for a run
/// admitted without structured output.
pub fn ended_structured_output(
    recorded: Option<broca_protocol::structured_output::StructuredOutputResult>,
    frozen: Option<&broca_protocol::structured_output::FrozenStructuredOutput>,
) -> Option<broca_protocol::structured_output::StructuredOutputResult> {
    recorded.or_else(|| {
        frozen.map(|frozen| {
            broca_protocol::structured_output::result(frozen, false, None, None, Vec::new())
        })
    })
}
