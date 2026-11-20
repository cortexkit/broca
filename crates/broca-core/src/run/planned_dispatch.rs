use super::*;

impl PlannedDispatch {
    pub(super) fn into_tool_call(self) -> ToolCall {
        ToolCall {
            tool_call_id: self.tool_call_id,
            tool_name: self.tool_name,
            input: self.text,
            thought_signature: self.thought_signature,
            provider_state: None,
        }
    }
}
