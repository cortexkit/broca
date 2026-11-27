//! Provider-native declarations carried by plan notes in a request view.

use crate::json::Json;
use crate::text::Text;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolAdditionForm {
    ResponsesAdditionalTools,
    KimiSystemTools,
    AnthropicToolAddition,
}

impl ToolAdditionForm {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ResponsesAdditionalTools => "responses_additional_tools",
            Self::KimiSystemTools => "kimi_system_tools",
            Self::AnthropicToolAddition => "anthropic_tool_addition",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        [
            Self::ResponsesAdditionalTools,
            Self::KimiSystemTools,
            Self::AnthropicToolAddition,
        ]
        .into_iter()
        .find(|form| form.as_str() == name)
    }
}

/// Only the model-facing definition. Routing and execution metadata are frozen separately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: Json,
}

impl ToolDefinition {
    /// A request declaration only; dispatch metadata comes from the durable run's manifest.
    pub fn as_tool(&self) -> crate::Tool {
        crate::Tool {
            name: self.name.clone(),
            description: self.description.clone(),
            input_schema: self.input_schema.clone(),
            module: None,
            execution_mode: Default::default(),
            reply: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolAddition {
    pub form: ToolAdditionForm,
    pub parts: Vec<AdditionPart>,
}

/// Anthropic accepts one system message at this position. Its native blocks can share that
/// message with the text notes delivered at the same anchor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdditionPart {
    Text(Text),
    Tool(ToolDefinition),
}
