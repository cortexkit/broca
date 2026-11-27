use broca_protocol::{
    request::{ExecutionMode, ToolReply},
    tool_addition::ToolDefinition,
    Tool,
};
use broca_records::record::{
    DispatchSchemaPin, FetchOutcome, FrozenProviderDeclarations, FrozenReply, PlanManifest,
};
use serde::{Deserialize, Serialize};

use super::{by_module, reply_from_frozen, ToolManifest};

/// The durable dispatch half of an inline tool declaration. Model-facing fields are kept in
/// the note itself so the dispatcher and renderer cannot disagree on the definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AddedToolDispatch {
    pub module: Option<String>,
    pub sequential: bool,
    pub reply: Option<ToolReply>,
    pub provider: Option<ProviderTransport>,
    pub provider_reply: Option<FrozenReply>,
    pub schema_pin: Option<DispatchSchemaPin>,
}

/// The dispatch plane shares transport facts among all tools routed to the same provider.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderTransport {
    pub preset: Option<String>,
    pub fetched: bool,
    pub declarations: FrozenProviderDeclarations,
}

impl ProviderTransport {
    /// `None` means the provider has no dispatch entry in this plan.
    pub fn of(plan: &PlanManifest, provider: &str) -> Option<Self> {
        let definition: serde_json::Value = serde_json::from_str(plan.plan.as_json_text())
            .expect("the frozen plan holds valid JSON");
        let item = definition["tool_items"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|item| {
                item["provider"]
                    .as_str()
                    .is_some_and(|name| broca_protocol::same_module(name, provider))
            });
        let fetch = plan
            .fetches
            .iter()
            .find(|fetch| broca_protocol::same_module(&fetch.provider, provider));
        let mut transport = Self {
            preset: item
                .and_then(|item| item["preset"].as_str())
                .map(str::to_owned),
            ..Self::default()
        };
        if let Some(FetchOutcome::Answered {
            declarations_frozen,
            ..
        }) = fetch.map(|fetch| &fetch.outcome)
        {
            transport.fetched = true;
            transport.declarations = declarations_frozen.as_deref().cloned().unwrap_or_default();
            // Schema pins belong to individual tools, not to the route they share.
            transport.declarations.schema_pins = None;
        }
        (item.is_some()
            || fetch.is_some()
            || plan.tools.iter().any(|tool| {
                tool.module
                    .as_deref()
                    .is_some_and(|module| broca_protocol::same_module(module, provider))
            }))
        .then_some(transport)
    }
}

impl AddedToolDispatch {
    pub fn from_plan(tool: &Tool, plan: &PlanManifest) -> Self {
        let module = tool.module.as_deref();
        let answered = module.and_then(|module| {
            plan.fetches
                .iter()
                .find(|fetch| broca_protocol::same_module(&fetch.provider, module))
        });
        let (provider_reply, schema_pin) = match answered.map(|fetch| &fetch.outcome) {
            Some(FetchOutcome::Answered {
                reply,
                declarations_frozen,
                ..
            }) => (
                reply
                    .as_ref()
                    .and_then(|replies| replies.get(&tool.name))
                    .cloned(),
                declarations_frozen
                    .as_deref()
                    .and_then(|facts| facts.schema_pins.as_ref())
                    .and_then(|pins| pins.get(&tool.name))
                    .cloned(),
            ),
            _ => (None, None),
        };
        Self {
            module: tool.module.clone(),
            sequential: tool.execution_mode == ExecutionMode::Sequential,
            reply: tool.reply,
            provider: module.and_then(|module| ProviderTransport::of(plan, module)),
            provider_reply,
            schema_pin,
        }
    }
}

impl ToolManifest {
    /// Admit a native declaration without changing the request's prefix tool list. The writer
    /// may add a tool at an existing provider only if that provider's pending and applied
    /// transport settings match. Preserve the settings already in use by the applied tools.
    pub fn add_inline_tool(&mut self, definition: ToolDefinition, dispatch: AddedToolDispatch) {
        if self.name_to_index.contains_key(&definition.name) {
            return;
        }
        if let (Some(module), Some(transport)) = (&dispatch.module, dispatch.provider) {
            if let Some(preset) = transport.preset {
                if by_module(&self.provider_presets, module).is_none() {
                    self.provider_presets.insert(module.clone(), preset);
                }
            }
            if transport.fetched {
                self.fetched_providers.insert(module.clone());
            }
            let key = self
                .provider_declarations
                .get_key_value(module)
                .or_else(|| {
                    broca_protocol::other_module_name(module)
                        .and_then(|other| self.provider_declarations.get_key_value(other))
                })
                .map_or_else(|| module.clone(), |(key, _)| key.clone());
            let facts = self
                .provider_declarations
                .entry(key)
                .or_insert(transport.declarations);
            if let Some(pin) = dispatch.schema_pin {
                facts
                    .schema_pins
                    .get_or_insert_with(Default::default)
                    .insert(definition.name.clone(), pin);
            }
            if let Some(reply) = dispatch.provider_reply {
                self.provider_replies
                    .entry(module.clone())
                    .or_default()
                    .insert(definition.name.clone(), reply_from_frozen(&reply));
            }
        }
        let tools = std::sync::Arc::make_mut(&mut self.tools);
        self.name_to_index
            .insert(definition.name.clone(), tools.len());
        tools.push(Tool {
            name: definition.name,
            description: definition.description,
            input_schema: definition.input_schema,
            module: dispatch.module,
            execution_mode: if dispatch.sequential {
                ExecutionMode::Sequential
            } else {
                ExecutionMode::Parallel
            },
            reply: dispatch.reply,
        });
    }
}
