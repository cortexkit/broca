//! Tool-set assembly + the approval seam. Capability ≠ approval: the per-run
//! manifest = `requested ∩ harness-policy`, resolved ONCE and immutable for the
//! run (and persisted in the WAL, so a resume can't silently change the admitted
//! tools — avoids a time-of-check/time-of-use gap). Approval may NARROW a call,
//! never widen it.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use broca_protocol::{ApprovalResponse, Tool, ToolCall, ToolCallId};

/// Default cap on a single tool call's serialized input (oversized-payload guard).
pub const DEFAULT_MAX_INPUT_BYTES: usize = 256 * 1024;

/// Why a tool call was refused at the capability gate (before any approval).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapabilityViolation {
    /// The model called a tool not in the run's manifest.
    NotInManifest { tool_name: String },
    /// The serialized input exceeded the payload cap.
    OversizedInput { bytes: usize, max: usize },
}

impl std::fmt::Display for CapabilityViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CapabilityViolation::NotInManifest { tool_name } => {
                write!(
                    f,
                    "tool '{tool_name}' is not in this run's capability manifest"
                )
            }
            CapabilityViolation::OversizedInput { bytes, max } => {
                write!(f, "tool input {bytes} bytes exceeds the {max}-byte cap")
            }
        }
    }
}

/// The immutable per-run tool manifest.
#[derive(Debug, Clone)]
pub struct ToolManifest {
    tools: Arc<Vec<Tool>>,
    name_to_index: HashMap<String, usize>,
    max_input_bytes: usize,
    provider_presets: HashMap<String, String>,
    provider_replies:
        HashMap<String, HashMap<String, cortexkit_role_tool_provider::catalog::Reply>>,
}

impl ToolManifest {
    /// Resolve the manifest ONCE: `requested ∩ harness-policy` (the harness
    /// policy is the set of tool names the harness permits for this run).
    pub fn resolve(requested: Vec<Tool>, policy_allowed: &HashSet<String>) -> Self {
        let tools = requested
            .into_iter()
            .filter(|t| policy_allowed.contains(&t.name))
            .collect();
        Self::from_tools(tools)
    }

    /// Build a manifest from an already-resolved owned tool set, such as the authoritative
    /// `run_config.tools` on resume. The tools need no additional policy filtering.
    pub fn from_tools(tools: Vec<Tool>) -> Self {
        Self::from_shared_tools(Arc::new(tools))
    }

    /// Index a shared ordered tool set without cloning its declarations.
    pub(crate) fn from_shared_tools(tools: Arc<Vec<Tool>>) -> Self {
        // When names repeat, get() and execution-mode dispatch use the final declaration.
        let name_to_index = tools
            .iter()
            .enumerate()
            .map(|(index, tool)| (tool.name.clone(), index))
            .collect();
        Self {
            tools,
            name_to_index,
            max_input_bytes: DEFAULT_MAX_INPUT_BYTES,
            provider_presets: HashMap::new(),
            provider_replies: HashMap::new(),
        }
    }

    /// Use only the plan frozen in the WAL manifest, not a later send or configuration.
    pub fn with_plan_manifest(
        mut self,
        manifest: &broca_wal::PlanManifest,
    ) -> Result<Self, serde_json::Error> {
        let plan: serde_json::Value = serde_json::from_str(manifest.plan.as_json_text())?;
        if let Some(items) = plan["tool_items"].as_array() {
            for item in items {
                if let (Some(provider), Some(preset)) =
                    (item["provider"].as_str(), item["preset"].as_str())
                {
                    self.provider_presets
                        .insert(provider.to_owned(), preset.to_owned());
                }
            }
        }
        for fetch in &manifest.fetches {
            if let broca_wal::FetchOutcome::Answered {
                reply: Some(replies),
                ..
            } = &fetch.outcome
            {
                for (name, reply) in replies {
                    if self.get(name).and_then(|tool| tool.module.as_deref())
                        == Some(fetch.provider.as_str())
                    {
                        self.provider_replies
                            .entry(fetch.provider.clone())
                            .or_default()
                            .insert(name.clone(), reply_from_frozen(reply));
                    }
                }
            }
        }
        Ok(self)
    }

    /// The frozen tool item's preset for the provider a call actually routes to.
    pub fn provider_preset(&self, provider: &str) -> Option<&str> {
        self.provider_presets.get(provider).map(String::as_str)
    }

    /// The selected tool owner's reply declaration, read only from the frozen plan.
    pub fn provider_reply(
        &self,
        provider: &str,
        tool: &str,
    ) -> Option<&cortexkit_role_tool_provider::catalog::Reply> {
        self.provider_replies.get(provider)?.get(tool)
    }

    pub fn with_max_input_bytes(mut self, max: usize) -> Self {
        self.max_input_bytes = max;
        self
    }

    /// Return one declaration per name in the name-index HashMap's iteration order.
    pub fn tools(&self) -> Vec<Tool> {
        self.name_to_index
            .values()
            .map(|index| self.tools[*index].clone())
            .collect()
    }

    pub fn get(&self, name: &str) -> Option<&Tool> {
        self.name_to_index
            .get(name)
            .map(|index| &self.tools[*index])
    }

    /// Validate a call against the manifest: membership + payload cap. (Full
    /// JSON-schema validation of args against `input_schema` is a later hardening
    /// follow-up; membership + payload cap cover the loop contract.)
    pub fn validate_call(&self, call: &ToolCall) -> Result<(), CapabilityViolation> {
        if !self.name_to_index.contains_key(&call.tool_name) {
            return Err(CapabilityViolation::NotInManifest {
                tool_name: call.tool_name.clone(),
            });
        }
        let bytes = serde_json::to_vec(&call.input)
            .map(|v| v.len())
            .unwrap_or(0);
        if bytes > self.max_input_bytes {
            return Err(CapabilityViolation::OversizedInput {
                bytes,
                max: self.max_input_bytes,
            });
        }
        Ok(())
    }
}

/// Runtime deadline arithmetic uses the role's types; durable bytes use the WAL's types.
fn reply_from_frozen(
    saved: &broca_wal::FrozenReply,
) -> cortexkit_role_tool_provider::catalog::Reply {
    let reply = cortexkit_role_tool_provider::catalog::Reply::new(saved.max_ms);
    match &saved.hold_argument {
        Some(hold) => {
            reply.with_hold_argument(cortexkit_role_tool_provider::catalog::HoldArgument::new(
                hold.name.clone(),
                hold.default_ms,
                hold.grace_ms,
            ))
        }
        None => reply,
    }
}

/// The approval gate: a per-tool-call decision made before dispatch.
#[async_trait]
pub trait ApprovalPolicy: Send + Sync {
    async fn decide(&self, call: &ToolCall) -> ApprovalResponse;
}

/// Interactive default: allow every call. A real harness replaces this with a
/// control-lane round-trip that asks the user.
pub struct AllowAll;

#[async_trait]
impl ApprovalPolicy for AllowAll {
    async fn decide(&self, call: &ToolCall) -> ApprovalResponse {
        ApprovalResponse::Allow {
            tool_call_id: call.tool_call_id.clone(),
        }
    }
}

/// Headless / no-human-present default — DENY every call (the safe choice when no
/// one is available to approve).
pub struct DenyAll {
    pub reason: String,
}

impl Default for DenyAll {
    fn default() -> Self {
        Self {
            reason: "denied: no human present (headless default-deny)".into(),
        }
    }
}

#[async_trait]
impl ApprovalPolicy for DenyAll {
    async fn decide(&self, call: &ToolCall) -> ApprovalResponse {
        ApprovalResponse::Deny {
            tool_call_id: call.tool_call_id.clone(),
            reason: self.reason.clone(),
        }
    }
}

/// A scripted policy for tests: per-tool_call_id decisions, defaulting to allow.
pub struct ScriptedApproval {
    decisions: HashMap<ToolCallId, ApprovalResponse>,
}

impl ScriptedApproval {
    pub fn new(decisions: HashMap<ToolCallId, ApprovalResponse>) -> Self {
        Self { decisions }
    }
}

#[async_trait]
impl ApprovalPolicy for ScriptedApproval {
    async fn decide(&self, call: &ToolCall) -> ApprovalResponse {
        self.decisions
            .get(&call.tool_call_id)
            .cloned()
            .unwrap_or(ApprovalResponse::Allow {
                tool_call_id: call.tool_call_id.clone(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn frozen_reply_is_scoped_to_selected_owner_and_tool() {
        let selected = json!({"name":"read", "description":"", "input_schema":{}, "module":"selected", "execution_mode":"parallel"});
        let frozen: broca_wal::PlanManifest = serde_json::from_value(json!({
            "plan":"{}", "plan_identity":"", "composition_digest":"", "fetches":[
                {"provider":"selected", "catalog_digest":"", "tools":[selected], "reply":{"read":{"max_ms":180000}, "unselected":{"max_ms":900000}}},
                {"provider":"other", "catalog_digest":"", "tools":[], "reply":{"read":{"max_ms":900000}}}
            ], "tools":[selected], "system_text":"", "system_digest":"", "join_version":1, "role_versions":{}
        })).unwrap();
        let legacy = ToolManifest::from_tools(frozen.tools.as_ref().clone());
        assert!(legacy.provider_reply("selected", "read").is_none());
        let loaded = legacy.with_plan_manifest(&frozen).unwrap();
        assert_eq!(
            loaded.provider_reply("selected", "read").unwrap().max_ms,
            180000
        );
        assert!(loaded.provider_reply("other", "read").is_none());
        assert!(loaded.provider_reply("selected", "unselected").is_none());
    }
}
