//! Write-time step transforms. Wire types and operation rules belong to commons; this
//! module owns runner cancellation, durability attribution, and the output byte ceiling.

use async_trait::async_trait;
use broca_protocol::BindIdentity;
use broca_wal::{HookOp, Hooks, PlanManifest};
pub use cortexkit_role_step_transform_provider::{
    answer::HookAnswer,
    hook::{HookCall, Subject},
};
use cortexkit_role_step_transform_provider::{
    answer::{apply_ops, check_answer, check_post_tool_grant, Operation},
    grant::{plan_user_grants, UserGrant},
    subscription::{
        check_reduction, hook_order, Declaration, Hook, OnUnavailable, Op, Phase, Subscription,
    },
};
use serde::Deserialize;
use serde_json::{Map, Value};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub use broca_protocol::STEP_TRANSFORM_HOOK_CAP;

/// Transport failure, timeout, or an undecodable answer. It is not a provider denial.
#[derive(Debug, Clone)]
pub struct HookUnavailable;

/// Session-scoped route adapter. Opening/reopening a route and any transient retry happen
/// inside this future, so the core's timeout covers the entire call, not just the RPC.
#[async_trait]
pub trait HookPlane: Send + Sync {
    async fn hook(
        &self,
        provider: &str,
        bind: &BindIdentity,
        call: HookCall,
    ) -> Result<HookAnswer, HookUnavailable>;
}

#[derive(Clone, Deserialize)]
pub(crate) struct HookItem {
    pub provider: String,
    #[serde(default)]
    pub preset: Option<String>,
    #[serde(default)]
    pub params: Map<String, Value>,
    #[serde(default)]
    pub subscriptions: Vec<Subscription>,
}

#[derive(Clone)]
pub struct HookPlan {
    reduction_owner: Option<String>,
    items: Vec<HookItem>,
    declarations: Vec<Declaration>,
    grants: Vec<UserGrant>,
    /// Result-operation permissions are retained in the frozen catalog JSON. The model's
    /// Tool schema intentionally contains only the renderer-facing catalog fields.
    result_ops: std::collections::HashMap<String, Vec<Op>>,
}

pub fn has_transforms(manifest: &PlanManifest) -> bool {
    serde_json::from_str::<Value>(manifest.plan.as_json_text())
        .ok()
        .and_then(|plan| {
            plan.get("step_transform_items")
                .and_then(Value::as_array)
                .map(|items| !items.is_empty())
        })
        .unwrap_or(false)
}

impl HookPlan {
    pub fn from_manifest(manifest: &PlanManifest) -> Result<Self, String> {
        let plan: Value =
            serde_json::from_str(manifest.plan.as_json_text()).map_err(|e| e.to_string())?;
        let object = plan.as_object().ok_or("frozen plan is not an object")?;
        let items: Vec<HookItem> = serde_json::from_value(
            object
                .get("step_transform_items")
                .cloned()
                .unwrap_or_else(|| Value::Array(Vec::new())),
        )
        .map_err(|e| e.to_string())?;
        if items.len() != manifest.declarations.len() {
            return Err("frozen transform declarations do not match plan items".into());
        }
        let mut declarations = Vec::new();
        for (item, frozen) in items.iter().zip(&manifest.declarations) {
            // Resume bypasses admission, so enforce the fail-closed approval policy here too.
            if item.subscriptions.iter().any(|sub| {
                sub.phase == Some(Phase::Approve) && sub.on_unavailable == OnUnavailable::Pass
            }) {
                return Err("approve_on_unavailable_pass".into());
            }
            if item.provider != frozen.provider {
                return Err("frozen transform declaration provider mismatch".into());
            }
            declarations.push(
                serde_json::from_str(frozen.answer.as_json_text()).map_err(|e| e.to_string())?,
            );
        }
        let grants = plan_user_grants(object).map_err(|e| e.to_string())?;
        let mut result_ops = std::collections::HashMap::new();
        let tool_owners: std::collections::HashMap<_, _> = manifest
            .tools
            .iter()
            .map(|tool| (tool.name.as_str(), tool.module.as_deref()))
            .collect();
        // Permissions come only from the catalog that owns the selected model tool.
        // An unused provider's same-named tool cannot widen another tool's permissions.
        for fetch in &manifest.fetches {
            if let broca_wal::FetchOutcome::Answered {
                result_ops: Some(permissions),
                ..
            } = &fetch.outcome
            {
                for (name, ops) in permissions {
                    if tool_owners.get(name.as_str()).copied().flatten()
                        != Some(fetch.provider.as_str())
                    {
                        continue;
                    }
                    result_ops.insert(
                        name.clone(),
                        // Catalog permissions are open strings. A future operation grants no
                        // permission to this runner, but must not disable the known operations.
                        ops.iter()
                            .filter_map(|op| match op.as_str() {
                                "prepend" => Some(Op::Prepend),
                                "append" => Some(Op::Append),
                                "replace" => Some(Op::Replace),
                                _ => None,
                            })
                            .collect(),
                    );
                }
            }
        }
        Ok(Self {
            reduction_owner: object
                .get("compaction_item")
                .and_then(|item| item.get("provider"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            items,
            declarations,
            grants,
            result_ops,
        })
    }
    fn matching<'a>(
        &'a self,
        subject: &Subject,
    ) -> Vec<(&'a HookItem, &'a Subscription, &'a Declaration)> {
        let providers: Vec<&str> = self
            .items
            .iter()
            .map(|item| item.provider.as_str())
            .collect();
        hook_order(&providers)
            .into_iter()
            .flat_map(|index| {
                let item = &self.items[index];
                item.subscriptions
                    .iter()
                    .filter(move |sub| {
                        sub.hook == subject.hook()
                            && sub.phase == subject.phase()
                            && sub.tools.as_ref().is_none_or(|tools| {
                                subject
                                    .tool()
                                    .is_some_and(|tool| tools.iter().any(|t| t == tool))
                            })
                    })
                    .map(move |sub| (item, sub, &self.declarations[index]))
            })
            .collect()
    }
    pub fn post_tool(&self, tool: &str) -> bool {
        !self
            .matching(&Subject::PostTool {
                step_id: String::new(),
                tool: tool.into(),
                tool_call_id: String::new(),
                call_key: None,
                blocks: Vec::new(),
                is_error: false,
            })
            .is_empty()
    }

    pub(super) fn second_validation(&self) -> Self {
        let mut plan = self.clone();
        for item in &mut plan.items {
            for sub in &mut item.subscriptions {
                if sub.phase == Some(Phase::Validate) {
                    sub.on_unavailable = OnUnavailable::Refuse;
                }
            }
        }
        plan
    }
    pub fn approves(&self) -> bool {
        self.items.iter().any(|item| {
            item.subscriptions
                .iter()
                .any(|sub| sub.phase == Some(Phase::Approve))
        })
    }
}

#[derive(Clone, Default)]
pub struct Trace {
    pub ops: Vec<HookOp>,
    pub unavailable: Vec<String>,
}
impl Trace {
    pub fn metadata(
        &self,
        original: Value,
        final_value: Value,
        intentless: bool,
    ) -> Option<Box<Hooks>> {
        let changed = original != final_value;
        if self.ops.is_empty() && self.unavailable.is_empty() {
            return None;
        }
        Some(Box::new(Hooks {
            original: changed.then_some(original),
            final_value: (changed && intentless).then_some(final_value),
            ops: self.ops.clone(),
            unavailable: (!self.unavailable.is_empty()).then(|| self.unavailable.clone()),
        }))
    }
}

/// Why a live hook chain was cancelled, distinct from provider unavailability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookCancellation {
    CallerCancel,
    StopRequested,
    ShutdownSeal,
}

enum HookCompletion {
    Answer(Option<HookAnswer>),
    Cancelled(HookCancellation),
}

#[derive(Clone)]
pub struct ChainOutput {
    pub subject: Subject,
    pub operations: Vec<Operation>,
    pub trace: Trace,
    pub refusal: Option<(String, String, bool)>,
    pub cancellation: Option<HookCancellation>,
    pub ask: Option<broca_wal::DeferredAsk>,
}
impl ChainOutput {
    fn fallback(&self, remaining: &[(&HookItem, &Subscription, &Declaration)]) -> Self {
        let mut output = self.clone();
        for (item, sub, _) in remaining {
            if sub.on_unavailable == OnUnavailable::Refuse {
                output.refusal = Some((item.provider.clone(), "unavailable".into(), true));
                break;
            }
            output.trace.unavailable.push(item.provider.clone());
        }
        output
    }
}

pub struct HookRuntime<'a> {
    pub plan: &'a HookPlan,
    pub plane: &'a Arc<dyn HookPlane>,
    pub bind: &'a BindIdentity,
    pub writer: &'a crate::WalHandle,
    pub cancel: Option<&'a CancellationToken>,
    pub stop: Option<&'a CancellationToken>,
    pub seal: Option<&'a crate::ShutdownSeal>,
}
async fn cancelled(token: Option<&CancellationToken>) {
    match token {
        Some(token) => token.cancelled().await,
        None => std::future::pending().await,
    }
}
impl HookRuntime<'_> {
    /// Run one ordered chain. `observe` receives a whole-chain fallback before the
    /// first call and after each accepted answer; false loses hand-over and parks the run.
    /// Cancelled input-hook outputs must be discarded by the send handler or batch gate.
    pub async fn chain(
        &self,
        subject: Subject,
        trace: Trace,
        mut observe: impl FnMut(&ChainOutput) -> bool,
    ) -> ChainOutput {
        let matching = self.plan.matching(&subject);
        let mut output = ChainOutput {
            subject,
            operations: Vec::new(),
            trace,
            refusal: None,
            cancellation: None,
            ask: None,
        };
        if !observe(&output.fallback(&matching)) {
            std::future::pending::<()>().await;
        }
        for (index, (item, sub, declaration)) in matching.iter().enumerate() {
            let mut call = HookCall::new(
                &self.bind.session,
                &self.bind.harness,
                output.subject.clone(),
            )
            .with_item(item.preset.clone(), item.params.clone());
            // A file header may already have minted a lineage, but the first pre-user
            // subject belongs to a session with no recorded message yet.
            if self.writer.next_record_seq() > 1 {
                call.lineage_id = self.writer.lineage_id().map(|id| id.to_string());
            }
            let answer = tokio::select! {
                biased;
                _ = cancelled(self.cancel) => HookCompletion::Cancelled(HookCancellation::CallerCancel),
                _ = cancelled(self.stop) => HookCompletion::Cancelled(HookCancellation::StopRequested),
                _ = async { match self.seal { Some(seal) => seal.tripped().await, None => std::future::pending().await } } => HookCompletion::Cancelled(HookCancellation::ShutdownSeal),
                answer = tokio::time::timeout(std::time::Duration::from_millis(sub.budget_ms.min(STEP_TRANSFORM_HOOK_CAP)),
                    self.plane.hook(&item.provider, self.bind, call)) => HookCompletion::Answer(answer.ok().and_then(Result::ok)),
            };
            let answer = match answer {
                HookCompletion::Answer(answer) => answer,
                HookCompletion::Cancelled(cause) => {
                    // The actor owns all writes after sealing. Even pre-tool, which has no
                    // slot, must not race its close-out with stopped terminals from the loop.
                    if self.seal.is_some_and(crate::ShutdownSeal::is_tripped)
                        && output.subject.hook() != Hook::PreUser
                    {
                        std::future::pending::<()>().await;
                    }
                    output.cancellation = Some(cause);
                    // Input-hook cancellation is not unavailability. Send handlers discard the
                    // pre-user output; the gate loop stops the entire pre-tool batch.
                    if matches!(output.subject.hook(), Hook::PreUser | Hook::PreTool) {
                        return output;
                    }
                    output = output.fallback(&matching[index..]);
                    if !observe(&output) {
                        std::future::pending::<()>().await;
                    }
                    return output;
                }
            };
            let answer = answer.filter(|answer| {
                declaration.bound(sub).is_ok()
                    && check_reduction(sub, &item.provider, self.plan.reduction_owner.as_deref())
                        .is_ok()
                    && check_answer(
                        &output.subject,
                        &sub.ops,
                        output
                            .subject
                            .tool()
                            .and_then(|tool| self.plan.result_ops.get(tool))
                            .map(Vec::as_slice)
                            .or(Some(&[Op::Prepend, Op::Append])),
                        answer,
                    )
                    .is_ok()
                    && check_post_tool_grant(
                        &output.subject,
                        &item.provider,
                        &self.plan.grants,
                        answer,
                    )
                    .is_ok()
                    && match (&output.subject, answer) {
                        (Subject::PostTool { blocks, .. }, HookAnswer::Ops { ops }) => {
                            apply_ops(blocks, ops).is_ok_and(|final_blocks| {
                                final_blocks.iter().map(String::len).sum::<usize>()
                                    <= crate::run::TOOL_RESULT_MAX_BYTES
                            })
                        }
                        _ => true,
                    }
            });
            match answer {
                None if sub.on_unavailable == OnUnavailable::Refuse
                    || output.subject.phase() == Some(Phase::Approve) =>
                {
                    output.refusal = Some((item.provider.clone(), "unavailable".into(), true));
                }
                None => output.trace.unavailable.push(item.provider.clone()),
                Some(HookAnswer::Pass) => {}
                Some(HookAnswer::Ops { ops }) => {
                    let blocks = match &mut output.subject {
                        Subject::PreUser { blocks, .. }
                        | Subject::PostAssistant { blocks, .. }
                        | Subject::PostTool { blocks, .. } => blocks,
                        _ => unreachable!("check_answer permits ops only on text subjects"),
                    };
                    *blocks = apply_ops(blocks, &ops).expect("checked block indices");
                    output
                        .trace
                        .ops
                        .extend(ops.iter().map(|op| attribution(&item.provider, sub, op)));
                    output.operations.extend(ops);
                }
                Some(HookAnswer::Mutate { input, note }) => {
                    if let Subject::PreTool { input: current, .. } = &mut output.subject {
                        *current = input;
                    }
                    output.trace.ops.push(HookOp {
                        module: item.provider.clone(),
                        hook: "pre_tool".into(),
                        phase: Some("mutate".into()),
                        op: "mutate".into(),
                        block: None,
                        note,
                    });
                }
                Some(HookAnswer::Deny { text }) => {
                    output.trace.ops.push(HookOp {
                        module: item.provider.clone(),
                        hook: "pre_tool".into(),
                        phase: Some("validate".into()),
                        op: "deny".into(),
                        block: None,
                        note: None,
                    });
                    output.refusal = Some((item.provider.clone(), text, false));
                }
                Some(HookAnswer::Ask { ask }) => {
                    output.ask = Some(
                        serde_json::from_value(
                            serde_json::to_value(ask).expect("ApprovalAsk serializes"),
                        )
                        .expect("WAL preserves ApprovalAsk"),
                    );
                    // Keep the first provider's approval question for this tool call. A later
                    // provider must not replace the consent terms the human will answer.
                    return output;
                }
            }
            if output.refusal.is_some() {
                if !observe(&output) {
                    std::future::pending::<()>().await;
                }
                return output;
            }
            if !observe(&output.fallback(&matching[index + 1..])) {
                std::future::pending::<()>().await;
            }
        }
        output
    }
}
fn attribution(provider: &str, sub: &Subscription, operation: &Operation) -> HookOp {
    let note = match operation {
        Operation::Prepend { note, .. }
        | Operation::Append { note, .. }
        | Operation::Replace { note, .. } => note.clone(),
    };
    HookOp {
        module: provider.into(),
        hook: match sub.hook {
            Hook::PreUser => "pre_user",
            Hook::PostAssistant => "post_assistant",
            Hook::PreTool => "pre_tool",
            Hook::PostTool => "post_tool",
        }
        .into(),
        phase: sub.phase.map(|phase| {
            match phase {
                Phase::Mutate => "mutate",
                Phase::Validate => "validate",
                Phase::Approve => "approve",
            }
            .into()
        }),
        op: match operation.op() {
            Op::Append => "append",
            Op::Prepend => "prepend",
            Op::Replace => "replace",
        }
        .into(),
        block: Some(operation.block()),
        note,
    }
}

/// Splice text-only answers in place. The step-transform answer validator preserves block count
/// and order, so tool-call inputs retain their exact original values. Editing provider-owned text
/// removes its opaque state: a signature must never be attached to rewritten text.
pub fn apply_message_ops(
    content: &mut [broca_protocol::ContentBlock],
    ops: &[Operation],
) -> Result<(), cortexkit_role_step_transform_provider::answer::AnswerProblem> {
    let before = content.to_vec();
    cortexkit_role_step_transform_provider::answer::apply_to_parts(
        content,
        |part| match part {
            broca_protocol::ContentBlock::Text { text }
            | broca_protocol::ContentBlock::TextWithState { text, .. } => Some(text),
            _ => None,
        },
        ops,
    )?;
    for (part, original) in content.iter_mut().zip(before) {
        if *part != original {
            if let broca_protocol::ContentBlock::TextWithState { text, .. } = part {
                *part = broca_protocol::ContentBlock::Text {
                    text: std::mem::take(text),
                };
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod permission_tests {
    use super::*;
    use serde_json::json;

    fn manifest(result_ops: Value) -> PlanManifest {
        serde_json::from_value(json!({
            "plan":json!({"step_transform_items":[]}).to_string(), "plan_identity":"", "composition_digest":"",
            "tools":[{"name":"edit", "description":"", "input_schema":{}, "module":"aft", "execution_mode":"sequential"}],
            "fetches":[{"provider":"aft", "catalog_digest":"", "tools":[], "result_ops":result_ops}],
            "system_text":"", "system_digest":"", "join_version":1, "role_versions":{}
        })).unwrap()
    }

    #[test]
    fn unknown_catalog_result_ops_grant_only_known_permissions() {
        let plan =
            HookPlan::from_manifest(&manifest(json!({"edit":["prepend", "rotate"]}))).unwrap();
        assert_eq!(plan.result_ops["edit"], vec![Op::Prepend]);
    }

    #[test]
    fn a_tool_without_catalog_permissions_has_no_grant() {
        let mut frozen = manifest(json!({"edit":["replace"]}));
        frozen.fetches.clear();
        let plan = HookPlan::from_manifest(&frozen).unwrap();
        assert!(plan.result_ops.is_empty());
    }
}
