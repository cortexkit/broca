//! Small, replayable head facts, committed with the projection's exact WAL watermark.

use broca_protocol::RunId;
use broca_records::record::WalRecord;
use cortexkit_role_llm_runner::{read::HeadModel, usage::SessionUsage};
use serde::{Deserialize, Serialize};

/// A log uses mode-3 semantics exactly when its first RunStarted has a plan version.
/// Later runs cannot change that choice. HeadProjection caches this predicate, with None
/// meaning unknown rather than legacy.
pub fn is_mode3_log<'a>(records: impl IntoIterator<Item = &'a WalRecord>) -> bool {
    records
        .into_iter()
        .find_map(|record| match record {
            WalRecord::RunStarted {
                compaction_plan_version,
                ..
            } => Some(compaction_plan_version.is_some()),
            _ => None,
        })
        .unwrap_or(false)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HeadProjection {
    /// Set once from the first RunStarted. None is unknown, never evidence of a legacy log;
    /// callers must obtain the shared is_mode3_log predicate's answer before choosing semantics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode3: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode3_times: Option<crate::message_times::MessageTimeSeed>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creation_times: Option<crate::creation_times::CreationTimeSeed>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_rich_ui: Option<broca_protocol::RichUiStamp>,
    pub model: Option<HeadModel>,
    /// The next-run choice; the episode model above remains the source of usage attribution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection: Option<cortexkit_role_llm_runner::head_selection::PlanModel>,
    pub usage: Option<SessionUsage>,
    run_id: Option<RunId>,
    ownership: crate::SessionOwnershipV1,
    senders: Vec<cortexkit_role_llm_runner::delivery_order::PlanSender>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    queued_times: std::collections::BTreeMap<String, u64>,
    /// Mode-3 placement and provenance continuation; native message bytes stay in rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction_metadata: Option<crate::compaction_v2::metadata::Fold>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction_newest: Option<crate::compaction_v2::protocol::read::Cursor>,
}

impl Default for HeadProjection {
    fn default() -> Self {
        Self {
            mode3: None,
            mode3_times: None,
            creation_times: None,
            last_run_rich_ui: None,
            model: None,
            selection: None,
            usage: None,
            run_id: None,
            ownership: crate::SessionOwnershipV1::default().for_owner_access(),
            senders: vec![],
            queued_times: Default::default(),
            compaction_metadata: None,
            compaction_newest: None,
        }
    }
}

impl HeadProjection {}

/// The plan's senders as `session.head` reads them: each sender's grants without its templates,
/// which only admission reads.
pub fn head_senders(
    manifest: &broca_records::record::PlanManifest,
) -> Vec<cortexkit_role_llm_runner::delivery_order::PlanSender> {
    let mut senders = crate::admission::manifest_senders(manifest);
    for sender in &mut senders {
        sender.templates.clear();
    }
    senders
}

/// What `session.head` tells a caller it may send to the session: every delivery mode for the
/// session's owner, and for a reserved module the plan names as a sender, the modes and the
/// interrupt that plan grants it. `None` before the session's first run, and for any other
/// caller. `ownership` is the session's owner summary and `senders` its plan's senders
/// ([`head_senders`]), whichever storage keeps the session.
pub fn head_sender(
    ownership: &crate::SessionOwnershipV1,
    senders: &[cortexkit_role_llm_runner::delivery_order::PlanSender],
    principal: Option<&broca_protocol::BindPrincipal>,
    scope: &broca_protocol::SendScope,
    modes: &[cortexkit_role_llm_runner::delivery_order::Mode],
) -> Option<cortexkit_role_llm_runner::read::HeadSender> {
    use cortexkit_role_llm_runner::{delivery_order, read::HeadSender};
    if ownership.episodes == 0 {
        return None;
    }
    if crate::scope_admission::decide_owner_access(ownership, principal, scope).is_ok() {
        return Some(HeadSender::for_owner(
            modes.iter().map(|mode| mode.as_str().into()).collect(),
        ));
    }
    let broca_protocol::BindPrincipal::Reserved { module_id } = principal? else {
        return None;
    };
    let owner = ownership
        .scope
        .as_ref()
        .map(|scope| &scope.owner)
        .or(ownership.first_principal.as_ref())?
        .canonical_owner_string();
    // The contract compares ids exactly; a renamed module's two ids compare as one.
    let module_id = broca_protocol::canonical_module(module_id);
    let senders = &crate::delivery_order::canonical_senders(senders);
    let granted = modes
        .iter()
        .filter(|mode| delivery_order::admit(module_id, Some(&owner), senders, &[**mode]).is_ok())
        .map(|mode| mode.as_str().to_owned())
        .collect::<Vec<_>>();
    senders.iter().find(|sender| sender.module == module_id)?;
    let interrupt = delivery_order::admit_delivery(
        module_id,
        Some(&owner),
        senders,
        cortexkit_role_llm_runner::send::Delivery::Interrupt,
    )
    .is_ok();
    Some(HeadSender::new(granted, interrupt))
}

/// Only admission interprets untrusted plan choices. Replay reads an already admitted manifest.
pub fn manifest_selection(
    manifest: &broca_records::record::PlanManifest,
) -> Option<cortexkit_role_llm_runner::head_selection::PlanModel> {
    let plan: serde_json::Value = serde_json::from_str(manifest.plan.as_json_text()).ok()?;
    serde_json::from_value(plan.get("model")?.clone()).ok()
}
