//! Status exposes only calls the operational fold still waits for, in held-record order.

use broca_protocol::content::ContentBlock;
use broca_session_store::fold::operations::{CallState, Run};
use broca_session_store::record::Record;
use cortexkit_role_llm_runner::run::HeldCall;

pub(super) fn calls(run: Option<&Run>, records: &[(u64, Record)]) -> Vec<HeldCall> {
    let Some(batch) = run.and_then(|run| run.open_batch.as_ref()) else {
        return Vec::new();
    };
    // The fold retains the held metadata but not tool display names. Existing assistant and
    // intent records supply those names without adding another stored copy of them.
    let mut names = std::collections::HashMap::new();
    let mut order = Vec::new();
    for (_, record) in records {
        match record {
            Record::Assistant(assistant) => {
                names.clear();
                for block in &assistant.message.content {
                    if let ContentBlock::ToolCall(call) = block {
                        names.insert(call.tool_call_id.clone(), call.tool_name.clone());
                    }
                }
            }
            Record::ToolIntent(intent) if intent.batch_id == batch.batch_id => {
                if let Some(name) = &intent.tool_name {
                    names
                        .entry(intent.tool_call_id.clone())
                        .or_insert_with(|| name.clone());
                }
            }
            Record::ToolHeld(held)
                if held.batch_id == batch.batch_id && !order.contains(&held.tool_call_id) =>
            {
                order.push(held.tool_call_id.clone());
            }
            _ => {}
        }
    }
    order
        .into_iter()
        .filter_map(|id| {
            let position = (0..batch.calls.len()).find(|position| batch.id(*position) == &id)?;
            if batch.state(position) != CallState::ProviderHeld {
                return None;
            }
            let held = batch.held_reply(position)?;
            Some(HeldCall::new(
                held.call_key.clone(),
                names.remove(&id).unwrap_or_default(),
                held.module.clone(),
                held.reason.clone(),
                held.held_at_ms,
            ))
        })
        .collect()
}
