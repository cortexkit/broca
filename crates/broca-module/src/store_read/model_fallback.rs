//! Display-only reply facts from the runner's durable replacement records.

use std::collections::BTreeSet;

use broca_protocol::Role;
use broca_session_store::backend::SessionBackend;
use broca_session_store::session::Session;
use cortexkit_role_llm_runner::display::ModelFallback;
use cortexkit_role_llm_runner::read::ReadMessage;

pub(super) fn attach<B: SessionBackend>(
    session: &Session<B>,
    response: &mut broca_wire::SessionReadResponse,
    withheld: &BTreeSet<u64>,
) {
    for entry in &mut response.messages {
        if entry.message.role != Role::Assistant || withheld.contains(&(entry.ordinal - 1)) {
            continue;
        }
        let Some(origin) = &entry.message.origin else {
            continue;
        };
        let Some(recorded) = session.reply_model_fallback(&entry.mid) else {
            continue;
        };
        // The row's model is the stored origin. Never label a reply with a different answer
        // than that origin, even when an older or incomplete record claims a routed step.
        if recorded.answered != format!("{}/{}", origin.provider_module_id, origin.model_id) {
            continue;
        }
        let Ok(fact) = ModelFallback::new(&recorded.requested, &recorded.answered) else {
            continue;
        };
        // The shared validator checks the known role without interpreting the opaque body.
        let checked = ReadMessage::new(entry.ordinal, &entry.mid, serde_json::Value::Null)
            .with_model_fallback(fact);
        if checked.check_model_fallback("assistant").is_ok() {
            entry.model_fallback = checked.model_fallback;
        }
    }
}
