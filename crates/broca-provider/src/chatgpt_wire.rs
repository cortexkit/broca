//! Durable opt-in to the released Codex wire policy. Missing profiles remain legacy.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChatgptWireProfile {
    #[serde(rename = "codex-0.163.0")]
    Codex0163,
}

/// Identity captured at admission, never rediscovered while rendering or resuming.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatgptWire {
    pub profile: ChatgptWireProfile,
    pub user_agent: String,
    pub originator: String,
    pub session_id: String,
    pub thread_id: String,
    pub installation_id: String,
    pub window_id: String,
    pub turn_id: String,
    pub turn_started_at_unix_ms: u64,
}

impl ChatgptWire {
    pub const VERSION: &str = "0.163.0";
    pub const ORIGINATOR: &str = "codex_cli_rs";

    /// Separate installation and conversation namespaces; the conversation source includes
    /// the full bind identity so equal short session names cannot share a thread.
    pub fn scoped_id(domain: &str, source: &str) -> String {
        Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("broca:{domain}:{source}").as_bytes(),
        )
        .to_string()
    }
}
