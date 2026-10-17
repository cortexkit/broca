//! The identity values a Claude Code persona run carries, as broca derives them.
//!
//! Every value is frozen into the episode's render config at admission (see
//! `broca_provider::wire::ClaudeCodePersona`), so a resume sends exactly what the episode first
//! sent. How each value lives:
//! - the device id is stable per account and per broca installation, derived from an install
//!   id persisted once under the state root, so a restart does not look like a new device;
//! - the session id is derived from the broca session's reference, so every episode of one
//!   conversation sends the same id, before and after a restart;
//! - the prompt id is fresh per episode, shared by all of its steps;
//! - the account UUID is the vault's account id for the credential when it is a UUID.

use std::io::{Read, Write};
use std::path::Path;

use broca_protocol::BindIdentity;
use broca_provider::wire::{uuid_v4_text, ClaudeCodePersona};

/// The file under the state root that holds this installation's id.
pub const INSTALL_ID_FILE: &str = "claude-code-install-id";

/// Read this installation's id, creating it (32 random bytes as hex, owner-only) on first use.
pub fn load_or_create_install_id(state_root: &Path) -> std::io::Result<String> {
    let path = state_root.join(INSTALL_ID_FILE);
    let mut options = broca_owner_only::OpenOptions::new();
    options.write(true).create_new(true);
    match options.open(&path) {
        Ok(mut file) => {
            let mut bytes = [0u8; 32];
            getrandom::fill(&mut bytes).map_err(std::io::Error::other)?;
            let id: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
            file.write_all(id.as_bytes())?;
            file.sync_all()?;
            Ok(id)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            broca_owner_only::validate(&path)?;
            let mut text = String::new();
            std::fs::File::open(&path)?.read_to_string(&mut text)?;
            let id = text.trim();
            if id.len() == 64 && id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                Ok(id.to_ascii_lowercase())
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("{} does not hold a 64-hex install id", path.display()),
                ))
            }
        }
        Err(error) => Err(error),
    }
}

/// The persona a fresh episode freezes. `account_id` is the vault's account id for the
/// credential, `credential_id` stands in for it when the vault has none, and `session` is the
/// episode's bind triple.
pub fn fresh_persona(
    install_id: &str,
    account_id: Option<&str>,
    credential_id: &str,
    session: &BindIdentity,
) -> std::io::Result<ClaudeCodePersona> {
    let account = account_id
        .filter(|id| !id.is_empty())
        .unwrap_or(credential_id);
    let session_ref = broca_wire::agent_data::session_ref(
        &session.harness,
        &session.project_root.to_string_lossy(),
        &session.session,
    );
    let mut prompt = [0u8; 16];
    getrandom::fill(&mut prompt).map_err(std::io::Error::other)?;
    Ok(ClaudeCodePersona {
        version: ClaudeCodePersona::CURRENT_VERSION.to_string(),
        device_id: ClaudeCodePersona::derive_device_id(install_id, account),
        account_uuid: account_id.filter(|id| is_uuid(id)).map(str::to_string),
        session_id: ClaudeCodePersona::derive_session_id(&session_ref),
        prompt_id: Some(uuid_v4_text(prompt)),
    })
}

fn is_uuid(id: &str) -> bool {
    id.len() == 36
        && id.char_indices().all(|(index, ch)| match index {
            8 | 13 | 18 | 23 => ch == '-',
            _ => ch.is_ascii_hexdigit(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn install_id_is_created_once_and_read_back() {
        let root = broca_test_support::scratch_root()
            .join(format!("claude-code-install-id-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        broca_owner_only::create_dir_all(&root).unwrap();
        let first = load_or_create_install_id(&root).unwrap();
        assert_eq!(first.len(), 64);
        assert_eq!(load_or_create_install_id(&root).unwrap(), first);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_session_keeps_its_ids_and_each_episode_gets_a_new_prompt_id() {
        let session = BindIdentity::new("/tmp/project", "harness", "session-a");
        let account = "00000000-0000-4000-8000-000000000000";
        let one = fresh_persona(
            "ab".repeat(32).as_str(),
            Some(account),
            "oauth:anthropic",
            &session,
        )
        .unwrap();
        let two = fresh_persona(
            "ab".repeat(32).as_str(),
            Some(account),
            "oauth:anthropic",
            &session,
        )
        .unwrap();
        assert_eq!(one.device_id, two.device_id);
        assert_eq!(one.session_id, two.session_id);
        assert_ne!(one.prompt_id, two.prompt_id);
        assert_eq!(one.account_uuid.as_deref(), Some(account));

        let other_session = BindIdentity::new("/tmp/project", "harness", "session-b");
        let other = fresh_persona(
            "ab".repeat(32).as_str(),
            Some(account),
            "oauth:anthropic",
            &other_session,
        )
        .unwrap();
        assert_ne!(other.session_id, one.session_id);
        assert_eq!(other.device_id, one.device_id);

        let other_account = fresh_persona(
            "ab".repeat(32).as_str(),
            Some("acct-not-a-uuid"),
            "oauth:anthropic",
            &session,
        )
        .unwrap();
        assert_ne!(other_account.device_id, one.device_id);
        assert_eq!(other_account.account_uuid, None);
    }
}
