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
//! - the account UUID is the vault's account id for the credential when it is a UUID;
//! - the `cc_version` suffix is pinned per session: computed once from the session's first
//!   durable user message and inherited by later episodes, so a compaction that rewrites that
//!   message does not change the billing text.

use std::io::{Read, Write};
use std::path::Path;

use broca_protocol::{BindIdentity, Message};
use broca_provider::families::anthropic_messages::cc_version_suffix_for_transcript;
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

/// The `cc_version` suffix a new episode inherits from the session's latest episode: the one
/// that episode froze, when it spoke the version new episodes speak (the suffix hashes the
/// version, so another version's suffix does not carry over). `None` when there is nothing to
/// inherit.
pub fn inherited_cc_version_suffix(
    last_episode_frozen_config: Option<&serde_json::Value>,
) -> Option<String> {
    let persona = last_episode_frozen_config?.get("claude_code_persona")?;
    if persona.get("version")?.as_str()? != ClaudeCodePersona::CURRENT_VERSION {
        return None;
    }
    persona
        .get("cc_version_suffix")?
        .as_str()
        .map(str::to_string)
}

/// The `cc_version` suffix pinned for a session that has none to inherit: from the session's
/// first durable user message (`durable`, the session's recorded transcript, never a
/// compaction view), or from the new episode's own input when the session has no user message
/// yet.
pub fn first_durable_cc_version_suffix(
    durable: &[Message],
    admitted_input: &[Message],
) -> Option<String> {
    cc_version_suffix_for_transcript(durable, ClaudeCodePersona::CURRENT_VERSION).or_else(|| {
        cc_version_suffix_for_transcript(admitted_input, ClaudeCodePersona::CURRENT_VERSION)
    })
}

/// The persona a fresh episode freezes. `account_id` is the vault's account id for the
/// credential, `credential_id` stands in for it when the vault has none, `session` is the
/// episode's bind triple, and `cc_version_suffix` is the session's pinned suffix (`None` leaves
/// the renderer to compute it from the request).
pub fn fresh_persona(
    install_id: &str,
    account_id: Option<&str>,
    credential_id: &str,
    session: &BindIdentity,
    cc_version_suffix: Option<String>,
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
        cc_version_suffix,
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
            None,
        )
        .unwrap();
        let two = fresh_persona(
            "ab".repeat(32).as_str(),
            Some(account),
            "oauth:anthropic",
            &session,
            None,
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
            None,
        )
        .unwrap();
        assert_ne!(other.session_id, one.session_id);
        assert_eq!(other.device_id, one.device_id);

        let other_account = fresh_persona(
            "ab".repeat(32).as_str(),
            Some("acct-not-a-uuid"),
            "oauth:anthropic",
            &session,
            None,
        )
        .unwrap();
        assert_ne!(other_account.device_id, one.device_id);
        assert_eq!(other_account.account_uuid, None);
    }

    fn text(role: broca_protocol::Role, text: &str) -> Message {
        Message::new(
            role,
            vec![broca_protocol::ContentBlock::Text { text: text.into() }],
        )
    }

    /// The `cc_version=<version>.<suffix>` field of the billing block `frozen` renders for
    /// `prompt`. The rest of the block carries the episode's own prompt id, so it is left out.
    fn cc_version_field(
        frozen: &broca_provider::wire::FrozenRenderConfig,
        prompt: Vec<Message>,
    ) -> String {
        use broca_provider::{families::AnthropicMessages, wire::WireFamily};
        let shaped = AnthropicMessages.shape(frozen, &broca_protocol::CallOptions::new(prompt));
        let billing = shaped["system"][0]["text"].as_str().unwrap().to_string();
        billing
            .split(' ')
            .find(|field| field.starts_with("cc_version="))
            .expect("billing block carries cc_version")
            .to_string()
    }

    /// A session's first episode pins the suffix from its first user message. A compaction
    /// then replaces that message, and the next episode, admitted with the suffix it inherits
    /// from the first episode's recorded config, renders the same `cc_version` over the
    /// compacted history. Recomputed from that history, it would differ.
    #[test]
    fn a_compaction_that_replaces_the_first_user_message_keeps_the_next_episodes_cc_version() {
        use broca_protocol::Role;
        let session = BindIdentity::new("/tmp/project", "harness", "session-a");
        let install = "ab".repeat(32);
        let first = text(Role::User, "Summarise the README in two sentences.");

        let pinned = first_durable_cc_version_suffix(&[], std::slice::from_ref(&first));
        let mut episode_one = broca_provider::families::anthropic_frozen_config("claude-opus-4-8");
        episode_one.claude_code_persona = Some(
            fresh_persona(&install, None, "oauth:anthropic", &session, pinned.clone()).unwrap(),
        );
        let before = cc_version_field(
            &episode_one,
            vec![first.clone(), text(Role::Assistant, "It is a demo.")],
        );

        let recorded = serde_json::to_value(&episode_one).unwrap();
        let inherited = inherited_cc_version_suffix(Some(&recorded));
        assert_eq!(inherited, pinned);
        let mut episode_two = broca_provider::families::anthropic_frozen_config("claude-opus-4-8");
        episode_two.claude_code_persona =
            Some(fresh_persona(&install, None, "oauth:anthropic", &session, inherited).unwrap());
        let compacted = vec![
            text(
                Role::User,
                "Summary of the conversation so far: the README describes a demo.",
            ),
            text(Role::Assistant, "Understood."),
            text(Role::User, "Now list the files to change."),
        ];
        assert_eq!(cc_version_field(&episode_two, compacted.clone()), before);

        let mut recomputed = episode_two.clone();
        recomputed
            .claude_code_persona
            .as_mut()
            .unwrap()
            .cc_version_suffix = None;
        assert_ne!(
            cc_version_field(&recomputed, compacted),
            before,
            "the compacted history's own first user message gives another suffix"
        );
    }

    #[test]
    fn the_suffix_comes_from_the_durable_transcript_and_inherits_only_within_a_version() {
        use broca_protocol::Role;
        let durable = vec![
            text(Role::System, "Be brief."),
            text(Role::User, "Summarise the README in two sentences."),
        ];
        let input = vec![text(Role::User, "Now list the files to change.")];
        assert_eq!(
            first_durable_cc_version_suffix(&durable, &input),
            first_durable_cc_version_suffix(&[], &durable),
        );
        assert_ne!(
            first_durable_cc_version_suffix(&durable, &input),
            first_durable_cc_version_suffix(&[], &input),
        );
        assert_eq!(first_durable_cc_version_suffix(&[], &[]), None);

        let recorded = |version: &str, suffix: Option<&str>| {
            let mut persona = serde_json::json!({
                "version": version, "device_id": "0", "session_id": "s"
            });
            if let Some(suffix) = suffix {
                persona["cc_version_suffix"] = suffix.into();
            }
            serde_json::json!({ "claude_code_persona": persona })
        };
        let current = ClaudeCodePersona::CURRENT_VERSION;
        assert_eq!(
            inherited_cc_version_suffix(Some(&recorded(current, Some("abc")))),
            Some("abc".to_string())
        );
        assert_eq!(
            inherited_cc_version_suffix(Some(&recorded(current, None))),
            None
        );
        assert_eq!(
            inherited_cc_version_suffix(Some(&recorded("0.0.1", Some("abc")))),
            None
        );
        assert_eq!(
            inherited_cc_version_suffix(Some(&serde_json::json!({}))),
            None
        );
        assert_eq!(inherited_cc_version_suffix(None), None);
    }
}
