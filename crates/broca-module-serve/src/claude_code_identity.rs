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
///
/// The id is written to a temporary file in the same directory, synced, then linked into place
/// with a no-clobber hard link, so the file at [`INSTALL_ID_FILE`] is never seen empty or half
/// written, and two processes creating it at once both end up with the one that was linked
/// first. A file that is malformed or cannot be read is logged at ERROR, moved aside as
/// `claude-code-install-id.bad-<unix ms>` and replaced by a fresh id: the persona's device id
/// then changes once, rather than the persona switching off for good.
pub fn load_or_create_install_id(state_root: &Path) -> std::io::Result<String> {
    let path = state_root.join(INSTALL_ID_FILE);
    // Each round either returns an id or removes the obstacle that prevented one; three rounds
    // cover a bad file being replaced by a concurrent creator in between.
    let mut last_error = None;
    for _ in 0..3 {
        match read_install_id(&path) {
            Ok(Some(id)) => return Ok(id),
            Ok(None) => {}
            Err(error) => {
                tracing::error!(
                    %error,
                    "Claude Code install id file is unreadable or malformed; moving it aside and creating a new id"
                );
                set_aside(&path)?;
                last_error = Some(error);
                continue;
            }
        }
        let mut bytes = [0u8; 32];
        getrandom::fill(&mut bytes).map_err(std::io::Error::other)?;
        let candidate: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        match publish_install_id(&path, &candidate) {
            Ok(id) => return Ok(id),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        std::io::Error::other("the Claude Code install id could not be created")
    }))
}

/// The id stored at `path`: `Ok(None)` when there is no file, an error when it cannot be read
/// or does not hold 64 hex digits.
fn read_install_id(path: &Path) -> std::io::Result<Option<String>> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    }
    broca_owner_only::validate(path)?;
    let mut text = String::new();
    std::fs::File::open(path)?.read_to_string(&mut text)?;
    let id = text.trim();
    if id.len() == 64 && id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(Some(id.to_ascii_lowercase()))
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{} does not hold a 64-hex install id", path.display()),
        ))
    }
}

/// Write `candidate` to a synced temporary file and link it to `path` without replacing an
/// existing file. Returns the id at `path` afterwards: `candidate`, or the id another process
/// linked first.
fn publish_install_id(path: &Path, candidate: &str) -> std::io::Result<String> {
    let mut nonce = [0u8; 8];
    getrandom::fill(&mut nonce).map_err(std::io::Error::other)?;
    let temporary = path.with_file_name(format!(
        "{INSTALL_ID_FILE}.tmp-{}-{}",
        std::process::id(),
        nonce
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    ));
    let written = (|| {
        let mut options = broca_owner_only::OpenOptions::new();
        options.write(true).create_new(true);
        let mut file = options.open(&temporary)?;
        file.write_all(candidate.as_bytes())?;
        file.sync_all()
    })();
    let linked = written.and_then(|()| std::fs::hard_link(&temporary, path));
    let _ = std::fs::remove_file(&temporary);
    match linked {
        Ok(()) => {
            sync_parent(path);
            Ok(candidate.to_string())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => read_install_id(path)?
            .ok_or_else(|| std::io::Error::other("the linked install id vanished")),
        Err(error) => Err(error),
    }
}

/// Move a bad install id file aside, keeping it for inspection.
fn set_aside(path: &Path) -> std::io::Result<()> {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis());
    match std::fs::rename(
        path,
        path.with_file_name(format!("{INSTALL_ID_FILE}.bad-{stamp}")),
    ) {
        Ok(()) => Ok(()),
        // Another process already moved or replaced it.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Make the new directory entry durable where the platform allows syncing a directory.
fn sync_parent(path: &Path) {
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        if let Ok(directory) = std::fs::File::open(parent) {
            let _ = directory.sync_all();
        }
    }
    #[cfg(not(unix))]
    let _ = path;
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

/// Count human prompts from durable episode inputs, including imported history. Steered
/// records belong to the already-running prompt and compaction messages are model views,
/// so neither opens another human prompt. Tool results are not human prompts either.
pub fn human_prompt_ordinal<'a>(
    records: impl IntoIterator<Item = &'a broca_wal::WalRecord>,
    admitted_input: &[Message],
) -> u64 {
    fn count(messages: &[Message]) -> u64 {
        messages
            .iter()
            .filter(|message| {
                message.role == broca_protocol::Role::User
                    && message
                        .content
                        .iter()
                        .any(|block| !matches!(block, broca_protocol::ContentBlock::ToolResult(_)))
            })
            .count() as u64
    }
    records
        .into_iter()
        .filter_map(|record| match record {
            broca_wal::WalRecord::RunStarted { input, .. } => Some(count(input)),
            _ => None,
        })
        .sum::<u64>()
        + count(admitted_input)
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
    prompt_ordinal: u64,
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
        prompt_ordinal: Some(prompt_ordinal),
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

    fn scratch(label: &str) -> std::path::PathBuf {
        let root = broca_test_support::scratch_root().join(format!(
            "claude-code-install-id-{label}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        broca_owner_only::create_dir_all(&root).unwrap();
        root
    }

    /// Two creators racing on a first boot converge: the second link finds the first one's file
    /// and adopts its id instead of replacing it.
    #[test]
    fn a_second_creator_adopts_the_id_linked_first() {
        let root = scratch("race");
        let path = root.join(INSTALL_ID_FILE);
        let first = "a".repeat(64);
        let second = "b".repeat(64);
        assert_eq!(publish_install_id(&path, &first).unwrap(), first);
        assert_eq!(publish_install_id(&path, &second).unwrap(), first);
        assert_eq!(load_or_create_install_id(&root).unwrap(), first);
        let leftovers: Vec<_> = std::fs::read_dir(&root)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name() != INSTALL_ID_FILE)
            .collect();
        assert!(leftovers.is_empty(), "no temporary file is left behind");

        let threads: Vec<_> = (0..8)
            .map(|_| {
                let root = scratch_concurrent_root(&root);
                std::thread::spawn(move || load_or_create_install_id(&root).unwrap())
            })
            .collect();
        let ids: std::collections::BTreeSet<String> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        assert_eq!(
            ids.len(),
            1,
            "concurrent first boots converge on one id: {ids:?}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    fn scratch_concurrent_root(parent: &Path) -> std::path::PathBuf {
        let root = parent.join("concurrent");
        let _ = broca_owner_only::create_dir_all(&root);
        root
    }

    /// A file a crash left empty, or one holding anything but an id, is moved aside and
    /// replaced by a fresh id instead of disabling the persona.
    #[test]
    fn a_malformed_install_id_is_set_aside_and_replaced() {
        for (label, contents) in [("empty", ""), ("garbage", "not an id")] {
            let root = scratch(label);
            let path = root.join(INSTALL_ID_FILE);
            let mut options = broca_owner_only::OpenOptions::new();
            options.write(true).create_new(true);
            options
                .open(&path)
                .unwrap()
                .write_all(contents.as_bytes())
                .unwrap();

            let id = load_or_create_install_id(&root).unwrap();
            assert_eq!(id.len(), 64);
            assert_eq!(read_install_id(&path).unwrap(), Some(id.clone()));
            let set_aside: Vec<String> = std::fs::read_dir(&root)
                .unwrap()
                .filter_map(Result::ok)
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .filter(|name| name.starts_with(&format!("{INSTALL_ID_FILE}.bad-")))
                .collect();
            assert_eq!(set_aside.len(), 1, "{label}: the bad file is kept aside");
            assert_eq!(
                std::fs::read_to_string(root.join(&set_aside[0])).unwrap(),
                contents
            );
            assert_eq!(
                load_or_create_install_id(&root).unwrap(),
                id,
                "{label}: stable"
            );
            let _ = std::fs::remove_dir_all(&root);
        }
    }

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
            1,
        )
        .unwrap();
        let two = fresh_persona(
            "ab".repeat(32).as_str(),
            Some(account),
            "oauth:anthropic",
            &session,
            None,
            2,
        )
        .unwrap();
        assert_eq!(one.device_id, two.device_id);
        assert_eq!(one.session_id, two.session_id);
        assert_ne!(one.prompt_id, two.prompt_id);
        assert_eq!(one.version, "2.1.302");
        assert_eq!(one.prompt_ordinal, Some(1));
        assert_eq!(two.prompt_ordinal, Some(2));
        assert_eq!(one.account_uuid.as_deref(), Some(account));

        let other_session = BindIdentity::new("/tmp/project", "harness", "session-b");
        let other = fresh_persona(
            "ab".repeat(32).as_str(),
            Some(account),
            "oauth:anthropic",
            &other_session,
            None,
            1,
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
            3,
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
            fresh_persona(
                &install,
                None,
                "oauth:anthropic",
                &session,
                pinned.clone(),
                1,
            )
            .unwrap(),
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
            Some(fresh_persona(&install, None, "oauth:anthropic", &session, inherited, 2).unwrap());
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
            inherited_cc_version_suffix(Some(&recorded("2.1.280", Some("abc")))),
            None
        );
        assert_eq!(
            inherited_cc_version_suffix(Some(&serde_json::json!({}))),
            None
        );
        assert_eq!(inherited_cc_version_suffix(None), None);
    }

    #[test]
    fn durable_prompt_ordinal_includes_imports_but_not_steers_or_tool_results() {
        use broca_protocol::Role;
        use serde_json::json;
        let config = json!({
            "model": {"provider_module_id": "anthropic", "model_id": "claude-sonnet-5-5"},
            "tools": [], "tool_choice": {"type": "auto"}, "generation": {},
            "response_format": {"type": "text"}, "provider_options": null, "cache_tiers": null,
            "context_epoch": 0, "renderer_version": 1, "frozen_render_config": null
        });
        let started = |input: Vec<Message>| -> broca_wal::WalRecord {
            serde_json::from_value(json!({"type": "run_started", "run_id": "r",
                "session": BindIdentity::new("/tmp/project", "test", "session"),
                "config": config, "input": input}))
            .unwrap()
        };
        let imported = started(vec![
            text(Role::User, "imported first"),
            text(Role::Assistant, "answer"),
            text(Role::User, "imported second"),
        ]);
        let later = started(vec![text(Role::User, "new episode")]);
        let steer: broca_wal::WalRecord = serde_json::from_value(
            json!({"type": "steered", "input": [text(Role::User, "steer")] }),
        )
        .unwrap();
        let tool_result = Message::new(
            Role::User,
            vec![broca_protocol::ContentBlock::ToolResult(
                broca_protocol::ToolResult {
                    tool_call_id: broca_protocol::ToolCallId("toolu_1".into()),
                    output: broca_protocol::ToolOutput::Text {
                        text: "done".into(),
                    },
                    is_error: false,
                },
            )],
        );
        let records = [imported, later, steer, started(vec![tool_result])];
        assert_eq!(
            human_prompt_ordinal(&records, &[text(Role::User, "current prompt")]),
            4
        );
        assert_eq!(human_prompt_ordinal(&records, &[]), 3);
    }
}
