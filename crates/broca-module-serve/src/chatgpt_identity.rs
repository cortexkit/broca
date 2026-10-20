//! Host discovery is admission-only; resumed episodes use recorded identity, even absence.

use broca_protocol::BindIdentity;
use broca_provider::chatgpt_wire::{ChatgptWire, ChatgptWireProfile};
use broca_provider::spec::WireFamilyId;
use broca_provider::wire::uuid_v4_text;
use std::sync::OnceLock;

/// Resolve the policy before constructing the provider. The lazy factory must never run on
/// resume: a different host or process must not replace the episode's original identity.
pub fn for_episode(
    family: Option<WireFamilyId>,
    recorded_config: Option<&serde_json::Value>,
    fresh: impl FnOnce() -> Result<ChatgptWire, String>,
) -> Result<Option<ChatgptWire>, String> {
    if let Some(config) = recorded_config {
        return config
            .get("chatgpt_wire")
            .filter(|value| !value.is_null())
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()
            .map_err(|error| format!("invalid recorded ChatGPT wire profile: {error}"));
    }
    if family != Some(WireFamilyId::OpenAiResponsesChatgpt) {
        return Ok(None);
    }
    fresh().map(Some)
}

pub fn fresh(
    install_id: &str,
    session: &BindIdentity,
    turn_id: &str,
    admitted_at_ms: u64,
) -> Result<ChatgptWire, String> {
    static WINDOW_ID: OnceLock<String> = OnceLock::new();
    if WINDOW_ID.get().is_none() {
        let mut bytes = [0; 16];
        getrandom::fill(&mut bytes).map_err(|error| error.to_string())?;
        let _ = WINDOW_ID.set(uuid_v4_text(bytes));
    }
    let os = os_info::get();
    let terminal = terminal_user_agent(&|name| std::env::var(name).ok());
    let user_agent = format!(
        "{}/{} ({} {}; {}) {}",
        ChatgptWire::ORIGINATOR,
        ChatgptWire::VERSION,
        os.os_type(),
        os.version(),
        os.architecture().unwrap_or("unknown"),
        terminal
    )
    .chars()
    .map(|c| if matches!(c, ' '..='~') { c } else { '_' })
    .collect();
    let bind = serde_json::to_string(session).map_err(|error| error.to_string())?;
    Ok(ChatgptWire {
        profile: ChatgptWireProfile::Codex0163,
        user_agent,
        originator: ChatgptWire::ORIGINATOR.into(),
        session_id: session.session.as_str().to_string(),
        thread_id: ChatgptWire::scoped_id("chatgpt-thread", &bind),
        // Reuse the owner-only persisted seed, with a separate provider namespace.
        installation_id: ChatgptWire::scoped_id("chatgpt-installation", install_id),
        window_id: WINDOW_ID.get().unwrap().clone(),
        turn_id: turn_id.to_string(),
        turn_started_at_unix_ms: admitted_at_ms,
    })
}

// Detection precedence and token sanitization match Codex rust-v0.163.0.
fn terminal_user_agent(env: &impl Fn(&str) -> Option<String>) -> String {
    let nonempty = |name| env(name).filter(|value| !value.trim().is_empty());
    let versioned = |name: &str, version: Option<String>| match version {
        Some(version) => format!("{name}/{version}"),
        None => name.to_string(),
    };
    let raw = if let Some(program) =
        nonempty("TERM_PROGRAM").filter(|p| !p.eq_ignore_ascii_case("tmux"))
    {
        versioned(&program, nonempty("TERM_PROGRAM_VERSION"))
    } else if nonempty("GHOSTTY_RESOURCES_DIR").is_some() {
        "Ghostty".into()
    } else if env("WEZTERM_VERSION").is_some() {
        versioned("WezTerm", nonempty("WEZTERM_VERSION"))
    } else if ["ITERM_SESSION_ID", "ITERM_PROFILE", "ITERM_PROFILE_NAME"]
        .iter()
        .any(|key| env(key).is_some())
    {
        "iTerm.app".into()
    } else if env("KITTY_WINDOW_ID").is_some() || env("TERM").is_some_and(|v| v.contains("kitty")) {
        "kitty".into()
    } else if env("ALACRITTY_SOCKET").is_some() || env("TERM").is_some_and(|v| v == "alacritty") {
        "Alacritty".into()
    } else if env("KONSOLE_VERSION").is_some() {
        versioned("Konsole", nonempty("KONSOLE_VERSION"))
    } else if env("GNOME_TERMINAL_SCREEN").is_some() {
        "gnome-terminal".into()
    } else if env("VTE_VERSION").is_some() {
        versioned("VTE", nonempty("VTE_VERSION"))
    } else if env("WT_SESSION").is_some() {
        "WindowsTerminal".into()
    } else {
        nonempty("TERM").unwrap_or_else(|| "unknown".into())
    };
    raw.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '/') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_tokens_match_codex() {
        for (entries, expected) in [
            (vec![], "unknown"),
            (
                vec![
                    ("TERM_PROGRAM", "Apple_Terminal"),
                    ("TERM_PROGRAM_VERSION", "455"),
                ],
                "Apple_Terminal/455",
            ),
            (
                vec![("TERM_PROGRAM", "tmux"), ("TERM", "screen-256color")],
                "screen-256color",
            ),
            (
                vec![
                    ("TERM_PROGRAM", "my terminal\n"),
                    ("TERM_PROGRAM_VERSION", "1"),
                ],
                "my_terminal_/1",
            ),
            (vec![("WEZTERM_VERSION", "2026")], "WezTerm/2026"),
            (vec![("WT_SESSION", "window")], "WindowsTerminal"),
        ] {
            assert_eq!(
                terminal_user_agent(&|key| entries
                    .iter()
                    .find(|(k, _)| *k == key)
                    .map(|(_, v)| v.to_string())),
                expected
            );
        }
    }

    #[test]
    fn admission_and_resume_preserve_profile_presence_and_absence() {
        let session = BindIdentity::new("/tmp/project", "h", "s");
        let first = for_episode(Some(WireFamilyId::OpenAiResponsesChatgpt), None, || {
            fresh("seed", &session, "run-1", 123)
        })
        .unwrap()
        .unwrap();
        let next = fresh("seed", &session, "run-2", 456).unwrap();
        assert_eq!(first.thread_id, next.thread_id);
        assert_eq!(first.installation_id, next.installation_id);
        assert_eq!(first.window_id, next.window_id);
        assert_ne!(first.thread_id, first.session_id);
        assert_ne!(first.turn_id, next.turn_id);
        assert!(first.user_agent.starts_with("codex_cli_rs/0.163.0 ("));
        assert_eq!(first.originator, "codex_cli_rs");
        assert_eq!(first.session_id, "s");
        assert_eq!(first.turn_id, "run-1");
        assert_eq!(first.turn_started_at_unix_ms, 123);
        assert_eq!(first.window_id.len(), 36);
        assert_eq!(first.profile, ChatgptWireProfile::Codex0163);
        let changed_host = || Err("host discovery must not run on resume".into());
        let config = serde_json::json!({"chatgpt_wire": first});
        assert_eq!(
            for_episode(None, Some(&config), changed_host).unwrap(),
            Some(first)
        );
        for legacy in [
            serde_json::json!({}),
            serde_json::json!({"chatgpt_wire":null}),
        ] {
            assert_eq!(
                for_episode(
                    Some(WireFamilyId::OpenAiResponsesChatgpt),
                    Some(&legacy),
                    changed_host
                )
                .unwrap(),
                None
            );
        }
        assert_eq!(
            for_episode(Some(WireFamilyId::OpenAiResponses), None, changed_host).unwrap(),
            None
        );
        assert!(for_episode(
            None,
            Some(&serde_json::json!({"chatgpt_wire":{"profile":"future"}})),
            changed_host
        )
        .is_err());
        assert_ne!(first_thread("/tmp/other", "seed"), next.thread_id);
        assert_ne!(
            fresh("other-seed", &session, "r", 0)
                .unwrap()
                .installation_id,
            next.installation_id
        );
    }

    fn first_thread(root: &str, seed: &str) -> String {
        fresh(seed, &BindIdentity::new(root, "h", "s"), "r", 0)
            .unwrap()
            .thread_id
    }
}
