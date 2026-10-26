//! Private-content consent text. Identity belongs to the daemon stamp; the labels here are
//! display names resolved by the caller, never identifiers or request-supplied paths.

use serde_json::{json, Value};

use crate::SourceKind;

/// Build the generic consent body without an expiry. The call's ten-minute wait is separate
/// from the person's opportunity to answer the card later.
pub fn private_content_card(
    provider: &str,
    model: &str,
    sources: &[SourceKind],
    dedup_key: &str,
) -> Result<Value, &'static str> {
    if [provider, model]
        .iter()
        .any(|label| label.trim().is_empty())
    {
        return Err("private-content consent needs provider and model display names");
    }
    if dedup_key.len() != 64
        || !dedup_key
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err("private-content consent dedup_key must be lowercase SHA-256 hex");
    }
    let labels: Vec<_> = [
        (SourceKind::PersonBrowser, "your Chrome"),
        (SourceKind::DesktopApp, "a desktop app"),
        (SourceKind::SignedInSite, "a site where you are signed in"),
    ]
    .into_iter()
    .filter_map(|(kind, label)| sources.contains(&kind).then_some(label))
    .collect();
    let source = match labels.as_slice() {
        [] => return Err("private-content consent needs a recognized source"),
        [one] => (*one).to_owned(),
        [one, two] => format!("{one} and {two}"),
        [one, two, three] => format!("{one}, {two} and {three}"),
        _ => unreachable!("only three source kinds have display text"),
    };
    Ok(json!({
        "kind": "consent",
        "title": format!("Let the agent send private content to {provider}?"),
        "prompt": format!("The agent has read content from {source}.\n\nA program it is running now wants to make a model call with {provider} ({model}). The call may include some of that content.\n\nThis approval covers only {provider}. Another provider needs its own approval."),
        "options": [
            {"id": "allow_run", "label": "Allow for this program run", "effect": "grant"},
            {"id": "allow_24h", "label": "Allow for 24 hours", "effect": "grant"},
            {"id": "deny", "label": "Deny", "effect": "decline"}
        ],
        "default": "deny", "on_expiry": "deny", "urgency": "normal",
        "material_damage": false, "late_execution": "execute",
        "target": {"kind": "provider", "label": provider},
        // Cingulate treats `scope` as opaque content; it names only what the approval covers.
        // The asking agent comes from the daemon's stamp, never from this body.
        "scope": {"provider": provider},
        "dedup_key": dedup_key, "requester_ref": dedup_key,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_card_text_and_options() {
        let key = "a".repeat(64);
        let card =
            private_content_card("TypeSafe", "Jev", &[SourceKind::PersonBrowser], &key).unwrap();
        assert_eq!(
            card["title"],
            "Let the agent send private content to TypeSafe?"
        );
        assert_eq!(card["prompt"], "The agent has read content from your Chrome.\n\nA program it is running now wants to make a model call with TypeSafe (Jev). The call may include some of that content.\n\nThis approval covers only TypeSafe. Another provider needs its own approval.");
        assert_eq!(
            card["options"],
            json!([
                {"id":"allow_run","label":"Allow for this program run","effect":"grant"},
                {"id":"allow_24h","label":"Allow for 24 hours","effect":"grant"},
                {"id":"deny","label":"Deny","effect":"decline"}
            ])
        );
        assert_eq!(card["default"], "deny");
        assert_eq!(card["on_expiry"], "deny");
        assert_eq!(card["requester_ref"], key);
        assert_eq!(card["scope"], json!({"provider":"TypeSafe"}));
        assert!(card.get("expires_in_ms").is_none());
    }

    #[test]
    fn source_labels_drop_unknowns_and_join_in_display_order() {
        let key = "0".repeat(64);
        for (sources, expected) in [
            (vec![SourceKind::Other("secret-id".into())], None),
            (
                vec![SourceKind::DesktopApp, SourceKind::PersonBrowser],
                Some("your Chrome and a desktop app"),
            ),
            (
                vec![
                    SourceKind::SignedInSite,
                    SourceKind::PersonBrowser,
                    SourceKind::DesktopApp,
                    SourceKind::PersonBrowser,
                ],
                Some("your Chrome, a desktop app and a site where you are signed in"),
            ),
        ] {
            let card = private_content_card("TypeSafe", "Jev", &sources, &key);
            match expected {
                None => assert!(card.is_err()),
                Some(source) => assert!(card.unwrap()["prompt"]
                    .as_str()
                    .unwrap()
                    .starts_with(&format!("The agent has read content from {source}."))),
            }
        }
        assert!(private_content_card("", "Jev", &[SourceKind::DesktopApp], &key).is_err());
        assert!(
            private_content_card("TypeSafe", "Jev", &[SourceKind::DesktopApp], "raw-key").is_err()
        );
    }
}
