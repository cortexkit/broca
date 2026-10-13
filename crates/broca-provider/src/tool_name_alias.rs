//! Render-edge normalization for model-visible tool names.
//!
//! Canonical tool names stay unchanged in manifests, WAL records, prompts, and dispatch. A
//! provider request may need a stricter name grammar, so this module builds a per-render map from
//! canonical names to valid wire names and maps echoed names back before the loop sees them.

use std::collections::{BTreeMap, BTreeSet};

use broca_protocol::{
    content::{ContentBlock, Message},
    provider::{NormalizedEvent, ProviderControl},
    request::Tool,
};

use crate::hash::{base62, fnv1a64};

/// The OpenAI-safe name budget applied to every provider family.
pub const MAX_TOOL_NAME_BYTES: usize = 64;
const HASH_SUFFIX_LEN: usize = 6;

fn is_allowed(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '_' | '-')
}

/// Normalize a canonical tool name for a model-facing request.
///
/// Valid names are returned byte-for-byte unchanged. Any mutation replaces invalid characters
/// with `_`, keeps the result within 64 bytes, and appends six deterministic base62 characters
/// derived from the original name. The suffix preserves distinct aliases for ordinary
/// sanitization collisions while keeping the common valid-name path byte-identical.
pub fn normalize(name: &str) -> String {
    if is_valid_wire_name(name) {
        return name.to_owned();
    }
    alias_for_attempt(name, 0)
}

fn is_valid_wire_name(name: &str) -> bool {
    name.len() <= MAX_TOOL_NAME_BYTES && name.chars().all(is_allowed)
}

fn alias_for_attempt(name: &str, attempt: u64) -> String {
    let sanitized: String = name
        .chars()
        .map(|character| {
            if is_allowed(character) {
                character
            } else {
                '_'
            }
        })
        .collect();
    let suffix = hash_suffix_for_attempt(name, attempt);
    let prefix_budget = MAX_TOOL_NAME_BYTES - 1 - HASH_SUFFIX_LEN;
    let mut alias: String = sanitized.chars().take(prefix_budget).collect();
    if alias.is_empty() {
        alias.push('_');
    }
    alias.push('_');
    alias.push_str(&suffix);
    alias
}

fn hash_suffix_for_attempt(original: &str, attempt: u64) -> String {
    let seed = if attempt == 0 {
        original.to_owned()
    } else {
        format!("{original}\u{0}probe:{attempt}")
    };
    let encoded = base62(fnv1a64(&seed, 0) as u128);
    if encoded.len() >= HASH_SUFFIX_LEN {
        encoded[..HASH_SUFFIX_LEN].to_owned()
    } else {
        format!("{encoded:0>width$}", width = HASH_SUFFIX_LEN)
    }
}

/// A per-render canonical-name ↔ wire-name map.
#[derive(Debug, Clone, Default)]
pub struct ToolNameMap {
    canonical_to_wire: BTreeMap<String, String>,
    wire_to_canonical: BTreeMap<String, String>,
}

impl ToolNameMap {
    /// Build a map from the current tool manifest and any historical tool calls in the prompt.
    ///
    /// The prompt scan keeps resumed turns aligned with the manifest even when a caller supplies
    /// a partial tool list. Valid names reserve their own bytes before aliases are allocated, so
    /// distinct canonical names always receive distinct wire names.
    pub fn build(tools: &[Tool], prompt: &[Message]) -> Self {
        let mut names = Vec::with_capacity(tools.len());
        names.extend(tools.iter().map(|tool| tool.name.as_str()));
        for message in prompt {
            for block in &message.content {
                if let ContentBlock::ToolCall(call) = block {
                    names.push(call.tool_name.as_str());
                }
            }
        }
        Self::from_names(names)
    }

    /// Build a map from arbitrary canonical names. This is useful for render-edge tests and
    /// keeps the collision invariant independent of a particular request shape.
    pub fn from_names<I, S>(names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let canonicals: BTreeSet<String> = names
            .into_iter()
            .map(|name| name.as_ref().to_owned())
            .collect();
        let mut map = Self::default();

        // Reserve every valid canonical name before allocating aliases. Otherwise an invalid
        // name's generated alias can occupy bytes belonging to a valid name encountered later.
        for canonical in canonicals
            .iter()
            .filter(|canonical| normalize(canonical) == canonical.as_str())
        {
            let previous = map
                .wire_to_canonical
                .insert(canonical.clone(), canonical.clone());
            assert!(
                previous.is_none(),
                "distinct valid canonical tool names unexpectedly shared wire bytes"
            );
            map.canonical_to_wire
                .insert(canonical.clone(), canonical.clone());
        }

        // Attempt zero preserves the historical alias bytes. Only an occupied alias takes the
        // deterministic probe path, so every previously working render remains byte-identical.
        for canonical in canonicals
            .iter()
            .filter(|canonical| normalize(canonical) != canonical.as_str())
        {
            let mut attempt = 0u64;
            let wire = loop {
                let candidate = alias_for_attempt(canonical, attempt);
                if attempt > 0 {
                    assert!(
                        is_valid_wire_name(&candidate),
                        "probed tool-name alias escaped the wire grammar: {candidate:?}"
                    );
                }
                if !map.wire_to_canonical.contains_key(&candidate) {
                    break candidate;
                }
                attempt = attempt
                    .checked_add(1)
                    .expect("tool-name alias probe space exhausted");
            };
            let previous = map
                .wire_to_canonical
                .insert(wire.clone(), canonical.clone());
            assert!(
                previous.is_none(),
                "tool-name alias allocator selected occupied wire name {wire:?}"
            );
            map.canonical_to_wire.insert(canonical.clone(), wire);
        }

        assert_eq!(
            map.canonical_to_wire.len(),
            map.wire_to_canonical.len(),
            "tool-name alias map lost injectivity"
        );
        for (canonical, wire) in &map.canonical_to_wire {
            assert_eq!(
                map.wire_to_canonical.get(wire),
                Some(canonical),
                "tool-name alias reverse map is inconsistent"
            );
        }
        map
    }

    /// Return the model-visible name for a canonical name.
    pub fn wire_name<'a>(&'a self, canonical: &'a str) -> &'a str {
        self.canonical_to_wire
            .get(canonical)
            .map(String::as_str)
            .unwrap_or_else(|| canonical)
    }

    /// Return the canonical name for a name echoed by the provider.
    pub fn canonical_name<'a>(&'a self, wire: &'a str) -> &'a str {
        self.wire_to_canonical
            .get(wire)
            .map(String::as_str)
            .unwrap_or(wire)
    }

    /// Restore canonical tool names in an inbound assembled assistant event.
    pub fn restore_event(&self, event: NormalizedEvent) -> NormalizedEvent {
        match event {
            NormalizedEvent::Control(ProviderControl::ToolArgsTrailingGarbage { mut cut }) => {
                cut.tool_name = self.canonical_name(&cut.tool_name).to_owned();
                NormalizedEvent::Control(ProviderControl::ToolArgsTrailingGarbage { cut })
            }
            NormalizedEvent::Control(ProviderControl::AssistantMessage { mut message }) => {
                for block in &mut message.content {
                    if let ContentBlock::ToolCall(call) = block {
                        call.tool_name = self.canonical_name(&call.tool_name).to_owned();
                    }
                }
                NormalizedEvent::Control(ProviderControl::AssistantMessage { message })
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::{
        content::{AssistantMessage, ToolCall},
        ids::{MessageId, ToolCallId},
        request::Tool,
    };
    use serde_json::json;

    fn tool(name: &str) -> Tool {
        Tool {
            name: name.into(),
            description: "test tool".into(),
            input_schema: json!({"type": "object"}),
            module: None,
            execution_mode: Default::default(),
        }
    }

    fn legacy_from_names<I, S>(names: I) -> ToolNameMap
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut map = ToolNameMap::default();
        for name in names {
            let canonical = name.as_ref();
            if map.canonical_to_wire.contains_key(canonical) {
                continue;
            }
            let wire = normalize(canonical);
            if let Some(previous) = map.wire_to_canonical.get(&wire) {
                assert_eq!(previous, canonical, "legacy alias collision");
            }
            map.wire_to_canonical
                .insert(wire.clone(), canonical.to_owned());
            map.canonical_to_wire.insert(canonical.to_owned(), wire);
        }
        map
    }

    fn serialized_map(map: &ToolNameMap) -> serde_json::Value {
        serde_json::to_value((&map.canonical_to_wire, &map.wire_to_canonical)).unwrap()
    }

    #[test]
    fn valid_name_is_byte_identity() {
        let name = "read_file-v2_é";
        let normalized = normalize(name);
        assert_ne!(normalized, name, "non-ASCII is not in the wire grammar");
        let valid = "read_file-v2";
        assert_eq!(normalize(valid).as_bytes(), valid.as_bytes());
    }

    #[test]
    fn sanitization_collision_stays_distinct() {
        let first = "mcp.server/list-files";
        let second = "mcp_server/list-files";
        let map = ToolNameMap::from_names([first, second]);
        assert_ne!(map.wire_name(first), map.wire_name(second));
        assert_eq!(map.canonical_name(map.wire_name(first)), first);
        assert_eq!(map.canonical_name(map.wire_name(second)), second);
    }

    #[test]
    fn long_name_is_capped_with_a_hash_suffix() {
        let name = "x".repeat(65);
        let alias = normalize(&name);
        assert_eq!(alias.len(), MAX_TOOL_NAME_BYTES);
        assert!(alias.ends_with(&hash_suffix_for_attempt(&name, 0)));
        assert_ne!(alias, name);
    }

    #[test]
    fn echoed_tool_use_name_round_trips_to_canonical_dispatch_name() {
        let canonical = "mcp.server/list-files";
        let map = ToolNameMap::build(&[tool(canonical)], &[]);
        let wire = map.wire_name(canonical).to_owned();
        let event = NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("message".into()),
                content: vec![ContentBlock::ToolCall(ToolCall {
                    tool_call_id: ToolCallId("call-1".into()),
                    tool_name: wire,
                    input: json!({}),
                    thought_signature: None,
                })],
            },
        });

        let restored = map.restore_event(event);
        let NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) = restored
        else {
            panic!("expected an assistant message");
        };
        let [ContentBlock::ToolCall(call)] = message.content.as_slice() else {
            panic!("expected one tool call");
        };
        assert_eq!(call.tool_name, canonical);
    }

    #[test]
    fn map_keeps_valid_names_unchanged() {
        let map = ToolNameMap::build(&[tool("read_file")], &[]);
        assert_eq!(map.wire_name("read_file"), "read_file");
        assert_eq!(map.canonical_name("read_file"), "read_file");
    }

    #[test]
    fn generated_alias_cannot_displace_a_valid_canonical_name() {
        const INVALID: &str = "bad:name";
        const VALID_LEGACY_ALIAS: &str = "bad_name_pO2PUf";
        assert_eq!(normalize(INVALID), VALID_LEGACY_ALIAS);
        assert!(
            std::panic::catch_unwind(|| { legacy_from_names([INVALID, VALID_LEGACY_ALIAS]) })
                .is_err()
        );

        let map = ToolNameMap::build(&[tool(INVALID), tool(VALID_LEGACY_ALIAS)], &[]);
        let invalid_wire = map.wire_name(INVALID);
        let valid_wire = map.wire_name(VALID_LEGACY_ALIAS);
        assert_ne!(invalid_wire, valid_wire);
        assert_eq!(valid_wire, VALID_LEGACY_ALIAS);
        assert!(is_valid_wire_name(invalid_wire));
        assert!(is_valid_wire_name(valid_wire));
        assert_eq!(map.canonical_name(invalid_wire), INVALID);
        assert_eq!(map.canonical_name(valid_wire), VALID_LEGACY_ALIAS);
    }

    #[test]
    fn colliding_allocation_is_input_order_independent() {
        const INVALID: &str = "bad:name";
        const VALID_LEGACY_ALIAS: &str = "bad_name_pO2PUf";
        let forward = ToolNameMap::from_names([INVALID, VALID_LEGACY_ALIAS]);
        let reverse = ToolNameMap::from_names([VALID_LEGACY_ALIAS, INVALID]);

        assert_eq!(serialized_map(&forward), serialized_map(&reverse));
        assert_eq!(forward.wire_name(INVALID), reverse.wire_name(INVALID));
        assert_eq!(
            forward.wire_name(VALID_LEGACY_ALIAS),
            reverse.wire_name(VALID_LEGACY_ALIAS)
        );
    }

    #[test]
    fn non_colliding_allocation_matches_legacy_serialized_map() {
        let names = [
            "read_file".to_string(),
            "mcp.server/list-files".to_string(),
            "mcp_server/list-files".to_string(),
            "x".repeat(65),
        ];
        let current = ToolNameMap::from_names(names.iter());
        let legacy = legacy_from_names(names.iter());
        assert_eq!(serialized_map(&current), serialized_map(&legacy));
    }

    #[test]
    fn probed_alias_stays_valid_at_the_length_boundary() {
        let invalid = "x".repeat(MAX_TOOL_NAME_BYTES + 1);
        let valid_legacy_alias = normalize(&invalid);
        assert_eq!(valid_legacy_alias.len(), MAX_TOOL_NAME_BYTES);
        assert!(is_valid_wire_name(&valid_legacy_alias));

        let map = ToolNameMap::from_names([invalid.as_str(), valid_legacy_alias.as_str()]);
        let probed = map.wire_name(&invalid);
        assert_ne!(probed, valid_legacy_alias);
        assert_eq!(probed, alias_for_attempt(&invalid, 1));
        assert_eq!(probed.len(), MAX_TOOL_NAME_BYTES);
        assert!(probed.chars().all(is_allowed));
        assert_eq!(map.canonical_name(probed), invalid);
        assert_eq!(map.canonical_name(&valid_legacy_alias), valid_legacy_alias);
    }
}
