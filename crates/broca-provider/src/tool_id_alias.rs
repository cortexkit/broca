//! Cross-family tool-call-id aliasing (CK#1 §5.6.1 rule 3).
//!
//! A tool-call id is CK-canonical and pairs a `tool_use` with its `tool_result` on that
//! canonical string. Most families echo the id back verbatim, so the canonical id IS the
//! wire id. But some canonical ids are INVALID for a target family's id grammar — e.g. an
//! OpenAI-Responses `call_…|…` id (450 chars, `|`) rendered to Anthropic, whose ids must
//! match `^[a-zA-Z0-9_-]+$` and be ≤ 64 chars. Rendering an invalid id would be rejected.
//!
//! So when (and ONLY when) a canonical id is invalid for the target family, the renderer
//! substitutes a deterministic TARGET-NATIVE alias, applied to BOTH the call and its
//! matching result so the arc still pairs. The alias is:
//!  - byte-identical to `safe_prefix + base62(fnv1a128(family_tag ⊕ canonical_id))` when
//!    that alias is free, preserving every pre-existing successful render;
//!  - a PURE DETERMINISTIC function of the prompt and target family when that first alias is
//!    taken: already-valid ids reserve their own bytes first, then invalid ids probe salted
//!    hashes in prompt order until each has a distinct alias. The fixed platform-stable hash
//!    never depends on process-seeded state, so live and resume render the same bytes.
//!
//! ## The render-edge-only invariant (load-bearing for resume byte-identity)
//! The alias exists ONLY in the rendered request bytes. The canonical id is the SOLE
//! persisted form (the session's log, materialized store, the in-memory `Message`). The alias map
//! is built from a read-only scan of the prompt and never mutates a `Message`. If the alias
//! ever leaked into a persisted `Message` (e.g. a stored `tool_result` carrying the aliased
//! id), the stored id would no longer be canonical and a later render would alias the
//! already-aliased id — breaking reproducible resume renders. So aliasing is strictly a
//! render-time projection: build the map, look ids up while emitting bytes, discard it.

use std::collections::{BTreeMap, BTreeSet};

use broca_protocol::content::{ContentBlock, Message};

use crate::hash::{base62, fnv1a128};

/// A target family's tool-call-id grammar — the validity rule + the length budget an id
/// must satisfy to be echoed back verbatim, plus a stable tag that separates this family's
/// alias space from another's (so the same canonical id aliases differently per family, per
/// CK#1's "pure fn of (canonical_id, target family)").
pub struct IdGrammar {
    /// A STABLE per-family tag mixed into the alias hash. Must never change for a family
    /// (it is part of the deterministic alias value), so it uses the family's pinned wire
    /// string, not a Debug spelling.
    pub family_tag: &'static str,
    /// The maximum id length the family accepts.
    pub max_len: usize,
    /// Whether a single character is allowed in an id of this family.
    pub allowed: fn(char) -> bool,
}

impl IdGrammar {
    /// Anthropic Messages: `^[a-zA-Z0-9_-]+$`, max 64 chars.
    pub fn anthropic() -> Self {
        Self {
            family_tag: "anthropic_messages",
            max_len: 64,
            allowed: |c| c.is_ascii_alphanumeric() || c == '_' || c == '-',
        }
    }

    /// Bedrock Converse: `^[a-zA-Z0-9_.:-]+$`, max 64 chars.
    pub fn bedrock() -> Self {
        Self {
            family_tag: "bedrock_converse",
            max_len: 64,
            allowed: |c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-'),
        }
    }

    /// Whether `id` is a valid wire id for this family (echoed back unchanged).
    fn is_valid(&self, id: &str) -> bool {
        !id.is_empty() && id.len() <= self.max_len && id.chars().all(self.allowed)
    }

    /// The deterministic target-native alias for a canonical id that is INVALID for this
    /// family. `safe_prefix + base62(hash(family_tag ⊕ canonical))`, truncated to the
    /// length budget. The hash carries the entropy; the prefix only guarantees a non-empty,
    /// grammar-valid leading character. Pure function of `(canonical, family)`.
    fn alias(&self, canonical: &str) -> String {
        self.alias_for_attempt(canonical, 0)
    }

    fn alias_for_attempt(&self, canonical: &str, attempt: u64) -> String {
        // Mix the family tag into the hashed seed (NUL-separated so `tag+canonical` can't
        // collide with a different `tag'+canonical'` split), so the SAME canonical id maps
        // to a DIFFERENT alias per target family. Attempt zero deliberately retains the
        // original seed bytes; only a taken alias adds the deterministic probe suffix.
        let seed = if attempt == 0 {
            format!("{}\u{0}{}", self.family_tag, canonical)
        } else {
            format!("{}\u{0}{}\u{0}probe:{attempt}", self.family_tag, canonical)
        };
        let mut out = String::from("t_");
        out.push_str(&base62(fnv1a128(&seed)));
        out.truncate(self.max_len);
        out
    }
}

/// A per-request canonical-id → wire-id map for one target family. Identity for ids the
/// family accepts verbatim; a deterministic alias for ids it rejects. Built from a
/// read-only scan of the prompt (never mutates a `Message`), then consulted while emitting
/// request bytes.
pub struct ToolIdMap {
    map: BTreeMap<String, String>,
}

impl ToolIdMap {
    /// Build the map by scanning every tool-call and tool-result id in `prompt`. Valid ids
    /// reserve their own wire bytes before invalid ids receive aliases, so a generated alias
    /// can never displace a valid canonical id that appears later in the prompt.
    pub fn build(prompt: &[Message], grammar: &IdGrammar) -> Self {
        let mut map: BTreeMap<String, String> = BTreeMap::new();
        let mut invalid = Vec::new();
        let mut seen_invalid = BTreeSet::new();
        // Valid canonical ids reserve their wire bytes by reference. Only an id that needs an
        // alias gets an owned map entry; identity lookups fall through to the canonical string.
        let mut valid: BTreeSet<&str> = BTreeSet::new();
        let mut aliases = BTreeSet::new();

        // Pass one reserves every already-valid id regardless of where it appears. Keep only the
        // unique invalid sequence for deterministic prompt-order allocation in pass two.
        for m in prompt {
            for b in &m.content {
                let canonical = match b {
                    ContentBlock::ToolCall(tc) => tc.tool_call_id.0.as_str(),
                    ContentBlock::ToolResult(tr) => tr.tool_call_id.0.as_str(),
                    _ => continue,
                };
                if grammar.is_valid(canonical) {
                    valid.insert(canonical);
                } else if seen_invalid.insert(canonical) {
                    invalid.push(canonical);
                }
            }
        }

        // Pass two gives invalid ids their legacy alias when free. A collision probes a
        // platform-stable salted seed, preserving determinism without changing the common path.
        for canonical in invalid {
            let mut attempt = 0u64;
            let wire = loop {
                let candidate = if attempt == 0 {
                    grammar.alias(canonical)
                } else {
                    grammar.alias_for_attempt(canonical, attempt)
                };
                if !valid.contains(candidate.as_str()) && !aliases.contains(candidate.as_str()) {
                    break candidate;
                }
                attempt = attempt
                    .checked_add(1)
                    .expect("tool-call-id alias probe space exhausted");
            };
            assert!(
                aliases.insert(wire.clone()),
                "tool-call-id alias allocator selected an occupied wire id"
            );
            let previous = map.insert(canonical.to_owned(), wire);
            assert!(
                previous.is_none(),
                "tool-call-id alias map contains a duplicate canonical id"
            );
        }

        Self { map }
    }

    /// The wire id for a canonical id (identity when valid, the alias when not). An id not
    /// seen during `build` (none should occur — every rendered id is scanned) falls back to
    /// the canonical string unchanged.
    pub fn wire_id<'a>(&'a self, canonical: &'a str) -> &'a str {
        self.map
            .get(canonical)
            .map(String::as_str)
            .unwrap_or(canonical)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::content::{Role, ToolCall, ToolOutput, ToolResult};
    use broca_protocol::ToolCallId;
    use serde_json::json;

    fn anthropic() -> IdGrammar {
        IdGrammar::anthropic()
    }

    #[test]
    fn valid_id_is_identity() {
        let g = anthropic();
        assert!(g.is_valid("toolu_abc-123"));
        assert_eq!(g.alias_or_identity("toolu_abc-123"), "toolu_abc-123");
    }

    #[test]
    fn invalid_id_aliases_deterministically_and_validly() {
        let g = anthropic();
        let long = format!("call_{}|extra", "x".repeat(460));
        assert!(!g.is_valid(&long));
        let a1 = g.alias(&long);
        let a2 = g.alias(&long);
        assert_eq!(a1, a2, "alias must be deterministic");
        assert!(
            g.is_valid(&a1),
            "alias must be valid for the target grammar"
        );
        assert!(a1.len() <= g.max_len);
    }

    #[test]
    fn alias_differs_by_target_family() {
        let anthropic = IdGrammar::anthropic();
        let other = IdGrammar {
            family_tag: "some_other_family",
            ..IdGrammar::anthropic()
        };
        let id = format!("call_{}|x", "y".repeat(500));
        assert_ne!(
            anthropic.alias(&id),
            other.alias(&id),
            "the same canonical id must alias differently per target family"
        );
    }

    impl IdGrammar {
        // Test helper mirroring build's per-id decision.
        fn alias_or_identity(&self, id: &str) -> String {
            if self.is_valid(id) {
                id.to_string()
            } else {
                self.alias(id)
            }
        }
    }

    #[test]
    fn map_pairs_call_and_result_to_the_same_wire_id() {
        let g = anthropic();
        let foreign = format!("call_{}|z", "q".repeat(500));
        let prompt = vec![
            Message::assistant_from(
                vec![ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    provider_state: None,
                    tool_call_id: ToolCallId(foreign.clone()),
                    tool_name: "edit".into(),
                    input: json!({}).into(),
                })],
                broca_protocol::request::ModelHandle::new("openai", "gpt"),
            ),
            Message::new(
                Role::Tool,
                vec![ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId(foreign.clone()),
                    output: ToolOutput::Text { text: "ok".into() },
                    is_error: false,
                })],
            ),
        ];
        let map = ToolIdMap::build(&prompt, &g);
        let wire = map.wire_id(&foreign);
        assert_ne!(wire, foreign, "the foreign id must be aliased");
        assert!(g.is_valid(wire));
        // Both the call and the result resolve to the SAME wire id → the arc still pairs.
        assert_eq!(map.wire_id(&foreign), wire);
    }

    #[test]
    fn valid_ids_do_not_build_identity_entries() {
        let id = "call_valid_123".to_owned();
        let prompt = vec![Message::assistant_from(
            vec![ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                provider_state: None,
                tool_call_id: ToolCallId(id.clone()),
                tool_name: "lookup".into(),
                input: json!({}).into(),
            })],
            broca_protocol::request::ModelHandle::new("openai", "gpt"),
        )];

        let map = ToolIdMap::build(&prompt, &anthropic());

        assert!(
            map.map.is_empty(),
            "an already-valid id needs no owned identity entry"
        );
        let wire = map.wire_id(&id);
        assert_eq!(wire, id);
        assert!(
            std::ptr::eq(wire.as_ptr(), id.as_ptr()),
            "an already-valid id should be returned from the canonical string"
        );
    }

    #[test]
    fn colliding_map_build_is_deterministic() {
        const INVALID: &str = "bad:id";
        const VALID_LEGACY_ALIAS: &str = "t_Vpezy1fil0QVLlVzRotpU2";
        let prompt = vec![Message::assistant_from(
            vec![
                ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    provider_state: None,
                    tool_call_id: ToolCallId(INVALID.into()),
                    tool_name: "first".into(),
                    input: json!({}).into(),
                }),
                ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    provider_state: None,
                    tool_call_id: ToolCallId(VALID_LEGACY_ALIAS.into()),
                    tool_name: "second".into(),
                    input: json!({}).into(),
                }),
            ],
            broca_protocol::request::ModelHandle::new("openai", "gpt"),
        )];

        let first = ToolIdMap::build(&prompt, &anthropic());
        let second = ToolIdMap::build(&prompt, &anthropic());
        assert_eq!(first.wire_id(INVALID), second.wire_id(INVALID));
        assert_eq!(
            first.wire_id(VALID_LEGACY_ALIAS),
            second.wire_id(VALID_LEGACY_ALIAS)
        );
        assert_ne!(first.wire_id(INVALID), first.wire_id(VALID_LEGACY_ALIAS));
    }
}
