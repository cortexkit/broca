//! The shared reasoning keep/clear decision (CK#1 §6.1) + the §6.1.1 segmentation guard.
//!
//! This is codec-library code: the owned broca render AND the downstream
//! reordering serializers (the MITM/plugin legs) route their reasoning keep/strip
//! decision through `decide_reasoning`, so the segmentation guard lives in ONE place and
//! every consumer inherits it.
//!
//! The keep predicate itself (recency + same-model + merge-safety) is computed by the
//! caller and handed in as `base_keep`; this function adds the GUARD that overrides a clear
//! to a keep when clearing would corrupt tool_use segmentation on a reordering profile.

use broca_protocol::content::ContentBlock;

/// How a downstream serializer orders an assistant message's content blocks — the input
/// the segmentation guard is gated on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerializerProfile {
    /// Content blocks render in SOURCE ORDER (broca's own render, Pi). tool_use is
    /// never moved, so removing a reasoning block can never reorder tool_use — the
    /// segmentation guard is a no-op on this profile.
    SourceOrder,
    /// The serializer REORDERS tool_use to the end of each thinking-delimited segment (the
    /// `@ai-sdk`/Claude-Code `moveToolUseBlocksToEnd` shape). On this profile, removing a
    /// signed thinking block merges two segments and can reflow tool_use — so the
    /// segmentation guard applies.
    ReordersToolUseByThinkingBoundary,
}

impl SerializerProfile {
    fn reorders_tool_use(self) -> bool {
        matches!(self, SerializerProfile::ReordersToolUseByThinkingBoundary)
    }
}

/// The decision for one reasoning block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReasoningDecision {
    /// Keep the block (echo it back, signature intact).
    Keep,
    /// Clear the block (strip it from the rendered request).
    Clear,
}

/// Decide whether to keep or clear the signed reasoning block at `idx` in an assistant
/// message's `content`.
///
/// `base_keep` is the caller's composable keep predicate (CK#1 §6.1: recency AND same-model
/// AND NOT after-first-of-merge-group) — true to keep, false to clear. This function returns
/// that decision UNCHANGED on a non-reordering profile.
///
/// On a reordering profile it applies the §6.1.1 SEGMENTATION GUARD (the conservative,
/// always-correct floor): never clear a SIGNED thinking / redacted_thinking block whose
/// removal would MERGE a `tool_use`-bearing segment with a non-`tool_use` segment. Removing
/// the block deletes a segment boundary, so the two adjacent segments merge and
/// `moveToolUseBlocksToEnd` reflows tool_use across what was a boundary — changing the
/// rendered byte order. A text-downgrade is NOT a substitute (it also removes the boundary),
/// so the guard is DON'T-CLEAR only.
///
/// A tighter predicate (clear when no `tool_use` actually straddles the merged boundary) is
/// a permitted §6.1.1 optimization, not built here — the conservative floor is the normative
/// requirement and is correct on every profile a future consumer might bind.
pub fn decide_reasoning(
    base_keep: bool,
    is_signed: bool,
    idx: usize,
    content: &[ContentBlock],
    profile: SerializerProfile,
) -> ReasoningDecision {
    if base_keep {
        return ReasoningDecision::Keep;
    }
    if is_signed && profile.reorders_tool_use() && clearing_merges_tool_use_boundary(idx, content) {
        return ReasoningDecision::Keep;
    }
    ReasoningDecision::Clear
}

/// A block that renders as a thinking-segment boundary: a SIGNED `Reasoning` or any
/// `RedactedReasoning` (an unsigned reasoning block is not emitted as `thinking`, so it is
/// not a boundary). Mirrors the `moveToolUseBlocksToEnd` split rule.
fn is_thinking_boundary(b: &ContentBlock) -> bool {
    matches!(
        b,
        ContentBlock::Reasoning {
            signature: Some(_),
            ..
        } | ContentBlock::RedactedReasoning { .. }
    )
}

fn is_tool_use(b: &ContentBlock) -> bool {
    matches!(b, ContentBlock::ToolCall(_))
}

/// Would clearing the thinking block at `idx` merge a `tool_use`-bearing segment with a
/// non-`tool_use` one? The block's segments are the runs of content between it and the
/// previous / next thinking boundary; removing it fuses those two runs. The conservative
/// guard fires when the fused run would combine tool_use with non-tool_use (either side),
/// because that is exactly when the reorder produces a different block order.
///
/// This is the literal §6.1.1 floor ("merge a tool_use-bearing segment with a non-tool_use
/// segment") as written. It has only been validated against the HYPOTHETICAL reorder
/// semantics — no real `ReordersToolUseByThinkingBoundary` serializer exists in this
/// codebase yet. When one is built (the MITM/plugin leg), whoever builds it MUST re-validate
/// this predicate against that serializer's ACTUAL reflow — in particular the
/// all-`tool_use`-both-sides case, which this floor treats as safe-to-clear (no
/// tool_use/non-tool_use merge) but which a real reorder that reflows tool_use within a
/// merged all-tool_use run could still perturb; if so, the floor needs a revision with MC.
fn clearing_merges_tool_use_boundary(idx: usize, content: &[ContentBlock]) -> bool {
    // before = the run from the previous thinking boundary (exclusive) up to `idx`.
    let before_start = content[..idx]
        .iter()
        .rposition(is_thinking_boundary)
        .map(|p| p + 1)
        .unwrap_or(0);
    let before = &content[before_start..idx];
    // after = the run from `idx` (exclusive) up to the next thinking boundary (exclusive).
    let after_end = content[idx + 1..]
        .iter()
        .position(is_thinking_boundary)
        .map(|p| idx + 1 + p)
        .unwrap_or(content.len());
    let after = &content[idx + 1..after_end];

    let before_tool = before.iter().any(is_tool_use);
    let before_non = before.iter().any(|b| !is_tool_use(b));
    let after_tool = after.iter().any(is_tool_use);
    let after_non = after.iter().any(|b| !is_tool_use(b));

    (before_tool && after_non) || (before_non && after_tool)
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::content::ToolCall;
    use broca_protocol::ToolCallId;
    use serde_json::json;

    fn text(s: &str) -> ContentBlock {
        ContentBlock::Text { text: s.into() }
    }
    fn signed_thinking() -> ContentBlock {
        ContentBlock::Reasoning {
            text: "t".into(),
            signature: Some("sig".into()),
        }
    }
    fn tool_call(id: &str) -> ContentBlock {
        ContentBlock::ToolCall(ToolCall {
            thought_signature: None,
            tool_call_id: ToolCallId(id.into()),
            tool_name: "x".into(),
            input: json!({}),
        })
    }

    /// The canonical §6.1.1 fixture: `text, tool_call, reasoning, text, tool_call`. Clearing
    /// the reasoning merges `[text, tool_call]` with `[text, tool_call]` — both runs carry
    /// tool_use AND non-tool_use, so the reorder would change byte order → don't-clear.
    #[test]
    fn guard_fires_on_the_canonical_merge_boundary_fixture() {
        let content = vec![
            text("a"),
            tool_call("t1"),
            signed_thinking(),
            text("b"),
            tool_call("t2"),
        ];
        // base wants to CLEAR (e.g. an older turn), but the reordering profile + the merge
        // hazard force a keep.
        let d = decide_reasoning(
            false,
            true,
            2,
            &content,
            SerializerProfile::ReordersToolUseByThinkingBoundary,
        );
        assert_eq!(
            d,
            ReasoningDecision::Keep,
            "the guard must not clear this block"
        );
    }

    /// On the SOURCE-ORDER profile the SAME fixture clears (the guard is a no-op — no
    /// reorder can happen, so removing the block is byte-safe). This proves the guard does
    /// not over-fire on the non-reordering owned render.
    #[test]
    fn guard_is_a_no_op_on_source_order() {
        let content = vec![
            text("a"),
            tool_call("t1"),
            signed_thinking(),
            text("b"),
            tool_call("t2"),
        ];
        let d = decide_reasoning(false, true, 2, &content, SerializerProfile::SourceOrder);
        assert_eq!(
            d,
            ReasoningDecision::Clear,
            "source-order never reorders, so the guard must not change the clear decision"
        );
    }

    /// base_keep always wins (the positional/same-model rule wants the block) regardless of
    /// profile.
    #[test]
    fn base_keep_is_returned_unchanged() {
        let content = vec![signed_thinking()];
        assert_eq!(
            decide_reasoning(true, true, 0, &content, SerializerProfile::SourceOrder),
            ReasoningDecision::Keep
        );
        assert_eq!(
            decide_reasoning(
                true,
                true,
                0,
                &content,
                SerializerProfile::ReordersToolUseByThinkingBoundary
            ),
            ReasoningDecision::Keep
        );
    }

    /// All-tool_use on both sides of the boundary: clearing merges two tool_use-only runs,
    /// whose reorder preserves order → safe to clear even on a reordering profile.
    #[test]
    fn guard_does_not_fire_when_both_sides_are_all_tool_use() {
        let content = vec![tool_call("t1"), signed_thinking(), tool_call("t2")];
        let d = decide_reasoning(
            false,
            true,
            1,
            &content,
            SerializerProfile::ReordersToolUseByThinkingBoundary,
        );
        assert_eq!(d, ReasoningDecision::Clear);
    }

    /// All-text on both sides: no tool_use anywhere, no reorder possible → safe to clear.
    #[test]
    fn guard_does_not_fire_when_no_tool_use_present() {
        let content = vec![text("a"), signed_thinking(), text("b")];
        let d = decide_reasoning(
            false,
            true,
            1,
            &content,
            SerializerProfile::ReordersToolUseByThinkingBoundary,
        );
        assert_eq!(d, ReasoningDecision::Clear);
    }

    /// An UNSIGNED reasoning block is not a segment boundary and `is_signed` is false, so the
    /// guard never applies (it would clear regardless).
    #[test]
    fn unsigned_block_is_not_guarded() {
        let content = vec![
            text("a"),
            tool_call("t1"),
            ContentBlock::Reasoning {
                text: "t".into(),
                signature: None,
            },
            text("b"),
            tool_call("t2"),
        ];
        let d = decide_reasoning(
            false,
            false,
            2,
            &content,
            SerializerProfile::ReordersToolUseByThinkingBoundary,
        );
        assert_eq!(d, ReasoningDecision::Clear);
    }
}
