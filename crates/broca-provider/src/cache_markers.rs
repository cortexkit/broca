//! Deterministic placement for provider prefix-cache breakpoints.
//!
//! The renderers own all marker emission, so this module only computes message
//! positions from the frozen policy inputs and the already-final prompt. It never
//! reads ambient state and never mutates the prompt.

use broca_protocol::content::{ContentBlock, Role};
use broca_protocol::request::CacheTail;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarkerTarget {
    pub message_index: usize,
    /// Cacheable-block ordinal. `None` preserves the legacy last-cacheable-block target.
    pub cacheable_block_index: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkerPlan {
    /// Mark the coalesced system instruction tail when selected by the frozen order.
    pub system_tail: bool,
    /// Mark the previous user-side boundary instead of the system tail.
    pub bridge_message: Option<MarkerTarget>,
    /// Selected targets; block targeting does not change message selection or slot spending.
    pub message_anchors: Vec<MarkerTarget>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarkerViewMessage {
    pub role: Role,
    /// Count of rendered blocks that can participate in provider prefix caching.
    pub block_count: usize,
    pub has_cacheable: bool,
    pub cache_prefix_blocks: Option<u32>,
}

/// A stable declaration identifies the message's position in the rendered request.
/// Rendering can filter messages, so this index may differ from its canonical prompt position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeclaredMarker {
    pub message_index: usize,
    /// Lower ranks win first; equal ranks preserve declaration order.
    pub stability_rank: u32,
}

/// Select cache markers using the priority fixed when the session was admitted.
/// Version 1 selects tail, ranked declarations, then system. An absent version preserves
/// the original marker placement and therefore the original rendered request bytes.
/// A needed bridge replaces the system slot or uses a spare slot; it never evicts the tail
/// or a declared message.
pub fn plan_markers(
    lookback_blocks: u32,
    max_breakpoints: u32,
    system_blocks: usize,
    tail: Option<CacheTail>,
    messages: &[MarkerViewMessage],
    version: Option<u32>,
    declared: &[DeclaredMarker],
) -> MarkerPlan {
    if version != Some(1) {
        return plan_legacy_markers(
            lookback_blocks,
            max_breakpoints,
            system_blocks,
            tail,
            messages,
        );
    }
    let legacy = plan_legacy_markers(lookback_blocks, 4, system_blocks, tail, messages);
    let mut candidates = Vec::new();
    if tail != Some(CacheTail::Volatile) {
        if let Some(index) = messages.len().checked_sub(1) {
            if messages[index].has_cacheable {
                candidates.push(Some(marker_target(index, &messages[index])));
            }
        }
    }
    let mut ranked = declared.to_vec();
    ranked.sort_by_key(|candidate| candidate.stability_rank);
    for candidate in ranked {
        if let Some(message) = messages.get(candidate.message_index) {
            let target = Some(marker_target(candidate.message_index, message));
            if message.has_cacheable && !candidates.contains(&target) {
                candidates.push(target);
            }
        }
    }
    if system_blocks > 0 {
        candidates.push(None);
    }
    let max = max_breakpoints.min(4) as usize;
    candidates.truncate(max);
    let mut plan = MarkerPlan {
        system_tail: candidates.contains(&None),
        bridge_message: None,
        message_anchors: candidates.iter().flatten().copied().collect(),
    };
    if let Some(bridge) = legacy.bridge_message {
        if !plan.message_anchors.contains(&bridge) && (plan.system_tail || candidates.len() < max) {
            plan.system_tail = false;
            plan.bridge_message = Some(bridge);
        }
    }
    plan
}

/// Compute the legacy bounded prefix-cache marker plan.
///
/// `tail` is the caller's frozen declaration about its own final message. `Some(Volatile)`
/// means that message is different next request, so a breakpoint on it can only ever pay the
/// cache-write premium for bytes no later request can match. That one slot is withheld and
/// left unspent; `None` (no declaration) plans exactly as before.
fn plan_legacy_markers(
    lookback_blocks: u32,
    max_breakpoints: u32,
    system_blocks: usize,
    tail: Option<CacheTail>,
    messages: &[MarkerViewMessage],
) -> MarkerPlan {
    let user_boundaries = user_boundaries(messages);
    let latest = user_boundaries
        .iter()
        .rev()
        .find(|boundary| boundary.index > 1)
        .copied();
    let bridge = latest.and_then(|latest| {
        let previous = user_boundaries
            .iter()
            .rev()
            .find(|boundary| boundary.index < latest.index)
            .copied()?;
        let distance = latest
            .block_position
            .saturating_sub(previous.block_position)
            + 1;
        (previous.index > 1 && distance > lookback_blocks as usize).then_some(previous.index)
    });

    // The withheld position is the anchor on the FINAL message, whatever its index -- not a
    // fixed slot. On a two-message prompt that is index 1, but on a longer prompt the earlier
    // anchors keep their markers and only the one landing on the end of the array is dropped. A
    // single-message prompt therefore gets no message anchor at all, which is what the
    // declaration says: its only message is the volatile one. `bridge_message` is never
    // affected: it is chosen strictly before the latest user-side boundary, so it can never be
    // the final message.
    let volatile_index = match tail {
        Some(CacheTail::Volatile) => messages.len().checked_sub(1),
        None => None,
    };

    let mut plan = MarkerPlan {
        system_tail: false,
        bridge_message: None,
        message_anchors: Vec::new(),
    };
    let mut spent = 0usize;
    let max = max_breakpoints as usize;

    if let Some(index) = bridge {
        if spent < max && messages[index].has_cacheable {
            plan.bridge_message = Some(marker_target(index, &messages[index]));
            spent += 1;
        }
    } else if system_blocks > 0 && spent < max {
        plan.system_tail = true;
        spent += 1;
    }

    for (index, message) in messages.iter().enumerate().take(2) {
        if spent >= max {
            break;
        }
        if Some(index) == volatile_index {
            continue;
        }
        if message.has_cacheable {
            plan.message_anchors.push(marker_target(index, message));
            spent += 1;
        }
    }

    if let Some(latest) = latest {
        if spent < max
            && Some(latest.index) != volatile_index
            && messages[latest.index].has_cacheable
        {
            plan.message_anchors
                .push(marker_target(latest.index, &messages[latest.index]));
        }
    }

    plan
}

fn marker_target(message_index: usize, message: &MarkerViewMessage) -> MarkerTarget {
    let cacheable_block_index = message
        .cache_prefix_blocks
        .and_then(|count| usize::try_from(count).ok())
        .filter(|count| *count > 0 && message.block_count > *count)
        .map(|count| count - 1);
    MarkerTarget {
        message_index,
        cacheable_block_index,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct UserBoundary {
    index: usize,
    block_position: usize,
}

fn user_boundaries(messages: &[MarkerViewMessage]) -> Vec<UserBoundary> {
    let mut anchors = Vec::new();
    let mut block_position = 0usize;

    for (index, message) in messages.iter().enumerate() {
        debug_assert!(message.block_count > 0 || !message.has_cacheable);
        if is_user_side_boundary(message.role) && message.block_count > 0 {
            anchors.push(UserBoundary {
                index,
                block_position: block_position + message.block_count - 1,
            });
        }
        block_position += message.block_count;
    }

    anchors
}

fn is_user_side_boundary(role: Role) -> bool {
    // Tool-result turns render as user-role content for marker providers, so they
    // are user-side prefix boundaries for the provider lookback window.
    matches!(role, Role::User | Role::Tool)
}

/// Return the last cacheable canonical content-block index in one message.
/// Reasoning blocks are skipped because marker providers reject breakpoints on
/// thinking/redacted-thinking blocks.
pub fn last_cacheable_block_index(blocks: &[ContentBlock]) -> Option<usize> {
    blocks.iter().rposition(is_cacheable_block)
}

fn is_cacheable_block(block: &ContentBlock) -> bool {
    match block {
        ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } => !text.is_empty(),
        ContentBlock::ToolCall(_) | ContentBlock::ToolResult(_) => true,
        ContentBlock::Reasoning { .. } | ContentBlock::RedactedReasoning { .. } => false,
    }
}

#[cfg(test)]
mod tests {
    use super::plan_legacy_markers as plan_markers;
    use super::*;
    use broca_protocol::{
        content::{Message, ToolCall, ToolOutput, ToolResult},
        ToolCallId,
    };
    use serde_json::json;

    fn text(value: &str) -> ContentBlock {
        ContentBlock::Text { text: value.into() }
    }

    fn thinking(value: &str) -> ContentBlock {
        ContentBlock::Reasoning {
            text: value.into(),
            signature: Some("sig".into()),
        }
    }

    fn redacted() -> ContentBlock {
        ContentBlock::RedactedReasoning {
            data: "blob".into(),
            provider_format: None,
        }
    }

    fn tool_call(id: &str) -> ContentBlock {
        ContentBlock::ToolCall(ToolCall {
            thought_signature: None,
            tool_call_id: ToolCallId(id.into()),
            tool_name: "lookup".into(),
            input: json!({"q": id}),
        })
    }

    fn tool_result(id: &str) -> ContentBlock {
        ContentBlock::ToolResult(ToolResult {
            tool_call_id: ToolCallId(id.into()),
            output: ToolOutput::Text { text: "ok".into() },
            is_error: false,
        })
    }

    fn msg(role: Role, content: Vec<ContentBlock>) -> Message {
        Message {
            role,
            content,
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
        }
    }

    fn user(label: &str) -> Message {
        msg(Role::User, vec![text(label)])
    }

    fn marker_view(message: &Message) -> MarkerViewMessage {
        let block_count = message
            .content
            .iter()
            .filter(|block| is_cacheable_block(block))
            .count();
        MarkerViewMessage {
            role: message.role,
            block_count,
            has_cacheable: block_count > 0,
            cache_prefix_blocks: message.cache_prefix_blocks,
        }
    }

    fn marker_views(messages: &[Message]) -> Vec<MarkerViewMessage> {
        messages.iter().map(marker_view).collect()
    }

    fn target(message_index: usize) -> MarkerTarget {
        MarkerTarget {
            message_index,
            cacheable_block_index: None,
        }
    }

    #[test]
    fn frozen_order_slot_pressure_preserves_tail_and_ranked_declarations() {
        let views = marker_views(&[
            user("m0"),
            user("m1"),
            user("bridge"),
            msg(Role::Assistant, vec![text("a"), text("b"), text("c")]),
            user("tail"),
        ]);
        let declared = [
            DeclaredMarker {
                message_index: 1,
                stability_rank: 20,
            },
            DeclaredMarker {
                message_index: 0,
                stability_rank: 10,
            },
        ];
        let four = super::plan_markers(20, 4, 1, None, &views, Some(1), &declared);
        assert_eq!(four.message_anchors, vec![target(4), target(0), target(1)]);
        assert!(four.system_tail);
        let bridge = super::plan_markers(2, 4, 1, None, &views, Some(1), &declared);
        assert_eq!(bridge.message_anchors, four.message_anchors);
        assert_eq!(bridge.bridge_message, Some(target(2)));
        assert!(!bridge.system_tail);
        for lookback in [2, 20] {
            let two = super::plan_markers(lookback, 2, 1, None, &views, Some(1), &declared);
            assert_eq!(two.message_anchors, vec![target(4), target(0)]);
            assert!(!two.system_tail);
            assert_eq!(two.bridge_message, None);
        }
    }

    #[test]
    fn absent_frozen_order_retains_legacy_selection_under_pressure() {
        let views = marker_views(&[user("m0"), user("m1"), user("tail")]);
        let plan = super::plan_markers(20, 2, 1, None, &views, None, &[]);
        assert!(plan.system_tail);
        assert_eq!(plan.message_anchors, vec![target(0)]);
    }

    #[test]
    fn new_order_empty_declarations_and_volatile_tail() {
        let views = marker_views(&[user("m0"), user("tail")]);
        let stable = super::plan_markers(20, 4, 1, None, &views, Some(1), &[]);
        assert_eq!(stable.message_anchors, vec![target(1)]);
        assert!(stable.system_tail);
        let volatile =
            super::plan_markers(20, 4, 1, Some(CacheTail::Volatile), &views, Some(1), &[]);
        assert!(volatile.message_anchors.is_empty());
        assert!(volatile.system_tail);
    }

    #[test]
    fn m0_m1_anchors_use_last_cacheable_block() {
        let messages = vec![
            msg(Role::User, vec![text("m0"), tool_call("a")]),
            msg(Role::Assistant, vec![text("m1"), thinking("tail")]),
            user("latest"),
        ];

        let plan = plan_markers(20, 4, 1, None, &marker_views(&messages));
        assert!(plan.message_anchors.contains(&target(0)));
        assert!(plan.message_anchors.contains(&target(1)));
        assert_eq!(last_cacheable_block_index(&messages[0].content), Some(1));
        assert_eq!(last_cacheable_block_index(&messages[1].content), Some(0));
    }

    #[test]
    fn short_prompts_spend_only_available_slots() {
        let empty: Vec<Message> = Vec::new();
        assert_eq!(
            plan_markers(20, 4, 0, None, &marker_views(&empty)),
            MarkerPlan {
                system_tail: false,
                bridge_message: None,
                message_anchors: vec![]
            }
        );

        let one = vec![user("m0")];
        assert_eq!(
            plan_markers(20, 4, 0, None, &marker_views(&one)).message_anchors,
            vec![target(0)]
        );

        let two = vec![user("m0"), msg(Role::Assistant, vec![text("m1")])];
        assert_eq!(
            plan_markers(20, 4, 0, None, &marker_views(&two)).message_anchors,
            vec![target(0), target(1)]
        );
    }

    #[test]
    fn latest_user_boundary_moves_and_never_selects_m0_or_m1() {
        let first_tail = vec![
            user("m0"),
            user("m1"),
            user("u2"),
            msg(Role::Assistant, vec![text("a")]),
        ];
        let first = plan_markers(20, 4, 0, None, &marker_views(&first_tail));
        assert_eq!(first.message_anchors, vec![target(0), target(1), target(2)]);

        let mut appended = first_tail;
        appended.push(user("u4"));
        let second = plan_markers(20, 4, 0, None, &marker_views(&appended));
        assert_eq!(
            second.message_anchors,
            vec![target(0), target(1), target(4)]
        );
        assert!(!second
            .message_anchors
            .iter()
            .skip(2)
            .any(|target| target.message_index < 2));
    }

    #[test]
    fn bridge_triggers_at_lookback_plus_one_and_replaces_system_tail() {
        let at_lookback = vec![
            user("m0"),
            user("m1"),
            user("previous"),
            msg(Role::Assistant, vec![text("a"), tool_call("b")]),
            user("latest"),
        ];
        let no_bridge = plan_markers(4, 4, 1, None, &marker_views(&at_lookback));
        assert!(no_bridge.system_tail);
        assert_eq!(no_bridge.bridge_message, None);

        let over_lookback = vec![
            user("m0"),
            user("m1"),
            user("previous"),
            msg(
                Role::Assistant,
                vec![text("a"), tool_call("b"), tool_call("c")],
            ),
            user("latest"),
        ];
        let bridge = plan_markers(4, 4, 1, None, &marker_views(&over_lookback));
        assert!(!bridge.system_tail);
        assert_eq!(bridge.bridge_message, Some(target(2)));
        assert_eq!(
            bridge.message_anchors,
            vec![target(0), target(1), target(4)]
        );
    }

    #[test]
    fn bridge_guard_keeps_system_tail_when_previous_boundary_is_m1() {
        let messages = vec![
            user("m0"),
            user("m1"),
            msg(
                Role::Assistant,
                vec![text("a"), tool_call("b"), tool_call("c")],
            ),
            user("latest"),
        ];

        let plan = plan_markers(2, 4, 1, None, &marker_views(&messages));
        assert!(plan.system_tail);
        assert_eq!(plan.bridge_message, None);
        assert_eq!(plan.message_anchors, vec![target(0), target(1), target(3)]);
    }

    #[test]
    fn trailing_thinking_blocks_are_skipped_and_only_thinking_gets_no_marker() {
        let mixed = msg(
            Role::Assistant,
            vec![text("answer"), thinking("why"), redacted()],
        );
        assert_eq!(last_cacheable_block_index(&mixed.content), Some(0));

        let only_thinking = msg(Role::User, vec![thinking("why"), redacted()]);
        assert_eq!(last_cacheable_block_index(&only_thinking.content), None);
        let plan = plan_markers(20, 4, 0, None, &[marker_view(&only_thinking)]);
        assert!(plan.message_anchors.is_empty());
    }

    /// The shape this declaration exists for: a stable system block, an invariant user block
    /// the caller re-sends every round, and a per-round block that is different by
    /// construction. Anchoring the per-round block writes a cache entry no later request can
    /// match, so the declaration withholds exactly that one slot.
    fn split_prefix_and_tail() -> Vec<Message> {
        vec![user("invariant draft snapshot"), user("this round's fold")]
    }

    #[test]
    fn volatile_tail_withholds_the_final_message_anchor() {
        let plan = plan_markers(
            20,
            4,
            1,
            Some(CacheTail::Volatile),
            &marker_views(&split_prefix_and_tail()),
        );

        assert!(
            plan.system_tail,
            "the stable system block still gets its anchor"
        );
        assert_eq!(plan.bridge_message, None);
        assert_eq!(
            plan.message_anchors,
            vec![target(0)],
            "the invariant block is anchored; the declared-volatile final message is not"
        );
    }

    #[test]
    fn absent_tail_declaration_anchors_both_pinned_messages() {
        let plan = plan_markers(20, 4, 1, None, &marker_views(&split_prefix_and_tail()));

        assert!(plan.system_tail);
        assert_eq!(plan.bridge_message, None);
        assert_eq!(
            plan.message_anchors,
            vec![target(0), target(1)],
            "a caller that declares nothing keeps today's placement exactly"
        );
    }

    #[test]
    fn the_withheld_anchor_is_the_last_message_not_a_fixed_index() {
        // One message: the only message IS the volatile one, so no message anchor at all.
        let one = vec![user("the whole prompt")];
        assert_eq!(
            plan_markers(20, 4, 0, Some(CacheTail::Volatile), &marker_views(&one)).message_anchors,
            Vec::<MarkerTarget>::new()
        );

        // Longer: index 1 is nowhere near the end, so it keeps its anchor; the slot that lands
        // on the end of the array (the latest user boundary) is the one withheld.
        let many = vec![
            user("m0"),
            user("m1"),
            msg(Role::Assistant, vec![text("a")]),
            user("u3"),
            user("latest"),
        ];
        let views = marker_views(&many);
        assert_eq!(
            plan_markers(20, 4, 0, None, &views).message_anchors,
            vec![target(0), target(1), target(4)]
        );
        assert_eq!(
            plan_markers(20, 4, 0, Some(CacheTail::Volatile), &views).message_anchors,
            vec![target(0), target(1)]
        );
    }

    #[test]
    fn same_inputs_produce_identical_marker_plan() {
        let messages = vec![
            user("m0"),
            user("m1"),
            msg(Role::Tool, vec![tool_result("t1")]),
            msg(Role::Assistant, vec![text("a"), thinking("hidden")]),
            user("latest"),
        ];

        let first = plan_markers(20, 4, 1, None, &marker_views(&messages));
        let second = plan_markers(20, 4, 1, None, &marker_views(&messages));
        assert_eq!(first, second);
    }
}
