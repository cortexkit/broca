//! An estimate of what the engine holds, for the operator's memory report.
//!
//! It is made the way `broca_session_store::held` estimates the store's own values, walked when
//! asked and never kept up to date on the append path: the value's inline size once, the heap
//! its strings and vectors own by capacity, a hash table by its buckets and control bytes, and a
//! B-tree by its nodes. That module can't be used here (the store doesn't depend on this crate,
//! nor this crate on it), so its formulas are repeated below, for this crate's types and the
//! engine's owners' (`broca_provider`'s aging state).

use std::collections::{BTreeMap, HashMap};
use std::mem::size_of;

use broca_protocol::{ContentBlock, MediaRef, Message, ToolOutput, ToolOutputPart};

use super::{Capture, MediaBinding, MediaChange, MediaId, SessionMedia};

/// Entries one B-tree node holds at most (`std`'s `B` of 6).
const BTREE_NODE_ENTRIES: usize = 11;

/// The nodes of a B-tree of `len` entries of `entry` bytes each, filled mostly in key order:
/// a node then stands for about 7 entries at every level.
pub fn btree_bytes(len: usize, entry: usize) -> usize {
    const PER_NODE: usize = 7;
    if len == 0 {
        return 0;
    }
    let leaf = BTREE_NODE_ENTRIES * entry + 16;
    if len <= BTREE_NODE_ENTRIES {
        return leaf;
    }
    let internal = leaf + (BTREE_NODE_ENTRIES + 1) * size_of::<usize>();
    let leaves = (len + 1).div_ceil(PER_NODE);
    let mut bytes = leaves * leaf;
    let mut level = leaves;
    while level > 1 {
        level = level.div_ceil(PER_NODE);
        bytes += level * internal;
    }
    bytes
}

/// The table a hash map of `capacity` usable slots allocates: a power of two of buckets, one
/// control byte for each and a group's worth more.
pub fn hash_table_bytes(capacity: usize, entry: usize) -> usize {
    if capacity == 0 {
        return 0;
    }
    let buckets = if capacity < 8 {
        (capacity + 1).next_power_of_two().max(4)
    } else {
        (capacity * 8 / 7).next_power_of_two()
    };
    buckets * entry + buckets + 16
}

pub fn hash_map<K, V>(map: &HashMap<K, V>, heap: impl Fn(&K, &V) -> usize) -> usize {
    hash_table_bytes(map.capacity(), size_of::<K>() + size_of::<V>())
        + map
            .iter()
            .map(|(key, value)| heap(key, value))
            .sum::<usize>()
}

pub fn btree_map<K, V>(map: &BTreeMap<K, V>, heap: impl Fn(&K, &V) -> usize) -> usize {
    btree_bytes(map.len(), size_of::<K>() + size_of::<V>())
        + map
            .iter()
            .map(|(key, value)| heap(key, value))
            .sum::<usize>()
}

pub fn vec<T>(items: &Vec<T>, heap: impl Fn(&T) -> usize) -> usize {
    items.capacity() * size_of::<T>() + items.iter().map(heap).sum::<usize>()
}

fn ids(ids: &Vec<MediaId>) -> usize {
    vec(ids, |_| 0)
}

pub fn media_ref(reference: &MediaRef) -> usize {
    reference.mime.capacity() + reference.sha256.capacity()
}

fn binding(binding: &MediaBinding) -> usize {
    media_ref(&binding.reference)
}

/// A message skeleton: text blocks left empty, media blocks, and tool results whose parts are
/// empty text or media. A skeleton has no origin and no tags.
fn message(message: &Message) -> usize {
    vec(&message.content, |block| match block {
        ContentBlock::Text { text } => text.capacity(),
        ContentBlock::Media(reference) => media_ref(reference),
        ContentBlock::ToolResult(result) => {
            result.tool_call_id.0.capacity()
                + match &result.output {
                    ToolOutput::Parts { parts } => vec(parts, |part| match part {
                        ToolOutputPart::Text { text } => text.capacity(),
                        ToolOutputPart::Media { media } => media_ref(media),
                    }),
                    _ => 0,
                }
        }
        _ => 0,
    })
}

fn capture(capture: &Capture) -> usize {
    media_ref(&capture.reference)
        + capture
            .tool_call
            .as_ref()
            .map_or(0, |call| call.0.capacity())
}

impl SessionMedia {
    /// What the engine holds in all: its inline size and [`SessionMedia::held_parts`].
    pub fn held_bytes(&self) -> usize {
        size_of::<Self>()
            + self
                .held_parts()
                .iter()
                .map(|(_, bytes)| bytes)
                .sum::<usize>()
    }

    /// The heap the engine holds, by part: its captures, its lookups by digest, source,
    /// position and tool call, the captures waiting for or holding protection, the messages it
    /// keeps as skeletons, the references it may render, and what a converted session's first
    /// compaction left. The bytes source it reads through is its owner's, and counted there.
    /// The old protocol's compaction state is left out but for its captured references: on
    /// media events it is never set.
    pub fn held_parts(&self) -> Vec<(&'static str, usize)> {
        vec![
            (
                "captures",
                hash_map(&self.captures, |_, value| capture(value)),
            ),
            (
                "by digest",
                hash_map(&self.digests, |key, value| key.capacity() + ids(value)),
            ),
            (
                "by source",
                hash_map(&self.sources, |key, value| key.capacity() + ids(value)),
            ),
            (
                "by position",
                hash_map(&self.positions, |(_, call, digest), value| {
                    call.as_ref().map_or(0, |call| call.0.capacity())
                        + digest.capacity()
                        + ids(value)
                }),
            ),
            (
                "by tool call",
                hash_map(&self.tools, |(call, _), value| {
                    call.0.capacity() + ids(value)
                }),
            ),
            (
                "pending, held, protected",
                ids(&self.pending) + vec(&self.steers, ids) + ids(&self.protected),
            ),
            (
                "changes",
                vec(&self.changes, |change| match change {
                    MediaChange::Show(shown) => binding(shown),
                    MediaChange::Release(_) => 0,
                    MediaChange::View(view) => vec(view, binding),
                }),
            ),
            (
                "placed skeletons",
                btree_map(&self.skeleton, |_, value| message(value)),
            ),
            (
                "held skeletons",
                hash_map(&self.held_messages, |_, value| message(value)),
            ),
            (
                "renderable references",
                self.compaction.captured.heap_bytes(),
            ),
            (
                "batch order",
                hash_map(&self.batch_order, |key, _| key.0.capacity()),
            ),
            (
                "restored compaction",
                vec(&self.replacements, |(references, _, _)| {
                    vec(references, media_ref)
                }) + self
                    .restored_selection
                    .as_ref()
                    .map_or(0, |selection| vec(selection, binding))
                    + self
                        .read_only
                        .as_ref()
                        .map_or(0, |(root, _)| root.as_os_str().len()),
            ),
        ]
    }
}
