//! Estimates of the memory a value holds, for the memory report and its budget tests.
//!
//! An estimate walks the value when it is asked for, never a counter kept up to date as the
//! value changes: nothing on a step's or an append's path does work for it. It counts:
//! - the value's own inline size (`size_of`), once, by whoever holds it ([`held_bytes`]);
//! - the heap its strings and vectors own, by capacity, not length;
//! - [`JSON_OBJECT_BYTES`] for each object in a `serde_json::Value`, the size of the map node one
//!   parsed JSON object allocates, plus its keys and values.
//!
//! What a value shares behind an `Arc` is counted in full by each holder that reaches it, so an
//! estimate is an upper bound where content is shared. The memory report's budget tests compare
//! each holder's estimate with what the counting allocator measured on the same value.
//!
//! Types declared with the codec's macros (`message!`, `oneof!`, `values!`, the folds' `state!`
//! and `records!`) get their estimate from the same declaration, so a new field is counted
//! without anyone remembering to.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::mem::size_of;
use std::sync::Arc;

use broca_protocol::content::{
    ContentBlock, MediaRef, Message, TextProviderState, ToolCall, ToolOutput, ToolOutputPart,
    ToolResult,
};
use broca_protocol::ids::{MessageId, ToolCallId};
use broca_protocol::request::ModelHandle;

/// The block one parsed JSON object allocates: `serde_json::Map` is a B-tree whose first node,
/// holding up to 11 entries of a `String` key and a `Value`, is about 630 bytes, rounded up by
/// the allocator.
pub const JSON_OBJECT_BYTES: usize = 640;

/// Entries one B-tree node holds at most (`std`'s `B` of 6).
const BTREE_NODE_ENTRIES: usize = 11;

/// The memory a value holds beyond its own inline size.
pub trait HeldBytes {
    /// Heap bytes the value owns: what freeing it would return, less its own `size_of`.
    fn heap_bytes(&self) -> usize;
}

/// What `value` holds in all: its inline size plus [`HeldBytes::heap_bytes`]. For a value held
/// by itself, such as one behind a `Box` or in a map; a struct's field is counted inline by the
/// struct and adds only its heap.
pub fn held_bytes<T: HeldBytes>(value: &T) -> usize {
    size_of::<T>() + value.heap_bytes()
}

macro_rules! no_heap {
    ($($ty:ty),* $(,)?) => {
        $(
            impl HeldBytes for $ty {
                fn heap_bytes(&self) -> usize {
                    0
                }
            }
        )*
    };
}

no_heap!(
    (),
    bool,
    u8,
    u16,
    u32,
    u64,
    u128,
    usize,
    i8,
    i16,
    i32,
    i64,
    isize,
    f32,
    f64,
    std::time::Duration,
    std::time::Instant,
    broca_protocol::usage::Usage,
    broca_protocol::content::Role,
    broca_protocol::content::MediaKind,
    broca_protocol::content::EncryptedReasoningFormat,
    broca_protocol::content::TextProviderFormat,
    broca_protocol::content::FinishReason,
    broca_protocol::caller::RunFinishReason,
    broca_protocol::caller::PauseReason,
);

impl HeldBytes for String {
    fn heap_bytes(&self) -> usize {
        self.capacity()
    }
}

impl HeldBytes for str {
    fn heap_bytes(&self) -> usize {
        0
    }
}

impl<T: HeldBytes> HeldBytes for Vec<T> {
    fn heap_bytes(&self) -> usize {
        self.capacity() * size_of::<T>() + self.iter().map(T::heap_bytes).sum::<usize>()
    }
}

impl<T: HeldBytes> HeldBytes for VecDeque<T> {
    fn heap_bytes(&self) -> usize {
        self.capacity() * size_of::<T>() + self.iter().map(T::heap_bytes).sum::<usize>()
    }
}

impl<T: HeldBytes> HeldBytes for [T] {
    fn heap_bytes(&self) -> usize {
        self.iter().map(T::heap_bytes).sum()
    }
}

impl<T: HeldBytes> HeldBytes for Option<T> {
    fn heap_bytes(&self) -> usize {
        self.as_ref().map_or(0, T::heap_bytes)
    }
}

impl<T: HeldBytes> HeldBytes for Box<T> {
    fn heap_bytes(&self) -> usize {
        held_bytes(&**self)
    }
}

/// An `Arc` counts its pointee in full, with the two reference counts beside it: an estimate of
/// what the holder keeps alive, which other holders may share.
impl<T: HeldBytes> HeldBytes for Arc<T> {
    fn heap_bytes(&self) -> usize {
        2 * size_of::<usize>() + held_bytes(&**self)
    }
}

impl<A: HeldBytes, B: HeldBytes> HeldBytes for (A, B) {
    fn heap_bytes(&self) -> usize {
        self.0.heap_bytes() + self.1.heap_bytes()
    }
}

impl<A: HeldBytes, B: HeldBytes, C: HeldBytes> HeldBytes for (A, B, C) {
    fn heap_bytes(&self) -> usize {
        self.0.heap_bytes() + self.1.heap_bytes() + self.2.heap_bytes()
    }
}

impl<const N: usize> HeldBytes for [u8; N] {
    fn heap_bytes(&self) -> usize {
        0
    }
}

/// The nodes of a B-tree of `len` entries of `entry` bytes each. Entries arrive mostly in key
/// order (seqs, sorted JSON keys): each split then leaves a node with 6 entries and moves one up,
/// so a node stands for about 7 entries at every level.
fn btree_bytes(len: usize, entry: usize) -> usize {
    const PER_NODE: usize = 7;
    if len == 0 {
        return 0;
    }
    // A leaf: its entries, a parent pointer, its place in the parent and its length.
    let leaf = BTREE_NODE_ENTRIES * entry + 16;
    if len <= BTREE_NODE_ENTRIES {
        return leaf;
    }
    // An internal node is a leaf with an edge per child.
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

impl<K: HeldBytes, V: HeldBytes> HeldBytes for BTreeMap<K, V> {
    fn heap_bytes(&self) -> usize {
        btree_bytes(self.len(), size_of::<K>() + size_of::<V>())
            + self
                .iter()
                .map(|(key, value)| key.heap_bytes() + value.heap_bytes())
                .sum::<usize>()
    }
}

impl<T: HeldBytes> HeldBytes for BTreeSet<T> {
    fn heap_bytes(&self) -> usize {
        btree_bytes(self.len(), size_of::<T>()) + self.iter().map(T::heap_bytes).sum::<usize>()
    }
}

/// The table a hash map of `capacity` usable slots allocates: a power of two of buckets, one
/// control byte for each and a group's worth more.
fn hash_table_bytes(capacity: usize, entry: usize) -> usize {
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

impl<K: HeldBytes, V: HeldBytes, S> HeldBytes for HashMap<K, V, S> {
    fn heap_bytes(&self) -> usize {
        hash_table_bytes(self.capacity(), size_of::<K>() + size_of::<V>())
            + self
                .iter()
                .map(|(key, value)| key.heap_bytes() + value.heap_bytes())
                .sum::<usize>()
    }
}

impl<T: HeldBytes, S> HeldBytes for HashSet<T, S> {
    fn heap_bytes(&self) -> usize {
        hash_table_bytes(self.capacity(), size_of::<T>())
            + self.iter().map(T::heap_bytes).sum::<usize>()
    }
}

impl HeldBytes for serde_json::Value {
    fn heap_bytes(&self) -> usize {
        match self {
            serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {
                0
            }
            serde_json::Value::String(text) => text.capacity(),
            serde_json::Value::Array(items) => items.heap_bytes(),
            serde_json::Value::Object(map) => map.heap_bytes(),
        }
    }
}

impl HeldBytes for serde_json::Map<String, serde_json::Value> {
    fn heap_bytes(&self) -> usize {
        let nodes = if self.is_empty() {
            0
        } else if self.len() <= BTREE_NODE_ENTRIES {
            JSON_OBJECT_BYTES
        } else {
            btree_bytes(
                self.len(),
                size_of::<String>() + size_of::<serde_json::Value>(),
            )
        };
        nodes
            + self
                .iter()
                .map(|(key, value)| key.heap_bytes() + value.heap_bytes())
                .sum::<usize>()
    }
}

// The protocol's content types, which records and folds hold. Each destructures its type in
// full, so a new field fails to compile here until it is counted.

impl HeldBytes for ToolCallId {
    fn heap_bytes(&self) -> usize {
        self.0.heap_bytes()
    }
}

impl HeldBytes for MessageId {
    fn heap_bytes(&self) -> usize {
        self.0.heap_bytes()
    }
}

impl HeldBytes for ModelHandle {
    fn heap_bytes(&self) -> usize {
        let ModelHandle {
            provider_module_id,
            model_id,
        } = self;
        provider_module_id.heap_bytes() + model_id.heap_bytes()
    }
}

impl HeldBytes for MediaRef {
    fn heap_bytes(&self) -> usize {
        let MediaRef {
            kind: _,
            mime,
            byte_size: _,
            sha256,
            width: _,
            height: _,
        } = self;
        mime.heap_bytes() + sha256.heap_bytes()
    }
}

impl HeldBytes for TextProviderState {
    fn heap_bytes(&self) -> usize {
        let TextProviderState { format: _, data } = self;
        data.heap_bytes()
    }
}

impl HeldBytes for ToolCall {
    fn heap_bytes(&self) -> usize {
        let ToolCall {
            tool_call_id,
            tool_name,
            input,
            thought_signature,
        } = self;
        tool_call_id.heap_bytes()
            + tool_name.heap_bytes()
            + input.heap_bytes()
            + thought_signature.heap_bytes()
    }
}

impl HeldBytes for ToolOutputPart {
    fn heap_bytes(&self) -> usize {
        match self {
            ToolOutputPart::Text { text } => text.heap_bytes(),
            ToolOutputPart::Media { media } => media.heap_bytes(),
        }
    }
}

impl HeldBytes for ToolOutput {
    fn heap_bytes(&self) -> usize {
        match self {
            ToolOutput::Text { text } => text.heap_bytes(),
            ToolOutput::Json { value } => value.heap_bytes(),
            ToolOutput::Parts { parts } => parts.heap_bytes(),
            ToolOutput::Denied { reason } => reason.heap_bytes(),
            ToolOutput::Error {
                message,
                module,
                code,
                detail,
                nothing_sent: _,
                retry,
            } => {
                message.heap_bytes()
                    + module.heap_bytes()
                    + code.heap_bytes()
                    + detail.heap_bytes()
                    + retry.heap_bytes()
            }
        }
    }
}

impl HeldBytes for ToolResult {
    fn heap_bytes(&self) -> usize {
        let ToolResult {
            tool_call_id,
            output,
            is_error: _,
        } = self;
        tool_call_id.heap_bytes() + output.heap_bytes()
    }
}

impl HeldBytes for ContentBlock {
    fn heap_bytes(&self) -> usize {
        match self {
            ContentBlock::Media(media) => media.heap_bytes(),
            ContentBlock::Text { text } => text.heap_bytes(),
            ContentBlock::TextWithState {
                text,
                provider_state,
            } => text.heap_bytes() + provider_state.heap_bytes(),
            ContentBlock::Reasoning { text, signature } => {
                text.heap_bytes() + signature.heap_bytes()
            }
            ContentBlock::RedactedReasoning {
                data,
                provider_format: _,
            } => data.heap_bytes(),
            ContentBlock::ToolCall(call) => call.heap_bytes(),
            ContentBlock::ToolResult(result) => result.heap_bytes(),
            ContentBlock::ProviderFallback {
                from_model,
                to_model,
                raw,
            } => from_model.heap_bytes() + to_model.heap_bytes() + raw.heap_bytes(),
        }
    }
}

impl HeldBytes for broca_protocol::tags::PartTags {
    fn heap_bytes(&self) -> usize {
        self.parts.heap_bytes()
    }
}

impl HeldBytes for Message {
    fn heap_bytes(&self) -> usize {
        let Message {
            role: _,
            content,
            turn_system: _,
            cache_prefix_blocks: _,
            origin,
            ordinal: _,
            tags: _,
        } = self;
        // A message's part tags are shared with the prompt that stamped them; a stored message
        // has none.
        let tags = self.part_tags().map_or(0, |tags| {
            2 * size_of::<usize>() + size_of_val(tags) + tags.heap_bytes()
        });
        content.heap_bytes() + origin.heap_bytes() + tags
    }
}

impl HeldBytes for broca_protocol::content::AssistantMessage {
    fn heap_bytes(&self) -> usize {
        let broca_protocol::content::AssistantMessage {
            message_id,
            content,
        } = self;
        message_id.heap_bytes() + content.heap_bytes()
    }
}

impl HeldBytes for crate::codec::ByteBuf {
    fn heap_bytes(&self) -> usize {
        self.0.capacity()
    }
}

no_heap!(crate::codec::Digest, crate::codec::Id128);

/// An estimate of the heap a value of another crate's type holds, from its `Serialize` walk, for
/// types this crate can't give an estimator of their own. A string or byte string holds its
/// length; a sequence holds 32 bytes per element (a `serde_json::Value`'s size, which most such
/// sequences hold) and its elements' heap; a map holds its nodes as a parsed JSON object does
/// ([`JSON_OBJECT_BYTES`] up to 11 entries) and its keys' and values' heap; a struct, tuple or
/// enum holds its fields' heap. It can't see what the walk skips: a field not serialized, a
/// vector's spare capacity, a box around an inline value.
pub fn serde_heap_bytes<T: serde::Serialize + ?Sized>(value: &T) -> usize {
    value.serialize(serde_walk::Walk).unwrap_or(0)
}

mod serde_walk {
    use serde::ser::{
        Error, Serialize, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant,
        SerializeTuple, SerializeTupleStruct, SerializeTupleVariant, Serializer,
    };

    /// The walk never fails; its error type exists because `Serializer` needs one.
    #[derive(Debug)]
    pub struct Never;

    impl std::fmt::Display for Never {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("never")
        }
    }

    impl std::error::Error for Never {}

    impl Error for Never {
        fn custom<T: std::fmt::Display>(_: T) -> Self {
            Never
        }
    }

    pub struct Walk;

    /// The heap of a sequence, tuple or struct being walked: its elements' heap, and `per_item`
    /// bytes for each element's own slot.
    pub struct Sum {
        heap: usize,
        items: usize,
        per_item: usize,
    }

    impl Sum {
        fn new(per_item: usize) -> Self {
            Self {
                heap: 0,
                items: 0,
                per_item,
            }
        }

        fn add<T: Serialize + ?Sized>(&mut self, value: &T) {
            self.heap += value.serialize(Walk).unwrap_or(0);
            self.items += 1;
        }

        fn done(self) -> usize {
            self.heap + self.items * self.per_item
        }
    }

    pub struct Map {
        heap: usize,
        entries: usize,
    }

    /// A sequence's elements are counted as `serde_json::Value`s.
    const SEQ_ITEM: usize = 32;

    macro_rules! no_heap {
        ($($name:ident: $ty:ty),*) => {
            $(fn $name(self, _: $ty) -> Result<usize, Never> { Ok(0) })*
        };
    }

    impl Serializer for Walk {
        type Ok = usize;
        type Error = Never;
        type SerializeSeq = Sum;
        type SerializeTuple = Sum;
        type SerializeTupleStruct = Sum;
        type SerializeTupleVariant = Sum;
        type SerializeMap = Map;
        type SerializeStruct = Sum;
        type SerializeStructVariant = Sum;

        no_heap!(
            serialize_bool: bool, serialize_i8: i8, serialize_i16: i16, serialize_i32: i32,
            serialize_i64: i64, serialize_u8: u8, serialize_u16: u16, serialize_u32: u32,
            serialize_u64: u64, serialize_f32: f32, serialize_f64: f64, serialize_char: char
        );

        fn serialize_str(self, value: &str) -> Result<usize, Never> {
            Ok(value.len())
        }
        fn serialize_bytes(self, value: &[u8]) -> Result<usize, Never> {
            Ok(value.len())
        }
        fn serialize_none(self) -> Result<usize, Never> {
            Ok(0)
        }
        fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<usize, Never> {
            value.serialize(self)
        }
        fn serialize_unit(self) -> Result<usize, Never> {
            Ok(0)
        }
        fn serialize_unit_struct(self, _: &'static str) -> Result<usize, Never> {
            Ok(0)
        }
        fn serialize_unit_variant(
            self,
            _: &'static str,
            _: u32,
            _: &'static str,
        ) -> Result<usize, Never> {
            Ok(0)
        }
        fn serialize_newtype_struct<T: Serialize + ?Sized>(
            self,
            _: &'static str,
            value: &T,
        ) -> Result<usize, Never> {
            value.serialize(self)
        }
        fn serialize_newtype_variant<T: Serialize + ?Sized>(
            self,
            _: &'static str,
            _: u32,
            _: &'static str,
            value: &T,
        ) -> Result<usize, Never> {
            value.serialize(self)
        }
        fn serialize_seq(self, _: Option<usize>) -> Result<Sum, Never> {
            Ok(Sum::new(SEQ_ITEM))
        }
        fn serialize_tuple(self, _: usize) -> Result<Sum, Never> {
            Ok(Sum::new(0))
        }
        fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Sum, Never> {
            Ok(Sum::new(0))
        }
        fn serialize_tuple_variant(
            self,
            _: &'static str,
            _: u32,
            _: &'static str,
            _: usize,
        ) -> Result<Sum, Never> {
            Ok(Sum::new(0))
        }
        fn serialize_map(self, _: Option<usize>) -> Result<Map, Never> {
            Ok(Map {
                heap: 0,
                entries: 0,
            })
        }
        fn serialize_struct(self, _: &'static str, _: usize) -> Result<Sum, Never> {
            Ok(Sum::new(0))
        }
        fn serialize_struct_variant(
            self,
            _: &'static str,
            _: u32,
            _: &'static str,
            _: usize,
        ) -> Result<Sum, Never> {
            Ok(Sum::new(0))
        }
    }

    impl SerializeSeq for Sum {
        type Ok = usize;
        type Error = Never;
        fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Never> {
            self.add(value);
            Ok(())
        }
        fn end(self) -> Result<usize, Never> {
            Ok(self.done())
        }
    }

    impl SerializeTuple for Sum {
        type Ok = usize;
        type Error = Never;
        fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Never> {
            self.add(value);
            Ok(())
        }
        fn end(self) -> Result<usize, Never> {
            Ok(self.done())
        }
    }

    impl SerializeTupleStruct for Sum {
        type Ok = usize;
        type Error = Never;
        fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Never> {
            self.add(value);
            Ok(())
        }
        fn end(self) -> Result<usize, Never> {
            Ok(self.done())
        }
    }

    impl SerializeTupleVariant for Sum {
        type Ok = usize;
        type Error = Never;
        fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Never> {
            self.add(value);
            Ok(())
        }
        fn end(self) -> Result<usize, Never> {
            Ok(self.done())
        }
    }

    impl SerializeStruct for Sum {
        type Ok = usize;
        type Error = Never;
        fn serialize_field<T: Serialize + ?Sized>(
            &mut self,
            _: &'static str,
            value: &T,
        ) -> Result<(), Never> {
            self.add(value);
            Ok(())
        }
        fn end(self) -> Result<usize, Never> {
            Ok(self.done())
        }
    }

    impl SerializeStructVariant for Sum {
        type Ok = usize;
        type Error = Never;
        fn serialize_field<T: Serialize + ?Sized>(
            &mut self,
            _: &'static str,
            value: &T,
        ) -> Result<(), Never> {
            self.add(value);
            Ok(())
        }
        fn end(self) -> Result<usize, Never> {
            Ok(self.done())
        }
    }

    impl SerializeMap for Map {
        type Ok = usize;
        type Error = Never;
        fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Never> {
            self.heap += key.serialize(Walk).unwrap_or(0);
            self.entries += 1;
            Ok(())
        }
        fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Never> {
            self.heap += value.serialize(Walk).unwrap_or(0);
            Ok(())
        }
        fn end(self) -> Result<usize, Never> {
            let nodes = match self.entries {
                0 => 0,
                n if n <= super::BTREE_NODE_ENTRIES => super::JSON_OBJECT_BYTES,
                n => super::btree_bytes(
                    n,
                    std::mem::size_of::<String>() + std::mem::size_of::<serde_json::Value>(),
                ),
            };
            Ok(nodes + self.heap)
        }
    }
}

#[cfg(test)]
mod tests;
