//! Values many records of a session repeat, held once in memory.
//!
//! A message's facts repeat its run's id, its sender, how it was delivered, the view it was
//! written under and its run's Rich UI stamp, and each tool call's facts repeat the module it
//! went to, the runner that minted its key and its schema pin. Held as their own copies they cost
//! as much as a small message. In memory they are held once instead:
//!
//! - [`Interned`]: text and small values a session repeats, which its [`Interner`] holds one copy
//!   of. A value read or built elsewhere is an `Interned` of its own until a session interns it.
//! - [`SharedJson`]: a JSON value held once per process for every holder of the same text: a
//!   session has a few dozen distinct schema pins and send marks, not one per call.
//!
//! Neither changes a stored byte: each is written exactly as the value it holds.

use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::{Arc, Mutex, Weak};

use serde::{Serialize, Serializer};

use crate::codec::{len_payload, DecodeError, Raw, Value, Wire};
use crate::record::{EntryFacts, Notice, Record, RichUi, TurnFacts, ViewRef};

/// A value a session holds one copy of for all its records (see the module's notes). Compared,
/// hashed, written and serialized as the value itself.
pub struct Interned<T: ?Sized>(Arc<T>);

impl<T: ?Sized> Clone for Interned<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T: ?Sized> Interned<T> {
    /// Whether `a` and `b` are the one copy, not only equal.
    pub fn same(a: &Self, b: &Self) -> bool {
        Arc::ptr_eq(&a.0, &b.0)
    }
}

impl<T: ?Sized> Deref for Interned<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T: ?Sized> Borrow<T> for Interned<T> {
    fn borrow(&self) -> &T {
        &self.0
    }
}

impl<T: ?Sized + PartialEq> PartialEq for Interned<T> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0) || *self.0 == *other.0
    }
}

impl<T: ?Sized + Eq> Eq for Interned<T> {}

impl<T: ?Sized + Hash> Hash for Interned<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (*self.0).hash(state);
    }
}

impl<T: ?Sized + std::fmt::Debug> std::fmt::Debug for Interned<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (*self.0).fmt(f)
    }
}

impl<T: ?Sized + std::fmt::Display> std::fmt::Display for Interned<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (*self.0).fmt(f)
    }
}

impl<T: ?Sized + Serialize> Serialize for Interned<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        (*self.0).serialize(serializer)
    }
}

impl<T> From<T> for Interned<T> {
    fn from(value: T) -> Self {
        Self(Arc::new(value))
    }
}

impl From<String> for Interned<str> {
    fn from(value: String) -> Self {
        Self(Arc::from(value))
    }
}

impl From<&str> for Interned<str> {
    fn from(value: &str) -> Self {
        Self(Arc::from(value))
    }
}

impl From<&String> for Interned<str> {
    fn from(value: &String) -> Self {
        Self(Arc::from(value.as_str()))
    }
}

impl<T: Value> Value for Interned<T> {
    const WIRE: Wire = T::WIRE;
    const NAME: &'static str = T::NAME;
    fn write(&self, out: &mut Vec<u8>) {
        (*self.0).write(out);
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        T::read(raw).map(Self::from)
    }
    fn is_default(&self) -> bool {
        (*self.0).is_default()
    }
    fn missing() -> Option<Self> {
        T::missing().map(Self::from)
    }
}

impl Value for Interned<str> {
    const WIRE: Wire = Wire::Len;
    const NAME: &'static str = <String as Value>::NAME;
    fn write(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.0.as_bytes());
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        let text = String::read(raw)?;
        Ok(Self::from(text))
    }
    fn is_default(&self) -> bool {
        self.0.is_empty()
    }
    fn missing() -> Option<Self> {
        Some(Self::from(""))
    }
}

/// An interned value is the session's [`Interner`]'s to count, once, not each holder's.
impl<T: ?Sized> crate::held::HeldBytes for Interned<T> {
    fn heap_bytes(&self) -> usize {
        0
    }
}

/// The values a session holds one copy of (see the module's notes). Records are interned as the
/// session loads or appends them, before the conversation fold and the kept records share them.
/// A value no record holds any longer leaves the next time the interner has doubled in size.
#[derive(Default)]
pub(crate) struct Interner {
    text: HashSet<Interned<str>>,
    notices: HashSet<Interned<Notice>>,
    views: HashSet<Interned<ViewRef>>,
    rich_ui: HashSet<Interned<RichUi>>,
    /// How many values the interner held after it last let go of the unused ones.
    kept_at_prune: usize,
}

/// Replaces `value` with the copy `held` keeps of it, or keeps it there as the copy.
fn intern<T: ?Sized + Hash + Eq>(held: &mut HashSet<Interned<T>>, value: &mut Interned<T>) {
    match held.get(&*value.0) {
        Some(copy) => {
            if !Arc::ptr_eq(&copy.0, &value.0) {
                *value = copy.clone();
            }
        }
        None => {
            held.insert(value.clone());
        }
    }
}

fn intern_opt<T: ?Sized + Hash + Eq>(
    held: &mut HashSet<Interned<T>>,
    value: &mut Option<Interned<T>>,
) {
    if let Some(value) = value {
        intern(held, value);
    }
}

impl Interner {
    /// Interns what `record` repeats, in place. A record shared with another holder is copied
    /// first, so only records not yet shared should come here.
    pub(crate) fn record(&mut self, record: &mut Record) {
        match record {
            Record::EntryFacts(facts) => self.facts(Arc::make_mut(facts)),
            Record::TurnFacts(turn) => self.turn(Arc::make_mut(turn)),
            _ => return,
        }
        self.prune();
    }

    /// `record` with what it repeats interned, when it holds anything to intern: the copy a
    /// session folds and keeps of a record its caller appended.
    pub(crate) fn interned(&mut self, record: &Record) -> Option<Record> {
        let mut record = match record {
            Record::EntryFacts(facts) => Record::EntryFacts(Arc::new(EntryFacts::clone(facts))),
            Record::TurnFacts(turn) => Record::TurnFacts(Arc::new(TurnFacts::clone(turn))),
            _ => return None,
        };
        self.record(&mut record);
        Some(record)
    }

    fn facts(&mut self, facts: &mut EntryFacts) {
        intern_opt(&mut self.notices, &mut facts.notice);
        intern_opt(&mut self.views, &mut facts.generated_under);
        intern_opt(&mut self.text, &mut facts.run_id);
        intern_opt(&mut self.text, &mut facts.delivery_mode);
        intern_opt(&mut self.text, &mut facts.sender);
        intern_opt(&mut self.rich_ui, &mut facts.rich_ui);
    }

    fn turn(&mut self, turn: &mut TurnFacts) {
        for call in &mut turn.calls {
            intern_opt(&mut self.text, &mut call.carrier);
            intern_opt(&mut self.text, &mut call.dispatched_to);
        }
    }

    fn len(&self) -> usize {
        self.text.len() + self.notices.len() + self.views.len() + self.rich_ui.len()
    }

    /// Lets go of the values only the interner still holds, once it holds twice as many as after
    /// it last did, so a long session's interner keeps only what its records still use.
    fn prune(&mut self) {
        if self.len() < 2 * self.kept_at_prune + 64 {
            return;
        }
        self.text.retain(|value| Arc::strong_count(&value.0) > 1);
        self.notices.retain(|value| Arc::strong_count(&value.0) > 1);
        self.views.retain(|value| Arc::strong_count(&value.0) > 1);
        self.rich_ui.retain(|value| Arc::strong_count(&value.0) > 1);
        self.kept_at_prune = self.len();
    }
}

impl crate::held::HeldBytes for Interner {
    fn heap_bytes(&self) -> usize {
        use crate::held::{held_bytes, HeldBytes};
        /// What one interned value holds: its `Arc`'s counts and the value.
        fn held<T: HeldBytes>(set: &HashSet<Interned<T>>) -> usize {
            set.capacity() * (std::mem::size_of::<Interned<T>>() + 1)
                + set
                    .iter()
                    .map(|value| 2 * std::mem::size_of::<usize>() + held_bytes(&*value.0))
                    .sum::<usize>()
        }
        let text = self.text.capacity() * (std::mem::size_of::<Interned<str>>() + 1)
            + self
                .text
                .iter()
                .map(|value| 2 * std::mem::size_of::<usize>() + value.len())
                .sum::<usize>();
        text + held(&self.notices) + held(&self.views) + held(&self.rich_ui)
    }
}

/// A JSON value held once per process for every holder of the same compact text (see the
/// module's notes). Compared, written and serialized as the value; reading a field parses
/// nothing, the value is already parsed.
#[derive(Clone)]
pub struct SharedJson(Arc<serde_json::Value>);

/// Every [`SharedJson`] value alive, by its compact text. Values leave when their last holder
/// drops them: the table holds them weakly, and lets go of the dead entries each time it has
/// doubled since it last did.
#[derive(Default)]
struct JsonTable {
    by_text: HashMap<Box<[u8]>, Weak<serde_json::Value>>,
    live_at_prune: usize,
}

fn json_table() -> &'static Mutex<JsonTable> {
    static TABLE: std::sync::OnceLock<Mutex<JsonTable>> = std::sync::OnceLock::new();
    TABLE.get_or_init(Default::default)
}

impl SharedJson {
    /// The process's value for the compact text `text`, or `None` when no holder holds it.
    fn held(table: &JsonTable, text: &[u8]) -> Option<Self> {
        table.by_text.get(text).and_then(Weak::upgrade).map(Self)
    }

    /// Makes `value`, whose compact text is `text`, the process's value for it, unless another
    /// holder made one first.
    fn hold(text: Vec<u8>, value: serde_json::Value) -> Self {
        let mut table = json_table()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if let Some(held) = Self::held(&table, &text) {
            return held;
        }
        let value = Arc::new(value);
        table
            .by_text
            .insert(text.into_boxed_slice(), Arc::downgrade(&value));
        if table.by_text.len() >= 2 * table.live_at_prune + 64 {
            table.by_text.retain(|_, value| value.strong_count() > 0);
            table.live_at_prune = table.by_text.len();
        }
        Self(value)
    }

    /// The shared value equal to `value`.
    pub fn new(value: serde_json::Value) -> Self {
        let text = serde_json::to_vec(&value).expect("a JSON value always serializes");
        Self::hold(text, value)
    }

    /// The value, for a reader that needs it as its own.
    pub fn to_value(&self) -> serde_json::Value {
        (*self.0).clone()
    }

    /// Whether `a` and `b` are the one copy, not only equal.
    pub fn same(a: &Self, b: &Self) -> bool {
        Arc::ptr_eq(&a.0, &b.0)
    }
}

impl From<serde_json::Value> for SharedJson {
    fn from(value: serde_json::Value) -> Self {
        Self::new(value)
    }
}

impl Deref for SharedJson {
    type Target = serde_json::Value;
    fn deref(&self) -> &serde_json::Value {
        &self.0
    }
}

impl PartialEq for SharedJson {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0) || *self.0 == *other.0
    }
}

impl std::fmt::Debug for SharedJson {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (*self.0).fmt(f)
    }
}

impl Serialize for SharedJson {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        (*self.0).serialize(serializer)
    }
}

/// Written as the value's compact text, as a `serde_json::Value` is. Reading text a holder
/// already holds parses nothing: equal text is the same value, already checked to be compact.
impl Value for SharedJson {
    const WIRE: Wire = Wire::Len;
    const NAME: &'static str = <serde_json::Value as Value>::NAME;
    fn write(&self, out: &mut Vec<u8>) {
        (*self.0).write(out);
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        let text = len_payload(raw, Self::NAME)?;
        {
            let table = json_table()
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if let Some(held) = Self::held(&table, text) {
                return Ok(held);
            }
        }
        let value = serde_json::Value::read(raw)?;
        Ok(Self::hold(text.to_vec(), value))
    }
}

/// A shared value is held once per process, not by any one holder.
impl crate::held::HeldBytes for SharedJson {
    fn heap_bytes(&self) -> usize {
        0
    }
}

#[cfg(test)]
mod tests;
