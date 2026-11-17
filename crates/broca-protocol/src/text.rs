//! Message text held once and shared.
//!
//! A message's text (its text blocks, reasoning, signatures, opaque provider state and tool output
//! text) is the bulk of what a session holds, and several holders keep the same message: the
//! session's conversation, a running run's own copy of it, the run's stored prompt, its rendered
//! prompt cache and the records kept in memory. Held as `String`, each holder copies the text.
//! [`Text`] holds it once: cloning it copies a pointer, and every holder shares one allocation.
//!
//! It is immutable. A writer that changes text builds a new one ([`Text::appended`]).
//! Serializing a `Text` writes the string it holds, exactly as a `String` field did, so stored
//! bytes, request bytes and replies don't change.

use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;
use std::sync::{Arc, LazyLock};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Shared, immutable UTF-8 text. See the module documentation.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Text(Arc<str>);

impl Text {
    /// The text.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The text, shared: another holder can keep it without a copy.
    pub fn shared(&self) -> Arc<str> {
        Arc::clone(&self.0)
    }

    /// This text followed by `more`, as a new text.
    pub fn appended(&self, more: &str) -> Self {
        let mut joined = String::with_capacity(self.0.len() + more.len());
        joined.push_str(&self.0);
        joined.push_str(more);
        Self::from(joined)
    }

    /// Bytes held for the text, for memory accounting. Holders that share one text each count
    /// it, as they did when each held a copy.
    pub fn held_bytes(&self) -> usize {
        self.0.len()
    }

    /// Whether another holder shares this text's allocation now.
    pub fn is_shared(&self) -> bool {
        Arc::strong_count(&self.0) > 1
    }
}

/// The one empty text every empty `Text` shares. Empty text is common (media skeletons, released
/// parts, empty replies), and an `Arc<str>` allocates its counts even for no bytes.
static EMPTY: LazyLock<Arc<str>> = LazyLock::new(|| Arc::from(""));

impl Text {
    fn new(text: &str) -> Self {
        if text.is_empty() {
            Self::default()
        } else {
            Self(Arc::from(text))
        }
    }
}

impl Default for Text {
    fn default() -> Self {
        Self(Arc::clone(&EMPTY))
    }
}

impl Deref for Text {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for Text {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for Text {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl From<String> for Text {
    fn from(text: String) -> Self {
        if text.is_empty() {
            Self::default()
        } else {
            Self(Arc::from(text))
        }
    }
}

impl From<&String> for Text {
    fn from(text: &String) -> Self {
        Self::new(text)
    }
}

impl From<&str> for Text {
    fn from(text: &str) -> Self {
        Self::new(text)
    }
}

impl From<Arc<str>> for Text {
    fn from(text: Arc<str>) -> Self {
        Self(text)
    }
}

impl From<std::borrow::Cow<'_, str>> for Text {
    fn from(text: std::borrow::Cow<'_, str>) -> Self {
        match text {
            std::borrow::Cow::Borrowed(text) => Self::new(text),
            std::borrow::Cow::Owned(text) => Self::from(text),
        }
    }
}

impl From<Text> for String {
    fn from(text: Text) -> Self {
        text.0.to_string()
    }
}

impl From<&Text> for String {
    fn from(text: &Text) -> Self {
        text.0.to_string()
    }
}

impl PartialEq<str> for Text {
    fn eq(&self, other: &str) -> bool {
        &*self.0 == other
    }
}

impl PartialEq<&str> for Text {
    fn eq(&self, other: &&str) -> bool {
        &*self.0 == *other
    }
}

impl PartialEq<String> for Text {
    fn eq(&self, other: &String) -> bool {
        &*self.0 == other.as_str()
    }
}

impl PartialEq<Text> for str {
    fn eq(&self, other: &Text) -> bool {
        self == &*other.0
    }
}

impl PartialEq<Text> for &str {
    fn eq(&self, other: &Text) -> bool {
        *self == &*other.0
    }
}

impl PartialEq<Text> for String {
    fn eq(&self, other: &Text) -> bool {
        self.as_str() == &*other.0
    }
}

/// Written as the string, so a block holding a `Text` debug-prints as it did with a `String`.
impl fmt::Debug for Text {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&*self.0, f)
    }
}

impl fmt::Display for Text {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for Text {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Text {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Read through a borrowed string when the input allows it, so the text is copied once,
        // into the shared allocation.
        struct Visitor;
        impl serde::de::Visitor<'_> for Visitor {
            type Value = Text;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a string")
            }
            fn visit_str<E: serde::de::Error>(self, text: &str) -> Result<Text, E> {
                Ok(Text::from(text))
            }
            fn visit_string<E: serde::de::Error>(self, text: String) -> Result<Text, E> {
                Ok(Text::from(text))
            }
        }
        deserializer.deserialize_str(Visitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_as_the_string_did() {
        for text in ["", "plain", "quote \" and \\ and \n and \u{1F600}"] {
            let held = Text::from(text);
            assert_eq!(
                serde_json::to_string(&held).unwrap(),
                serde_json::to_string(text).unwrap()
            );
            assert_eq!(
                serde_json::from_str::<Text>(&serde_json::to_string(text).unwrap()).unwrap(),
                text
            );
            assert_eq!(format!("{held:?}"), format!("{text:?}"));
        }
    }

    #[test]
    fn clones_share_one_allocation() {
        let held = Text::from("shared".to_string());
        let copy = held.clone();
        assert!(Arc::ptr_eq(&held.shared(), &copy.shared()));
        assert_eq!(held.appended(" more"), "shared more");
    }

    #[test]
    fn empty_texts_share_one_allocation() {
        let made = [
            Text::default(),
            Text::from(""),
            Text::from(String::new()),
            Text::from(std::borrow::Cow::Borrowed("")),
        ];
        for text in &made {
            assert!(Arc::ptr_eq(&text.shared(), &made[0].shared()));
        }
    }
}
