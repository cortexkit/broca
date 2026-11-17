//! A JSON value held as its canonical compact text.
//!
//! Message content carries JSON in a few places: a tool call's input and a tool result's JSON
//! output. Held as a parsed `serde_json::Value`, a small object costs a map node of several
//! hundred bytes plus one allocation per key and string, so a session's tool calls cost many
//! times their text. [`Json`] holds the text once, shared: cloning it copies a pointer, and its
//! size in memory is its length.
//!
//! The text is the canonical form [`crate::canonical::canonical_json_bytes`] writes (object
//! keys sorted, compact, serde_json's number spelling), which is also what serializing the
//! parsed value writes. So serializing a `Json` writes the same bytes the value did, a request
//! that embeds it renders the same bytes, and two `Json`s are equal exactly when their values
//! are. Every constructor canonicalizes, so no other text can be held.
//!
//! A reader that needs a field calls [`Json::parse`] and gets a `Value` it owns.

use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::canonical::canonical_json_bytes;

/// A JSON value held as its canonical compact text. See the module documentation.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Json(Arc<str>);

impl Json {
    /// The value's canonical text.
    pub fn from_value(value: &Value) -> Self {
        let bytes = canonical_json_bytes(value);
        Self(Arc::from(
            String::from_utf8(bytes).expect("canonical JSON is UTF-8"),
        ))
    }

    /// The canonical text of the JSON `text`, which may be spelled any way JSON allows.
    pub fn from_text(text: &str) -> Result<Self, serde_json::Error> {
        Ok(Self::from_value(&serde_json::from_str(text)?))
    }

    /// `text` itself, when it is already the canonical text of its value; `None` for any other
    /// spelling. Checking costs one parse, and the text is kept without being written again.
    pub fn from_canonical_text(text: &str) -> Option<Self> {
        let value: Value = serde_json::from_str(text).ok()?;
        let mut rest = text.as_bytes();
        let mut same = MatchesPrefix(&mut rest);
        serde_json::to_writer(&mut same, &value).ok()?;
        rest.is_empty().then(|| Self(Arc::from(text)))
    }

    /// JSON `null`.
    pub fn null() -> Self {
        Self(Arc::from("null"))
    }

    /// An empty JSON object, `{}`.
    pub fn empty_object() -> Self {
        Self(Arc::from("{}"))
    }

    /// The canonical text, shared: another holder of the same JSON can keep it without a copy.
    pub fn shared_text(&self) -> Arc<str> {
        Arc::clone(&self.0)
    }

    /// The canonical text. Named apart from `Value::as_str`, which reads a JSON string's
    /// content, so code moved from a `Value` can't read the text by mistake.
    pub fn text(&self) -> &str {
        &self.0
    }

    /// The value, parsed from the text. The text is canonical JSON, so this can't fail.
    pub fn parse(&self) -> Value {
        serde_json::from_str(&self.0).expect("held JSON text is valid")
    }

    /// Whether the value is a JSON object.
    pub fn is_object(&self) -> bool {
        self.0.starts_with('{')
    }

    /// Whether the value is a JSON array.
    pub fn is_array(&self) -> bool {
        self.0.starts_with('[')
    }

    /// Whether the value is `null`.
    pub fn is_null(&self) -> bool {
        &*self.0 == "null"
    }

    /// Bytes held for the text, for memory accounting.
    pub fn held_bytes(&self) -> usize {
        self.0.len()
    }

    /// Whether another holder shares this text's allocation now.
    pub fn is_shared(&self) -> bool {
        Arc::strong_count(&self.0) > 1
    }
}

/// A writer that accepts only the next bytes of the text it holds, so serializing into it checks
/// that the text is exactly what serializing writes, without building a copy. serde_json's
/// map is ordered by key, as the canonical text is, so for a parsed value the two agree.
struct MatchesPrefix<'a, 'b>(&'a mut &'b [u8]);

impl std::io::Write for MatchesPrefix<'_, '_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self.0.strip_prefix(buf) {
            Some(rest) => {
                *self.0 = rest;
                Ok(buf.len())
            }
            None => Err(std::io::Error::other("text differs")),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Default for Json {
    /// `null`, as `Value::default()` is.
    fn default() -> Self {
        Self::null()
    }
}

impl From<Value> for Json {
    fn from(value: Value) -> Self {
        Self::from_value(&value)
    }
}

impl From<&Value> for Json {
    fn from(value: &Value) -> Self {
        Self::from_value(value)
    }
}

impl From<&Json> for Value {
    fn from(json: &Json) -> Self {
        json.parse()
    }
}

impl From<Json> for Value {
    fn from(json: Json) -> Self {
        json.parse()
    }
}

impl PartialEq<Value> for Json {
    fn eq(&self, other: &Value) -> bool {
        self.parse() == *other
    }
}

impl PartialEq<Json> for Value {
    fn eq(&self, other: &Json) -> bool {
        other == self
    }
}

impl fmt::Debug for Json {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.parse(), f)
    }
}

impl fmt::Display for Json {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for Json {
    /// Serializes the parsed value, so every serializer writes what it wrote for the value:
    /// serde_json writes the same text, and a canonicalizing serializer (`serde_jcs`, RFC 8785)
    /// writes its own spelling. A
    /// raw-text shortcut would be cheaper for serde_json but wrong for any other serializer,
    /// which would write serde_json's private raw-value wrapper instead.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.parse().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Json {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Value::deserialize(deserializer).map(|value| Self::from_value(&value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_text_is_what_serializing_the_value_writes() {
        for value in [
            json!({"b": 1, "a": [1.5, "x", null, {"d": true, "c": -2}], "e": "\u{1F600}\n"}),
            json!(1.0),
            json!(12345678901234567890u64),
            json!("plain"),
            json!([]),
            json!(null),
        ] {
            let held = Json::from_value(&value);
            assert_eq!(held.text(), serde_json::to_string(&value).unwrap());
            assert_eq!(serde_json::to_string(&held).unwrap(), held.text());
            assert_eq!(held.parse(), value);
            assert_eq!(serde_json::to_value(&held).unwrap(), value);
            assert_eq!(
                serde_jcs::to_vec(&held).unwrap(),
                serde_jcs::to_vec(&value).unwrap()
            );
        }
    }

    #[test]
    fn any_spelling_reads_back_canonical() {
        let held: Json = serde_json::from_str(r#"{ "b" : 1 ,"a":2.50 }"#).unwrap();
        assert_eq!(held.text(), r#"{"a":2.5,"b":1}"#);
        assert_eq!(
            Json::from_text("[ 1 , 2 ]").unwrap(),
            Json::from_value(&json!([1, 2]))
        );
    }

    #[test]
    fn only_canonical_text_is_kept_as_is() {
        let kept = Json::from_canonical_text(r#"{"a":[1,2.5],"b":"x"}"#).unwrap();
        assert_eq!(kept.text(), r#"{"a":[1,2.5],"b":"x"}"#);
        for other in [r#"{"b":1,"a":2}"#, r#"{ "a":1}"#, "2.50", "[1,2] ", "nope"] {
            assert!(Json::from_canonical_text(other).is_none(), "{other}");
        }
    }

    #[test]
    fn embedding_writes_the_same_bytes_as_the_value() {
        let input = json!({"path": "/tmp/x", "lines": [1, 2]});
        let with_value = json!({"input": input.clone(), "name": "read"});
        #[derive(Serialize)]
        struct Call {
            input: Json,
            name: &'static str,
        }
        let with_json = Call {
            input: Json::from_value(&input),
            name: "read",
        };
        assert_eq!(
            serde_json::to_string(&with_json).unwrap(),
            serde_json::to_string(&with_value).unwrap()
        );
    }
}
