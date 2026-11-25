//! A provider request written from the content it carries rather than built as a JSON tree.
//!
//! A wire family used to shape each request as a `serde_json::Value`, which copies every text,
//! signature and tool input into the tree before writing bytes from it: a request of n bytes
//! briefly held several times n. A [`Node`] borrows the message content instead. Only the
//! request's structure (its objects and arrays) is allocated, and the content is escaped
//! straight into the output.
//!
//! [`Node::write_canonical`] writes exactly the bytes [`crate::canonical_json_bytes`] writes for
//! the same value: object keys sorted at every level, compact, serde_json's spelling of strings
//! and numbers. A family can therefore move from `Value` to `Node` without changing one byte of
//! any request, which is what keeps a resumed run's prompt cache.

use std::borrow::Cow;

use serde_json::Value;

use crate::json::Json;

/// One JSON value of a request being written.
#[derive(Debug, Clone)]
pub enum Node<'a> {
    /// Any JSON value, borrowed or owned. For the small pieces (flags, numbers, ids, nested
    /// configuration) whose copy costs nothing worth avoiding.
    Value(Cow<'a, Value>),
    /// A string, escaped as serde_json escapes it.
    Str(Cow<'a, str>),
    /// A JSON value held as its canonical text, such as a tool input schema or call input. The
    /// canonical writer copies the text directly.
    Json(&'a Json),
    Array(Vec<Node<'a>>),
    /// An object's members. The writer orders them, so their order here is free. A key appears
    /// at most once: [`Node::insert`] replaces an existing member.
    Object(Vec<(Cow<'a, str>, Node<'a>)>),
}

impl<'a> Node<'a> {
    /// A borrowed string.
    pub fn str(text: &'a str) -> Self {
        Node::Str(Cow::Borrowed(text))
    }

    /// An owned string, for text the request makes up (a joined system tail, a marker).
    pub fn string(text: String) -> Self {
        Node::Str(Cow::Owned(text))
    }

    /// An owned JSON value.
    pub fn value(value: Value) -> Self {
        Node::Value(Cow::Owned(value))
    }

    /// A borrowed JSON value.
    pub fn value_ref(value: &'a Value) -> Self {
        Node::Value(Cow::Borrowed(value))
    }

    /// An object with these members, each key named once.
    pub fn object<const N: usize>(members: [(&'a str, Node<'a>); N]) -> Self {
        Node::Object(
            members
                .into_iter()
                .map(|(key, value)| (Cow::Borrowed(key), value))
                .collect(),
        )
    }

    /// The member `key` of an object, or of a JSON value that is an object.
    pub fn get(&self, key: &str) -> Option<NodeRef<'_, 'a>> {
        match self {
            Node::Object(members) => members
                .iter()
                .find(|(name, _)| name == key)
                .map(|(_, value)| NodeRef::Node(value)),
            Node::Value(value) => value.get(key).map(NodeRef::Value),
            _ => None,
        }
    }

    /// The member `key` of an object node, to change it in place. A member held inside a JSON
    /// value is not reachable this way: the families only change members they wrote as nodes.
    pub fn get_mut(&mut self, key: &str) -> Option<&mut Node<'a>> {
        match self {
            Node::Object(members) => members
                .iter_mut()
                .find(|(name, _)| name == key)
                .map(|(_, value)| value),
            _ => None,
        }
    }

    /// Whether this is an object: an object node, or a JSON value that is one.
    pub fn is_object(&self) -> bool {
        match self {
            Node::Object(_) => true,
            Node::Value(value) => value.is_object(),
            _ => false,
        }
    }

    /// Sets member `key` of an object, replacing the member it had. A JSON value object takes
    /// the member as JSON, copying the value first if it was borrowed. Does nothing on any
    /// other node.
    pub fn insert(&mut self, key: impl Into<Cow<'a, str>>, value: Node<'a>) {
        match self {
            Node::Object(members) => {
                let key = key.into();
                match members.iter_mut().find(|(name, _)| *name == key) {
                    Some((_, slot)) => *slot = value,
                    None => members.push((key, value)),
                }
            }
            Node::Value(object) if object.is_object() => {
                if let Some(members) = object.to_mut().as_object_mut() {
                    members.insert(key.into().into_owned(), value.to_value());
                }
            }
            _ => {}
        }
    }

    /// Removes member `key` of an object.
    pub fn remove(&mut self, key: &str) {
        match self {
            Node::Object(members) => members.retain(|(name, _)| name != key),
            Node::Value(object) if object.get(key).is_some() => {
                if let Some(members) = object.to_mut().as_object_mut() {
                    members.remove(key);
                }
            }
            _ => {}
        }
    }

    /// The string this node is, when it is one.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Node::Str(text) => Some(text),
            Node::Value(value) => value.as_str(),
            _ => None,
        }
    }

    /// The items of an array node.
    pub fn as_array(&self) -> Option<&[Node<'a>]> {
        match self {
            Node::Array(items) => Some(items),
            _ => None,
        }
    }

    /// The items of an array node, to change them in place.
    pub fn as_array_mut(&mut self) -> Option<&mut Vec<Node<'a>>> {
        match self {
            Node::Array(items) => Some(items),
            _ => None,
        }
    }

    /// About how many bytes this value writes: its strings and JSON texts before escaping, and
    /// its punctuation. A writer sizes its buffer from it, so a large request is written into
    /// one allocation instead of being copied each time the buffer doubles.
    pub fn len_hint(&self) -> usize {
        match self {
            // Small by construction: a flag, a number, a short configuration object.
            Node::Value(_) => 16,
            Node::Str(text) => text.len() + 2,
            Node::Json(json) => json.text().len(),
            Node::Array(items) => 2 + items.iter().map(|item| item.len_hint() + 1).sum::<usize>(),
            Node::Object(members) => {
                2 + members
                    .iter()
                    .map(|(key, value)| key.len() + 4 + value.len_hint())
                    .sum::<usize>()
            }
        }
    }

    /// The canonical bytes of this value: those [`crate::canonical_json_bytes`] writes for it.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let hint = self.len_hint();
        // A sixteenth over the hint covers the escapes of ordinary text.
        let mut out = Vec::with_capacity(hint + hint / 16);
        self.write_canonical(&mut out);
        out.shrink_to_fit();
        out
    }

    /// How many bytes [`Node::canonical_bytes`] writes, counted without writing them: a step
    /// sizes its request this way rather than rendering it a second time.
    pub fn canonical_len(&self) -> usize {
        let mut count = crate::canonical::ByteCount::default();
        self.write_canonical(&mut count);
        count.0
    }

    /// Appends this value's canonical bytes to `out`, a buffer or a byte count.
    pub fn write_canonical<W: std::io::Write>(&self, out: &mut W) {
        use crate::canonical::put;
        match self {
            Node::Value(value) => crate::canonical::write_canonical(out, value),
            Node::Str(text) => serde_json::to_writer(&mut *out, text.as_ref())
                .expect("writing a string to memory never fails"),
            Node::Json(json) => put(out, json.text().as_bytes()),
            Node::Array(items) => {
                put(out, b"[");
                for (index, item) in items.iter().enumerate() {
                    if index > 0 {
                        put(out, b",");
                    }
                    item.write_canonical(out);
                }
                put(out, b"]");
            }
            Node::Object(members) => {
                let mut sorted: Vec<&(Cow<'a, str>, Node<'a>)> = members.iter().collect();
                sorted.sort_unstable_by(|left, right| left.0.cmp(&right.0));
                put(out, b"{");
                for (index, (key, value)) in sorted.into_iter().enumerate() {
                    if index > 0 {
                        put(out, b",");
                    }
                    serde_json::to_writer(&mut *out, key.as_ref())
                        .expect("writing a string to memory never fails");
                    put(out, b":");
                    value.write_canonical(out);
                }
                put(out, b"}");
            }
        }
    }

    /// The JSON value this node writes. For tests and the rare reader that needs a whole
    /// value; it copies everything the node borrows.
    pub fn to_value(&self) -> Value {
        match self {
            Node::Value(value) => value.as_ref().clone(),
            Node::Str(text) => Value::String(text.to_string()),
            Node::Json(json) => json.parse(),
            Node::Array(items) => Value::Array(items.iter().map(Node::to_value).collect()),
            Node::Object(members) => Value::Object(
                members
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_value()))
                    .collect(),
            ),
        }
    }
}

/// A member read from a node: a node, or a value inside a JSON value node.
#[derive(Debug, Clone, Copy)]
pub enum NodeRef<'n, 'a> {
    Node(&'n Node<'a>),
    Value(&'n Value),
}

impl<'n, 'a> NodeRef<'n, 'a> {
    pub fn as_str(self) -> Option<&'n str> {
        match self {
            NodeRef::Node(node) => node.as_str(),
            NodeRef::Value(value) => value.as_str(),
        }
    }

    pub fn get(self, key: &str) -> Option<NodeRef<'n, 'a>> {
        match self {
            NodeRef::Node(node) => node.get(key),
            NodeRef::Value(value) => value.get(key).map(NodeRef::Value),
        }
    }
}

impl From<Value> for Node<'_> {
    fn from(value: Value) -> Self {
        Node::value(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Every node shape writes the bytes `canonical_json_bytes` writes for the same value,
    /// including keys that sort differently from their insertion order, escapes, numbers of
    /// every kind and JSON text spliced from a `Json`.
    #[test]
    fn a_node_writes_the_canonical_bytes_of_its_value() {
        let input: Json =
            (&json!({"z": [1, 2.5, -3], "a": {"y": null, "b": "q\"\n\u{1}é"}})).into();
        let nested = json!({"k": [true, false, 1e300, 0.1], "\u{7f}": "\\"});
        let node = Node::Object(vec![
            (Cow::Borrowed("type"), Node::str("tool_use")),
            (Cow::Borrowed("input"), Node::Json(&input)),
            (Cow::Borrowed("Zeta"), Node::value(nested.clone())),
            (
                Cow::Borrowed("content"),
                Node::Array(vec![
                    Node::string("tab\there \u{2028} \u{0} end".into()),
                    Node::value_ref(&nested),
                    Node::Array(vec![]),
                    Node::Object(vec![]),
                ]),
            ),
            (Cow::Borrowed("é"), Node::str("")),
            (
                Cow::Borrowed("cache_control"),
                Node::value(json!({"type": "ephemeral"})),
            ),
        ]);
        assert_eq!(
            node.canonical_bytes(),
            crate::canonical_json_bytes(&node.to_value())
        );
    }

    #[test]
    fn insert_replaces_a_member_and_remove_drops_it() {
        let mut node = Node::object([("a", Node::str("1")), ("b", Node::str("2"))]);
        node.insert("a", Node::str("3"));
        node.remove("b");
        node.insert("c", Node::str("4"));
        assert_eq!(node.to_value(), json!({"a": "3", "c": "4"}));
        assert_eq!(node.canonical_bytes(), br#"{"a":"3","c":"4"}"#.to_vec());
    }
}
