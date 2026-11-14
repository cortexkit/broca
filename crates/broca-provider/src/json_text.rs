//! Reading a JSON text's structure without parsing it into a tree.
//!
//! A rendered request is canonical JSON text. Code that needs its top-level members, or the
//! items of one array, finds their spans with these scanners and reads or copies the slices,
//! instead of parsing the whole request into a `serde_json::Value` first.

/// The members of a top-level JSON object as `(key, value)` text slices, the key with its
/// quotes. `None` when `text` is not a single JSON object.
pub(crate) fn top_level_members(text: &str) -> Option<Vec<(&str, &str)>> {
    let bytes = text.as_bytes();
    let mut at = skip_whitespace(bytes, 0);
    if bytes.get(at) != Some(&b'{') {
        return None;
    }
    at += 1;
    let mut members = Vec::new();
    loop {
        at = skip_whitespace(bytes, at);
        match bytes.get(at) {
            Some(b'}') if members.is_empty() => {
                at += 1;
                break;
            }
            Some(b'"') => {}
            _ => return None,
        }
        let key_end = string_end(bytes, at)?;
        let key = &text[at..key_end];
        at = skip_whitespace(bytes, key_end);
        if bytes.get(at) != Some(&b':') {
            return None;
        }
        at = skip_whitespace(bytes, at + 1);
        let value_end = value_end(bytes, at)?;
        members.push((key, &text[at..value_end]));
        at = skip_whitespace(bytes, value_end);
        match bytes.get(at) {
            Some(b',') => at += 1,
            Some(b'}') => {
                at += 1;
                break;
            }
            _ => return None,
        }
    }
    (skip_whitespace(bytes, at) == bytes.len()).then_some(members)
}

pub(crate) fn skip_whitespace(bytes: &[u8], mut at: usize) -> usize {
    while bytes
        .get(at)
        .is_some_and(|byte| matches!(byte, b' ' | b'\t' | b'\n' | b'\r'))
    {
        at += 1;
    }
    at
}

/// The end (exclusive) of the JSON string starting at the quote at `at`.
pub(crate) fn string_end(bytes: &[u8], at: usize) -> Option<usize> {
    let mut index = at + 1;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => index += 2,
            b'"' => return Some(index + 1),
            _ => index += 1,
        }
    }
    None
}

/// The end (exclusive) of the JSON value starting at `at`.
pub(crate) fn value_end(bytes: &[u8], at: usize) -> Option<usize> {
    match bytes.get(at)? {
        b'"' => string_end(bytes, at),
        b'{' | b'[' => {
            let mut depth = 0usize;
            let mut index = at;
            while index < bytes.len() {
                match bytes[index] {
                    b'"' => {
                        index = string_end(bytes, index)?;
                        continue;
                    }
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' => {
                        depth -= 1;
                        if depth == 0 {
                            return Some(index + 1);
                        }
                    }
                    _ => {}
                }
                index += 1;
            }
            None
        }
        _ => {
            let length = bytes[at..]
                .iter()
                .take_while(|byte| {
                    !matches!(byte, b',' | b'}' | b']' | b' ' | b'\t' | b'\n' | b'\r')
                })
                .count();
            (length > 0).then_some(at + length)
        }
    }
}

/// The items of a JSON array's text, each as its own text. `None` when `text` is not a single
/// JSON array.
pub(crate) fn array_items(text: &str) -> Option<Vec<&str>> {
    let bytes = text.as_bytes();
    let mut at = skip_whitespace(bytes, 0);
    if bytes.get(at) != Some(&b'[') {
        return None;
    }
    at = skip_whitespace(bytes, at + 1);
    let mut items = Vec::new();
    if bytes.get(at) == Some(&b']') {
        at += 1;
    } else {
        loop {
            let end = value_end(bytes, at)?;
            items.push(&text[at..end]);
            at = skip_whitespace(bytes, end);
            match bytes.get(at) {
                Some(b',') => at = skip_whitespace(bytes, at + 1),
                Some(b']') => {
                    at += 1;
                    break;
                }
                _ => return None,
            }
        }
    }
    (skip_whitespace(bytes, at) == bytes.len()).then_some(items)
}

/// A key's name, from its quoted JSON text.
pub(crate) fn key_name(quoted: &str) -> std::borrow::Cow<'_, str> {
    let inner = &quoted[1..quoted.len() - 1];
    if inner.contains('\\') {
        serde_json::from_str::<String>(quoted).map_or(std::borrow::Cow::Borrowed(inner), Into::into)
    } else {
        std::borrow::Cow::Borrowed(inner)
    }
}

/// The text of the top-level member `name`, from members [`top_level_members`] found.
pub(crate) fn member<'t>(members: &[(&'t str, &'t str)], name: &str) -> Option<&'t str> {
    members
        .iter()
        .find(|(key, _)| key_name(key) == name)
        .map(|(_, value)| *value)
}

/// Writes `members`, each a key name and its value's canonical text, as one JSON object: the
/// keys named in `order` first, in that order, then the rest by key, as a sorted map writes
/// them. With an empty `order` this is the canonical object.
pub(crate) fn object_bytes(
    mut members: Vec<(std::borrow::Cow<'_, str>, std::borrow::Cow<'_, str>)>,
    order: &[&str],
) -> Vec<u8> {
    members.sort_by(|(left, _), (right, _)| left.cmp(right));
    if !order.is_empty() {
        let rank = |key: &str| order.iter().position(|known| *known == key);
        // A stable sort keeps the members outside the order sorted by key behind it.
        members.sort_by_key(|(key, _)| rank(key).unwrap_or(order.len()));
    }
    let length = members
        .iter()
        .map(|(key, value)| key.len() + value.len() + 4)
        .sum::<usize>();
    let mut bytes = Vec::with_capacity(length + 2);
    bytes.push(b'{');
    for (key, value) in &members {
        if bytes.len() > 1 {
            bytes.push(b',');
        }
        serde_json::to_writer(&mut bytes, key.as_ref()).expect("writing a key never fails");
        bytes.push(b':');
        bytes.extend_from_slice(value.as_bytes());
    }
    bytes.push(b'}');
    bytes
}
