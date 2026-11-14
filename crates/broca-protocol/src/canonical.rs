//! Deterministic JSON serialization — the determinism primitive under the
//! provider-request renderer. Prompt-cache stability on resume requires the
//! rendered request BYTES to be byte-identical across a no-crash run and a resumed
//! run; `serde_json`'s object-key order is NOT guaranteed for arbitrary `Value`
//! data (and `preserve_order` may be enabled transitively in the dependency
//! graph), which is exactly where `provider_options`, tool input schemas, and
//! tool args live. So we serialize with EXPLICITLY sorted object keys rather than
//! trusting `Map` iteration order.
//!
//! Numbers use `serde_json`'s formatter (ryu-backed, deterministic). The output
//! is compact (no insignificant whitespace).

use serde_json::Value;

/// Canonical, deterministic bytes for a JSON value: object keys sorted
/// lexicographically at every level, compact, stable number formatting.
///
/// The returned buffer's capacity equals its length. The writer grows the vector by doubling,
/// so without the final shrink up to half of a large request body's allocation would be spare
/// capacity, and `Bytes::from(Vec)` keeps all of it alive for as long as the request can be
/// retried. Shrinking moves no byte, so the rendered output is unchanged.
pub fn canonical_json_bytes(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    write_canonical(&mut out, value);
    out.shrink_to_fit();
    out
}

pub(crate) fn write_canonical<W: std::io::Write>(out: &mut W, v: &Value) {
    match v {
        Value::Object(map) => {
            put(out, b"{");
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    put(out, b",");
                }
                // Serialize the key as a properly-escaped JSON string.
                serde_json::to_writer(&mut *out, k).expect("string key never fails");
                put(out, b":");
                write_canonical(out, &map[*k]);
            }
            put(out, b"}");
        }
        Value::Array(arr) => {
            put(out, b"[");
            for (i, e) in arr.iter().enumerate() {
                if i > 0 {
                    put(out, b",");
                }
                write_canonical(out, e);
            }
            put(out, b"]");
        }
        // Scalars (null/bool/number/string) have a single canonical serde_json form.
        other => serde_json::to_writer(&mut *out, other).expect("scalar never fails"),
    }
}

/// Writes `bytes` to a sink that can't fail: memory, or a [`ByteCount`].
pub(crate) fn put<W: std::io::Write>(out: &mut W, bytes: &[u8]) {
    out.write_all(bytes)
        .expect("the canonical writers write to memory or count");
}

/// A sink that keeps no bytes, only how many were written: the length of a request without
/// writing it.
#[derive(Default)]
pub(crate) struct ByteCount(pub usize);

impl std::io::Write for ByteCount {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 += bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn key_order_is_independent_of_insertion_order() {
        // Two objects with the SAME content inserted in different orders must
        // render to identical bytes (the cache-stability guarantee).
        let a: Value = serde_json::from_str(r#"{"b":1,"a":2,"c":{"z":1,"y":2}}"#).unwrap();
        let b: Value = serde_json::from_str(r#"{"c":{"y":2,"z":1},"a":2,"b":1}"#).unwrap();
        assert_eq!(canonical_json_bytes(&a), canonical_json_bytes(&b));
        assert_eq!(
            canonical_json_bytes(&a),
            br#"{"a":2,"b":1,"c":{"y":2,"z":1}}"#.to_vec()
        );
    }

    #[test]
    fn arrays_preserve_order() {
        let v = json!([3, 1, 2]);
        assert_eq!(canonical_json_bytes(&v), b"[3,1,2]".to_vec());
    }

    #[test]
    fn nested_and_scalars() {
        let v = json!({"tool": "get_time", "args": {"tz": "Europe/Madrid", "dst": true}});
        assert_eq!(
            canonical_json_bytes(&v),
            br#"{"args":{"dst":true,"tz":"Europe/Madrid"},"tool":"get_time"}"#.to_vec()
        );
    }

    #[test]
    fn large_output_holds_no_spare_capacity() {
        // A request body of a few megabytes is the case that matters: doubling growth would
        // leave up to half of its allocation unused for the life of the retained request.
        let blocks: Vec<Value> = (0..2_000)
            .map(|i| json!({"type": "text", "text": format!("{i}:{}", "x".repeat(1_500))}))
            .collect();
        let bytes = canonical_json_bytes(&json!({"messages": blocks}));
        assert!(
            bytes.len() > 3_000_000,
            "fixture renders {} bytes",
            bytes.len()
        );
        assert_eq!(bytes.capacity(), bytes.len());
    }
}
