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
pub fn canonical_json_bytes(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    write_canonical(&mut out, value);
    out
}

fn write_canonical(out: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Object(map) => {
            out.push(b'{');
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                // Serialize the key as a properly-escaped JSON string.
                serde_json::to_writer(&mut *out, k).expect("string key never fails");
                out.push(b':');
                write_canonical(out, &map[*k]);
            }
            out.push(b'}');
        }
        Value::Array(arr) => {
            out.push(b'[');
            for (i, e) in arr.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_canonical(out, e);
            }
            out.push(b']');
        }
        // Scalars (null/bool/number/string) have a single canonical serde_json form.
        other => serde_json::to_writer(&mut *out, other).expect("scalar never fails"),
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
}
