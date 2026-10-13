//! Two-tier deep merge: the vendored models.dev snapshot is the base, the
//! hand-maintained overlay is on top and wins.
//!
//! Merge rules (deliberately simple to avoid ambiguity):
//!  - two objects merge KEY-WISE (recurse into shared keys, add overlay-only keys);
//!  - a scalar in the overlay REPLACES the base value;
//!  - an array in the overlay REPLACES the base array WHOLESALE — never element-wise
//!    (partial array merge has no unambiguous meaning and is a footgun).
//!
//! So overlay always wins on a leaf, and only nested objects combine.

use serde_json::Value;

/// Deep-merge `overlay` onto `base`, returning the effective value.
pub fn deep_merge(base: &Value, overlay: &Value) -> Value {
    match (base, overlay) {
        (Value::Object(base_obj), Value::Object(overlay_obj)) => {
            let mut out = base_obj.clone();
            for (key, overlay_val) in overlay_obj {
                let merged = match out.get(key) {
                    Some(base_val) => deep_merge(base_val, overlay_val),
                    None => overlay_val.clone(),
                };
                out.insert(key.clone(), merged);
            }
            Value::Object(out)
        }
        // Any non-(object,object) pair: the overlay leaf replaces the base wholesale
        // (scalars replace, arrays replace, and a type change replaces).
        (_, overlay_val) => overlay_val.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn objects_merge_key_wise() {
        let base = json!({ "a": 1, "b": { "x": 1, "y": 2 } });
        let overlay = json!({ "b": { "y": 20, "z": 3 }, "c": 4 });
        assert_eq!(
            deep_merge(&base, &overlay),
            // a kept; b merged (x kept, y replaced, z added); c added.
            json!({ "a": 1, "b": { "x": 1, "y": 20, "z": 3 }, "c": 4 })
        );
    }

    #[test]
    fn scalar_overlay_replaces() {
        assert_eq!(deep_merge(&json!(1), &json!(2)), json!(2));
        assert_eq!(
            deep_merge(&json!({ "a": 1 }), &json!({ "a": 9 })),
            json!({ "a": 9 })
        );
    }

    #[test]
    fn arrays_replace_wholesale_not_element_wise() {
        let base = json!({ "tools": [1, 2, 3] });
        let overlay = json!({ "tools": [9] });
        // The whole array is replaced — NOT merged to [9, 2, 3].
        assert_eq!(deep_merge(&base, &overlay), json!({ "tools": [9] }));
    }

    #[test]
    fn type_change_replaces() {
        // An object replaced by a scalar (or vice-versa) takes the overlay.
        assert_eq!(
            deep_merge(&json!({ "a": { "x": 1 } }), &json!({ "a": "flat" })),
            json!({ "a": "flat" })
        );
    }

    #[test]
    fn overlay_only_key_is_added() {
        assert_eq!(
            deep_merge(&json!({}), &json!({ "new": 1 })),
            json!({ "new": 1 })
        );
    }
}
