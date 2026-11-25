//! Typing of `enum` schema nodes in rendered tool declarations, for providers whose schema
//! validator requires a `type` on every node.
//!
//! JSON Schema allows a node with an `enum` and no `type`, and Anthropic and OpenAI accept it.
//! Moonshot's API (the Kimi routes) does not: it refuses the whole request with HTTP 400, "not
//! a valid moonshot flavored json schema ... type is not defined". Tool providers declare such
//! nodes, so the render edge adds the missing `type` for those routes. Stored tools keep the
//! schema exactly as their provider declared it; only the request body changes.

use serde_json::Value;

/// Add a `type` to every node of `schema` that has an `enum` and no `type`, when all the enum's
/// values share one JSON type: `"string"`, `"boolean"`, `"integer"` (every value a whole
/// number), or `"number"` (numbers, some not whole). A node whose values are mixed, include
/// null, an array or an object, or that has an empty `enum`, is left as it is.
///
/// The walk follows the positions that hold subschemas: `properties`, `items` (one schema or a
/// list of them), `additionalProperties`, the `anyOf`/`oneOf`/`allOf` branches, and `$defs` and
/// `definitions`. Nothing else is added, removed or reordered.
pub(crate) fn add_enum_types(schema: &mut Value) {
    let Value::Object(node) = schema else {
        return;
    };
    if !node.contains_key("type") {
        if let Some(json_type) = node.get("enum").and_then(shared_json_type) {
            node.insert("type".to_string(), Value::String(json_type.to_string()));
        }
    }
    for key in ["properties", "$defs", "definitions"] {
        if let Some(Value::Object(children)) = node.get_mut(key) {
            children.values_mut().for_each(add_enum_types);
        }
    }
    for key in ["items", "additionalProperties"] {
        match node.get_mut(key) {
            Some(Value::Array(children)) => children.iter_mut().for_each(add_enum_types),
            Some(child) => add_enum_types(child),
            None => {}
        }
    }
    for key in ["anyOf", "oneOf", "allOf"] {
        if let Some(Value::Array(branches)) = node.get_mut(key) {
            branches.iter_mut().for_each(add_enum_types);
        }
    }
}

/// The JSON Schema type every value of an `enum` array shares, or `None` when they share none
/// that a single `type` keyword can state without excluding one of them.
fn shared_json_type(values: &Value) -> Option<&'static str> {
    let Value::Array(values) = values else {
        return None;
    };
    let mut shared = None;
    for value in values {
        let json_type = match value {
            Value::String(_) => "string",
            Value::Bool(_) => "boolean",
            Value::Number(number) if number.is_i64() || number.is_u64() => "integer",
            Value::Number(_) => "number",
            Value::Null | Value::Array(_) | Value::Object(_) => return None,
        };
        shared = match (shared, json_type) {
            (None, json_type) => Some(json_type),
            (Some(seen), json_type) if seen == json_type => Some(seen),
            // Whole and fractional numbers together are all numbers.
            (Some("integer" | "number"), "integer" | "number") => Some("number"),
            _ => return None,
        };
    }
    shared
}

/// A copy of `schema` with [`add_enum_types`] applied, leaving the caller's value untouched.
#[cfg(test)]
fn with_enum_types(schema: &Value) -> Value {
    let mut typed = schema.clone();
    add_enum_types(&mut typed);
    typed
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn typed(schema: Value) -> Value {
        with_enum_types(&schema)
    }

    #[test]
    fn string_enum_properties_without_a_type_get_string() {
        let schema = json!({
            "type": "object",
            "properties": {
                "surface": {"enum": ["tool", "management"]},
                "op": {"enum": ["call", "status", "cancel"], "description": "what to do"},
                "module": {"type": "string", "minLength": 1}
            },
            "required": ["surface"]
        });
        assert_eq!(
            typed(schema),
            json!({
                "type": "object",
                "properties": {
                    "surface": {"enum": ["tool", "management"], "type": "string"},
                    "op": {
                        "enum": ["call", "status", "cancel"],
                        "description": "what to do",
                        "type": "string"
                    },
                    "module": {"type": "string", "minLength": 1}
                },
                "required": ["surface"]
            })
        );
    }

    #[test]
    fn each_shared_value_type_is_named() {
        let schema = json!({"properties": {
            "flag": {"enum": [true, false]},
            "count": {"enum": [1, 2, 3]},
            "ratio": {"enum": [0.5, 1]},
            "negative": {"enum": [-1, 0]}
        }});
        let out = typed(schema);
        assert_eq!(out["properties"]["flag"]["type"], "boolean");
        assert_eq!(out["properties"]["count"]["type"], "integer");
        assert_eq!(out["properties"]["ratio"]["type"], "number");
        assert_eq!(out["properties"]["negative"]["type"], "integer");
    }

    #[test]
    fn mixed_null_compound_and_empty_enums_are_left_alone() {
        let schema = json!({"properties": {
            "mixed": {"enum": ["a", 1]},
            "nullable": {"enum": ["a", null]},
            "only_null": {"enum": [null]},
            "compound": {"enum": [{"a": 1}, [1]]},
            "empty": {"enum": []},
            "not_a_list": {"enum": "a"},
            "bool_and_number": {"enum": [true, 1]}
        }});
        assert_eq!(typed(schema.clone()), schema);
    }

    #[test]
    fn an_existing_type_is_never_replaced() {
        let schema = json!({"properties": {
            "loose": {"type": ["string", "null"], "enum": ["a", null]},
            "declared": {"type": "string", "enum": ["a"]}
        }});
        assert_eq!(typed(schema.clone()), schema);
    }

    #[test]
    fn nested_positions_are_typed() {
        let schema = json!({
            "type": "object",
            "properties": {
                "targets": {"type": "array", "items": {"enum": ["linux", "macos"]}},
                "pair": {"type": "array", "items": [{"enum": ["a"]}, {"enum": [1]}]},
                "priority": {"anyOf": [{"enum": [1, 2]}, {"enum": ["low", "high"]}]},
                "one": {"oneOf": [{"enum": [true]}]},
                "all": {"allOf": [{"enum": ["x"]}]},
                "labels": {"type": "object", "additionalProperties": {"enum": ["on", "off"]}},
                "nested": {"type": "object", "properties": {"deep": {"enum": ["d"]}}},
                "ref": {"$ref": "#/$defs/mode"}
            },
            "$defs": {"mode": {"enum": ["fast", "slow"]}},
            "definitions": {"level": {"enum": [1, 2]}}
        });
        let out = typed(schema);
        let props = &out["properties"];
        assert_eq!(props["targets"]["items"]["type"], "string");
        assert_eq!(props["targets"]["type"], "array");
        assert_eq!(props["pair"]["items"][0]["type"], "string");
        assert_eq!(props["pair"]["items"][1]["type"], "integer");
        assert_eq!(props["priority"]["anyOf"][0]["type"], "integer");
        assert_eq!(props["priority"]["anyOf"][1]["type"], "string");
        assert!(props["priority"].get("type").is_none());
        assert_eq!(props["one"]["oneOf"][0]["type"], "boolean");
        assert_eq!(props["all"]["allOf"][0]["type"], "string");
        assert_eq!(props["labels"]["additionalProperties"]["type"], "string");
        assert_eq!(props["nested"]["properties"]["deep"]["type"], "string");
        assert_eq!(props["ref"], json!({"$ref": "#/$defs/mode"}));
        assert_eq!(out["$defs"]["mode"]["type"], "string");
        assert_eq!(out["definitions"]["level"]["type"], "integer");
    }

    #[test]
    fn a_boolean_additional_properties_and_other_keywords_are_untouched() {
        let schema = json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {"a": {"type": "string"}},
            "not": {"enum": ["x"]},
            "patternProperties": {"^x": {"enum": ["y"]}}
        });
        assert_eq!(typed(schema.clone()), schema);
    }

    #[test]
    fn a_root_enum_is_typed_and_the_input_is_not_changed() {
        let schema = json!({"enum": ["a", "b"]});
        let out = with_enum_types(&schema);
        assert_eq!(out, json!({"enum": ["a", "b"], "type": "string"}));
        assert_eq!(schema, json!({"enum": ["a", "b"]}));
    }
}
