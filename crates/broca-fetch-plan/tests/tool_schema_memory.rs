use broca_protocol::request::Tool;
use broca_test_support::alloc::{self, CountingAlloc};
use serde_json::json;

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

#[test]
fn decoded_tool_schemas_hold_near_their_text_size() {
    assert!(alloc::installed());
    let schema = json!({
        "type": "object",
        "required": ["query", "scope"],
        "additionalProperties": false,
        "properties": {
            "query": {"type": "string", "description": "Text or regular expression to search for"},
            "scope": {
                "type": "object",
                "description": "Limits the search to files below the given repository path",
                "required": ["path"],
                "properties": {
                    "path": {"type": "string", "description": "Path relative to the repository root"},
                    "include": {
                        "type": "array",
                        "description": "Only include files matching these glob patterns",
                        "items": {"type": "string"}
                    }
                }
            },
            "options": {
                "type": "object",
                "properties": {
                    "case": {"type": "string", "enum": ["smart", "sensitive", "insensitive"]},
                    "context": {"type": "integer", "minimum": 0, "maximum": 20},
                    "limit": {"type": "integer", "description": "Maximum number of matches to return"}
                }
            }
        }
    });
    let tools: Vec<_> = (0..40)
        .map(|i| {
            json!({
                "name": format!("search_{i}"),
                "description": "Find matching lines in the repository",
                "input_schema": schema,
            })
        })
        .collect();
    let schema_text_bytes = serde_json::to_vec(&schema).unwrap().len() * tools.len();
    let wire = serde_json::to_vec(&tools).unwrap();

    // Count retained allocations, not the temporary trees and buffers used to decode the list.
    let (decoded, counts) = alloc::measure(|| serde_json::from_slice::<Vec<Tool>>(&wire).unwrap());
    assert_eq!(decoded.len(), tools.len());
    assert!(
        counts.held < 2 * schema_text_bytes as isize,
        "decoded tools held {} bytes ({} map-sized blocks), for {schema_text_bytes} bytes of schema text",
        counts.held,
        counts.object_blocks,
    );
}
