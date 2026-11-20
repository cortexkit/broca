//! Check consent answers against frozen tool arguments and recover missing card ids
//! without reopening calls already closed by crash recovery.
use broca_wal::WalRecord;
use serde_json::{json, Value};

/// The returned binding is opaque to Cingulate; broca alone checks all four frozen fields.
pub fn binding(prepared: &WalRecord) -> Value {
    let WalRecord::ToolCallDeferred {
        args_hash,
        module,
        schema_pin,
        tool,
        ..
    } = prepared
    else {
        panic!("consent binding requires ToolCallDeferred")
    };
    json!({"args_hash":args_hash,"module":module,"schema_pin":schema_pin,"tool":tool})
}

#[cfg(test)]
mod tests;
