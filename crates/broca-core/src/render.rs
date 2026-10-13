//! The deterministic provider-request renderer + the prefix/cache hash.
//!
//! Cache stability on resume requires the request BYTES to be byte-identical
//! across a no-crash run and a resumed run. So [`Provider::render_request`] is
//! the ONE renderer used for live-send AND the cache/resume hash AND tests — they
//! cannot drift, because they are the same function. The hash is computed over
//! the renderer's OUTPUT (the renderer IS the hash domain), never over semantic
//! struct equality.
//!
//! This module provides the SHARED canonical renderer a provider may use (the
//! stub does; a real provider overrides with its own wire shape but MUST stay
//! deterministic) plus the digest.

use broca_protocol::{canonical_json_bytes, CallOptions};
use sha2::{Digest, Sha256};

/// Version of the shared canonical renderer. Recorded in the WAL `run_config`
/// (`renderer_version`); a renderer change is a resume BUST. A provider with its
/// own renderer carries its OWN version.
pub const CANONICAL_RENDERER_VERSION: u32 = 1;

/// Deterministic canonical bytes for a `CallOptions` (object keys sorted at every
/// level, compact, stable number formatting). The default renderer, suitable for
/// the stub and any provider that doesn't impose a bespoke wire shape.
pub fn canonical_request_bytes(opts: &CallOptions) -> Vec<u8> {
    let value = serde_json::to_value(opts).expect("CallOptions is always JSON-serializable");
    canonical_json_bytes(&value)
}

/// The request digest: SHA-256 over the renderer's exact output bytes. This is
/// the cache/resume identity — equal digests ⇒ byte-identical requests ⇒ the
/// provider cache sees the same prefix.
pub fn request_digest(rendered: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(rendered);
    h.finalize().into()
}

/// Lowercase-hex of [`request_digest`] (for record fields / logs / test names).
pub fn request_digest_hex(rendered: &[u8]) -> String {
    request_digest(rendered)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::content::{Message, Role};
    use broca_protocol::request::Tool;
    use serde_json::json;
    use std::sync::Arc;

    fn opts() -> CallOptions {
        let mut o = CallOptions::new(vec![Message {
            cache_prefix_blocks: None,
            role: Role::User,
            content: vec![broca_protocol::content::ContentBlock::Text { text: "hi".into() }],
            origin: None,
            ordinal: None,
        }]);
        o.tools = Arc::new(vec![Tool {
            name: "get_time".into(),
            description: "t".into(),
            input_schema: json!({"type": "object", "properties": {"tz": {"type": "string"}}}),
            module: None,
            execution_mode: Default::default(),
        }]);
        o
    }

    #[test]
    fn render_is_deterministic_and_stable() {
        // GOLDEN: pins the canonical render so cache stability can't silently
        // regress. If this changes intentionally, bump CANONICAL_RENDERER_VERSION.
        let bytes = canonical_request_bytes(&opts());
        let s = String::from_utf8(bytes.clone()).unwrap();
        assert_eq!(
            s,
            r#"{"generation":{},"prompt":[{"content":[{"text":"hi","type":"text"}],"role":"user"}],"response_format":{"type":"text"},"tool_choice":{"type":"auto"},"tools":[{"description":"t","execution_mode":"parallel","input_schema":{"properties":{"tz":{"type":"string"}},"type":"object"},"name":"get_time"}]}"#
        );
        // Re-render is identical (determinism).
        assert_eq!(bytes, canonical_request_bytes(&opts()));
    }

    #[test]
    fn digest_tracks_bytes() {
        let a = canonical_request_bytes(&opts());
        assert_eq!(request_digest(&a), request_digest(&a));
        let mut o2 = opts();
        let mut changed_tools = o2.tools.as_ref().clone();
        changed_tools[0].description = "changed".into();
        o2.tools = Arc::new(changed_tools);
        assert_ne!(
            request_digest(&a),
            request_digest(&canonical_request_bytes(&o2))
        );
    }
}
