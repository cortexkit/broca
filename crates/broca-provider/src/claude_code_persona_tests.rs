//! Unit checks for the persona primitives against standard hash test values, plus the guard
//! that keeps the module free of I/O and of any dependency outside the agreed four. The
//! shared wire vectors pin the same functions in `tests/it/claude_oauth_wire_vectors.rs`.

use super::*;

/// The module's own source, without this test module, so the guard reads exactly the code that
/// would move to the shared crate.
fn module_source() -> &'static str {
    let source = include_str!("claude_code_persona.rs");
    let tests_at = source
        .find("#[cfg(test)]")
        .expect("the persona module declares its test module last");
    &source[..tests_at]
}

#[test]
fn the_persona_module_imports_nothing_but_std_serde_json_sha2_and_xxhash() {
    // Lower-case path roots that may appear before `::`: the four agreed crates, std's
    // pure `cmp`, and primitive types' associated functions.
    const ALLOWED: [&str; 10] = [
        "std",
        "cmp",
        "serde_json",
        "sha2",
        "xxhash_rust",
        "xxh64",
        "u16",
        "usize",
        "str",
        "i128",
    ];
    for (number, line) in module_source().lines().enumerate() {
        let code = line.split("//").next().unwrap_or_default();
        assert!(
            !code.contains("extern crate"),
            "line {}: no extern crate is allowed: {line}",
            number + 1
        );
        let bytes = code.as_bytes();
        let mut index = 0;
        while let Some(found) = code[index..].find("::") {
            let end = index + found;
            let start = code[..end]
                .rfind(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
                .map_or(0, |at| at + 1);
            let root = &code[start..end];
            let preceded_by_path = start >= 2 && &bytes[start - 2..start] == b"::";
            if !root.is_empty()
                && !preceded_by_path
                && root.starts_with(|ch: char| ch.is_ascii_lowercase())
            {
                assert!(
                    ALLOWED.contains(&root),
                    "line {}: `{root}::` is outside the persona module's allowed dependencies: \
                     {line}",
                    number + 1
                );
            }
            index = end + 2;
        }
        for forbidden in [
            "std::fs",
            "std::io",
            "std::net",
            "std::env",
            "std::process",
            "std::time",
            "std::thread",
            "std::sync",
        ] {
            assert!(
                !code.contains(forbidden),
                "line {}: `{forbidden}` is I/O or ambient state: {line}",
                number + 1
            );
        }
    }
}

#[test]
fn the_persona_module_reaches_no_other_broca_module() {
    // A `crate::` or `super::` path would tie the module to broca and block the move.
    assert!(!module_source().contains("crate::"));
    assert!(!module_source().contains("super::"));
}

#[test]
fn hash_dependencies_match_their_standard_test_values() {
    assert_eq!(
        xxhash_rust::xxh64::xxh64(b"", 0),
        0xef46_db37_51d8_e999,
        "xxHash64 of the empty input, seed 0"
    );
    assert_eq!(
        xxhash_rust::xxh64::xxh64(b"abc", 0),
        0x44bc_2cf5_ad77_0999,
        "xxHash64 of \"abc\", seed 0"
    );
    assert_eq!(
        hex(&Sha256::digest(b"abc")),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        "SHA-256 of \"abc\" (FIPS 180-2)"
    );
}

#[test]
fn suffix_samples_utf16_positions_and_pads_with_zero() {
    // "short" has no index 7 or 20, so both are sampled as '0'.
    assert_eq!(cc_version_suffix("short", "2.1.258"), "587");
    // A character outside the BMP takes two UTF-16 units, which moves every later position.
    let astral = "ab\u{1F600}cdefghijklmnopqrstuvwxyz";
    let bmp = "ab\u{00E9}cdefghijklmnopqrstuvwxyz";
    assert_ne!(
        cc_version_suffix(astral, CLAUDE_CODE_VERSION),
        cc_version_suffix(bmp, CLAUDE_CODE_VERSION)
    );
}

#[test]
fn first_user_text_prefers_a_command_then_the_last_nonempty_text() {
    let messages = serde_json::json!([
        {"role": "assistant", "content": "ignored"},
        {"role": "user", "content": [
            {"type": "text", "text": "first"},
            {"type": "text", "text": "last"},
            {"type": "text", "text": ""}
        ]},
        {"role": "user", "content": "later user message"}
    ]);
    let messages = messages.as_array().unwrap();
    assert_eq!(first_user_message_text(messages), "last");
    let command = serde_json::json!([{"role": "user", "content": [
        {"type": "text", "text": "plain"},
        {"type": "text", "text": "x <command-name>/review</command-name>"}
    ]}]);
    assert_eq!(
        first_user_message_text(command.as_array().unwrap()),
        "<command-name>/review</command-name>"
    );
    assert_eq!(first_user_message_text(&[]), "");
}

#[test]
fn billing_text_writes_only_well_formed_lineage() {
    let prompt = "33333333-3333-4333-8333-333333333333";
    assert_eq!(
        billing_header_text("2.1.280", "d7c", "00000", None, Some(prompt)),
        "x-anthropic-billing-header: cc_version=2.1.280.d7c; cc_entrypoint=cli; cch=00000; \
         cc_prompt_id=33333333-3333-4333-8333-333333333333;"
    );
    assert_eq!(
        billing_header_text(
            "2.1.280",
            "d7c",
            "00000",
            Some("req_short"),
            Some("not-a-uuid")
        ),
        "x-anthropic-billing-header: cc_version=2.1.280.d7c; cc_entrypoint=cli; cch=00000;"
    );
    assert_eq!(
        billing_header_text(
            "2.1.280",
            "d7c",
            "00000",
            Some("req_PLACEHOLDER0000000000000001"),
            Some(prompt)
        ),
        "x-anthropic-billing-header: cc_version=2.1.280.d7c; cc_entrypoint=cli; cch=00000; \
         cc_prev_req=req_PLACEHOLDER0000000000000001; \
         cc_prompt_id=33333333-3333-4333-8333-333333333333;"
    );
}

#[test]
fn placeholder_is_found_only_in_the_first_system_block_with_type_before_text() {
    let signed_shape = r#"{"model":"m","system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.280.abc; cc_entrypoint=cli; cch=00000;"}]}"#;
    let offset = cch_placeholder_offset(signed_shape).expect("billing block found");
    assert_eq!(&signed_shape[offset..offset + 5], "00000");
    let sorted_keys = r#"{"model":"m","system":[{"text":"x-anthropic-billing-header: cc_version=2.1.280.abc; cc_entrypoint=cli; cch=00000;","type":"text"}]}"#;
    assert_eq!(cch_placeholder_offset(sorted_keys), None);
    assert_eq!(
        sign_body(sorted_keys),
        sorted_keys,
        "unsigned when not located"
    );
}

#[test]
fn canonical_body_blanks_model_and_drops_max_tokens_in_place() {
    let body = r#"{"model":"claude-x","messages":[{"role":"user","content":"a,b}"}],"max_tokens":32000,"stream":true}"#;
    assert_eq!(
        cch_canonical_body(body).unwrap(),
        r#"{"model":"","messages":[{"role":"user","content":"a,b}"}],"stream":true}"#
    );
    assert_eq!(cch_canonical_body("[1]"), None);
}

#[test]
fn persona_json_orders_top_level_and_nested_keys() {
    let body = serde_json::json!({
        "stream": true,
        "zeta": 1,
        "system": [{"text": "s", "type": "text", "cache_control": {"ttl": "1h", "type": "ephemeral"}}],
        "model": "m",
        "temperature": 1.0,
        "messages": [{"content": [{"text": "t", "type": "text"}], "role": "user"}],
        "max_tokens": 10
    });
    assert_eq!(
        to_persona_json(&body),
        r#"{"model":"m","messages":[{"role":"user","content":[{"type":"text","text":"t"}]}],"system":[{"type":"text","text":"s","cache_control":{"type":"ephemeral","ttl":"1h"}}],"max_tokens":10,"temperature":1,"stream":true,"zeta":1}"#
    );
}

#[test]
fn user_id_omits_an_unknown_account() {
    assert_eq!(
        metadata_user_id("d", None, "s"),
        r#"{"device_id":"d","session_id":"s"}"#
    );
    assert_eq!(
        metadata_user_id("d", Some("a"), "s"),
        r#"{"device_id":"d","account_uuid":"a","session_id":"s"}"#
    );
}
