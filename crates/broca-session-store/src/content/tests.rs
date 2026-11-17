use broca_protocol::content::{
    ContentBlock, EncryptedReasoningFormat, MediaKind, MediaRef, Message, Role, TextProviderFormat,
    TextProviderState, ToolCall, ToolOutput, ToolOutputPart, ToolResult,
};
use broca_protocol::ids::ToolCallId;
use broca_protocol::request::ModelHandle;
use broca_protocol::usage::Usage;
use proptest::prelude::*;

use super::{
    base64_encode, canonical_base64, longest_base64_run, read_opaque, writer_splits_at, Alphabet,
    MIN_BASE64_RUN,
};
use crate::codec::{put_req, ByteBuf, Raw};
use crate::codec::{DecodeErrorKind, Message as CodecMessage};

fn every_block() -> Vec<ContentBlock> {
    let image = MediaRef {
        kind: MediaKind::Image,
        mime: "image/png".into(),
        byte_size: 10,
        sha256: "ab".repeat(32),
        width: Some(1),
        height: Some(0),
    };
    vec![
        ContentBlock::Media(image.clone()),
        ContentBlock::Text {
            text: String::new(),
        },
        ContentBlock::Text {
            text: "hello".into(),
        },
        ContentBlock::TextWithState {
            text: String::new(),
            provider_state: TextProviderState {
                format: TextProviderFormat::Gemini,
                data: "c2lnbmF0dXJl".into(),
            },
        },
        ContentBlock::Reasoning {
            text: "thinking".into(),
            signature: Some("RXFVQ0NrZ0lB".into()),
        },
        ContentBlock::Reasoning {
            text: String::new(),
            signature: Some(String::new()),
        },
        ContentBlock::Reasoning {
            text: "x".into(),
            signature: None,
        },
        ContentBlock::RedactedReasoning {
            data: r#"{"type":"reasoning","encrypted_content":"gAAAA=="}"#.into(),
            provider_format: Some(EncryptedReasoningFormat::OpenAiResponsesChatgpt),
        },
        ContentBlock::RedactedReasoning {
            data: "QUJD".into(),
            provider_format: None,
        },
        ContentBlock::ToolCall(ToolCall {
            tool_call_id: ToolCallId("call_0".into()),
            tool_name: "read".into(),
            input: serde_json::Value::Null.into(),
            thought_signature: Some("QUI=".into()),
        }),
        ContentBlock::ToolCall(ToolCall {
            tool_call_id: ToolCallId("call_1".into()),
            tool_name: "bash".into(),
            input: serde_json::json!({"command": "ls", "n": 1.5, "z": [true, null]}).into(),
            thought_signature: None,
        }),
        ContentBlock::ToolResult(ToolResult {
            tool_call_id: ToolCallId("call_1".into()),
            output: ToolOutput::Parts {
                parts: vec![
                    ToolOutputPart::Text { text: "a".into() },
                    ToolOutputPart::Media { media: image },
                ],
            },
            is_error: false,
        }),
        ContentBlock::ToolResult(ToolResult {
            tool_call_id: ToolCallId("c".into()),
            output: ToolOutput::Error {
                message: "down".into(),
                module: Some("aft".into()),
                code: None,
                detail: Some(String::new()),
                nothing_sent: Some(false),
                retry: None,
            },
            is_error: true,
        }),
        ContentBlock::ToolResult(ToolResult {
            tool_call_id: ToolCallId("j".into()),
            output: ToolOutput::Json {
                value: serde_json::json!({"k": 0}).into(),
            },
            is_error: false,
        }),
        ContentBlock::ToolResult(ToolResult {
            tool_call_id: ToolCallId("d".into()),
            output: ToolOutput::Denied {
                reason: "policy".into(),
            },
            is_error: true,
        }),
        ContentBlock::ProviderFallback {
            from_model: "claude-a".into(),
            to_model: "claude-b".into(),
            raw: Some(r#"{ "type":"fallback" }"#.into()),
        },
    ]
}

#[test]
fn every_content_block_round_trips() {
    for block in every_block() {
        let bytes = block.encode();
        let back = ContentBlock::decode(&bytes).unwrap();
        assert_eq!(back, block);
        assert_eq!(back.encode(), bytes);
    }
}

#[test]
fn a_message_round_trips_and_drops_only_its_in_memory_members() {
    let mut message = Message::assistant_from(
        every_block(),
        ModelHandle {
            provider_module_id: "openai".into(),
            model_id: "gpt-6.1-sol".into(),
        },
    );
    message.turn_system = true;
    message.cache_prefix_blocks = Some(0);
    message.ordinal = Some(7);
    let back = Message::decode(&message.encode()).unwrap();
    assert_eq!(back.ordinal, None, "the ordinal is derived, never stored");
    message.ordinal = None;
    assert_eq!(back, message);
    assert_eq!(back.cache_prefix_blocks, Some(0));

    let user = Message::new(Role::User, vec![]);
    assert_eq!(Message::decode(&user.encode()).unwrap(), user);
}

#[test]
fn usage_keeps_reported_zero_apart_from_unreported() {
    let usage = Usage {
        input_tokens: Some(0),
        output_tokens: Some(4),
        ..Usage::default()
    };
    assert_eq!(Usage::decode(&usage.encode()).unwrap(), usage);
    assert_eq!(
        Usage::decode(&Usage::default().encode()).unwrap(),
        Usage::default()
    );
}

#[test]
fn usage_before_cache_write_lifetimes_keeps_exact_bytes() {
    // These bytes predate cache-write lifetime counters; absent additions must cost no bytes.
    let pinned = [0x10, 10, 0x20, 20, 0x30, 30, 0x40, 4, 0x50, 2];
    let usage = Usage::decode(&pinned).unwrap();
    assert_eq!(usage.input_tokens, Some(10));
    assert_eq!(usage.cache_write_tokens, Some(30));
    assert_eq!(usage.encode(), pinned);

    // The same decoder used by old builds skips unrecognized optional usage members.
    let mut newer = pinned.to_vec();
    put_req(&mut newer, 99, false, &20u64);
    assert_eq!(Usage::decode(&newer).unwrap(), usage);
    assert_eq!(Usage::decode(&newer).unwrap().encode(), pinned);
}

#[test]
fn cache_write_lifetimes_are_optional_in_the_usage_codec() {
    let pinned = [0x30, 30, 0x60, 0, 0x70, 30];
    let usage = Usage::decode(&pinned).unwrap();
    assert_eq!(usage.cache_write_tokens, Some(30));
    assert_eq!(usage.cache_write_5m_tokens, Some(0));
    assert_eq!(usage.cache_write_1h_tokens, Some(30));
    assert_eq!(usage.encode(), pinned);

    // Freeze the old Usage reader's field dispatch: ids 6 and 7 are optional unknown fields.
    let mut old = Usage::default();
    crate::codec::read_fields(Raw::Len(&pinned), "old Usage", |field| match field.number {
        1 => crate::codec::take_one(&mut old.input_tokens, field, "old Usage"),
        2 => crate::codec::take_one(&mut old.cached_input_tokens, field, "old Usage"),
        3 => crate::codec::take_one(&mut old.cache_write_tokens, field, "old Usage"),
        4 => crate::codec::take_one(&mut old.output_tokens, field, "old Usage"),
        5 => crate::codec::take_one(&mut old.reasoning_tokens, field, "old Usage"),
        _ => crate::codec::skip_unknown(field, "old Usage"),
    })
    .unwrap();
    assert_eq!(old.cache_write_tokens, Some(30));
    assert_eq!(old.cache_write_5m_tokens, None);
    assert_eq!(old.cache_write_1h_tokens, None);
}

#[test]
fn opaque_text_is_stored_as_bytes_only_when_it_re_encodes_exactly() {
    let standard = |t| canonical_base64(t, Alphabet::Standard);
    let url_safe = |t| canonical_base64(t, Alphabet::UrlSafe);
    assert_eq!(standard("QUJD"), Some(b"ABC".to_vec()));
    assert_eq!(standard("QUI="), Some(b"AB".to_vec()));
    assert_eq!(standard("QQ=="), Some(b"A".to_vec()));
    // Non-zero unused bits, padding mid-string, the other alphabet, missing padding.
    for text in ["QUJ=", "QQ==QUJD", "-_8=", "QUJ", "", "QU I="] {
        assert_eq!(standard(text), None, "{text:?}");
    }
    assert_eq!(url_safe("-_8="), Some(vec![0xfb, 0xff]));
    assert_eq!(url_safe("+/8="), None);
    for bytes in [&b"a"[..], b"ab", b"abc", b"\xff\x00\x10", b"\xfb\xff\xbf"] {
        for alphabet in [Alphabet::Standard, Alphabet::UrlSafe] {
            let text = base64_encode(bytes, alphabet);
            assert_eq!(canonical_base64(&text, alphabet).as_deref(), Some(bytes));
        }
    }
}

fn reasoning_item(ciphertext: &str) -> String {
    format!(r#"{{"id":"rs_1","type":"reasoning","summary":[],"encrypted_content":"{ciphertext}"}}"#)
}

#[test]
fn the_ciphertext_inside_a_reasoning_item_is_stored_as_bytes_and_reads_back_exactly() {
    let payload: Vec<u8> = (0..600u32).map(|i| (i * 7 + i / 3) as u8).collect();
    // OpenAI's encrypted reasoning is URL-safe base64; signatures use the standard alphabet.
    for alphabet in [Alphabet::UrlSafe, Alphabet::Standard] {
        let ciphertext = base64_encode(&payload, alphabet);
        assert!(
            alphabet == Alphabet::Standard || ciphertext.contains(['-', '_']),
            "the fixture exercises the URL-safe characters"
        );
        let item = reasoning_item(&ciphertext);
        let run = longest_base64_run(&item).unwrap();
        assert_eq!(&item[run.start..run.end], ciphertext);
        assert_eq!((run.alphabet, run.bytes.len()), (alphabet, 600));

        let block = ContentBlock::RedactedReasoning {
            data: item.clone(),
            provider_format: Some(EncryptedReasoningFormat::OpenAiResponses),
        };
        let encoded = block.encode();
        assert!(
            encoded.len() < item.len() - 150,
            "{} vs {}",
            encoded.len(),
            item.len()
        );
        assert_eq!(ContentBlock::decode(&encoded).unwrap(), block);
    }
}

#[test]
fn short_or_non_canonical_runs_stay_text() {
    assert_eq!(
        longest_base64_run("QUJD"),
        None,
        "shorter than the minimum run"
    );
    // 64 characters whose last one leaves non-zero unused bits.
    let mut run = "A".repeat(63);
    run.push_str("B=");
    assert_eq!(longest_base64_run(&run), None);
    // A word directly after the ciphertext belongs to the text after it.
    let ciphertext = base64_encode(&[5u8; 60], Alphabet::Standard);
    let text = format!("{ciphertext}xyz");
    let run = longest_base64_run(&text).unwrap();
    assert_eq!((run.start, &text[run.end..]), (0, "xyz"));
}

#[test]
fn both_byte_fields_in_one_opaque_value_refuse() {
    let mut inner = Vec::new();
    put_req(&mut inner, 2, false, &ByteBuf(vec![1]));
    put_req(&mut inner, 4, false, &ByteBuf(vec![1]));
    let error = read_opaque(Raw::Len(&inner)).unwrap_err();
    assert_eq!(error.kind, DecodeErrorKind::OneofMultiple);
}

#[test]
fn an_image_without_dimensions_and_a_document_with_them_are_refused() {
    let mut image = MediaRef {
        kind: MediaKind::Image,
        mime: "image/png".into(),
        byte_size: 1,
        sha256: "00".into(),
        width: Some(1),
        height: Some(1),
    };
    let mut document = image.clone();
    image.height = None;
    document.kind = MediaKind::Document;
    for media in [image, document] {
        let kind = MediaRef::decode(&media.encode()).unwrap_err().kind;
        assert!(matches!(kind, DecodeErrorKind::BadValue(_)), "{kind:?}");
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 2_000, ..ProptestConfig::default() })]

    #[test]
    fn any_opaque_text_reads_back_identically(text in ".{0,64}|[A-Za-z0-9+/]{0,40}={0,2}") {
        let block = ContentBlock::RedactedReasoning { data: text.clone(), provider_format: None };
        let back = ContentBlock::decode(&block.encode()).unwrap();
        prop_assert_eq!(back, block);
    }

    #[test]
    fn arbitrary_bytes_never_panic_a_message_decode(
        bytes in proptest::collection::vec(any::<u8>(), 0..512)
    ) {
        let _ = Message::decode(&bytes);
        let _ = ContentBlock::decode(&bytes);
    }
}

/// `writer_splits_at` accepts a stored split exactly when the writer's own choice
/// (`longest_base64_run`) is that split, over texts built from runs of either alphabet's own
/// characters, shared characters, padding and other text, for every canonical run in them.
#[test]
fn the_reader_accepts_exactly_the_splits_the_writer_makes() {
    let mut seed = 0x2545_f491_4f6c_dd1du64;
    let mut next = |bound: usize| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        (seed % bound as u64) as usize
    };
    let piece = |kind: usize, length: usize| -> String {
        let chars: &[u8] = match kind {
            0 => b"ABCxyz019+/",
            1 => b"ABCxyz019-_",
            2 => b"ABCDEFxyz0123",
            3 => b"==",
            _ => b" \":{}.",
        };
        (0..length)
            .map(|i| chars[(i * 7 + length) % chars.len()] as char)
            .collect()
    };
    let mut checked = 0;
    for _ in 0..60 {
        let mut text = String::new();
        for _ in 0..1 + next(4) {
            let kind = next(5);
            let length = if kind >= 3 {
                1 + next(3)
            } else {
                56 + next(20)
            };
            text.push_str(&piece(kind, length));
        }
        let writer = longest_base64_run(&text);
        for alphabet in [Alphabet::Standard, Alphabet::UrlSafe] {
            for start in 0..text.len() {
                for end in (start + MIN_BASE64_RUN..=text.len()).step_by(4) {
                    if canonical_base64(&text[start..end], alphabet).is_none() {
                        continue;
                    }
                    let expected = writer.as_ref().is_some_and(|run| {
                        (run.alphabet, run.start, run.end) == (alphabet, start, end)
                    });
                    assert_eq!(
                        writer_splits_at(&text, (start, end), alphabet),
                        expected,
                        "{text:?} at {start}..{end} in {alphabet:?}"
                    );
                    checked += 1;
                }
            }
        }
    }
    assert!(checked > 100, "only {checked} canonical runs were checked");
}
