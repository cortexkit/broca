//! Encodings for the conversation content types defined in `broca-protocol`.
//!
//! Records store messages as the renderer's own types, so a loaded window renders the same
//! bytes as the run that wrote it, with no conversion layer between storage and rendering.
//! Those types live in another crate, so their encodings are written by hand here instead of
//! with `message!`. Each impl destructures its type exhaustively: adding a field to a protocol
//! type fails to compile here until the field is given a number, so nothing can be silently left
//! out of storage.
//!
//! Field and variant numbers below are part of the stored format. Never renumber one; retire it
//! and take a new number.
//!
//! In-memory members are not stored: `Message::ordinal` and `Message::tags` are derived when a
//! prompt is assembled.

use broca_protocol::content::{
    ContentBlock, EncryptedReasoningFormat, FinishReason, MediaKind, MediaRef, Message, Role,
    TextProviderFormat, TextProviderState, ToolCall, ToolOutput, ToolOutputPart, ToolResult,
};
use broca_protocol::ids::{MessageId, ToolCallId};
use broca_protocol::request::ModelHandle;
use broca_protocol::usage::Usage;

use crate::codec::{
    finish_req, put_nested, put_opt, put_rep, put_req, read_fields, skip_unknown, take_one,
    take_rep, ByteBuf, DecodeError, DecodeErrorKind, Message as CodecMessage, Raw, Value, Wire,
};

/// Implements `Value` for a field-less protocol enum as a varint, with an exhaustive match in
/// each direction so a new variant can't be stored or read under a guessed number.
macro_rules! protocol_values {
    ($ty:ty, $name:literal, { $($num:literal => $var:path),* $(,)? }) => {
        impl Value for $ty {
            const WIRE: Wire = Wire::Varint;
            const NAME: &'static str = $name;
            fn write(&self, out: &mut Vec<u8>) {
                let n: u64 = match self { $( $var => $num, )* };
                crate::codec::put_varint(out, n);
            }
            fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
                let n = u64::read(raw).map_err(|e| DecodeError::new($name, e.kind))?;
                match n {
                    $( $num => Ok($var), )*
                    other => Err(DecodeError::new($name, DecodeErrorKind::UnknownVariant(other))),
                }
            }
        }
    };
}

protocol_values!(Role, "Role", {
    1 => Role::System,
    2 => Role::User,
    3 => Role::Assistant,
    4 => Role::Tool,
});

protocol_values!(MediaKind, "MediaKind", {
    1 => MediaKind::Image,
    2 => MediaKind::Document,
    3 => MediaKind::Audio,
    4 => MediaKind::Video,
});

protocol_values!(EncryptedReasoningFormat, "EncryptedReasoningFormat", {
    1 => EncryptedReasoningFormat::OpenAiResponses,
    2 => EncryptedReasoningFormat::OpenAiResponsesChatgpt,
    3 => EncryptedReasoningFormat::OpenRouter,
});

protocol_values!(TextProviderFormat, "TextProviderFormat", {
    1 => TextProviderFormat::OpenAiResponses,
    2 => TextProviderFormat::OpenAiResponsesChatgpt,
    3 => TextProviderFormat::Gemini,
    4 => TextProviderFormat::GoogleCodeAssist,
});

protocol_values!(FinishReason, "FinishReason", {
    1 => FinishReason::Stop,
    2 => FinishReason::Length,
    3 => FinishReason::ToolCalls,
    4 => FinishReason::ContentFilter,
    5 => FinishReason::Error,
    6 => FinishReason::Other,
});

/// Newtype ids are their string.
macro_rules! string_newtype {
    ($ty:ident) => {
        impl Value for $ty {
            const WIRE: Wire = Wire::Len;
            const NAME: &'static str = stringify!($ty);
            fn write(&self, out: &mut Vec<u8>) {
                self.0.write(out);
            }
            fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
                String::read(raw).map($ty)
            }
            fn is_default(&self) -> bool {
                self.0.is_empty()
            }
            fn missing() -> Option<Self> {
                Some($ty(String::new()))
            }
        }
    };
}
string_newtype!(MessageId);
string_newtype!(ToolCallId);

/// Opaque provider text: signatures, thought signatures and reasoning. Providers send its bulk
/// as base64, either as the whole string (Anthropic signatures and Gemini thought signatures, in
/// the standard alphabet) or as one long run inside a JSON item (OpenAI's `encrypted_content`, in
/// the URL-safe alphabet). Base64 costs a third more than the bytes it encodes, and opaque
/// reasoning is about a quarter of today's log, so the longest base64 run is stored as its bytes,
/// with the text before and after it kept as text:
///
/// `text = before (field 1) + base64(bytes) (field 2 standard, field 4 URL-safe) + after (field 3)`
///
/// A run is used only when re-encoding its bytes gives back exactly the same characters, so the
/// string always reads back byte for byte. Shorter runs aren't worth a field and stay text.
fn put_opaque(out: &mut Vec<u8>, number: u32, text: &str) {
    put_nested(out, number, false, |inner| match longest_base64_run(text) {
        Some(run) => {
            put_req(inner, 1, false, &text[..run.start].to_owned());
            let field = match run.alphabet {
                Alphabet::Standard => 2,
                Alphabet::UrlSafe => 4,
            };
            put_req(inner, field, false, &ByteBuf(run.bytes));
            put_req(inner, 3, false, &text[run.end..].to_owned());
        }
        None => put_req(inner, 1, false, &text.to_owned()),
    });
}

fn read_opaque(raw: Raw<'_>) -> Result<String, DecodeError> {
    const AT: &str = "opaque";
    let mut before: Option<String> = None;
    let mut standard: Option<ByteBuf> = None;
    let mut url_safe: Option<ByteBuf> = None;
    let mut after: Option<String> = None;
    read_fields(raw, AT, |field| match field.number {
        1 => take_one(&mut before, field, AT),
        2 => take_one(&mut standard, field, AT),
        3 => take_one(&mut after, field, AT),
        4 => take_one(&mut url_safe, field, AT),
        _ => skip_unknown(field, AT),
    })?;
    let mut text = before.unwrap_or_default();
    match (standard, url_safe) {
        (Some(_), Some(_)) => return Err(DecodeError::new(AT, DecodeErrorKind::OneofMultiple)),
        (Some(bytes), None) => text.push_str(&base64_encode(&bytes.0, Alphabet::Standard)),
        (None, Some(bytes)) => text.push_str(&base64_encode(&bytes.0, Alphabet::UrlSafe)),
        (None, None) => {}
    }
    text.push_str(&after.unwrap_or_default());
    Ok(text)
}

/// Base64 runs shorter than this stay text: the two extra fields would cost about what the
/// bytes save.
const MIN_BASE64_RUN: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Alphabet {
    /// RFC 4648 section 4: `+` and `/`.
    Standard,
    /// RFC 4648 section 5: `-` and `_`.
    UrlSafe,
}

impl Alphabet {
    fn chars(self) -> &'static [u8; 64] {
        match self {
            Alphabet::Standard => {
                b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
            }
            Alphabet::UrlSafe => {
                b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
            }
        }
    }

    fn value(self, c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            _ => self.chars()[62..]
                .iter()
                .position(|&x| x == c)
                .map(|i| 62 + i as u8),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Base64Run {
    start: usize,
    end: usize,
    alphabet: Alphabet,
    bytes: Vec<u8>,
}

/// The longest run of base64 in `text`, in either alphabet, that is at least `MIN_BASE64_RUN`
/// characters and re-encodes exactly.
fn longest_base64_run(text: &str) -> Option<Base64Run> {
    [Alphabet::Standard, Alphabet::UrlSafe]
        .into_iter()
        .filter_map(|alphabet| longest_run_in(text, alphabet))
        .max_by_key(|run| run.end - run.start)
}

fn longest_run_in(text: &str, alphabet: Alphabet) -> Option<Base64Run> {
    let bytes = text.as_bytes();
    let in_alphabet = |c: u8| alphabet.value(c).is_some();
    let mut best: Option<(usize, usize)> = None;
    let mut i = 0;
    while i < bytes.len() {
        if !in_alphabet(bytes[i]) {
            i += 1;
            continue;
        }
        let start = i;
        while i < bytes.len() && in_alphabet(bytes[i]) {
            i += 1;
        }
        let mut end = i;
        while end < bytes.len() && end - i < 2 && bytes[end] == b'=' {
            end += 1;
        }
        if best.is_none_or(|(s, e)| end - start > e - s) {
            best = Some((start, end));
        }
        i = end.max(i);
    }
    let (start, mut end) = best?;
    // A run whose length isn't a multiple of four may still hold a canonical run if a trailing
    // alphabet character belongs to the text after it, as in a word directly following the
    // ciphertext; trimming to the largest multiple of four keeps that case.
    if (end - start) % 4 != 0 && !text[start..end].ends_with('=') {
        end = start + (end - start) / 4 * 4;
    }
    if end - start < MIN_BASE64_RUN {
        return None;
    }
    canonical_base64(&text[start..end], alphabet).map(|bytes| Base64Run {
        start,
        end,
        alphabet,
        bytes,
    })
}

fn base64_encode(bytes: &[u8], alphabet: Alphabet) -> String {
    let chars = alphabet.chars();
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(chars[((n >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// The bytes of `text` when it is padded base64 in `alphabet` that re-encodes to itself exactly.
fn canonical_base64(text: &str, alphabet: Alphabet) -> Option<Vec<u8>> {
    let input = text.as_bytes();
    if input.is_empty() || !input.len().is_multiple_of(4) {
        return None;
    }
    let mut out = Vec::with_capacity(input.len() / 4 * 3);
    for chunk in input.chunks(4) {
        let mut n: u32 = 0;
        let mut pad = 0;
        for (i, &c) in chunk.iter().enumerate() {
            let v = match c {
                b'=' if i >= 2 => {
                    pad += 1;
                    0
                }
                _ => alphabet.value(c)?,
            };
            if pad > 0 && c != b'=' {
                return None;
            }
            n = (n << 6) | u32::from(v);
        }
        out.push((n >> 16) as u8);
        if pad < 2 {
            out.push((n >> 8) as u8);
        }
        if pad < 1 {
            out.push(n as u8);
        }
    }
    // Padding is only legal in the last chunk, and unused low bits must be zero: checking the
    // round trip covers both.
    (base64_encode(&out, alphabet) == text).then_some(out)
}

impl Value for ModelHandle {
    const WIRE: Wire = Wire::Len;
    const NAME: &'static str = "ModelHandle";
    fn write(&self, out: &mut Vec<u8>) {
        let ModelHandle {
            provider_module_id,
            model_id,
        } = self;
        put_req(out, 1, false, provider_module_id);
        put_req(out, 2, false, model_id);
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        const AT: &str = "ModelHandle";
        let (mut provider, mut model) = (None, None);
        read_fields(raw, AT, |field| match field.number {
            1 => take_one(&mut provider, field, AT),
            2 => take_one(&mut model, field, AT),
            _ => skip_unknown(field, AT),
        })?;
        Ok(ModelHandle {
            provider_module_id: finish_req(provider, AT)?,
            model_id: finish_req(model, AT)?,
        })
    }
}
impl CodecMessage for ModelHandle {}

impl Value for Usage {
    const WIRE: Wire = Wire::Len;
    const NAME: &'static str = "Usage";
    fn write(&self, out: &mut Vec<u8>) {
        let Usage {
            input_tokens,
            cached_input_tokens,
            cache_write_tokens,
            output_tokens,
            reasoning_tokens,
        } = self;
        put_opt(out, 1, false, input_tokens);
        put_opt(out, 2, false, cached_input_tokens);
        put_opt(out, 3, false, cache_write_tokens);
        put_opt(out, 4, false, output_tokens);
        put_opt(out, 5, false, reasoning_tokens);
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        const AT: &str = "Usage";
        let mut usage = Usage::default();
        read_fields(raw, AT, |field| match field.number {
            1 => take_one(&mut usage.input_tokens, field, AT),
            2 => take_one(&mut usage.cached_input_tokens, field, AT),
            3 => take_one(&mut usage.cache_write_tokens, field, AT),
            4 => take_one(&mut usage.output_tokens, field, AT),
            5 => take_one(&mut usage.reasoning_tokens, field, AT),
            _ => skip_unknown(field, AT),
        })?;
        Ok(usage)
    }
}
impl CodecMessage for Usage {}

impl Value for MediaRef {
    const WIRE: Wire = Wire::Len;
    const NAME: &'static str = "MediaRef";
    fn write(&self, out: &mut Vec<u8>) {
        let MediaRef {
            kind,
            mime,
            byte_size,
            sha256,
            width,
            height,
        } = self;
        put_req(out, 1, false, kind);
        put_req(out, 2, false, mime);
        put_req(out, 3, false, byte_size);
        put_req(out, 4, false, sha256);
        put_opt(out, 5, false, width);
        put_opt(out, 6, false, height);
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        const AT: &str = "MediaRef";
        let (mut kind, mut mime, mut size, mut sha) = (None, None, None, None);
        let (mut width, mut height) = (None, None);
        read_fields(raw, AT, |field| match field.number {
            1 => take_one(&mut kind, field, AT),
            2 => take_one(&mut mime, field, AT),
            3 => take_one(&mut size, field, AT),
            4 => take_one(&mut sha, field, AT),
            5 => take_one(&mut width, field, AT),
            6 => take_one(&mut height, field, AT),
            _ => skip_unknown(field, AT),
        })?;
        let kind: MediaKind = finish_req(kind, AT)?;
        // The same rule the protocol's own decoder enforces: only images have dimensions, and
        // an image always has both.
        let dimensions_valid = if kind == MediaKind::Image {
            width.is_some() && height.is_some()
        } else {
            width.is_none() && height.is_none()
        };
        if !dimensions_valid {
            return Err(DecodeError::new(
                AT,
                DecodeErrorKind::BadValue("dimensions must be present exactly for images"),
            ));
        }
        Ok(MediaRef {
            kind,
            mime: finish_req(mime, AT)?,
            byte_size: finish_req(size, AT)?,
            sha256: finish_req(sha, AT)?,
            width,
            height,
        })
    }
}

impl Value for TextProviderState {
    const WIRE: Wire = Wire::Len;
    const NAME: &'static str = "TextProviderState";
    fn write(&self, out: &mut Vec<u8>) {
        let TextProviderState { format, data } = self;
        put_req(out, 1, false, format);
        put_opaque(out, 2, data);
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        const AT: &str = "TextProviderState";
        let mut format = None;
        let mut data = None;
        read_fields(raw, AT, |field| match field.number {
            1 => take_one(&mut format, field, AT),
            2 => take_opaque(&mut data, field, AT),
            _ => skip_unknown(field, AT),
        })?;
        Ok(TextProviderState {
            format: finish_req(format, AT)?,
            data: data.unwrap_or_default(),
        })
    }
}

fn take_opaque(
    slot: &mut Option<String>,
    field: crate::codec::FieldRef<'_>,
    at: &'static str,
) -> Result<(), DecodeError> {
    if slot.is_some() {
        return Err(DecodeError::new(at, DecodeErrorKind::Duplicate));
    }
    *slot = Some(read_opaque(field.raw)?);
    Ok(())
}

impl Value for ToolCall {
    const WIRE: Wire = Wire::Len;
    const NAME: &'static str = "ToolCall";
    fn write(&self, out: &mut Vec<u8>) {
        let ToolCall {
            tool_call_id,
            tool_name,
            input,
            thought_signature,
        } = self;
        put_req(out, 1, false, tool_call_id);
        put_req(out, 2, false, tool_name);
        // Always written: a Null input and an absent one must not be confused on resume.
        crate::codec::put_one(out, 3, false, input);
        if let Some(signature) = thought_signature {
            put_opaque(out, 4, signature);
        }
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        const AT: &str = "ToolCall";
        let (mut id, mut name, mut input, mut signature) = (None, None, None, None);
        read_fields(raw, AT, |field| match field.number {
            1 => take_one(&mut id, field, AT),
            2 => take_one(&mut name, field, AT),
            3 => take_one(&mut input, field, AT),
            4 => take_opaque(&mut signature, field, AT),
            _ => skip_unknown(field, AT),
        })?;
        Ok(ToolCall {
            tool_call_id: finish_req(id, AT)?,
            tool_name: finish_req(name, AT)?,
            input: finish_req(input, AT)?,
            thought_signature: signature,
        })
    }
}

impl Value for ToolOutputPart {
    const WIRE: Wire = Wire::Len;
    const NAME: &'static str = "ToolOutputPart";
    fn write(&self, out: &mut Vec<u8>) {
        match self {
            ToolOutputPart::Text { text } => {
                put_nested(out, 1, false, |inner| put_req(inner, 1, false, text))
            }
            ToolOutputPart::Media { media } => crate::codec::put_one(out, 2, false, media),
        }
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        const AT: &str = "ToolOutputPart";
        read_variant(raw, AT, |number, raw| match number {
            1 => {
                let mut text = None;
                read_fields(raw, AT, |field| match field.number {
                    1 => take_one(&mut text, field, AT),
                    _ => skip_unknown(field, AT),
                })?;
                Ok(ToolOutputPart::Text {
                    text: finish_req(text, AT)?,
                })
            }
            2 => Ok(ToolOutputPart::Media {
                media: MediaRef::read(raw)?,
            }),
            other => Err(DecodeError::new(
                AT,
                DecodeErrorKind::UnknownVariant(u64::from(other)),
            )),
        })
    }
}

impl Value for ToolOutput {
    const WIRE: Wire = Wire::Len;
    const NAME: &'static str = "ToolOutput";
    fn write(&self, out: &mut Vec<u8>) {
        match self {
            ToolOutput::Text { text } => {
                put_nested(out, 1, false, |inner| put_req(inner, 1, false, text))
            }
            ToolOutput::Json { value } => put_nested(out, 2, false, |inner| {
                crate::codec::put_one(inner, 1, false, value)
            }),
            ToolOutput::Parts { parts } => {
                put_nested(out, 3, false, |inner| put_rep(inner, 1, false, parts))
            }
            ToolOutput::Denied { reason } => {
                put_nested(out, 4, false, |inner| put_req(inner, 1, false, reason))
            }
            ToolOutput::Error {
                message,
                module,
                code,
                detail,
                nothing_sent,
                retry,
            } => put_nested(out, 5, false, |inner| {
                put_req(inner, 1, false, message);
                put_opt(inner, 2, false, module);
                put_opt(inner, 3, false, code);
                put_opt(inner, 4, false, detail);
                put_opt(inner, 5, false, nothing_sent);
                put_opt(inner, 6, false, retry);
            }),
        }
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        const AT: &str = "ToolOutput";
        read_variant(raw, AT, |number, raw| match number {
            1 => {
                let mut text = None;
                read_fields(raw, AT, |f| match f.number {
                    1 => take_one(&mut text, f, AT),
                    _ => skip_unknown(f, AT),
                })?;
                Ok(ToolOutput::Text {
                    text: finish_req(text, AT)?,
                })
            }
            2 => {
                let mut value = None;
                read_fields(raw, AT, |f| match f.number {
                    1 => take_one(&mut value, f, AT),
                    _ => skip_unknown(f, AT),
                })?;
                Ok(ToolOutput::Json {
                    value: finish_req(value, AT)?,
                })
            }
            3 => {
                let mut parts = Vec::new();
                read_fields(raw, AT, |f| match f.number {
                    1 => take_rep(&mut parts, f, AT),
                    _ => skip_unknown(f, AT),
                })?;
                Ok(ToolOutput::Parts { parts })
            }
            4 => {
                let mut reason = None;
                read_fields(raw, AT, |f| match f.number {
                    1 => take_one(&mut reason, f, AT),
                    _ => skip_unknown(f, AT),
                })?;
                Ok(ToolOutput::Denied {
                    reason: finish_req(reason, AT)?,
                })
            }
            5 => {
                let (mut message, mut module, mut code) = (None, None, None);
                let (mut detail, mut nothing_sent, mut retry) = (None, None, None);
                read_fields(raw, AT, |f| match f.number {
                    1 => take_one(&mut message, f, AT),
                    2 => take_one(&mut module, f, AT),
                    3 => take_one(&mut code, f, AT),
                    4 => take_one(&mut detail, f, AT),
                    5 => take_one(&mut nothing_sent, f, AT),
                    6 => take_one(&mut retry, f, AT),
                    _ => skip_unknown(f, AT),
                })?;
                Ok(ToolOutput::Error {
                    message: finish_req(message, AT)?,
                    module,
                    code,
                    detail,
                    nothing_sent,
                    retry,
                })
            }
            other => Err(DecodeError::new(
                AT,
                DecodeErrorKind::UnknownVariant(u64::from(other)),
            )),
        })
    }
}

impl Value for ToolResult {
    const WIRE: Wire = Wire::Len;
    const NAME: &'static str = "ToolResult";
    fn write(&self, out: &mut Vec<u8>) {
        let ToolResult {
            tool_call_id,
            output,
            is_error,
        } = self;
        put_req(out, 1, false, tool_call_id);
        crate::codec::put_one(out, 2, false, output);
        put_req(out, 3, false, is_error);
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        const AT: &str = "ToolResult";
        let (mut id, mut output, mut is_error) = (None, None, None);
        read_fields(raw, AT, |f| match f.number {
            1 => take_one(&mut id, f, AT),
            2 => take_one(&mut output, f, AT),
            3 => take_one(&mut is_error, f, AT),
            _ => skip_unknown(f, AT),
        })?;
        Ok(ToolResult {
            tool_call_id: finish_req(id, AT)?,
            output: finish_req(output, AT)?,
            is_error: finish_req(is_error, AT)?,
        })
    }
}

/// Reads a one-of message: exactly one field, whose number selects the variant.
fn read_variant<T>(
    raw: Raw<'_>,
    at: &'static str,
    variant: impl FnOnce(u32, Raw<'_>) -> Result<T, DecodeError>,
) -> Result<T, DecodeError> {
    let mut only: Option<crate::codec::FieldRef<'_>> = None;
    read_fields(raw, at, |field| {
        if only.is_some() {
            return Err(DecodeError::new(at, DecodeErrorKind::OneofMultiple));
        }
        only = Some(field);
        Ok(())
    })?;
    let field = only.ok_or_else(|| DecodeError::new(at, DecodeErrorKind::OneofEmpty))?;
    if !matches!(field.raw, Raw::Len(_)) {
        return Err(DecodeError::new(
            at,
            DecodeErrorKind::BadValue("variant payload is not a message"),
        ));
    }
    variant(field.number, field.raw)
}

impl Value for ContentBlock {
    const WIRE: Wire = Wire::Len;
    const NAME: &'static str = "ContentBlock";
    fn write(&self, out: &mut Vec<u8>) {
        match self {
            ContentBlock::Media(media) => crate::codec::put_one(out, 1, false, media),
            ContentBlock::Text { text } => {
                put_nested(out, 2, false, |inner| put_req(inner, 1, false, text))
            }
            ContentBlock::TextWithState {
                text,
                provider_state,
            } => put_nested(out, 3, false, |inner| {
                put_req(inner, 1, false, text);
                crate::codec::put_one(inner, 2, false, provider_state);
            }),
            ContentBlock::Reasoning { text, signature } => put_nested(out, 4, false, |inner| {
                put_req(inner, 1, false, text);
                if let Some(signature) = signature {
                    put_opaque(inner, 2, signature);
                }
            }),
            ContentBlock::RedactedReasoning {
                data,
                provider_format,
            } => put_nested(out, 5, false, |inner| {
                put_opaque(inner, 1, data);
                put_opt(inner, 2, false, provider_format);
            }),
            ContentBlock::ToolCall(call) => crate::codec::put_one(out, 6, false, call),
            ContentBlock::ToolResult(result) => crate::codec::put_one(out, 7, false, result),
            ContentBlock::ProviderFallback {
                from_model,
                to_model,
                raw,
            } => put_nested(out, 8, false, |inner| {
                put_req(inner, 1, false, from_model);
                put_req(inner, 2, false, to_model);
                put_opt(inner, 3, false, raw);
            }),
        }
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        const AT: &str = "ContentBlock";
        read_variant(raw, AT, |number, raw| match number {
            1 => Ok(ContentBlock::Media(MediaRef::read(raw)?)),
            2 => {
                let mut text = None;
                read_fields(raw, AT, |f| match f.number {
                    1 => take_one(&mut text, f, AT),
                    _ => skip_unknown(f, AT),
                })?;
                Ok(ContentBlock::Text {
                    text: finish_req(text, AT)?,
                })
            }
            3 => {
                let (mut text, mut state) = (None, None);
                read_fields(raw, AT, |f| match f.number {
                    1 => take_one(&mut text, f, AT),
                    2 => take_one(&mut state, f, AT),
                    _ => skip_unknown(f, AT),
                })?;
                Ok(ContentBlock::TextWithState {
                    text: finish_req(text, AT)?,
                    provider_state: finish_req(state, AT)?,
                })
            }
            4 => {
                let (mut text, mut signature) = (None, None);
                read_fields(raw, AT, |f| match f.number {
                    1 => take_one(&mut text, f, AT),
                    2 => take_opaque(&mut signature, f, AT),
                    _ => skip_unknown(f, AT),
                })?;
                Ok(ContentBlock::Reasoning {
                    text: finish_req(text, AT)?,
                    signature,
                })
            }
            5 => {
                let (mut data, mut format) = (None, None);
                read_fields(raw, AT, |f| match f.number {
                    1 => take_opaque(&mut data, f, AT),
                    2 => take_one(&mut format, f, AT),
                    _ => skip_unknown(f, AT),
                })?;
                Ok(ContentBlock::RedactedReasoning {
                    data: data.unwrap_or_default(),
                    provider_format: format,
                })
            }
            6 => Ok(ContentBlock::ToolCall(ToolCall::read(raw)?)),
            7 => Ok(ContentBlock::ToolResult(ToolResult::read(raw)?)),
            8 => {
                let (mut from, mut to, mut fallback_raw) = (None, None, None);
                read_fields(raw, AT, |f| match f.number {
                    1 => take_one(&mut from, f, AT),
                    2 => take_one(&mut to, f, AT),
                    3 => take_one(&mut fallback_raw, f, AT),
                    _ => skip_unknown(f, AT),
                })?;
                Ok(ContentBlock::ProviderFallback {
                    from_model: finish_req(from, AT)?,
                    to_model: finish_req(to, AT)?,
                    raw: fallback_raw,
                })
            }
            other => Err(DecodeError::new(
                AT,
                DecodeErrorKind::UnknownVariant(u64::from(other)),
            )),
        })
    }
}

impl Value for Message {
    const WIRE: Wire = Wire::Len;
    const NAME: &'static str = "Message";
    fn write(&self, out: &mut Vec<u8>) {
        let Message {
            role,
            content,
            turn_system,
            cache_prefix_blocks,
            origin,
            // Derived when a prompt is assembled; never stored.
            ordinal: _,
            tags: _,
        } = self;
        put_req(out, 1, false, role);
        put_rep(out, 2, false, content);
        put_req(out, 3, false, turn_system);
        put_opt(out, 4, false, cache_prefix_blocks);
        put_opt(out, 5, false, origin);
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        const AT: &str = "Message";
        let (mut role, mut content) = (None, Vec::new());
        let (mut turn_system, mut prefix, mut origin) = (None, None, None);
        read_fields(raw, AT, |f| match f.number {
            1 => take_one(&mut role, f, AT),
            2 => take_rep(&mut content, f, AT),
            3 => take_one(&mut turn_system, f, AT),
            4 => take_one(&mut prefix, f, AT),
            5 => take_one(&mut origin, f, AT),
            _ => skip_unknown(f, AT),
        })?;
        let mut message = Message::new(finish_req(role, AT)?, content);
        message.turn_system = finish_req(turn_system, AT)?;
        message.cache_prefix_blocks = prefix;
        message.origin = origin;
        Ok(message)
    }
}
impl CodecMessage for Message {}
impl CodecMessage for ContentBlock {}
impl CodecMessage for MediaRef {}
impl CodecMessage for ToolCall {}
impl CodecMessage for ToolResult {}
impl CodecMessage for ToolOutput {}
impl CodecMessage for ToolOutputPart {}
impl CodecMessage for TextProviderState {}

#[cfg(test)]
mod tests;
