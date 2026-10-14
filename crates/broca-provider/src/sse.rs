//! Minimal Server-Sent-Events framing — extracts the `data:` payloads from a
//! raw SSE byte buffer.
//!
//! Anthropic emits one JSON object per `data:` line, one event per record
//! (records separated by a blank line); the `event:` line is redundant with the
//! payload's own `type` field, so we key off the payload. The incremental
//! splitter handles a live byte stream, and the offline fixture path runs the same
//! splitter over the whole buffer so both read records identically.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::wire::NormalizeError;

/// Maximum bytes accepted for one live SSE record, including non-data fields.
pub const MAX_SSE_RECORD_BYTES: usize = 8 * 1024 * 1024;

static STREAM_CHUNKS: AtomicU64 = AtomicU64::new(0);

/// Non-empty SSE data payloads decoded by this process since boot.
///
/// This process-local lifetime count resets on restart; it is not a fleet total. Diff two
/// readings to derive a rate.
pub fn stream_chunks() -> u64 {
    STREAM_CHUNKS.load(Ordering::Relaxed)
}

/// Extract each `data:` payload (records split on blank lines; multi-line `data:`
/// joined with `\n`; comment lines starting `:` ignored). A single leading UTF-8 BOM is
/// ignored. Empty payloads and the SSE `[DONE]` sentinel are skipped.
///
/// A framing failure (a record over [`MAX_SSE_RECORD_BYTES`], or a record that is not valid
/// UTF-8) is returned as an error rather than an empty list, so a capture that could not be
/// read is never mistaken for one that carried no events.
pub fn parse_data_events(raw: &str) -> Result<Vec<String>, NormalizeError> {
    let mut decoder = SseDecoder::default();
    let mut events = decoder.push(raw.as_bytes())?;
    events.extend(decoder.flush()?);
    Ok(events)
}

/// Split a record into fields using the SAME end-of-line grammar as the record
/// separator, which `str::lines()` does not: it splits LF and CRLF and treats a BARE
/// CR as ordinary text.
///
/// The two grammars disagreed in one file. `eol_len` below has recognised bare CR as a
/// terminator since the record-boundary fix, so `event: x\rdata: {...}\r\r` was correctly
/// cut into one record -- and then arrived here as a SINGLE line beginning `event:`, so
/// the `data:` prefix never matched and the whole payload was dropped. A silent token
/// loss on every SSE family, reported by the decoder as no progress at all.
///
/// Fixing the record boundary and leaving the field split on `str::lines()` is the same
/// mistake twice in one file: the neighbouring function was written with the same
/// assumption and needed the same correction.
fn record_lines(record: &str) -> impl Iterator<Item = &str> {
    let mut rest = record;
    std::iter::from_fn(move || {
        if rest.is_empty() {
            return None;
        }
        let bytes = rest.as_bytes();
        // memchr2 RATHER THAN A BYTE PREDICATE, because this runs once per SSE record
        // on the token-streaming hot path. The scalar `position` closure scans every
        // byte of the payload when no terminator remains -- which is the whole final
        // JSON line, the largest part of a real delta envelope. Measured on four wire
        // shapes, ns per record, legacy str::lines() | scalar predicate | memchr2:
        //
        //   one data line,    46 B    26.0 |  30.7 | 22.0
        //   event-then-data, 118 B    33.1 |  48.8 | 25.1
        //   three data lines, 60 B    38.9 |  43.4 | 33.0
        //   long JSON delta, 286 B    40.8 | 105.3 | 32.2   <- 2.58x on the common shape
        //
        // The scalar version was a REGRESSION against the `str::lines()` it replaced;
        // this is faster than both. str::lines() is still not an option -- it treats a
        // bare CR as ordinary text, which is the payload-loss defect this split exists
        // to fix -- so the choice was never speed versus correctness.
        let cut = memchr::memchr2(b'\r', b'\n', bytes).unwrap_or(bytes.len());
        let line = &rest[..cut];
        // Consume the terminator, treating CRLF as one token so an empty line is not
        // manufactured between the two bytes.
        rest = match bytes.get(cut..) {
            Some([b'\r', b'\n', ..]) => &rest[cut + 2..],
            Some([_, ..]) => &rest[cut + 1..],
            _ => "",
        };
        Some(line)
    })
}

fn record_data(record: &str) -> Option<String> {
    let mut data_lines: Vec<&str> = Vec::new();
    for line in record_lines(record) {
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("data:") {
            data_lines.push(rest.strip_prefix(' ').unwrap_or(rest));
        }
    }
    (!data_lines.is_empty()).then(|| data_lines.join("\n"))
}

/// Incremental byte-oriented SSE record splitter.
///
/// HTTP chunk boundaries may divide either a UTF-8 code point or any record-separator
/// end-of-line token. The decoder therefore buffers bytes until a complete record arrives;
/// a single leading UTF-8 BOM may be split across pushes and is removed, while later BOM
/// bytes remain part of the stream. It tracks only the possible separator prefix between
/// reads, so an incomplete record is not rescanned from its beginning on every network
/// read.
#[derive(Default)]
pub struct SseDecoder {
    buf: Vec<u8>,
    scan_from: usize,
    /// At most three bytes are held here while deciding whether the stream starts with a BOM.
    bom_probe: Vec<u8>,
    bom_checked: bool,
    data_records: u64,
    comment_records: u64,
    saw_done: bool,
    last_event_type: Option<String>,
    keepalives_after_content: u64,
    event_type_counts: Vec<(String, u64)>,
    #[cfg(test)]
    scanned_positions: usize,
}

impl SseDecoder {
    /// Append raw response bytes and return the data payloads from every newly
    /// completed record. Invalid UTF-8 in a complete record is a wire error.
    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<String>, NormalizeError> {
        if !self.bom_checked {
            let mut prefix = std::mem::take(&mut self.bom_probe);
            prefix.extend_from_slice(chunk);
            const UTF8_BOM: &[u8] = b"\xef\xbb\xbf";

            if prefix.len() < UTF8_BOM.len() && UTF8_BOM.starts_with(&prefix) {
                self.bom_probe = prefix;
                return Ok(Vec::new());
            }

            self.bom_checked = true;
            if prefix.starts_with(UTF8_BOM) {
                self.buf.extend_from_slice(&prefix[UTF8_BOM.len()..]);
            } else {
                self.buf.extend_from_slice(&prefix);
            }
        } else {
            self.buf.extend_from_slice(chunk);
        }
        self.decode_records(false)
    }

    /// Flush any remaining buffered records at the end of the stream.
    ///
    /// At end of stream, a pending trailing carriage return cannot be followed by a line feed,
    /// so it is treated as a valid bare CR line end per the WHATWG SSE specification.
    pub fn flush(&mut self) -> Result<Vec<String>, NormalizeError> {
        if !self.bom_checked {
            self.bom_checked = true;
            let prefix = std::mem::take(&mut self.bom_probe);
            const UTF8_BOM: &[u8] = b"\xef\xbb\xbf";
            if prefix.starts_with(UTF8_BOM) {
                self.buf.extend_from_slice(&prefix[UTF8_BOM.len()..]);
            } else {
                self.buf.extend_from_slice(&prefix);
            }
        }
        self.decode_records(true)
    }

    fn decode_records(&mut self, is_eof: bool) -> Result<Vec<String>, NormalizeError> {
        let mut payloads = Vec::new();
        let mut record_start = 0usize;
        let mut consumed = 0usize;
        let mut cursor = self.scan_from.min(self.buf.len());

        while cursor < self.buf.len() {
            #[cfg(test)]
            {
                self.scanned_positions += 1;
            }
            // MEASURED AND DECLINED (r169). This advances ONE BYTE when an ordinary JSON
            // byte cannot begin a separator, so the scan costs work per wire byte. A variant
            // that fast-forwards to the next CR/LF before calling the recognizer was compiled
            // against this exact source and proved payload-identical at every chunk size from
            // 1 to 682 bytes plus mixed newline grammar: 635.02 ns -> 444.76 ns on a 682-byte
            // seven-payload fixture, a 30% saving on framing.
            //
            // It is still not worth taking, and the arithmetic is the reason rather than a
            // feeling about smallness. 190.26 ns per 682 bytes is 0.279 ns/byte. Production
            // decoded 650,564 non-empty payloads in 28.4 hours (health.check `stream_chunks`,
            // two readings from one launch); at the fixture's own 97 bytes/payload that is
            // 63 MB of wire and 17.7 ms of CPU saved across the whole window — 0.000017% of
            // wall. To save even ONE SECOND over that window the stream would have to average
            // 5,510 bytes per payload, and token deltas are tens of bytes.
            //
            // REVERSE THIS IF the byte volume moves three orders of magnitude — sustained
            // multi-KB payloads, or `stream_chunks` climbing past ~10^9 per day. Both are
            // readable from the same counter that priced it.
            let Some(separator_len) = record_separator_len(&self.buf[cursor..], is_eof) else {
                cursor += 1;
                continue;
            };

            if cursor.saturating_sub(record_start) > MAX_SSE_RECORD_BYTES {
                return Err(record_limit_error());
            }
            let record = std::str::from_utf8(&self.buf[record_start..cursor]).map_err(|error| {
                NormalizeError::Invalid(format!("SSE record is not valid UTF-8: {error}"))
            })?;
            let event_name = record_lines(record)
                .filter_map(|line| {
                    line.strip_prefix("event:")
                        .map(|name| name.strip_prefix(' ').unwrap_or(name))
                })
                .last()
                .filter(|name| !name.is_empty());
            if let Some(name) = event_name {
                self.last_event_type = Some(name.to_owned());
            }
            if let Some(payload) = record_data(record) {
                if event_name.is_none() && !payload.is_empty() {
                    // Deserialize only the envelope discriminator, never retain response content
                    // in diagnostics. Untyped SSE data has the standard event name `message`.
                    #[derive(serde::Deserialize)]
                    struct EnvelopeType {
                        #[serde(rename = "type")]
                        kind: Option<String>,
                    }
                    self.last_event_type = Some(
                        serde_json::from_str::<EnvelopeType>(&payload)
                            .ok()
                            .and_then(|envelope| envelope.kind)
                            .unwrap_or_else(|| "message".into()),
                    );
                }
                if !payload.is_empty() {
                    // COUNTS `[DONE]` TOO, because the increment precedes the check below.
                    // That is one extra per stream, immaterial against a per-second rate but
                    // wrong if anyone reads this as "content chunks": it is DATA PAYLOADS, and
                    // the terminal sentinel is one. Found by a perf worker reading the source
                    // rather than the counter's description, which said non-empty data payload
                    // and was literally true while inviting the other reading.
                    STREAM_CHUNKS.fetch_add(1, Ordering::Relaxed);
                    self.data_records = self.data_records.saturating_add(1);
                    self.keepalives_after_content = 0;
                    let event_type = self.last_event_type.as_deref().unwrap_or("message");
                    observe_event_type(
                        &mut self.event_type_counts,
                        safe_event_type_name(event_type),
                    );
                    if payload == "[DONE]" {
                        self.saw_done = true;
                    } else {
                        payloads.push(payload);
                    }
                } else if record_has_comment(record) {
                    self.comment_records = self.comment_records.saturating_add(1);
                    if self.data_records != 0 {
                        self.keepalives_after_content =
                            self.keepalives_after_content.saturating_add(1);
                    }
                }
            } else if record_has_comment(record) {
                self.comment_records = self.comment_records.saturating_add(1);
                if self.data_records != 0 {
                    self.keepalives_after_content = self.keepalives_after_content.saturating_add(1);
                }
            }
            consumed = cursor + separator_len;
            record_start = consumed;
            cursor = consumed;
        }

        if self.buf.len().saturating_sub(record_start) > MAX_SSE_RECORD_BYTES {
            return Err(record_limit_error());
        }
        if consumed != 0 {
            self.buf.drain(..consumed);
        }
        // A separator is at most four bytes, so only its final three possible
        // prefix bytes need to be reconsidered when the next chunk arrives.
        self.scan_from = self.buf.len().saturating_sub(3);
        Ok(payloads)
    }

    /// Type of the last complete SSE event: its event name, JSON discriminator, or `message`.
    /// An incomplete record and a keepalive comment do not replace the last event.
    pub fn last_event_type(&self) -> Option<&str> {
        self.last_event_type.as_deref()
    }

    /// Number of complete SSE records with a non-empty `data:` payload.
    /// Comment-only, blank, and empty-data records do not count as semantic progress;
    /// `[DONE]` counts because it is a non-empty data payload that the normalizer consumes.
    pub fn data_records(&self) -> u64 {
        self.data_records
    }

    /// Number of complete SSE records that carried no non-empty `data:` payload but did
    /// carry at least one comment line (a line starting `:`), such as `: keepalive`.
    ///
    /// A comment record proves the peer is alive and still framing records, but carries no
    /// content, so it is counted apart from [`Self::data_records`]. xAI sends ONLY these,
    /// every 15 seconds, while a model reasons without streaming the reasoning; the SSE
    /// specification recommends exactly this shape as a keepalive.
    ///
    /// Blank records and records whose only field is an empty `data:` (or `event:`/`id:`/
    /// `retry:` with no data) are counted in neither total. No provider we have captured
    /// uses those shapes as a keepalive, and admitting them would let a peer extend a
    /// stream's life on framing slack alone; the comment is the one keepalive shape seen on
    /// the wire and named by the specification.
    pub fn comment_records(&self) -> u64 {
        self.comment_records
    }

    /// Number of comment-only keepalive records since the last data event.
    pub fn keepalives_after_content(&self) -> u64 {
        self.keepalives_after_content
    }

    /// Event-type counts for the first eight distinct decoded data event names, plus `other` for
    /// every later or unsafe name. The bound keeps upstream-controlled names from growing it.
    pub fn event_type_counts(&self) -> &[(String, u64)] {
        &self.event_type_counts
    }

    /// Number of complete records that prove the peer is alive: every data record plus
    /// every comment record. This is what an SSE family reports as its liveness marker.
    pub fn liveness_records(&self) -> u64 {
        self.data_records.saturating_add(self.comment_records)
    }

    /// Whether a completed SSE record carried the explicit `[DONE]` sentinel.
    pub fn saw_done(&self) -> bool {
        self.saw_done
    }
}

/// Count `event_type` in at most eight named entries. Once eight names are held, every new name
/// is counted under `other`, so each named count stays exact: a Responses stream alone uses more
/// than eight event types, and reusing a slot would credit one type with another's events.
fn observe_event_type(counts: &mut Vec<(String, u64)>, event_type: &str) {
    const MAX_EVENT_TYPES: usize = 8;
    const OTHER: &str = "other";
    if let Some((_, count)) = counts.iter_mut().find(|(name, _)| name == event_type) {
        *count = count.saturating_add(1);
    } else if counts.iter().filter(|(name, _)| name != OTHER).count() < MAX_EVENT_TYPES
        && event_type != OTHER
    {
        counts.push((event_type.to_owned(), 1));
    } else if let Some((_, count)) = counts.iter_mut().find(|(name, _)| name == OTHER) {
        *count = count.saturating_add(1);
    } else {
        counts.push((OTHER.to_owned(), 1));
    }
}

/// Keep provider-controlled event names safe to include in diagnostic fields.
pub(crate) fn safe_event_type_name(event_type: &str) -> &str {
    if !event_type.is_empty()
        && event_type.len() <= 64
        && event_type
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        event_type
    } else {
        "other"
    }
}

/// Whether a complete record contains at least one comment line. Only called for records
/// with no non-empty data payload, so the data hot path never pays for it.
fn record_has_comment(record: &str) -> bool {
    record_lines(record).any(|line| line.starts_with(':'))
}

fn record_separator_len(bytes: &[u8], is_eof: bool) -> Option<usize> {
    let first = eol_len(bytes, is_eof)?;
    let second = eol_len(&bytes[first..], is_eof)?;
    Some(first + second)
}

/// Return the byte length of one complete SSE end-of-line token.
///
/// A trailing carriage return stays incomplete until the next byte arrives because it may
/// become part of a CRLF token. At end-of-stream (`is_eof == true`), no further bytes can
/// arrive, so a trailing CR is recognized as a bare CR line end per the WHATWG SSE specification.
fn eol_len(bytes: &[u8], is_eof: bool) -> Option<usize> {
    match bytes {
        [b'\r', b'\n', ..] => Some(2),
        [b'\r', _, ..] | [b'\n', ..] => Some(1),
        [b'\r'] if is_eof => Some(1),
        _ => None,
    }
}

fn record_limit_error() -> NormalizeError {
    NormalizeError::stream_limit(format!(
        "SSE record exceeds the {MAX_SSE_RECORD_BYTES}-byte limit"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_payloads_and_skips_noise() {
        let raw = "event: message_start\ndata: {\"type\":\"message_start\"}\n\n\
                   : this is a comment\n\
                   event: ping\ndata: {\"type\":\"ping\"}\n\n\
                   event: done\ndata: [DONE]\n\n";
        let got = parse_data_events(raw).unwrap();
        assert_eq!(
            got,
            vec![
                "{\"type\":\"message_start\"}".to_string(),
                "{\"type\":\"ping\"}".to_string(),
            ]
        );
    }

    #[test]
    fn leading_bom_is_stripped_in_one_or_split_chunks() {
        let decode = |chunks: &[&[u8]]| {
            let mut decoder = SseDecoder::default();
            let mut payloads = Vec::new();
            for chunk in chunks {
                payloads.extend(decoder.push(chunk).expect("BOM-prefixed record parses"));
            }
            payloads
        };

        assert_eq!(decode(&[b"\xef\xbb\xbfdata: one\n\n"]), vec!["one"]);
        assert_eq!(
            decode(&[b"\xef", b"\xbb\xbfdata: split\n\n"]),
            vec!["split"]
        );
        assert_eq!(
            decode(&[b"\xef\xbb", b"\xbfdata: split\n\n"]),
            vec!["split"]
        );
    }

    #[test]
    fn no_bom_keeps_bytes_and_midstream_bom_is_not_stripped() {
        let mut plain = SseDecoder::default();
        let plain_payloads = plain.push(b"data: same\n\n").unwrap();

        let mut prefixed = SseDecoder::default();
        let prefixed_payloads = prefixed.push(b"\xef\xbb\xbfdata: same\n\n").unwrap();
        assert_eq!(prefixed_payloads, plain_payloads);
        assert_eq!(
            parse_data_events("\u{feff}data: same\n\n").unwrap(),
            plain_payloads
        );

        let mut midstream = SseDecoder::default();
        assert_eq!(midstream.push(b"data: first\n\n").unwrap(), vec!["first"]);
        assert!(midstream
            .push(b"\xef\xbb\xbfdata: second\n\n")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn incomplete_bom_prefix_is_preserved_when_next_byte_is_not_bom_tail() {
        let mut decoder = SseDecoder::default();
        assert!(decoder.push(b"\xef\xbb").unwrap().is_empty());
        let error = decoder
            .push(b"x\n\n")
            .expect_err("a non-BOM continuation must not silently discard EF BB");
        assert!(matches!(error, NormalizeError::Invalid(message) if message.contains("UTF-8")));
    }

    #[test]
    fn data_record_progress_ignores_comment_and_empty_frames() {
        let mut decoder = SseDecoder::default();

        decoder.push(b": keepalive\n\n").unwrap();
        assert_eq!(
            decoder.data_records(),
            0,
            "comment-only keepalives must not advance the semantic data-record counter"
        );

        decoder.push(b"event: ping\ndata:\n\n").unwrap();
        assert_eq!(
            decoder.data_records(),
            0,
            "empty data frames must not advance the semantic data-record counter"
        );

        decoder.push(b"data: {\"live\":true}\n\n").unwrap();
        assert_eq!(
            decoder.data_records(),
            1,
            "a non-empty data frame must advance the semantic data-record counter"
        );

        decoder.push(b"data: [DONE]\n\n").unwrap();
        assert_eq!(
            decoder.data_records(),
            2,
            "[DONE] is a non-empty data frame and must count as semantic progress"
        );
    }

    #[test]
    fn comment_records_are_counted_apart_from_data_records() {
        let mut decoder = SseDecoder::default();

        decoder.push(b": keepalive\n\n").unwrap();
        assert_eq!(decoder.comment_records(), 1);
        assert_eq!(decoder.data_records(), 0);
        assert_eq!(decoder.liveness_records(), 1);

        // A comment split across pushes counts once, and only when its record completes.
        decoder.push(b": keep").unwrap();
        assert_eq!(
            decoder.comment_records(),
            1,
            "an incomplete comment is not a record"
        );
        decoder.push(b"alive\n\n").unwrap();
        assert_eq!(decoder.comment_records(), 2);

        // Blank and empty-data records are neither content nor liveness.
        decoder.push(b"\n\n").unwrap();
        decoder.push(b"event: ping\ndata:\n\n").unwrap();
        assert_eq!(decoder.comment_records(), 2);
        assert_eq!(decoder.data_records(), 0);

        // A data record that also carries a comment line is a data record only.
        decoder.push(b": note\ndata: {\"x\":1}\n\n").unwrap();
        assert_eq!(decoder.comment_records(), 2);
        assert_eq!(decoder.data_records(), 1);
        assert_eq!(decoder.liveness_records(), 3);
    }

    #[test]
    fn handles_crlf() {
        let raw = "data: {\"a\":1}\r\n\r\ndata: {\"b\":2}\r\n\r\n";
        assert_eq!(parse_data_events(raw).unwrap().len(), 2);
    }

    #[test]
    fn incremental_splitter_drains_complete_records_keeps_partial() {
        let mut decoder = SseDecoder::default();
        let got = decoder
            .push(b"event: a\ndata: {\"x\":1}\n\ndata: {\"y\"")
            .expect("records under the cap");
        assert_eq!(got, vec!["{\"x\":1}".to_string()]);
        let got2 = decoder.push(b":2}\n\n").expect("record under the cap");
        assert_eq!(got2, vec!["{\"y\":2}".to_string()]);
        assert!(decoder.buf.is_empty());
    }

    #[test]
    fn split_utf8_code_point_is_preserved_at_every_byte_boundary() {
        let wire = "data: {\"text\":\"café\"}\n\n".as_bytes();
        for split in 0..=wire.len() {
            let mut decoder = SseDecoder::default();
            let mut got = decoder.push(&wire[..split]).unwrap();
            got.extend(decoder.push(&wire[split..]).unwrap());
            assert_eq!(got, vec!["{\"text\":\"café\"}"], "split at {split}");
        }
    }

    #[test]
    fn split_crlf_boundary_is_recognized_at_every_byte_boundary() {
        let wire = b"data: one\r\n\r\ndata: two\r\n\r\n";
        for split in 0..=wire.len() {
            let mut decoder = SseDecoder::default();
            let mut got = decoder.push(&wire[..split]).unwrap();
            got.extend(decoder.push(&wire[split..]).unwrap());
            assert_eq!(got, vec!["one", "two"], "split at {split}");
        }
    }

    #[test]
    fn recognizes_all_eol_record_separators_whole_and_split_at_every_byte() {
        let separators: [(&str, &[u8]); 7] = [
            ("CRLF + CRLF", b"\r\n\r\n"),
            ("LF + LF", b"\n\n"),
            ("CR + CR", b"\r\r"),
            ("CRLF + LF", b"\r\n\n"),
            ("LF + CRLF", b"\n\r\n"),
            ("LF + CR", b"\n\r"),
            ("CR + CRLF", b"\r\r\n"),
        ];

        for (name, separator) in separators {
            let mut wire = b"data: first".to_vec();
            wire.extend_from_slice(separator);
            wire.extend_from_slice(b"data: second");
            wire.extend_from_slice(separator);
            // The final `: trailing` line has no end-of-line token, so it remains buffered and
            // cannot emit a third record. Its `:` confirms that a preceding CR completed the
            // separator rather than beginning a CRLF token.
            wire.extend_from_slice(b": trailing");

            let mut decoder = SseDecoder::default();
            assert_eq!(
                decoder.push(&wire).unwrap(),
                vec!["first", "second"],
                "whole {name} separator"
            );

            for split in 1..wire.len() {
                let mut decoder = SseDecoder::default();
                let mut got = decoder.push(&wire[..split]).unwrap();
                got.extend(decoder.push(&wire[split..]).unwrap());
                assert_eq!(
                    got,
                    vec!["first", "second"],
                    "{name} separator split at byte {split}"
                );
            }
        }
    }

    #[test]
    fn cr_only_framing_matches_lf_framing() {
        let cr_only = b"data: {\"sequence\":1}\r\rdata: {\"sequence\":2}\r\r: trailing";
        let lf_only = b"data: {\"sequence\":1}\n\ndata: {\"sequence\":2}\n\n: trailing";

        let mut cr_decoder = SseDecoder::default();
        let cr_events = cr_decoder.push(cr_only).unwrap();
        let mut lf_decoder = SseDecoder::default();
        let lf_events = lf_decoder.push(lf_only).unwrap();

        assert_eq!(cr_events, vec!["{\"sequence\":1}", "{\"sequence\":2}"]);
        assert_eq!(cr_events, lf_events);
    }

    /// A FIELD LINE BEFORE `data:` MUST NOT SWALLOW IT, under every line ending.
    ///
    /// `cr_only_framing_matches_lf_framing` above puts ONE data line in each record, so
    /// the field split is never exercised: a single line is the whole record either way.
    /// Real Anthropic traffic sends `event:` first, and with bare-CR framing
    /// `str::lines()` returned that as one line beginning `event:` -- the `data:` prefix
    /// never matched and the payload was dropped with no error.
    ///
    /// All three endings are asserted EQUAL to each other rather than to a literal, so
    /// the property is "framing does not change the payload" rather than a transcription
    /// of today's output. Split at every byte boundary, because the decoder is
    /// incremental and a terminator arriving alone is the interesting case.
    #[test]
    fn a_field_before_data_survives_every_line_ending() {
        let payload = "{\"delta\":\"hello\"}";
        for (name, eol) in [("cr", "\r"), ("lf", "\n"), ("crlf", "\r\n")] {
            let stream =
                format!("event: content_block_delta{eol}data: {payload}{eol}{eol}: tail{eol}{eol}");
            let bytes = stream.as_bytes();

            let mut whole = SseDecoder::default();
            assert_eq!(
                whole.push(bytes).unwrap(),
                vec![payload.to_string()],
                "{name}: whole-buffer push lost the payload behind the event field"
            );

            for split in 1..bytes.len() {
                let mut decoder = SseDecoder::default();
                let mut seen = decoder.push(&bytes[..split]).unwrap();
                seen.extend(decoder.push(&bytes[split..]).unwrap());
                assert_eq!(
                    seen,
                    vec![payload.to_string()],
                    "{name}: split at byte {split} lost the payload"
                );
            }
        }
    }

    /// Multi-line `data:` joins with LF regardless of the framing that delimited it.
    ///
    /// The trailing `: tail` is load-bearing and not padding: a bare CR at the very end
    /// of the buffer is deliberately INCOMPLETE, because the next byte could make it
    /// CRLF. Without a following record the final separator stays open through `push`
    /// and the decoder correctly emits nothing -- which my first version of this test read
    /// as the fix failing. Only `flush` at end of stream closes it, and that EOF policy is
    /// tested separately; this asserts field splitting.
    #[test]
    fn multiline_data_joins_identically_across_line_endings() {
        for (name, eol) in [("cr", "\r"), ("lf", "\n"), ("crlf", "\r\n")] {
            let stream = format!("event: x{eol}data: one{eol}data: two{eol}{eol}: tail{eol}{eol}");
            let mut decoder = SseDecoder::default();
            assert_eq!(
                decoder.push(stream.as_bytes()).unwrap(),
                vec!["one\ntwo".to_string()],
                "{name}: multiline data did not join"
            );
        }
    }

    #[test]
    fn complete_invalid_utf8_record_is_a_typed_error() {
        let mut decoder = SseDecoder::default();
        let error = decoder
            .push(b"data: \xff\n\n")
            .expect_err("complete invalid UTF-8 must fail normalization");
        assert!(matches!(error, NormalizeError::Invalid(_)));
    }

    #[test]
    fn fragmented_near_cap_record_is_scanned_incrementally() {
        let mut decoder = SseDecoder::default();
        let record_len = MAX_SSE_RECORD_BYTES - 32;
        let fragment = vec![b'x'; 1024];
        for _ in 0..(record_len / fragment.len()) {
            decoder.push(&fragment).unwrap();
        }
        let remainder = record_len % fragment.len();
        decoder.push(&fragment[..remainder]).unwrap();
        assert!(
            decoder.scanned_positions <= record_len * 2,
            "scan work must remain linear; scanned {} positions for {record_len} bytes",
            decoder.scanned_positions
        );
    }

    #[test]
    fn incremental_splitter_rejects_an_oversized_record() {
        let mut decoder = SseDecoder::default();
        let record = vec![b'x'; MAX_SSE_RECORD_BYTES + 1];
        let error = decoder
            .push(&record)
            .expect_err("an unterminated record above the cap must be rejected");
        assert!(error.is_stream_limit());
    }

    #[test]
    fn final_record_ending_in_bare_cr_is_emitted_on_flush() {
        let mut decoder = SseDecoder::default();
        let chunk = b"event: message_stop\r\ndata: {\"type\":\"message_stop\"}\r\r";
        let pushed = decoder.push(chunk).unwrap();
        assert!(
            pushed.is_empty(),
            "pending trailing CR should not emit during push"
        );
        let flushed = decoder.flush().unwrap();
        assert_eq!(flushed, vec!["{\"type\":\"message_stop\"}".to_string()]);
    }

    #[test]
    fn crlf_split_across_chunks_still_works() {
        let mut decoder = SseDecoder::default();
        let p1 = decoder.push(b"data: first\r\n\r\ndata: second\r").unwrap();
        assert_eq!(p1, vec!["first".to_string()]);
        let p2 = decoder.push(b"\n\r\ndata: third\r\r").unwrap();
        assert_eq!(p2, vec!["second".to_string()]);
        let flushed = decoder.flush().unwrap();
        assert_eq!(flushed, vec!["third".to_string()]);
    }

    #[test]
    fn offline_parser_matches_incremental_with_cr_separators() {
        let raw = "data: {\"seq\":1}\r\rdata: {\"seq\":2}\r\r";
        let offline = parse_data_events(raw).unwrap();
        let mut decoder = SseDecoder::default();
        let mut incremental = decoder.push(raw.as_bytes()).unwrap();
        incremental.extend(decoder.flush().unwrap());
        assert_eq!(offline, incremental);
        assert_eq!(offline, vec!["{\"seq\":1}", "{\"seq\":2}"]);
    }

    /// A capture whose record exceeds the size cap must report the limit, not parse as a
    /// capture with no events. The over-cap record follows a valid one, so a parser that
    /// dropped the error would return either nothing or only the first payload; both are
    /// rejected here.
    #[test]
    fn offline_parser_reports_an_over_cap_record_instead_of_no_events() {
        let mut raw = String::from("data: {\"seq\":1}\n\ndata: ");
        raw.push_str(&"x".repeat(MAX_SSE_RECORD_BYTES + 1));
        raw.push_str("\n\n");
        let error = parse_data_events(&raw)
            .expect_err("a record above the cap must fail the whole capture parse");
        assert!(error.is_stream_limit(), "unexpected error: {error}");
    }
}

#[cfg(test)]
mod event_type_tests {
    use super::SseDecoder;

    #[test]
    fn event_type_tracks_names_and_discriminators_but_not_content_or_partial_records() {
        let mut decoder = SseDecoder::default();
        decoder
            .push(b"event: named\ndata: {\"type\":\"ignored\",\"delta\":\"private\"}\n\n")
            .unwrap();
        assert_eq!(decoder.last_event_type(), Some("named"));
        decoder
            .push(b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"private\"}\n")
            .unwrap();
        assert_eq!(decoder.last_event_type(), Some("named"));
        decoder
            .push(b"\n: keepalive\n\nevent: unfinished\ndata: ")
            .unwrap();
        assert_eq!(
            decoder.last_event_type(),
            Some("response.output_text.delta")
        );
    }
}

#[cfg(test)]
mod event_type_count_tests {
    use super::observe_event_type;

    /// Past eight names, new names go to `other` and every named count stays exact.
    #[test]
    fn a_ninth_event_type_is_counted_as_other_without_disturbing_named_counts() {
        let mut counts = Vec::new();
        for index in 0..8 {
            for _ in 0..=index {
                observe_event_type(&mut counts, &format!("type.{index}"));
            }
        }
        observe_event_type(&mut counts, "type.8");
        observe_event_type(&mut counts, "type.9");
        observe_event_type(&mut counts, "type.0");

        for index in 1..8 {
            let name = format!("type.{index}");
            let count = counts.iter().find(|(n, _)| *n == name).map(|(_, c)| *c);
            assert_eq!(
                count,
                Some(index as u64 + 1),
                "{name} keeps its exact count"
            );
        }
        assert_eq!(
            counts.iter().find(|(n, _)| n == "type.0").map(|(_, c)| *c),
            Some(2)
        );
        assert_eq!(
            counts.iter().find(|(n, _)| n == "other").map(|(_, c)| *c),
            Some(2)
        );
        assert!(counts.iter().all(|(n, _)| n != "type.8" && n != "type.9"));
        assert_eq!(counts.len(), 9);
    }
}
