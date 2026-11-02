//! Optional raw quota taps. They do not participate in normalization or request rendering.

/// Implementations must enqueue locally without waiting for quota-service IO.
pub trait QuotaObserver: Send + Sync + std::fmt::Debug {
    fn headers(&self, headers: &reqwest::header::HeaderMap, observed_at_ms: u64);
    fn frame(&self, frame: &str, observed_at_ms: u64);
    fn wants_frames(&self) -> bool;
}

/// The provider uses a literal event discriminator. Search before parsing unrelated output.
pub fn frame_candidate(frame: &[u8]) -> bool {
    static FINDER: std::sync::OnceLock<memchr::memmem::Finder<'static>> =
        std::sync::OnceLock::new();
    FINDER
        .get_or_init(|| memchr::memmem::Finder::new(b"codex.rate_limits"))
        .find(frame)
        .is_some()
}

pub(crate) fn observe_frame(observer: &dyn QuotaObserver, frame: &str) {
    if frame_candidate(frame.as_bytes()) {
        observer.frame(frame, crate::transport::now_millis());
    }
}

// Prompt-cache warm requests retain their whole HTTP response in a body buffer. Scan that
// existing buffer by offset rather than buffering or decoding a second copy of every record.
pub(crate) struct SseTap<'a> {
    observer: &'a dyn QuotaObserver,
    record_start: usize,
    scan_from: usize,
    last_received_at_ms: u64,
}
impl<'a> SseTap<'a> {
    pub(crate) fn new(observer: Option<&'a dyn QuotaObserver>) -> Option<Self> {
        observer
            .filter(|observer| observer.wants_frames())
            .map(|observer| Self {
                observer,
                record_start: 0,
                scan_from: 0,
                last_received_at_ms: 0,
            })
    }
    /// `body` is cumulative, including all bytes passed by previous calls.
    pub(crate) fn push(&mut self, body: &[u8], at: u64) {
        self.last_received_at_ms = at;
        self.scan(body, false);
    }
    pub(crate) fn finish(&mut self, body: &[u8]) {
        self.scan(body, true);
    }
    fn scan(&mut self, body: &[u8], eof: bool) {
        let mut cursor = self.scan_from;
        while let Some(next) = memchr::memchr2(b'\r', b'\n', &body[cursor..]) {
            cursor += next;
            if let Some(len) = crate::sse::record_separator_len(&body[cursor..], eof) {
                let record = &body[self.record_start..cursor];
                if frame_candidate(record) {
                    let record = if self.record_start == 0 {
                        record.strip_prefix(b"\xef\xbb\xbf").unwrap_or(record)
                    } else {
                        record
                    };
                    if let Ok(record) = std::str::from_utf8(record) {
                        if let Some(payload) = crate::sse::record_data(record) {
                            if frame_candidate(payload.as_bytes()) {
                                self.observer.frame(&payload, self.last_received_at_ms);
                            }
                        }
                    }
                }
                cursor += len;
                self.record_start = cursor;
            } else {
                cursor += 1;
            }
        }
        // A separator is at most four bytes; retain its possible split prefix, not the body.
        self.scan_from = self.record_start.max(body.len().saturating_sub(3));
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    #[derive(Debug, Default)]
    pub(crate) struct Capture {
        pub frame_calls: std::sync::atomic::AtomicU64,
        pub headers: std::sync::Mutex<Vec<(reqwest::header::HeaderMap, u64)>>,
        pub frames: std::sync::Mutex<Vec<(String, u64)>>,
    }
    impl QuotaObserver for Capture {
        fn headers(&self, headers: &reqwest::header::HeaderMap, at: u64) {
            self.headers.lock().unwrap().push((headers.clone(), at));
        }
        fn frame(&self, frame: &str, at: u64) {
            self.frame_calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if serde_json::from_str::<serde_json::Value>(frame)
                .is_ok_and(|value| value["type"] == "codex.rate_limits")
            {
                self.frames.lock().unwrap().push((frame.to_owned(), at));
            }
        }
        fn wants_frames(&self) -> bool {
            true
        }
    }

    #[test]
    fn quota_gate_never_calls_observer_for_non_quota_frames() {
        let capture = Capture::default();
        for frame in [
            r#"{"type":"response.output_text.delta","delta":"hello"}"#,
            r#"{"type":"response.completed","response":{"output":[]}}"#,
            "not JSON",
        ] {
            observe_frame(&capture, frame);
        }
        assert_eq!(
            capture
                .frame_calls
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn quota_warm_sse_borrows_body_and_handles_every_split_and_line_ending() {
        let payload = "{\"type\":\"codex.rate_limits\",\n\"rate_limits\":null}";
        for separator in [
            "\n\n", "\r\r", "\r\n\r\n", "\r\n\n", "\n\r\n", "\n\r", "\r\r\n",
        ] {
            let wire = format!("\u{feff}data: {{\"type\":\"response.output_text.delta\",\"delta\":\"ordinary\"}}{separator}data: {{\"type\":\"codex.rate_limits\",\ndata: \"rate_limits\":null}}{separator}: tail");
            for split in 1..wire.len() {
                let capture = Capture::default();
                let mut tap = SseTap::new(Some(&capture)).unwrap();
                tap.push(&wire.as_bytes()[..split], 1);
                tap.push(wire.as_bytes(), 2);
                tap.finish(wire.as_bytes());
                assert_eq!(
                    capture
                        .frame_calls
                        .load(std::sync::atomic::Ordering::Relaxed),
                    1,
                    "separator {separator:?}, split {split}"
                );
                assert_eq!(capture.frames.lock().unwrap()[0].0, payload);
            }
        }
    }
}
