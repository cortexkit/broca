//! Optional raw quota taps. They do not participate in normalization or request rendering.

/// Implementations must enqueue locally without waiting for quota-service IO.
pub trait QuotaObserver: Send + Sync + std::fmt::Debug {
    fn headers(&self, headers: &reqwest::header::HeaderMap, observed_at_ms: u64);
    fn frame(&self, frame: &str, observed_at_ms: u64);
    fn wants_frames(&self) -> bool;
}

pub(crate) struct SseTap<'a> {
    observer: &'a dyn QuotaObserver,
    decoder: crate::sse::SseDecoder,
    last_received_at_ms: u64,
}
impl<'a> SseTap<'a> {
    pub(crate) fn new(observer: Option<&'a dyn QuotaObserver>) -> Option<Self> {
        observer
            .filter(|observer| observer.wants_frames())
            .map(|observer| Self {
                observer,
                decoder: Default::default(),
                last_received_at_ms: 0,
            })
    }
    pub(crate) fn push(&mut self, chunk: &[u8], at: u64) {
        self.last_received_at_ms = at;
        if let Ok(records) = self.decoder.push(chunk) {
            for record in records {
                self.observer.frame(&record, at);
            }
        }
    }
    pub(crate) fn finish(&mut self) {
        if let Ok(records) = self.decoder.flush() {
            for record in records {
                self.observer.frame(&record, self.last_received_at_ms);
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    #[derive(Debug, Default)]
    pub(crate) struct Capture {
        pub headers: std::sync::Mutex<Vec<(reqwest::header::HeaderMap, u64)>>,
        pub frames: std::sync::Mutex<Vec<(String, u64)>>,
    }
    impl QuotaObserver for Capture {
        fn headers(&self, headers: &reqwest::header::HeaderMap, at: u64) {
            self.headers.lock().unwrap().push((headers.clone(), at));
        }
        fn frame(&self, frame: &str, at: u64) {
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
}
