use broca_protocol::DisplayEvent;
use cortexkit_compaction_view::clean::is_js_space;

pub(super) struct DisplayOutput<'a> {
    pub(super) out: &'a tokio::sync::mpsc::Sender<super::OutputEvent>,
    cleaner: Option<DisplayText>,
}

impl<'a> DisplayOutput<'a> {
    pub(super) fn new(out: &'a tokio::sync::mpsc::Sender<super::OutputEvent>, clean: bool) -> Self {
        Self {
            out,
            cleaner: clean.then(DisplayText::default),
        }
    }

    pub(super) async fn send(&mut self, event: DisplayEvent) {
        let events = match &mut self.cleaner {
            Some(cleaner) => cleaner.push(event),
            None => vec![event],
        };
        for event in events {
            let _ = super::send(self.out, super::OutputEvent::Display(event)).await;
        }
    }

    pub(super) async fn finish(&mut self) {
        if let Some(cleaner) = &mut self.cleaner {
            for event in cleaner.finish() {
                let _ = super::send(self.out, super::OutputEvent::Display(event)).await;
            }
        }
    }
}

/// Only the unresolved suffix is retained: a section-sign sequence through its next
/// whitespace boundary, and whitespace that might be trimmed at the block's end. The
/// library's matchers do not cross that boundary; the wrapper never parses tag syntax.
#[derive(Default)]
struct ReplyTextCleaner {
    pending: String,
    started: bool,
}

impl ReplyTextCleaner {
    fn push(&mut self, delta: &str) -> String {
        self.pending.push_str(delta);
        let word = self.pending.rfind(is_js_space).map_or(0, |i| {
            i + self.pending[i..].chars().next().unwrap().len_utf8()
        });
        let safe = self.pending[word..]
            .find('§')
            .map_or(self.pending.len(), |i| word + i);
        if safe == 0 {
            return String::new();
        }
        let rest = self.pending.split_off(safe);
        // Guards preserve interior whitespace. The left guard also prevents an inline
        // tag from being mistaken for the reply's own leading tag after an earlier emit.
        let mut guarded = String::new();
        if self.started {
            guarded.push('x');
        }
        guarded.push_str(&self.pending);
        guarded.push('x');
        let mut clean = broca_protocol::tags::exposed_model_text(true, &guarded).into_owned();
        clean.pop();
        if self.started {
            clean.remove(0);
        }
        let end = clean.trim_end_matches(is_js_space).len();
        self.pending = clean[end..].to_owned();
        self.pending.push_str(&rest);
        clean.truncate(end);
        self.started |= !clean.is_empty();
        clean
    }

    fn finish(&mut self) -> String {
        let mut pending = std::mem::take(&mut self.pending);
        if std::mem::take(&mut self.started) {
            pending.insert(0, 'x');
        } else {
            return broca_protocol::tags::exposed_model_text(true, &pending).into_owned();
        }
        let mut clean = broca_protocol::tags::exposed_model_text(true, &pending).into_owned();
        clean.remove(0);
        clean
    }
}

/// One contiguous assistant text block on the display lane. A different message id,
/// transcript mid or part key ends the block, as does a non-text delta: providers do
/// not send explicit block-end display events.
#[derive(Default)]
pub(super) struct DisplayText {
    event: Option<DisplayEvent>,
    cleaner: ReplyTextCleaner,
}

impl DisplayText {
    pub(super) fn push(&mut self, event: DisplayEvent) -> Vec<DisplayEvent> {
        if event == DisplayEvent::DisplayReset {
            *self = Self::default();
            return vec![event];
        }
        let same = matches!((&self.event, &event), (
            Some(DisplayEvent::TextDelta { mid: a_mid, part: a_part, message_id: a_id, .. }),
            DisplayEvent::TextDelta { mid: b_mid, part: b_part, message_id: b_id, tool_call_id: None, .. }
        ) if (a_mid, a_part, a_id) == (b_mid, b_part, b_id));
        let mut output = if same { Vec::new() } else { self.finish() };
        match event {
            mut event @ DisplayEvent::TextDelta {
                tool_call_id: None, ..
            } => {
                if let DisplayEvent::TextDelta { delta, .. } = &mut event {
                    *delta = self.cleaner.push(delta);
                }
                self.event = Some(event.clone());
                if matches!(&event, DisplayEvent::TextDelta { delta, .. } if !delta.is_empty()) {
                    output.push(event);
                }
            }
            other => output.push(other),
        }
        output
    }

    pub(super) fn finish(&mut self) -> Vec<DisplayEvent> {
        let delta = self.cleaner.finish();
        match self.event.take() {
            Some(mut event) if !delta.is_empty() => {
                if let DisplayEvent::TextDelta { delta: text, .. } = &mut event {
                    *text = delta;
                }
                vec![event]
            }
            _ => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(part: u64, delta: &str) -> DisplayEvent {
        DisplayEvent::TextDelta {
            mid: None,
            part: Some(part),
            message_id: broca_protocol::MessageId("a".into()),
            tool_call_id: None,
            delta: delta.into(),
        }
    }

    #[test]
    fn ordinary_text_is_live_while_tag_suffixes_and_trailing_space_wait() {
        let mut cleaner = ReplyTextCleaner::default();
        assert_eq!(cleaner.push("§3"), "");
        assert_eq!(cleaner.push("2§ I have"), "I have");
        assert_eq!(cleaner.push(" read it  "), " read it");
        assert_eq!(cleaner.finish(), "");
        assert_eq!(cleaner.push("fresh"), "fresh");
        assert_eq!(cleaner.finish(), "");
    }

    #[test]
    fn display_blocks_flush_independently_and_reset_discards_pending() {
        let mut display = DisplayText::default();
        let mut events = display.push(text(0, "first §12.3"));
        events.extend(display.push(text(1, "§4§ second §5")));
        let reasoning = DisplayEvent::ReasoningDelta {
            mid: None,
            part: Some(2),
            message_id: broca_protocol::MessageId("a".into()),
            delta: "§6§ thought".into(),
        };
        events.extend(display.push(reasoning.clone()));
        assert_eq!(
            events,
            [
                text(0, "first"),
                text(0, " 12.3"),
                text(1, "second"),
                reasoning
            ]
        );
        let mut tool_text = text(3, "§7§ tool text");
        if let DisplayEvent::TextDelta { tool_call_id, .. } = &mut tool_text {
            *tool_call_id = Some(broca_protocol::ToolCallId("call".into()));
        }
        assert_eq!(display.push(tool_text.clone()), [tool_text]);
        assert!(display.push(text(4, "§12.3")).is_empty());
        assert_eq!(
            display.push(DisplayEvent::DisplayReset),
            [DisplayEvent::DisplayReset]
        );
        assert!(display.push(text(4, "§3§new")).is_empty());
        assert_eq!(display.finish(), [text(4, "new")]);
        assert!(display.finish().is_empty());
    }

    struct HeldText(
        std::sync::Mutex<Option<tokio::sync::mpsc::Sender<broca_protocol::NormalizedEvent>>>,
    );

    #[async_trait::async_trait]
    impl crate::Provider for HeldText {
        async fn step(
            &self,
            _: broca_protocol::StepId,
            _: &broca_protocol::request::CallOptions,
        ) -> tokio::sync::mpsc::Receiver<broca_protocol::NormalizedEvent> {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            tx.send(broca_protocol::NormalizedEvent::Display(text(
                0,
                "before §12.3",
            )))
            .await
            .unwrap();
            *self.0.lock().unwrap() = Some(tx);
            rx
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_or_timed_out_step_flushes_the_pending_text_suffix() {
        use super::super::{run_step_watched, AttemptCallbacks, OutputEvent};
        for watchdog in [false, true] {
            let provider = HeldText(std::sync::Mutex::new(None));
            let cancel = tokio_util::sync::CancellationToken::new();
            let control = crate::RunControl {
                cancel: Some(cancel.clone()),
                provider_call_timeout: watchdog.then_some(std::time::Duration::from_secs(1)),
                ..Default::default()
            };
            let opts = std::sync::Arc::new(broca_protocol::request::CallOptions::new(Vec::new()));
            let (out, mut events) = tokio::sync::mpsc::channel(8);
            let run = run_step_watched(
                &provider,
                broca_protocol::StepId(1),
                &opts,
                0,
                &control,
                DisplayOutput::new(&out, true),
                AttemptCallbacks {
                    record: &|_| Box::pin(async { Ok(()) }),
                    before_send: None,
                },
            );
            let stop = async {
                while let Some(event) = events.recv().await {
                    if let OutputEvent::Display(DisplayEvent::TextDelta { delta, .. }) = event {
                        assert_eq!(delta, "before");
                        if watchdog {
                            tokio::time::advance(std::time::Duration::from_secs(1)).await;
                        } else {
                            cancel.cancel();
                        }
                        return;
                    }
                }
                panic!("no live text before stopping the step");
            };
            let (result, ()) = tokio::join!(run, stop);
            let expected = if watchdog {
                broca_protocol::RunFinishReason::Interrupted
            } else {
                broca_protocol::RunFinishReason::Cancelled
            };
            assert!(matches!(result.unwrap(), Err(reason) if reason == expected));
            assert_eq!(
                events.try_recv().unwrap(),
                OutputEvent::Display(text(0, " 12.3"))
            );
            assert!(events.try_recv().is_err());
        }
    }

    #[test]
    fn streaming_reply_matches_stored_cleaning_at_every_split() {
        for reply in [
            "§32§ I have read it",
            "§3§ hi as §1§ says",
            "§3§ §4§ hi",
            "hi §12",
            "hi §12§",
            "§3§hello",
            "é漢§3§🙂 hi",
            "",
            "§3§",
            "§5.1 and §12.3",
            "§12§ §13§ Dropped §4§ and §5\">§6§ as asked.",
            "hi §3) there §42important",
            "hi §3_ there",
            "§15298\">§15298§ ",
            "\u{feff}§3§\u{a0}hi\u{feff}",
            "hi\u{85}",
            "  \n§3§ hi  \t",
        ] {
            let expected = broca_protocol::tags::exposed_model_text(true, reply);
            let boundaries: Vec<_> = reply
                .char_indices()
                .map(|(i, _)| i)
                .chain(std::iter::once(reply.len()))
                .collect();
            for &a in &boundaries {
                for &b in boundaries.iter().filter(|&&b| b >= a) {
                    let mut cleaner = ReplyTextCleaner::default();
                    let mut actual = cleaner.push(&reply[..a]);
                    actual.push_str(&cleaner.push(&reply[a..b]));
                    actual.push_str(&cleaner.push(&reply[b..]));
                    actual.push_str(&cleaner.finish());
                    assert_eq!(actual, expected, "reply={reply:?}, splits={a},{b}");
                }
            }
            let mut cleaner = ReplyTextCleaner::default();
            let mut actual = String::new();
            for ch in reply.chars() {
                actual.push_str(&cleaner.push(&ch.to_string()));
            }
            actual.push_str(&cleaner.finish());
            assert_eq!(actual, expected, "character deltas for {reply:?}");
        }
    }
}
