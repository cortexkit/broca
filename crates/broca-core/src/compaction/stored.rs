use broca_protocol::{Message, Role};
use std::collections::BTreeMap;

/// Save a message's unmodified content only when rendering tags or repetition guidance
/// changes what the model sees.
#[derive(Default)]
pub(crate) struct OriginalMessages(BTreeMap<usize, Message>);

impl OriginalMessages {
    pub(crate) fn retain_change(
        &mut self,
        index: usize,
        mut original: Message,
        rendered: &Message,
    ) {
        if original.content != rendered.content {
            original.ordinal = Some(index as u64);
            self.0.entry(index).or_insert(original);
        }
    }
}

/// Borrow unchanged messages from the prompt and changed messages from saved originals,
/// so compaction sees exactly the untagged content that `session.read` returns.
#[derive(Clone, Copy)]
pub(crate) struct StoredTranscript<'a> {
    prompt: &'a [Message],
    originals: Option<&'a OriginalMessages>,
}

impl<'a> StoredTranscript<'a> {
    pub(crate) fn new(prompt: &'a [Message], originals: Option<&'a OriginalMessages>) -> Self {
        Self { prompt, originals }
    }

    pub(crate) fn plain(prompt: &'a [Message]) -> Self {
        Self::new(prompt, None)
    }

    fn original(self, message: &'a Message) -> &'a Message {
        message
            .ordinal
            .and_then(|ordinal| self.originals?.0.get(&(ordinal as usize)))
            .unwrap_or(message)
    }

    pub(crate) fn range(self, from: u64, to: u64) -> impl DoubleEndedIterator<Item = &'a Message> {
        super::view::ordinal_range(self.prompt, from, to)
            .iter()
            .filter(|m| m.role != Role::System && m.ordinal.is_some())
            .map(move |message| self.original(message))
    }

    pub(crate) fn at(self, ordinal: u64) -> Option<&'a Message> {
        self.range(ordinal, ordinal.saturating_add(1)).next()
    }

    pub(crate) fn newest(self) -> Option<&'a Message> {
        self.prompt
            .iter()
            .rev()
            .find(|m| m.role != Role::System && m.ordinal.is_some())
            .map(|message| self.original(message))
    }

    pub(crate) fn len(self) -> usize {
        self.prompt.len()
    }

    pub(crate) fn messages_after(self, ordinal: Option<u64>) -> impl Iterator<Item = &'a Message> {
        super::view::ordinal_range(
            self.prompt,
            ordinal.map_or(0, |o| o.saturating_add(1)),
            u64::MAX,
        )
        .iter()
        .map(move |message| self.original(message))
    }

    /// Materialize the stored transcript only for the provider's one-time Setup validation.
    pub(crate) fn setup_messages(self) -> Vec<Message> {
        self.prompt
            .iter()
            .map(|message| self.original(message).clone())
            .collect()
    }
}

#[test]
fn unchanged_messages_are_borrowed_and_only_changed_bytes_are_retained() {
    let mut prompt = vec![
        Message::new(
            Role::User,
            vec![broca_protocol::ContentBlock::Text { text: "raw".into() }]
        );
        1000
    ];
    broca_protocol::content::stamp_ordinals(&mut prompt);
    let mut originals = OriginalMessages::default();
    for (i, message) in prompt.iter_mut().enumerate() {
        let original = message.clone();
        if i == 999 {
            message.content = vec![broca_protocol::ContentBlock::Text {
                text: "tagged".into(),
            }];
        }
        originals.retain_change(i, original, message);
    }
    assert_eq!(originals.0.len(), 1);
    let stored = StoredTranscript::new(&prompt, Some(&originals));
    assert!(std::ptr::eq(stored.at(0).unwrap(), &prompt[0]));
    assert_eq!(stored.newest().unwrap().content, prompt[0].content);
    let original = prompt[999].clone();
    prompt[999].content.clear();
    originals.retain_change(999, original, &prompt[999]);
    let stored = StoredTranscript::new(&prompt, Some(&originals));
    assert_eq!(
        stored.newest().unwrap().content,
        prompt[0].content,
        "later decorations must not replace the original bytes"
    );
}
