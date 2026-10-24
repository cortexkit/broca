use broca_protocol::{Message, Role};
use std::collections::BTreeMap;

/// Save a message's unmodified content only when rendering tags or repetition guidance
/// changes what the model sees.
#[derive(Default)]
pub(crate) struct OriginalMessages {
    messages: BTreeMap<usize, Message>,
    origin_roles: [bool; 4],
    indexed_len: usize,
}

fn role_index(role: Role) -> usize {
    match role {
        Role::System => 0,
        Role::User => 1,
        Role::Assistant => 2,
        Role::Tool => 3,
    }
}

impl OriginalMessages {
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.messages.len()
    }
    pub(crate) fn release(
        &mut self,
        view: &crate::compaction_provider::view::CompactionView,
        held: usize,
    ) {
        self.messages
            .retain(|_, message| crate::run::held_prompt::held_by(view, message));
        self.indexed_len = held;
    }
    pub(crate) fn retain_change(
        &mut self,
        index: usize,
        mut original: Message,
        rendered: &Message,
    ) {
        if original.content != rendered.content {
            original.ordinal = Some(index as u64);
            self.messages.entry(index).or_insert(original);
        }
    }

    /// Tags and guidance never change a message's role or model origin. Remember which roles
    /// can have a model origin so plain summaries of other roles need no copy search at all.
    pub(crate) fn index_origins(&mut self, prompt: &[Message]) {
        for message in &prompt[self.indexed_len..] {
            if message.origin.is_some() {
                self.origin_roles[role_index(message.role)] = true;
            }
        }
        self.indexed_len = prompt.len();
    }
}

/// Borrow unchanged messages from the prompt and changed messages from saved originals,
/// so compaction sees exactly the untagged content that `session.read` returns.
#[derive(Clone, Copy)]
pub(crate) struct StoredTranscript<'a> {
    prompt: &'a [Message],
    originals: Option<&'a OriginalMessages>,
    extent: u64,
}

impl<'a> StoredTranscript<'a> {
    pub(crate) fn new(prompt: &'a [Message], originals: Option<&'a OriginalMessages>) -> Self {
        Self {
            prompt,
            originals,
            extent: prompt
                .last()
                .and_then(|m| m.ordinal)
                .map_or(prompt.len() as u64, |o| o + 1),
        }
    }

    pub(crate) fn with_extent(mut self, extent: u64) -> Self {
        self.extent = extent;
        self
    }

    pub(crate) fn extent(self) -> u64 {
        self.extent
    }

    pub(crate) fn plain(prompt: &'a [Message]) -> Self {
        Self::new(prompt, None)
    }

    fn original(self, message: &'a Message) -> &'a Message {
        message
            .ordinal
            .and_then(|ordinal| self.originals?.messages.get(&(ordinal as usize)))
            .unwrap_or(message)
    }

    pub(crate) fn range(self, from: u64, to: u64) -> impl DoubleEndedIterator<Item = &'a Message> {
        super::view::ordinal_range(self.prompt, from, to)
            .iter()
            .filter(|m| m.role != Role::System && m.ordinal.is_some())
            .map(move |message| self.original(message))
            .inspect(|_| {
                #[cfg(test)]
                RANGE_VISITS.with(|visits| visits.set(visits.get() + 1));
            })
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

    pub(crate) fn has_model_origin(self, role: Role) -> bool {
        match self
            .originals
            .filter(|originals| originals.indexed_len == self.prompt.len())
        {
            Some(originals) => originals.origin_roles[role_index(role)],
            None => self
                .prompt
                .iter()
                .any(|message| message.role == role && message.origin.is_some()),
        }
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

#[cfg(test)]
thread_local! {
    static RANGE_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn measure_range_visits<T>(run: impl FnOnce() -> T) -> (T, usize) {
    RANGE_VISITS.with(|visits| visits.set(0));
    let result = run();
    (result, RANGE_VISITS.with(std::cell::Cell::get))
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
    assert_eq!(originals.messages.len(), 1);
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
