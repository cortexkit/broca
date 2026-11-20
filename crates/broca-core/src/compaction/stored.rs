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
}

/// Borrow unchanged messages from the prompt and changed messages from saved originals,
/// so compaction sees exactly the untagged content that `session.read` returns.
#[derive(Clone, Copy)]
pub(crate) struct StoredTranscript<'a> {
    prompt: &'a [Message],
    originals: Option<&'a OriginalMessages>,
}

impl<'a> StoredTranscript<'a> {
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
}

#[cfg(test)]
thread_local! {
    static RANGE_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
