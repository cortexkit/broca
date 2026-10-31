//! Out-of-band transcript identities collected while a renderer emits wire rows.
//! This metadata is never serialized into a provider request.

#[derive(Debug, Clone, Default)]
pub struct RenderProvenance {
    messages: Vec<Option<u64>>,
    parts: Vec<PartSpan>,
}

#[derive(Debug, Clone)]
struct PartSpan {
    message: usize,
    start: usize,
    end: usize,
    ordinal: Option<u64>,
}

impl RenderProvenance {
    /// Record a row actually emitted by the builder, including synthetic rows with no ordinal.
    pub fn record_message(&mut self, index: usize, ordinal: Option<u64>) {
        self.messages
            .resize(self.messages.len().max(index + 1), None);
        self.messages[index] = ordinal;
    }

    /// A source message may emit several adjacent wire items, as with Responses tool calls.
    pub fn record_messages(&mut self, start: usize, end: usize, ordinal: Option<u64>) {
        if start < end {
            self.messages.resize(self.messages.len().max(end), None);
            self.messages[start..end].fill(ordinal);
        }
    }

    /// Record contiguous parts from one source inside a grouped wire row. The row itself has
    /// no singular identity when it combines different transcript messages. Record spans
    /// in wire emission order so lookup can use a binary search without per-part allocation.
    pub fn record_parts(&mut self, message: usize, start: usize, end: usize, ordinal: Option<u64>) {
        if start == end {
            return;
        }
        self.messages
            .resize(self.messages.len().max(message + 1), None);
        if start == 0 {
            self.messages[message] = ordinal;
        } else if self.messages[message] != ordinal {
            self.messages[message] = None;
        }
        self.parts.push(PartSpan {
            message,
            start,
            end,
            ordinal,
        });
    }

    /// Account for a synthetic instruction row inserted before already emitted input items.
    pub fn prepend_synthetic_message(&mut self) {
        self.messages.insert(0, None);
        for span in &mut self.parts {
            span.message += 1;
        }
    }

    pub fn message_count(&self) -> usize {
        self.messages.len()
    }

    /// Resolve only recorded provenance, never infer an ordinal from a wire position.
    pub fn message_ordinal(&self, message: usize, part: Option<usize>) -> Option<u64> {
        let whole = *self.messages.get(message)?;
        if let Some(part) = part {
            let end = self
                .parts
                .partition_point(|span| (span.message, span.start) <= (message, part));
            if let Some(span) = end.checked_sub(1).and_then(|i| self.parts.get(i)) {
                if span.message == message && part < span.end {
                    return span.ordinal;
                }
            }
        }
        whole
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merged_rows_keep_part_origins_without_inventing_a_message_identity() {
        let mut trace = RenderProvenance::default();
        trace.record_parts(0, 0, 2, Some(7));
        trace.record_parts(0, 2, 3, Some(11));
        assert_eq!(trace.message_ordinal(0, None), None);
        assert_eq!(trace.message_ordinal(0, Some(1)), Some(7));
        assert_eq!(trace.message_ordinal(0, Some(2)), Some(11));
        trace.prepend_synthetic_message();
        assert_eq!(trace.message_count(), 2);
        assert_eq!(trace.message_ordinal(0, Some(0)), None);
        assert_eq!(trace.message_ordinal(1, Some(2)), Some(11));
    }
}
