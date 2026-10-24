//! Repetition restoration from raw store pages and an already-replayed WAL tail.

use super::append_repetition_guidance;
use crate::context::{MaterializedStore, ReadView};
use crate::tool_repetition::{RepetitionFold, RepetitionObserver};
use broca_protocol::{BindIdentity, ContentBlock, Message};

/// A resume's fixed store/WAL boundary. The caller captures these coordinates atomically with
/// its held rows and replays only records past `watermark_seq` into `tail`, using the shared
/// placement logic. Tail messages must carry their absolute ordinals and raw tool outputs.
/// Store pages are checked against this boundary; a racing projection is a replay fallback,
/// not permission to combine rows from different snapshots.
pub struct GuidanceSource<'a> {
    pub store: &'a dyn MaterializedStore,
    pub session: &'a BindIdentity,
    pub lineage_id: &'a str,
    pub watermark_seq: u64,
    pub prefix_len: u64,
    pub tail: &'a [Message],
    pub page_size: u64,
}

impl GuidanceSource<'_> {
    /// Fold one guidance episode's model-step range, excluding its admitted input. `None`
    /// means missing projection metadata (replay required); `Some(None)` is a known episode
    /// with no model steps, so it needs no rows. `end` is the next episode's start or extent.
    ///
    /// Only tool messages already in `held` are decorated. They must be sorted by absolute
    /// ordinal, and may already carry guidance: outputs are rebuilt from raw rows rather than
    /// hashed or appended twice. On error the caller must discard the partial restoration and
    /// replay. The returned observer remains live for subsequent calls in the active episode.
    pub fn restore_episode(
        &self,
        first_step: Option<Option<u64>>,
        end: u64,
        held: &mut [Message],
    ) -> Result<RepetitionObserver, String> {
        let Some(first_step) = first_step else {
            return Err("episode first model-step ordinal is absent".into());
        };
        let Some(mut next) = first_step else {
            return Ok(RepetitionObserver::new());
        };
        let start = next;
        if next > end || self.page_size == 0 {
            return Err("invalid episode guidance range or page size".into());
        }
        if held.iter().any(|message| message.ordinal.is_none())
            || held
                .windows(2)
                .any(|pair| pair[0].ordinal >= pair[1].ordinal)
        {
            return Err("held guidance messages need increasing absolute ordinals".into());
        }
        let mut fold = RepetitionFold::default();
        let store_end = end.min(self.prefix_len);
        while next < store_end {
            let page_end = next.saturating_add(self.page_size).min(store_end);
            let page = self.store.read_page_with_watermark(
                self.session,
                Some(next),
                page_end - next,
                ReadView::Raw,
            )?;
            if page.watermark_seq != self.watermark_seq
                || page.prefix_len != self.prefix_len
                || page.lineage_id.as_deref() != Some(self.lineage_id)
            {
                return Err(
                    "guidance store page does not match resume watermark or lineage".into(),
                );
            }
            // Page APIs can include lookahead. Consume only the requested range, once, and
            // release the page before reading another; the fold keeps only counters and calls.
            for row in page.rows.iter().filter(|row| row.ordinal < page_end) {
                if row.ordinal != next {
                    return Err(format!("guidance store gap at ordinal {next}"));
                }
                fold_held(&mut fold, row.ordinal, &row.message, held)?;
                next += 1;
            }
            if next != page_end {
                return Err(format!("guidance store gap at ordinal {next}"));
            }
        }
        for message in self.tail {
            let ordinal = message
                .ordinal
                .ok_or("guidance WAL tail message has no absolute ordinal")?;
            if ordinal < self.prefix_len || ordinal < start || ordinal >= end {
                continue;
            }
            if ordinal != next {
                return Err(format!("guidance WAL tail gap at ordinal {next}"));
            }
            fold_held(&mut fold, ordinal, message, held)?;
            next += 1;
        }
        if next != end {
            return Err(format!("guidance WAL tail gap at ordinal {next}"));
        }
        Ok(fold.into_observer())
    }
}

fn fold_held(
    fold: &mut RepetitionFold,
    ordinal: u64,
    raw: &Message,
    held: &mut [Message],
) -> Result<(), String> {
    let verdicts = fold.push(raw);
    let Ok(index) = held.binary_search_by_key(&Some(ordinal), |message| message.ordinal) else {
        return Ok(());
    };
    for (block_index, verdict) in verdicts {
        let Some(ContentBlock::ToolResult(result)) = held[index].content.get_mut(block_index)
        else {
            return Err(format!(
                "held tool result differs from raw ordinal {ordinal}"
            ));
        };
        let ContentBlock::ToolResult(original) = &raw.content[block_index] else {
            unreachable!("fold verdicts name tool results");
        };
        result.output.clone_from(&original.output);
        if let Some(guidance) = verdict.guidance() {
            append_repetition_guidance(result, &guidance);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{StorePageRow, StorePageSnapshot, StorePrefix};
    use crate::tool_repetition::RepetitionVerdict;
    use broca_protocol::{Role, ToolCall, ToolCallId, ToolOutput, ToolResult};

    struct Pages {
        rows: Vec<Message>,
        watermark: u64,
    }

    impl MaterializedStore for Pages {
        fn read_prefix_with_watermark(&self, _: &BindIdentity) -> Result<StorePrefix, String> {
            panic!("streaming guidance cannot read the full prefix")
        }

        fn read_page_with_watermark(
            &self,
            _: &BindIdentity,
            from: Option<u64>,
            limit: u64,
            view: ReadView,
        ) -> Result<StorePageSnapshot, String> {
            assert_eq!(view, ReadView::Raw);
            Ok(StorePageSnapshot {
                watermark_seq: self.watermark,
                lineage_id: Some("lineage".into()),
                prefix_len: 3,
                rows: self
                    .rows
                    .iter()
                    .filter(|m| m.ordinal >= from && m.ordinal < from.map(|o| o + limit))
                    .map(|m| StorePageRow {
                        written_at: None,
                        ordinal: m.ordinal.unwrap(),
                        message: m.clone(),
                        run: None,
                    })
                    .collect(),
                ..StorePageSnapshot::empty()
            })
        }
    }

    fn call() -> ToolCall {
        ToolCall {
            tool_call_id: ToolCallId("reused".into()),
            tool_name: "read".into(),
            input: serde_json::json!({}),
            thought_signature: None,
        }
    }

    fn history() -> Vec<Message> {
        (0..4)
            .map(|ordinal| Message {
                ordinal: Some(ordinal),
                role: if ordinal % 2 == 0 {
                    Role::Assistant
                } else {
                    Role::Tool
                },
                content: vec![if ordinal % 2 == 0 {
                    ContentBlock::ToolCall(call())
                } else {
                    ContentBlock::ToolResult(ToolResult {
                        tool_call_id: call().tool_call_id,
                        output: ToolOutput::Text {
                            text: "same".into(),
                        },
                        is_error: false,
                    })
                }],
                cache_prefix_blocks: None,
                origin: None,
                tags: Default::default(),
            })
            .collect()
    }

    fn session() -> BindIdentity {
        BindIdentity {
            project_root: "/project".into(),
            harness: "test".into(),
            session: "s".into(),
        }
    }

    #[test]
    fn last_store_assistant_carries_into_the_wal_tail() {
        let raw = history();
        let store = Pages {
            rows: raw[..3].to_vec(),
            watermark: 7,
        };
        let source = GuidanceSource {
            store: &store,
            session: &session(),
            lineage_id: "lineage",
            watermark_seq: 7,
            prefix_len: 3,
            tail: &raw[3..],
            page_size: 2,
        };
        let mut held = raw[3..].to_vec();
        let mut observer = source.restore_episode(Some(Some(0)), 4, &mut held).unwrap();
        let mut expected = raw.clone();
        super::super::restore_repetition_guidance(&mut expected);
        assert_eq!(held, expected[3..]);
        assert_eq!(observer, RepetitionObserver::from_prompt(&raw));
        assert_eq!(
            observer.observe(&call(), b"same", false),
            RepetitionVerdict::NoProgress {
                tool_name: "read".into(),
                count: 3
            }
        );
    }

    #[test]
    fn known_no_first_step_needs_no_rows_but_absent_metadata_is_a_gap() {
        let source = GuidanceSource {
            store: &Pages {
                rows: vec![],
                watermark: 0,
            },
            session: &session(),
            lineage_id: "lineage",
            watermark_seq: 7,
            prefix_len: 3,
            tail: &[],
            page_size: 0,
        };
        assert_eq!(
            source.restore_episode(Some(None), 9, &mut []).unwrap(),
            RepetitionObserver::new()
        );
        assert_eq!(
            source.restore_episode(None, 9, &mut []).unwrap_err(),
            "episode first model-step ordinal is absent"
        );
    }

    #[test]
    fn whole_episode_hands_the_raw_observer_to_live_calls() {
        let raw = history();
        let mut prompt = raw.clone();
        let mut observer = super::super::restore_guidance_with_originals(&mut prompt, 0, None);
        assert_eq!(observer, RepetitionObserver::from_prompt(&raw));
        assert_ne!(prompt, raw);
        assert_eq!(
            observer.observe(&call(), b"same", false),
            RepetitionVerdict::NoProgress {
                tool_name: "read".into(),
                count: 3
            }
        );
    }

    #[test]
    fn store_gaps_and_watermark_drift_require_replay() {
        let raw = history();
        let mut store = Pages {
            rows: raw[..3].to_vec(),
            watermark: 8,
        };
        let source = GuidanceSource {
            store: &store,
            session: &session(),
            lineage_id: "lineage",
            watermark_seq: 7,
            prefix_len: 3,
            tail: &raw[3..],
            page_size: 2,
        };
        assert!(source
            .restore_episode(Some(Some(0)), 4, &mut [])
            .unwrap_err()
            .contains("watermark or lineage"));
        store.watermark = 7;
        store.rows.remove(1);
        let source = GuidanceSource {
            store: &store,
            session: &session(),
            lineage_id: "lineage",
            watermark_seq: 7,
            prefix_len: 3,
            tail: &raw[3..],
            page_size: 2,
        };
        assert_eq!(
            source
                .restore_episode(Some(Some(0)), 4, &mut [])
                .unwrap_err(),
            "guidance store gap at ordinal 1"
        );
    }
}
