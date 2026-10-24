use super::*;
use crate::SqliteStore;
use broca_core::run::repetition_guidance::GuidanceSource;
use broca_core::tool_repetition::{RepetitionObserver, RepetitionVerdict};
use broca_core::{ReadView, StorePageSnapshot, StorePrefix};
use broca_protocol::request::CallOptions;
use std::sync::{Arc, Mutex};

struct CountingStore {
    store: SqliteStore,
    reads: Mutex<Vec<(u64, u64)>>,
}

impl MaterializedStore for CountingStore {
    fn read_prefix_with_watermark(&self, _: &BindIdentity) -> Result<StorePrefix, String> {
        panic!("guidance must not read the full prefix")
    }

    fn read_page_with_watermark(
        &self,
        session: &BindIdentity,
        from: Option<u64>,
        limit: u64,
        view: ReadView,
    ) -> Result<StorePageSnapshot, String> {
        assert_eq!(view, ReadView::Raw);
        self.reads.lock().unwrap().push((from.unwrap(), limit));
        self.store
            .read_page_with_watermark(session, from, limit, view)
    }
}

fn history(session: &BindIdentity, input_pair: bool) -> Vec<FramedRecord> {
    let mut records = vec![run_started(
        1,
        "streamed",
        session,
        "question",
        ModelHandle::new("stub", "m"),
    )];
    for step in 0..4 {
        let mut turn = tool_turn(records.len() as u64 + 1, step);
        if let WalRecord::ModelStepFinished {
            assistant_message, ..
        } = &mut turn[0].record
        {
            if let ContentBlock::ToolCall(call) = &mut Arc::make_mut(assistant_message).content[0] {
                call.tool_name = "read".into();
            }
        }
        records.extend(turn);
    }
    if input_pair {
        let raw = broca_core::replay(&records.iter().map(|f| f.record.clone()).collect::<Vec<_>>())
            .unwrap();
        if let WalRecord::RunStarted { input, .. } = &mut records[0].record {
            Arc::make_mut(input).extend_from_slice(&raw.prompt[1..3]);
        }
    }
    records
}

fn compare(input_pair: bool, page_size: u64, already_guided: bool, held_count: Option<usize>) {
    let session = session("streamed-guidance");
    let records = history(&session, input_pair);
    let raw =
        broca_core::replay(&records.iter().map(|f| f.record.clone()).collect::<Vec<_>>()).unwrap();
    let span = raw.episode_step_spans.as_ref().unwrap()[0].clone();
    let mut expected = raw.prompt.clone();
    broca_core::run::restore_repetition_guidance(&mut expected[span.clone()]);
    let expected_observer = RepetitionObserver::from_prompt(&raw.prompt[span.clone()]);
    // The resumed episode is still open. Exercise each tool-turn materialization boundary,
    // including a repeated call present only in the tail, which must not be a store gap.
    for projected_steps in 1..=4 {
        let store = CountingStore {
            store: SqliteStore::open_in_memory().unwrap(),
            reads: Mutex::new(Vec::new()),
        };
        store
            .store
            .project(&session, &records[..1 + projected_steps * 5])
            .unwrap();
        store
            .store
            .conn
            .lock()
            .unwrap()
            .execute("UPDATE meta SET lineage_id='guidance-lineage'", [])
            .unwrap();
        let snapshot = store
            .store
            .read_page_with_watermark(&session, Some(0), 1, ReadView::Raw)
            .unwrap();
        let prefix = store.store.read_prefix_with_watermark(&session).unwrap();
        let tail_records: Vec<_> = records
            .iter()
            .filter(|f| f.seq > snapshot.watermark_seq)
            .map(|f| f.record.clone())
            .collect();
        let resumed = broca_core::resume_state_from_store(
            &prefix,
            &tail_records,
            raw.run_config.clone(),
            raw.run_id.clone(),
            None,
        )
        .unwrap();
        assert_eq!(resumed.prompt, raw.prompt);
        let tail = &resumed.prompt[snapshot.prefix_len as usize..];
        let source = GuidanceSource {
            store: &store,
            session: &session,
            lineage_id: snapshot.lineage_id.as_deref().unwrap(),
            watermark_seq: snapshot.watermark_seq,
            prefix_len: snapshot.prefix_len,
            tail,
            page_size,
        };
        let mut held = if already_guided {
            expected.clone()
        } else {
            raw.prompt.clone()
        };
        if let Some(count) = held_count {
            held.retain(|m| {
                m.ordinal
                    .is_some_and(|o| o >= (raw.prompt.len() - count) as u64)
            });
        }
        let mut observer = source
            .restore_episode(Some(Some(span.start as u64)), span.end as u64, &mut held)
            .unwrap();
        assert_eq!(
            observer, expected_observer,
            "all counters and raw-output digests must match"
        );
        let expected_held: Vec<_> = expected
            .iter()
            .filter(|m| held.iter().any(|h| h.ordinal == m.ordinal))
            .cloned()
            .collect();
        assert_eq!(held, expected_held);
        assert_eq!(
            broca_core::canonical_request_bytes(&CallOptions::new(held.clone())),
            broca_core::canonical_request_bytes(&CallOptions::new(expected_held))
        );
        let text = serde_json::to_string(&held).unwrap();
        assert_eq!(
            text.matches("Tool repetition guidance:").count(),
            held_count.unwrap_or(3)
        );
        let call = ToolCall {
            tool_call_id: ToolCallId("later".into()),
            tool_name: "read".into(),
            input: serde_json::json!({}),
            thought_signature: None,
        };
        assert_eq!(
            observer.observe(&call, b"ok", false),
            RepetitionVerdict::NoProgress {
                tool_name: "read".into(),
                count: 5
            }
        );
        let reads = store.reads.lock().unwrap();
        let requested: Vec<_> = reads
            .iter()
            .flat_map(|(from, limit)| *from..*from + *limit)
            .collect();
        assert_eq!(
            requested,
            (span.start as u64..snapshot.prefix_len).collect::<Vec<_>>()
        );
    }
}

#[test]
fn streamed_guidance_excludes_admitted_input_pairs() {
    compare(true, 4, false, None);
}

#[test]
fn streamed_guidance_carries_pairs_across_store_pages() {
    compare(false, 1, false, None);
}

#[test]
fn streamed_guidance_rebuilds_already_guided_held_outputs() {
    compare(false, 3, true, None);
}

#[test]
fn streamed_guidance_keeps_active_observer_without_held_history() {
    compare(false, 2, false, Some(1));
}

#[test]
fn streamed_guidance_active_episode_can_have_no_held_messages() {
    compare(false, 2, false, Some(0));
}
