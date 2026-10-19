//! How a session's writer assigns runner tags, and the per-lineage tag state admission keeps.
//!
//! Every message part broca stores gets a tag (see `broca_protocol::tags`). The session's
//! [`WalHandle`](crate::WalHandle) assigns them while it appends the record carrying the parts,
//! under the same lock that orders the record's sequence number, so tags are strictly
//! increasing in WAL order. The next number is derived from the WAL (the highest tag any record
//! holds, plus one), so it survives restarts, resumes and agent-data moves, which carry the WAL
//! bytes; retention deletion records the next number on its tombstone so a lineage created
//! after it keeps counting from there.
//!
//! Tags are on for every lineage whose first episode a tag-writing build admits. A lineage
//! admitted earlier stays untagged for good and keeps its exact bytes.

use std::sync::Arc;

use broca_protocol::tags::{MessageTags, PartTags};
use broca_wal::WalRecord;
use serde::{Deserialize, Serialize};

/// What a lineage's records say about tags, as folded from its WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LineageTags {
    /// No episode has started. The first `RunStarted` turns tags on and takes `floor` as its
    /// first tag: 1 for a new session, or the number a deleted predecessor reached.
    Unstarted { floor: u64 },
    /// The lineage started without tags (admitted before tags existed). Nothing is tagged.
    Off,
    /// The lineage is tagged; `next` is the tag the next part gets.
    On { next: u64 },
}

impl LineageTags {
    /// The state of an empty lineage.
    pub const fn unstarted() -> Self {
        LineageTags::Unstarted { floor: 1 }
    }

    /// Fold one record, in WAL order.
    pub fn fold(&mut self, record: &WalRecord) {
        let highest = record
            .runner_tags()
            .iter()
            .filter_map(MessageTags::max_tag)
            .max();
        match self {
            LineageTags::Unstarted { floor } => {
                if matches!(record, WalRecord::RunStarted { .. }) {
                    *self = match (record.runner_tags().is_empty(), highest) {
                        // A first episode written without tags: an untagged lineage.
                        (true, _) => LineageTags::Off,
                        (false, highest) => LineageTags::On {
                            next: highest.map_or(*floor, |tag| tag.saturating_add(1).max(*floor)),
                        },
                    };
                }
            }
            LineageTags::On { next } => {
                if let Some(tag) = highest {
                    *next = (*next).max(tag.saturating_add(1));
                }
            }
            LineageTags::Off => {}
        }
    }

    /// The tag the next part of this lineage gets, if the lineage is (or will be) tagged.
    pub fn next(self) -> Option<u64> {
        match self {
            LineageTags::Unstarted { floor } => Some(floor),
            LineageTags::On { next } => Some(next),
            LineageTags::Off => None,
        }
    }

    /// Raise an unstarted lineage's first tag to at least `floor` (a deleted predecessor's
    /// next tag). A started lineage already counts from its own records.
    pub fn with_floor(self, floor: u64) -> Self {
        match self {
            LineageTags::Unstarted { floor: own } => LineageTags::Unstarted {
                floor: own.max(floor),
            },
            other => other,
        }
    }
}

/// Whether a session's frozen plan asks its runs to show runner tags to the model: the plan's
/// `render_tags` member is `true`. Absent or `false` means off.
pub fn plan_renders_tags(manifest: &broca_wal::PlanManifest) -> bool {
    serde_json::from_str::<serde_json::Value>(manifest.plan.as_json_text())
        .ok()
        .and_then(|plan| plan.get("render_tags").and_then(serde_json::Value::as_bool))
        == Some(true)
}

/// Fold a complete WAL from its origin.
pub fn lineage_tags<'a>(records: impl IntoIterator<Item = &'a WalRecord>) -> LineageTags {
    let mut tags = LineageTags::unstarted();
    for record in records {
        tags.fold(record);
    }
    tags
}

/// The number of parts in each message `record` carries, in record order, when it is a record
/// whose parts get tags; `None` for every other record.
pub(crate) fn tag_shape(record: &WalRecord) -> Option<Vec<usize>> {
    match record {
        WalRecord::RunStarted { input, .. } | WalRecord::Steered { input, .. } => {
            Some(input.iter().map(|message| message.content.len()).collect())
        }
        WalRecord::SendDelivered {
            input: Some(input), ..
        } => Some(input.iter().map(|message| message.content.len()).collect()),
        WalRecord::ModelStepFinished {
            assistant_message, ..
        } => Some(vec![assistant_message.content.len()]),
        WalRecord::ToolResult { .. }
        | WalRecord::ToolNoDispatch { .. }
        | WalRecord::BudgetFired { .. }
        | WalRecord::ApprovalRecorded {
            closing_result: Some(_),
            ..
        } => Some(vec![1]),
        _ => None,
    }
}

/// Write `tags` into `record`'s tag field. `tags` has one entry per message of `record`.
pub(crate) fn set_record_tags(record: &mut WalRecord, tags: Vec<MessageTags>) {
    match record {
        WalRecord::RunStarted { tags: field, .. }
        | WalRecord::Steered { tags: field, .. }
        | WalRecord::SendDelivered { tags: field, .. } => {
            *field = Some(Arc::new(tags));
        }
        WalRecord::ModelStepFinished { tags: field, .. }
        | WalRecord::ToolResult { tags: field, .. }
        | WalRecord::ToolNoDispatch { tags: field, .. }
        | WalRecord::BudgetFired { tags: field, .. }
        | WalRecord::ApprovalRecorded { tags: field, .. } => {
            *field = tags.into_iter().next().map(Box::new);
        }
        _ => {}
    }
}

/// Assign tags from `state` to a record about to be appended, in place. Returns the tags it
/// assigned, one entry per message. A record that already holds tags keeps them, and the
/// counter moves past them.
pub(crate) fn stamp(state: &mut LineageTags, record: &mut WalRecord) -> Option<Vec<MessageTags>> {
    if !record.runner_tags().is_empty() {
        let mut probe = match *state {
            LineageTags::Unstarted { floor } => LineageTags::Unstarted { floor },
            other => other,
        };
        probe.fold(record);
        *state = probe;
        return None;
    }
    let shape = tag_shape(record)?;
    let first = match (*state, &*record) {
        (LineageTags::Unstarted { floor }, WalRecord::RunStarted { .. }) => floor,
        (LineageTags::On { next }, _) => next,
        // Nothing before a lineage's first episode carries parts, and an untagged lineage stays
        // untagged.
        _ => return None,
    };
    let mut next = first;
    let tags: Vec<MessageTags> = shape
        .into_iter()
        .map(|parts| {
            let start = next;
            next = next.saturating_add(parts as u64);
            MessageTags::new((start..next).collect())
        })
        .collect();
    set_record_tags(record, tags.clone());
    *state = LineageTags::On { next };
    Some(tags)
}

/// Attach one record's `tags` to `message`, the message that record carries.
pub fn attach(message: &mut broca_protocol::Message, tags: Option<&MessageTags>) {
    attach_parts(message, tags.map(PartTags::from));
}

/// Attach per-part `tags` to `message`. They cover its blocks one for one: a record's tags are
/// assigned from the same blocks, so a mismatch is a broken invariant, never a partial tagging.
pub fn attach_parts(message: &mut broca_protocol::Message, tags: Option<PartTags>) {
    let Some(tags) = tags else {
        return;
    };
    assert_eq!(
        tags.parts.len(),
        message.content.len(),
        "runner tags must cover a message's blocks one for one"
    );
    message.set_tags(Some(Arc::new(tags)));
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::content::{ContentBlock, Message, Role, ToolOutput, ToolResult};
    use broca_protocol::{BatchId, ToolCallId};
    use broca_wal::record::DispatchOutcome;

    fn result(id: &str) -> WalRecord {
        WalRecord::ToolResult {
            tags: None,
            batch_id: BatchId("b".into()),
            result: Arc::new(ToolResult {
                tool_call_id: ToolCallId(id.into()),
                output: ToolOutput::Text { text: "r".into() },
                is_error: false,
            }),
            outcome: DispatchOutcome::Completed,
            hooks: None,
            reason: None,
            withdraw_elicitation: None,
            provider_held: None,
        }
    }

    fn two_block_steer() -> WalRecord {
        steer(2)
    }

    fn steer(blocks: usize) -> WalRecord {
        WalRecord::Steered {
            tags: None,
            retention: None,
            ts_ms: None,
            send_id: None,
            request_identity_digest: None,
            delivery: None,
            title: None,
            input: Arc::new(vec![Message::new(
                Role::User,
                (0..blocks)
                    .map(|i| ContentBlock::Text {
                        text: format!("{i}"),
                    })
                    .collect(),
            )]),
            hooks: None,
        }
    }

    /// A first episode with one single-block user message, written without tags.
    fn run_started() -> WalRecord {
        let mut record = crate::replay::projection_boundary_tests::run_started(1, "r").record;
        if let WalRecord::RunStarted { input, .. } = &mut record {
            *input = Arc::new(vec![Message::new(
                Role::User,
                vec![ContentBlock::Text { text: "hi".into() }],
            )]);
        }
        record
    }

    #[test]
    fn an_untagged_first_episode_keeps_the_lineage_untagged() {
        let mut state = LineageTags::unstarted();
        let started = run_started();
        state.fold(&started);
        assert_eq!(state, LineageTags::Off);
        let mut later = result("a");
        assert_eq!(stamp(&mut state, &mut later), None);
        assert!(later.runner_tags().is_empty());
    }

    #[test]
    fn stamping_counts_parts_in_wal_order_from_the_floor() {
        let mut state = LineageTags::unstarted().with_floor(40);
        let mut started = run_started();
        let assigned = stamp(&mut state, &mut started).expect("first episode is tagged");
        assert_eq!(assigned, vec![MessageTags::new(vec![40])]);
        let mut two = steer(2);
        assert_eq!(
            stamp(&mut state, &mut two),
            Some(vec![MessageTags::new(vec![41, 42])])
        );
        let mut res = result("a");
        stamp(&mut state, &mut res);
        assert_eq!(res.runner_tags(), &[MessageTags::new(vec![43])]);
        assert_eq!(state, LineageTags::On { next: 44 });

        // Folding the written records gives the writer's own next tag back.
        let folded = lineage_tags([&started, &two, &res]);
        assert_eq!(folded, LineageTags::On { next: 44 });
    }

    fn scratch(tag: &str) -> std::path::PathBuf {
        let dir = broca_test_support::scratch_root()
            .join(format!("broca-tags-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// Every tag in `store`'s WAL for `session`, in WAL order.
    fn wal_tags(store: &crate::FileWalStore, session: &broca_protocol::BindIdentity) -> Vec<u64> {
        use crate::WalStore;
        store
            .read_records(session)
            .unwrap()
            .iter()
            .flat_map(|frame| frame.record.runner_tags().to_vec())
            .flat_map(|tags| tags.parts)
            .collect()
    }

    /// Open a writer on an existing WAL the way admission does: fold the lineage's tag state
    /// from its records and seed the writer with it.
    fn reopen(
        store: &crate::FileWalStore,
        session: &broca_protocol::BindIdentity,
    ) -> crate::WalHandle {
        use crate::WalStore;
        let records = store.read_records(session).unwrap();
        let checkpoint = crate::AdmissionCheckpointV1::from_frames(&records).unwrap();
        let writer = crate::WalHandle::spawn(store.open(session).unwrap(), 1).unwrap();
        assert_eq!(
            writer.lineage_tags(),
            None,
            "a non-empty WAL starts unknown"
        );
        writer.seed_lineage_tags(checkpoint.tags);
        writer
    }

    fn strictly_increasing(tags: &[u64]) -> bool {
        tags.windows(2).all(|pair| pair[0] < pair[1])
    }

    /// Tags keep counting, never repeating, across a restart (a new writer over the same WAL,
    /// seeded from its records), a resumed episode, and an agent-data move (the WAL's bytes
    /// copied to another root and continued there).
    #[tokio::test]
    async fn tags_are_strictly_increasing_across_restart_resume_and_a_moved_session() {
        use crate::WalStore;
        let session = broca_protocol::BindIdentity::new("/tmp/p", "h", "tags");
        let store = crate::FileWalStore::new(scratch("origin"));

        let writer = crate::WalHandle::spawn(store.open(&session).unwrap(), 1).unwrap();
        assert_eq!(writer.lineage_tags(), Some(LineageTags::unstarted()));
        let (_, input) = writer.record_tagged(run_started());
        assert_eq!(input, Some(vec![MessageTags::new(vec![1])]));
        writer.record(two_block_steer());
        writer.record(result("a"));
        writer.shutdown().await.unwrap();
        assert_eq!(wal_tags(&store, &session), vec![1, 2, 3, 4]);

        // A restart: a fresh writer over the same WAL.
        let writer = reopen(&store, &session);
        assert_eq!(writer.lineage_tags(), Some(LineageTags::On { next: 5 }));
        writer.record(result("b"));
        writer.shutdown().await.unwrap();

        // A resume of the episode after a crash, through another restart.
        let writer = reopen(&store, &session);
        writer.record(result("c"));
        writer.record(two_block_steer());
        writer.shutdown().await.unwrap();
        let before_move = wal_tags(&store, &session);
        assert_eq!(before_move, vec![1, 2, 3, 4, 5, 6, 7, 8]);

        // An agent-data move: the WAL's bytes, as they are, under another root.
        let moved = crate::FileWalStore::new(scratch("moved"));
        let destination = moved.wal_path(&session);
        std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
        std::fs::copy(store.wal_path(&session), &destination).unwrap();
        let writer = reopen(&moved, &session);
        writer.record(result("d"));
        writer.shutdown().await.unwrap();
        let after_move = wal_tags(&moved, &session);
        assert_eq!(&after_move[..before_move.len()], before_move.as_slice());
        assert_eq!(after_move.last(), Some(&9));
        assert!(strictly_increasing(&after_move), "{after_move:?}");
        assert!(moved.read_records(&session).unwrap().len() > 1);
    }

    /// A writer reopened over a non-empty WAL knows nothing of its lineage's tags until an
    /// admission seeds it. It refuses a record with parts rather than write it untagged, and
    /// its next barrier fails, so nothing can claim the record durable.
    #[tokio::test]
    async fn an_unseeded_writer_refuses_a_record_with_parts() {
        use crate::WalStore;
        let session = broca_protocol::BindIdentity::new("/tmp/p", "h", "unseeded");
        let store = crate::FileWalStore::new(scratch("unseeded"));
        let writer = crate::WalHandle::spawn(store.open(&session).unwrap(), 1).unwrap();
        writer.record(run_started());
        writer.shutdown().await.unwrap();

        let writer = crate::WalHandle::spawn(store.open(&session).unwrap(), 1).unwrap();
        assert_eq!(writer.lineage_tags(), None);
        // A record without parts is written as before.
        assert!(matches!(
            writer.record(WalRecord::TurnFinished {
                step_id: broca_protocol::StepId(1)
            }),
            crate::RecordOutcome::Accepted(_)
        ));
        assert!(matches!(
            writer.record(result("a")),
            crate::RecordOutcome::Refused(_)
        ));
        assert!(writer.is_poisoned());
        assert!(writer.sync().await.is_err());
        assert!(!store
            .read_records(&session)
            .unwrap()
            .iter()
            .any(|frame| matches!(frame.record, WalRecord::ToolResult { .. })));
    }

    /// A lineage whose first episode was written without tags (by a build before tags) stays
    /// untagged: later records keep their exact, ungated bytes.
    #[tokio::test]
    async fn a_lineage_started_untagged_is_never_tagged() {
        use crate::WalStore;
        let session = broca_protocol::BindIdentity::new("/tmp/p", "h", "untagged");
        let store = crate::FileWalStore::new(scratch("untagged"));
        {
            let mut wal = store.open(&session).unwrap();
            wal.append(1, &run_started(), true).unwrap();
        }
        let writer = reopen(&store, &session);
        assert_eq!(writer.lineage_tags(), Some(LineageTags::Off));
        writer.record(result("a"));
        writer.shutdown().await.unwrap();
        assert!(wal_tags(&store, &session).is_empty());
        let plain = store.read_records(&session).unwrap();
        assert!(plain.iter().all(|frame| !frame
            .record
            .required_features()
            .contains(&broca_wal::framing::RUNNER_TAGS_FEATURE)));
    }

    /// A lineage created at an address whose previous lineage retention deleted starts past
    /// every tag the deleted one used, as its tombstone recorded.
    #[tokio::test]
    async fn a_lineage_after_retention_deletion_never_reuses_a_tag() {
        use crate::WalStore;
        let session = broca_protocol::BindIdentity::new("/tmp/p", "h", "deleted");
        let store = crate::FileWalStore::new(scratch("deleted"));
        let writer = crate::WalHandle::spawn(store.open(&session).unwrap(), 1).unwrap();
        writer.record(run_started());
        writer.record(result("a"));
        writer.shutdown().await.unwrap();
        let deleted = wal_tags(&store, &session);
        let next = lineage_tags(
            store
                .read_records(&session)
                .unwrap()
                .iter()
                .map(|frame| &frame.record),
        )
        .next()
        .unwrap();

        // Retention moves the WAL away and leaves a tombstone naming the next tag.
        std::fs::remove_file(store.wal_path(&session)).unwrap();
        let stone = store.tombstone_path(&session, "deleted-lineage");
        std::fs::create_dir_all(stone.parent().unwrap()).unwrap();
        std::fs::write(
            &stone,
            serde_json::json!({"lineage_id":"deleted-lineage","expired_at_ms":1,"seq":1,"next_tag":next})
                .to_string(),
        )
        .unwrap();
        assert_eq!(store.retired_tag_floor(&session).unwrap(), next);

        let writer = crate::WalHandle::spawn(store.open(&session).unwrap(), 1).unwrap();
        writer.seed_lineage_tags(Some(
            LineageTags::unstarted().with_floor(store.retired_tag_floor(&session).unwrap()),
        ));
        writer.record(run_started());
        writer.shutdown().await.unwrap();
        let fresh = wal_tags(&store, &session);
        assert_eq!(fresh, vec![next]);
        assert!(deleted.iter().all(|tag| *tag < fresh[0]));
    }
}
