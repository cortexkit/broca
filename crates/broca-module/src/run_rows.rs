//! Run-scoped reads from the content-free offset index on opted-in state roots.

use std::cell::RefCell;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use broca_core::replay::{PositionedFrame, SeedBoundary};
use broca_core::{FileWalStore, ReconciledRunState, ReplayState};
use broca_protocol::{BindIdentity, RunId};
use broca_store::db::{DbError, OverhaulDb};
use broca_wal::framing::{decode_next, DecodeStep};
use broca_wal::{FramedRecord, WalLineageId, WalRecord};
use rusqlite::{params, OptionalExtension};
use sha2::{Digest, Sha256};

/// Owned by the registry; legacy registries never construct or consult this source.
pub struct OverhaulRunSource {
    db: Arc<OverhaulDb>,
    wal: Arc<FileWalStore>,
    #[cfg(test)]
    reads: std::sync::Mutex<Vec<(u64, u64)>>,
}

pub(crate) struct RunRead {
    pub records: Vec<FramedRecord>,
    pub replayed: ReplayState,
    pub state: ReconciledRunState,
    pub lineage_id: Option<WalLineageId>,
}

struct Row {
    sid: String,
    lineage_id: Option<String>,
    start_seq: u64,
    start_off: u64,
    next_off: Option<u64>,
    boundary: Option<SeedBoundary>,
    boundary_kind: Option<String>,
    qualifier: Option<(u64, u64, u64, Vec<u8>)>,
    ordinal_base: u64,
    tag_high: u64,
    first_episode: bool,
}

impl OverhaulRunSource {
    pub fn new(db: Arc<OverhaulDb>, wal: Arc<FileWalStore>) -> Self {
        Self {
            db,
            wal,
            #[cfg(test)]
            reads: Default::default(),
        }
    }

    fn row(&self, key: &BindIdentity, run_id: &RunId) -> Result<Option<Row>, DbError> {
        self.db.read_snapshot(|tx| {
            let row = tx.query_row(
                "SELECT r.sid,e.run_started_seq,e.run_started_off,
                    (SELECT MIN(run_started_off) FROM episode WHERE sid=r.sid AND n>e.n),
                    NOT EXISTS(SELECT 1 FROM episode WHERE sid=r.sid AND n<e.n AND run_id IS NOT NULL),s.lineage_id
                 FROM run r JOIN session s ON s.sid=r.sid
                 JOIN episode e ON e.sid=r.sid AND e.n=r.episode AND e.run_id=r.run_id
                 WHERE r.run_id=?1 AND s.project_root=?2 AND s.harness=?3 AND s.session_key=?4
                    AND s.location='live'
                    AND (SELECT COUNT(*) FROM episode WHERE sid=r.sid AND run_id=r.run_id)=1",
                params![run_id.0, key.project_root.to_string_lossy(), key.harness, key.session],
                |r| Ok(Row {
                    sid: r.get(0)?, lineage_id: r.get(5)?, start_seq: r.get(1)?, start_off: r.get(2)?,
                    next_off: r.get(3)?, first_episode: r.get(4)?, boundary: None,
                    qualifier: None, boundary_kind: None, ordinal_base: 0, tag_high: 0,
                }),
            ).optional()?;
            let Some(mut row) = row else { return Ok(None) };
            let boundary = tx.query_row(
                "SELECT lineage_id,seq,frame_off,frame_end,frame_identity,qualified,
                        terminal_seq,terminal_off,terminal_end,terminal_identity,kind
                 FROM projection_boundary WHERE sid=?1 AND seq<?2 AND qualified=1
                 ORDER BY seq DESC LIMIT 1",
                params![row.sid, row.start_seq],
                |r| {
                    let boundary = SeedBoundary {
                        lineage_id: r.get(0)?, seq: r.get(1)?, frame_off: r.get(2)?,
                        frame_end: r.get(3)?, frame_identity: r.get(4)?, qualified: r.get(5)?,
                    };
                    let qualifier = match r.get::<_, Option<u64>>(6)? {
                        Some(seq) => Some((seq, r.get(7)?, r.get(8)?, r.get(9)?)),
                        None => None,
                    };
                    Ok((boundary, qualifier, r.get::<_, String>(10)?))
                },
            ).optional()?;
            if let Some((boundary, qualifier, kind)) = boundary {
                (row.ordinal_base, row.tag_high) = tx.query_row(
                    "SELECT COALESCE(MAX(ord)+1,0),COALESCE(MAX(tag_high),0)
                     FROM message WHERE sid=?1 AND hi_off<=?2",
                    params![row.sid, boundary.frame_end], |r| Ok((r.get(0)?,r.get(1)?)),
                )?;
                row.boundary = Some(boundary);
                row.qualifier = qualifier;
                row.boundary_kind = Some(kind);
            }
            Ok(Some(row))
        })
    }

    pub(super) fn read(
        &self,
        key: &BindIdentity,
        run_id: &RunId,
    ) -> Result<Option<RunRead>, String> {
        let Some(row) = self.row(key, run_id).map_err(|e| e.to_string())? else {
            return Ok(None);
        };
        // Without a safe cut, only the first episode can replay at the actual log origin.
        // Later episodes fall back rather than inventing a seed from a bare run offset.
        if row.boundary.is_none() && !row.first_episode {
            return Ok(None);
        }
        let path = self
            .wal
            .resolve_live_path(key)
            .map_err(|e| e.to_string())?
            .ok_or("run log is not live")?;
        // Windows requires write access for sync_all's FlushFileBuffers call.
        let file = File::options()
            .read(true)
            .write(cfg!(windows))
            .open(path)
            .map_err(|e| e.to_string())?;
        let len = file.metadata().map_err(|e| e.to_string())?.len();
        // Sync the same descriptor we read, not a pathname that a move could replace.
        // Bytes appended after this captured length belong to the next read.
        file.sync_all().map_err(|e| e.to_string())?;
        let file = RefCell::new(file);
        let read_at = |off: u64, count: u64| -> std::io::Result<Vec<u8>> {
            #[cfg(test)]
            self.reads.lock().unwrap().push((off, count));
            let mut file = file.borrow_mut();
            file.seek(SeekFrom::Start(off))?;
            let mut bytes = Vec::new();
            file.by_ref()
                .take(
                    count
                        .min(len.saturating_sub(off))
                        .min(self.wal.max_read_bytes()),
                )
                .read_to_end(&mut bytes)?;
            Ok(bytes)
        };
        let header =
            read_at(0, broca_wal::framing::LINEAGE_FRAME_LEN as u64).map_err(|e| e.to_string())?;
        let lineage_id = match decode_next(&header) {
            DecodeStep::Lineage { lineage_id, .. } => Some(lineage_id),
            _ => None,
        };
        if lineage_id.map(|id| id.to_string()) != row.lineage_id {
            return Ok(None);
        }
        let off = row.boundary.as_ref().map_or(0, |b| b.frame_end);
        let end = row.next_off.unwrap_or(len).min(len);
        if off > row.start_off || row.start_off >= end {
            return Ok(None);
        }
        if let Some(next) = row.next_off {
            let mut count = 1024u64;
            loop {
                let bytes = read_at(next, count).map_err(|e| e.to_string())?;
                match decode_next(&bytes) {
                    DecodeStep::Record { framed, .. }
                        if matches!(framed.record, WalRecord::RunStarted { .. }) =>
                    {
                        break
                    }
                    DecodeStep::TornTail
                        if count < len.saturating_sub(next)
                            && count < self.wal.max_read_bytes() =>
                    {
                        count = count.saturating_mul(2).min(self.wal.max_read_bytes())
                    }
                    _ => return Ok(None),
                }
            }
        }
        let bytes = read_at(off, end - off).map_err(|e| e.to_string())?;
        if bytes.len() as u64 != end - off {
            return Ok(None);
        }
        let mut tail = Vec::new();
        let mut at = 0;
        let mut found = false;
        while at < bytes.len() {
            match decode_next(&bytes[at..]) {
                DecodeStep::Record { framed, consumed } => {
                    if let WalRecord::RunStarted {
                        run_id: id,
                        session,
                        ..
                    } = &framed.record
                    {
                        if found {
                            break;
                        }
                        if id != run_id
                            || session != key
                            || framed.seq != row.start_seq
                            || off + at as u64 != row.start_off
                        {
                            return Err(format!("run start mismatch: got {id:?} seq={} off={}, indexed {run_id:?} seq={} off={}",framed.seq,off+at as u64,row.start_seq,row.start_off));
                        }
                        found = true;
                    }
                    tail.push(PositionedFrame {
                        off: off + at as u64,
                        framed: *framed,
                    });
                    at += consumed;
                }
                DecodeStep::Lineage {
                    lineage_id: found,
                    consumed,
                } if Some(found) == lineage_id => at += consumed,
                DecodeStep::TornTail => break,
                DecodeStep::Lineage { .. } => {
                    return Err(format!("unexpected lineage frame at {}", off + at as u64))
                }
                DecodeStep::Corrupt(error) => {
                    return Err(format!("corrupt run frame at {}: {error}", off + at as u64))
                }
                DecodeStep::NewerWriter { .. } => {
                    return Err(format!("newer run frame at {}", off + at as u64))
                }
            }
        }
        if !found
            || tail
                .windows(2)
                .any(|p| p[0].framed.seq.checked_add(1) != Some(p[1].framed.seq))
        {
            return Err(format!(
                "run range is incomplete: found={found}, sequences={:?}",
                tail.iter().map(|p| p.framed.seq).collect::<Vec<_>>()
            ));
        }
        let records: Vec<_> = tail.iter().map(|p| p.framed.clone()).collect();
        if broca_core::replay::tail_contains_compaction_setup(&records) {
            return Ok(None);
        }
        let replayed = if let Some(boundary) = &row.boundary {
            let Some(count) = boundary.frame_end.checked_sub(boundary.frame_off) else {
                return Ok(None);
            };
            let frame = read_at(boundary.frame_off, count).map_err(|e| e.to_string())?;
            let kind_matches = match (row.boundary_kind.as_deref(), decode_next(&frame)) {
                (Some("turn_finished"), DecodeStep::Record { framed, .. }) => {
                    matches!(framed.record, WalRecord::TurnFinished { .. })
                }
                (Some("no_tool_step"), DecodeStep::Record { framed, .. }) => {
                    matches!(&framed.record,WalRecord::ModelStepFinished { assistant_message, .. } if assistant_message.tool_calls().next().is_none())
                }
                (Some("zero_step_seed"), DecodeStep::Record { framed, .. }) => matches!(
                    framed.record,
                    WalRecord::RunStarted {
                        origin: broca_wal::RunOrigin::SeededFrom { .. },
                        ..
                    }
                ),
                _ => false,
            };
            if !kind_matches {
                return Ok(None);
            }
            // No-tool and seed cuts are qualified by a later terminal. Validate that
            // frame too, so a restore before the qualifier cannot reuse the cut.
            if let Some((seq, lo, hi, hash)) = &row.qualifier {
                let Some(count) = hi.checked_sub(*lo) else {
                    return Ok(None);
                };
                let bytes = read_at(*lo, count).map_err(|e| e.to_string())?;
                if Sha256::digest(&bytes).as_slice() != hash
                    || !matches!(decode_next(&bytes),
                    DecodeStep::Record { framed, consumed } if framed.seq == *seq
                        && consumed == bytes.len() && matches!(framed.record, WalRecord::RunFinished { .. }))
                {
                    return Ok(None);
                }
            }
            match broca_core::replay::replay_seeded_at_validated_boundary(
                &read_at,
                boundary,
                &tail,
                None,
                None,
                None,
                None,
                row.ordinal_base,
                false,
                broca_core::tags::TagSeed {
                    high: row.tag_high,
                    rules: None,
                },
                Vec::new(),
            ) {
                Ok(state) => state,
                Err(_) => return Ok(None),
            }
        } else {
            // This is an origin replay, not a read resumed from persisted state.
            if tail.first().is_none_or(|p| p.framed.seq != 1) {
                return Err("origin replay does not begin with sequence one".into());
            }
            broca_core::replay::replay(
                &records.iter().map(|f| f.record.clone()).collect::<Vec<_>>(),
            )
            .map_err(|e| e.to_string())?
        };
        let Some(state) =
            broca_core::reconcile_run_state(records.iter().map(|f| &f.record), &run_id.0)
        else {
            return Err("run range has no reconcilable run state".into());
        };
        if state.state == broca_core::RunIndexState::Completed
            && !replayed
                .prompt
                .iter()
                .any(|m| m.role == broca_protocol::content::Role::Assistant)
        {
            return Err("completed run range has no assistant message".into());
        }
        if let Some(terminal) = tail.iter().rev().find(|p| {
            p.framed.seq > row.start_seq && matches!(p.framed.record, WalRecord::RunFinished { .. })
        }) {
            let terminal_kind = match &terminal.framed.record {
                WalRecord::RunFinished { reason, .. } => {
                    serde_json::to_string(reason).map_err(|e| e.to_string())?
                }
                _ => unreachable!("terminal selection requires RunFinished"),
            };
            self.db.authoritative_transaction(|tx| {
                tx.execute("UPDATE run SET state=?2,state_seq=?3,terminal_off=?4 WHERE run_id=?1 AND sid=?5 AND state_seq<=?3",
                    params![run_id.0,state.state.as_str(),terminal.framed.seq,terminal.off,row.sid])?;
                tx.execute("UPDATE episode SET terminal_off=?3,terminal_kind=?4 WHERE sid=?1 AND run_id=?2",
                    params![row.sid,run_id.0,terminal.off,terminal_kind])?;
                Ok(())
            }).map_err(|e| e.to_string())?;
        }
        Ok(Some(RunRead {
            records,
            replayed,
            state,
            lineage_id,
        }))
    }

    pub(super) fn active_runs(
        &self,
        fallback: &[(RunId, BindIdentity)],
    ) -> Result<Vec<(RunId, BindIdentity)>, String> {
        let mut candidates = self
            .db
            .read_snapshot(|tx| {
                let mut stmt = tx.prepare(
                    "SELECT r.run_id,s.project_root,s.harness,s.session_key
                FROM run r JOIN session s ON s.sid=r.sid WHERE r.state='active'",
                )?;
                let rows = stmt
                    .query_map([], |r| {
                        Ok((
                            RunId(r.get(0)?),
                            BindIdentity::new(
                                r.get::<_, String>(1)?,
                                r.get::<_, String>(2)?,
                                r.get::<_, String>(3)?,
                            ),
                        ))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .map_err(|e| e.to_string())?;
        // Until publication covers every run, retain legacy candidates too. The registry
        // reconciles them against validated run rows before deciding whether to mark them.
        for (run_id, key) in fallback {
            if !candidates.contains(&(run_id.clone(), key.clone())) {
                candidates.push((run_id.clone(), key.clone()));
            }
        }
        Ok(candidates)
    }
}

#[cfg(all(test, feature = "test-clock"))]
mod tests {
    use super::*;
    use crate::registry::{ActorHandle, LineageOpener, OpenError, RegistryHandle};
    use crate::serve::ServeHandler;
    use broca_core::WalStore;
    use broca_protocol::content::{AssistantMessage, ContentBlock, Message, Role};
    use broca_protocol::{MessageId, RunFinishReason, StepId, Usage};
    use broca_store::db::{LogLocation, PublishedSession};
    use broca_wal::Wal;
    use cortexkit_role_llm_runner::tool_calls::ToolCallsRequest;
    use serde_json::json;

    struct NeverOpen;
    #[async_trait::async_trait]
    impl LineageOpener for NeverOpen {
        async fn open(
            &self,
            _: &BindIdentity,
            _: u64,
            _: RegistryHandle,
        ) -> Result<ActorHandle, OpenError> {
            panic!("terminal run reads must not open an actor")
        }
    }

    struct Fixture {
        root: std::path::PathBuf,
        key: BindIdentity,
        wal: Box<dyn Wal + Send>,
        source: Arc<OverhaulRunSource>,
    }
    impl Fixture {
        fn new(name: &str) -> Self {
            let root = broca_test_support::scratch_root()
                .join(format!("run-rows-{name}-{}", std::process::id()));
            std::fs::create_dir_all(&root).unwrap();
            std::fs::write(root.join("state-format"), "broca-state/2").unwrap();
            let key = BindIdentity::new(root.join("project"), "runner", "session");
            let store = Arc::new(FileWalStore::with_config(
                root.join("wal"),
                broca_wal::WalConfig::OVERHAUL,
            ));
            let wal = store.open(&key).unwrap();
            let source = Arc::new(OverhaulRunSource::new(
                Arc::new(OverhaulDb::open(&root).unwrap()),
                store,
            ));
            Self {
                root,
                key,
                wal,
                source,
            }
        }
        fn registry(&self, indexed: bool) -> crate::LineageRegistry {
            crate::LineageRegistry::new_with_run_rows(
                Arc::new(NeverOpen),
                Some(self.source.wal.clone()),
                None,
                indexed.then(|| self.source.clone()),
            )
        }
        fn append(&mut self, record: WalRecord) {
            self.wal.append(1, &record, true).unwrap();
        }
        fn start(&mut self, id: &str) {
            self.append(serde_json::from_value(json!({
                "type":"run_started", "run_id":id, "session":self.key,
                "config":{"model":{"provider_module_id":"provider","model_id":"model"},
                    "tools":[], "tool_choice":{"type":"auto"}, "generation":{},
                    "response_format":{"type":"text"}, "context_epoch":0,"renderer_version":1},
                "input":[Message::new(Role::User,vec![ContentBlock::Text{text:format!("input-{id}")}])],
                "origin":{"kind":"fresh"}
            })).unwrap());
        }
        fn step(&mut self, step: u64, content: Vec<ContentBlock>) {
            let step_id = StepId(step);
            self.append(WalRecord::StepStarted { step_id });
            self.append(WalRecord::ModelStepFinished {
                fallback: Default::default(),
                written_at_ms: None,
                step_id,
                assistant_message: Arc::new(AssistantMessage {
                    message_id: MessageId(format!("a{step}")),
                    content,
                }),
                usage: Usage {
                    input_tokens: Some(7),
                    output_tokens: Some(3),
                    ..Default::default()
                },
                finish_reason: None,
                retries_used: 1,
                service_tier_used: None,
                speed_used: None,
                hooks: None,
                tags: None,
            });
        }
        fn finish(&mut self, reason: RunFinishReason) {
            match reason {
                RunFinishReason::BudgetExhausted => {
                    self.append(WalRecord::InputTokenBudgetExhausted {
                        detail: broca_protocol::BudgetExhaustedDetail::new(10, 10, "limit_reached"),
                    })
                }
                RunFinishReason::ContextCeiling => self.append(WalRecord::ContextCeilingDecided {
                    step_id: StepId(2),
                    detail: Some(broca_protocol::ContextCeilingDetail::new(
                        100,
                        90,
                        "view_timeout",
                    )),
                }),
                _ => {}
            }
            self.append(WalRecord::RunFinished {
                interrupt_reason: None,
                reason,
                ts_ms: None,
                usage: Usage {
                    input_tokens: Some(7),
                    output_tokens: Some(3),
                    ..Default::default()
                },
                error: None,
                retries_used: 1,
            });
        }
        fn index(&self) {
            let bytes = std::fs::read(self.source.wal.wal_path(&self.key)).unwrap();
            self.source
                .db
                .index(&PublishedSession {
                    bind: &self.key,
                    session_ref: "test-ref",
                    location: LogLocation::Live,
                    bytes: &bytes,
                })
                .unwrap();
        }
        fn history(&mut self) {
            self.start("history");
            self.step(
                1,
                vec![ContentBlock::Text {
                    text: "history answer".repeat(1000),
                }],
            );
            self.finish(RunFinishReason::Completed);
        }
        fn tool_turn(&mut self) {
            let calls: Vec<_> = (1..=2)
                .map(|n| broca_protocol::content::ToolCall {
                    tool_call_id: broca_protocol::ToolCallId(format!("call-{n}")),
                    tool_name: "echo".into(),
                    input: json!({"text":"hello"}),
                    thought_signature: None,
                })
                .collect();
            self.step(
                1,
                calls.iter().cloned().map(ContentBlock::ToolCall).collect(),
            );
            let batch_id = broca_protocol::BatchId("batch".into());
            self.append(WalRecord::ToolBatchStarted {
                batch_id: batch_id.clone(),
                step_id: StepId(1),
                planned_calls: calls.iter().map(|call| call.tool_call_id.clone()).collect(),
            });
            for call in calls {
                self.append(WalRecord::ToolDispatchIntent {
                    dispatched_at_ms: None,
                    batch_id: batch_id.clone(),
                    tool_call_id: call.tool_call_id.clone(),
                    tool_name: Some(call.tool_name),
                    args: Arc::new(call.input),
                    module_id: Some("tools".into()),
                    schema_pin: None,
                    hooks: None,
                    approval_digest: None,
                    call_key: None,
                });
                self.append(WalRecord::ToolResult {
                    finished_at_ms: None,
                    batch_id: batch_id.clone(),
                    result: Arc::new(broca_protocol::content::ToolResult {
                        tool_call_id: call.tool_call_id,
                        output: broca_protocol::content::ToolOutput::Text {
                            text: "hello".into(),
                        },
                        is_error: false,
                    }),
                    outcome: broca_wal::record::DispatchOutcome::Completed,
                    hooks: None,
                    reason: None,
                    withdraw_elicitation: None,
                    provider_held: None,
                    tags: None,
                });
            }
            self.append(WalRecord::ToolBatchFinished { batch_id });
            self.append(WalRecord::TurnFinished { step_id: StepId(1) });
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn run_row_read_barrier_works_with_an_open_writer() {
        let mut f = Fixture::new("read-barrier");
        f.start("target");
        f.index();
        let run = RunId("target".into());
        let path = f.source.wal.wal_path(&f.key);
        let before = std::fs::read(&path).unwrap();
        let active = f
            .source
            .read(&f.key, &run)
            .expect("the run-row durability barrier must succeed with the WAL writer open")
            .expect("the indexed active run must be readable");
        assert_eq!(active.state.state.as_str(), "active");
        assert_eq!(std::fs::read(&path).unwrap(), before);

        f.finish(RunFinishReason::Interrupted);
        let before = std::fs::read(&path).unwrap();
        let terminal = f.source.read(&f.key, &run).unwrap().unwrap();
        assert_eq!(terminal.state.state.as_str(), "interrupted");
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert_eq!(f.source.wal.range_reads_for_test(), 0);
    }

    #[tokio::test]
    async fn run_rows_equal_legacy_without_sequence_zero_reads() {
        for (n, reason) in [
            RunFinishReason::Completed,
            RunFinishReason::Interrupted,
            RunFinishReason::Cancelled,
            RunFinishReason::MaxSteps,
            RunFinishReason::BudgetExhausted,
            RunFinishReason::ContextCeiling,
            RunFinishReason::Error,
            RunFinishReason::TransformUnavailable,
        ]
        .into_iter()
        .enumerate()
        {
            let mut f = Fixture::new(&format!("equivalence-{n}"));
            f.history();
            f.start("target");
            f.tool_turn();
            f.step(
                2,
                vec![ContentBlock::Text {
                    text: "target answer".into(),
                }],
            );
            f.finish(reason);
            f.index();
            let legacy = f.registry(false);
            let indexed = f.registry(true);
            for id in ["history", "target"] {
                let run = RunId(id.into());
                assert!(
                    f.source.read(&f.key, &run).unwrap().is_some(),
                    "indexed run {id} must have a usable read; ranges: {:?}",
                    f.source.reads.lock().unwrap()
                );
                let expected_status = legacy.run_status(&f.key, run.clone()).await.unwrap();
                let expected_result = legacy.run_result(&f.key, run.clone()).await.unwrap();
                let expected_calls = legacy
                    .run_tool_calls(&f.key, ToolCallsRequest::new(id))
                    .await
                    .unwrap();
                let before = f.source.wal.range_reads_for_test();
                assert_eq!(
                    serde_json::to_value(indexed.run_status(&f.key, run.clone()).await.unwrap())
                        .unwrap(),
                    serde_json::to_value(expected_status).unwrap()
                );
                assert_eq!(
                    serde_json::to_value(indexed.run_result(&f.key, run).await.unwrap()).unwrap(),
                    serde_json::to_value(expected_result).unwrap()
                );
                assert_eq!(
                    serde_json::to_value(
                        indexed
                            .run_tool_calls(&f.key, ToolCallsRequest::new(id))
                            .await
                            .unwrap()
                    )
                    .unwrap(),
                    serde_json::to_value(expected_calls).unwrap()
                );
                assert_eq!(
                    f.source.wal.range_reads_for_test(),
                    before,
                    "run-row reads must not issue even one sequence-zero range"
                );
            }
            f.source.reads.lock().unwrap().clear();
            let read = f
                .source
                .read(&f.key, &RunId("target".into()))
                .unwrap()
                .unwrap();
            assert_eq!(read.state.state.as_str(), reason_as_state(reason));
            let reads = f.source.reads.lock().unwrap();
            assert!(
                reads
                    .iter()
                    .filter(|(off, _)| *off == 0)
                    .all(|(_, n)| *n == broca_wal::framing::LINEAGE_FRAME_LEN as u64),
                "only the fixed lineage header may be read at byte zero"
            );
        }
    }

    fn reason_as_state(reason: RunFinishReason) -> String {
        serde_json::to_value(reason)
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned()
    }

    #[tokio::test]
    async fn run_rows_tool_calls_pages_keep_global_ordinals_without_full_ranges() {
        let mut f = Fixture::new("tool-pages");
        f.history();
        f.start("target");
        f.tool_turn();
        f.finish(RunFinishReason::MaxSteps);
        f.index();
        let expected = f
            .registry(false)
            .run_tool_calls(&f.key, ToolCallsRequest::new("target"))
            .await
            .unwrap();
        let registry = f.registry(true);
        let before = f.source.wal.range_reads_for_test();
        let first = registry
            .run_tool_calls(&f.key, ToolCallsRequest::new("target").with_limit(1))
            .await
            .unwrap();
        assert_eq!(first.calls.len(), 1);
        let second = registry
            .run_tool_calls(
                &f.key,
                ToolCallsRequest::new("target")
                    .with_limit(1)
                    .with_after(first.next.unwrap()),
            )
            .await
            .unwrap();
        assert_eq!(second.calls.len(), 1);
        assert!(second.next.is_none());
        let calls = [first.calls, second.calls].concat();
        assert_eq!(
            serde_json::to_value(calls).unwrap(),
            serde_json::to_value(expected.calls).unwrap()
        );
        assert_eq!(f.source.wal.range_reads_for_test(), before);
    }

    #[tokio::test]
    async fn run_status_reconciles_from_terminal() {
        let mut f = Fixture::new("terminal-cut");
        f.history();
        f.start("target");
        f.tool_turn();
        f.index();
        assert_eq!(
            f.registry(true).overhaul_active_runs(&[]).unwrap().unwrap(),
            vec![(RunId("target".into()), f.key.clone())]
        );
        // The log's terminal is durable, but its run-row transaction never happened.
        f.finish(RunFinishReason::Interrupted);
        let before_bytes = std::fs::read(f.source.wal.wal_path(&f.key)).unwrap();
        let reopened = Arc::new(OverhaulRunSource::new(
            Arc::new(OverhaulDb::open(&f.root).unwrap()),
            f.source.wal.clone(),
        ));
        let registry = crate::LineageRegistry::new_with_run_rows(
            Arc::new(NeverOpen),
            Some(f.source.wal.clone()),
            None,
            Some(reopened.clone()),
        );
        let before = f.source.wal.range_reads_for_test();
        assert_eq!(
            registry
                .overhaul_active_runs(&[(RunId("target".into()), f.key.clone())])
                .unwrap()
                .unwrap(),
            Vec::new(),
            "a stale active row is repaired, not marked for another terminal"
        );
        assert_eq!(
            serde_json::to_value(
                registry
                    .run_status(&f.key, RunId("target".into()))
                    .await
                    .unwrap()
            )
            .unwrap()["state"],
            "interrupted"
        );
        let state: String = reopened
            .db
            .read_snapshot(|tx| {
                Ok(
                    tx.query_row("SELECT state FROM run WHERE run_id='target'", [], |r| {
                        r.get(0)
                    })?,
                )
            })
            .unwrap();
        assert_eq!(state, "interrupted");
        assert_eq!(
            std::fs::read(f.source.wal.wal_path(&f.key)).unwrap(),
            before_bytes
        );
        assert_eq!(f.source.wal.range_reads_for_test(), before);
    }

    #[tokio::test]
    async fn run_rows_missing_or_mismatched_fall_back_and_legacy_never_consults_them() {
        let mut f = Fixture::new("fallback");
        f.history();
        f.start("target");
        f.step(
            1,
            vec![ContentBlock::Text {
                text: "answer".into(),
            }],
        );
        f.finish(RunFinishReason::Completed);
        f.index();
        let legacy = f.registry(false);
        let expected = legacy
            .run_result(&f.key, RunId("target".into()))
            .await
            .unwrap();
        assert!(f.source.reads.lock().unwrap().is_empty());
        assert!(legacy.overhaul_active_runs(&[]).is_none());
        f.source
            .db
            .authoritative_transaction(|tx| {
                tx.execute(
                    "UPDATE projection_boundary SET frame_identity=zeroblob(32)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        let before = f.source.wal.range_reads_for_test();
        let actual = f
            .registry(true)
            .run_result(&f.key, RunId("target".into()))
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(actual).unwrap(),
            serde_json::to_value(&expected).unwrap()
        );
        assert!(f.source.wal.range_reads_for_test() > before);
        f.source
            .db
            .authoritative_transaction(|tx| {
                tx.execute("DELETE FROM run WHERE run_id='target'", [])?;
                Ok(())
            })
            .unwrap();
        let unpublished = vec![(RunId("target".into()), f.key.clone())];
        assert_eq!(
            f.registry(true)
                .overhaul_active_runs(&unpublished)
                .unwrap()
                .unwrap(),
            unpublished
        );
        let actual = f
            .registry(true)
            .run_result(&f.key, RunId("target".into()))
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(actual).unwrap(),
            serde_json::to_value(expected).unwrap()
        );
        assert_eq!(
            serde_json::to_value(
                f.registry(true)
                    .run_status(&f.key, RunId("missing".into()))
                    .await
                    .unwrap()
            )
            .unwrap(),
            json!({"state":"unknown"})
        );
    }

    #[tokio::test]
    async fn run_rows_after_zero_step_ordinary_episode_visibly_fall_back() {
        let mut f = Fixture::new("zero-step-fallback");
        f.start("zero-step");
        f.finish(RunFinishReason::Interrupted);
        f.start("target");
        f.step(
            1,
            vec![ContentBlock::Text {
                text: "answer".into(),
            }],
        );
        f.finish(RunFinishReason::Completed);
        f.index();
        assert!(f
            .source
            .read(&f.key, &RunId("target".into()))
            .unwrap()
            .is_none());
        assert!(
            f.source.reads.lock().unwrap().is_empty(),
            "there is no qualified preceding cut to read"
        );
        let before = f.source.wal.range_reads_for_test();
        let result = f
            .registry(true)
            .run_result(&f.key, RunId("target".into()))
            .await
            .unwrap();
        assert_eq!(result.final_message.unwrap().text, "answer");
        assert!(
            f.source.wal.range_reads_for_test() > before,
            "missing historical seed must visibly take the full-replay range path"
        );
    }
}
