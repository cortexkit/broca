//! Run-scoped reads from the content-free offset index on opted-in state roots.

use std::cell::RefCell;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use broca_core::replay::{PositionedFrame, SeedBoundary};
use broca_core::{FileWalStore, ReconciledRunState, ReplayState};
use broca_protocol::{BindIdentity, RunId};
use broca_store::db::{DbError, OverhaulDb};
use broca_wal::access_counter::{self, decode_next, ValidationRead};
use broca_wal::framing::DecodeStep;
use broca_wal::{FramedRecord, WalLineageId, WalRecord};
use rusqlite::{params, OptionalExtension};
use sha2::{Digest, Sha256};

/// Owned by the registry; legacy registries never construct or consult this source.
pub struct OverhaulRunSource {
    db: Arc<OverhaulDb>,
    wal: Arc<FileWalStore>,
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
        Self { db, wal }
    }

    pub(super) fn lookup_run(
        &self,
        run_id: &str,
    ) -> Result<Option<broca_core::RunIndexRow>, String> {
        self.db.lookup_run(run_id).map_err(|e| e.to_string())
    }

    pub(super) fn restart_resume_candidates(&self) -> Result<Vec<(String, BindIdentity)>, String> {
        self.db
            .restart_resume_candidates()
            .map_err(|e| e.to_string())
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
        let fallback = |reason| {
            access_counter::seed_fallback(reason);
            Ok(None)
        };
        let Some(row) = self.row(key, run_id).map_err(|e| e.to_string())? else {
            return fallback("run row missing");
        };
        // Without a safe cut, only the first episode can replay at the actual log origin.
        // Later episodes fall back rather than inventing a seed from a bare run offset.
        if row.boundary.is_none() && !row.first_episode {
            return fallback("run row has no historical boundary");
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
            let mut file = file.borrow_mut();
            file.seek(SeekFrom::Start(off))?;
            let mut bytes = Vec::new();
            access_counter::read_to_end(
                file.by_ref().take(
                    count
                        .min(len.saturating_sub(off))
                        .min(self.wal.max_read_bytes()),
                ),
                off,
                &mut bytes,
            )?;
            Ok(bytes)
        };
        let header =
            read_at(0, broca_wal::framing::LINEAGE_FRAME_LEN as u64).map_err(|e| e.to_string())?;
        let lineage_id = match decode_next(&header) {
            DecodeStep::Lineage { lineage_id, .. } => Some(lineage_id),
            _ => None,
        };
        if lineage_id.map(|id| id.to_string()) != row.lineage_id {
            return fallback("run row lineage mismatch");
        }
        // The authenticated lineage header is metadata, not part of the first run's range.
        let off = row.boundary.as_ref().map_or_else(
            || lineage_id.map_or(0, |_| broca_wal::framing::LINEAGE_FRAME_LEN as u64),
            |b| b.frame_end,
        );
        let end = row.next_off.unwrap_or(len).min(len);
        if off > row.start_off || row.start_off >= end {
            return fallback("run row range outside log");
        }
        if let Some(next) = row.next_off {
            let mut count = 1024u64;
            loop {
                let bytes = access_counter::read_validation_at(
                    ValidationRead::NextRunDelimiter,
                    &read_at,
                    next,
                    count,
                )
                .map_err(|e| e.to_string())?;
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
                    _ => return fallback("run row next delimiter mismatch"),
                }
            }
        }
        let bytes = read_at(off, end - off).map_err(|e| e.to_string())?;
        if bytes.len() as u64 != end - off {
            return fallback("run row range truncated");
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
            return fallback("run row tail contains compaction");
        }
        let replayed = if let Some(boundary) = &row.boundary {
            let Some(count) = boundary.frame_end.checked_sub(boundary.frame_off) else {
                return fallback("run row boundary range reversed");
            };
            let frame = access_counter::read_validation_at(
                ValidationRead::BoundaryFrame,
                &read_at,
                boundary.frame_off,
                count,
            )
            .map_err(|e| e.to_string())?;
            let kind_matches = match (row.boundary_kind.as_deref(), decode_next(&frame)) {
                (Some("turn_finished"), DecodeStep::Record { framed, .. }) => {
                    matches!(framed.record, WalRecord::TurnFinished { .. })
                }
                (Some("no_tool_step"), DecodeStep::Record { framed, .. }) => {
                    matches!(&framed.record,WalRecord::ModelStepFinished { assistant_message, .. } if assistant_message.tool_calls().next().is_none())
                }
                (Some("zero_step_seed"), DecodeStep::Record { framed, .. }) => {
                    matches!(framed.record, WalRecord::RunStarted { .. })
                }
                _ => false,
            };
            if !kind_matches {
                return fallback("run row boundary kind mismatch");
            }
            // No-tool and seed cuts are qualified by a later terminal. Validate that
            // frame too, so a restore before the qualifier cannot reuse the cut.
            if let Some((seq, lo, hi, hash)) = &row.qualifier {
                let Some(count) = hi.checked_sub(*lo) else {
                    return fallback("run row qualifier range reversed");
                };
                let bytes = access_counter::read_validation_at(
                    ValidationRead::QualifierTerminal,
                    &read_at,
                    *lo,
                    count,
                )
                .map_err(|e| e.to_string())?;
                if Sha256::digest(&bytes).as_slice() != hash
                    || !matches!(decode_next(&bytes),
                    DecodeStep::Record { framed, consumed } if framed.seq == *seq
                        && consumed == bytes.len() && matches!(framed.record, WalRecord::RunFinished { .. }))
                {
                    return fallback("run row qualifier mismatch");
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
            // The first episode has no persisted seed; replay all of its records.
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
        let mut candidates: Vec<_> = self
            .db
            .active_runs()
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|(id, key)| (RunId(id), key))
            .collect();
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
    use broca_store::db::LogLocation;
    use broca_wal::Wal;
    use cortexkit_role_llm_runner::tool_calls::{ToolCallsPage, ToolCallsRequest};
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
        publish_on_append: bool,
        media: bool,
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
            let wal = Self::open_writer(&store, &key, &root);
            let source = Arc::new(OverhaulRunSource::new(
                Arc::new(OverhaulDb::open(&root).unwrap()),
                store,
            ));
            Self {
                root,
                key,
                wal,
                source,
                publish_on_append: true,
                media: false,
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
        fn open_writer(
            store: &FileWalStore,
            key: &BindIdentity,
            root: &std::path::Path,
        ) -> Box<dyn Wal + Send> {
            // Initialize the store's path layout, then keep the concrete FileWal writer open
            // so publication can use the durable byte end recorded by its successful sync.
            drop(store.open(key).unwrap());
            Box::new(
                broca_wal::FileWal::open(store.wal_path(key))
                    .unwrap()
                    .with_feature_floor(root.join("wal-floor"))
                    .with_wal_config(broca_wal::WalConfig::OVERHAUL)
                    .unwrap(),
            )
        }
        fn append(&mut self, record: WalRecord) {
            self.wal.append(1, &record, true).unwrap();
            if self.publish_on_append {
                self.index();
            }
        }
        fn start(&mut self, id: &str) {
            let mut content = vec![ContentBlock::Text {
                text: format!("input-{id}"),
            }];
            if self.media {
                content.push(ContentBlock::Media(
                    broca_core::media_validation::validate_media(
                        b"%PDF-1.7\nrun-row fixture\n%%EOF",
                        "application/pdf",
                    )
                    .unwrap(),
                ));
            }
            self.append(
                serde_json::from_value(json!({
                    "type":"run_started", "run_id":id, "session":self.key,
                    "config":{"model":{"provider_module_id":"provider","model_id":"model"},
                        "tools":[], "tool_choice":{"type":"auto"}, "generation":{},
                        "response_format":{"type":"text"}, "context_epoch":0,"renderer_version":1},
                    "input":[Message::new(Role::User,content)],
                    "origin":{"kind":"fresh"}
                }))
                .unwrap(),
            );
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
            self.source
                .db
                .publish(
                    &self.key,
                    "test-ref",
                    LogLocation::Live,
                    &self
                        .wal
                        .durable_publication()
                        .expect("a synced append mints a publication"),
                )
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

    struct ReadPlan {
        ranges: Vec<(&'static str, access_counter::WalReadAccess)>,
        decodes: u64,
    }

    impl Fixture {
        fn read_plan(&self, id: &str, preceding: Option<(&str, &str)>) -> ReadPlan {
            use access_counter::{ReadClass, WalReadAccess};
            let bytes = std::fs::read(self.source.wal.wal_path(&self.key)).unwrap();
            let mut frames = Vec::new();
            let mut metadata = Vec::new();
            let mut off = broca_wal::framing::LINEAGE_FRAME_LEN as u64;
            while off < bytes.len() as u64 {
                match broca_wal::framing::decode_next(&bytes[off as usize..]) {
                    DecodeStep::Record { framed, consumed } => {
                        frames.push((off, off + consumed as u64, framed.record));
                        off += consumed as u64;
                    }
                    DecodeStep::Lineage { consumed, .. } => {
                        metadata.push((off, off + consumed as u64));
                        off += consumed as u64;
                    }
                    _ => panic!("fixture must contain complete records at offset {off}"),
                }
            }
            let start = |id: &str| {
                frames.iter().position(|(_, _, record)| {
                matches!(record, WalRecord::RunStarted { run_id, .. } if run_id.0 == id)
            }).unwrap()
            };
            let target = start(id);
            let next = frames
                .iter()
                .enumerate()
                .skip(target + 1)
                .find(|(_, (_, _, record))| matches!(record, WalRecord::RunStarted { .. }))
                .map(|(n, _)| n);
            let mut ranges = Vec::new();
            let mut add = |name, offset, length, class| {
                ranges.push((
                    name,
                    WalReadAccess {
                        offset,
                        length,
                        class,
                        writer_open: None,
                    },
                ))
            };
            add("lineage header", 0, 69, ReadClass::LineageHeader);
            let end = next.map_or(bytes.len() as u64, |n| frames[n].0);
            if let Some(n) = next {
                add(
                    "next-run delimiter probe",
                    frames[n].0,
                    1024.min(bytes.len() as u64 - frames[n].0),
                    ReadClass::Validation(ValidationRead::NextRunDelimiter),
                );
            }
            let mut lo = 69;
            let mut validations = 0;
            let mut cut = None;
            if let Some((previous, kind)) = preceding {
                let previous = start(previous);
                let n = (previous..target)
                    .rev()
                    .find(|&n| match (&frames[n].2, kind) {
                        (WalRecord::RunStarted { .. }, "zero_step_seed") => true,
                        (
                            WalRecord::ModelStepFinished {
                                assistant_message, ..
                            },
                            "no_tool_step",
                        ) => assistant_message.tool_calls().next().is_none(),
                        (WalRecord::TurnFinished { .. }, "turn_finished") => true,
                        _ => false,
                    })
                    .unwrap();
                lo = frames[n].1;
                cut = Some((n, previous, kind));
            }
            add(
                "target run and contiguous boundary tail",
                lo,
                end - lo,
                ReadClass::Source,
            );
            if let Some((n, previous, kind)) = cut {
                add(
                    "boundary kind check",
                    frames[n].0,
                    frames[n].1 - frames[n].0,
                    ReadClass::Validation(ValidationRead::BoundaryFrame),
                );
                if kind != "turn_finished" {
                    let terminal = (previous..target)
                        .rev()
                        .find(|&n| matches!(frames[n].2, WalRecord::RunFinished { .. }))
                        .unwrap();
                    add(
                        "qualifier terminal",
                        frames[terminal].0,
                        frames[terminal].1 - frames[terminal].0,
                        ReadClass::Validation(ValidationRead::QualifierTerminal),
                    );
                    validations += 1;
                }
                let first = frames.iter().find(|(off, _, _)| *off >= lo).unwrap().0;
                if first != lo {
                    add(
                        "boundary lineage gap",
                        lo,
                        first - lo,
                        ReadClass::Validation(ValidationRead::LineageGap),
                    );
                    validations += metadata
                        .iter()
                        .filter(|(off, _)| *off >= lo && *off < first)
                        .count() as u64;
                }
                add(
                    "validated replay boundary authentication",
                    frames[n].0,
                    frames[n].1 - frames[n].0,
                    ReadClass::Validation(ValidationRead::BoundaryFrame),
                );
                add(
                    "validated replay lineage authentication",
                    0,
                    69,
                    ReadClass::LineageHeader,
                );
                validations += 3;
            }
            ReadPlan {
                ranges,
                decodes: frames
                    .iter()
                    .filter(|(off, _, _)| *off >= lo && *off < end)
                    .count() as u64
                    + metadata
                        .iter()
                        .filter(|(off, _)| *off >= lo && *off < end)
                        .count() as u64
                    + 1
                    + u64::from(next.is_some())
                    + validations,
            }
        }
    }

    impl ReadPlan {
        fn assert_operation(&self, measured: &access_counter::OperationAccess) {
            assert!(measured.seed_fallbacks.is_empty(), "{measured:?}");
            assert!(measured.writer_opens.is_empty(), "{measured:?}");
            assert_eq!(measured.frames_decoded, self.decodes, "{}", measured.name);
            assert_eq!(
                measured.reads.len(),
                self.ranges.len(),
                "{measured:?}; named plan: {:?}",
                self.ranges
            );
            for (actual, (name, expected)) in measured.reads.iter().zip(&self.ranges) {
                assert_eq!(actual, expected, "{}: {name}", measured.name);
                assert_ne!(actual.class, access_counter::ReadClass::Origin);
            }
        }
    }

    fn assert_tool_page(actual: &ToolCallsPage, expected: &ToolCallsPage) {
        assert_eq!(actual.run_id, expected.run_id);
        assert_eq!(
            serde_json::to_value(&actual.calls).unwrap(),
            serde_json::to_value(&expected.calls).unwrap()
        );
        match (&actual.next, &expected.next) {
            (None, None) => {}
            (Some(actual), Some(expected)) => {
                let logical = |cursor: &str| {
                    let mut value: serde_json::Value = serde_json::from_str(cursor).unwrap();
                    let object = value.as_object_mut().unwrap();
                    // Opaque cursors may include route-specific anchor_before values.
                    // Compare every logical position; each route consumes its own signed cursor.
                    object.remove("anchor_before");
                    assert!(!object
                        .remove("authentication")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .is_empty());
                    value
                };
                assert_eq!(logical(actual), logical(expected));
            }
            _ => panic!("both routes must agree whether a continuation exists"),
        }
    }

    fn cursor_fixture(name: &str) -> Fixture {
        let mut f = Fixture::new(name);
        f.history();
        f.start("target");
        f.tool_turn();
        f.finish(RunFinishReason::MaxSteps);
        f
    }

    async fn cursors(f: &Fixture) -> (String, String) {
        let legacy = f
            .registry(false)
            .run_tool_calls(&f.key, ToolCallsRequest::new("target").with_limit(1))
            .await
            .unwrap();
        let indexed = f
            .registry(true)
            .run_tool_calls(&f.key, ToolCallsRequest::new("target").with_limit(1))
            .await
            .unwrap();
        assert_tool_page(&indexed, &legacy);
        (legacy.next.unwrap(), indexed.next.unwrap())
    }

    async fn assert_cursor_refusals(f: &Fixture, tokens: (String, String)) {
        for (indexed, token) in [(false, tokens.0), (true, tokens.1)] {
            let error = f
                .registry(indexed)
                .run_tool_calls(&f.key, ToolCallsRequest::new("target").with_after(token))
                .await
                .unwrap_err();
            assert!(matches!(error, crate::serve::ServeError::InvalidParams { ref field, .. } if field == "after"),
                "both routes must refuse the cursor with invalid params for after, indexed={indexed}: {error:?}");
        }
    }

    #[tokio::test]
    async fn run_row_cursors_refuse_another_session_like_legacy() {
        let source = cursor_fixture("cursor-source");
        let target = cursor_fixture("cursor-other-session");
        assert_cursor_refusals(&target, cursors(&source).await).await;
    }

    #[tokio::test]
    async fn run_row_cursors_refuse_recreated_lineage_like_legacy() {
        let f = cursor_fixture("cursor-recreated");
        let tokens = cursors(&f).await;
        let replacement = cursor_fixture("cursor-new-lineage");
        let bytes = std::fs::read(replacement.source.wal.wal_path(&replacement.key)).unwrap();
        std::fs::write(f.source.wal.wal_path(&f.key), bytes).unwrap();
        assert_cursor_refusals(&f, tokens).await;
    }

    #[tokio::test]
    async fn run_row_cursors_refuse_missing_position_like_legacy() {
        let f = cursor_fixture("cursor-missing-position");
        let tokens = cursors(&f).await;
        let token: serde_json::Value = serde_json::from_str(&tokens.0).unwrap();
        let missing_seq = token["step_seq"].as_u64().unwrap();
        let path = f.source.wal.wal_path(&f.key);
        let bytes = std::fs::read(&path).unwrap();
        let mut off = 0;
        loop {
            match broca_wal::framing::decode_next(&bytes[off..]) {
                DecodeStep::Record { framed, consumed } => {
                    if framed.seq == missing_seq {
                        break;
                    }
                    off += consumed;
                }
                DecodeStep::Lineage { consumed, .. } => off += consumed,
                _ => panic!("cursor must name a complete fixture record"),
            }
        }
        std::fs::write(path, &bytes[..off]).unwrap();
        assert_cursor_refusals(&f, tokens).await;
    }

    #[tokio::test]
    async fn published_run_rows_have_exact_read_plans_and_legacy_answers() {
        for media in [false, true] {
            let mut f = Fixture::new(&format!("publication-plans-{media}"));
            f.media = media;
            f.start("zero-first");
            f.finish(RunFinishReason::Interrupted);
            f.start("answer");
            f.step(
                1,
                vec![ContentBlock::Text {
                    text: "answer".into(),
                }],
            );
            f.finish(RunFinishReason::Completed);
            f.start("tools");
            f.tool_turn();
            f.finish(RunFinishReason::MaxSteps);
            // Reopen the database between runs; subsequent appends still publish automatically.
            f.source = Arc::new(OverhaulRunSource::new(
                Arc::new(OverhaulDb::open(&f.root).unwrap()),
                f.source.wal.clone(),
            ));
            f.start("zero-later");
            f.finish(RunFinishReason::Cancelled);
            f.start("last");
            f.step(
                1,
                vec![ContentBlock::Text {
                    text: "last answer".into(),
                }],
            );
            f.finish(RunFinishReason::Completed);
            let legacy = f.registry(false);
            let indexed = f.registry(true);
            for (id, preceding) in [
                ("zero-first", None),
                ("answer", Some(("zero-first", "zero_step_seed"))),
                ("tools", Some(("answer", "no_tool_step"))),
                ("zero-later", Some(("tools", "turn_finished"))),
                ("last", Some(("zero-later", "zero_step_seed"))),
            ] {
                let plan = f.read_plan(id, preceding);
                eprintln!(
                    "run={id}, media={media}: {} reads, {} decodes; named ranges: {:?}",
                    plan.ranges.len(),
                    plan.decodes,
                    plan.ranges
                );
                let run = RunId(id.into());
                let expected_status = legacy.run_status(&f.key, run.clone()).await.unwrap();
                let expected_result = legacy.run_result(&f.key, run.clone()).await.unwrap();
                let counter = access_counter::WalAccessCounter::default();
                let before = f.source.wal.range_reads_for_test();
                let status = counter
                    .operation(format!("{id} status"))
                    .run_future(indexed.run_status(&f.key, run.clone()))
                    .await
                    .unwrap();
                let result = counter
                    .operation(format!("{id} result"))
                    .run_future(indexed.run_result(&f.key, run))
                    .await
                    .unwrap();
                assert_eq!(
                    serde_json::to_value(status).unwrap(),
                    serde_json::to_value(expected_status).unwrap()
                );
                assert_eq!(
                    serde_json::to_value(result).unwrap(),
                    serde_json::to_value(expected_result).unwrap()
                );
                assert_eq!(f.source.wal.range_reads_for_test(), before);
                let mut after = None;
                let mut expected_after = None;
                loop {
                    let request = ToolCallsRequest::new(id).with_limit(1);
                    let request = if let Some(cursor) = after {
                        request.with_after(cursor)
                    } else {
                        request
                    };
                    let expected_request = ToolCallsRequest::new(id).with_limit(1);
                    let expected_request = if let Some(cursor) = expected_after {
                        expected_request.with_after(cursor)
                    } else {
                        expected_request
                    };
                    let expected = legacy
                        .run_tool_calls(&f.key, expected_request)
                        .await
                        .unwrap();
                    let before = f.source.wal.range_reads_for_test();
                    let actual = counter
                        .operation(format!("{id} tool-call page"))
                        .run_future(indexed.run_tool_calls(&f.key, request))
                        .await
                        .unwrap();
                    assert_tool_page(&actual, &expected);
                    assert_eq!(f.source.wal.range_reads_for_test(), before);
                    after = actual.next;
                    expected_after = expected.next;
                    if after.is_none() {
                        break;
                    }
                    assert_eq!(id, "tools");
                    assert_eq!(actual.calls.len(), 1);
                }
                for measured in counter.snapshot() {
                    plan.assert_operation(&measured);
                }
            }
        }
    }

    #[test]
    fn published_run_discovery_is_content_free_and_keeps_paused_candidates() {
        let mut f = Fixture::new("discovery");
        f.history();
        f.start("paused");
        f.append(WalRecord::RunPaused {
            run_id: RunId("paused".into()),
            ts_ms: None,
            reason: broca_protocol::PauseReason::Restart,
            error: None,
        });
        let other = BindIdentity::new(f.root.join("other"), "runner", "active-session");
        f.source.db.authoritative_transaction(|tx| {
            tx.execute("UPDATE run SET submission_id='submission',request_identity_digest='digest' WHERE run_id='paused'", [])?;
            Ok(())
        }).unwrap();
        let registry = f.registry(true);
        let counter = access_counter::WalAccessCounter::default();
        counter.operation("SQL discovery").run(|| {
            let row = registry
                .overhaul_lookup_run("paused")
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(row.session, f.key);
            assert_eq!(row.state, broca_core::RunIndexState::Paused);
            assert_eq!(row.submission_id.as_deref(), Some("submission"));
            assert_eq!(row.request_identity_digest.as_deref(), Some("digest"));
            assert!(
                row.title.is_none()
                    && row.usage.is_none()
                    && row.final_step_finish_reason.is_none()
            );
            assert_eq!(row.retries_used, 0);
            assert!(registry
                .overhaul_lookup_run("missing")
                .unwrap()
                .unwrap()
                .is_none());
            assert_eq!(
                registry
                    .overhaul_restart_resume_candidates()
                    .unwrap()
                    .unwrap(),
                vec![("paused".into(), f.key.clone())]
            );
            assert!(f.source.db.active_runs().unwrap().is_empty());
            assert!(registry
                .overhaul_lookup_run("history")
                .unwrap()
                .unwrap()
                .unwrap()
                .state
                .is_terminal());
        });
        assert!(counter.snapshot()[0].reads.is_empty());
        assert!(counter.snapshot()[0].seed_fallbacks.is_empty());
        assert!(f.registry(false).overhaul_lookup_run("paused").is_none());
        assert!(f
            .registry(false)
            .overhaul_restart_resume_candidates()
            .is_none());
        // A newly published active run joins both discovery sets, but terminal history does not.
        f.key = other;
        f.wal = Fixture::open_writer(&f.source.wal, &f.key, &f.root);
        f.start("active");
        assert_eq!(
            f.source.db.active_runs().unwrap(),
            vec![("active".into(), f.key.clone())]
        );
        assert_eq!(
            registry
                .overhaul_restart_resume_candidates()
                .unwrap()
                .unwrap()
                .len(),
            2
        );
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
                    "indexed run {id} must have a usable read"
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
            let counter = access_counter::WalAccessCounter::default();
            let read = counter.operation("target run").run(|| {
                f.source
                    .read(&f.key, &RunId("target".into()))
                    .unwrap()
                    .unwrap()
            });
            assert_eq!(read.state.state.as_str(), reason_as_state(reason));
            let measured = counter.snapshot();
            let reads = &measured[0].reads;
            assert!(
                reads
                    .iter()
                    .filter(|read| read.offset == 0)
                    .all(|read| read.class == access_counter::ReadClass::LineageHeader),
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
        let expected_first = f
            .registry(false)
            .run_tool_calls(&f.key, ToolCallsRequest::new("target").with_limit(1))
            .await
            .unwrap();
        let expected_second = f
            .registry(false)
            .run_tool_calls(
                &f.key,
                ToolCallsRequest::new("target")
                    .with_limit(1)
                    .with_after(expected_first.next.clone().unwrap()),
            )
            .await
            .unwrap();
        let before = f.source.wal.range_reads_for_test();
        let first = registry
            .run_tool_calls(&f.key, ToolCallsRequest::new("target").with_limit(1))
            .await
            .unwrap();
        assert_eq!(first.calls.len(), 1);
        assert!(
            first.next.is_some(),
            "the fixture must exercise a nonempty continuation"
        );
        assert_tool_page(&first, &expected_first);
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
        assert_tool_page(&second, &expected_second);
        assert!(second.next.is_none());
        let calls = [first.calls, second.calls].concat();
        assert_eq!(
            serde_json::to_value(calls).unwrap(),
            serde_json::to_value(expected.calls).unwrap()
        );
        assert_eq!(f.source.wal.range_reads_for_test(), before);
        let counter = access_counter::WalAccessCounter::default();
        let resumed = counter
            .operation("legacy optimization cursor on indexed route")
            .run_future(
                registry.run_tool_calls(
                    &f.key,
                    ToolCallsRequest::new("target")
                        .with_limit(1)
                        .with_after(expected_first.next.unwrap()),
                ),
            )
            .await
            .unwrap();
        assert_tool_page(&resumed, &expected_second);
        f.read_plan("target", Some(("history", "no_tool_step")))
            .assert_operation(&counter.snapshot()[0]);
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
        f.publish_on_append = false;
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
            reopened.db.lookup_run("target").unwrap().unwrap().state,
            broca_core::RunIndexState::Active
        );
        let counter = access_counter::WalAccessCounter::default();
        let plan = f.read_plan("target", Some(("history", "no_tool_step")));
        assert_eq!(
            counter
                .operation("terminal gap candidate reconciliation")
                .run(|| registry.overhaul_active_runs(&[(RunId("target".into()), f.key.clone())]))
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
        plan.assert_operation(&counter.snapshot()[0]);
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
        let counter = access_counter::WalAccessCounter::default();
        let expected = counter
            .operation("legacy result")
            .run_future(legacy.run_result(&f.key, RunId("target".into())))
            .await
            .unwrap();
        assert!(counter.snapshot()[0]
            .reads
            .iter()
            .all(|read| !matches!(read.class, access_counter::ReadClass::Validation(_))));
        assert!(counter.snapshot()[0]
            .reads
            .iter()
            .any(|read| read.class == access_counter::ReadClass::Origin));
        assert!(counter.snapshot()[0].frames_decoded > 0);
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
        let indexed = f.registry(true);
        let actual = counter
            .operation("forced full replay")
            .run_future(indexed.run_result(&f.key, RunId("target".into())))
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(actual).unwrap(),
            serde_json::to_value(&expected).unwrap()
        );
        assert!(f.source.wal.range_reads_for_test() > before);
        let measured = counter.snapshot();
        let forced = &measured[1];
        assert_eq!(forced.seed_fallbacks, ["boundary frame identity mismatch"]);
        assert!(forced
            .reads
            .iter()
            .any(|read| read.class == access_counter::ReadClass::Origin
                && read.offset == 0
                && read.length
                    == std::fs::metadata(f.source.wal.wal_path(&f.key))
                        .unwrap()
                        .len()));
        // Full replay decodes every record and lineage frame, in addition to the
        // failed seed's validation work. An origin read alone cannot prove this.
        assert!(forced.frames_decoded >= f.wal.last_seq() * 2);
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
    async fn run_rows_without_historical_boundary_visibly_fall_back() {
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
        // An older index has no ordinary zero-step cut. It must still use full replay.
        f.source
            .db
            .authoritative_transaction(|tx| {
                tx.execute(
                    "DELETE FROM projection_boundary WHERE kind='zero_step_seed'",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        let counter = access_counter::WalAccessCounter::default();
        assert!(counter.operation("missing historical boundary").run(|| f
            .source
            .read(&f.key, &RunId("target".into()))
            .unwrap()
            .is_none()));
        assert!(
            counter.snapshot()[0].reads.is_empty(),
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

    #[test]
    fn ordinary_zero_step_boundary_requires_terminal_and_preserves_legacy_cuts() {
        let mut f = Fixture::new("zero-step-qualification");
        f.start("zero-step");
        f.index();
        let db = f.source.db.clone();
        let qualified = || {
            db.read_snapshot(|tx| {
                Ok(tx.query_row(
                    "SELECT qualified FROM projection_boundary WHERE kind='zero_step_seed'",
                    [],
                    |r| r.get::<_, bool>(0),
                )?)
            })
            .unwrap()
        };
        assert!(!qualified(), "a start alone is not a committed cut");
        f.append(WalRecord::RunPaused {
            run_id: RunId("zero-step".into()),
            ts_ms: None,
            reason: broca_protocol::PauseReason::AuthRequired,
            error: None,
        });
        f.index();
        assert!(!qualified(), "a pause cannot qualify a historical cut");
        f.finish(RunFinishReason::Interrupted);
        f.index();
        assert!(qualified());
        let frames = f.source.wal.read_records(&f.key).unwrap();
        assert_eq!(
            broca_core::replay::latest_projection_boundary(&frames, 0),
            None,
            "legacy materialization must not acquire an ordinary zero-step cut"
        );

        f.start("stepped");
        f.step(
            1,
            vec![ContentBlock::Text {
                text: "answer".into(),
            }],
        );
        f.finish(RunFinishReason::Completed);
        f.index();
        f.source.db.read_snapshot(|tx| {
            let qualified: bool = tx.query_row(
                "SELECT qualified FROM projection_boundary WHERE kind='zero_step_seed' AND run_id='stepped'",
                [], |r| r.get(0),
            )?;
            assert!(!qualified, "a step disqualifies the run-start cut");
            Ok(())
        }).unwrap();
    }

    #[tokio::test]
    async fn run_rows_after_zero_step_ordinary_episodes_equal_full_replay() {
        let mut f = Fixture::new("zero-step-historical");
        let runs = [
            ("zero-first", false, RunFinishReason::Interrupted),
            ("tool-answer", true, RunFinishReason::Completed),
            ("zero-interrupted", false, RunFinishReason::Interrupted),
            ("answer", true, RunFinishReason::Completed),
            ("zero-cancelled", false, RunFinishReason::Cancelled),
            ("tool-limit", true, RunFinishReason::MaxSteps),
            ("zero-last", false, RunFinishReason::Interrupted),
            ("last-answer", true, RunFinishReason::Completed),
        ];
        for (id, stepped, reason) in runs {
            f.start(id);
            f.index();
            if stepped {
                if id.starts_with("tool-") {
                    f.tool_turn();
                }
                f.step(
                    2,
                    vec![ContentBlock::Text {
                        text: format!("answer-{id}"),
                    }],
                );
            }
            f.finish(reason);
            // Exercise incremental publication, including qualification of a cut whose
            // candidate was saved by an earlier index transaction.
            f.index();
        }
        let legacy = f.registry(false);
        let indexed = f.registry(true);
        let mut origin_runs = 0;
        for (id, _, _) in runs {
            let run = RunId(id.into());
            let expected_status = legacy.run_status(&f.key, run.clone()).await.unwrap();
            let expected_result = legacy.run_result(&f.key, run.clone()).await.unwrap();
            let expected_calls = legacy
                .run_tool_calls(&f.key, ToolCallsRequest::new(id))
                .await
                .unwrap();
            let counter = access_counter::WalAccessCounter::default();
            assert!(
                counter
                    .operation("historical run")
                    .run(|| f.source.read(&f.key, &run))
                    .unwrap()
                    .is_some(),
                "run {id} needs a usable historical boundary"
            );
            if counter.snapshot()[0]
                .reads
                .iter()
                .any(|read| read.class == access_counter::ReadClass::Origin)
            {
                origin_runs += 1;
            }
            let before = f.source.wal.range_reads_for_test();
            assert_eq!(
                serde_json::to_value(indexed.run_status(&f.key, run.clone()).await.unwrap())
                    .unwrap(),
                serde_json::to_value(expected_status).unwrap(),
                "status for {id}"
            );
            assert_eq!(
                serde_json::to_value(indexed.run_result(&f.key, run).await.unwrap()).unwrap(),
                serde_json::to_value(expected_result).unwrap(),
                "result for {id}"
            );
            assert_eq!(
                serde_json::to_value(
                    indexed
                        .run_tool_calls(&f.key, ToolCallsRequest::new(id))
                        .await
                        .unwrap()
                )
                .unwrap(),
                serde_json::to_value(expected_calls).unwrap(),
                "tool calls for {id}"
            );
            assert_eq!(
                f.source.wal.range_reads_for_test(),
                before,
                "indexed run {id} must not fall back to a sequence-zero range"
            );
        }
        eprintln!(
            "historical run reads: {origin_runs}/{} origin replays; 0 full-range fallbacks",
            runs.len()
        );
        assert_eq!(
            origin_runs, 0,
            "the lineage header is read separately from the first episode"
        );
    }
}
