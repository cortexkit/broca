use broca_core::projection_plan::{u64_to_i64, OffsetEpisode, OffsetSeed};
use broca_protocol::BindIdentity;
use broca_wal::framing::{decode_next, DecodeStep};
use rusqlite::{params, OptionalExtension, Transaction};
use sha2::{Digest, Sha256};

use super::{schema, DbError, OverhaulDb};

#[derive(Debug, Clone, Copy)]
pub enum LogLocation {
    Live,
    Archived,
}
impl LogLocation {
    fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Archived => "archived",
        }
    }
}

/// `bytes` must end at the writer's durable publication boundary, not at a live
/// file's speculative EOF. The index never writes, repairs or truncates the log.
///
/// Within one lineage, committed bytes are immutable. Normal transitions are
/// appends or truncation of an unpublished torn tail; the checkpoint authenticates
/// only the last indexed frame, not the entire preceding prefix. Any path that
/// rewrites earlier bytes under the same lineage (including restore or archive
/// reasoning pruning) must call [`OverhaulDb::forget_session`] before indexing
/// again, even when the final indexed frame and its offsets are unchanged.
pub struct PublishedSession<'a> {
    pub bind: &'a BindIdentity,
    pub session_ref: &'a str,
    pub location: LogLocation,
    pub bytes: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexOutcome {
    pub rebuilt: bool,
    pub indexed_seq: Option<u64>,
    pub indexed_end: u64,
    pub head_ord: u64,
}

struct Checkpoint {
    seq: Option<u64>,
    off: u64,
    end: u64,
    identity: Option<Vec<u8>>,
    lineage: Option<String>,
    lineage_off: Option<u64>,
    seed: Option<OffsetSeed>,
}

pub(super) fn sid(bind: &BindIdentity) -> Result<String, DbError> {
    bind.validate_keyable().map_err(DbError::Invalid)?;
    Ok(format!(
        "{}\u{1f}{}\u{1f}{}",
        bind.project_root.display(),
        bind.harness,
        bind.session
    ))
}

impl OverhaulDb {
    /// Discard only this session's rebuildable rows before a same-lineage rewrite.
    /// Authoritative state and other sessions are never cleared by this operation.
    pub fn forget_session(&self, bind: &BindIdentity) -> Result<(), DbError> {
        let key = sid(bind)?;
        self.transaction(false, |tx| clear_session(tx, &key))
    }

    /// Self-check the last indexed frame before using rows. A shorter log,
    /// different lineage or changed checkpoint rebuilds only this session's
    /// derived rows, in the same transaction as replay.
    /// Earlier committed bytes must remain immutable within a lineage; callers
    /// that rewrite them must first use [`Self::forget_session`]. The last-frame
    /// self-check is a bounded guard, not authentication of the whole prefix.
    pub fn index(&self, log: &PublishedSession<'_>) -> Result<IndexOutcome, DbError> {
        self.transaction(false, |tx| index_in_tx(tx, log, false))
    }
}

pub(super) fn clear_session(tx: &Transaction<'_>, key: &str) -> Result<(), DbError> {
    for table in schema::SESSION_TABLES {
        tx.execute(&format!("DELETE FROM {table} WHERE sid=?1"), [key])?;
    }
    tx.execute("DELETE FROM session WHERE sid=?1", [key])?;
    Ok(())
}

fn lineage(bytes: &[u8]) -> Result<(Option<String>, usize), DbError> {
    match decode_next(bytes) {
        DecodeStep::Lineage {
            lineage_id,
            consumed,
        } => Ok((Some(lineage_id.to_string()), consumed)),
        DecodeStep::NewerWriter { missing, .. } => {
            Err(DbError::Invalid(format!("newer WAL writer: {missing:?}")))
        }
        DecodeStep::Corrupt(e) => Err(DbError::Invalid(format!("corrupt WAL: {e}"))),
        _ => Ok((None, 0)),
    }
}

fn matches_checkpoint(cp: &Checkpoint, bytes: &[u8], lineage: &Option<String>) -> bool {
    if &cp.lineage != lineage || cp.end > bytes.len() as u64 || cp.seed.is_none() {
        return false;
    }
    let Some(seq) = cp.seq else {
        return cp.identity.is_none();
    };
    let Some(range) = bytes.get(cp.off as usize..cp.end as usize) else {
        return false;
    };
    match decode_next(range) {
        DecodeStep::Record { framed, consumed } => {
            framed.seq == seq
                && cp.identity.as_deref() == Some(Sha256::digest(&range[..consumed]).as_slice())
        }
        _ => false,
    }
}

fn load_seed(encoded: &str, bytes: &[u8]) -> Option<OffsetSeed> {
    let value: serde_json::Value = serde_json::from_str(encoded).ok()?;
    let budget = match value.get(1)?.get("admission")?.get("run_started")?.as_u64() {
        Some(off) => match decode_next(bytes.get(usize::try_from(off).ok()?..)?) {
            DecodeStep::Record { framed, .. } => match framed.record {
                broca_wal::WalRecord::RunStarted { config, .. } => config.budget,
                _ => return None,
            },
            _ => return None,
        },
        None => None,
    };
    OffsetSeed::decode(encoded, budget)
}

pub(super) fn index_in_tx(
    tx: &Transaction<'_>,
    log: &PublishedSession<'_>,
    force: bool,
) -> Result<IndexOutcome, DbError> {
    let key = sid(log.bind)?;
    let (initial_lineage, origin_end) = lineage(log.bytes)?;
    let mut lineage_id = initial_lineage.clone();
    let checkpoint = tx.query_row(
        "SELECT indexed_seq,indexed_off,indexed_end,indexed_identity,lineage_id,tail_seed,lineage_off FROM session WHERE sid=?1",
        [&key], |r| Ok(Checkpoint { seq: r.get(0)?, off: r.get(1)?, end: r.get(2)?, identity: r.get(3)?, lineage: r.get(4)?,
            seed: load_seed(&r.get::<_, String>(5)?, log.bytes), lineage_off: r.get(6)? }))
        .optional()?;
    if let Some(off) = checkpoint.as_ref().and_then(|cp| cp.lineage_off) {
        lineage_id = log
            .bytes
            .get(off as usize..)
            .and_then(|bytes| match decode_next(bytes) {
                DecodeStep::Lineage { lineage_id, .. } => Some(lineage_id.to_string()),
                _ => None,
            });
    }
    let rebuilt = force
        || checkpoint
            .as_ref()
            .is_some_and(|cp| !matches_checkpoint(cp, log.bytes, &lineage_id));
    let cp = if rebuilt {
        clear_session(tx, &key)?;
        None
    } else {
        checkpoint
    };
    if rebuilt {
        lineage_id = initial_lineage;
    }
    let mut lineage_off = cp
        .as_ref()
        .and_then(|cp| cp.lineage_off)
        .or_else(|| (origin_end > 0).then_some(0));
    let mut offset = cp.as_ref().map_or(origin_end as u64, |cp| cp.end);
    let mut last_off = cp.as_ref().map_or(0, |cp| cp.off);
    let mut last_seq = cp.as_ref().and_then(|cp| cp.seq);
    let mut identity = cp.as_ref().and_then(|cp| cp.identity.clone());
    let mut seed = cp.and_then(|cp| cp.seed).unwrap_or_default();
    let boundary_lineage = lineage_id.clone();
    tx.execute("INSERT INTO session (sid,project_root,harness,session_key,session_ref,lineage_id,location,indexed_off,indexed_end,head_ord,tail_seed,admission_seed)
        VALUES (?1,?2,?3,?4,?5,?6,?7,0,0,0,'','') ON CONFLICT(sid) DO UPDATE SET session_ref=excluded.session_ref,location=excluded.location",
        params![key, log.bind.project_root.display().to_string(), log.bind.harness, log.bind.session, log.session_ref, lineage_id, log.location.as_str()])?;
    while offset < log.bytes.len() as u64 {
        let remaining = &log.bytes[offset as usize..];
        let (frame, consumed) = match decode_next(remaining) {
            DecodeStep::Record { framed, consumed } => (framed, consumed),
            // A published torn append is not a committed frame. Keep the verified
            // boundary so a later complete publication can index the whole frame.
            DecodeStep::TornTail => break,
            DecodeStep::Lineage {
                lineage_id: found,
                consumed,
            } => {
                let found = found.to_string();
                if lineage_id.as_ref().is_some_and(|id| id != &found) {
                    return Err(DbError::Invalid("conflicting WAL lineage metadata".into()));
                }
                lineage_id = Some(found);
                lineage_off = Some(offset);
                offset += consumed as u64;
                continue;
            }
            DecodeStep::Corrupt(e) => {
                return Err(DbError::Invalid(format!("corrupt WAL at {offset}: {e}")))
            }
            DecodeStep::NewerWriter { missing, .. } => {
                return Err(DbError::Invalid(format!("newer WAL writer: {missing:?}")))
            }
        };
        if last_seq.is_some_and(|seq| seq.checked_add(1) != Some(frame.seq)) {
            return Err(DbError::Invalid(format!(
                "noncontiguous WAL sequence at {offset}"
            )));
        }
        let end = offset + consumed as u64;
        if let broca_wal::WalRecord::RunStarted { session, .. } = &frame.record {
            if session != log.bind {
                return Err(DbError::Invalid(
                    "WAL bind identity differs from indexed session".into(),
                ));
            }
        }
        let delta = seed.observe(&frame, offset, end)?;
        if let Some(to) = delta.truncate {
            tx.execute(
                "DELETE FROM message WHERE sid=?1 AND ord>=?2",
                params![key, u64_to_i64(to)?],
            )?;
        }
        for (ord, source) in delta.messages {
            tx.execute("INSERT INTO message (sid,ord,lo_seq,lo_off,hi_off,input_start,input_end,tag_base,tag_n,tag_high,flags) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)
                ON CONFLICT(sid,ord) DO UPDATE SET lo_seq=excluded.lo_seq,lo_off=excluded.lo_off,hi_off=excluded.hi_off,input_start=excluded.input_start,input_end=excluded.input_end,tag_base=excluded.tag_base,tag_n=excluded.tag_n,tag_high=excluded.tag_high,flags=excluded.flags",
                params![key,u64_to_i64(ord)?,u64_to_i64(source.lo_seq)?,u64_to_i64(source.lo_off)?,u64_to_i64(source.hi_off)?,u64_to_i64(source.input_start)?,u64_to_i64(source.input_end)?,source.tag_base.map(u64_to_i64).transpose()?,u64_to_i64(source.tag_n)?,u64_to_i64(source.tag_high)?,source.flags])?;
        }
        if let Some(ep) = delta.closed_episode {
            write_episode(tx, &key, &ep)?;
        }
        if let Some(ep) = &seed.episode {
            write_episode(tx, &key, ep)?;
        }
        let frame_identity = Sha256::digest(&remaining[..consumed]);
        for fact in &delta.boundaries {
            write_boundary(
                tx,
                &key,
                lineage_id.as_deref(),
                fact,
                &frame,
                offset,
                end,
                &frame_identity,
            )?;
        }
        if let Some((cut, checkpoint)) = delta.reanchor {
            reanchor(tx, &key, cut, checkpoint, &mut seed)?;
        }
        last_seq = Some(frame.seq);
        last_off = offset;
        identity = Some(frame_identity.to_vec());
        offset = end;
    }
    if boundary_lineage != lineage_id {
        tx.execute(
            "UPDATE projection_boundary SET lineage_id=?2 WHERE sid=?1",
            params![key, lineage_id],
        )?;
    }
    tx.execute("UPDATE session SET lineage_id=?2,indexed_seq=?3,indexed_off=?4,indexed_end=?5,indexed_identity=?6,head_ord=?7,tail_seed=?8,admission_seed=?9,frozen_policy=?10,last_activity_ms=?11,last_write_ms=?12,title_off=?13,lineage_off=?14 WHERE sid=?1",
        params![key,lineage_id,last_seq.map(u64_to_i64).transpose()?,u64_to_i64(last_off)?,u64_to_i64(offset)?,identity,u64_to_i64(seed.head_ord())?,seed.encode()?,serde_json::to_string(&seed.admission).map_err(|e| DbError::Invalid(e.to_string()))?,seed.frozen_policy.map(u64_to_i64).transpose()?,seed.last_activity_ms.map(u64_to_i64).transpose()?,seed.last_write_ms.map(u64_to_i64).transpose()?,seed.title_off.map(u64_to_i64).transpose()?,lineage_off.map(u64_to_i64).transpose()?])?;
    Ok(IndexOutcome {
        rebuilt,
        indexed_seq: last_seq,
        indexed_end: offset,
        head_ord: seed.head_ord(),
    })
}

#[expect(
    clippy::too_many_arguments,
    reason = "the scanner supplies one verified frame and its locator"
)]
fn write_boundary(
    tx: &Transaction<'_>,
    key: &str,
    lineage: Option<&str>,
    fact: &broca_core::projection_plan::ProjectionBoundaryFact,
    frame: &broca_wal::FramedRecord,
    off: u64,
    end: u64,
    identity: &[u8],
) -> Result<(), DbError> {
    if let Some(terminal_seq) = fact.terminal_seq {
        if terminal_seq != frame.seq {
            return Err(DbError::Invalid(
                "boundary qualifier differs from current frame".into(),
            ));
        }
        let changed = tx.execute("UPDATE projection_boundary SET qualified=1,terminal_seq=?3,terminal_off=?4,terminal_end=?5,terminal_identity=?6
            WHERE sid=?1 AND seq=?2 AND qualified=0 AND kind=?7 AND episode=?8 AND run_id IS ?9",
            params![key,u64_to_i64(fact.seq)?,u64_to_i64(terminal_seq)?,u64_to_i64(off)?,u64_to_i64(end)?,identity,fact.kind.as_str(),u64_to_i64(fact.episode)?,fact.run_id])?;
        if changed != 1 {
            return Err(DbError::Invalid(
                "boundary qualifier has no matching candidate".into(),
            ));
        }
    } else {
        if fact.seq != frame.seq {
            return Err(DbError::Invalid(
                "boundary candidate differs from current frame".into(),
            ));
        }
        tx.execute("INSERT INTO projection_boundary (sid,seq,lineage_id,frame_off,frame_end,frame_identity,kind,episode,run_id,step_id,batch_id,qualified)
            VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            params![key,u64_to_i64(fact.seq)?,lineage,u64_to_i64(off)?,u64_to_i64(end)?,identity,fact.kind.as_str(),u64_to_i64(fact.episode)?,fact.run_id,fact.step_id.map(u64_to_i64).transpose()?,fact.batch_id,fact.qualified])?;
    }
    Ok(())
}

fn write_episode(tx: &Transaction<'_>, key: &str, ep: &OffsetEpisode) -> Result<(), DbError> {
    tx.execute("INSERT INTO episode (sid,n,run_id,attributable,run_started_seq,run_started_off,first_ord,step_first_ord,end_ord,tag_seed,terminal_off,terminal_kind)
        VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12) ON CONFLICT(sid,n) DO UPDATE SET end_ord=excluded.end_ord,step_first_ord=excluded.step_first_ord,terminal_off=excluded.terminal_off,terminal_kind=excluded.terminal_kind",
        params![key,u64_to_i64(ep.n)?,ep.run_id,ep.run_id.is_some(),ep.run_started_seq.map(u64_to_i64).transpose()?,ep.run_started_off.map(u64_to_i64).transpose()?,u64_to_i64(ep.first_ord)?,ep.step_first_ord.map(u64_to_i64).transpose()?,u64_to_i64(ep.end_ord)?,u64_to_i64(ep.tag_seed)?,ep.terminal_off.map(u64_to_i64).transpose()?,ep.terminal_kind])?;
    if let Some(run_id) = &ep.run_id {
        tx.execute("INSERT INTO run (run_id,sid,episode,state,state_seq,last_step_off,terminal_off,submission_id,request_identity_digest)
            VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9) ON CONFLICT(run_id) DO UPDATE SET state=excluded.state,state_seq=excluded.state_seq,last_step_off=excluded.last_step_off,terminal_off=excluded.terminal_off WHERE run.sid=excluded.sid",
            params![run_id,key,u64_to_i64(ep.n)?,ep.state,u64_to_i64(ep.state_seq)?,ep.last_step_off.map(u64_to_i64).transpose()?,ep.terminal_off.map(u64_to_i64).transpose()?,ep.submission_id,ep.request_identity_digest])?;
    }
    Ok(())
}

fn reanchor(
    tx: &Transaction<'_>,
    key: &str,
    cut: u64,
    checkpoint: u64,
    seed: &mut OffsetSeed,
) -> Result<(), DbError> {
    let mut stmt = tx.prepare("SELECT ord,tag_base,tag_n,tag_high,flags FROM message WHERE sid=?1 AND ord>=?2 ORDER BY ord")?;
    let rows = stmt
        .query_map(params![key, u64_to_i64(cut)?], |r| {
            Ok((
                r.get::<_, u64>(0)?,
                r.get::<_, Option<u64>>(1)?,
                r.get::<_, u64>(2)?,
                r.get::<_, u64>(3)?,
                r.get::<_, u32>(4)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if rows.first().is_none_or(|r| r.0 != cut) {
        return Ok(());
    }
    let mut high = checkpoint.max(rows[0].1.unwrap_or(0));
    for (ord, _, n, stored_high, flags) in rows {
        if flags & 2 != 0 {
            let base = high;
            high = high
                .checked_add(n)
                .ok_or_else(|| DbError::Invalid("tag numbering overflow".into()))?;
            tx.execute(
                "UPDATE message SET tag_base=?3,tag_high=?4 WHERE sid=?1 AND ord=?2",
                params![key, u64_to_i64(ord)?, u64_to_i64(base)?, u64_to_i64(high)?],
            )?;
        } else {
            high = high.max(stored_high);
        }
    }
    seed.tag_high = high;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_wal::framing::encode;
    use broca_wal::{FramedRecord, WalRecord};
    use rusqlite::types::Value;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Scratch(PathBuf);
    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    struct TestDb {
        db: OverhaulDb,
        _scratch: Scratch,
    }
    fn database() -> TestDb {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "offset-db-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("state-format"), b"broca-state/2\n").unwrap();
        let db = OverhaulDb::open(&root).unwrap();
        TestDb {
            db,
            _scratch: Scratch(root),
        }
    }
    fn fixture(name: &str) -> Vec<u8> {
        std::fs::read(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .join(name),
        )
        .unwrap()
    }
    fn decoded(bytes: &[u8]) -> Vec<(usize, usize, FramedRecord)> {
        let mut records = Vec::new();
        let mut off = 0;
        while off < bytes.len() {
            match decode_next(&bytes[off..]) {
                DecodeStep::Lineage { consumed, .. } => off += consumed,
                DecodeStep::Record { framed, consumed } => {
                    records.push((off, off + consumed, *framed));
                    off += consumed;
                }
                DecodeStep::TornTail => break,
                _ => panic!("fixture decode at {off}"),
            }
        }
        records
    }
    fn bind(bytes: &[u8]) -> BindIdentity {
        decoded(bytes)
            .into_iter()
            .find_map(|(_, _, f)| match f.record {
                WalRecord::RunStarted { session, .. } => Some(session),
                _ => None,
            })
            .unwrap()
    }
    fn publication<'a>(bind: &'a BindIdentity, bytes: &'a [u8]) -> PublishedSession<'a> {
        PublishedSession {
            bind,
            session_ref: "s-test",
            location: LogLocation::Live,
            bytes,
        }
    }
    fn rows(db: &OverhaulDb, tables: &[&str]) -> Vec<(String, Vec<Vec<Value>>)> {
        db.read_snapshot(|tx| {
            tables
                .iter()
                .map(|table| {
                    let mut stmt = tx.prepare(&format!("SELECT * FROM {table} ORDER BY 1,2"))?;
                    let count = stmt.column_count();
                    let values = stmt
                        .query_map([], |r| {
                            (0..count)
                                .map(|i| r.get(i))
                                .collect::<rusqlite::Result<Vec<Value>>>()
                        })?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    Ok(((*table).into(), values))
                })
                .collect()
        })
        .unwrap()
    }
    fn r_rows(db: &OverhaulDb) -> Vec<(String, Vec<Vec<Value>>)> {
        rows(db, schema::REBUILDABLE_TABLES)
    }
    fn qualified_boundaries(db: &OverhaulDb) -> Vec<u64> {
        db.read_snapshot(|tx| {
            let mut stmt =
                tx.prepare("SELECT seq FROM projection_boundary WHERE qualified=1 ORDER BY seq")?;
            let rows = stmt
                .query_map([], |r| r.get(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .unwrap()
    }
    fn encode_records(records: &[WalRecord]) -> Vec<u8> {
        records
            .iter()
            .enumerate()
            .flat_map(|(n, record)| encode(n as u64 + 1, 1, record).unwrap())
            .collect()
    }
    fn boundary_cases() -> Vec<(&'static str, Vec<WalRecord>, Vec<u64>)> {
        let source: Vec<_> = decoded(&fixture("scripts/fixtures/resume-close-out-sealed.wal"))
            .into_iter()
            .map(|(_, _, frame)| frame.record)
            .collect();
        let start = source
            .iter()
            .find(|r| matches!(r, WalRecord::RunStarted { .. }))
            .unwrap()
            .clone();
        let step = source
            .iter()
            .find(|r| matches!(r, WalRecord::StepStarted { .. }))
            .unwrap()
            .clone();
        let tool_pos = source.iter().position(|r| matches!(r, WalRecord::ModelStepFinished { assistant_message, .. } if assistant_message.tool_calls().next().is_some())).unwrap();
        let intent_pos = source
            .iter()
            .position(|r| matches!(r, WalRecord::ToolDispatchIntent { .. }))
            .unwrap();
        let turn_pos = source
            .iter()
            .position(|r| matches!(r, WalRecord::TurnFinished { .. }))
            .unwrap();
        let mut text_step = source[tool_pos].clone();
        if let WalRecord::ModelStepFinished {
            assistant_message,
            tags,
            hooks,
            ..
        } = &mut text_step
        {
            *assistant_message = broca_protocol::AssistantMessage {
                message_id: broca_protocol::MessageId("text-step".into()),
                content: vec![broca_protocol::ContentBlock::Text {
                    text: "text".into(),
                }],
            }
            .into();
            *tags = None;
            *hooks = None;
        }
        let mut terminal = source
            .iter()
            .find(|r| matches!(r, WalRecord::RunFinished { .. }))
            .unwrap()
            .clone();
        if let WalRecord::RunFinished { reason, .. } = &mut terminal {
            *reason = broca_protocol::caller::RunFinishReason::Completed;
        }
        let mut interrupted = terminal.clone();
        if let WalRecord::RunFinished { reason, .. } = &mut interrupted {
            *reason = broca_protocol::caller::RunFinishReason::Interrupted;
        }
        let (run_id, request) = match &start {
            WalRecord::RunStarted {
                run_id,
                config,
                input,
                ..
            } => (
                run_id.clone(),
                serde_json::from_value(serde_json::json!({
                    "model": config.model, "input": input, "tools": config.tools,
                }))
                .unwrap(),
            ),
            _ => unreachable!(),
        };
        let paused = WalRecord::RunPaused {
            run_id,
            ts_ms: None,
            reason: broca_protocol::PauseReason::AuthRequired,
            error: None,
        };
        let queued = WalRecord::Queued {
            written_at_ms: None,
            retention: None,
            ts_ms: None,
            keep_warm: None,
            conversation_key: None,
            submission_id: broca_protocol::SubmissionId("boundary-queued".into()),
            request: Box::new(request),
            request_identity_digest: None,
            principal: None,
            send_id: None,
            title: None,
            delivery: None,
            hooks: None,
        };
        let steer = decoded(&fixture(
            "crates/broca-core/tests/fixtures/runner_tags_v1/steer_anthropic.wal",
        ))
        .into_iter()
        .find_map(|(_, _, frame)| {
            matches!(frame.record, WalRecord::Steered { .. }).then_some(frame.record)
        })
        .unwrap();
        let mut seeded = start.clone();
        if let WalRecord::RunStarted { origin, .. } = &mut seeded {
            *origin = broca_wal::RunOrigin::SeededFrom {
                origin_kind: broca_wal::SeedOriginKind::Import,
                source_ref: "fixture:boundary".into(),
                origin_device_pseudonym: None,
                base_seq: None,
            };
        }
        let mut next = start.clone();
        if let WalRecord::RunStarted { run_id, .. } = &mut next {
            run_id.0 = "next-episode".into();
        }
        let mut next_paused = paused.clone();
        if let (
            WalRecord::RunPaused { run_id, .. },
            WalRecord::RunStarted {
                run_id: next_id, ..
            },
        ) = (&mut next_paused, &next)
        {
            *run_id = next_id.clone();
        }
        let mut interposed = source[..=tool_pos].to_vec();
        interposed.extend([queued, steer.clone()]);
        interposed.extend_from_slice(&source[tool_pos + 1..=intent_pos]);
        let mut steer_across_turn = source[..=intent_pos].to_vec();
        steer_across_turn.push(steer.clone());
        steer_across_turn.extend_from_slice(&source[intent_pos + 1..=turn_pos]);
        vec![
            ("swallowed_tool_step", source[..=tool_pos].to_vec(), vec![]),
            (
                "empty_after_dispatch_intent",
                source[..=intent_pos].to_vec(),
                vec![],
            ),
            ("queued_steered_before_unfinished_tool", interposed, vec![]),
            ("steer_across_turn", steer_across_turn, vec![]),
            (
                "committed_turn_without_episode_terminal",
                source[..=turn_pos].to_vec(),
                vec![turn_pos as u64 + 1],
            ),
            (
                "no_tool_terminal",
                vec![
                    start.clone(),
                    step.clone(),
                    text_step.clone(),
                    terminal.clone(),
                ],
                vec![3],
            ),
            (
                "no_tool_paused",
                vec![
                    start.clone(),
                    step.clone(),
                    text_step.clone(),
                    paused.clone(),
                ],
                vec![],
            ),
            (
                "no_tool_open",
                vec![start.clone(), step.clone(), text_step.clone()],
                vec![],
            ),
            (
                "zero_seed_terminal",
                vec![seeded.clone(), terminal.clone()],
                vec![1],
            ),
            (
                "zero_seed_interrupted_terminal",
                vec![seeded.clone(), interrupted],
                vec![1],
            ),
            ("zero_seed_paused", vec![seeded.clone(), paused], vec![]),
            ("zero_seed_open", vec![seeded.clone()], vec![]),
            (
                "zero_seed_intervening_step",
                vec![seeded, step.clone(), terminal.clone()],
                vec![],
            ),
            (
                "prior_boundary_with_paused_episode",
                vec![
                    start.clone(),
                    step.clone(),
                    text_step.clone(),
                    terminal.clone(),
                    next.clone(),
                    step.clone(),
                    text_step.clone(),
                    next_paused,
                ],
                vec![3],
            ),
            (
                "prior_boundary_with_terminal_less_episode",
                vec![
                    start.clone(),
                    step.clone(),
                    text_step.clone(),
                    terminal.clone(),
                    next.clone(),
                    step.clone(),
                    text_step.clone(),
                ],
                vec![3],
            ),
            (
                "prior_boundary_with_steered_episode",
                vec![
                    start.clone(),
                    step.clone(),
                    text_step.clone(),
                    terminal.clone(),
                    next.clone(),
                    steer,
                    step.clone(),
                    text_step.clone(),
                    terminal.clone(),
                ],
                vec![3, 8],
            ),
            (
                "later_episode_terminal",
                vec![start, step, text_step, next, terminal],
                vec![],
            ),
        ]
    }
    fn corpus() -> Vec<Vec<u8>> {
        let mut logs = vec![
            fixture("scripts/fixtures/resume-close-out-sealed.wal"),
            fixture("scripts/fixtures/resume-close-out-unsealed.wal"),
            fixture("crates/broca-core/tests/fixtures/runner_tags_v1/steer_anthropic.wal"),
        ];
        // A multi-episode sample with long, distinctive caller content and titles
        // exercises the same frozen start/step/tool/terminal shapes as real sessions.
        let records = decoded(&logs[0]);
        let mut sample = Vec::new();
        let mut seq = 0;
        for episode in 0..3 {
            for (_, _, frame) in &records {
                let mut record = frame.record.clone();
                if let WalRecord::RunStarted {
                    run_id,
                    title,
                    input,
                    ..
                } = &mut record
                {
                    run_id.0 = format!("production-shaped-{episode}");
                    *title = Some("distinctive-private-title".into());
                    *input = std::sync::Arc::new(vec![broca_protocol::Message::new(
                        broca_protocol::Role::User,
                        vec![broca_protocol::ContentBlock::Text {
                            text: "distinctive-private-input".repeat(100),
                        }],
                    )]);
                }
                sample.extend(encode(seq, frame.fence, &record).unwrap());
                seq += 1;
            }
        }
        logs.push(sample);
        let mut budgeted = Vec::new();
        let mut steer = decoded(&logs[2])
            .into_iter()
            .find_map(|(_, _, f)| matches!(f.record, WalRecord::Steered { .. }).then_some(f.record))
            .unwrap();
        if let WalRecord::Steered { input, tags, .. } = &mut steer {
            *input = std::sync::Arc::new(vec![broca_protocol::Message::new(
                broca_protocol::Role::User,
                vec![broca_protocol::ContentBlock::Text {
                    text: "PRIVATE-HELD-STEER".into(),
                }],
            )]);
            *tags = None;
        }
        let threshold = records
            .iter()
            .find_map(|(_, _, f)| match &f.record {
                WalRecord::ModelStepFinished {
                    assistant_message, ..
                } if assistant_message.tool_calls().count() != 0 => {
                    Some(assistant_message.tool_calls().count() as u64)
                }
                _ => None,
            })
            .unwrap();
        let mut planner = OffsetSeed::default();
        let mut inserted_budget = false;
        let mut run_id = None;
        let mut seq = 0;
        for (_, _, frame) in &records {
            let mut record = frame.record.clone();
            if let WalRecord::RunStarted {
                config, run_id: id, ..
            } = &mut record
            {
                run_id = Some(id.clone());
                config.computed_tags = Some(broca_wal::ComputedTagRules { checkpoint: 0 });
                config.render_tags = true;
                config.budget = Some(broca_protocol::request::BudgetConfig {
                    nudges: vec![broca_protocol::request::BudgetNudge {
                        at_tool_calls: threshold,
                        text: "PRIVATE-NUDGE".into(),
                    }],
                    finalize_text: "PRIVATE-FINALIZE".into(),
                    ..Default::default()
                });
            }
            let is_start = matches!(record, WalRecord::RunStarted { .. });
            let is_batch = matches!(record, WalRecord::ToolBatchFinished { .. });
            planner
                .observe(
                    &FramedRecord {
                        seq,
                        fence: 1,
                        record: record.clone(),
                    },
                    0,
                    1,
                )
                .unwrap();
            budgeted.extend(encode(seq, 1, &record).unwrap());
            seq += 1;
            if is_start {
                planner
                    .observe(
                        &FramedRecord {
                            seq,
                            fence: 1,
                            record: steer.clone(),
                        },
                        0,
                        1,
                    )
                    .unwrap();
                budgeted.extend(encode(seq, 1, &steer).unwrap());
                seq += 1;
            }
            if is_batch && !inserted_budget {
                let record = WalRecord::BudgetFired {
                    run_id: run_id.clone().unwrap(),
                    kind: broca_protocol::request::BudgetKind::Nudge,
                    threshold,
                    ordinal: planner.head_ord(),
                    caller_text: Some("PRIVATE-NUDGE".into()),
                    tags: None,
                };
                planner
                    .observe(
                        &FramedRecord {
                            seq,
                            fence: 1,
                            record: record.clone(),
                        },
                        0,
                        1,
                    )
                    .unwrap();
                budgeted.extend(encode(seq, 1, &record).unwrap());
                seq += 1;
                inserted_budget = true;
            }
        }
        assert!(inserted_budget);
        logs.push(budgeted);
        // later_episode_terminal checks that a terminal cannot license a text
        // step from an earlier unfinished episode. Full replay correctly refuses
        // that invalid history. The other cases also compare indexed message
        // ordinals and tag metadata with a full replay.
        logs.extend(
            boundary_cases()
                .into_iter()
                .filter(|(name, _, _)| *name != "later_episode_terminal")
                .map(|(_, records, _)| encode_records(&records)),
        );
        logs
    }

    #[test]
    fn boundary_candidates_require_their_qualifying_context() {
        for (name, records, expected) in boundary_cases() {
            let bytes = encode_records(&records);
            let bind = bind(&bytes);
            let db = database();
            db.db.index(&publication(&bind, &bytes)).unwrap();
            assert_eq!(qualified_boundaries(&db.db), expected, "{name}");
            let frames: Vec<_> = decoded(&bytes)
                .into_iter()
                .map(|(_, _, frame)| frame)
                .collect();
            assert_eq!(
                expected.last().copied(),
                broca_core::latest_projection_boundary(&frames, 0),
                "{name}"
            );
            let rebuilt = database();
            rebuilt
                .db
                .rebuild_session(&publication(&bind, &bytes))
                .unwrap();
            assert_eq!(r_rows(&db.db), r_rows(&rebuilt.db), "{name}");
        }
    }

    #[test]
    fn boundary_entries_identify_exact_frames_and_leave_terminals_beyond_the_boundary() {
        for (name, records, _) in boundary_cases() {
            let bytes = encode_records(&records);
            let bind = bind(&bytes);
            let db = database();
            db.db.index(&publication(&bind, &bytes)).unwrap();
            db.db.read_snapshot(|tx| {
                let mut stmt = tx.prepare("SELECT seq,frame_off,frame_end,frame_identity,qualified,terminal_seq,terminal_off,terminal_end,terminal_identity,kind,episode,run_id,step_id,batch_id FROM projection_boundary ORDER BY seq")?;
                let mut rows = stmt.query([])?;
                while let Some(row) = rows.next()? {
                    let seq: u64 = row.get(0)?;
                    let off: usize = row.get(1)?;
                    let end: usize = row.get(2)?;
                    let identity: Vec<u8> = row.get(3)?;
                    assert_eq!(identity, Sha256::digest(&bytes[off..end]).as_slice(), "{name}");
                    let DecodeStep::Record { framed, consumed } = decode_next(&bytes[off..end]) else { panic!("boundary is not a complete frame"); };
                    assert_eq!(consumed, end-off);
                    assert_eq!(framed.seq, seq);
                    let kind: String = row.get(9)?;
                    let prefix_records = &records[..seq as usize];
                    let starts: Vec<_> = prefix_records.iter().filter_map(|record| match record {
                        WalRecord::RunStarted { run_id, .. } => Some(run_id), _ => None,
                    }).collect();
                    assert_eq!(row.get::<_,u64>(10)?, starts.len() as u64 - 1);
                    let run_id: String = row.get(11)?;
                    assert_eq!(run_id, starts.last().unwrap().0);
                    let expected_step = match &framed.record {
                        WalRecord::ModelStepFinished { step_id, .. } | WalRecord::TurnFinished { step_id } => Some(step_id.0),
                        WalRecord::RunStarted { .. } => None,
                        _ => panic!("boundary points at a non-candidate frame"),
                    };
                    assert_eq!(row.get::<_,Option<u64>>(12)?, expected_step);
                    let expected_batch = (kind == "turn_finished").then(|| prefix_records.iter().rev()
                        .take_while(|record| !matches!(record, WalRecord::RunStarted { .. } | WalRecord::StepStarted { .. }))
                        .find_map(|record| match record { WalRecord::ToolBatchStarted { batch_id, .. } => Some(batch_id.0.clone()), _ => None })).flatten();
                    assert_eq!(row.get::<_,Option<String>>(13)?, expected_batch);
                    if row.get::<_,bool>(4)? && kind != "turn_finished" {
                        let terminal_seq: u64 = row.get(5)?;
                        let terminal_off: usize = row.get(6)?;
                        let terminal_end: usize = row.get(7)?;
                        let terminal_identity: Vec<u8> = row.get(8)?;
                        assert!(terminal_seq > seq && terminal_off >= end);
                        assert_eq!(terminal_identity, Sha256::digest(&bytes[terminal_off..terminal_end]).as_slice());
                        assert!(matches!(decode_next(&bytes[terminal_off..terminal_end]), DecodeStep::Record { framed, .. }
                            if framed.seq == terminal_seq && matches!(framed.record, WalRecord::RunFinished { .. })));
                    }
                    let mut rewritten = bytes[off..end].to_vec();
                    *rewritten.last_mut().unwrap() ^= 1;
                    assert_ne!(identity, Sha256::digest(&rewritten).as_slice(), "stale frame must not match stored integrity data");
                }
                Ok(())
            }).unwrap();
        }
    }

    #[test]
    fn qualified_boundaries_merge_prefix_and_tail_to_full_replay() {
        let logs = corpus();
        let mut checked = 0;
        let mut saw_steered_tail = false;
        let mut saw_paused_tail = false;
        let mut saw_seeded_boundary = false;
        let mut saw_episode_transition = false;
        for (log_n, bytes) in logs.iter().enumerate() {
            let bind = bind(bytes);
            let db = database();
            for (_, end, _) in decoded(bytes) {
                db.db.index(&publication(&bind, &bytes[..end])).unwrap();
                let framed: Vec<_> = decoded(&bytes[..end])
                    .into_iter()
                    .map(|(_, _, frame)| frame)
                    .collect();
                let full_records: Vec<_> =
                    framed.iter().map(|frame| frame.record.clone()).collect();
                let full = broca_core::replay(&full_records).unwrap();
                for boundary in qualified_boundaries(&db.db) {
                    let split = framed.partition_point(|frame| frame.seq <= boundary);
                    let prefix_frames = &framed[..split];
                    let prefix_records: Vec<_> = prefix_frames
                        .iter()
                        .map(|frame| frame.record.clone())
                        .collect();
                    let prefix_replay = broca_core::replay(&prefix_records).unwrap();
                    let tail: Vec<_> = framed[split..]
                        .iter()
                        .map(|frame| frame.record.clone())
                        .collect();
                    let checkpoint =
                        broca_core::AdmissionCheckpointV1::from_frames(prefix_frames).unwrap();
                    let budget = checkpoint
                        .last_episode
                        .as_ref()
                        .and_then(|episode| episode.budget_replay_state.clone());
                    let config = prefix_replay.run_config.clone();
                    let run_id = prefix_replay.run_id.clone();
                    let check_simple_merge =
                        config.as_ref().is_none_or(|config| config.budget.is_none());
                    let prefix = broca_core::StorePrefix {
                        prefix: prefix_replay.prompt,
                        completed_steps: prefix_replay.completed_steps,
                        usage: prefix_replay.usage,
                        retries_used: prefix_replay.retries_used,
                        last_step_usage: prefix_replay.last_step_usage,
                        last_step_finish_reason: prefix_replay.last_step_finish_reason,
                        watermark_seq: boundary,
                        ..broca_core::StorePrefix::empty()
                    };
                    // Rebuild the stored prefix from the WAL without consulting
                    // eligibility facts, then exercise the real prefix/tail merge.
                    // Frozen config and budget facts are needed when a budget
                    // message in the tail belongs to an episode opened before it.
                    let merged =
                        broca_core::resume_state_from_store(&prefix, &tail, config, run_id, budget)
                            .unwrap_or_else(|error| {
                                panic!("merge log {log_n}, end {end}, boundary {boundary}: {error}")
                            })
                            .prompt;
                    assert_eq!(
                        merged, full.prompt,
                        "log {log_n}, end {end}, boundary {boundary}"
                    );
                    assert_eq!(
                        merged
                            .iter()
                            .map(|message| message.part_tags())
                            .collect::<Vec<_>>(),
                        full.prompt
                            .iter()
                            .map(|message| message.part_tags())
                            .collect::<Vec<_>>(),
                        "tag metadata at log {log_n}, end {end}, boundary {boundary}"
                    );
                    if check_simple_merge {
                        assert_eq!(
                            broca_core::effective_context(&prefix, &tail).unwrap(),
                            full.prompt,
                            "plain context merge at log {log_n}, end {end}, boundary {boundary}"
                        );
                    }
                    saw_steered_tail |= tail
                        .iter()
                        .any(|record| matches!(record, WalRecord::Steered { .. }));
                    saw_paused_tail |= tail
                        .iter()
                        .any(|record| matches!(record, WalRecord::RunPaused { .. }));
                    saw_seeded_boundary |= matches!(
                        &framed[split - 1].record,
                        WalRecord::RunStarted {
                            origin: broca_wal::RunOrigin::SeededFrom { .. },
                            ..
                        }
                    );
                    saw_episode_transition |= tail
                        .iter()
                        .any(|record| matches!(record, WalRecord::RunStarted { .. }));
                    checked += 1;
                }
            }
        }
        assert!(
            checked > 0
                && saw_steered_tail
                && saw_paused_tail
                && saw_seeded_boundary
                && saw_episode_transition
        );
        eprintln!("independent prefix/tail replay oracle: {checked} qualified cuts across {} logs; steered/paused tails, seeded terminals and episode transitions covered", logs.len());
    }

    #[test]
    fn incremental_rows_equal_rebuild_for_crash_cut_corpus_and_production_shaped_sample() {
        for bytes in corpus() {
            let bind = bind(&bytes);
            let incremental = database();
            let rebuilt = database();
            for (_, end, _) in decoded(&bytes) {
                let log = publication(&bind, &bytes[..end]);
                let outcome = incremental.db.index(&log).unwrap();
                rebuilt.db.rebuild_session(&log).unwrap();
                assert_eq!(
                    r_rows(&incremental.db),
                    r_rows(&rebuilt.db),
                    "at byte {end}"
                );
                let records: Vec<_> = decoded(&bytes[..end])
                    .into_iter()
                    .map(|(_, _, f)| f.record)
                    .collect();
                let replay = broca_core::replay(&records).unwrap();
                let framed: Vec<_> = decoded(&bytes[..end])
                    .into_iter()
                    .map(|(_, _, frame)| frame)
                    .collect();
                let boundaries = qualified_boundaries(&incremental.db);
                for watermark in std::iter::once(0).chain(framed.iter().map(|f| f.seq)) {
                    assert_eq!(
                        boundaries
                            .iter()
                            .copied()
                            .filter(|seq| *seq > watermark)
                            .max(),
                        broca_core::latest_projection_boundary(&framed, watermark),
                        "planner/index boundary disagreement at byte {end}, watermark {watermark}"
                    );
                }
                assert_eq!(outcome.head_ord, replay.extent, "ordinal oracle at {end}");
                let tags = incremental
                    .db
                    .read_snapshot(|tx| {
                        let mut stmt = tx.prepare(
                            "SELECT tag_base,tag_n,tag_high,flags FROM message ORDER BY ord",
                        )?;
                        let rows = stmt
                            .query_map([], |r| {
                                Ok((
                                    r.get::<_, Option<u64>>(0)?,
                                    r.get::<_, u64>(1)?,
                                    r.get::<_, u64>(2)?,
                                    r.get::<_, u32>(3)?,
                                ))
                            })?
                            .collect::<rusqlite::Result<Vec<_>>>()?;
                        Ok(rows)
                    })
                    .unwrap();
                assert_eq!(tags.len(), replay.prompt.len());
                for (row, message) in tags.iter().zip(&replay.prompt) {
                    assert_eq!(
                        row.2,
                        message.part_tags().map_or(0, |tags| tags.high()),
                        "tag high at {end}"
                    );
                    if row.3 & 2 != 0 {
                        assert_eq!(
                            row.0,
                            message
                                .part_tags()
                                .and_then(|tags| tags.computed)
                                .map(|tags| tags.base)
                        );
                    }
                }
                incremental
                    .db
                    .read_snapshot(|tx| {
                        let seed: String =
                            tx.query_row("SELECT tail_seed FROM session", [], |r| r.get(0))?;
                        for text in [
                            "distinctive-private-input",
                            "distinctive-private-title",
                            "PRIVATE-NUDGE",
                            "PRIVATE-FINALIZE",
                            "PRIVATE-HELD-STEER",
                        ] {
                            assert!(!seed.contains(text), "index seed copied content: {text}");
                        }
                        Ok(())
                    })
                    .unwrap();
            }
        }
    }

    #[test]
    fn index_self_check_rebuilds_on_shorter_log() {
        let bytes = corpus().remove(0);
        let bind = bind(&bytes);
        let db = database();
        db.db.index(&publication(&bind, &bytes)).unwrap();
        let frames = decoded(&bytes);
        let end = frames[frames.len() - 2].1;
        let short = publication(&bind, &bytes[..end]);
        assert!(db.db.index(&short).unwrap().rebuilt);
        let expected = database();
        expected.db.rebuild_session(&short).unwrap();
        assert_eq!(r_rows(&db.db), r_rows(&expected.db));
        let (off, _, last) = &frames[frames.len() - 2];
        let mut rewritten = bytes[..*off].to_vec();
        rewritten.extend(encode(last.seq, last.fence + 1, &last.record).unwrap());
        assert!(
            db.db
                .index(&publication(&bind, &rewritten))
                .unwrap()
                .rebuilt,
            "changed frame identity must rebuild even with the same seq"
        );
    }

    #[test]
    fn forget_session_reindexes_same_lineage_prefix_rewrites_without_touching_authoritative_rows() {
        use broca_wal::{FileWal, Wal};
        let (_, records, _) = boundary_cases()
            .into_iter()
            .find(|(name, _, _)| *name == "no_tool_terminal")
            .unwrap();
        let db = database();
        let path = db._scratch.0.join("rewrite.wal");
        {
            let mut wal = FileWal::open(&path).unwrap();
            for record in &records {
                wal.append(1, record, true).unwrap();
            }
        }
        let original = std::fs::read(&path).unwrap();
        assert!(matches!(decode_next(&original), DecodeStep::Lineage { .. }));
        let bind = bind(&original);
        let frames = decoded(&original);
        let (off, end, mut changed) = frames
            .iter()
            .find(|(_, _, frame)| matches!(frame.record, WalRecord::ModelStepFinished { .. }))
            .unwrap()
            .clone();
        if let WalRecord::ModelStepFinished {
            assistant_message, ..
        } = &mut changed.record
        {
            std::sync::Arc::make_mut(assistant_message).content =
                vec![broca_protocol::ContentBlock::Text {
                    text: "edit".into(),
                }];
        }
        let replacement = encode(changed.seq, changed.fence, &changed.record).unwrap();
        assert_eq!(replacement.len(), end - off);
        let mut rewritten = original.clone();
        rewritten[off..end].copy_from_slice(&replacement);
        let (checkpoint_off, checkpoint_end, _) = frames.last().unwrap();
        assert_eq!(
            &original[*checkpoint_off..*checkpoint_end],
            &rewritten[*checkpoint_off..*checkpoint_end]
        );

        let mut other_records = records.clone();
        let mut other_bind = bind.clone();
        other_bind.session.push_str("-other");
        if let WalRecord::RunStarted {
            run_id, session, ..
        } = &mut other_records[0]
        {
            *session = other_bind.clone();
            run_id.0 = "other-index-session".into();
        }
        let other = encode_records(&other_records);
        db.db.authoritative_transaction(|tx| {
            tx.execute("INSERT INTO log_reader VALUES ('reader',1,'declared')", [])?;
            tx.execute_batch("CREATE TABLE retention_job (id TEXT PRIMARY KEY, evidence BLOB NOT NULL); INSERT INTO retention_job VALUES ('kept',X'000102FF');")?;
            Ok(())
        }).unwrap();
        let authoritative = rows(&db.db, &["log_reader", "retention_job"]);
        db.db.index(&publication(&other_bind, &other)).unwrap();
        db.db.index(&publication(&bind, &original)).unwrap();
        let original_rows = r_rows(&db.db);
        // This intentionally violates the immutable-prefix contract: authenticating
        // an unchanged checkpoint cannot detect this earlier rewrite. Keeping the
        // stale boundary identity demonstrates exactly why callers must forget.
        assert!(
            !db.db
                .index(&publication(&bind, &rewritten))
                .unwrap()
                .rebuilt
        );
        assert_eq!(r_rows(&db.db), original_rows);
        let fresh = database();
        fresh.db.index(&publication(&other_bind, &other)).unwrap();
        let other_rows = r_rows(&fresh.db);
        fresh
            .db
            .rebuild_session(&publication(&bind, &rewritten))
            .unwrap();
        assert_ne!(
            original_rows,
            r_rows(&fresh.db),
            "the changed earlier boundary must have a different identity"
        );

        db.db.forget_session(&bind).unwrap();
        assert_eq!(
            r_rows(&db.db),
            other_rows,
            "forget must remove only the selected session's R rows"
        );
        assert_eq!(
            rows(&db.db, &["log_reader", "retention_job"]),
            authoritative
        );
        db.db.index(&publication(&bind, &rewritten)).unwrap();
        assert_eq!(r_rows(&db.db), r_rows(&fresh.db));
        assert_eq!(
            rows(&db.db, &["log_reader", "retention_job"]),
            authoritative
        );
    }

    #[test]
    fn stale_undecodable_and_wrong_budget_seeds_rebuild() {
        let bytes = corpus().remove(0);
        let bind = bind(&bytes);
        for invalid in ["[0,{}]", "not-json"] {
            let db = database();
            db.db.index(&publication(&bind, &bytes)).unwrap();
            db.db
                .authoritative_transaction(|tx| {
                    tx.execute("UPDATE session SET tail_seed=?1", [invalid])?;
                    Ok(())
                })
                .unwrap();
            assert!(db.db.index(&publication(&bind, &bytes)).unwrap().rebuilt);
        }
        let mut seed = OffsetSeed::default();
        let mut frame = decoded(&bytes)
            .into_iter()
            .find(|(_, _, f)| matches!(f.record, WalRecord::RunStarted { .. }))
            .unwrap()
            .2;
        if let WalRecord::RunStarted { config, .. } = &mut frame.record {
            config.budget = Some(broca_protocol::request::BudgetConfig {
                nudges: vec![broca_protocol::request::BudgetNudge {
                    at_tool_calls: 1,
                    text: "PRIVATE-NUDGE".into(),
                }],
                finalize_text: "PRIVATE-FINALIZE".into(),
                ..Default::default()
            });
        }
        seed.observe(&frame, 0, 123).unwrap();
        let encoded = seed.encode().unwrap();
        assert!(!encoded.contains("PRIVATE-NUDGE"));
        assert!(!encoded.contains("PRIVATE-FINALIZE"));
        let budget = match &frame.record {
            WalRecord::RunStarted { config, .. } => config.budget.clone(),
            _ => unreachable!(),
        };
        assert!(OffsetSeed::decode(&encoded, budget.clone()).is_some());
        let mut stale: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        stale[0] = (broca_core::projection_plan::OFFSET_SEED_VERSION - 1).into();
        assert!(
            OffsetSeed::decode(&stale.to_string(), budget).is_none(),
            "a prior seed format must not guess boundary state"
        );
        assert!(OffsetSeed::decode(&encoded, None).is_none());
    }

    #[test]
    fn index_rebuilds_on_unsupported_inner_budget_version() {
        let logs = corpus();
        for budgeted in [false, true] {
            let bytes = logs.iter().find(|bytes| decoded(bytes).iter().any(|(_, _, frame)|
                matches!(&frame.record, WalRecord::RunStarted { config, .. } if config.budget.is_some())) == budgeted).unwrap();
            let bind = bind(bytes);
            let db = database();
            db.db.index(&publication(&bind, bytes)).unwrap();
            db.db
                .authoritative_transaction(|tx| {
                    let encoded: String =
                        tx.query_row("SELECT tail_seed FROM session", [], |row| row.get(0))?;
                    let mut seed: serde_json::Value = serde_json::from_str(&encoded).unwrap();
                    assert_eq!(seed[0], broca_core::projection_plan::OFFSET_SEED_VERSION);
                    seed[1]["placement"]["budget"]["version"] = u32::MAX.into();
                    tx.execute("UPDATE session SET tail_seed=?1", [seed.to_string()])?;
                    Ok(())
                })
                .unwrap();
            assert!(db.db.index(&publication(&bind, bytes)).unwrap().rebuilt,
                "a current outer version must not authenticate an unsupported inner budget version (budgeted={budgeted})");
            let fresh = database();
            fresh
                .db
                .rebuild_session(&publication(&bind, bytes))
                .unwrap();
            assert_eq!(r_rows(&db.db), r_rows(&fresh.db));
        }
    }

    #[test]
    fn rebuild_skips_authoritative() {
        let db = database();
        let tables = [
            "log_reader",
            "retention_job",
            "withdrawal_debt",
            "withdrawal_stop",
            "compaction_view_receipt",
            "bus_event_outbox",
            "bus_event_ack",
            "compaction_notice_outbox",
            "classify_claim",
        ];
        db.db.authoritative_transaction(|tx| {
            tx.execute("INSERT INTO log_reader VALUES ('reader',1,'declared')", [])?;
            // Future owners define these state-machine schemas. Unknown tables
            // must survive rebuild just as the already-owned log_reader does.
            for table in &tables[1..] {
                tx.execute_batch(&format!("CREATE TABLE {table} (id TEXT PRIMARY KEY, payload BLOB NOT NULL); INSERT INTO {table} VALUES ('row',X'000102FF');"))?;
            }
            Ok(())
        }).unwrap();
        let before = rows(&db.db, &tables);
        let bytes = corpus().remove(0);
        let bind = bind(&bytes);
        db.db.index(&publication(&bind, &bytes)).unwrap();
        db.db.rebuild_session(&publication(&bind, &bytes)).unwrap();
        db.db
            .rebuild(
                [publication(&bind, &bytes)],
                &super::super::LedgerRows {
                    condemned: &[],
                    address_floors: &[],
                },
            )
            .unwrap();
        assert_eq!(before, rows(&db.db, &tables));
    }

    #[test]
    fn index_commit_failure_preserves_both_rows_and_indexed_seq() {
        let bytes = corpus().remove(0);
        let bind = bind(&bytes);
        let frames = decoded(&bytes);
        let db = database();
        db.db
            .index(&publication(&bind, &bytes[..frames[0].1]))
            .unwrap();
        let before = r_rows(&db.db);
        for (_, end, _) in &frames[1..] {
            let result: Result<(), DbError> = db.db.transaction(false, |tx| {
                index_in_tx(tx, &publication(&bind, &bytes[..*end]), false)?;
                Err(DbError::Invalid(
                    "injected failure immediately before COMMIT".into(),
                ))
            });
            assert!(result.is_err());
            assert_eq!(r_rows(&db.db), before);
        }
        let result: Result<(), DbError> = db.db.transaction(false, |tx| {
            index_in_tx(tx, &publication(&bind, &bytes), true)?;
            let qualified: u64 = tx.query_row(
                "SELECT COUNT(*) FROM projection_boundary WHERE qualified=1",
                [],
                |r| r.get(0),
            )?;
            assert!(
                qualified > 0,
                "the failed rebuild must reach boundary writes before COMMIT"
            );
            Err(DbError::Invalid(
                "injected rebuild failure before COMMIT".into(),
            ))
        });
        assert!(result.is_err());
        assert_eq!(r_rows(&db.db), before);
    }

    /// SQLite is taken as-is: NORMAL can lose its latest index commits, while a
    /// restore can leave it ahead of the log. Only log I/O is power-cut here;
    /// transaction rollback is covered separately by the pre-COMMIT error test.
    #[cfg(unix)]
    #[test]
    fn power_loss_reconciliation_matches_rebuild_for_ahead_and_lagging_indexes() {
        use broca_crash_harness::power_loss::{RecordingFs, State};
        use broca_wal::fs_ops::with_fs_ops;
        use broca_wal::{FileWal, Wal};
        use std::collections::BTreeSet;
        use std::path::Path;
        use std::sync::Arc;

        let scratch = database();
        let source = scratch._scratch.0.join("source");
        std::fs::create_dir(&source).unwrap();
        let recording = Arc::new(RecordingFs::new(&source).unwrap());
        let original = corpus().remove(0);
        let bind = bind(&original);
        let start = decoded(&original)
            .into_iter()
            .find_map(|(_, _, frame)| {
                matches!(frame.record, WalRecord::RunStarted { .. }).then_some(frame.record)
            })
            .unwrap();
        let pending = WalRecord::ModelStepFinished {
            fallback: Default::default(),
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: broca_protocol::StepId(1),
            assistant_message: broca_protocol::AssistantMessage {
                message_id: broca_protocol::MessageId("pending".into()),
                content: vec![broca_protocol::ContentBlock::Text {
                    text: "p".repeat(9000),
                }],
            }
            .into(),
            usage: broca_protocol::Usage::default(),
            finish_reason: None,
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        };
        with_fs_ops(recording.clone(), || {
            let mut wal = FileWal::open(source.join("wal/log")).unwrap();
            wal.append(1, &start, true).unwrap();
            wal.append(
                1,
                &WalRecord::StepStarted {
                    step_id: broca_protocol::StepId(1),
                },
                true,
            )
            .unwrap();
            wal.append(1, &pending, false).unwrap();
        });
        let precut = std::fs::read(source.join("wal/log")).unwrap();
        let frames = decoded(&precut);
        let known_non_tail_refusal = |error: &broca_wal::WalError, bytes: &[u8]| {
            let broca_wal::WalError::Corrupt { offset, .. } = error else {
                return false;
            };
            // The writer has no durable synced-length boundary: an earlier
            // damaged region before an authentic later frame is refused rather
            // than guessed away. Keep that known availability gap explicit.
            (0..bytes.len()).filter(|off| *off > *offset).any(|off| {
                match decode_next(&bytes[off..]) {
                    DecodeStep::Record { framed, .. } => frames
                        .iter()
                        .any(|(_, _, expected)| expected == framed.as_ref()),
                    _ => false,
                }
            })
        };
        let mut index_ends = vec![0];
        index_ends.extend(frames.iter().map(|(_, end, _)| *end));

        // The budget refuses an incomplete exploration; it never samples away
        // states. Deduplication below merges only byte-identical filesystems.
        let first = recording.enumerate(1_000_000).unwrap();
        let last_cut = first.iter().map(|cut| cut.cut).max().unwrap();
        assert!(
            first.iter().any(|cut| cut.cut == last_cut
                && matches!(
                    cut.family,
                    broca_crash_harness::power_loss::Family::SyncedOnly
                )
                && cut
                    .state
                    .files
                    .get(Path::new("wal/log"))
                    .is_some_and(|bytes| decoded(bytes).len() + 1 == frames.len())),
            "the final unsynced append must be lost in some final-cut states"
        );
        let second = recording
            .enumerate_recovery(
                &scratch._scratch.0.join("recovery-cuts"),
                1_000_000,
                |root| {
                    match FileWal::open(root.join("wal/log")) {
                        Ok(wal) => {
                            wal.replay().map_err(std::io::Error::other)?;
                        }
                        Err(error) => {
                            let bytes = std::fs::read(root.join("wal/log"))?;
                            assert!(
                                known_non_tail_refusal(&error, &bytes),
                                "unexpected WAL recovery refusal: {error}"
                            );
                        }
                    }
                    Ok(())
                },
            )
            .unwrap();
        assert!(!first.is_empty() && !second.is_empty());
        assert!(
            second.iter().any(|cut| cut.second.cut > 0),
            "recovery must itself have cut points"
        );
        assert!(
            second.iter().any(
                |cut| cut
                    .first
                    .state
                    .files
                    .get(Path::new("wal/log"))
                    .is_some_and(|first| cut
                        .second
                        .state
                        .files
                        .get(Path::new("wal/log"))
                        .is_some_and(|second| second.len() < first.len()))
            ),
            "a cut during recovery must retain the real torn-tail truncation"
        );
        let first_count = first.len();
        let second_count = second.len();
        let states: BTreeSet<State> = first
            .into_iter()
            .map(|cut| cut.state)
            .chain(second.into_iter().map(|cut| cut.second.state))
            .collect();
        let mut ahead = 0;
        let mut lagging = 0;
        let mut torn = 0;
        let mut repaired = 0;
        let mut pairs = 0;
        let mut refused = 0;
        let mut refused_pairs = 0;
        for (n, state) in states.iter().enumerate() {
            let before = state
                .files
                .get(Path::new("wal/log"))
                .map(Vec::as_slice)
                .unwrap_or_default();
            let mut off = 0;
            while off < before.len() {
                match decode_next(&before[off..]) {
                    DecodeStep::Record { consumed, .. } | DecodeStep::Lineage { consumed, .. } => {
                        off += consumed
                    }
                    DecodeStep::TornTail | DecodeStep::Corrupt(_) => {
                        torn += 1;
                        break;
                    }
                    DecodeStep::NewerWriter { .. } => panic!("power cut invented a newer feature"),
                }
            }
            let root = scratch._scratch.0.join(format!("survivor-{n}"));
            state.install(&root).unwrap();
            let wal = match FileWal::open(root.join("wal/log")) {
                Ok(wal) => wal,
                Err(error) => {
                    assert!(
                        known_non_tail_refusal(&error, before),
                        "unexpected WAL recovery refusal: {error}"
                    );
                    refused += 1;
                    let fresh = database();
                    assert!(fresh
                        .db
                        .rebuild_session(&publication(&bind, before))
                        .is_err());
                    for end in &index_ends {
                        let TestDb {
                            db,
                            _scratch: index_root,
                        } = database();
                        db.index(&publication(&bind, &precut[..*end])).unwrap();
                        let rows_before = r_rows(&db);
                        drop(db);
                        let resumed = OverhaulDb::open(&index_root.0).unwrap();
                        assert!(
                            resumed.index(&publication(&bind, before)).is_err(),
                            "a refused log must not serve an indexed prefix"
                        );
                        assert_eq!(
                            r_rows(&resumed),
                            rows_before,
                            "refusal must not partially rewrite the index"
                        );
                        refused_pairs += 1;
                    }
                    continue;
                }
            };
            let recovered_frames = wal.replay().unwrap();
            let recovered = std::fs::read(root.join("wal/log")).unwrap();
            let mut verified_end = 0;
            while verified_end < recovered.len() {
                verified_end += match decode_next(&recovered[verified_end..]) {
                    DecodeStep::Record { consumed, .. } | DecodeStep::Lineage { consumed, .. } => {
                        consumed
                    }
                    _ => panic!("recovery left a damaged frame at {verified_end}"),
                };
            }
            repaired += usize::from(before != recovered);
            assert!(recovered_frames.len() <= frames.len());
            for (actual, (_, _, expected)) in recovered_frames.iter().zip(&frames) {
                assert_eq!(actual, expected, "recovery fabricated a non-prefix record");
            }
            let baseline = database();
            let expected = baseline
                .db
                .rebuild_session(&publication(&bind, &recovered))
                .unwrap();
            let expected_rows = r_rows(&baseline.db);
            assert_eq!(expected.indexed_end, verified_end as u64);
            assert_eq!(expected.indexed_seq, recovered_frames.last().map(|f| f.seq));
            let records: Vec<_> = recovered_frames.iter().map(|f| f.record.clone()).collect();
            assert_eq!(
                expected.head_ord,
                broca_core::replay(&records).unwrap().extent
            );
            for end in &index_ends {
                let TestDb {
                    db,
                    _scratch: index_root,
                } = database();
                let prior = db.index(&publication(&bind, &precut[..*end])).unwrap();
                ahead += usize::from(prior.indexed_end > expected.indexed_end);
                lagging += usize::from(prior.indexed_end < expected.indexed_end);
                drop(db);
                // Reopen the actual SQLite file rather than constructing its
                // post-crash rows in memory or assuming the index and log agree.
                let resumed = OverhaulDb::open(&index_root.0).unwrap();
                let reconciled = resumed.index(&publication(&bind, &recovered)).unwrap();
                assert_eq!(
                    r_rows(&resumed),
                    expected_rows,
                    "survivor {n}, prior index end {end}"
                );
                assert_eq!(reconciled.indexed_seq, expected.indexed_seq);
                assert_eq!(reconciled.indexed_end, expected.indexed_end);
                assert_eq!(reconciled.indexed_end, verified_end as u64);
                resumed
                    .read_snapshot(|tx| {
                        let high: Option<u64> =
                            tx.query_row("SELECT MAX(hi_off) FROM message", [], |row| row.get(0))?;
                        assert!(high.is_none_or(|end| end <= reconciled.indexed_end));
                        Ok(())
                    })
                    .unwrap();
                pairs += 1;
            }
        }
        assert!(ahead > 0 && lagging > 0 && torn > 0 && repaired > 0);
        eprintln!("power-loss index matrix: {first_count} first cuts, {second_count} recovery cuts, {} unique states, {pairs} reconciled pairs; {ahead} ahead, {lagging} lagging, {torn} torn, {repaired} repaired; {refused} known non-tail refusals ({refused_pairs} refused pairs)", states.len());
    }

    #[test]
    #[cfg(unix)]
    fn boundary_qualification_survives_shared_power_loss_cuts() {
        use broca_crash_harness::power_loss::RecordingFs;
        use broca_wal::fs_ops::with_fs_ops;
        use broca_wal::{FileWal, Wal};
        use std::collections::BTreeSet;
        use std::sync::Arc;
        for (name, records, _) in boundary_cases()
            .into_iter()
            .filter(|(name, _, _)| matches!(*name, "no_tool_terminal" | "zero_seed_terminal"))
        {
            let scratch = database();
            let source = scratch._scratch.0.join("source");
            std::fs::create_dir(&source).unwrap();
            let path = source.join("wal/log");
            {
                let mut wal = FileWal::open(&path).unwrap();
                for record in &records[..records.len() - 1] {
                    wal.append(1, record, true).unwrap();
                }
            }
            let candidate = std::fs::read(&path).unwrap();
            let bind = bind(&candidate);
            let db = database();
            db.db.index(&publication(&bind, &candidate)).unwrap();
            assert!(
                qualified_boundaries(&db.db).is_empty(),
                "{name}: candidate before terminal"
            );
            let recording = Arc::new(RecordingFs::new(&source).unwrap());
            with_fs_ops(recording.clone(), || {
                FileWal::open(&path)
                    .unwrap()
                    .append(1, records.last().unwrap(), false)
                    .unwrap();
            });
            let precut = std::fs::read(&path).unwrap();
            let frames = decoded(&precut);
            let candidate_seq = frames[frames.len() - 2].2.seq;
            let ends: Vec<_> = std::iter::once(0)
                .chain(frames.iter().map(|(_, end, _)| *end))
                .collect();
            let first = recording.enumerate(1_000_000).unwrap();
            let second = recording
                .enumerate_recovery(
                    &scratch._scratch.0.join("recovery-cuts"),
                    1_000_000,
                    |root| {
                        FileWal::open(root.join("wal/log"))
                            .map_err(std::io::Error::other)?
                            .replay()
                            .map_err(std::io::Error::other)?;
                        Ok(())
                    },
                )
                .unwrap();
            let first_count = first.len();
            let second_count = second.len();
            let states: BTreeSet<_> = first
                .into_iter()
                .map(|cut| cut.state)
                .chain(second.into_iter().map(|cut| cut.second.state))
                .collect();
            let mut qualified = 0;
            let mut unqualified = 0;
            let mut pairs = 0;
            for (n, state) in states.iter().enumerate() {
                let root = scratch._scratch.0.join(format!("survivor-{n}"));
                state.install(&root).unwrap();
                let wal = FileWal::open(root.join("wal/log")).unwrap();
                let has_terminal = wal
                    .replay()
                    .unwrap()
                    .iter()
                    .any(|frame| matches!(frame.record, WalRecord::RunFinished { .. }));
                let recovered = std::fs::read(root.join("wal/log")).unwrap();
                let expected_lineage = match decode_next(&recovered) {
                    DecodeStep::Lineage { lineage_id, .. } => lineage_id.to_string(),
                    _ => panic!("the recovered log must retain its durable lineage frame"),
                };
                let expected = if has_terminal {
                    qualified += 1;
                    vec![candidate_seq]
                } else {
                    unqualified += 1;
                    vec![]
                };
                let baseline = database();
                baseline
                    .db
                    .rebuild_session(&publication(&bind, &recovered))
                    .unwrap();
                let expected_rows = r_rows(&baseline.db);
                for end in &ends {
                    let TestDb {
                        db,
                        _scratch: index_root,
                    } = database();
                    db.index(&publication(&bind, &precut[..*end])).unwrap();
                    drop(db);
                    let resumed = OverhaulDb::open(&index_root.0).unwrap();
                    resumed.index(&publication(&bind, &recovered)).unwrap();
                    assert_eq!(
                        qualified_boundaries(&resumed),
                        expected,
                        "{name}: survivor {n}, prior end {end}"
                    );
                    assert_eq!(
                        r_rows(&resumed),
                        expected_rows,
                        "{name}: survivor {n}, prior end {end}"
                    );
                    resumed.read_snapshot(|tx| {
                        let mismatched: u64 = tx.query_row("SELECT COUNT(*) FROM projection_boundary WHERE lineage_id IS NOT ?1", [&expected_lineage], |row| row.get(0))?;
                        assert_eq!(mismatched, 0, "boundary locators must retain the log's lineage");
                        Ok(())
                    }).unwrap();
                    pairs += 1;
                }
            }
            assert!(qualified > 0 && unqualified > 0);
            eprintln!("boundary qualification {name}: {first_count} first cuts, {second_count} recovery cuts, {} unique states, {pairs} index/log pairs, {qualified} qualified, {unqualified} unqualified", states.len());
        }
    }

    #[test]
    fn synchronous_switches_before_each_transaction_and_errors_rollback() {
        let db = database();
        for full in [false, true, false, true, false] {
            db.db
                .transaction(full, |tx| {
                    let mode: i64 = tx.query_row("PRAGMA synchronous", [], |r| r.get(0))?;
                    assert_eq!(mode, if full { 2 } else { 1 });
                    Ok(())
                })
                .unwrap();
        }
        let bytes = corpus().remove(0);
        let bind = bind(&bytes);
        let before = r_rows(&db.db);
        let error: Result<(), DbError> = db.db.transaction(false, |tx| {
            index_in_tx(tx, &publication(&bind, &bytes), false)?;
            Err(DbError::Invalid("abort before commit".into()))
        });
        assert!(error.is_err());
        assert_eq!(before, r_rows(&db.db));
        db.db.index(&publication(&bind, &bytes)).unwrap();
        assert_ne!(before, r_rows(&db.db));
    }

    #[test]
    fn refuses_legacy_roots_and_corrupt_or_newer_suffixes_without_mutation() {
        let db = database();
        std::fs::remove_file(db._scratch.0.join("state-format")).unwrap();
        assert!(OverhaulDb::open(&db._scratch.0).is_err());
        let mut bytes = corpus().remove(0);
        let bind = bind(&bytes);
        let frames = decoded(&bytes);
        let end = frames[frames.len() - 2].1;
        db.db.index(&publication(&bind, &bytes[..end])).unwrap();
        let before = r_rows(&db.db);
        *bytes.last_mut().unwrap() ^= 1;
        assert!(db.db.index(&publication(&bind, &bytes)).is_err());
        assert_eq!(before, r_rows(&db.db));
        let last = &frames.last().unwrap().2;
        let mut newer = bytes[..end].to_vec();
        newer.extend(
            broca_wal::framing::encode_with_requires(
                last.seq,
                last.fence,
                &["unrecognized-index-feature/v1"],
                &last.record,
            )
            .unwrap(),
        );
        let error = db.db.index(&publication(&bind, &newer)).unwrap_err();
        assert!(error.to_string().contains("newer WAL writer"));
        assert_eq!(before, r_rows(&db.db));
        assert!(db
            .db
            .read_snapshot(|tx| {
                tx.execute("DELETE FROM session", [])?;
                Ok(())
            })
            .is_err());
        assert_eq!(before, r_rows(&db.db));
    }
}
