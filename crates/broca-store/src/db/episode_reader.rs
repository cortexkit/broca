//! Publication-bounded episode reads. SQL stores locators and continuation metadata only.

use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use broca_core::context::{EpisodeFrames, EpisodeModelPage, EpisodePageIndex};
use broca_core::projection::{decode_positioned_frames, EpisodeSeed, IndexedEpisode};
use broca_core::projection_plan::{OffsetSeed, ProjectionContinuation};
use broca_core::replay::{PositionedFrame, SeedFallback};
use broca_core::{Cursor, ModelPageRequest};
use broca_protocol::BindIdentity;
use broca_wal::access_counter;
use broca_wal::file_wal::DurablePublication;
use broca_wal::FramedRecord;
use rusqlite::{params, OptionalExtension, Transaction};
use sha2::{Digest, Sha256};

use super::{DbError, LogLocation, OverhaulDb};

/// One session and one writer-provided `DurablePublication`, shared by page and attach reads.
#[derive(Clone)]
pub struct DatabaseEpisodeReader {
    db: Arc<OverhaulDb>,
    session: BindIdentity,
    publication: DurablePublication,
}

/// An atomic index snapshot, authenticated before any coordinate is exposed.
pub struct EpisodeSnapshot {
    pub episodes: Vec<IndexedEpisode>,
    pub head_ord: u64,
    pub head: Option<Cursor>,
    pub lineage_id: String,
    tags: BTreeMap<u64, u64>,
    compaction: Vec<FramedRecord>,
}

fn bad(reason: &'static str) -> DbError {
    DbError::SeedFallback(SeedFallback::new(reason))
}

impl OverhaulDb {
    /// Publication and reads must be serialized for this session, as for raw pages.
    /// Catch-up happens before coordinates are loaded: Setup can renumber older messages.
    pub fn episode_reader(
        self: &Arc<Self>,
        session: BindIdentity,
        publication: DurablePublication,
    ) -> Result<DatabaseEpisodeReader, DbError> {
        let key = super::index::sid(&session)?;
        let indexed = self.read_snapshot(|tx| {
            Ok(tx
                .query_row(
                    "SELECT indexed_seq,lineage_id FROM session WHERE sid=?1",
                    [&key],
                    |r| Ok((r.get::<_, u64>(0)?, r.get::<_, String>(1)?)),
                )
                .optional()?)
        })?;
        if indexed.is_none_or(|(seq, lineage)| {
            seq < publication.sequence() || lineage != publication.lineage().to_string()
        }) {
            self.publish(
                &session,
                &broca_core::agent_session_ref(&session),
                LogLocation::Live,
                &publication,
            )?;
        }
        Ok(DatabaseEpisodeReader {
            db: self.clone(),
            session,
            publication,
        })
    }
}

impl DatabaseEpisodeReader {
    pub fn sequence(&self) -> u64 {
        self.publication.sequence()
    }

    pub fn byte_end(&self) -> u64 {
        self.publication.byte_end()
    }

    pub fn read_at(&self, off: u64, len: u64) -> std::io::Result<Vec<u8>> {
        let mut file = broca_wal::fs_ops::File::open(self.publication.path())?;
        file.seek(SeekFrom::Start(off))?;
        let mut bytes = Vec::new();
        file.take(len.min(self.byte_end().saturating_sub(off)))
            .read_to_end(&mut bytes)?;
        Ok(bytes)
    }

    fn read_range(&self, lo: u64, hi: u64) -> Result<Vec<PositionedFrame>, DbError> {
        if hi > self.byte_end() {
            return Err(bad("episode range exceeds publication"));
        }
        decode_positioned_frames(&|off, len| self.read_at(off, len), lo, hi, None)
            .map_err(|_| bad("episode frames unreadable"))
    }

    /// Callers name and count the fallback decision before entering this full-read route.
    pub fn complete_read(&self) -> Result<Vec<FramedRecord>, DbError> {
        // Falling back discards index state, not the publication's identity.
        // A new log at the same path must not be served with the old lineage.
        let (_, lineage) =
            broca_core::projection::log_header_info(&|off, len| self.read_at(off, len))?;
        if lineage != Some(self.publication.lineage()) {
            return Err(DbError::Invalid(
                "episode publication lineage changed".into(),
            ));
        }
        let records: Vec<_> = self
            .read_range(0, self.byte_end())?
            .into_iter()
            .map(|p| p.framed)
            .collect();
        if records.first().is_some_and(|first| first.seq != 1)
            || records.last().map_or(0, |last| last.seq) != self.sequence()
            || records
                .windows(2)
                .any(|pair| pair[0].seq.checked_add(1) != Some(pair[1].seq))
        {
            return Err(DbError::Invalid(
                "full episode replay does not cover publication".into(),
            ));
        }
        Ok(records)
    }

    fn seed(
        &self,
        tx: &Transaction<'_>,
        key: &str,
        start: u64,
    ) -> Result<Option<EpisodeSeed>, DbError> {
        let boundary = tx.query_row(
            "SELECT b.seq,s.continuation FROM projection_boundary b LEFT JOIN episode_seed s ON s.sid=b.sid AND s.seq=b.seq WHERE b.sid=?1 AND b.qualified=1 AND b.seq<?2 ORDER BY b.seq DESC LIMIT 1",
            params![key,start], |r| Ok((r.get::<_,u64>(0)?,r.get::<_,Option<String>>(1)?)),
        ).optional()?;
        let Some((seq, encoded)) = boundary else {
            let earlier: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM episode WHERE sid=?1 AND run_started_seq<?2)",
                params![key, start],
                |r| r.get(0),
            )?;
            if earlier {
                return Err(bad("historical episode boundary missing"));
            }
            return Ok(None);
        };
        let encoded = encoded.ok_or_else(|| bad("historical episode continuation missing"))?;
        let stored = ProjectionContinuation::decode(&encoded)
            .map_err(|_| bad("historical episode continuation unsupported"))?;
        let seed = EpisodeSeed::from_continuation(&stored);
        let matches: bool = tx.query_row("SELECT lineage_id IS ?3 AND frame_off=?4 AND frame_end=?5 AND frame_identity=?6 AND qualified=1 FROM projection_boundary WHERE sid=?1 AND seq=?2", params![key,seq,seed.boundary.lineage_id,seed.boundary.frame_off,seed.boundary.frame_end,seed.boundary.frame_identity], |r| r.get(0))?;
        if !matches
            || seed.boundary.seq != seq
            || seed.boundary.lineage_id.as_deref()
                != Some(self.publication.lineage().to_string().as_str())
        {
            return Err(bad("historical episode continuation boundary mismatch"));
        }
        Ok(Some(seed))
    }

    fn frames(
        &self,
        tx: &Transaction<'_>,
        key: &str,
        episodes: &[IndexedEpisode],
        range: std::ops::Range<usize>,
    ) -> Result<EpisodeFrames, DbError> {
        let episode = &episodes[range.start];
        let seed = episode
            .run_started_seq
            .map(|seq| self.seed(tx, key, seq))
            .transpose()?
            .flatten();
        let start = match &seed {
            Some(seed) => seed.boundary.frame_end,
            None => broca_core::projection::log_header_info(&|off, len| self.read_at(off, len))?.0,
        };
        let end = episodes
            .get(range.end)
            .and_then(|e| e.run_started_off)
            .unwrap_or(self.byte_end());
        Ok(EpisodeFrames {
            seed,
            frames: self.read_range(start, end)?,
        })
    }

    fn snapshot_in(
        &self,
        tx: &Transaction<'_>,
        key: &str,
        compaction: bool,
    ) -> Result<EpisodeSnapshot, DbError> {
        let (seq,off,end,identity,lineage,count,locator,tail): (u64,u64,u64,Vec<u8>,String,u64,String,String) = tx.query_row("SELECT indexed_seq,indexed_off,indexed_end,indexed_identity,lineage_id,head_ord,locator_seed,tail_seed FROM session WHERE sid=?1", [key], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?)))?;
        if seq != self.sequence()
            || end != self.byte_end()
            || lineage != self.publication.lineage().to_string()
        {
            return Err(bad("episode snapshot differs from publication"));
        }
        let bytes = access_counter::read_at(
            &|off, len| self.read_at(off, len),
            off,
            end.checked_sub(off)
                .ok_or_else(|| bad("episode cursor range invalid"))?,
        )?;
        if !matches!(access_counter::decode_next(&bytes), broca_wal::framing::DecodeStep::Record { framed, consumed } if framed.seq == seq && Sha256::digest(&bytes[..consumed]).as_slice() == identity)
        {
            return Err(bad("episode cursor identity mismatch"));
        }
        let continuation = ProjectionContinuation::decode(&tail).map_err(DbError::SeedFallback)?;
        let seed = EpisodeSeed::from_continuation(&continuation);
        let frames = self.read_range(seed.boundary.frame_end, end)?;
        broca_core::replay::replay_seeded_at_validated_boundary(
            &|off, len| self.read_at(off, len),
            &seed.boundary,
            &frames,
            None,
            None,
            None,
            None,
            seed.ordinal_base,
            false,
            seed.tag_seed,
            Vec::new(),
        )
        .map_err(DbError::SeedFallback)?;
        let locator = OffsetSeed::decode(&locator, None)
            .ok_or_else(|| bad("episode locator continuation unsupported"))?;
        let head = broca_core::last_control_cursor_through(
            &frames.iter().map(|p| p.framed.clone()).collect::<Vec<_>>(),
            None,
        )
        .or(locator
            .qualified_control_head
            .map(|(wal_seq, sub_index)| Cursor { wal_seq, sub_index }));
        let episodes = tx.prepare("SELECT run_started_seq,run_started_off,first_ord,end_ord FROM episode WHERE sid=?1 ORDER BY n")?
            .query_map([key], |r| Ok(IndexedEpisode{run_started_seq:r.get(0)?,run_started_off:r.get(1)?,first_ord:r.get(2)?,end_ord:r.get(3)?}))?.collect::<rusqlite::Result<Vec<_>>>()?;
        let mut metadata = Vec::new();
        if compaction {
            for off in locator.admission.compaction {
                let auth:Option<(u64,u64,Vec<u8>)> = tx.query_row("SELECT frame_end,seq,frame_identity FROM source_auth WHERE sid=?1 AND frame_off=?2",params![key,off],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
                let (end, seq, identity) =
                    auth.ok_or_else(|| bad("model compaction source authentication missing"))?;
                let len = end
                    .checked_sub(off)
                    .filter(|len| *len > 0 && end <= self.byte_end())
                    .ok_or_else(|| bad("model compaction source range invalid"))?;
                let bytes = access_counter::read_at(&|off, len| self.read_at(off, len), off, len)?;
                let frame = match access_counter::decode_next(&bytes) {
                    broca_wal::framing::DecodeStep::Record { framed, consumed }
                        if framed.seq == seq
                            && consumed == bytes.len()
                            && Sha256::digest(&bytes).as_slice() == identity =>
                    {
                        *framed
                    }
                    _ => return Err(bad("model compaction source identity mismatch")),
                };
                if !matches!(
                    frame.record,
                    broca_wal::WalRecord::CompactionMessage { .. }
                        | broca_wal::WalRecord::CompactionAnswer { .. }
                ) {
                    return Err(bad("model compaction locator kind mismatch"));
                }
                metadata.push(frame);
            }
        }
        Ok(EpisodeSnapshot {
            episodes,
            head_ord: count,
            head,
            lineage_id: lineage,
            tags: BTreeMap::new(),
            compaction: metadata,
        })
    }

    pub fn read_model_page(&self, request: &ModelPageRequest) -> Result<EpisodeModelPage, DbError> {
        let key = super::index::sid(&self.session)?;
        let result = self.db.read_snapshot(|tx| {
            let mut snapshot = self.snapshot_in(tx,&key,true)?;
            // SQL rows are content-free. Fetch tag coordinates only for the bounded page window.
            let limit = request.limit.unwrap_or(broca_core::SESSION_READ_DEFAULT_LIMIT).clamp(1,broca_core::SESSION_READ_MAX_LIMIT);
            let start = request.from_ordinal.unwrap_or_else(|| snapshot.head_ord.saturating_sub(limit));
            let hidden = snapshot.compaction.iter().fold(broca_core::compaction::CompactionCheckpoint::None, |mut state, frame| { state.fold(&frame.record); state }).state().and_then(|state|state.applied.as_ref()).map_or(0,|applied|applied.range.to);
            let start = start.max(hidden);
            snapshot.tags = tx.prepare("SELECT ord,tag_base FROM message WHERE sid=?1 AND ord>=?2 AND ord<?3 AND flags&2!=0")?
                .query_map(params![key,start,start.saturating_add(limit).saturating_add(1)], |r| Ok((r.get::<_,u64>(0)?,r.get::<_,u64>(1)?)))?.collect::<rusqlite::Result<_>>()?;
            broca_core::context::read_session_model_page_by_episode(
                &|off,len|self.read_at(off,len),
                &EpisodePageIndex { episodes:&snapshot.episodes,head_ord:snapshot.head_ord,compaction_frames:&snapshot.compaction,tag_base:&|ord| snapshot.tags.get(&ord).copied(),head:snapshot.head,lineage_id:Some(snapshot.lineage_id) },
                request,
                |range|self.frames(tx,&key,&snapshot.episodes,range).map_err(|e| match e { DbError::SeedFallback(reason) => reason, _ => SeedFallback::new("episode frames unreadable") }),
                ||self.complete_read().map_err(|e|e.to_string()),
            ).map_err(|e| DbError::Invalid(e.to_string()))
        });
        match result {
            Ok(result) => Ok(result),
            Err(DbError::SeedFallback(reason)) => self.full_model_page(request, reason.reason),
            Err(error) => Err(error),
        }
    }

    fn full_model_page(
        &self,
        request: &ModelPageRequest,
        reason: &'static str,
    ) -> Result<EpisodeModelPage, DbError> {
        let records = self.complete_read()?;
        let mut page = broca_core::read_session_model_page(
            &broca_core::StorePageSnapshot::empty(),
            &records,
            request,
            || Ok(records.clone()),
        )
        .map_err(|e| DbError::Invalid(e.to_string()))?;
        page.lineage_id = Some(self.publication.lineage().to_string());
        Ok(EpisodeModelPage {
            page,
            fallback: Some(reason),
        })
    }

    /// Resolve the episode and its seed within the same authenticated database snapshot.
    pub fn attach_start(
        &self,
        at: u64,
    ) -> Result<Option<(IndexedEpisode, Option<EpisodeSeed>)>, DbError> {
        let key = super::index::sid(&self.session)?;
        self.db.read_snapshot(|tx| {
            let lineage: String = tx.query_row("SELECT lineage_id FROM session WHERE sid=?1",[&key],|r|r.get(0))?;
            if lineage != self.publication.lineage().to_string() { return Err(bad("attach snapshot lineage mismatch")); }
            // Episode starts and historical seeds are immutable within a lineage. A newer
            // publication may change end ordinals or tags, neither of which attach consumes.
            let episodes = tx.prepare("SELECT run_started_seq,run_started_off,first_ord,end_ord FROM episode WHERE sid=?1 AND (run_started_seq IS NULL OR run_started_seq<=?2) ORDER BY n")?
                .query_map(params![key,self.sequence()],|r|Ok(IndexedEpisode{run_started_seq:r.get(0)?,run_started_off:r.get(1)?,first_ord:r.get(2)?,end_ord:r.get(3)?}))?.collect::<rusqlite::Result<Vec<_>>>()?;
            let Some(index) = broca_core::projection::episode_holding_seq(&episodes, at)
            else {
                return Ok(None);
            };
            let episode = episodes[index];
            let seed = episode
                .run_started_seq
                .map(|seq| self.seed(tx, &key, seq))
                .transpose()?
                .flatten();
            Ok(Some((episode, seed)))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::{ContentBlock, Message, Role};
    use broca_wal::{Wal, WalRecord};

    struct Fixture {
        root: std::path::PathBuf,
        db: Arc<OverhaulDb>,
        wal: broca_wal::FileWal,
        bind: BindIdentity,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    fn text(value: &str) -> Message {
        Message::new(Role::User, vec![ContentBlock::Text { text: value.into() }])
    }

    fn corpus(prefix: usize) -> Vec<WalRecord> {
        let bytes = std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../scripts/fixtures/resume-close-out-sealed.wal"),
        )
        .unwrap();
        let all = decode_positioned_frames(
            &|off, len| Ok(bytes[off as usize..(off + len) as usize].to_vec()),
            0,
            bytes.len() as u64,
            None,
        )
        .unwrap();
        let all: Vec<_> = all.into_iter().map(|p| p.framed.record).collect();
        let mut start = all
            .iter()
            .find(|r| matches!(r, WalRecord::RunStarted { .. }))
            .unwrap()
            .clone();
        if let WalRecord::RunStarted { input, .. } = &mut start {
            *input = vec![text("discarded-content-sentinel"); prefix].into();
        }
        let step = all
            .iter()
            .position(|r| matches!(r, WalRecord::StepStarted { .. }))
            .unwrap();
        let turn = all
            .iter()
            .position(|r| matches!(r, WalRecord::TurnFinished { .. }))
            .unwrap();
        let terminal = all
            .iter()
            .find(|r| matches!(r, WalRecord::RunFinished { .. }))
            .unwrap()
            .clone();
        let mut result = vec![start.clone()];
        result.extend_from_slice(&all[step..=turn]);
        result.push(terminal.clone());
        if let WalRecord::RunStarted { input, run_id, .. } = &mut start {
            *input = vec![text("current question")].into();
            *run_id = broca_protocol::RunId("current-episode".into());
        }
        result.push(start);
        result.extend_from_slice(&all[step..=turn]);
        result.push(terminal);
        result
    }

    impl Fixture {
        fn new(records: &[WalRecord]) -> Self {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let root = broca_test_support::scratch_root().join(format!(
                "episode-db-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&root).unwrap();
            std::fs::write(root.join("state-format"), b"broca-state/2\n").unwrap();
            let WalRecord::RunStarted { session, .. } = &records[0] else {
                unreachable!()
            };
            let bind = session.clone();
            let wal = broca_wal::FileWal::open(root.join("session.wal"))
                .unwrap()
                .with_feature_floor(root.join("wal-floor"))
                .with_wal_config(broca_wal::WalConfig::OVERHAUL)
                .unwrap();
            Self {
                db: Arc::new(OverhaulDb::open(&root).unwrap()),
                wal,
                bind,
                root,
            }
        }

        fn append(&mut self, record: &WalRecord) {
            self.wal.append(1, record, false).unwrap();
            self.wal.sync().unwrap();
            self.db
                .publish(
                    &self.bind,
                    "episode",
                    LogLocation::Live,
                    &self.wal.durable_publication().unwrap(),
                )
                .unwrap();
            assert_source_auth(self);
        }

        fn reader(&self) -> DatabaseEpisodeReader {
            self.db
                .episode_reader(self.bind.clone(), self.wal.durable_publication().unwrap())
                .unwrap()
        }

        fn full_page(&self, request: &ModelPageRequest) -> broca_core::ModelPage {
            let records = self.wal.replay().unwrap();
            let mut page = broca_core::read_session_model_page(
                &broca_core::StorePageSnapshot::empty(),
                &records,
                request,
                || Ok(records.clone()),
            )
            .unwrap();
            page.lineage_id = Some(
                self.wal
                    .durable_publication()
                    .unwrap()
                    .lineage()
                    .to_string(),
            );
            page
        }
    }

    fn assert_source_auth(fixture: &Fixture) {
        fixture
            .db
            .read_snapshot(|tx| {
                let encoded: String =
                    tx.query_row("SELECT locator_seed FROM session", [], |r| r.get(0))?;
                let offsets = OffsetSeed::decode(&encoded, None)
                    .unwrap()
                    .admission
                    .compaction;
                let rows: Vec<u64> = tx
                    .prepare("SELECT frame_off FROM source_auth ORDER BY frame_off")?
                    .query_map([], |r| r.get(0))?
                    .collect::<rusqlite::Result<_>>()?;
                assert_eq!(
                    offsets, rows,
                    "each published compaction offset has exactly one authentication row"
                );
                Ok(())
            })
            .unwrap();
    }

    fn page_value(page: &broca_core::ModelPage) -> serde_json::Value {
        serde_json::json!({"entries":page.messages,"next":page.next_from_ordinal,"head":page.head.map(|h|(h.wal_seq,h.sub_index)),"lineage":page.lineage_id,"compaction":page.compaction_id,"version":page.version,"groups":page.atomic_groups.iter().map(|g|(g.kind,g.from_ordinal,g.to_ordinal)).collect::<Vec<_>>()})
    }

    fn assert_page_read_plan(fixture: &Fixture, operation: &access_counter::OperationAccess) {
        use access_counter::{ReadClass as C, ValidationRead as V};
        let publication = fixture.wal.durable_publication().unwrap();
        let bytes = std::fs::read(publication.path()).unwrap();
        let mut at = 0;
        let mut frames = Vec::new();
        while at < bytes.len() {
            match broca_wal::framing::decode_next(&bytes[at..]) {
                broca_wal::framing::DecodeStep::Record { framed, consumed } => {
                    frames.push((at as u64, (at + consumed) as u64, framed.record));
                    at += consumed;
                }
                broca_wal::framing::DecodeStep::Lineage { consumed, .. } => at += consumed,
                _ => panic!("fixture WAL must be complete"),
            }
        }
        let boundaries: Vec<_> = frames
            .iter()
            .enumerate()
            .filter(|(_, (_, _, record))| matches!(record, WalRecord::TurnFinished { .. }))
            .collect();
        let (previous_index, previous) = boundaries[0];
        let (current_index, current) = boundaries[1];
        let last = frames.last().unwrap();
        let end = publication.byte_end();
        let mut expected = vec![
            ("snapshot cursor", last.0, end - last.0, C::Source),
            ("head tail", current.1, end - current.1, C::Source),
            (
                "head lineage gap",
                current.1,
                frames[current_index + 1].0 - current.1,
                C::Validation(V::LineageGap),
            ),
            (
                "head boundary",
                current.0,
                current.1 - current.0,
                C::Validation(V::BoundaryFrame),
            ),
            ("head lineage", 0, 69, C::LineageHeader),
            ("covering episode", previous.1, end - previous.1, C::Source),
            (
                "episode lineage gap",
                previous.1,
                frames[previous_index + 1].0 - previous.1,
                C::Validation(V::LineageGap),
            ),
            (
                "episode boundary",
                previous.0,
                previous.1 - previous.0,
                C::Validation(V::BoundaryFrame),
            ),
            ("episode lineage", 0, 69, C::LineageHeader),
        ];
        let compaction: Vec<_> = frames
            .iter()
            .filter(|(_, _, record)| {
                matches!(
                    record,
                    WalRecord::CompactionMessage { .. } | WalRecord::CompactionAnswer { .. }
                )
            })
            .collect();
        for source in &compaction {
            expected.push((
                "authenticated compaction source",
                source.0,
                source.1 - source.0,
                C::Source,
            ));
        }
        for read in &operation.reads {
            let index = expected
                .iter()
                .position(|(_, off, len, class)| {
                    (*off, *len, *class) == (read.offset, read.length, read.class)
                })
                .unwrap_or_else(|| panic!("unlisted read {read:?}; remaining {expected:?}"));
            expected.remove(index);
        }
        assert!(expected.is_empty(), "missing reads: {expected:?}");
        // Each compaction input adds one source decode and its record/lineage pair
        // within the covering episode range.
        assert_eq!(operation.frames_decoded, 37 + 3 * compaction.len() as u64);
    }

    fn setup(to: u64) -> WalRecord {
        let replacement = Arc::new(vec![text("compacted summary")]);
        WalRecord::CompactionMessage {
            provider: "compactor".into(),
            kind: broca_wal::CompactionMessageKind::Setup,
            compaction_id: "page-setup".into(),
            version: 1,
            range: broca_wal::CompactionRange { from: 1, to },
            sha256: broca_wal::compaction_replacement_sha256(&replacement).unwrap(),
            replacement,
            request_id: None,
            cursor: None,
            call_when: None,
            stability: None,
            ceiling: None,
            tag_checkpoint: None,
            ride_wait_ms: None,
            view_features: None,
            view_due: None,
        }
    }

    fn with_noop_compaction(mut records: Vec<WalRecord>) -> Vec<WalRecord> {
        let setup = setup(3);
        let answer = WalRecord::CompactionAnswer {
            provider: "compactor".into(),
            request_id: "no-pending-request".into(),
            cursor: broca_wal::CompactionCursor {
                lineage_id: "unused-lineage".into(),
                after_ordinal: Some(3),
            },
            disposition: broca_wal::CompactionDisposition::Late,
            rejected: None,
            wait: None,
        };
        let mut fold = broca_core::compaction::CompactionCheckpoint::None;
        fold.fold(&setup);
        let before = fold.clone();
        fold.fold(&answer);
        fold.fold(&answer);
        assert_eq!(
            fold, before,
            "late answers do not change the rendered compaction"
        );
        let at = records
            .iter()
            .enumerate()
            .filter(|(_, r)| matches!(r, WalRecord::RunStarted { .. }))
            .nth(1)
            .unwrap()
            .0;
        records.splice(at..at, [setup, answer.clone(), answer]);
        records
    }

    #[test]
    fn database_model_pages_equal_full_replay_at_every_cut_and_later_setup() {
        let mut records = corpus(6);
        for record in &mut records {
            if let WalRecord::RunStarted { config, .. } = record {
                config.render_tags = true;
                config.computed_tags = Some(broca_wal::ComputedTagRules { checkpoint: 0 });
            }
        }
        if let Some(WalRecord::RunStarted { input, config, .. }) = records
            .iter_mut()
            .filter(|r| matches!(r, WalRecord::RunStarted { .. }))
            .nth(1)
        {
            Arc::make_mut(input)[0]
                .content
                .push(ContentBlock::Media(broca_protocol::MediaRef {
                    kind: broca_protocol::MediaKind::Image,
                    mime: "image/png".into(),
                    byte_size: 69,
                    sha256: "a".repeat(64),
                    width: Some(1),
                    height: Some(1),
                }));
            config.frozen_render_config["media"] = serde_json::json!({"images":{"max":4}});
        }
        records.push(setup(3));
        if let WalRecord::CompactionMessage { tag_checkpoint, .. } = records.last_mut().unwrap() {
            *tag_checkpoint = Some(broca_wal::SetupTagCheckpoint {
                tag_checkpoint: 50,
                cut: "m4".into(),
            });
        }
        let mut later = records[0].clone();
        if let WalRecord::RunStarted { input, run_id, .. } = &mut later {
            *input = vec![text("after Setup")].into();
            *run_id = broca_protocol::RunId("after-setup".into());
        }
        records.push(later);
        records.push(
            records
                .iter()
                .find(|r| matches!(r, WalRecord::RunFinished { .. }))
                .unwrap()
                .clone(),
        );
        let mut fixture = Fixture::new(&records);
        for (cut, record) in records.iter().enumerate() {
            fixture.append(record);
            for from in [None, Some(0), Some(3), Some(6), Some(8)] {
                let request = ModelPageRequest {
                    from_ordinal: from,
                    limit: Some(2),
                    ..Default::default()
                };
                let actual = fixture.reader().read_model_page(&request).unwrap();
                if cut >= records.len() - 3 {
                    assert_eq!(actual.fallback, None, "cut {cut},from {from:?}");
                }
                assert_eq!(
                    page_value(&actual.page),
                    page_value(&fixture.full_page(&request)),
                    "cut {cut}, from {from:?}, fallback {:?}",
                    actual.fallback
                );
            }
        }
    }

    #[test]
    fn database_model_page_reads_are_independent_of_discarded_prefix() {
        for compaction in [false, true] {
            let mut measurements = Vec::new();
            for prefix in [1_000, 150_000] {
                let records = if compaction {
                    with_noop_compaction(corpus(prefix))
                } else {
                    corpus(prefix)
                };
                let mut fixture = Fixture::new(&records);
                for record in &records {
                    fixture.append(record);
                }
                let reader = fixture.reader();
                let request = ModelPageRequest {
                    limit: Some(2),
                    ..Default::default()
                };
                let counter = access_counter::WalAccessCounter::default();
                let actual = counter
                    .operation("model page")
                    .run(|| reader.read_model_page(&request).unwrap());
                let counts = counter.snapshot().remove(0);
                assert_page_read_plan(&fixture, &counts);
                assert!(actual.fallback.is_none(), "{:?}", actual.fallback);
                assert_eq!(
                    page_value(&actual.page),
                    page_value(&fixture.full_page(&request))
                );
                assert!(counts.seed_fallbacks.is_empty(), "{counts:?}");
                assert!(
                    counts.reads.iter().all(|r| r.offset != 0 || r.length <= 69),
                    "{counts:?}"
                );
                let sizes: Vec<u64> = fixture
                    .db
                    .read_snapshot(|tx| {
                        Ok(tx
                            .prepare("SELECT length(continuation) FROM episode_seed ORDER BY seq")?
                            .query_map([], |r| r.get(0))?
                            .collect::<rusqlite::Result<_>>()?)
                    })
                    .unwrap();
                eprintln!(
                "compaction={compaction}, prefix={prefix}, stored boundary bytes={sizes:?}, total={}",
                sizes.iter().sum::<u64>()
            );
                measurements.push((
                    counts.reads.len(),
                    counts.frames_decoded,
                    counts.reads.iter().map(|r| r.length).sum::<u64>(),
                ));
            }
            assert_eq!(measurements[0], measurements[1]);
            eprintln!("bounded model-page reads/decodes/bytes: {measurements:?}");
        }
    }

    #[test]
    fn database_model_page_corrupt_historical_locator_falls_back_by_name() {
        for (change, expected) in [
            ("boundary", "historical episode boundary missing"),
            (
                "offset",
                "historical episode continuation boundary mismatch",
            ),
            ("version", "historical episode continuation unsupported"),
            ("missing", "historical episode continuation missing"),
        ] {
            let records = corpus(8);
            let mut fixture = Fixture::new(&records);
            for record in &records {
                fixture.append(record);
            }
            fixture
                .db
                .transaction(false, |tx| {
                    let (seq, encoded): (u64, String) = tx.query_row(
                        "SELECT seq,continuation FROM episode_seed ORDER BY seq LIMIT 1",
                        [],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )?;
                    let mut value: serde_json::Value = serde_json::from_str(&encoded).unwrap();
                    match change {
                        "boundary" => {
                            tx.execute("DELETE FROM projection_boundary WHERE seq=?1", [seq])?;
                            return Ok(());
                        }
                        "offset" => {
                            value["admission"]["state"]["boundary"]["off"] = serde_json::json!(1)
                        }
                        "version" => value["version"] = serde_json::json!(0),
                        "missing" => {
                            tx.execute("DELETE FROM episode_seed WHERE seq=?1", [seq])?;
                            return Ok(());
                        }
                        _ => unreachable!(),
                    }
                    tx.execute(
                        "UPDATE episode_seed SET continuation=?2 WHERE seq=?1",
                        params![seq, value.to_string()],
                    )?;
                    Ok(())
                })
                .unwrap();
            let request = ModelPageRequest {
                limit: Some(2),
                ..Default::default()
            };
            let reader = fixture.reader();
            let counter = access_counter::WalAccessCounter::default();
            let actual = counter
                .operation(change)
                .run(|| reader.read_model_page(&request).unwrap());
            assert_eq!(actual.fallback, Some(expected));
            assert_eq!(counter.snapshot()[0].seed_fallbacks, [expected]);
            assert_eq!(
                counter.snapshot()[0]
                    .reads
                    .iter()
                    .filter(|r| r.class == access_counter::ReadClass::Origin)
                    .count(),
                1
            );
            assert_eq!(
                page_value(&actual.page),
                page_value(&fixture.full_page(&request))
            );
        }
    }

    #[test]
    fn database_episode_boundaries_and_content_free_seeds_commit_atomically() {
        let records = corpus(8);
        let mut fixture = Fixture::new(&records);
        let second = records
            .iter()
            .enumerate()
            .filter(|(_, r)| matches!(r, WalRecord::RunStarted { .. }))
            .nth(1)
            .unwrap()
            .0;
        for record in &records[..second] {
            fixture.append(record);
        }
        let before = fixture
            .db
            .read_snapshot(|tx| {
                Ok((
                    tx.query_row("SELECT count(*) FROM projection_boundary", [], |r| {
                        r.get::<_, u64>(0)
                    })?,
                    tx.query_row("SELECT count(*) FROM episode_seed", [], |r| {
                        r.get::<_, u64>(0)
                    })?,
                    tx.query_row("SELECT indexed_seq FROM session", [], |r| {
                        r.get::<_, u64>(0)
                    })?,
                ))
            })
            .unwrap();
        fixture.db.transaction(false,|tx|{tx.execute_batch("CREATE TRIGGER abort_episode_seed BEFORE INSERT ON episode_seed BEGIN SELECT RAISE(ABORT,'seed commit interrupted'); END;")?;Ok(())}).unwrap();
        for record in &records[second..] {
            fixture.wal.append(1, record, false).unwrap();
        }
        fixture.wal.sync().unwrap();
        assert!(fixture
            .db
            .publish(
                &fixture.bind,
                "episode",
                LogLocation::Live,
                &fixture.wal.durable_publication().unwrap()
            )
            .is_err());
        let after = fixture
            .db
            .read_snapshot(|tx| {
                Ok((
                    tx.query_row("SELECT count(*) FROM projection_boundary", [], |r| {
                        r.get::<_, u64>(0)
                    })?,
                    tx.query_row("SELECT count(*) FROM episode_seed", [], |r| {
                        r.get::<_, u64>(0)
                    })?,
                    tx.query_row("SELECT indexed_seq FROM session", [], |r| {
                        r.get::<_, u64>(0)
                    })?,
                ))
            })
            .unwrap();
        assert_eq!(after, before);
        fixture
            .db
            .transaction(false, |tx| {
                tx.execute_batch("DROP TRIGGER abort_episode_seed")?;
                Ok(())
            })
            .unwrap();
        let _ = fixture.reader();
        fixture.db.read_snapshot(|tx|{
            let missing:u64=tx.query_row("SELECT count(*) FROM projection_boundary b LEFT JOIN episode_seed s USING(sid,seq) WHERE b.qualified=1 AND s.seq IS NULL",[],|r|r.get(0))?;
            assert_eq!(missing,0);
            let continuations=tx.prepare("SELECT continuation FROM episode_seed")?.query_map([],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
            for encoded in continuations {
                for sentinel in ["discarded-content-sentinel","current question","reasoning"] {
                    assert!(!encoded.contains(sentinel),"persisted content {sentinel}");
                }
                ProjectionContinuation::decode(&encoded).unwrap();
            }
            Ok(())
        }).unwrap();
    }

    #[test]
    fn database_compaction_locators_and_authentication_commit_at_the_same_cut() {
        let records = with_noop_compaction(corpus(8));
        let mut fixture = Fixture::new(&records);
        let first_compaction = records
            .iter()
            .position(|r| matches!(r, WalRecord::CompactionMessage { .. }))
            .unwrap();
        for record in &records[..first_compaction] {
            fixture.append(record);
        }
        let before: String = fixture
            .db
            .read_snapshot(|tx| {
                Ok(tx.query_row("SELECT locator_seed FROM session", [], |r| r.get(0))?)
            })
            .unwrap();
        fixture.db.transaction(false,|tx|{tx.execute_batch("CREATE TRIGGER abort_source_auth BEFORE INSERT ON source_auth BEGIN SELECT RAISE(ABORT,'source authentication interrupted'); END;")?;Ok(())}).unwrap();
        for record in &records[first_compaction..] {
            fixture.wal.append(1, record, false).unwrap();
        }
        fixture.wal.sync().unwrap();
        assert!(fixture
            .db
            .publish(
                &fixture.bind,
                "episode",
                LogLocation::Live,
                &fixture.wal.durable_publication().unwrap()
            )
            .is_err());
        let after: String = fixture
            .db
            .read_snapshot(|tx| {
                Ok(tx.query_row("SELECT locator_seed FROM session", [], |r| r.get(0))?)
            })
            .unwrap();
        assert_eq!(after, before);
        assert_source_auth(&fixture);
        fixture
            .db
            .transaction(false, |tx| {
                tx.execute_batch("DROP TRIGGER abort_source_auth")?;
                Ok(())
            })
            .unwrap();
        let _ = fixture.reader();
        assert_source_auth(&fixture);
    }

    #[test]
    fn database_model_compaction_locators_fail_closed_and_old_indexes_fall_back() {
        for (change, reason) in [
            ("offset", "model compaction source authentication missing"),
            ("hash", "model compaction source identity mismatch"),
            (
                "old index",
                "model compaction source authentication missing",
            ),
        ] {
            let records = with_noop_compaction(corpus(8));
            let mut fixture = Fixture::new(&records);
            for record in &records {
                fixture.append(record);
            }
            fixture.db.transaction(false,|tx|{
                match change {
                    "offset"=>{
                        let encoded:String=tx.query_row("SELECT locator_seed FROM session",[],|r|r.get(0))?;
                        let mut value:serde_json::Value=serde_json::from_str(&encoded).unwrap();
                        value[1]["admission"]["compaction"][0]=serde_json::json!(1);
                        tx.execute("UPDATE session SET locator_seed=?1",[value.to_string()])?;
                    }
                    "hash"=>{tx.execute("UPDATE source_auth SET frame_identity=zeroblob(32) WHERE frame_off=(SELECT min(frame_off) FROM source_auth)",[])?;}
                    "old index"=>tx.execute_batch("DROP TABLE source_auth")?,
                    _=>unreachable!(),
                }
                Ok(())
            }).unwrap();
            if change == "old index" {
                fixture.db = Arc::new(OverhaulDb::open(&fixture.root).unwrap());
            }
            let reader = fixture.reader();
            let counter = access_counter::WalAccessCounter::default();
            let request = ModelPageRequest {
                limit: Some(2),
                ..Default::default()
            };
            let actual = counter
                .operation(change)
                .run(|| reader.read_model_page(&request).unwrap());
            assert_eq!(actual.fallback, Some(reason));
            assert_eq!(counter.snapshot()[0].seed_fallbacks, [reason]);
            assert_eq!(
                counter.snapshot()[0]
                    .reads
                    .iter()
                    .filter(|r| r.class == access_counter::ReadClass::Origin)
                    .count(),
                1
            );
            assert_eq!(
                page_value(&actual.page),
                page_value(&fixture.full_page(&request))
            );
        }
    }

    #[test]
    fn database_episode_tables_are_never_created_on_legacy_roots() {
        let fixture = Fixture::new(&corpus(1));
        let legacy = fixture.root.join("legacy");
        std::fs::create_dir_all(&legacy).unwrap();
        assert!(OverhaulDb::open(&legacy).is_err());
        assert!(!legacy.join("broca.db").exists());
        assert!(!legacy.join("run-index.db").exists());
    }

    #[test]
    fn database_model_page_fallback_refuses_a_recreated_log() {
        let records = corpus(8);
        let mut original = Fixture::new(&records);
        let mut replacement = Fixture::new(&records);
        for record in &records {
            original.append(record);
            replacement.append(record);
        }
        let reader = original.reader();
        let original_publication = original.wal.durable_publication().unwrap();
        let replacement_publication = replacement.wal.durable_publication().unwrap();
        assert_ne!(
            original_publication.lineage(),
            replacement_publication.lineage()
        );
        std::fs::write(
            original_publication.path(),
            std::fs::read(replacement_publication.path()).unwrap(),
        )
        .unwrap();
        let request = ModelPageRequest {
            limit: Some(2),
            ..Default::default()
        };
        assert!(
            matches!(reader.read_model_page(&request),Err(DbError::Invalid(reason)) if reason=="episode publication lineage changed")
        );
    }
}
