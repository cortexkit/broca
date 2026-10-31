//! Transient request hydration from the content-free index and a synced WAL witness.

use std::collections::BTreeMap;
use std::sync::Arc;

use broca_core::context::{HeldEpisode, OffsetGuidanceCheckpoint, OffsetPromptWindow};
use broca_core::projection_plan::{OffsetSource, ProjectionContinuation};
use broca_core::replay::{PositionedFrame, SeedBoundary, SeedFallback};
use broca_core::resume::{OffsetResumeReader, OffsetResumeSnapshot};
use broca_protocol::{BindIdentity, Message, Role};
use broca_wal::{access_counter, file_wal::DurablePublication, framing::DecodeStep, WalRecord};
use rusqlite::{params, OptionalExtension, Transaction};

use super::OverhaulDb;

impl OverhaulDb {
    /// Build a session-local resume reader. The witness bounds every read, even if the
    /// writer publishes another commit while provider credentials are being resolved.
    pub fn resume_reader(
        self: &Arc<Self>,
        session: BindIdentity,
        publication: DurablePublication,
    ) -> Arc<dyn OffsetResumeReader> {
        Arc::new(DatabaseResumeReader {
            db: self.clone(),
            session,
            publication,
        })
    }
}

struct DatabaseResumeReader {
    db: Arc<OverhaulDb>,
    session: BindIdentity,
    publication: DurablePublication,
}

fn bad(reason: &'static str) -> SeedFallback {
    SeedFallback::new(reason)
}

impl OffsetResumeReader for DatabaseResumeReader {
    fn snapshot(&self, safe_head: u64) -> Result<OffsetResumeSnapshot, SeedFallback> {
        if safe_head != self.publication.sequence() {
            return Err(bad("resume publication does not name durable head"));
        }
        self.db
            .read_snapshot(|tx| snapshot(tx, self, safe_head).map_err(super::DbError::SeedFallback))
            .map_err(|error| match error {
                super::DbError::SeedFallback(reason) => reason,
                _ => bad("resume database snapshot unavailable"),
            })
    }

    fn read_at(&self, off: u64, len: u64) -> std::io::Result<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = broca_wal::fs_ops::File::open(self.publication.path())?;
        file.seek(SeekFrom::Start(off))?;
        let mut bytes = Vec::new();
        file.take(len.min(self.publication.byte_end().saturating_sub(off)))
            .read_to_end(&mut bytes)?;
        Ok(bytes)
    }
}

fn snapshot(
    tx: &Transaction<'_>,
    reader: &DatabaseResumeReader,
    safe_head: u64,
) -> Result<OffsetResumeSnapshot, SeedFallback> {
    let sql = |_| bad("resume index rows unavailable");
    let key =
        super::index::sid(&reader.session).map_err(|_| bad("resume session identity invalid"))?;
    let row: Option<(String, String, u64)> = tx
        .query_row(
            "SELECT tail_seed,lineage_id,indexed_seq FROM session WHERE sid=?1",
            [&key],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(sql)?;
    let (encoded, lineage, indexed) = row.ok_or_else(|| bad("resume session index missing"))?;
    let stored = ProjectionContinuation::decode(&encoded)?;
    let boundary = stored.boundary();
    let indexed_boundary = tx.query_row(
        "SELECT lineage_id,seq,frame_off,frame_end,frame_identity,qualified FROM projection_boundary WHERE sid=?1 AND seq=?2",
        params![key, boundary.seq],
        |r| Ok(SeedBoundary { lineage_id: r.get(0)?, seq: r.get(1)?, frame_off: r.get(2)?, frame_end: r.get(3)?, frame_identity: r.get(4)?, qualified: r.get(5)? }),
    ).optional().map_err(sql)?;
    if indexed_boundary.as_ref() != Some(&boundary)
        || !boundary.qualified
        || boundary.lineage_id.as_deref() != Some(lineage.as_str())
        || lineage != reader.publication.lineage().to_string()
        || boundary.seq > indexed
        || indexed > safe_head
        || boundary.frame_end > reader.publication.byte_end()
    {
        return Err(bad("resume seed and published boundary mismatch"));
    }
    let read = |off, len| reader.read_at(off, len);
    let tail = if boundary.seq == safe_head {
        Vec::new()
    } else {
        let bytes = access_counter::read_at(
            &read,
            boundary.frame_end,
            reader.publication.byte_end() - boundary.frame_end,
        )
        .map_err(|_| bad("resume tail unavailable"))?;
        decode(&bytes, boundary.frame_end)?
    };
    let projection = stored.hydrate(&read, &tail, safe_head)?;
    let mut ranges: Vec<_> = std::iter::once(0..projection.prefix_len).collect();
    let mut window = None;
    if let Some(compaction) = projection
        .admission_checkpoint
        .compaction
        .as_ref()
        .and_then(|c| c.state())
    {
        use broca_core::compaction_provider::view::ViewRange;
        let base = if let Some(view) = &compaction.view {
            ranges = view
                .view
                .ranges
                .iter()
                .filter_map(|r| match r {
                    ViewRange::Keep { from, to, .. } => Some(*from..*to),
                    _ => None,
                })
                .collect();
            view.view
                .covers_through
                .ordinal
                .checked_add(1)
                .ok_or_else(|| bad("resume window ordinal overflow"))?
        } else if let Some(applied) = &compaction.applied {
            ranges.clear();
            applied.range.to
        } else {
            return Err(bad("resume window has no applied compaction"));
        };
        let systems = stored
            .system_ordinals()
            .ok_or_else(|| bad("resume system coordinates missing"))?
            .to_vec();
        ranges.push(base..projection.prefix_len);
        ranges.extend(systems.iter().map(|o| *o..*o + 1));
        if projection.prefix_len != 0 {
            ranges.push(projection.prefix_len - 1..projection.prefix_len);
        }
        window = Some(OffsetPromptWindow {
            base,
            systems,
            episodes: projection
                .projection_episodes
                .as_ref()
                .ok_or_else(|| bad("resume episode coordinates missing"))?
                .iter()
                .enumerate()
                .map(|(i, e)| HeldEpisode {
                    projection: e.clone(),
                    next_start: projection
                        .projection_episodes
                        .as_ref()
                        .unwrap()
                        .get(i + 1)
                        .and_then(|e| e.start()),
                })
                .collect(),
            guidance: Vec::new(),
        });
    }
    let mut sources = BTreeMap::new();
    let mut messages = BTreeMap::new();
    for range in ranges {
        load_rows(
            tx,
            &key,
            &read,
            &projection,
            range,
            &mut sources,
            &mut messages,
        )?;
    }
    if let Some(window) = &mut window {
        // Historical gaps use content-free repetition state, never hidden message
        // sources. Only the current episode may be hydrated as a complete range.
        for (i, episode) in window.episodes.iter().enumerate() {
            let Some(first) = episode.projection.first_step().flatten() else {
                continue;
            };
            let end = episode.next_start.unwrap_or(projection.prefix_len);
            if episode.next_start.is_some() {
                for (ord, _) in messages
                    .range(first..end)
                    .filter(|(_, m)| m.role != Role::Tool)
                {
                    window.guidance.push(OffsetGuidanceCheckpoint {
                        ordinal: *ord,
                        at_seq: boundary.seq,
                        fold: stored.repetition_checkpoint(i, *ord)?,
                    });
                }
                continue;
            }
            stored.repetition_checkpoint(i, end)?;
            let mut steps = BTreeMap::new();
            load_rows(
                tx,
                &key,
                &read,
                &projection,
                first..end,
                &mut sources,
                &mut steps,
            )?;
            let mut fold = broca_core::tool_repetition::RepetitionFold::default();
            for (ord, message) in steps {
                if let Some(checkpoint) = fold.checkpoint() {
                    window.guidance.push(OffsetGuidanceCheckpoint {
                        ordinal: ord,
                        at_seq: boundary.seq,
                        fold: checkpoint,
                    });
                }
                fold.push(&message);
            }
            window.guidance.push(OffsetGuidanceCheckpoint {
                ordinal: end,
                at_seq: boundary.seq,
                fold: fold
                    .checkpoint()
                    .ok_or_else(|| bad("resume repetition state is ambiguous"))?,
            });
        }
        window.guidance.sort_by_key(|g| g.ordinal);
        window.guidance.dedup_by_key(|g| g.ordinal);
    }
    Ok(OffsetResumeSnapshot {
        boundary,
        projection,
        window,
        prefix: messages.into_values().collect(),
        tail,
    })
}

fn decode(bytes: &[u8], off: u64) -> Result<Vec<PositionedFrame>, SeedFallback> {
    let mut at = 0;
    let mut frames: Vec<PositionedFrame> = Vec::new();
    while at < bytes.len() {
        match access_counter::decode_next(&bytes[at..]) {
            DecodeStep::Record { framed, consumed } => {
                if matches!(framed.record, WalRecord::Unknown) {
                    return Err(bad("resume source contains an unknown record"));
                }
                if frames
                    .last()
                    .is_some_and(|p| p.framed.seq.checked_add(1) != Some(framed.seq))
                {
                    return Err(bad("resume source sequence gap"));
                }
                frames.push(PositionedFrame {
                    off: off + at as u64,
                    framed: *framed,
                });
                at += consumed;
            }
            DecodeStep::Lineage { consumed, .. } => at += consumed,
            _ => return Err(bad("resume source is not a complete frame range")),
        }
    }
    Ok(frames)
}

type ReadAt<'a> = dyn Fn(u64, u64) -> std::io::Result<Vec<u8>> + 'a;
type FrameCache = BTreeMap<(u64, u64), Vec<PositionedFrame>>;

fn load_rows(
    tx: &Transaction<'_>,
    key: &str,
    read: &ReadAt<'_>,
    projection: &broca_core::replay::ProjectionSeed,
    range: std::ops::Range<u64>,
    cache: &mut FrameCache,
    messages: &mut BTreeMap<u64, Message>,
) -> Result<(), SeedFallback> {
    let sql = |_| bad("resume message rows unavailable");
    if range.start > range.end || range.end > projection.prefix_len {
        return Err(bad("resume selected range outside prefix"));
    }
    let rows = tx.prepare("SELECT ord,lo_seq,lo_off,hi_off,input_start,input_end,tag_base,tag_n,tag_high,flags FROM message WHERE sid=?1 AND ord>=?2 AND ord<?3 ORDER BY ord").map_err(sql)?
        .query_map(params![key, range.start, range.end], |r| Ok((r.get::<_,u64>(0)?, OffsetSource {
            lo_seq: r.get(1)?, lo_off: r.get(2)?, hi_off: r.get(3)?, input_start: r.get(4)?, input_end: r.get(5)?, tag_base: r.get(6)?, tag_n: r.get(7)?, tag_high: r.get(8)?, flags: r.get(9)?,
        }))).map_err(sql)?.collect::<rusqlite::Result<Vec<_>>>().map_err(sql)?;
    if rows.len() as u64 != range.end - range.start {
        return Err(bad("resume message ordinal gap"));
    }
    for (ordinal, source) in rows {
        if messages.contains_key(&ordinal) {
            continue;
        }
        if source.input_start.checked_add(1) != Some(source.input_end)
            || source.lo_seq > projection.watermark_seq
            || source.hi_off <= source.lo_off
        {
            return Err(bad("resume message locator invalid"));
        }
        let frames = match cache.entry((source.lo_off, source.hi_off)) {
            std::collections::btree_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::btree_map::Entry::Vacant(e) => {
                let bytes =
                    access_counter::read_at(read, source.lo_off, source.hi_off - source.lo_off)
                        .map_err(|_| bad("resume message source unavailable"))?;
                if bytes.len() as u64 != source.hi_off - source.lo_off {
                    return Err(bad("resume message source is short"));
                }
                e.insert(decode(&bytes, source.lo_off)?)
            }
        };
        let first = frames
            .first()
            .filter(|p| p.framed.seq == source.lo_seq)
            .ok_or_else(|| bad("resume message source sequence mismatch"))?;
        let input = match &first.framed.record {
            WalRecord::RunStarted { input, .. }
            | WalRecord::Steered { input, .. }
            | WalRecord::SendDelivered {
                input: Some(input), ..
            } => Some(input),
            _ => None,
        };
        let mut message = if let Some(input) = input {
            let mut m = input
                .get(source.input_start as usize)
                .cloned()
                .ok_or_else(|| bad("resume input sub-index invalid"))?;
            broca_core::tags::attach(
                &mut m,
                first
                    .framed
                    .record
                    .runner_tags()
                    .get(source.input_start as usize),
            );
            m
        } else {
            let replay = broca_core::replay(
                &frames
                    .iter()
                    .map(|p| p.framed.record.clone())
                    .collect::<Vec<_>>(),
            )
            .map_err(|_| bad("resume turn source cannot replay"))?;
            replay
                .prompt
                .get(source.input_start as usize)
                .cloned()
                .ok_or_else(|| bad("resume turn sub-index invalid"))?
        };
        if message.role == Role::Assistant && message.origin.is_none() {
            let off: Option<u64> = tx.query_row(
                "SELECT run_started_off FROM episode WHERE sid=?1 AND first_ord<=?2 AND end_ord>?2 ORDER BY n DESC LIMIT 1",
                params![key,ordinal], |r| r.get(0),
            ).map_err(sql)?;
            if let Some(off) = off {
                message.origin = Some(episode_model(tx, key, read, off, cache)?);
            }
        }
        if source.flags & 2 != 0 {
            let mut high = source
                .tag_base
                .ok_or_else(|| bad("resume computed tag base missing"))?;
            broca_protocol::tags::number_message(
                &mut message,
                broca_protocol::tags::ComputedRules {
                    render: source.flags & 4 != 0,
                    clean: source.flags & 8 != 0,
                },
                &mut high,
            )
            .map_err(|_| bad("resume tag numbering overflow"))?;
            if high != source.tag_high {
                return Err(bad("resume tag count mismatch"));
            }
        }
        message.ordinal = Some(ordinal);
        messages.insert(ordinal, message);
    }
    Ok(())
}

fn episode_model(
    tx: &Transaction<'_>,
    key: &str,
    read: &ReadAt<'_>,
    off: u64,
    cache: &mut FrameCache,
) -> Result<broca_protocol::ModelHandle, SeedFallback> {
    let end: u64 = tx
        .query_row(
            "SELECT frame_end FROM projection_boundary WHERE sid=?1 AND frame_off=?2",
            params![key, off],
            |r| r.get(0),
        )
        .map_err(|_| bad("resume episode source missing"))?;
    if let std::collections::btree_map::Entry::Vacant(entry) = cache.entry((off, end)) {
        let bytes = access_counter::read_at(
            read,
            off,
            end.checked_sub(off)
                .ok_or_else(|| bad("resume episode source invalid"))?,
        )
        .map_err(|_| bad("resume episode source unavailable"))?;
        entry.insert(decode(&bytes, off)?);
    }
    match cache[&(off, end)].as_slice() {
        [PositionedFrame {
            framed:
                broca_wal::FramedRecord {
                    record: WalRecord::RunStarted { config, .. },
                    ..
                },
            ..
        }] => Ok(config.model.clone()),
        _ => Err(bad("resume episode source is not RunStarted")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_core::FileWalStore;
    use broca_protocol::{CallOptions, ContentBlock};
    use broca_wal::Wal;

    struct Scratch(std::path::PathBuf);
    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    struct Fixture {
        db: Arc<OverhaulDb>,
        wal: Box<dyn Wal + Send>,
        bind: BindIdentity,
        _scratch: Scratch,
    }

    impl Fixture {
        fn new() -> Self {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let root = broca_test_support::scratch_root().join(format!(
                "resume-db-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&root).unwrap();
            std::fs::write(root.join("state-format"), b"broca-state/2\n").unwrap();
            let store = FileWalStore::with_config(
                root.join("wal"),
                broca_wal::classify_state_root(&root).unwrap(),
            );
            let WalRecord::RunStarted { session, .. } = &corpus(false)[0] else {
                unreachable!()
            };
            let bind = session.clone();
            let wal = Box::new(
                broca_wal::FileWal::open(store.wal_path(&bind))
                    .unwrap()
                    .with_feature_floor(root.join("wal-floor"))
                    .with_wal_config(broca_wal::WalConfig::OVERHAUL)
                    .unwrap(),
            );
            Self {
                db: Arc::new(OverhaulDb::open(&root).unwrap()),
                wal,
                bind,
                _scratch: Scratch(root),
            }
        }

        fn append(&mut self, records: &[WalRecord], publish: bool) {
            for record in records {
                self.wal.append(1, record, false).unwrap();
            }
            self.wal.sync().unwrap();
            if publish {
                self.publish();
            }
        }

        fn publish(&self) {
            self.db
                .publish(
                    &self.bind,
                    "s-resume",
                    super::super::LogLocation::Live,
                    &self.wal.durable_publication().unwrap(),
                )
                .unwrap();
        }

        fn load(&self) -> broca_core::resume::OffsetResumeState {
            let publication = self.wal.durable_publication().unwrap();
            let head = publication.sequence();
            let reader = self.db.resume_reader(self.bind.clone(), publication);
            let snapshot = reader.snapshot(head).unwrap_or_else(|error| {
                let encoded: String = self
                    .db
                    .read_snapshot(|tx| {
                        Ok(tx.query_row("SELECT tail_seed FROM session", [], |r| r.get(0))?)
                    })
                    .unwrap();
                panic!("{error:?}: {encoded}");
            });
            broca_core::resume::reconstruct_from_offsets(
                &|off, len| reader.read_at(off, len),
                snapshot,
                head,
            )
            .unwrap()
        }
    }

    fn text(role: Role, value: &str) -> Message {
        Message::new(role, vec![ContentBlock::Text { text: value.into() }])
    }

    fn golden() -> Vec<WalRecord> {
        let bytes = std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../scripts/fixtures/resume-close-out-sealed.wal"),
        )
        .unwrap();
        let mut at = 0;
        let mut records = Vec::new();
        while at < bytes.len() {
            match broca_wal::framing::decode_next(&bytes[at..]) {
                DecodeStep::Record { framed, consumed } => {
                    records.push(framed.record);
                    at += consumed;
                }
                DecodeStep::Lineage { consumed, .. } => at += consumed,
                _ => panic!("golden must decode"),
            }
        }
        records
    }

    fn corpus(media: bool) -> Vec<WalRecord> {
        let source = golden();
        let mut start = source
            .iter()
            .find(|r| matches!(r, WalRecord::RunStarted { .. }))
            .unwrap()
            .clone();
        if let WalRecord::RunStarted { input, config, .. } = &mut start {
            *input = vec![text(Role::System, "system"), text(Role::User, "question")].into();
            if media {
                Arc::make_mut(input)[1].content.push(ContentBlock::Media(
                    broca_protocol::MediaRef {
                        kind: broca_protocol::MediaKind::Image,
                        mime: "image/png".into(),
                        byte_size: 69,
                        sha256: "a".repeat(64),
                        width: Some(1),
                        height: Some(1),
                    },
                ));
                config.frozen_render_config["media"] = serde_json::json!({"images":{"max":4}});
            }
        }
        let step = source
            .iter()
            .position(|r| matches!(r, WalRecord::StepStarted { .. }))
            .unwrap();
        let turn = source
            .iter()
            .position(|r| matches!(r, WalRecord::TurnFinished { .. }))
            .unwrap();
        let terminal = source
            .iter()
            .find(|r| matches!(r, WalRecord::RunFinished { .. }))
            .unwrap()
            .clone();
        let mut records = vec![start];
        records.extend_from_slice(&source[step..=turn]);
        records.push(terminal);
        records
    }

    fn setup(to: u64) -> WalRecord {
        let replacement = Arc::new(vec![text(Role::User, "summary")]);
        WalRecord::CompactionMessage {
            provider: "compactor".into(),
            kind: broca_wal::CompactionMessageKind::Setup,
            compaction_id: "resume-window".into(),
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
        }
    }

    fn full(fixture: &Fixture) -> broca_core::ReplayState {
        let wal =
            broca_wal::FileWal::open(fixture.wal.durable_publication().unwrap().path()).unwrap();
        let records: Vec<_> = wal
            .replay()
            .unwrap()
            .into_iter()
            .map(|f| f.record)
            .collect();
        broca_core::replay(&records).unwrap()
    }

    fn request(state: &broca_core::ReplayState, sparse: bool, full: bool) -> Vec<u8> {
        let mut options = CallOptions::new(state.prompt.clone());
        if full {
            for span in state.episode_step_spans.as_ref().unwrap() {
                broca_core::run::restore_repetition_guidance(&mut options.prompt[span.clone()]);
            }
        }
        options.render_episodes = Arc::new(state.render_episodes.clone().unwrap().into());
        if sparse {
            options =
                broca_core::compaction::model_options(&options, state.compaction.state().unwrap())
                    .unwrap();
        }
        broca_core::canonical_request_bytes(&options)
    }

    #[test]
    fn database_resume_request_bytes_at_every_dense_and_sparse_cut_including_media() {
        for media in [false, true] {
            for sparse in [false, true] {
                let mut fixture = Fixture::new();
                let records = corpus(media);
                let turn = records.len() - 2;
                let mut prefix = records[..=turn].to_vec();
                if sparse {
                    prefix.insert(turn, setup(4));
                }
                fixture.append(&prefix, true);
                let mut tails = vec![records.last().unwrap().clone()];
                let mut next = records.clone();
                if let WalRecord::RunStarted { run_id, input, .. } = &mut next[0] {
                    run_id.0.push_str("-next");
                    *input = vec![text(Role::User, "next")].into();
                }
                tails.extend(next);
                for cut in 0..=tails.len() {
                    if cut != 0 {
                        fixture.append(&tails[cut - 1..cut], false);
                    }
                    let loaded = fixture.load();
                    let full = full(&fixture);
                    assert_eq!(
                        request(&loaded.prior, sparse, false),
                        request(&full, sparse, true),
                        "media {media}, sparse {sparse}, cut {cut}"
                    );
                    assert_eq!(
                        loaded
                            .prior
                            .prompt
                            .iter()
                            .map(|m| m.ordinal)
                            .collect::<Vec<_>>(),
                        if sparse {
                            full.prompt
                                .iter()
                                .filter(|m| {
                                    m.role == Role::System
                                        || m.ordinal >= Some(4)
                                        || m.ordinal == Some(3)
                                })
                                .map(|m| m.ordinal)
                                .collect::<Vec<_>>()
                        } else {
                            full.prompt.iter().map(|m| m.ordinal).collect::<Vec<_>>()
                        }
                    );
                    assert_eq!(
                        loaded.snapshot.checkpoint_at_head.media,
                        broca_core::AdmissionCheckpointV1::from_frames(
                            &fixture.wal.replay().unwrap()
                        )
                        .unwrap()
                        .media
                    );
                }
                // A terminal publication still advances the durable database after offset reads.
                fixture.publish();
                let loaded = fixture.load();
                assert_eq!(
                    loaded.snapshot.head_seq,
                    fixture.wal.durable_publication().unwrap().sequence()
                );
                let (admission, projection): (String, String) = fixture
                    .db
                    .read_snapshot(|tx| {
                        Ok(tx.query_row(
                            "SELECT admission_seed,tail_seed FROM session",
                            [],
                            |r| Ok((r.get(0)?, r.get(1)?)),
                        )?)
                    })
                    .unwrap();
                assert!(!admission.contains("question") && !projection.contains("question"));
                assert!(!admission.contains("summary") && !projection.contains("summary"));
            }
        }
    }

    #[test]
    fn database_resume_window_reads_only_fixed_sources_at_1k_and_150k_messages() {
        use broca_wal::access_counter::{ReadClass, WalAccessCounter};
        let mut costs = Vec::new();
        for count in [1_000, 150_000] {
            let mut fixture = Fixture::new();
            let base = corpus(false);
            fixture.append(&base[..1], false);
            let steer: WalRecord = serde_json::from_value(serde_json::json!({
                "type":"steered", "input":vec![text(Role::User,"discarded payload");1000]
            }))
            .unwrap();
            for _ in 0..count / 1000 {
                fixture.append(std::slice::from_ref(&steer), false);
            }
            fixture.append(&base[1..], false);
            let mut current = base.clone();
            if let WalRecord::RunStarted { run_id, input, .. } = &mut current[0] {
                run_id.0.push_str("-current");
                *input = vec![text(Role::User, "current")].into();
            }
            let to = count as u64 + 7;
            current.insert(current.len() - 2, setup(to));
            fixture.append(&current[..current.len() - 1], true);
            fixture.append(&current[current.len() - 1..], false);
            let encoded: String = fixture
                .db
                .read_snapshot(|tx| {
                    Ok(tx.query_row("SELECT tail_seed FROM session", [], |r| r.get(0))?)
                })
                .unwrap();
            let value: serde_json::Value = serde_json::from_str(&encoded).unwrap();
            let sizes: Vec<_> = value["guidance"]
                .as_array()
                .unwrap()
                .iter()
                .map(|e| serde_json::to_vec(e).unwrap().len())
                .collect();
            eprintln!("resume {count}: repetition bytes per episode {sizes:?}, total {}; projection seed {} bytes",serde_json::to_vec(&value["guidance"]).unwrap().len(),encoded.len());
            let frames = fixture.wal.replay().unwrap();
            let counter = WalAccessCounter::default();
            let loaded = counter
                .operation(format!("resume {count} discarded messages"))
                .run(|| fixture.load());
            assert_eq!(
                request(&loaded.prior, true, false),
                request(&full(&fixture), true, true)
            );
            assert_eq!(
                loaded.prior.prompt.len(),
                2,
                "system and newest boundary anchor only"
            );
            let access = counter.snapshot().remove(0);
            assert!(access.seed_fallbacks.is_empty());
            assert!(access.reads.iter().all(|r| r.class != ReadClass::Origin));
            let bytes = std::fs::read(fixture.wal.durable_publication().unwrap().path()).unwrap();
            let positions = decode_fixture(&bytes);
            let boundary = positions[frames
                .iter()
                .rposition(|f| matches!(f.record, WalRecord::TurnFinished { .. }))
                .unwrap()];
            let mut named = Vec::new();
            for read in &access.reads {
                let name = if read.class == ReadClass::LineageHeader {
                    assert_eq!(
                        (read.offset, read.length),
                        (0, broca_wal::framing::LINEAGE_FRAME_LEN as u64)
                    );
                    "lineage header".to_string()
                } else if matches!(read.class, ReadClass::Validation(_)) {
                    use broca_wal::access_counter::ValidationRead;
                    match read.class {
                        ReadClass::Validation(ValidationRead::BoundaryFrame) => assert_eq!(
                            (read.offset, read.length),
                            (boundary.0, boundary.1 - boundary.0)
                        ),
                        ReadClass::Validation(ValidationRead::LineageGap) => assert_eq!(
                            (read.offset, read.length),
                            (boundary.1, broca_wal::framing::LINEAGE_FRAME_LEN as u64)
                        ),
                        _ => panic!("unlisted validation read {read:?}"),
                    }
                    format!("{:?}", read.class)
                } else if read.length == 0 {
                    format!("{:?}", read.class)
                } else {
                    let first = positions
                        .iter()
                        .position(|p| {
                            p.0 == read.offset
                                || p.0 == read.offset + broca_wal::framing::LINEAGE_FRAME_LEN as u64
                        })
                        .unwrap_or_else(|| panic!("unlisted source {read:?}"));
                    let last = positions
                        .iter()
                        .position(|p| p.1 == read.offset + read.length)
                        .or_else(|| {
                            positions
                                .iter()
                                .position(|p| p.0 == read.offset + read.length)
                                .and_then(|i| i.checked_sub(1))
                        })
                        .or_else(|| {
                            positions.iter().position(|p| {
                                p.1 + broca_wal::framing::LINEAGE_FRAME_LEN as u64
                                    == read.offset + read.length
                            })
                        })
                        .unwrap_or_else(|| panic!("unlisted source end {read:?}"));
                    assert!(
                        frames[first..=last]
                            .iter()
                            .all(|f| !matches!(f.record, WalRecord::Steered { .. })),
                        "discarded source was read"
                    );
                    format!(
                        "{:?}:{}..{}",
                        read.class,
                        first.saturating_sub(count / 1000),
                        last.saturating_sub(count / 1000)
                    )
                };
                named.push(name);
            }
            eprintln!(
                "resume {count}: {} reads, {} decodes: {named:?}",
                access.reads.len(),
                access.frames_decoded
            );
            // Sources: durable tail; first/current RunStarted and accounting; Setup;
            // selected system/anchor; current episode's tool turn. Both seed hydration
            // and request reconstruction validate boundary, lineage gap and header.
            assert_eq!(
                named,
                [
                    "Source:24..24",
                    "Source:0..0",
                    "Source:2..2",
                    "Source:11..11",
                    "Source:12..12",
                    "Source:14..14",
                    "Source:22..22",
                    "Source:12..12",
                    "Validation(LineageGap)",
                    "Validation(BoundaryFrame)",
                    "lineage header",
                    "Source:0..0",
                    "Source:0..0",
                    "Source:12..12",
                    "Source:0..0",
                    "Source:2..2",
                    "Source:11..11",
                    "Source:12..12",
                    "Source:14..14",
                    "Source:0..0",
                    "Source:14..21",
                    "Source:12..12",
                    "Validation(LineageGap)",
                    "Validation(BoundaryFrame)",
                    "lineage header",
                ]
            );
            assert_eq!(access.frames_decoded, 41);
            costs.push((named, access.frames_decoded));
        }
        assert_eq!(
            costs[0], costs[1],
            "discarded history must not grow resume work"
        );
    }

    fn decode_fixture(bytes: &[u8]) -> Vec<(u64, u64)> {
        let mut at = 0;
        let mut frames = Vec::new();
        while at < bytes.len() {
            match broca_wal::framing::decode_next(&bytes[at..]) {
                DecodeStep::Record { consumed, .. } => {
                    frames.push((at as u64, (at + consumed) as u64));
                    at += consumed;
                }
                DecodeStep::Lineage { consumed, .. } => at += consumed,
                _ => panic!("fixture must decode"),
            }
        }
        frames
    }

    fn view_record(
        fixture: &Fixture,
        version: u64,
        ranges: Vec<broca_core::compaction_provider::view::ViewRange>,
        covers: u64,
    ) -> WalRecord {
        use broca_core::compaction_provider::{lineage::MessageRef, view::CompactionView};
        let view = CompactionView::new(
            "s-resume",
            version,
            fixture
                .wal
                .durable_publication()
                .unwrap()
                .lineage()
                .to_string(),
            MessageRef {
                ordinal: covers,
                mid: format!("m{covers}"),
            },
            ranges,
        );
        WalRecord::CompactionViewApplied {
            provider: "compactor".into(),
            view_version: version,
            view: broca_wal::OpaqueJson::from_json_text(serde_json::to_string(&view).unwrap())
                .unwrap(),
            choice: broca_wal::CompactionViewChoice::Applied,
        }
    }

    fn assert_media(fixture: &Fixture, loaded: &broca_core::resume::OffsetResumeState) {
        assert_eq!(
            loaded.snapshot.checkpoint_at_head.media,
            broca_core::AdmissionCheckpointV1::from_frames(&fixture.wal.replay().unwrap())
                .unwrap()
                .media
        );
    }

    #[test]
    fn database_resume_views_keep_replace_edit_and_supersede_at_every_cut() {
        use broca_core::compaction_provider::view::{PartEdit, ViewRange};
        for shape in 0..4 {
            let mut fixture = Fixture::new();
            let base = corpus(true);
            let turn = base.len() - 2;
            fixture.append(&base[..turn], false);
            fixture.append(&[setup(4)], false);
            let summary =
                serde_json::to_string(&vec![text(Role::User, "private view summary")]).unwrap();
            let ranges = match shape {
                0 => vec![ViewRange::keep(0, 4)],
                1 => vec![
                    ViewRange::replace(0, 2, summary.clone()),
                    ViewRange::keep(2, 4),
                ],
                _ => vec![
                    ViewRange::keep_with_edits(
                        0,
                        2,
                        vec![PartEdit::replace(
                            1,
                            "m1",
                            broca_protocol::tags::part_key(1, 0),
                            serde_json::json!({"type":"text","text":"private part edit"})
                                .to_string(),
                        )],
                    ),
                    ViewRange::keep(2, 4),
                ],
            };
            fixture.append(&[view_record(&fixture, 2, ranges, 3)], false);
            if shape == 3 {
                fixture.append(
                    &[view_record(
                        &fixture,
                        3,
                        vec![ViewRange::replace(0, 2, summary), ViewRange::keep(2, 4)],
                        3,
                    )],
                    false,
                );
            }
            fixture.append(&base[turn..], true);
            let mut next = base.clone();
            if let WalRecord::RunStarted { run_id, input, .. } = &mut next[0] {
                run_id.0.push_str("-next");
                *input = vec![text(Role::User, "next")].into();
            }
            for cut in 0..=next.len() {
                if cut > 0 {
                    let counter = broca_wal::access_counter::WalAccessCounter::default();
                    counter
                        .operation("view publication")
                        .run(|| fixture.append(&next[cut - 1..cut], true));
                    let access = counter.snapshot().remove(0);
                    assert!(
                        access.seed_fallbacks.is_empty(),
                        "shape {shape}, cut {cut}: {:?}",
                        access.seed_fallbacks
                    );
                    assert!(access
                        .reads
                        .iter()
                        .all(|r| r.class != broca_wal::access_counter::ReadClass::Origin));
                }
                let loaded = fixture.load();
                assert_eq!(
                    request(&loaded.prior, true, false),
                    request(&full(&fixture), true, true),
                    "shape {shape}, cut {cut}"
                );
                assert_media(&fixture, &loaded);
                let encoded: String = fixture
                    .db
                    .read_snapshot(|tx| {
                        Ok(tx.query_row("SELECT tail_seed FROM session", [], |r| r.get(0))?)
                    })
                    .unwrap();
                assert!(
                    !encoded.contains("private view summary")
                        && !encoded.contains("private part edit")
                );
            }
        }
    }

    #[test]
    fn database_sparse_media_setup_then_new_capture_and_new_view_matches_full_replay() {
        use broca_core::compaction_provider::view::ViewRange;
        let mut fixture = Fixture::new();
        let base = corpus(true);
        let turn = base.len() - 2;
        fixture.append(&base[..turn], false);
        fixture.append(&[setup(4)], false);
        fixture.append(&base[turn..], true);
        let mut next = base.clone();
        if let WalRecord::RunStarted { run_id, input, .. } = &mut next[0] {
            run_id.0.push_str("-media-tail");
            let mut message = text(Role::User, "new media");
            message
                .content
                .push(ContentBlock::Media(broca_protocol::MediaRef {
                    kind: broca_protocol::MediaKind::Image,
                    mime: "image/png".into(),
                    byte_size: 70,
                    sha256: "b".repeat(64),
                    width: Some(1),
                    height: Some(1),
                }));
            *input = vec![message].into();
        }
        for record in &next[..turn] {
            fixture.append(std::slice::from_ref(record), false);
            let loaded = fixture.load();
            assert_eq!(
                request(&loaded.prior, true, false),
                request(&full(&fixture), true, true)
            );
            assert_media(&fixture, &loaded);
        }
        // The view supersedes the non-empty initial range, restoring the old image
        // and retaining the new one. Its tool pair stays whole.
        let view = view_record(&fixture, 2, vec![ViewRange::keep(0, 7)], 6);
        fixture.append(&[view], false);
        let reader = fixture.db.resume_reader(
            fixture.bind.clone(),
            fixture.wal.durable_publication().unwrap(),
        );
        let head = fixture.wal.durable_publication().unwrap().sequence();
        let transient = reader.snapshot(head).unwrap();
        let fallback = broca_core::resume::reconstruct_from_offsets(
            &|off, len| reader.read_at(off, len),
            transient,
            head,
        )
        .err()
        .unwrap();
        assert_eq!(fallback.reason, "offset prompt window changed in the tail");
        for record in &next[turn..] {
            fixture.append(std::slice::from_ref(record), true);
            let loaded = fixture.load();
            assert_eq!(
                request(&loaded.prior, true, false),
                request(&full(&fixture), true, true)
            );
            assert_media(&fixture, &loaded);
        }
    }

    #[test]
    fn database_resume_exit_gate_prefix_preserves_request_and_publication() {
        let mut fixture = Fixture::new();
        let base = corpus(false);
        fixture.append(&base, true);
        let mut next = base[0].clone();
        if let WalRecord::RunStarted { run_id, input, .. } = &mut next {
            run_id.0.push_str("-resume");
            *input = vec![text(Role::User, "next")].into();
        }
        fixture.append(&[next], false);
        let before = fixture.load();
        let exit = broca_core::resume::resume_started_record(
            &before.prior,
            before.snapshot.head_seq,
            broca_core::resume::planned_redo(&before.prior),
        );
        fixture.append(&[exit], false);
        let root = fixture._scratch.0.clone();
        drop(fixture.db);
        fixture.db = Arc::new(OverhaulDb::open(&root).unwrap());
        let after = fixture.load();
        assert_eq!(
            request(&after.prior, false, false),
            request(&before.prior, false, false)
        );
        assert_eq!(
            request(&after.prior, false, false),
            request(&full(&fixture), false, true)
        );
        assert_eq!(after.prior.extent, before.prior.extent);
        fixture.publish();
        assert_eq!(
            fixture.load().snapshot.head_seq,
            fixture.wal.durable_publication().unwrap().sequence()
        );
    }

    #[test]
    fn compaction_locator_damage_and_unknown_versions_are_named_fallbacks() {
        for damage in ["version", "digest", "missing", "fold_hash"] {
            let mut fixture = Fixture::new();
            let mut records = corpus(false);
            records.insert(records.len() - 2, setup(4));
            fixture.append(&records, true);
            fixture
                .db
                .authoritative_transaction(|tx| {
                    let encoded: String =
                        tx.query_row("SELECT tail_seed FROM session", [], |r| r.get(0))?;
                    let mut value: serde_json::Value = serde_json::from_str(&encoded).unwrap();
                    match damage {
                        "version" => value["admission"]["compaction"]["version"] = 999.into(),
                        "digest" => {
                            let byte = &mut value["admission"]["compaction"]["sources"][0]
                                ["source"]["identity"][0];
                            *byte = (byte.as_u64().unwrap() ^ 1).into();
                        }
                        "fold_hash" => {
                            let byte = &mut value["admission"]["compaction"]["identity"][0];
                            *byte = (byte.as_u64().unwrap() ^ 1).into();
                        }
                        _ => {
                            value["admission"]["compaction"]["sources"]
                                .as_array_mut()
                                .unwrap()
                                .clear();
                        }
                    }
                    tx.execute("UPDATE session SET tail_seed=?1", [value.to_string()])?;
                    Ok(())
                })
                .unwrap();
            let witness = fixture.wal.durable_publication().unwrap();
            let error = fixture
                .db
                .resume_reader(fixture.bind.clone(), witness.clone())
                .snapshot(witness.sequence())
                .err()
                .unwrap();
            assert_eq!(
                error.reason,
                match damage {
                    "digest" => "admission source identity mismatch",
                    "fold_hash" => "compaction continuation locators incomplete",
                    _ => "unsupported compaction continuation",
                }
            );
        }
    }

    #[test]
    fn unknown_compaction_record_kind_requires_named_full_replay() {
        let mut future_kind = serde_json::to_value(setup(4)).unwrap();
        future_kind["kind"] = "future_setup_kind".into();
        for (value, reason) in [
            (
                serde_json::json!({"type":"compaction_future_setup","kind":"negotiated","initial":{"from":0,"to":4}}),
                "resume source contains an unknown record",
            ),
            (future_kind, "resume source is not a complete frame range"),
        ] {
            let bytes =
                broca_wal::framing::encode_payload(1, 1, &serde_json::to_vec(&value).unwrap())
                    .unwrap();
            let counter = broca_wal::access_counter::WalAccessCounter::default();
            let fallback = counter
                .operation("unknown compaction kind")
                .run(|| decode(&bytes, 69))
                .err()
                .unwrap();
            assert_eq!(fallback.reason, reason);
            assert_eq!(counter.snapshot()[0].seed_fallbacks, vec![reason]);
        }
    }

    fn repeated_turn(step: u64) -> Vec<WalRecord> {
        let base = corpus(false);
        let mut turn = base[1..base.len() - 1].to_vec();
        for record in &mut turn {
            match record {
                WalRecord::StepStarted { step_id }
                | WalRecord::ModelStepFinished { step_id, .. }
                | WalRecord::ToolBatchStarted { step_id, .. }
                | WalRecord::TurnFinished { step_id } => *step_id = broca_protocol::StepId(step),
                _ => {}
            }
            match record {
                WalRecord::ModelStepFinished {
                    assistant_message, ..
                } => {
                    for block in &mut Arc::make_mut(assistant_message).content {
                        if let ContentBlock::ToolCall(call) = block {
                            call.tool_name = "read".into();
                            call.input = serde_json::json!({"path":"private argument"});
                        }
                    }
                }
                WalRecord::ToolResult {
                    result, outcome, ..
                } => {
                    let result = Arc::make_mut(result);
                    result.is_error = false;
                    result.output = broca_protocol::ToolOutput::Text {
                        text: "private stable result".into(),
                    };
                    *outcome = broca_wal::DispatchOutcome::Completed;
                }
                _ => {}
            }
        }
        turn
    }

    #[test]
    fn historical_partial_episode_repetition_warnings_match_full_fold_at_every_cut() {
        use broca_core::compaction_provider::view::ViewRange;
        for kept in 1..=3 {
            let mut fixture = Fixture::new();
            let base = corpus(false);
            fixture.append(&base[..1], false);
            for step in 1..=4 {
                fixture.append(&repeated_turn(step), false);
            }
            fixture.append(&base[base.len() - 1..], false);
            let mut start = base[0].clone();
            if let WalRecord::RunStarted { run_id, input, .. } = &mut start {
                run_id.0.push_str("-current");
                *input = vec![text(Role::User, "current")].into();
            }
            fixture.append(&[start, setup(11)], false);
            let from = 2 + 2 * (kept - 1);
            let summary = serde_json::to_string(&vec![text(Role::User, "summary")]).unwrap();
            let view = view_record(
                &fixture,
                2,
                vec![
                    ViewRange::replace(0, from, summary.clone()),
                    ViewRange::keep(from, from + 2),
                    ViewRange::replace(from + 2, 10, summary),
                    ViewRange::keep(10, 11),
                ],
                10,
            );
            fixture.append(&[view], false);
            fixture.append(&repeated_turn(1), true);
            let tail = repeated_turn(2);
            for cut in 0..=tail.len() {
                if cut > 0 {
                    fixture.append(&tail[cut - 1..cut], true);
                }
                let loaded = fixture.load();
                let expected = request(&full(&fixture), true, true);
                assert_eq!(
                    request(&loaded.prior, true, false),
                    expected,
                    "kept turn {kept}, cut {cut}"
                );
                let rendered = String::from_utf8(expected).unwrap();
                assert!(
                    rendered.contains("returned exactly the same result"),
                    "kept turn {kept} must retain its repetition warning"
                );
            }
            let encoded: String = fixture
                .db
                .read_snapshot(|tx| {
                    Ok(tx.query_row("SELECT tail_seed FROM session", [], |r| r.get(0))?)
                })
                .unwrap();
            assert!(
                !encoded.contains("private argument") && !encoded.contains("private stable result")
            );
            let value: serde_json::Value = serde_json::from_str(&encoded).unwrap();
            eprintln!(
                "historical repetition checkpoint sizes: {:?}",
                value["guidance"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|e| serde_json::to_vec(e).unwrap().len())
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn missing_or_different_repetition_rules_require_named_full_replay() {
        for missing in [false, true] {
            let mut fixture = Fixture::new();
            let mut records = corpus(false);
            records.insert(records.len() - 2, setup(4));
            fixture.append(&records, true);
            fixture
                .db
                .authoritative_transaction(|tx| {
                    let encoded: String =
                        tx.query_row("SELECT tail_seed FROM session", [], |r| r.get(0))?;
                    let mut value: serde_json::Value = serde_json::from_str(&encoded).unwrap();
                    if missing {
                        value["version"] = 2.into();
                        value.as_object_mut().unwrap().remove("guidance");
                    } else {
                        value["guidance"][0]["spans"][0]["checkpoint"]["rules_version"] =
                            999.into();
                    }
                    tx.execute("UPDATE session SET tail_seed=?1", [value.to_string()])?;
                    Ok(())
                })
                .unwrap();
            let publication = fixture.wal.durable_publication().unwrap();
            let error = fixture
                .db
                .resume_reader(fixture.bind.clone(), publication.clone())
                .snapshot(publication.sequence())
                .err()
                .unwrap();
            assert_eq!(
                error.reason,
                if missing {
                    "resume repetition checkpoint missing"
                } else {
                    "unsupported resume repetition checkpoint"
                }
            );
        }
    }
}
