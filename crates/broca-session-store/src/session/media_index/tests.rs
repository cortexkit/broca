use std::sync::{Arc, Mutex};

use broca_protocol::media_event::{MediaEvent, MediaInput, MediaMessage};
use futures::executor::block_on;

use super::*;
use crate::backend::{LocalBackend, MemoryBackend, SessionAddr};
use crate::codec::hex;
use crate::fold::media::tests::{
    assistant, attempt, batch_end, batch_start, call, end, facts, input, outcome, photo,
    render_config, reply, said, start, steer, step_end, step_start, text, turn_facts, History,
};
use crate::fold::media::MediaItem;
use crate::head::SessionTriple;
use crate::keys::{Cipher, DeploymentKey};
use crate::record::Placement;
use crate::session::{Session, GRACE_MS};

cipher_tests!(
    the_events_of_a_load_continue_the_saved_index_mid_run_and_between_runs,
    a_seal_names_the_same_index_until_media_change_and_cleanup_deletes_the_old_one,
    a_session_without_media_keeps_its_step_counter_across_a_seal_without_an_index,
    a_missing_damaged_or_unknown_index_is_its_own_answer,
    restore_treats_the_named_index_as_part_of_the_session,
    an_undo_cuts_the_index_at_the_message_it_returns_to,
);

/// A session with 500 tool images, each in its results message's skeleton, shown in its
/// requests and stored in an object of its own, half of them aged out: its saved index stays
/// under 1 KB per image.
#[test]
fn a_saved_index_holds_under_a_kilobyte_per_image() {
    let reference = |n: u64| -> broca_protocol::content::MediaRef {
        serde_json::from_value(serde_json::json!({
            "kind": "image",
            "mime": "image/png",
            "byte_size": 1_048_576 + n,
            "sha256": format!("{n:064x}"),
            "width": 1920,
            "height": 1080,
        }))
        .unwrap()
    };
    let mut index = StoredMediaIndex {
        steps: 4_000,
        last_step: Some(8),
        step_open: true,
        limits: Some(serde_json::json!({ "max_images": 100, "max_bytes": 33_554_432 })),
        ..StoredMediaIndex::default()
    };
    for n in 0..500u64 {
        let call = ToolCallId(format!("toolu_01{n:024}"));
        let id = StoredMediaId {
            record: 1_000_000 + n * 40,
            message: 0,
            block: 2,
            part: Some(1),
        };
        index.captures.push(StoredCapture {
            id,
            reference: reference(n),
            ordinal: Some(150_000 + n * 4),
            tool_call: Some(call.clone()),
            first_step: Some(3_000 + n * 2),
            person: false,
        });
        index.placed.push(PlacedSkeleton {
            ordinal: 150_000 + n * 4,
            skeleton: StoredSkeleton {
                role: broca_protocol::content::Role::Tool,
                blocks: 3,
                slots: vec![StoredSlot {
                    block: 2,
                    call: Some(call),
                    part: Some(1),
                    reference: reference(n),
                }],
            },
        });
        let occurrence = StoredOccurrence {
            capture: id,
            repetition: 0,
        };
        index.objects.push(StoredObject {
            sha256: format!("{n:064x}"),
            object: crate::record::MediaObject {
                part: 1,
                log: crate::codec::Id128([7; 16]),
                generation: crate::codec::Id128([9; 16]),
                seq: 1_000_000 + n * 40 - 1,
            },
        });
        if n % 2 == 0 {
            index.aging.aged.push(occurrence);
        } else {
            index.aging.shown.push(ShownOccurrence {
                occurrence,
                bytes: 1_048_576,
                tokens: 1_600,
                protected: false,
            });
            index.aging.unprotected.push(occurrence);
        }
    }
    index.check(2_000_000).unwrap();
    let bytes = MediaIndexObject {
        version: MEDIA_INDEX_VERSION,
        body: ByteBuf(index.encode()),
    }
    .encode()
    .len();
    assert!(bytes < 500 * 1024, "{bytes} bytes for 500 images");
    // About 314 bytes per image when this was written, and about 424 with where its bytes are
    // stored: far under the 1 KB budget.
    assert!(bytes / 500 < 500, "{} bytes per image", bytes / 500);
}

fn triple() -> SessionTriple {
    SessionTriple {
        project_root: "/project".into(),
        harness: "broca".into(),
        session: "images".into(),
    }
}

fn deployment(cipher: Cipher) -> DeploymentKey {
    DeploymentKey::new(1, &[5; 32]).creating(cipher)
}

fn addr() -> SessionAddr {
    SessionAddr("0123456789abcdef".into())
}

/// Stands in for the image engine: it takes what the session hands over, in order, and keeps a
/// capture per medium and the step counter, so its index changes when media or steps do.
#[derive(Default)]
struct Engine {
    index: StoredMediaIndex,
    /// The last seq whose records it took.
    through: u64,
}

impl Engine {
    fn take(&mut self, feed: &MediaFeed) {
        if let Some(start) = &feed.start {
            self.index.steps = start.steps;
        }
        for item in &feed.items {
            let mut capture = |record: u64, message: usize, block: usize, part: Option<usize>| {
                let id = StoredMediaId {
                    record,
                    message,
                    block,
                    part,
                };
                self.index.captures.push(StoredCapture {
                    id,
                    reference: crate::fold::media::tests::image("a"),
                    ordinal: None,
                    tool_call: None,
                    first_step: None,
                    person: true,
                });
            };
            match item {
                MediaItem::RunStart(run) => {
                    for input in &run.inputs {
                        for slot in &input.skeleton.slots {
                            capture(run.record, input.message, slot.block, None);
                        }
                    }
                }
                MediaItem::Event(MediaEvent::RunStart { record, inputs, .. }) => {
                    for input in inputs {
                        for slot in &input.skeleton.slots {
                            capture(*record, input.message, slot.block, None);
                        }
                    }
                }
                MediaItem::Event(MediaEvent::Accepted { record, messages }) => {
                    for (message, skeleton) in messages.iter().enumerate() {
                        for slot in &skeleton.slots {
                            capture(*record, message, slot.block, None);
                        }
                    }
                }
                MediaItem::Event(MediaEvent::ToolResult { record, media, .. }) => {
                    for part in media {
                        capture(*record, 0, 0, Some(part.part));
                    }
                }
                MediaItem::Event(MediaEvent::StepStart { .. }) => self.index.steps += 1,
                MediaItem::Event(_) => {}
            }
        }
    }
}

/// A session being written, with everything it appended and handed over.
struct Writer<B: SessionBackend> {
    session: Session<B>,
    engine: Arc<Mutex<Engine>>,
    /// Every batch appended, by its first seq.
    batches: Vec<(u64, Vec<Record>)>,
    /// What each batch handed over, with the seq of the batch's last record.
    handed: Vec<(u64, MediaItem)>,
}

impl<B: SessionBackend> Writer<B> {
    /// Starts feeding `session`'s media to a new engine holding `index`, current through
    /// `through`.
    async fn new(mut session: Session<B>, index: StoredMediaIndex, through: u64) -> Self {
        let engine = Arc::new(Mutex::new(Engine { index, through }));
        let source = engine.clone();
        session.set_media_index_source(Box::new(move || {
            let engine = source.lock().unwrap();
            Some(MediaIndexSave {
                index: engine.index.clone(),
                through: engine.through,
            })
        }));
        Self {
            session,
            engine,
            batches: Vec::new(),
            handed: Vec::new(),
        }
    }

    async fn append(&mut self, records: Vec<Record>) {
        let (first, feed) = self.session.append_feed(&records, true).await.unwrap();
        let last = first + records.len() as u64 - 1;
        let mut engine = self.engine.lock().unwrap();
        engine.take(&feed);
        engine.through = last;
        self.handed
            .extend(feed.items.into_iter().map(|item| (last, item)));
        self.batches.push((first, records));
    }

    /// Seals the open log with the engine current, as its owner would; returns the seal's
    /// intent's seq, which the new checkpoint is current through.
    async fn seal(&mut self) -> u64 {
        let intent = self.session.next_seq();
        self.engine.lock().unwrap().through = intent - 1;
        self.session.seal(None, 1_000).await.unwrap();
        assert!(self.session.take_media().is_empty());
        intent
    }

    fn index(&self) -> StoredMediaIndex {
        self.engine.lock().unwrap().index.clone()
    }

    fn named(&self) -> Option<String> {
        match &self.session.media.named {
            Named::Ref(media) => media.object.clone(),
            _ => None,
        }
    }
}

fn index_objects(backend: &MemoryBackend) -> Vec<String> {
    backend
        .object_names(&addr())
        .into_iter()
        .filter(|name| name.ends_with("/mi"))
        .collect()
}

/// A first run without media, at ordinals 1 and 2.
async fn plain_run<B: SessionBackend>(writer: &mut Writer<B>) {
    writer
        .append(vec![
            start("r1", vec![render_config()], false),
            input("r1", Placement::RunInput, None, said("hello")),
            facts(1),
        ])
        .await;
    writer.append(vec![step_start("r1", 1)]).await;
    writer.append(vec![attempt("r1", 1)]).await;
    writer
        .append(vec![
            assistant("r1", 1, vec![text("hi")]),
            step_end("r1", 1, 0),
            facts(2),
        ])
        .await;
    writer.append(vec![end("r1")]).await;
}

/// A run whose input carries an image, at ordinal 3, then its step 1 started and a steer with
/// an image accepted: a seal now falls in the middle of a run. Returns the steer's seq.
async fn photo_run_until_a_steer<B: SessionBackend>(writer: &mut Writer<B>) -> u64 {
    writer
        .append(vec![
            start("r2", vec![render_config()], false),
            input("r2", Placement::RunInput, None, photo("look", "a")),
            facts(3),
        ])
        .await;
    writer.append(vec![step_start("r2", 1)]).await;
    writer.append(vec![attempt("r2", 1)]).await;
    let steered = writer.session.next_seq();
    writer
        .append(vec![steer(vec![photo("and this", "b")])])
        .await;
    steered
}

/// The rest of the run: the steer placed at the next step's boundary, a tool result holding an
/// image, a final answer and the run's end.
async fn photo_run_from_the_steer<B: SessionBackend>(writer: &mut Writer<B>, steered: u64) {
    writer
        .append(vec![
            assistant("r2", 1, vec![text("seen")]),
            step_end("r2", 1, 0),
            facts(4),
        ])
        .await;
    writer
        .append(vec![
            input(
                "r2",
                Placement::Steer,
                Some(steered),
                photo("and this", "b"),
            ),
            step_start("r2", 2),
            facts(5),
        ])
        .await;
    writer.append(vec![attempt("r2", 2)]).await;
    writer
        .append(vec![
            assistant("r2", 2, vec![call("x")]),
            step_end("r2", 2, 1),
            facts(6),
        ])
        .await;
    writer.append(vec![batch_start("r2", 2, &["x"])]).await;
    writer
        .append(vec![
            reply("r2", 2, "x", &["c"]),
            outcome("r2", 2, "x"),
            facts(7),
            turn_facts(6),
            batch_end("r2", 2),
        ])
        .await;
    writer.append(vec![step_start("r2", 3)]).await;
    writer
        .append(vec![
            assistant("r2", 3, vec![text("done")]),
            step_end("r2", 3, 0),
            facts(8),
        ])
        .await;
    writer.append(vec![end("r2")]).await;
}

/// A seal saves the engine's index with the media fold's state; an open hands the index back
/// with what the open log's records after the seal handed over, and continues from both. For a
/// seal in the middle of a run (a step open, a steer waiting for its place) and one between
/// runs, what was handed over before the seal and what the load hands over after it are exactly
/// what folding every record from the session's start hands over.
fn the_events_of_a_load_continue_the_saved_index_mid_run_and_between_runs(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let loaded = session.feed_media().await.unwrap();
        assert_eq!(loaded.saved, SavedIndex::NoCapture);
        assert_eq!(loaded.feed, MediaFeed::default());
        let mut writer = Writer::new(session, StoredMediaIndex::default(), 0).await;
        plain_run(&mut writer).await;
        let steered = photo_run_until_a_steer(&mut writer).await;
        let mid_run = writer.seal().await;
        let saved_mid_run = writer.index();
        let (batches, mut handed) = (writer.batches, writer.handed);
        writer.session.close().await.unwrap();

        // The open in the middle of the run.
        let (session, loaded) =
            Session::load_feeding_media(&backend, addr(), &triple(), &deployment(cipher))
                .await
                .unwrap();
        assert_eq!(
            loaded.saved,
            SavedIndex::Saved(Box::new(saved_mid_run.clone()))
        );
        assert_eq!(loaded.through, mid_run);
        assert!(loaded.feed.is_empty());
        let mut writer = Writer::new(session, saved_mid_run, mid_run).await;
        writer.batches = batches;
        photo_run_from_the_steer(&mut writer, steered).await;
        let between_runs = writer.seal().await;
        let saved_between_runs = writer.index();
        writer
            .append(vec![
                start("r3", Vec::new(), false),
                input("r3", Placement::RunInput, None, said("again")),
                facts(9),
            ])
            .await;
        writer.append(vec![step_start("r3", 1)]).await;
        let after_seal: Vec<MediaItem> = writer
            .handed
            .iter()
            .filter(|(last, _)| *last > between_runs)
            .map(|(_, item)| item.clone())
            .collect();
        handed.extend(writer.handed);
        let batches = writer.batches;
        writer.session.close().await.unwrap();

        // The open between runs.
        let (_, loaded) =
            Session::load_feeding_media(&backend, addr(), &triple(), &deployment(cipher))
                .await
                .unwrap();
        assert_eq!(
            loaded.saved,
            SavedIndex::Saved(Box::new(saved_between_runs))
        );
        assert_eq!(loaded.through, between_runs);
        assert_eq!(loaded.feed.start, None);
        assert_eq!(loaded.feed.items, after_seal);
        assert!(!after_seal.is_empty());

        // A one-time build from the records through the saved index's seq, across both seals,
        // hands over what the writers were handed through it, and the load's feed continues it.
        let (session, _) =
            Session::load_feeding_media(&backend, addr(), &triple(), &deployment(cipher))
                .await
                .unwrap();
        let history = session
            .stored_records(&backend)
            .media_history(between_runs)
            .await
            .unwrap();
        let before: Vec<MediaItem> = handed
            .iter()
            .filter(|(last, _)| *last < between_runs)
            .map(|(_, item)| item.clone())
            .collect();
        assert_eq!(history.items, before);
        assert_eq!(
            history.start.as_ref().map(|start| start.steps),
            Some(1),
            "the first capture follows the plain run's one step"
        );

        // Every record folded from the session's start.
        let mut history = History::new();
        let mut everything = MediaFeed::default();
        for (first, records) in batches {
            history.next = first;
            everything.extend(history.batch(records).1);
        }
        let handed: Vec<MediaItem> = handed.into_iter().map(|(_, item)| item).collect();
        assert_eq!(everything.items, handed);
        // The placement across the mid-run seal is the steer's, before its step's start.
        assert!(handed.windows(2).any(|pair| matches!(
            pair,
            [
                MediaItem::Event(MediaEvent::Placed { first: 4 }),
                MediaItem::Event(MediaEvent::StepStart { .. })
            ]
        )));
    });
}

/// A seal whose engine index didn't change names the index the last seal saved and writes
/// nothing; one after a change writes a new index, and the old one goes with what the change
/// replaced, deleted after the grace period. A seal handed no index current through it saves
/// none once something was handed over since.
fn a_seal_names_the_same_index_until_media_change_and_cleanup_deletes_the_old_one(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        session.feed_media().await.unwrap();
        let mut writer = Writer::new(session, StoredMediaIndex::default(), 0).await;
        plain_run(&mut writer).await;
        photo_run_until_a_steer(&mut writer).await;
        writer.seal().await;
        let first = writer.named().expect("the seal saved an index");
        assert_eq!(index_objects(&backend), vec![first.clone()]);

        writer.seal().await;
        assert_eq!(writer.named(), Some(first.clone()));
        assert_eq!(index_objects(&backend), vec![first.clone()]);
        let pending = writer.session.head().pending.last().unwrap();
        assert!(!pending.objects.contains(&first));

        writer.append(vec![attempt("r2", 1)]).await;
        writer.seal().await;
        assert_eq!(
            writer.named(),
            Some(first.clone()),
            "an attempt changed no media"
        );

        writer.append(vec![step_start("r2", 2)]).await;
        writer.seal().await;
        let second = writer.named().expect("the seal saved an index");
        assert_ne!(second, first);
        let pending = writer.session.head().pending.last().unwrap();
        assert!(pending.objects.contains(&first));
        assert_eq!(index_objects(&backend).len(), 2);

        writer.session.collect(1_000 + GRACE_MS).await.unwrap();
        assert_eq!(index_objects(&backend), vec![second.clone()]);
        assert_eq!(
            writer.session.read_media_index().await.unwrap(),
            SavedIndex::Saved(Box::new(writer.index()))
        );

        // Without an index current through the seal.
        writer.session.media.source = None;
        writer.seal().await;
        assert_eq!(
            writer.named(),
            Some(second.clone()),
            "nothing was handed over"
        );
        writer.append(vec![attempt("r2", 2)]).await;
        writer.seal().await;
        assert_eq!(writer.named(), None);
        let pending = writer.session.head().pending.last().unwrap();
        assert!(pending.objects.contains(&second));
        writer.session.close().await.unwrap();
        let (_, loaded) =
            Session::load_feeding_media(&backend, addr(), &triple(), &deployment(cipher))
                .await
                .unwrap();
        assert_eq!(loaded.saved, SavedIndex::NotSaved);
    });
}

/// A session that never captured a medium saves no index, only the fold's scalars in its
/// checkpoint, and opens with its step counter where it was.
fn a_session_without_media_keeps_its_step_counter_across_a_seal_without_an_index(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        session.feed_media().await.unwrap();
        let mut writer = Writer::new(session, StoredMediaIndex::default(), 0).await;
        plain_run(&mut writer).await;
        assert!(writer.handed.is_empty());
        writer.seal().await;
        assert_eq!(writer.named(), None);
        assert!(index_objects(&backend).is_empty());
        writer.session.close().await.unwrap();
        let (session, loaded) =
            Session::load_feeding_media(&backend, addr(), &triple(), &deployment(cipher))
                .await
                .unwrap();
        assert_eq!(loaded.saved, SavedIndex::NoCapture);
        assert_eq!(session.media_fold().steps, 1);
        assert!(!session.media_fold().has_captured());
        // A build from the records hands over the scalars they end with as the index's start.
        let history = session
            .stored_records(&backend)
            .media_history(loaded.through)
            .await
            .unwrap();
        assert!(history.items.is_empty());
        let start = history.start.expect("a history hands over its scalars");
        assert_eq!((start.steps, start.step_open), (1, false));
    });
}

/// The index a checkpoint names reads back as three distinct answers when it isn't whole:
/// missing, damaged (it doesn't open, doesn't decode, or its parts disagree), or of a version
/// this build doesn't read. None of them reads as "no media".
fn a_missing_damaged_or_unknown_index_is_its_own_answer(cipher: Cipher) {
    let backend = MemoryBackend::default();
    let (name, through) = block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        session.feed_media().await.unwrap();
        let mut writer = Writer::new(session, StoredMediaIndex::default(), 0).await;
        plain_run(&mut writer).await;
        photo_run_until_a_steer(&mut writer).await;
        let through = writer.seal().await;
        let name = writer.named().unwrap();
        writer.session.close().await.unwrap();
        (name, through)
    });
    let saved = || {
        block_on(async {
            Session::load_feeding_media(&backend, addr(), &triple(), &deployment(cipher))
                .await
                .unwrap()
                .1
                .saved
        })
    };
    let whole = block_on(backend.get_object(&addr(), &name))
        .unwrap()
        .unwrap();
    assert!(matches!(saved(), SavedIndex::Saved(_)));

    let mut flipped = whole.clone();
    let last = flipped.len() - 1;
    flipped[last] ^= 1;
    backend.set_object(&addr(), &name, flipped);
    assert!(matches!(saved(), SavedIndex::Damaged(reason) if reason.starts_with(&name)));

    let session = block_on(Session::load(
        &backend,
        addr(),
        &triple(),
        &deployment(cipher),
    ))
    .unwrap();
    let seal = |envelope: MediaIndexObject| {
        session
            .key
            .frame_key()
            .seal_unit(name.as_bytes(), &envelope.encode())
    };
    backend.set_object(
        &addr(),
        &name,
        seal(MediaIndexObject {
            version: MEDIA_INDEX_VERSION + 1,
            body: ByteBuf(b"a newer build's index".to_vec()),
        }),
    );
    assert_eq!(saved(), SavedIndex::UnknownVersion(MEDIA_INDEX_VERSION + 1));

    backend.set_object(
        &addr(),
        &name,
        seal(MediaIndexObject {
            version: MEDIA_INDEX_VERSION,
            body: ByteBuf(b"not an index".to_vec()),
        }),
    );
    assert!(matches!(saved(), SavedIndex::Damaged(_)));

    let ahead = StoredMediaIndex {
        captures: vec![StoredCapture {
            id: StoredMediaId {
                record: through + 1,
                message: 0,
                block: 0,
                part: None,
            },
            reference: crate::fold::media::tests::image("a"),
            ordinal: None,
            tool_call: None,
            first_step: None,
            person: true,
        }],
        ..StoredMediaIndex::default()
    };
    backend.set_object(
        &addr(),
        &name,
        seal(MediaIndexObject {
            version: MEDIA_INDEX_VERSION,
            body: ByteBuf(ahead.encode()),
        }),
    );
    assert!(matches!(saved(), SavedIndex::Damaged(reason) if reason.contains("record")));

    let version = block_on(backend.read_head(&addr())).unwrap().unwrap().1;
    block_on(backend.delete_object(&addr(), &name, version)).unwrap();
    assert_eq!(saved(), SavedIndex::Missing(name.clone()));
    drop(session);
}

/// A store root of its own, removed when the test ends.
struct Root(std::path::PathBuf);

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Restore checks the index a checkpoint names, as it checks an import's id map: a version whose
/// index the backup lacks isn't complete, and restore rolls back to the version before it.
fn restore_treats_the_named_index_as_part_of_the_session(cipher: Cipher) {
    let root = Root(std::env::temp_dir().join(format!(
        "broca-media-index-{}",
        hex(&crate::keys::random_id().0)
    )));
    let backend = LocalBackend::new(&root.0);
    let (before, name) = block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        session.feed_media().await.unwrap();
        let mut writer = Writer::new(session, StoredMediaIndex::default(), 0).await;
        // A run that names no setup part, which the store would have to hold too.
        writer
            .append(vec![
                start("r1", Vec::new(), false),
                input("r1", Placement::RunInput, None, photo("look", "a")),
                facts(1),
            ])
            .await;
        writer.append(vec![step_start("r1", 1)]).await;
        let before = backend.read_head(&addr()).await.unwrap().unwrap().1;
        writer.seal().await;
        (before, writer.named().unwrap())
    });
    let complete = block_on(crate::session::restore::restore(
        &backend,
        &addr(),
        &deployment(cipher),
        2_000,
    ))
    .unwrap();
    assert!(
        matches!(complete, crate::session::restore::Restored::Kept { .. }),
        "{complete:?}"
    );
    let version = block_on(backend.read_head(&addr())).unwrap().unwrap().1;
    block_on(backend.delete_object(&addr(), &name, version)).unwrap();
    let restored = block_on(crate::session::restore::restore(
        &backend,
        &addr(),
        &deployment(cipher),
        2_000,
    ))
    .unwrap();
    assert!(
        matches!(
            restored,
            crate::session::restore::Restored::Repaired { to, cut: None, .. } if to == before
        ),
        "{restored:?}"
    );
}

/// An undo to message 3 (counted from 1, as the store's records count) keeps messages 1 and 2,
/// ordinals 0 and 1 as events count them: the image of the last message kept stays below the
/// cut, and the image of the first message removed doesn't. The undo hands over the cut and its
/// new run's start; its checkpoint names no index, the index the seal before saved goes with
/// what the undo replaced, and the next open builds the index again.
fn an_undo_cuts_the_index_at_the_message_it_returns_to(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        session.feed_media().await.unwrap();
        let mut writer = Writer::new(session, StoredMediaIndex::default(), 0).await;
        let mut inputs = Vec::new();
        for (ordinal, name) in [(1, "a"), (2, "b"), (3, "c")] {
            let run = format!("r{ordinal}");
            let mid = writer.session.message_id(2);
            let mut placed = facts(ordinal);
            if let Record::EntryFacts(placed) = &mut placed {
                Arc::make_mut(placed).mid = mid.clone();
            }
            writer
                .append(vec![
                    start(&run, Vec::new(), false),
                    input(&run, Placement::RunInput, None, photo("look", name)),
                    placed,
                ])
                .await;
            writer.append(vec![end(&run)]).await;
            inputs.push(mid);
        }
        writer.seal().await;
        let saved = writer.named().expect("the seal saved an index");
        let placed: Vec<u64> = writer
            .handed
            .iter()
            .filter_map(|(_, item)| match item {
                MediaItem::Event(MediaEvent::RunStart { inputs, .. }) => {
                    inputs.first().map(|input| input.ordinal)
                }
                _ => None,
            })
            .collect();
        assert_eq!(placed, vec![0, 1, 2]);

        let mut resend = start("r4", Vec::new(), false);
        if let Record::RunStart(resend) = &mut resend {
            resend.send_id = Some("resend".into());
        }
        let instead = photo("instead", "d");
        // The undo's intent, then its own record, then the resend.
        let reverted = writer.session.next_seq() + 1;
        writer
            .session
            .revert(
                &inputs[2],
                vec![
                    resend,
                    input("r4", Placement::RunInput, None, instead.clone()),
                    facts(0),
                ],
                None,
                2_000,
            )
            .await
            .unwrap();
        let feed = writer.session.take_media();
        let new_run = MediaItem::Event(MediaEvent::RunStart {
            record: reverted + 1,
            inputs: vec![MediaInput {
                message: 0,
                ordinal: 2,
                skeleton: MediaMessage::of(&instead),
            }],
            limits: None,
        });
        assert_eq!(
            feed.items,
            vec![
                MediaItem::Event(MediaEvent::Truncated { len: 2 }),
                new_run.clone()
            ]
        );
        // The second message's image is kept, the third's is not.
        assert!(placed[1] < 2 && placed[2] >= 2);
        assert_eq!(writer.named(), None);
        let pending = writer.session.head().pending.last().unwrap();
        assert!(pending.objects.contains(&saved));
        writer.session.close().await.unwrap();

        let (_, loaded) =
            Session::load_feeding_media(&backend, addr(), &triple(), &deployment(cipher))
                .await
                .unwrap();
        assert_eq!(loaded.saved, SavedIndex::NotSaved);
        assert_eq!(loaded.through, reverted);
        assert_eq!(loaded.feed.items, vec![new_run]);
    });
}
