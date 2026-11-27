//! Media in the requests of a run on a session kept in the session store.
//!
//! **The index.** The session's actor owns its image index ([`SessionIndex`]): the engine's
//! state for the session (`broca_provider::media_request::MediaIndex`: which media it captured
//! and where, which a person sent and so are kept out of aging, the aging totals, the media
//! limits in force), and where each medium's bytes are stored. The store hands the index's input
//! over as media events (`broca_session_store::fold::media`) with every write; the actor applies
//! each event once, in record order. A session that has captured nothing has no index: the
//! store keeps its few scalars and hands them over at its first capture, which creates it.
//!
//! **Opening.** A seal saves the index with the session. An open starts from the saved index and
//! applies what the open log's records after it handed over, so it reads no record twice. When
//! the checkpoint saves no index the session needs (it was written before indexes were saved,
//! or after an undo, or the index is missing, damaged or of an unknown version), the index is
//! built once from all the session's records, on a blocking thread while the actor keeps
//! answering commands, when the first run that may render media starts. Until then what the
//! session hands over waits, in order.
//!
//! **A run's view.** A run renders from the index as it stands, behind an `Arc` ([`RunMedia`]):
//! the actor applies a run's append before it answers it, so a request holds every record its
//! run wrote. The actor changes the index through `Arc::make_mut`, which copies it only if a
//! render holds the old one at that moment. A render writes nothing back. Until the index is
//! built, each of the run's model calls waits for it ([`LazyRenderer`]), and a cancel ends the
//! wait.
//!
//! **Failure.** An event the index can't take breaks it for the rest of the actor's life,
//! naming why. A request holding media is then refused by name before anything is sent, as it
//! is when the session has no way to read its media objects. It is never sent without its media.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};

use broca_core::media_request::SessionMedia;
use broca_core::media_store::{MediaBytes, MediaReadError};
use broca_protocol::media_event::{MediaEvent, MediaLimits};
use broca_protocol::{BindIdentity, MediaRef};
use broca_provider::media_request::{media_profile, MediaAging, MediaIndex};
use broca_session_loop::parts::Parts;
use broca_session_store::backend::SessionBackend;
use broca_session_store::fold::media::{limits_in, LimitsSource, MediaFeed, MediaItem, MediaStart};
use broca_session_store::record::{MediaObject, PartRef, Record};
use broca_session_store::session::{
    LoadedMedia, MediaError, MediaIndexSave, MediaObjects, SavedIndex, Session, SessionError,
    StoredMediaIndex,
};

/// What renders read of the index, and the actor changes.
#[derive(Default)]
struct Slot {
    /// The index, once the session captured a medium. Absent before, and while it is built.
    index: Option<Arc<MediaIndex>>,
    /// Why the index can't be had, once it can't: it stays so for the actor's life.
    broken: Option<String>,
    /// The seq of the last record whose events the index applied.
    through: u64,
    /// Whether the index applied every record through `through`: not while it waits to be
    /// built, so a seal meanwhile saves none.
    current: bool,
}

/// Where each medium's bytes are stored, by digest: the object the first record naming the
/// digest names. Kept apart from the index, since the engine reads bytes through it.
type Objects = Arc<Mutex<BTreeMap<Digest, MediaObject>>>;

/// A digest as [`Objects`] keys it: the 32 bytes a SHA-256 in lowercase hex names, held in
/// a quarter of its text's size, or any other text as it is. Keys order as their texts do, so
/// the saved index lists its objects in digest order.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Digest {
    Sha256([u8; 32]),
    Text(Box<str>),
}

impl Digest {
    fn of(text: &str) -> Self {
        fn nibble(digit: u8) -> Option<u8> {
            match digit {
                b'0'..=b'9' => Some(digit - b'0'),
                b'a'..=b'f' => Some(digit - b'a' + 10),
                _ => None,
            }
        }
        let digits = text.as_bytes();
        let mut bytes = [0u8; 32];
        let parsed = digits.len() == 64
            && bytes.iter_mut().enumerate().all(|(at, byte)| {
                match (nibble(digits[2 * at]), nibble(digits[2 * at + 1])) {
                    (Some(high), Some(low)) => {
                        *byte = high << 4 | low;
                        true
                    }
                    _ => false,
                }
            });
        if parsed {
            Self::Sha256(bytes)
        } else {
            Self::Text(text.into())
        }
    }

    fn text(&self) -> String {
        match self {
            Self::Sha256(bytes) => bytes.iter().map(|byte| format!("{byte:02x}")).collect(),
            Self::Text(text) => text.to_string(),
        }
    }
}

impl Ord for Digest {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            // Lowercase hex orders as the bytes it names.
            (Self::Sha256(left), Self::Sha256(right)) => left.cmp(right),
            _ => self.text().cmp(&other.text()),
        }
    }
}

impl PartialOrd for Digest {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl broca_session_store::held::HeldBytes for Digest {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::Sha256(_) => 0,
            Self::Text(text) => text.len(),
        }
    }
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Where the index stands in the actor's life.
enum Phase {
    /// The session doesn't feed its media yet: [`SessionIndex::open`] hasn't run.
    Unopened,
    /// The index applied everything handed over: each feed applies as it comes.
    Current,
    /// The index must be built once from the records through `through` first. What the
    /// session handed over since waits in `feeds`, in order.
    Unbuilt { through: u64, feeds: Vec<MediaFeed> },
    /// Being built; what the session hands over meanwhile waits.
    Building { feeds: Vec<MediaFeed> },
}

/// One session's image index, owned by its actor (see the module docs).
pub struct SessionIndex {
    slot: Arc<Mutex<Slot>>,
    objects: Objects,
    /// What the engine reads bytes through: the session's media objects over a reader of its
    /// own, or, without one, a source that refuses every read.
    bytes: Arc<dyn MediaBytes>,
    /// Why no medium can be read, when the actor has no reader of its own.
    unreadable: Option<String>,
    phase: Phase,
    /// The session's next seq when the actor got it: records appended before the session feeds
    /// its media would be in no feed, so the index is then built from the records instead.
    next_at_start: u64,
}

/// Why a run's media can't be read when the actor has no handle on the storage of its own.
const NO_READER: &str = "the session's media objects can't be read apart from its writer, so its \
                         media can't be rendered";

impl SessionIndex {
    /// The saved form of the index as it stands, with where its media are stored.
    pub fn stored(&self) -> Option<StoredMediaIndex> {
        let objects: Vec<(String, MediaObject)> = locked(&self.objects)
            .iter()
            .map(|(digest, object)| (digest.text(), object.clone()))
            .collect();
        self.index().map(|index| to_stored(&index, &objects))
    }

    /// The index as it stands, once the session captured a medium.
    pub fn index(&self) -> Option<Arc<MediaIndex>> {
        self.slot().index.clone()
    }

    /// The index of `session`, which the actor got with nothing appended since; its bytes are
    /// read over `reader` when there is one.
    pub fn new<B: SessionBackend, R: SessionBackend + Send + Sync + 'static>(
        session: &Session<B>,
        reader: Option<R>,
    ) -> Self {
        let objects = Objects::default();
        let (bytes, unreadable): (Arc<dyn MediaBytes>, _) = match reader {
            Some(reader) => (
                Arc::new(StoreMediaBytes {
                    objects: Arc::clone(&objects),
                    reader: std::panic::AssertUnwindSafe(session.media_objects(reader)),
                }),
                None,
            ),
            None => (
                Arc::new(Unavailable(NO_READER.into())),
                Some(NO_READER.into()),
            ),
        };
        Self {
            slot: Arc::default(),
            objects,
            bytes,
            unreadable,
            phase: Phase::Unopened,
            next_at_start: session.next_seq(),
        }
    }

    fn slot(&self) -> MutexGuard<'_, Slot> {
        locked(&self.slot)
    }

    /// Why the index can't be had, once it can't.
    pub fn broken(&self) -> Option<String> {
        self.slot().broken.clone()
    }

    /// The index `history` builds, which the media fold handed over for `session`'s records
    /// through `last` (see `Session::media_history`), reading bytes over `reader`. It feeds on
    /// nothing after: for rendering a session as it stands.
    pub fn from_history<B: SessionBackend, R: SessionBackend + Send + Sync + 'static>(
        session: &Session<B>,
        reader: R,
        history: MediaFeed,
        last: u64,
        parts: &dyn Parts,
    ) -> Self {
        let mut index = Self::new(session, Some(reader));
        index.phase = Phase::Building { feeds: Vec::new() };
        index.built(Ok(history), last, parts);
        index
    }

    /// Whether the session feeds its media to the index yet (see [`SessionIndex::open`]).
    pub fn opened(&self) -> bool {
        !matches!(self.phase, Phase::Unopened)
    }

    /// Has the index built once from the session's records through `through`, dropping what it
    /// holds, as an open that finds no saved index does: a test holds that build to hold a run's
    /// wait for its media.
    #[cfg(test)]
    pub(crate) fn build_from_records(&mut self, through: u64) {
        let mut slot = self.slot();
        slot.index = None;
        slot.current = false;
        drop(slot);
        self.phase = Phase::Unbuilt {
            through,
            feeds: Vec::new(),
        };
    }

    /// Whether the index waits to be built from the session's records.
    pub fn unbuilt(&self) -> bool {
        matches!(self.phase, Phase::Unbuilt { .. })
    }

    /// Whether the index applied everything the session handed over, so runs can read it.
    pub fn ready(&self) -> bool {
        matches!(self.phase, Phase::Current)
    }

    /// Starts feeding `session`'s media to the index, from what its open hands back: the saved
    /// index and what the open log's records after it handed over, or, when no saved index
    /// carries the session, a build from its records still to come. A change gets the index to
    /// save from here on. Runs once; later calls do nothing.
    pub async fn open<B: SessionBackend>(&mut self, session: &mut Session<B>, parts: &dyn Parts) {
        if !matches!(self.phase, Phase::Unopened) {
            return;
        }
        let unseen = session.next_seq() != self.next_at_start;
        let loaded = session.feed_media().await;
        self.install_source(session);
        let last = session.next_seq().saturating_sub(1);
        let loaded = match loaded {
            Ok(loaded) if !unseen => loaded,
            // A failed read of the saved index, or records appended before the session fed its
            // media: build from every record so far.
            _ => {
                self.phase = Phase::Unbuilt {
                    through: last,
                    feeds: Vec::new(),
                };
                return;
            }
        };
        let LoadedMedia {
            saved,
            through,
            feed,
        } = loaded;
        let start = match saved {
            SavedIndex::NoCapture => Some(None),
            SavedIndex::Saved(stored) => self.index_of_stored(&stored).ok().map(Some),
            SavedIndex::NotSaved
            | SavedIndex::Missing(_)
            | SavedIndex::Damaged(_)
            | SavedIndex::UnknownVersion(_) => None,
        };
        match start {
            Some(index) => {
                self.slot().index = index.map(Arc::new);
                self.phase = Phase::Current;
                self.apply(feed, last, parts);
            }
            None => {
                self.phase = Phase::Unbuilt {
                    through,
                    feeds: vec![feed],
                }
            }
        }
    }

    /// Gives `session`'s changes the index to save: the index as it stands, when it applied
    /// every record the change seals.
    fn install_source<B: SessionBackend>(&self, session: &mut Session<B>) {
        let slot = Arc::clone(&self.slot);
        let objects = Arc::clone(&self.objects);
        session.set_media_index_source(Box::new(move || {
            let slot = locked(&slot);
            if !slot.current || slot.broken.is_some() {
                return None;
            }
            let index = slot.index.as_ref()?;
            let objects: Vec<(String, MediaObject)> = locked(&objects)
                .iter()
                .map(|(digest, object)| (digest.text(), object.clone()))
                .collect();
            Some(MediaIndexSave {
                index: to_stored(index, &objects),
                through: slot.through,
            })
        }));
    }

    /// The engine's index a saved one holds, with where its media are stored.
    fn index_of_stored(&self, stored: &StoredMediaIndex) -> Result<MediaIndex, String> {
        let stored = broca_session_loop::media_index::from_stored(stored)?;
        let session = SessionMedia::from_index_state(stored.media, Arc::clone(&self.bytes))?;
        let aging = MediaAging::from_state(stored.aging)?;
        let profile = stored.limits.as_ref().map(media_profile).transpose()?;
        let mut objects = locked(&self.objects);
        for (digest, object) in stored.objects {
            objects.entry(Digest::of(&digest)).or_insert(object);
        }
        Ok(MediaIndex::new(session, aging, profile, stored.step_open))
    }

    /// Takes what `session`'s writes handed over since the last take, and applies it unless
    /// the index waits to be built.
    pub fn absorb<B: SessionBackend>(&mut self, session: &mut Session<B>, parts: &dyn Parts) {
        let feed = session.take_media();
        let last = session.next_seq().saturating_sub(1);
        match &mut self.phase {
            Phase::Unopened => {}
            Phase::Current => self.apply(feed, last, parts),
            Phase::Unbuilt { feeds, .. } | Phase::Building { feeds } => {
                if !feed.is_empty() {
                    feeds.push(feed);
                }
            }
        }
    }

    /// Applies `feed`, which the records through `last` handed over after those the index
    /// applied. A feed the index can't take breaks it.
    fn apply(&mut self, feed: MediaFeed, last: u64, parts: &dyn Parts) {
        {
            let mut objects = locked(&self.objects);
            for (digest, object) in &feed.objects {
                objects
                    .entry(Digest::of(digest))
                    .or_insert_with(|| object.clone());
            }
        }
        let mut slot = self.slot();
        slot.through = last;
        slot.current = true;
        if slot.broken.is_some() {
            return;
        }
        if let Err(reason) = self.apply_to(&mut slot, feed, parts) {
            slot.broken = Some(reason);
        }
    }

    fn apply_to(&self, slot: &mut Slot, feed: MediaFeed, parts: &dyn Parts) -> Result<(), String> {
        if let Some(seq) = feed.needs_saved_index {
            return Err(format!(
                "the session's record {seq} is of the first compaction protocol, whose effect on \
                 media only a saved media index carries, and the session has none"
            ));
        }
        if let Some(start) = &feed.start {
            // The session's first capture, or its first since its index held no media.
            slot.index = Some(Arc::new(self.index_of_scalars(start, parts)?));
        }
        let mut cut = false;
        for item in feed.items {
            let event = match item {
                MediaItem::Event(event) => event,
                MediaItem::RunStart(run) => {
                    let limits = limits_of(parts, &run.render_config)?;
                    run.with_limits(limits)
                }
            };
            let index = slot.index.as_mut().ok_or_else(|| {
                "the session's media events came before its first capture".to_owned()
            })?;
            Arc::make_mut(index).apply(&event)?;
            cut |= matches!(event, MediaEvent::Truncated { .. });
        }
        if cut {
            // A cut may have removed the records that named where some bytes are; a later
            // record naming the same media names them again.
            let held: BTreeSet<Digest> = slot
                .index
                .as_ref()
                .map(|index| {
                    index
                        .session()
                        .facts()
                        .captures
                        .into_iter()
                        .map(|capture| Digest::of(&capture.reference.sha256))
                        .collect()
                })
                .unwrap_or_default();
            locked(&self.objects).retain(|digest, _| held.contains(digest));
        }
        Ok(())
    }

    /// The index the session's first capture creates, from the scalars it kept until then.
    fn index_of_scalars(
        &self,
        start: &MediaStart,
        parts: &dyn Parts,
    ) -> Result<MediaIndex, String> {
        let limits = match &start.limits {
            LimitsSource::Known(limits) => limits.clone(),
            LimitsSource::Part(part) => limits_of(parts, part)?,
        };
        let stored = StoredMediaIndex {
            steps: start.steps,
            last_step: start.last_step.map(|step| step.0),
            step_open: start.step_open,
            batch: start.batch.clone(),
            limits: limits.map(|limits| limits.0),
            ..StoredMediaIndex::default()
        };
        self.index_of_stored(&stored)
    }

    /// Starts the one-time build, when the index waits for one: what the session handed over
    /// through the build's seq, read from its records over `records` by `build` (on a blocking
    /// thread). Returns the seq to build through, or `None` when no build is due.
    pub fn start_build(&mut self) -> Option<u64> {
        if !self.unbuilt() {
            return None;
        }
        let Phase::Unbuilt { through, feeds } = std::mem::replace(&mut self.phase, Phase::Unopened)
        else {
            unreachable!("checked above")
        };
        self.phase = Phase::Building { feeds };
        Some(through)
    }

    /// Takes in what the one-time build ended with: the index built from it, then what the
    /// session handed over meanwhile, in order.
    pub fn built(&mut self, history: Result<MediaFeed, String>, last: u64, parts: &dyn Parts) {
        let feeds = match std::mem::replace(&mut self.phase, Phase::Current) {
            Phase::Building { feeds } => feeds,
            other => {
                self.phase = other;
                return;
            }
        };
        match history {
            Ok(history) => {
                self.slot().index = None;
                self.apply(history, last, parts);
                for feed in feeds {
                    self.apply(feed, last, parts);
                }
            }
            Err(reason) => {
                let mut slot = self.slot();
                slot.through = last;
                slot.current = true;
                slot.broken.get_or_insert(reason);
            }
        }
    }

    /// What the index holds, for the operator's memory report: its handles (the shared slot,
    /// the map of where bytes are, the byte reader), and [`SessionIndex::content_bytes`].
    /// Estimated by walking it when asked, as `broca_session_store::held` estimates the
    /// store's values. A render that still holds an older copy of the engine keeps that copy
    /// alive too, which isn't counted.
    pub(crate) fn held_bytes(&self) -> usize {
        use std::mem::{size_of, size_of_val};
        let arc = 2 * size_of::<usize>();
        arc + size_of::<Mutex<Slot>>()
            + arc
            + size_of::<Mutex<BTreeMap<String, MediaObject>>>()
            + arc
            + size_of_val(&*self.bytes)
            + self.unreadable.as_ref().map_or(0, String::capacity)
            + self.content_bytes()
    }

    /// What the index holds beyond its fixed handles (see [`SessionIndex::held_parts`]).
    /// Nothing for a session that never captured a medium.
    pub(crate) fn content_bytes(&self) -> usize {
        self.held_parts().iter().map(|(_, bytes)| bytes).sum()
    }

    /// What the index holds beyond its fixed handles, by part: the engine's parts and its aging
    /// state's (see `SessionMedia::held_parts` and `MediaAging::held_parts`), the engine's
    /// inline size, the media limits in force, where each medium's bytes are, why it broke,
    /// and what waits for its one-time build.
    pub(crate) fn held_parts(&self) -> Vec<(&'static str, usize)> {
        use broca_session_store::held::{serde_heap_bytes, HeldBytes};
        let slot = self.slot();
        let mut parts = Vec::new();
        if let Some(index) = slot.index.as_ref() {
            parts.push((
                "engine, inline",
                2 * std::mem::size_of::<usize>() + std::mem::size_of::<MediaIndex>(),
            ));
            parts.extend(index.session().held_parts());
            parts.extend(index.aging().held_parts());
            parts.push(("limits", index.profile().map_or(0, serde_heap_bytes)));
        }
        parts.push(("broken", slot.broken.heap_bytes()));
        drop(slot);
        parts.push(("objects", locked(&self.objects).heap_bytes()));
        let waiting = match &self.phase {
            Phase::Unbuilt { feeds, .. } | Phase::Building { feeds } => {
                feeds.capacity() * std::mem::size_of::<MediaFeed>()
                    + feeds.iter().map(feed_heap_bytes).sum::<usize>()
            }
            Phase::Unopened | Phase::Current => 0,
        };
        parts.push(("waiting for the build", waiting));
        parts
    }

    /// What a run renders its media from: the index as it stands at each render.
    pub fn for_run(&self) -> RunMedia {
        RunMedia {
            slot: Arc::clone(&self.slot),
            unreadable: self.unreadable.clone(),
        }
    }
}

/// The heap a feed waiting for the one-time build holds: its events by their serialized walk,
/// and where its records stored their media.
fn feed_heap_bytes(feed: &MediaFeed) -> usize {
    use broca_session_store::held::{serde_heap_bytes, HeldBytes};
    let items = feed.items.capacity() * std::mem::size_of::<MediaItem>()
        + feed
            .items
            .iter()
            .map(|item| match item {
                MediaItem::Event(event) => serde_heap_bytes(event),
                MediaItem::RunStart(run) => serde_heap_bytes(&run.inputs),
            })
            .sum::<usize>();
    let start = feed
        .start
        .as_ref()
        .map_or(0, |start| start.batch.heap_bytes());
    items + start + feed.objects.heap_bytes()
}

/// `index` as the session saves it.
fn to_stored(index: &MediaIndex, objects: &[(String, MediaObject)]) -> StoredMediaIndex {
    let limits = index
        .profile()
        .and_then(|profile| serde_json::to_value(profile).ok())
        .map(MediaLimits);
    broca_session_loop::media_index::to_stored(
        index.session(),
        index.aging(),
        limits.as_ref(),
        index.step_open(),
        objects,
    )
}

/// The media limits in the render config setup part `part` holds, read through the setup parts
/// every holder shares, so a config already decoded isn't decoded again.
fn limits_of(parts: &dyn Parts, part: &PartRef) -> Result<Option<MediaLimits>, String> {
    let config = broca_session_loop::parts::shared::<serde_json::Value, _>(parts, part)
        .map_err(|error| format!("the run's render config can't be read: {error}"))?;
    Ok(limits_in(&config))
}

/// What a run of a session in the session store renders its media from: the actor's index, read
/// as it stands at each render. A cache warm that renders a recorded request again, to resend
/// its prefix, reads the same index through a clone of this.
#[derive(Clone)]
pub struct RunMedia {
    slot: Arc<Mutex<Slot>>,
    unreadable: Option<String>,
}

impl RunMedia {
    /// Media that can't be had, for `reason`: a request naming media is refused with it.
    pub fn unavailable(reason: String) -> Self {
        Self {
            slot: Arc::new(Mutex::new(Slot {
                broken: Some(reason),
                ..Slot::default()
            })),
            unreadable: None,
        }
    }

    /// The request body for `opts` under the frozen render config `config`. A prompt naming no
    /// media renders as it always does. One naming media renders from the index as it stands,
    /// or is refused by name when the index can't be had.
    pub fn render(
        &self,
        config: &serde_json::Value,
        opts: &broca_protocol::request::CallOptions,
        render_tags: bool,
        provenance: Option<&mut broca_protocol::render_provenance::RenderProvenance>,
    ) -> Result<Vec<u8>, Box<broca_protocol::ProviderError>> {
        let (index, broken) = {
            let slot = locked(&self.slot);
            (slot.index.clone(), slot.broken.clone())
        };
        match (broken.or_else(|| self.unreadable.clone()), index) {
            (None, Some(index)) => {
                index.render_with_provenance(config, opts, render_tags, provenance)
            }
            // The session captured no media, so no medium the prompt names is one of its own. A
            // compaction view's inserted image renders from the bytes the request carries
            // (`CallOptions::view_media`); any other renders as its provenance marker.
            (None, None) => uncaptured()
                .map_err(|reason| Box::new(refusal(reason)))?
                .render_with_provenance(config, opts, render_tags, provenance),
            (Some(reason), _) => broca_provider::media_request::render_without_index(
                config,
                opts,
                provenance,
                "store_media_unavailable",
                &reason,
            ),
        }
    }

    /// [`RunMedia::render`] as the provider's media renderer. Whether the render commits its
    /// aging doesn't matter: a render writes nothing back.
    pub fn renderer(&self) -> Arc<broca_core::provider::MediaRequestRenderer> {
        let media = self.clone();
        Arc::new(move |config, opts, tags, _commit, provenance| {
            media.render(config, opts, tags, provenance)
        })
    }
}

/// The index of a session that captured no media. It reads no bytes of its own: a prompt can only
/// name media it doesn't hold, a view's inserted image (whose bytes the request carries) or one
/// that renders as a marker.
fn uncaptured() -> Result<MediaIndex, String> {
    #[derive(Debug)]
    struct Nothing;
    impl MediaBytes for Nothing {
        fn read_verified(&self, reference: &MediaRef) -> Result<Vec<u8>, MediaReadError> {
            Err(MediaReadError::Corrupt(reference.sha256.clone()))
        }
    }
    Ok(MediaIndex::new(
        SessionMedia::from_index_state(Default::default(), Nothing)?,
        MediaAging::default(),
        None,
        false,
    ))
}

/// How a run gets its [`RunMedia`]: the actor sends it once the session's index is built, which
/// on the session's first run after an open without a saved index may take seconds. The actor
/// keeps answering commands meanwhile.
pub type StoreRunMediaWait = tokio::sync::oneshot::Receiver<RunMedia>;

/// Builds a run's renderer over the run's media (see [`RunMedia::renderer`]).
pub type BuildRenderer =
    dyn FnOnce(RunMedia) -> Result<Arc<broca_core::provider::MediaRequestRenderer>, String> + Send;

/// A store run's media renderer, installed on the provider when the run is admitted and ready
/// once the session's index is: each model call first waits for it (as the run's
/// `ModelCallReady`), so a run renders no request before its media can be rendered, and a
/// cancel ends the wait. When the index can't be had, the renderer is built over media that
/// can't: a request holding no media renders as it always does, and one holding media is
/// refused by name, never sent without them.
pub struct LazyRenderer {
    wait: tokio::sync::Mutex<Option<(StoreRunMediaWait, Box<BuildRenderer>)>>,
    ready: std::sync::OnceLock<Result<Arc<broca_core::provider::MediaRequestRenderer>, String>>,
}

/// The error a store run's media refusal ends its step with, naming why.
pub fn refusal(reason: String) -> broca_protocol::ProviderError {
    broca_protocol::ProviderError {
        class: broca_protocol::ErrorClass::Permanent,
        message: reason,
        provider_code: Some("store_media_unavailable".into()),
        provider_details: None,
        cause: None,
        provider_subcode: None,
        refusal: None,
        status: None,
        retry_after_secs: None,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
        account_continue: None,
    }
}

impl LazyRenderer {
    /// A renderer built by `build` once `wait` delivers the run's media.
    pub fn new(wait: StoreRunMediaWait, build: Box<BuildRenderer>) -> Arc<Self> {
        Arc::new(Self {
            wait: tokio::sync::Mutex::new(Some((wait, build))),
            ready: std::sync::OnceLock::new(),
        })
    }

    /// The renderer to install on the provider. Before the media are ready it refuses, which
    /// the run never meets: its model calls wait for [`LazyRenderer`] first.
    pub fn renderer(self: &Arc<Self>) -> Arc<broca_core::provider::MediaRequestRenderer> {
        let lazy = Arc::clone(self);
        Arc::new(
            move |frozen, opts, tags, commit, provenance| match lazy.ready.get() {
                Some(Ok(renderer)) => renderer(frozen, opts, tags, commit, provenance),
                Some(Err(reason)) => Err(Box::new(refusal(reason.clone()))),
                None => Err(Box::new(refusal(
                    "the session's media aren't ready to render yet".into(),
                ))),
            },
        )
    }
}

#[async_trait::async_trait]
impl broca_session_loop::run::ModelCallReady for LazyRenderer {
    async fn ready(&self) -> Result<(), broca_protocol::ProviderError> {
        if self.ready.get().is_none() {
            let mut wait = self.wait.lock().await;
            if let Some((media, build)) = wait.take() {
                let media = media.await.unwrap_or_else(|_| {
                    RunMedia::unavailable(
                        "the session's media index ended before it was ready".into(),
                    )
                });
                let _ = self.ready.set(build(media));
            }
        }
        match self.ready.get() {
            Some(Ok(_)) => Ok(()),
            Some(Err(reason)) => Err(refusal(reason.clone())),
            None => Err(refusal("the session's media index was never ready".into())),
        }
    }
}

/// The byte source of an index whose session's media objects can't be read: every read refuses.
#[derive(Debug)]
struct Unavailable(String);

impl MediaBytes for Unavailable {
    fn read_verified(&self, reference: &MediaRef) -> Result<Vec<u8>, MediaReadError> {
        Err(MediaReadError::Io {
            digest: reference.sha256.clone(),
            source: std::io::Error::other(self.0.clone()),
        })
    }
}

/// A session's media objects as a render reads them: by the object the session's records name
/// for the digest, checked against the reference.
struct StoreMediaBytes<R> {
    objects: Objects,
    /// Only reads: a read that panics leaves the storage handle as it found it.
    reader: std::panic::AssertUnwindSafe<MediaObjects<R>>,
}

impl<R> std::fmt::Debug for StoreMediaBytes<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StoreMediaBytes")
    }
}

impl<R: SessionBackend + Send + Sync> MediaBytes for StoreMediaBytes<R> {
    fn read_verified(&self, reference: &MediaRef) -> Result<Vec<u8>, MediaReadError> {
        use futures::FutureExt as _;
        let digest = || reference.sha256.clone();
        let object = locked(&self.objects)
            .get(&Digest::of(&reference.sha256))
            .cloned()
            .ok_or_else(|| MediaReadError::Missing(digest()))?;
        // A render runs inside a provider call and can't wait. The storage backends answer an
        // object read without waiting, so a read that would wait is refused rather than blocked.
        let read = self
            .reader
            .read(&object, reference)
            .now_or_never()
            .ok_or_else(|| MediaReadError::Io {
                digest: digest(),
                source: std::io::Error::other("the media object read would have waited"),
            })?;
        read.map_err(|error| match error {
            SessionError::Media(MediaError::Missing(_)) => MediaReadError::Missing(digest()),
            SessionError::Media(MediaError::Corrupt(_)) => MediaReadError::Corrupt(digest()),
            other => MediaReadError::Io {
                digest: digest(),
                source: std::io::Error::other(format!("{other:?}")),
            },
        })
    }
}

/// The session's writer as the actor serves a run's requests with it. Each append stores the
/// bytes of the tool media the run captured for the results it appends (see `PendingMedia`),
/// and the session's index applies what every write handed over before the run hears its write
/// went through, so the run's next request can render what it wrote.
pub struct RunAppends<'a, B: SessionBackend> {
    pub session: &'a mut Session<B>,
    /// The media the run's tool calls captured that no record stores yet.
    pub tool_media: &'a broca_session_loop::PendingMedia,
    /// The session's image index.
    pub index: &'a mut SessionIndex,
    /// The setup parts a run start's media limits are read from.
    pub parts: &'a dyn Parts,
}

impl<B: SessionBackend> RunAppends<'_, B> {
    /// After a write: the index applies what it handed over, a seal's carried records included.
    fn took<T>(&mut self, result: Result<T, SessionError>) -> Result<T, SessionError> {
        self.index.absorb(&mut *self.session, self.parts);
        result
    }
}

impl<B: SessionBackend> broca_session_loop::Writer for RunAppends<'_, B> {
    async fn responses_reasoning_removals(
        &self,
        run_id: &str,
        before_seq: Option<u64>,
    ) -> Result<broca_session_loop::responses_reasoning::ResponsesReasoningRemovals, SessionError>
    {
        broca_session_loop::Writer::responses_reasoning_removals(&*self.session, run_id, before_seq)
            .await
    }

    async fn append(&mut self, records: &[Record], barrier: bool) -> Result<u64, SessionError> {
        let appended = self
            .tool_media
            .append_records(&mut *self.session, records, barrier)
            .await;
        self.took(appended).map(|(first, _)| first)
    }

    async fn append_input_media(
        &mut self,
        records: &[Record],
        media: Vec<broca_session_store::session::NewInputMedia>,
        barrier: bool,
    ) -> Result<(u64, Vec<Record>), SessionError> {
        let appended = self
            .session
            .append_input_media(records, media, barrier)
            .await;
        self.took(appended)
    }

    async fn caught_up(&mut self) -> Result<(), SessionError> {
        broca_session_loop::Writer::caught_up(&mut *self.session).await
    }

    async fn send_records(&mut self, submission_id: &str) -> Result<Vec<Record>, SessionError> {
        broca_session_loop::Writer::send_records(&mut *self.session, submission_id).await
    }

    async fn append_with(
        &mut self,
        build: &mut (dyn for<'s> FnMut(
            &'s broca_session_store::fold::conversation::Conversation,
            &'s broca_session_store::fold::operations::Operations,
            u64,
        ) -> Vec<Record>
                  + Send),
        barrier: bool,
    ) -> Result<Vec<Record>, SessionError> {
        let records = build(
            &self.session.conversation,
            &self.session.operations,
            self.session.next_seq(),
        );
        if !records.is_empty() {
            let barrier = barrier || broca_session_loop::writer::ends_run(&records);
            broca_session_loop::Writer::append(self, &records, barrier).await?;
        }
        Ok(records)
    }

    fn conversation(&self) -> &broca_session_store::fold::conversation::Conversation {
        &self.session.conversation
    }

    fn operations(&self) -> &broca_session_store::fold::operations::Operations {
        &self.session.operations
    }

    fn session_id(&self) -> String {
        broca_session_loop::Writer::session_id(&*self.session)
    }

    fn next_seq(&self) -> u64 {
        self.session.next_seq()
    }

    fn retired_tag_floor(&self) -> u64 {
        self.session.retired_tag_floor()
    }

    fn message_id_prefix(&self) -> String {
        self.session.message_id_prefix()
    }

    async fn open_log_records(&self) -> Result<Vec<(u64, Record)>, SessionError> {
        self.session.open_log_records().await
    }

    async fn open_run_tail(
        &self,
        run_id: &str,
        first: u64,
    ) -> Result<Vec<(u64, Record)>, SessionError> {
        Ok(self.session.kept_run_tail(run_id, first))
    }

    async fn enrollment(
        &self,
    ) -> Result<Option<broca_session_store::session::EnrollmentOverlay>, SessionError> {
        broca_session_loop::Writer::enrollment(&*self.session).await
    }

    // Stores an object, not records: nothing is handed over.
    async fn put_enrollment(
        &mut self,
        entries: &broca_session_store::record::EnrolledEntries,
    ) -> Result<broca_session_store::record::EnrollmentOverlayRef, SessionError> {
        broca_session_loop::Writer::put_enrollment(&mut *self.session, entries).await
    }

    async fn run_start(
        &self,
        run_id: &str,
    ) -> Result<Option<broca_session_store::record::RunStart>, SessionError> {
        broca_session_loop::Writer::run_start(&*self.session, run_id).await
    }

    async fn window_message_whole(
        &self,
        ordinal: u64,
    ) -> Result<std::sync::Arc<broca_protocol::Message>, SessionError> {
        broca_session_loop::Writer::window_message_whole(&*self.session, ordinal).await
    }

    async fn release(&mut self, blocks: &[(u64, Vec<usize>)]) -> Result<(), SessionError> {
        broca_session_loop::Writer::release(&mut *self.session, blocks).await
    }

    async fn tool_intent(
        &self,
        call_key: &str,
    ) -> Result<Option<broca_session_store::record::ToolIntent>, SessionError> {
        broca_session_loop::Writer::tool_intent(&*self.session, call_key).await
    }

    async fn seal_at_entry(
        &mut self,
        ordinal: u64,
        carry: &broca_session_store::session::Carry<'_>,
        now_ms: u64,
    ) -> Result<(), SessionError> {
        let sealed =
            broca_session_loop::Writer::seal_at_entry(&mut *self.session, ordinal, carry, now_ms)
                .await;
        self.took(sealed)
    }

    async fn seal_ledger(
        &mut self,
        carry: &broca_session_store::session::Carry<'_>,
        now_ms: u64,
    ) -> Result<(), SessionError> {
        let sealed =
            broca_session_loop::Writer::seal_ledger(&mut *self.session, carry, now_ms).await;
        self.took(sealed)
    }

    async fn entry_by_mid(
        &self,
        mid: &str,
    ) -> Result<Option<broca_session_store::session::StoredEntry>, SessionError> {
        broca_session_loop::Writer::entry_by_mid(&*self.session, mid).await
    }

    async fn entry_at(
        &self,
        ordinal: u64,
    ) -> Result<Option<broca_session_store::session::StoredEntry>, SessionError> {
        broca_session_loop::Writer::entry_at(&*self.session, ordinal).await
    }

    async fn moved_through(&self) -> Result<Option<(String, u64)>, SessionError> {
        broca_session_loop::Writer::moved_through(&*self.session).await
    }

    async fn entries_by_mid(
        &self,
        mids: &[String],
    ) -> Result<Vec<broca_session_store::session::StoredEntry>, SessionError> {
        broca_session_loop::Writer::entries_by_mid(&*self.session, mids).await
    }

    async fn sealed_derived(
        &self,
    ) -> Result<Option<Arc<dyn std::any::Any + Send + Sync>>, SessionError> {
        broca_session_loop::Writer::sealed_derived(&*self.session).await
    }

    async fn keep_sealed_derived(
        &self,
        value: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<(), SessionError> {
        broca_session_loop::Writer::keep_sealed_derived(&*self.session, value).await
    }
}

/// The request a run of `session` sends for its step `step_id`, built offline: the prompt as the
/// run loop builds it for a provider that renders media (`offline_media_request`), rendered
/// through an index built from every record of `session` and its own media objects, read over
/// `reader`, under `frozen` with `max_output_tokens` as the step's cap. It is used to check a
/// session in the session store against the request rendered for it.
#[allow(clippy::too_many_arguments)]
pub async fn offline_render<B, R>(
    session: &Session<B>,
    reader: R,
    parts: &dyn Parts,
    bind: &BindIdentity,
    step_id: u64,
    frozen: &broca_provider::wire::FrozenRenderConfig,
    render_tags: bool,
    max_output_tokens: Option<u32>,
) -> Result<Vec<u8>, String>
where
    B: SessionBackend,
    R: SessionBackend + Send + Sync + 'static,
{
    let mut opts = broca_session_loop::run::offline_media_request(session, parts, bind, step_id)
        .await
        .map_err(|error| format!("the new run loop's request: {error:?}"))?;
    if let Some(context) = opts.step_context.as_mut() {
        context.max_output_tokens = max_output_tokens;
    }
    let last = session.next_seq().saturating_sub(1);
    let history = session
        .media_history(last)
        .await
        .map_err(|error| format!("fold the session's media: {error:?}"))?;
    let index = SessionIndex::from_history(session, reader, history, last, parts);
    if let Some(reason) = index.broken() {
        return Err(reason);
    }
    let config = serde_json::to_value(&*frozen.for_step(&opts))
        .map_err(|error| format!("encode the frozen render config: {error}"))?;
    // As the transport hands a request to its media renderer: the frozen config with the step's
    // cap applied, and the run's tag choice.
    index
        .for_run()
        .render(&config, &opts, render_tags, None)
        .map_err(|error| {
            format!(
                "the media renderer refused: {}",
                error.provider_code.as_deref().unwrap_or(&error.message)
            )
        })
}

#[cfg(test)]
#[path = "store_media/tests.rs"]
pub(crate) mod tests;
