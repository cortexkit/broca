//! The saved media index: the image engine's state, saved with the session as one session object
//! when a change writes a checkpoint, so an open folds only the records after it.
//!
//! The image engine (outside the store) decides which capture each medium in a request is,
//! whether it is still protected from aging, and whether it has aged out. Its state holds facts
//! only: per medium a reference and a place, per session a step counter, the latest tool batch's
//! call order and the aging totals. The store doesn't compute it: it hands the engine's input
//! over as `fold::media` events, and saves what the engine's owner hands back when a change
//! writes a checkpoint. The checkpoint names the object in its `media` member
//! ([`MediaIndexRef`]), together with the media fold's own state, both current through one seq.
//!
//! The object is derived. When it is missing, damaged or of a version this build doesn't know,
//! the session's index is built again from all its records, and saved at the next change. So a
//! read reports each of those as its own answer ([`SavedIndex`]), never as "no media".
//!
//! The object is the store's own encoding, versioned by an envelope: `MediaIndexObject` holds the
//! version and the encoded body, so a reader tells a version it doesn't know from damage before
//! decoding the body. It is sealed under the session's key with its name, as an import's id map
//! is (see `import`). It is not the JSON media checkpoint the old storage keeps in its own log
//! (`broca_core`'s `MediaCheckpoint`), and nothing reads one as the other.

use broca_protocol::content::{MediaRef, Role};
use broca_protocol::ids::ToolCallId;
use sha2::{Digest as _, Sha256};

use super::{Session, SessionError};
use crate::backend::SessionBackend;
use crate::checkpoint::Checkpoint;
use crate::codec::{ByteBuf, Digest, Message as _};
use crate::fold::conversation::Conversation;
use crate::fold::media::{MediaFeed, MediaFold, MediaIndexRef};
use crate::fold::operations::Operations;
use crate::message;
use crate::record::Record;

/// The only version of the saved index this build writes and reads.
pub const MEDIA_INDEX_VERSION: u32 = 1;

message! {
    /// A capture's id: the record that holds the medium (the run start for a run's inputs, the
    /// steer or delivery for accepted messages, the tool outcome for a tool result), the
    /// message's position in that record, the content block, and the part for a medium inside
    /// a tool result.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub struct StoredMediaId {
        1 req record: u64,
        2 req message: usize,
        3 req block: usize,
        4 opt part: usize,
    }
}

message! {
    /// One medium as the session recorded it.
    #[derive(Debug, Clone, PartialEq)]
    pub struct StoredCapture {
        1 req id: StoredMediaId,
        2 req reference: MediaRef,
        /// The ordinal (from 0) of the message holding it, once placed.
        3 opt ordinal: u64,
        /// For a tool's medium: the call whose result holds it.
        4 opt tool_call: ToolCallId,
        /// The step counter's value at the step that first showed it; absent while it waits for
        /// its first step.
        5 opt first_step: u64,
        /// A person sent it, rather than a tool.
        6 req person: bool,
    }
}

message! {
    /// Where one medium sits in a message skeleton.
    #[derive(Debug, Clone, PartialEq)]
    pub struct StoredSlot {
        1 req block: usize,
        /// For a medium inside a tool result block: the call the result answers, and the
        /// medium's position among the result's parts.
        2 opt call: ToolCallId,
        3 opt part: usize,
        4 req reference: MediaRef,
    }
}

message! {
    /// A message reduced to what the engine reads of it: its role, its number of content blocks,
    /// and where its media sit. Never its text.
    #[derive(Debug, Clone, PartialEq)]
    pub struct StoredSkeleton {
        1 req role: Role,
        2 req blocks: usize,
        3 rep slots: StoredSlot,
    }
}

message! {
    /// The skeleton of a placed message that holds media, by its ordinal (from 0).
    #[derive(Debug, Clone, PartialEq)]
    pub struct PlacedSkeleton {
        1 req ordinal: u64,
        2 req skeleton: StoredSkeleton,
    }
}

message! {
    /// The skeleton of an accepted message not placed yet, by the record that accepted it and
    /// its position there.
    #[derive(Debug, Clone, PartialEq)]
    pub struct HeldSkeleton {
        1 req record: u64,
        2 req message: usize,
        3 req skeleton: StoredSkeleton,
    }
}

message! {
    /// The captures of one accepted message not placed yet; none for a message without media,
    /// which still takes a position when the messages are placed.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct AcceptedMessage {
        1 rep ids: StoredMediaId,
    }
}

message! {
    /// Media a first-protocol compaction put in its replacement messages, and the ordinals the
    /// replacement covers.
    #[derive(Debug, Clone, PartialEq)]
    pub struct StoredReplacement {
        1 rep references: MediaRef,
        2 req from: u64,
        3 req to: u64,
    }
}

message! {
    /// One medium of a prompt as a selection binds it.
    #[derive(Debug, Clone, PartialEq)]
    pub struct StoredBinding {
        1 req id: StoredMediaId,
        2 req message: usize,
        3 req block: usize,
        4 opt part: usize,
        5 req reference: MediaRef,
        6 req tool: bool,
        7 req protected: bool,
    }
}

message! {
    /// The media a first-protocol compaction left showing, restored from a saved index rather
    /// than computed: only a session converted from the old storage has one.
    #[derive(Debug, Clone, PartialEq)]
    pub struct StoredSelection {
        1 rep bindings: StoredBinding,
    }
}

message! {
    /// One occurrence of a capture in an assembled prompt: a replacement message can repeat a
    /// captured medium, and each repetition counts against the limits.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub struct StoredOccurrence {
        1 req capture: StoredMediaId,
        2 req repetition: u32,
    }
}

message! {
    /// An occurrence shown in a request, with what it costs and whether it is protected.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ShownOccurrence {
        1 req occurrence: StoredOccurrence,
        2 req bytes: u64,
        3 req tokens: u64,
        4 req protected: bool,
    }
}

message! {
    /// The aging state: what is shown and its running totals, the unprotected occurrences
    /// oldest first, and those aged out, which stay aged.
    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub struct StoredAging {
        1 rep shown: ShownOccurrence,
        2 rep unprotected: StoredOccurrence,
        3 rep aged: StoredOccurrence,
        4 req bytes: u64,
        5 req tokens: u64,
    }
}

message! {
    /// The image engine's state, as data: what a session's saved media index holds. Its owner
    /// converts it to and from the engine's own types; the store reads none of it but the
    /// checks [`StoredMediaIndex::check`] makes.
    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct StoredMediaIndex {
        /// The step counter, the latest step started in the current run, and whether a step is
        /// open, as the media fold keeps them (see `fold::media::MediaFold`).
        1 req steps: u64,
        2 opt last_step: u64,
        3 req step_open: bool,
        /// The latest tool batch's calls in planned order.
        4 rep batch: ToolCallId,
        /// Every capture, in id order.
        5 rep captures: StoredCapture,
        /// Captures waiting for their first step, which then protects them.
        6 rep pending: StoredMediaId,
        /// Accepted messages not placed yet, in the order accepted.
        7 rep accepted: AcceptedMessage,
        /// Captures protected from aging until the next step starts.
        8 rep protected: StoredMediaId,
        /// Skeletons of the placed messages that hold media.
        9 rep placed: PlacedSkeleton,
        /// Skeletons of the accepted messages not placed yet that hold media.
        10 rep held: HeldSkeleton,
        11 rep replacements: StoredReplacement,
        12 opt selection: StoredSelection,
        /// The last ordinal a restored first-protocol compaction covers.
        13 opt covered: u64,
        /// A first-protocol compaction view was in force when the index was saved.
        14 req has_view: bool,
        /// The media limits in force: the `media` member of the run's render config.
        15 opt limits: serde_json::Value,
        16 req aging: StoredAging,
    }
}

message! {
    /// The saved index's envelope: its version, and its body encoded as that version says.
    #[derive(Debug, Clone, PartialEq)]
    struct MediaIndexObject {
        1 req version: u32,
        2 req body: ByteBuf,
    }
}

impl StoredMediaIndex {
    /// Whether the index holds nothing but the media fold's scalars: no capture, no accepted
    /// message waiting for its place, no aging. A session whose engine holds no more than that
    /// is saved as the scalars alone, and hands nothing over again until it captures a medium.
    pub fn holds_no_media(&self) -> bool {
        self.captures.is_empty()
            && self.pending.is_empty()
            && self.accepted.is_empty()
            && self.protected.is_empty()
            && self.placed.is_empty()
            && self.held.is_empty()
            && self.replacements.is_empty()
            && self.selection.is_none()
            && self.covered.is_none()
            && !self.has_view
            && self.aging == StoredAging::default()
    }

    /// Refuses an index whose parts don't agree: an id that names no capture, a capture from
    /// a record after `through`, a slot past its message's last block.
    pub fn check(&self, through: u64) -> Result<(), String> {
        let ids: std::collections::BTreeSet<StoredMediaId> =
            self.captures.iter().map(|capture| capture.id).collect();
        if ids.len() != self.captures.len() {
            return Err("a capture id repeats".into());
        }
        if let Some(capture) = self.captures.iter().find(|c| c.id.record > through) {
            return Err(format!(
                "a capture of record {} in an index through {through}",
                capture.id.record
            ));
        }
        let named = self
            .pending
            .iter()
            .chain(&self.protected)
            .chain(self.accepted.iter().flat_map(|message| &message.ids))
            .chain(
                self.selection
                    .iter()
                    .flat_map(|selection| selection.bindings.iter().map(|binding| &binding.id)),
            );
        for id in named {
            if !ids.contains(id) {
                return Err(format!("{id:?} names no capture"));
            }
        }
        let skeletons = self
            .placed
            .iter()
            .map(|placed| &placed.skeleton)
            .chain(self.held.iter().map(|held| &held.skeleton));
        for skeleton in skeletons {
            if skeleton
                .slots
                .iter()
                .any(|slot| slot.block >= skeleton.blocks)
            {
                return Err("a slot past its message's last block".into());
            }
        }
        Ok(())
    }
}

/// An index for a change to save, handed over by its owner (see
/// [`Session::set_media_index_source`]).
#[derive(Debug, Clone, PartialEq)]
pub struct MediaIndexSave {
    pub index: StoredMediaIndex,
    /// The seq of the last record whose events the index applied. A change saves it only when
    /// that is the record before the change's own: an index behind the session can't stand for
    /// the records the change seals.
    pub through: u64,
}

/// Asked for the engine's current index whenever a change writes a checkpoint; `None` when it
/// has none to give.
pub type MediaIndexSource = Box<dyn FnMut() -> Option<MediaIndexSave> + Send + Sync>;

/// What a session's checkpoint holds for its media index, as one answer per case.
#[derive(Debug, Clone, PartialEq)]
pub enum SavedIndex {
    /// The session had captured no medium through the checkpoint: there is no index, and the
    /// engine's starts at the first capture, from that feed's `start`.
    NoCapture,
    /// The index saved through the checkpoint.
    Saved(Box<StoredMediaIndex>),
    /// The checkpoint saves no index of a session that may hold media: it was written before
    /// saved indexes existed, by an older build, or by an undo or a recovery cut, which leave
    /// the engine's state to be built again. The index is built once from all the session's
    /// records through the checkpoint, by the session's owner, at the open that finds this
    /// answer; the next seal saves the index then held, and the open after it reads that one.
    NotSaved,
    /// The checkpoint names an object that isn't stored.
    Missing(String),
    /// The checkpoint names an object that doesn't open under the session's key, or doesn't
    /// decode, or whose parts don't agree.
    Damaged(String),
    /// The checkpoint names an index of a version this build doesn't read.
    UnknownVersion(u32),
}

/// What an open hands over about the session's media: the saved index (or why there is none),
/// the seq it is current through, and what the open log's records after that seq handed over.
/// No record is read for it twice: the feed is what the load's own fold produced.
#[derive(Debug, Clone, PartialEq)]
pub struct LoadedMedia {
    pub saved: SavedIndex,
    pub through: u64,
    pub feed: MediaFeed,
}

/// What the open log's checkpoint says about the media fold.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Named {
    /// The session's first log, whose checkpoint is empty state: its records are the whole
    /// history.
    FirstLog,
    /// A later log whose checkpoint has no member, or one not current through the checkpoint.
    Unknown {
        through: u64,
    },
    Ref(MediaIndexRef),
}

/// A session's media state: the media fold, what the open log's checkpoint names, and what the
/// fold handed over and nobody took yet.
pub(super) struct MediaState {
    pub(super) fold: MediaFold,
    named: Named,
    /// The body digest of the object `named` names, when this writer read or wrote it.
    digest: Option<Digest>,
    /// Whether the fold handed anything over since `named` was current.
    changed: bool,
    /// What the fold handed over since it was last taken, while the session feeds its media.
    queue: Option<MediaFeed>,
    /// What the load's fold handed over for the open log, until it is taken.
    loaded: Option<MediaFeed>,
    source: Option<MediaIndexSource>,
}

impl Default for MediaState {
    fn default() -> Self {
        Self {
            fold: MediaFold::default(),
            named: Named::FirstLog,
            digest: None,
            changed: false,
            queue: None,
            loaded: None,
            source: None,
        }
    }
}

/// The media fold of a load: started from what the checkpoint names, it folds the open log's
/// records after the seq that is current through.
pub(super) struct Loading {
    state: MediaState,
    through: u64,
    feed: MediaFeed,
}

impl Loading {
    pub(super) fn new(checkpoint: &Checkpoint) -> Self {
        let first_log = checkpoint.covers_through == 0
            && checkpoint.ledger_base.is_none()
            && checkpoint.transcript_base.is_none();
        let (fold, named, through) = match &checkpoint.media {
            Some(media) if media.through == checkpoint.covers_through => {
                (media.fold.clone(), Named::Ref(media.clone()), media.through)
            }
            None if first_log => (MediaFold::default(), Named::FirstLog, 0),
            _ => (
                MediaFold::unknown(&checkpoint.operational),
                Named::Unknown {
                    through: checkpoint.covers_through,
                },
                checkpoint.covers_through,
            ),
        };
        Self {
            state: MediaState {
                fold,
                named,
                ..MediaState::default()
            },
            through,
            feed: MediaFeed::default(),
        }
    }

    /// Folds one record of the open log, with the store's folds as they stand before it.
    pub(super) fn note(
        &mut self,
        seq: u64,
        record: &Record,
        conversation: &Conversation,
        operations: &Operations,
    ) {
        if seq > self.through {
            self.state
                .fold
                .apply(seq, record, conversation, operations, &mut self.feed);
        }
    }

    pub(super) fn finish(mut self) -> MediaState {
        self.state.fold.flush(&mut self.feed);
        self.state.changed = !self.feed.is_empty();
        self.state.loaded = Some(self.feed);
        self.state
    }
}

impl MediaState {
    /// Takes what an append's records handed over.
    pub(super) fn handed(&mut self, feed: MediaFeed) {
        if feed.is_empty() {
            return;
        }
        self.changed = true;
        if let Some(queue) = self.queue.as_mut() {
            queue.extend(feed);
        }
    }

    /// The seq the checkpoint's media state is current through.
    fn through(&self) -> u64 {
        match &self.named {
            Named::FirstLog => 0,
            Named::Unknown { through } => *through,
            Named::Ref(media) => media.through,
        }
    }

    fn named_object(&self) -> Option<&str> {
        match &self.named {
            Named::Ref(media) => media.object.as_deref(),
            _ => None,
        }
    }
}

/// Whether `envelope` (a saved index's object, opened) is whole: it decodes, and a body of the
/// version this build reads decodes too. A version this build doesn't read is a newer build's
/// index, not damage.
pub(super) fn is_whole(envelope: &[u8]) -> bool {
    match MediaIndexObject::decode(envelope) {
        Ok(envelope) if envelope.version == MEDIA_INDEX_VERSION => {
            StoredMediaIndex::decode(&envelope.body.0).is_ok()
        }
        Ok(_) => true,
        Err(_) => false,
    }
}

/// What a change writes for the media index, which its commit installs.
pub(super) struct MediaCommit {
    named: Named,
    fold: MediaFold,
    digest: Option<Digest>,
    /// What the change's own records handed over: a revert's cut and its new run start.
    feed: MediaFeed,
}

impl MediaCommit {
    /// The checkpoint member the change writes, if it can say where the fold stands.
    pub(super) fn member(&self) -> Option<MediaIndexRef> {
        match &self.named {
            Named::Ref(media) => Some(media.clone()),
            _ => None,
        }
    }

    /// Folds a record the change writes into its new log after the checkpoint, with the store's
    /// folds as they stand before it.
    pub(super) fn fold(
        &mut self,
        seq: u64,
        record: &Record,
        conversation: &Conversation,
        operations: &Operations,
    ) {
        self.fold
            .apply(seq, record, conversation, operations, &mut self.feed);
    }

    /// The change's records are all folded.
    pub(super) fn flush(&mut self) {
        self.fold.flush(&mut self.feed);
    }
}

impl MediaState {
    /// Installs what a committed change wrote.
    pub(super) fn install(&mut self, commit: MediaCommit) {
        self.fold = commit.fold;
        self.named = commit.named;
        self.digest = commit.digest;
        self.changed = !commit.feed.is_empty();
        if let Some(queue) = self.queue.as_mut() {
            queue.extend(commit.feed);
        }
    }
}

/// The media fold a recovery continues from the damaged log's checkpoint, and the seq that is
/// current through, as a load would start it; `None` when the checkpoint names no fold state.
pub(super) fn recovery_start(checkpoint: &Checkpoint) -> Option<(MediaFold, u64, Option<String>)> {
    let loading = Loading::new(checkpoint);
    match loading.state.named {
        Named::Unknown { .. } => None,
        Named::FirstLog => Some((loading.state.fold, loading.through, None)),
        Named::Ref(media) => Some((loading.state.fold, loading.through, media.object)),
    }
}

/// What a recovery writes: the fold it continued over the records it kept, current through the
/// recovery's intent, with no saved index, since the engine's state after the cut isn't the
/// store's to compute. `fold` is `None` when the damaged log's checkpoint named no fold state.
pub(super) fn recovery_commit(fold: Option<MediaFold>, intent_seq: u64) -> MediaCommit {
    let named = match &fold {
        Some(fold) => Named::Ref(MediaIndexRef {
            through: intent_seq,
            object: None,
            fold: fold.clone(),
        }),
        None => Named::Unknown {
            through: intent_seq,
        },
    };
    MediaCommit {
        named,
        fold: fold.unwrap_or_default(),
        digest: None,
        feed: MediaFeed::default(),
    }
}

impl<B: SessionBackend> Session<B> {
    /// Loads the session as [`Session::load`] does, and starts feeding its media (see
    /// [`Session::feed_media`]).
    pub async fn load_feeding_media(
        backend: B,
        addr: crate::backend::SessionAddr,
        triple: &crate::head::SessionTriple,
        deployment: &crate::keys::DeploymentKey,
    ) -> Result<(Self, LoadedMedia), SessionError> {
        let mut session = Self::load(backend, addr, triple, deployment).await?;
        let loaded = session.feed_media().await?;
        Ok((session, loaded))
    }

    /// Starts handing over what the media fold produces, and returns what the load produced:
    /// the saved index (or why there is none), the seq it is current through, and the feed of
    /// the open log's records after it. From now on, the records of every write other than
    /// [`Session::append_feed`] (a seal's carried records, a revert, an import, a plain
    /// append) queue their feed for [`Session::take_media`]. Call it once, right after the
    /// session is loaded or created, before anything is appended: what was appended before is
    /// in no feed.
    pub async fn feed_media(&mut self) -> Result<LoadedMedia, SessionError> {
        let through = self.media.through();
        let (saved, digest) = self.saved_media_index().await?;
        if digest.is_some() {
            self.media.digest = digest;
        }
        self.media.queue = Some(MediaFeed::default());
        Ok(LoadedMedia {
            saved,
            through,
            feed: self.media.loaded.take().unwrap_or_default(),
        })
    }

    /// Appends as [`Session::append`] does, and returns what the batch's records handed over to
    /// the image engine with the seq of its first record. The engine applies it before the
    /// caller answers the run that wrote the batch, so a request holds every record its run
    /// wrote.
    pub async fn append_feed(
        &mut self,
        records: &[Record],
        barrier: bool,
    ) -> Result<(u64, MediaFeed), SessionError> {
        let (first, feed) = self.append_folding(records, barrier).await?;
        self.media.changed |= !feed.is_empty();
        Ok((first, feed))
    }

    /// What every write since the last take handed over, while the session feeds its media
    /// (see [`Session::feed_media`]); empty otherwise.
    pub fn take_media(&mut self) -> MediaFeed {
        self.media
            .queue
            .as_mut()
            .map(std::mem::take)
            .unwrap_or_default()
    }

    /// Sets where a change gets the image engine's current index to save with its checkpoint.
    /// A change that gets none, or one behind the session, saves the media fold's state only,
    /// and names the index it saved before when nothing was handed over since.
    pub fn set_media_index_source(&mut self, source: MediaIndexSource) {
        self.media.source = Some(source);
    }

    /// The media fold as it stands.
    pub fn media_fold(&self) -> &MediaFold {
        &self.media.fold
    }

    /// The media index the open log's checkpoint names, read and checked, or why there is none.
    /// Only a backend failure is an error: a missing, damaged or unknown index is an answer.
    pub async fn read_media_index(&self) -> Result<SavedIndex, SessionError> {
        Ok(self.saved_media_index().await?.0)
    }

    /// [`Session::read_media_index`], with the digest of the body read.
    async fn saved_media_index(&self) -> Result<(SavedIndex, Option<Digest>), SessionError> {
        let media = match &self.media.named {
            Named::FirstLog => return Ok((SavedIndex::NoCapture, None)),
            Named::Unknown { .. } => return Ok((SavedIndex::NotSaved, None)),
            Named::Ref(media) => media,
        };
        let Some(name) = media.object.as_deref() else {
            let saved = if media.fold.has_captured() {
                SavedIndex::NotSaved
            } else {
                SavedIndex::NoCapture
            };
            return Ok((saved, None));
        };
        self.storage_read()?;
        let Some(sealed) = self.backend.get_object(&self.addr, name).await? else {
            return Ok((SavedIndex::Missing(name.to_owned()), None));
        };
        let damaged = |reason: String| Ok((SavedIndex::Damaged(format!("{name}: {reason}")), None));
        let Some(envelope) = self.key.frame_key().open_unit(name.as_bytes(), &sealed) else {
            return damaged("doesn't open under the session key".into());
        };
        let envelope = match MediaIndexObject::decode(&envelope) {
            Ok(envelope) => envelope,
            Err(error) => return damaged(format!("doesn't decode: {error}")),
        };
        if envelope.version != MEDIA_INDEX_VERSION {
            return Ok((SavedIndex::UnknownVersion(envelope.version), None));
        }
        let index = match StoredMediaIndex::decode(&envelope.body.0) {
            Ok(index) => index,
            Err(error) => return damaged(format!("doesn't decode: {error}")),
        };
        if let Err(reason) = index.check(media.through) {
            return damaged(reason);
        }
        let digest = Digest(Sha256::digest(&envelope.body.0).into());
        Ok((SavedIndex::Saved(Box::new(index)), Some(digest)))
    }

    /// The media fold and checkpoint member a change starts from: the fold as it stands after
    /// the change's intent, and the object the open log's checkpoint names.
    pub(super) fn media_commit(&self) -> (MediaCommit, Option<String>) {
        let commit = MediaCommit {
            named: self.media.named.clone(),
            fold: self.media.fold.clone(),
            digest: self.media.digest,
            feed: MediaFeed::default(),
        };
        (commit, self.media.named_object().map(str::to_owned))
    }

    /// What a seal saves of the session's media, current through the seal's intent at
    /// `intent_seq`: the media fold's state, and the engine's index when the session has
    /// captured something. The index is asked of the source (see
    /// [`Session::set_media_index_source`]) and written as a session object under the change's
    /// name `change`, unless it equals the one already saved, which is named again and not
    /// written. Without an index current through the intent, the one saved before is named
    /// again only if nothing was handed over since. Returns the commit and the object no
    /// checkpoint names any more, which the change's deletions take.
    pub(super) async fn media_for_seal(
        &mut self,
        change: &str,
        intent_seq: u64,
    ) -> Result<(MediaCommit, Option<String>), SessionError> {
        let (mut commit, previous) = self.media_commit();
        let save = self
            .media
            .source
            .as_mut()
            .and_then(|source| source())
            .filter(|save| save.through + 1 == intent_seq);
        let mut object = None;
        commit.digest = None;
        if commit.fold.has_captured() {
            match save {
                // An engine that holds nothing but the scalars is saved as the scalars, and the
                // session hands nothing over again until its next capture.
                Some(save) if save.index.holds_no_media() => {
                    commit.fold = MediaFold::from_scalars(
                        save.index.steps,
                        save.index.last_step,
                        save.index.step_open,
                        save.index.batch,
                    );
                }
                Some(save) => {
                    let body = save.index.encode();
                    let digest = Digest(Sha256::digest(&body).into());
                    if previous.is_some() && self.media.digest == Some(digest) {
                        object.clone_from(&previous);
                    } else {
                        let name = format!("{change}/mi");
                        let envelope = MediaIndexObject {
                            version: MEDIA_INDEX_VERSION,
                            body: ByteBuf(body),
                        }
                        .encode();
                        let sealed = self.key.frame_key().seal_unit(name.as_bytes(), &envelope);
                        self.backend
                            .put_object(&self.addr, &name, &sealed, self.version)
                            .await?;
                        object = Some(name);
                    }
                    commit.digest = Some(digest);
                }
                None if !self.media.changed && previous.is_some() => {
                    object.clone_from(&previous);
                    commit.digest = self.media.digest;
                }
                None => {}
            }
        }
        commit.named = Named::Ref(MediaIndexRef {
            through: intent_seq,
            object: object.clone(),
            fold: commit.fold.clone(),
        });
        let replaced = previous.filter(|previous| object.as_deref() != Some(previous.as_str()));
        Ok((commit, replaced))
    }
}

impl MediaCommit {
    /// What an undo saves of the session's media: the media fold's state after the undo's
    /// record at `reverted_seq`, which this commit folded, with no index. The engine's state
    /// after the cut isn't the store's to compute, so a session that captured something and
    /// closes before its next seal builds its index once from all its records when it opens
    /// again; the next seal saves the index the engine holds then.
    pub(super) fn after_revert(&mut self, reverted_seq: u64) {
        self.named = Named::Ref(MediaIndexRef {
            through: reverted_seq,
            object: None,
            fold: self.fold.clone(),
        });
        self.digest = None;
    }
}

#[cfg(test)]
mod tests;
