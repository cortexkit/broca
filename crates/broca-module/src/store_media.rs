//! Media in the requests of a run on a session kept in the new storage.
//!
//! A run renders a request holding media exactly as today's runs do: through
//! `broca_provider::media_request::LiveMedia`, over the projection `broca_core::media_request`
//! folds from today's records. A session in the new storage has no today's records, so they are
//! rebuilt from its own by the converter's inverse (`broca_session_loop::inverse`), which turns
//! each record back into the record today's storage would have written. Nothing else differs:
//! which media the session captured and where, which of them a person sent and so are kept out of
//! aging, where each sits in the prompt, which a compaction view still shows, and how many older
//! ones are aged out all come from the same fold. Only the bytes are read from somewhere else,
//! the session's own media objects.
//!
//! **The fold.** The actor keeps one [`MediaFold`] for its session's life. It is built from the
//! session's first record when the first run that may render media starts, on a blocking thread
//! while the actor keeps answering commands, then takes in each batch the session appends, so a
//! later run starts from where the last one left off rather than from the start. Rebuilt records
//! are numbered 1, 2, ... in order, which is the numbering the fold's positions use; a record the
//! inverse leaves out (a seal's bookkeeping, a facts record) takes no number.
//!
//! **A run's view.** A run gets the fold's seed as it stood when the fold was handed to it, and
//! reads the records rebuilt after it, numbered on from the seed, through the [`Shared`] state the
//! actor fills before it answers each of the run's appends. So a request the run renders holds
//! every record the run's own copy of the session holds. Until the fold is handed over, each of
//! the run's model calls waits for it ([`LazyRenderer`]), and a cancel ends the wait.
//!
//! **Failure.** A record the inverse or the fold can't take in breaks the fold for the rest of
//! the actor's life, naming the record. A request holding media is then refused by name before
//! anything is sent, as it is when the session has no way to read its media objects. It is never
//! sent without its media.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use broca_core::media_request::{MediaCheckpoint, MediaSeedFold};
use broca_core::media_store::{MediaBytes, MediaReadError};
use broca_protocol::{BindIdentity, MediaRef};
use broca_session_loop::inverse::Inverse;
use broca_session_loop::parts::Parts;
use broca_session_store::backend::SessionBackend;
use broca_session_store::record::{MediaObject, Record};
use broca_session_store::session::{MediaError, MediaObjects, Session, SessionError};
use broca_wal::{FramedRecord, WalRecord};

/// What the actor and its running run share.
#[derive(Debug, Default, Clone)]
pub struct Shared {
    /// Today's records rebuilt since the running run's seed, oldest first, from the seed's
    /// position + 1. The run drops each once it has read past it.
    frames: Vec<FramedRecord>,
    /// The object each medium's bytes are, by digest: the first record that says where.
    objects: BTreeMap<String, MediaObject>,
    /// Why the fold stopped, once it did.
    broken: Option<String>,
    /// Whether a run has started since the fold was built: only then are rebuilt records kept
    /// for a run to read. The fold built at the first run start keeps none of the history.
    reading: bool,
}

/// One session's media fold, kept by its actor.
pub struct MediaFold {
    inverse: Inverse,
    seed: MediaSeedFold,
    /// Run starts need the session's identity, which today's run start carries and the new
    /// storage keeps once per session.
    session: BindIdentity,
    shared: Arc<Mutex<Shared>>,
}

impl MediaFold {
    /// A fold at the start of `session`'s log, before its first record.
    pub fn new(session: BindIdentity) -> Self {
        Self {
            inverse: Inverse::new(),
            seed: MediaSeedFold::default(),
            session,
            shared: Arc::default(),
        }
    }

    /// The seq of the next record of the session the fold takes in.
    pub fn next_seq(&self) -> u64 {
        self.inverse.next_seq()
    }

    fn shared(&self) -> std::sync::MutexGuard<'_, Shared> {
        self.shared
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Why the fold stopped, once it did.
    pub fn broken(&self) -> Option<String> {
        self.shared().broken.clone()
    }

    /// Stops the fold for good, naming why.
    pub fn stop(&self, reason: String) {
        let mut shared = self.shared();
        shared.broken.get_or_insert(reason);
    }

    /// Takes in `records`, the session's records in seq order from the fold's next seq on. Each
    /// batch the session appended must come whole, as the inverse reads it. Records the fold
    /// already took are passed over; a gap stops the fold.
    pub fn take(&mut self, records: &[(u64, Record)], parts: &dyn Parts) {
        if self.broken().is_some() {
            return;
        }
        if let Err(reason) = self.try_take(records, parts) {
            self.stop(reason);
        }
    }

    fn try_take(&mut self, records: &[(u64, Record)], parts: &dyn Parts) -> Result<(), String> {
        let next = self.inverse.next_seq();
        let fresh: Vec<&(u64, Record)> = records.iter().filter(|(seq, _)| *seq >= next).collect();
        let Some((first, _)) = fresh.first() else {
            return Ok(());
        };
        if fresh
            .iter()
            .enumerate()
            .any(|(index, (seq, _))| *seq != first + index as u64)
            || *first != next
        {
            return Err(format!(
                "the session's media fold expected record {next} and was given records from {first}"
            ));
        }
        let batch: Vec<Record> = fresh.iter().map(|(_, record)| record.clone()).collect();
        let rebuilt = self
            .inverse
            .push(*first, &batch, &|part| {
                broca_session_loop::parts::value(parts, part)
            })
            .map_err(|error| {
                format!("the session's media fold can't rebuild today's records: {error}")
            })?;
        let mut shared = self
            .shared
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for (_, record) in &fresh {
            for (sha256, object) in broca_session_store::session::record_objects(record) {
                shared.objects.entry(sha256).or_insert(object);
            }
        }
        for mut value in rebuilt {
            if value.get("type").and_then(serde_json::Value::as_str) == Some("run_started") {
                if let Some(object) = value.as_object_mut() {
                    object.insert(
                        "session".into(),
                        serde_json::to_value(&self.session).map_err(|error| error.to_string())?,
                    );
                }
            }
            let record: WalRecord = serde_json::from_value(value).map_err(|error| {
                format!("the session's media fold rebuilt a record today's log can't hold: {error}")
            })?;
            let seq = self.seed.at_seq() + 1;
            self.seed.fold(seq, &record).map_err(|error| {
                format!("the session's media fold refused record {seq}: {error}")
            })?;
            if shared.reading {
                shared.frames.push(FramedRecord {
                    seq,
                    fence: 0,
                    record,
                });
            }
        }
        Ok(())
    }

    /// The records of `session` the fold hasn't taken in yet, in seq order. A read that fails
    /// stops the fold.
    pub async fn unread<B: SessionBackend>(&self, session: &Session<B>) -> Vec<(u64, Record)> {
        let last = session.next_seq().saturating_sub(1);
        let next = self.next_seq();
        if self.broken().is_some() || next > last {
            return Vec::new();
        }
        session
            .records_between(next, last)
            .await
            .unwrap_or_else(|error| {
                self.stop(format!(
                "the session's media fold couldn't read records {next} through {last}: {error:?}"
            ));
                Vec::new()
            })
    }

    /// Brings the fold up to `session`'s last record.
    pub async fn catch_up<B: SessionBackend>(&mut self, session: &Session<B>, parts: &dyn Parts) {
        let unread = self.unread(session).await;
        self.take(&unread, parts);
    }

    /// Takes in `records`, which the session's writer just appended from seq `first` on, and
    /// before them whatever the actor appended since the fold last took anything (a steer, a
    /// queued send). Those come from the records the writer keeps in memory from its latest run's
    /// start on, so taking an append never reads the open log. A fold behind what the writer
    /// keeps would have missed records outside the run; it never should be, and stops, naming
    /// the gap, rather than read the log.
    pub fn take_appended<B: SessionBackend>(
        &mut self,
        session: &Session<B>,
        first: u64,
        records: &[Record],
        parts: &dyn Parts,
    ) {
        if self.broken().is_some() {
            return;
        }
        let next = self.next_seq();
        if next < first {
            match session.kept_from(next) {
                Some(kept) => {
                    let before: Vec<(u64, Record)> = kept
                        .into_iter()
                        .take_while(|(seq, _)| *seq < first)
                        .collect();
                    self.take(&before, parts);
                }
                None => {
                    self.stop(format!(
                        "the session's media fold expected record {next}, which its writer no                          longer keeps, before the records appended from {first}"
                    ));
                    return;
                }
            }
        }
        let appended: Vec<(u64, Record)> = (first..).zip(records.iter().cloned()).collect();
        self.take(&appended, parts);
    }

    /// What a run starting now renders its media from: the seed through the session's records
    /// taken so far, and from here on the records the fold takes in. `bytes` reads the session's
    /// media objects, when the actor can read them apart from its writer.
    pub fn for_run(&self, bytes: Option<Arc<dyn MediaBytes>>) -> StoreRunMedia {
        let mut shared = self.shared();
        // Records taken before this run are in its seed; a run before it read what it needed.
        shared.frames.clear();
        shared.reading = true;
        StoreRunMedia {
            seed: match &shared.broken {
                Some(reason) => Err(reason.clone()),
                None => Ok(self.seed.seed()),
            },
            shared: Arc::clone(&self.shared),
            bytes,
        }
    }

    /// The byte source a run reads this session's media through, over `reader`.
    pub fn bytes<R: SessionBackend + Send + Sync + 'static>(
        &self,
        reader: MediaObjects<R>,
    ) -> Arc<dyn MediaBytes> {
        Arc::new(StoreMediaBytes {
            shared: Arc::clone(&self.shared),
            reader: std::panic::AssertUnwindSafe(reader),
        })
    }
}

/// What a run of a session in the new storage renders its media from, handed to the executor
/// with the run (see [`MediaFold::for_run`]).
pub struct StoreRunMedia {
    seed: Result<MediaCheckpoint, String>,
    shared: Arc<Mutex<Shared>>,
    bytes: Option<Arc<dyn MediaBytes>>,
}

/// How a run gets its [`StoreRunMedia`]: the actor sends it once the session's media fold is
/// ready, which on the session's first run may take seconds while the fold is built from the
/// session's records. The actor keeps answering commands meanwhile.
pub type StoreRunMediaWait = tokio::sync::oneshot::Receiver<StoreRunMedia>;

/// Builds a run's renderer from what [`StoreRunMedia::renderer_inputs`] gives: the executor's
/// `LiveMedia` over that byte source, seed and tail reader.
pub type BuildRenderer = dyn FnOnce(
        Arc<dyn MediaBytes>,
        MediaCheckpoint,
        Box<ReadTail>,
    ) -> Result<Arc<broca_core::provider::MediaRequestRenderer>, String>
    + Send;

/// A store run's media renderer, installed on the provider when the run is admitted and ready
/// once the session's media fold is: each model call first waits for it (as the run's
/// `ModelCallReady`), so a run renders no request before its media can be rendered, and a
/// cancel ends the wait. When the fold can't be had, the renderer is built over no captures and
/// a record reader that fails naming why: a request holding no media renders as it always does,
/// and one holding media is refused by name, never sent without them.
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
                let inputs = match media.await {
                    Ok(media) => media.renderer_inputs(),
                    Err(_) => Err("the session's media fold ended before it was ready".into()),
                };
                let built = match inputs {
                    Ok((bytes, seed, tail)) => build(bytes, seed, tail),
                    Err(reason) => {
                        let unreadable = reason.clone();
                        build(
                            Arc::new(Unavailable(reason.clone())),
                            MediaCheckpoint::empty(),
                            Box::new(move |_| Err(unreadable.clone())),
                        )
                    }
                };
                let _ = self.ready.set(built);
            }
        }
        match self.ready.get() {
            Some(Ok(_)) => Ok(()),
            Some(Err(reason)) => Err(refusal(reason.clone())),
            None => Err(refusal("the session's media fold was never ready".into())),
        }
    }
}

/// The records rebuilt after a run's seed, from the seq after the one given through the latest
/// the actor took in: the reader `LiveMedia` folds before each request it renders.
pub type ReadTail = dyn Fn(u64) -> Result<Vec<FramedRecord>, String> + Send + Sync;

/// What a run's `LiveMedia` is built from: the byte source, the seed and the tail reader.
pub type RendererInputs = (Arc<dyn MediaBytes>, MediaCheckpoint, Box<ReadTail>);

impl StoreRunMedia {
    /// A fixed view for rebuilding a recorded request. Its tail reader must not discard records
    /// the live run's independent renderer still needs.
    pub(crate) fn snapshot(&self) -> Self {
        Self {
            seed: self.seed.clone(),
            shared: Arc::new(Mutex::new(self.shared.lock().unwrap().clone())),
            bytes: self.bytes.clone(),
        }
    }

    /// The byte source, the seed and the tail reader a run's `LiveMedia` is built from, or why
    /// the run can't render media.
    pub fn renderer_inputs(self) -> Result<RendererInputs, String> {
        let seed = self.seed?;
        let bytes = self.bytes.ok_or_else(|| {
            "the session's media objects can't be read apart from its writer, so its media \
             can't be rendered"
                .to_owned()
        })?;
        let shared = self.shared;
        let read_tail = Box::new(move |after: u64| -> Result<Vec<FramedRecord>, String> {
            let mut shared = shared
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(reason) = &shared.broken {
                return Err(reason.clone());
            }
            shared.frames.retain(|frame| frame.seq > after);
            match shared.frames.first() {
                Some(frame) if frame.seq != after + 1 => Err(format!(
                    "the session's media records after {after} start at {}",
                    frame.seq
                )),
                _ => Ok(shared.frames.clone()),
            }
        });
        Ok((bytes, seed, read_tail))
    }
}

/// The byte source of a run whose session's media fold can't be had: every read refuses.
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
    shared: Arc<Mutex<Shared>>,
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
        let object = self
            .shared
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .objects
            .get(&reference.sha256)
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
/// and the session's media fold takes in everything appended before the run hears its append
/// went through, so the run's next request can render what it appended.
pub struct RunAppends<'a, B: SessionBackend> {
    pub session: &'a mut Session<B>,
    /// The media the run's tool calls captured that no record stores yet.
    pub tool_media: &'a broca_session_loop::PendingMedia,
    /// The session's media fold, once a run started that may render media.
    pub fold: Option<&'a mut MediaFold>,
    /// The setup parts a rebuilt run start reads its render config from.
    pub parts: &'a dyn Parts,
}

impl<B: SessionBackend> RunAppends<'_, B> {
    /// After a seal moved the ledger out of the open log: the fold reads the records it hasn't
    /// taken in yet, which include those the seal wrote to start the new log.
    async fn caught_up<T>(&mut self, result: Result<T, SessionError>) -> Result<T, SessionError> {
        if result.is_ok() {
            if let Some(fold) = self.fold.as_deref_mut() {
                fold.catch_up(&*self.session, self.parts).await;
            }
        }
        result
    }

    /// After an append: the fold takes the records as appended (see
    /// [`MediaFold::take_appended`]).
    fn took(
        &mut self,
        result: Result<(u64, Vec<Record>), SessionError>,
    ) -> Result<(u64, Vec<Record>), SessionError> {
        if let (Ok((first, records)), Some(fold)) = (&result, self.fold.as_deref_mut()) {
            fold.take_appended(&*self.session, *first, records, self.parts);
        }
        result
    }
}

impl<B: SessionBackend> broca_session_loop::Writer for RunAppends<'_, B> {
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

    async fn enrollment(
        &self,
    ) -> Result<Option<broca_session_store::session::EnrollmentOverlay>, SessionError> {
        broca_session_loop::Writer::enrollment(&*self.session).await
    }

    // Stores an object, not records: the media fold has nothing to catch up on.
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
        self.caught_up(sealed).await
    }

    async fn seal_ledger(
        &mut self,
        carry: &broca_session_store::session::Carry<'_>,
        now_ms: u64,
    ) -> Result<(), SessionError> {
        let sealed =
            broca_session_loop::Writer::seal_ledger(&mut *self.session, carry, now_ms).await;
        self.caught_up(sealed).await
    }

    async fn entry_by_mid(
        &self,
        mid: &str,
    ) -> Result<Option<broca_session_store::session::StoredEntry>, SessionError> {
        broca_session_loop::Writer::entry_by_mid(&*self.session, mid).await
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
/// through a media fold of every record of `session` and its own media objects, read over
/// `reader`, under `frozen` with `max_output_tokens` as the step's cap. For checking a session in
/// the new storage against the request today's storage renders for it.
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
    let mut fold = MediaFold::new(bind.clone());
    fold.catch_up(session, parts).await;
    let bytes = fold.bytes(session.media_objects(reader));
    let (bytes, seed, read_tail) = fold.for_run(Some(bytes)).renderer_inputs()?;
    let media = broca_provider::media_request::LiveMedia::from_checkpoint(
        bytes,
        seed,
        None,
        frozen.media.clone(),
        read_tail,
    )?;
    let config = serde_json::to_value(&*frozen.for_step(&opts))
        .map_err(|error| format!("encode the frozen render config: {error}"))?;
    // As the transport hands a request to its media renderer: the frozen config with the step's
    // cap applied, the run's tag choice, and the render's media aging committed.
    media
        .render(&config, &opts, render_tags, true)
        .map_err(|error| {
            format!(
                "the media renderer refused: {}",
                error.provider_code.as_deref().unwrap_or(&error.message)
            )
        })
}

#[cfg(test)]
#[path = "store_media/tests.rs"]
mod tests;
