//! Checks a session staged by [`crate::stage`] against today's answers for the same WAL, before a
//! move publishes it. Nothing here goes through the module's ordinary routing, which would read
//! today's storage: the staged session is read from its own folder through a fresh
//! `LocalBackend`, with the setup parts read back from the new storage's setup store, and served
//! by the new storage's own read code. Today's side is served from the frozen WAL by the same
//! builders the converter's checks use.
//!
//! Every check compares exactly. The only members set aside are the ones a moved session changes
//! by design: the log's incarnation and history version, and the subscribe cursor a page or head
//! carries, since a moved session is a new log whose old cursors are refused.
//!
//! The first check that differs ends the verification and is named in the [`Mismatch`].

use std::collections::BTreeMap;
use std::path::Path;

use broca_core::compaction_v2::protocol::read::{Detail, Head, ReadRequest, Select};
use broca_module::serve::{SessionReadRequest, SessionReadResponse};
use broca_module::store_head::{HeadFacts, InputSource};
use broca_module::store_read::todays::Todays;
use broca_module::store_status::{todays, Reader};
use broca_protocol::{BindIdentity, BindPrincipal, RunId, SendScope};
use broca_session_loop::parts::MemoryParts;
use broca_session_store::backend::{LocalBackend, MemoryBackend, SessionAddr, SessionBackend};
use broca_session_store::codec::Message as _;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::session::Session;
use broca_session_store::setup::SetupStore;
use broca_wal::{FramedRecord, WalLineageId, WalRecord};
use serde::Serialize;
use serde_json::{json, Value};

mod store;
pub use store::store_snapshot;

/// What to check.
pub struct Verify<'a> {
    /// The frozen WAL prefix the session was staged from.
    pub records: &'a [FramedRecord],
    pub lineage: Option<WalLineageId>,
    pub session: &'a BindIdentity,
    pub deployment: &'a DeploymentKey,
    /// The new storage's root, whose setup store holds the staged session's setup parts.
    pub store_root: &'a Path,
    /// The staged session's folder.
    pub staging: &'a Path,
    /// What the staging writer wrote, whose records name the setup parts to read back.
    pub converted: &'a crate::Converted,
    pub mode: Mode,
    /// Today's media store for the session, which every medium the staged session holds must
    /// read back equal to; `None` where there is none.
    pub media: Option<&'a dyn broca_core::media_store::MediaBytes>,
    /// Where that store lives, which today's offline render reads a request's media from; `None`
    /// where there is none, which refuses a request holding media.
    pub media_root: Option<MediaRoot<'a>>,
    /// The last update today's `session.head` serves for the session
    /// (`broca_module::head_time`), which the staged session's `Moved` record carries.
    pub todays_updated_at: u64,
}

/// Where today's media store for a session lives: the state root, and the class of store the
/// folder of the session's WAL chooses.
#[derive(Debug, Clone, Copy)]
pub struct MediaRoot<'a> {
    pub state_root: &'a Path,
    pub class: broca_core::media_store::MediaStoreClass,
}

/// How much to check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// What a move checks while it holds the session closed to new work (between freezing its
    /// WAL and publishing the copy). Its cost grows with the session's length, never
    /// with its square: of the recorded model requests, only the last is rebuilt
    /// ([`LAST_STEP`]), beside the session's next one ([`NEXT_REQUEST`]).
    Move,
    /// Everything [`Mode::Move`] checks, and every recorded model request rebuilt
    /// ([`ALL_STEPS`]). Run on a clone before a move, never while a move holds a session closed.
    Offline,
}

/// The checks, by the name a mismatch reports.
pub const RUN_STATUS: &str = "run_status";
pub const RUN_RESULT: &str = "run_result";
pub const SEND_STATUS: &str = "send_status";
pub const READ: &str = "read";
pub const HISTORY_RAW: &str = "history_raw";
pub const HISTORY_DISPLAY: &str = "history_display";
pub const ENTRIES: &str = "entries";
pub const HEAD: &str = "head";
pub const BILLING: &str = "billing";
pub const FIRST_OPEN: &str = "first_open";
pub const MEDIA: &str = "media";
pub const LAST_STEP: &str = "last_step";
pub const NEXT_REQUEST: &str = "next_request";
pub const ALL_STEPS: &str = "all_steps";
/// Not a comparison: reading the staged session back failed.
pub const STAGED: &str = "staged";

/// A verified session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Verified {
    pub runs: u64,
    pub messages: u64,
    pub media: u64,
    /// The checks that ran and agreed, in order.
    pub checks: Vec<&'static str>,
}

/// The first difference found, under the check that found it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mismatch {
    pub check: &'static str,
    pub detail: String,
}

impl std::fmt::Display for Mismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.check, self.detail)
    }
}

fn mismatch(check: &'static str, detail: impl Into<String>) -> Mismatch {
    Mismatch {
        check,
        detail: detail.into(),
    }
}

/// Fails `check` where `store` first differs from `today`, naming `what` was compared.
fn same(check: &'static str, what: &str, store: &Value, today: &Value) -> Result<(), Mismatch> {
    if store == today {
        return Ok(());
    }
    let path = crate::inverse::first_difference(store, today, "").unwrap_or_else(|| "?".into());
    Err(mismatch(check, format!("{what}: differs at {path}")))
}

/// An answer or its refusal, as JSON, so two compare whole.
fn reply<T: Serialize, E: std::fmt::Debug>(reply: Result<T, E>) -> Value {
    match reply {
        Ok(value) => serde_json::to_value(value).expect("replies serialize"),
        Err(error) => json!({ "refused": format!("{error:?}") }),
    }
}

/// Checks the staged session. `Ok` lists every check that ran.
pub async fn verify(verify: Verify<'_>) -> Result<Verified, Mismatch> {
    let addr = SessionAddr(broca_core::session_addr(verify.session));
    let triple = crate::stage::triple(verify.session);
    let view = Session::view(
        LocalBackend::new(verify.staging),
        addr.clone(),
        &triple,
        verify.deployment,
    )
    .await
    .map_err(|error| {
        mismatch(
            STAGED,
            format!("the staged session doesn't open: {error:?}"),
        )
    })?;
    let staged: &Session<LocalBackend> = &view;
    // The session must be stored in the mode the deployment creates sessions in, which is the
    // mode its setup parts are read back in.
    let cipher = staged.head().cipher();
    if cipher != verify.deployment.new_sessions() {
        return Err(mismatch(
            STAGED,
            format!(
                "the staged session is {}, and new sessions are {}",
                cipher.label(),
                verify.deployment.new_sessions().label()
            ),
        ));
    }
    let parts = setup_parts(&verify, cipher).await?;
    let lineage = verify.lineage.map(|id| id.to_string());
    let mut checks = Vec::new();
    runs(&verify, staged, &parts, &mut checks).await?;
    reads(&verify, lineage.as_deref(), staged, &parts).await?;
    checks.push(READ);
    history(verify.records, lineage.as_deref(), staged, &parts).await?;
    checks.push(HISTORY_RAW);
    history_walks(&verify, lineage.as_deref(), staged, &parts).await?;
    checks.push(HISTORY_DISPLAY);
    entries(&verify, lineage.as_deref(), staged, &parts).await?;
    checks.push(ENTRIES);
    head(verify.records, verify.todays_updated_at, staged, &parts).await?;
    checks.push(HEAD);
    if !staged.operations.billing_owed.is_empty() {
        return Err(mismatch(
            BILLING,
            format!(
                "{} runs owe billing today's storage already did",
                staged.operations.billing_owed.len()
            ),
        ));
    }
    checks.push(BILLING);
    let media = staged_media(&verify, staged).await?;
    checks.push(MEDIA);
    let steps = recorded_steps(verify.records);
    if let Some(last) = steps.last() {
        step(&verify, lineage.as_deref(), last, LAST_STEP).await?;
        checks.push(LAST_STEP);
    }
    next_request(&verify, lineage.as_deref(), staged, &addr, &triple).await?;
    checks.push(FIRST_OPEN);
    checks.push(NEXT_REQUEST);
    if verify.mode == Mode::Offline {
        for recorded in &steps {
            step(&verify, lineage.as_deref(), recorded, ALL_STEPS).await?;
        }
        checks.push(ALL_STEPS);
    }
    Ok(Verified {
        runs: verify.converted.runs,
        messages: broca_module::store_read::transcript_len(staged),
        media,
        checks,
    })
}

/// Every medium the WAL names must be one the staged session stores, whose object reads back
/// byte for byte as today's media store holds it. Returns how many there are.
async fn staged_media(
    verify: &Verify<'_>,
    staged: &Session<LocalBackend>,
) -> Result<u64, Mismatch> {
    let digests = crate::stage::media_digests(verify.records);
    for digest in &digests {
        let named = staged
            .named_media(digest)
            .await
            .map_err(|error| mismatch(MEDIA, format!("find the medium {digest}: {error:?}")))?
            .filter(|named| named.stored())
            .ok_or_else(|| mismatch(MEDIA, format!("no object holds the medium {digest}")))?;
        let stored = staged
            .named_media_bytes(&named)
            .await
            .map_err(|error| mismatch(MEDIA, format!("read the medium {digest}: {error:?}")))?;
        let today = verify
            .media
            .ok_or_else(|| mismatch(MEDIA, "today's media store isn't open to compare against"))?
            .read_verified(&named.reference)
            .map_err(|error| {
                mismatch(
                    MEDIA,
                    format!("today's media store can't read the medium {digest}: {error:?}"),
                )
            })?;
        if stored != today {
            return Err(mismatch(
                MEDIA,
                format!("the medium {digest} differs from today's bytes"),
            ));
        }
    }
    Ok(digests.len() as u64)
}

/// The setup parts the staged session's records name, read back from the new storage's setup
/// store in the name space of `cipher`, the staged session's mode, where the staging writer
/// stored them, as a module serving the session reads them.
async fn setup_parts(
    verify: &Verify<'_>,
    cipher: broca_session_store::keys::Cipher,
) -> Result<MemoryParts, Mismatch> {
    let backend = LocalBackend::new(verify.store_root);
    let key = verify.deployment.setup_key_for(cipher).ok_or_else(|| {
        mismatch(
            STAGED,
            "an encrypted session's setup parts can't be read before the deployment key has loaded",
        )
    })?;
    let store = SetupStore::new(&backend, &key);
    let mut parts = BTreeMap::new();
    for part in verify
        .converted
        .records
        .iter()
        .flat_map(crate::measure::named_parts)
    {
        if parts.contains_key(&part.digest) {
            continue;
        }
        let bytes = store.get(part).await.map_err(|error| {
            mismatch(
                STAGED,
                format!("a setup part doesn't read back from the setup store: {error:?}"),
            )
        })?;
        if verify.converted.parts.get(&part.digest).map(Vec::as_slice) != Some(&*bytes) {
            return Err(mismatch(
                STAGED,
                "a setup part reads back other than the conversion wrote it",
            ));
        }
        parts.insert(part.digest, bytes.to_vec());
    }
    Ok(MemoryParts { parts })
}

/// The run ids the WAL starts, the run ids of its queued sends that started none, and its send
/// ids with the run id each one's send owns.
fn ids(records: &[FramedRecord]) -> (Vec<String>, Vec<String>, Vec<(String, String)>) {
    let mut runs: Vec<String> = Vec::new();
    let mut queued = Vec::new();
    let mut sends = Vec::new();
    for frame in records {
        match &frame.record {
            WalRecord::RunStarted {
                run_id, send_id, ..
            } => {
                if !runs.contains(&run_id.0) {
                    runs.push(run_id.0.clone());
                }
                if let Some(send_id) = send_id {
                    sends.push((run_id.0.clone(), send_id.clone()));
                }
            }
            WalRecord::Queued {
                submission_id,
                send_id,
                ..
            } => {
                let run = submission_id
                    .0
                    .strip_prefix("sub-")
                    .unwrap_or(&submission_id.0)
                    .to_owned();
                if let Some(send_id) = send_id {
                    sends.push((run.clone(), send_id.clone()));
                }
                queued.push(run);
            }
            WalRecord::Steered {
                send_id: Some(send_id),
                ..
            } => sends.push((format!("steer-{send_id}"), send_id.clone())),
            _ => {}
        }
    }
    queued.retain(|run| !runs.contains(run));
    sends.dedup();
    (runs, queued, sends)
}

/// `run.status` and `run.result` of every run, the status of every queued send's run id and of
/// an id no run carries, and `send.status` of every send id.
async fn runs<B: SessionBackend>(
    verify: &Verify<'_>,
    view: &Session<B>,
    parts: &MemoryParts,
    checks: &mut Vec<&'static str>,
) -> Result<(), Mismatch> {
    let records = verify.records;
    let name = verify.session.session.as_str();
    let reader = Reader {
        parts,
        live: false,
        settled: None,
        session_name: name,
        live_step: None,
    };
    let (runs, queued, sends) = ids(records);
    for run in &runs {
        let id = RunId(run.clone());
        same(
            RUN_STATUS,
            run,
            &reply(reader.run_status_reply(view, &id).await),
            &reply(todays::run_status_reply(records, &id)),
        )?;
        same(
            RUN_RESULT,
            run,
            &reply(reader.run_result(view, &id).await),
            &reply(todays::run_result(records, &id, name, verify.lineage)),
        )?;
    }
    let unknown = RunId("store-move-no-such-run".into());
    for run in queued.iter().map(|run| RunId(run.clone())).chain([unknown]) {
        same(
            RUN_STATUS,
            &run.0,
            &reply(reader.run_status_reply(view, &run).await),
            &reply(todays::run_status_reply(records, &run)),
        )?;
    }
    checks.extend([RUN_STATUS, RUN_RESULT]);
    for (own_run, send_id) in &sends {
        same(
            SEND_STATUS,
            send_id,
            &reply(
                reader
                    .send_status(view, RunId(own_run.clone()), send_id, None)
                    .await,
            ),
            &reply(todays::send_status(
                records,
                verify.lineage,
                RunId(own_run.clone()),
                send_id,
            )),
        )?;
    }
    checks.push(SEND_STATUS);
    Ok(())
}

/// The read modes: whether each asks for display facts, tools and Rich UI.
const READ_MODES: [(&str, bool, bool, bool); 5] = [
    ("default", false, false, false),
    ("display_facts", true, false, false),
    ("tools", false, true, false),
    ("rich_ui", false, false, true),
    ("all", true, true, true),
];

/// How many messages a compared page asks for. Small, so a session's pages are many and their
/// boundaries are compared too.
const PAGE: u64 = 7;

/// A page's members beside its messages, as compared: all of them but the ones a moved session
/// changes by design (the log's incarnation and history version, and the subscribe cursor).
fn page_members(page: &SessionReadResponse) -> Value {
    let mut value = serde_json::to_value(page).expect("replies serialize");
    let members = value.as_object_mut().expect("a reply is an object");
    for changed in ["messages", "history_version", "incarnation", "head"] {
        members.remove(changed);
    }
    value
}

/// Every page of `session.read` in every mode, walked from the start as a client walks it.
async fn reads<B: SessionBackend>(
    verify: &Verify<'_>,
    lineage: Option<&str>,
    view: &Session<B>,
    parts: &MemoryParts,
) -> Result<(), Mismatch> {
    let todays = Todays::new(verify.records, lineage);
    for (mode, display_facts, tools, rich_ui) in READ_MODES {
        let mut from = 1;
        loop {
            let mut request =
                SessionReadRequest::new(verify.session.clone(), Some(from), Some(PAGE));
            request.include_display_facts = display_facts;
            request.include_tools = tools;
            request.include_rich_ui = rich_ui;
            let today = todays.read(&request);
            let store = broca_module::store_read::serve_view(view, parts, &request).await;
            let at = format!("{mode} page from {from}");
            let (today, store) = match (today, store) {
                (Ok(today), Ok(store)) => (today, store),
                (today, store) => {
                    return same(READ, &at, &reply(store), &reply(today));
                }
            };
            same(READ, &at, &page_members(&store), &page_members(&today))?;
            if store.messages.len() != today.messages.len() {
                return Err(mismatch(
                    READ,
                    format!(
                        "{at}: {} messages served, {} today",
                        store.messages.len(),
                        today.messages.len()
                    ),
                ));
            }
            for (served, today) in store.messages.iter().zip(&today.messages) {
                same(
                    READ,
                    &format!("{mode} message {}", today.ordinal),
                    &serde_json::to_value(served).expect("messages serialize"),
                    &serde_json::to_value(today).expect("messages serialize"),
                )?;
            }
            match today.next_from_ordinal {
                Some(next) if next > from => from = next,
                _ => break,
            }
        }
    }
    Ok(())
}

/// The raw history entries of every page `page` serves, walked from the start.
fn walk_history<F>(mut page: F) -> Result<Vec<Option<Value>>, String>
where
    F: FnMut(u64) -> Result<broca_core::SessionReadPage, String>,
{
    let mut entries = Vec::new();
    let mut from = 0;
    loop {
        let read = page(from)?;
        let next = read.next_from_ordinal;
        entries.extend(broca_module::store_entries::raw_history_entries(read));
        match next {
            Some(next) if next > from => from = next,
            _ => return Ok(entries),
        }
    }
}

/// `session.history` in its raw form, every entry. `lineage` is the log's lineage id.
async fn history<B: SessionBackend>(
    records: &[FramedRecord],
    lineage: Option<&str>,
    view: &Session<B>,
    parts: &MemoryParts,
) -> Result<(), Mismatch> {
    // Today's reads serve the output of a tool result a send rode on as the tool returned it,
    // from the display facts folded from the log, not with the ride's text joined to it as its
    // stored record keeps it (`LineageRegistry::read`, and `store_read::todays` beside it).
    let native = match lineage {
        Some(lineage) => broca_core::display_facts::native_tool_outputs(records, lineage)
            .map_err(|error| mismatch(HISTORY_RAW, format!("today's tool outputs: {error}")))?,
        None => Default::default(),
    };
    let today = walk_history(|from| {
        let mut page = broca_core::read_session_page(
            &broca_core::StorePageSnapshot::empty(),
            records,
            Some(from),
            Some(PAGE),
        )
        .map_err(|error| error.to_string())?;
        for entry in &mut page.messages {
            // Page ordinals count from 0, as the folded facts key them.
            let Some(outputs) = native.get(&entry.ordinal) else {
                continue;
            };
            for (part, block) in entry.message.content.iter_mut().enumerate() {
                if let broca_protocol::ContentBlock::ToolResult(result) = block {
                    if let Some(output) = outputs.get(&(part as u32)) {
                        result.output = output.clone();
                    }
                }
            }
        }
        Ok(page)
    })
    .map_err(|error| mismatch(HISTORY_RAW, format!("today's history: {error}")))?;
    let mut store = Vec::new();
    let mut from = 0;
    loop {
        let read = broca_module::store_read::read_page(view, Some(from), Some(PAGE))
            .await
            .map_err(|error| mismatch(HISTORY_RAW, format!("the staged history: {error:?}")))?;
        let mut page = read.page;
        broca_module::store_entries::attach_part_tags(
            view,
            parts,
            &mut page.messages,
            &read.numbering,
        )
        .await
        .map_err(|error| mismatch(HISTORY_RAW, format!("the staged history: {error}")))?;
        let next = page.next_from_ordinal;
        store.extend(broca_module::store_entries::raw_history_entries(page));
        match next {
            Some(next) if next > from => from = next,
            _ => break,
        }
    }
    same(
        HISTORY_RAW,
        "every entry",
        &serde_json::to_value(&store).expect("entries serialize"),
        &serde_json::to_value(&today).expect("entries serialize"),
    )
}

/// An answer or its refusal, as the compaction route's checks compare them.
fn outcome<T: Serialize, E: Serialize>(result: Result<T, E>) -> Value {
    match result {
        Ok(value) => json!({ "ok": value }),
        Err(refusal) => json!({ "refused": refusal }),
    }
}

/// How many mids or tags one compared `session.entries` lookup names.
const LOOKUP: usize = 16;

/// The compaction route: the bind decision for the session's own provider and another, and for
/// an admitted provider `session.applied_view`, every `session.entries` page in both details,
/// and a lookup of every mid and every tag.
async fn entries<B: SessionBackend>(
    verify: &Verify<'_>,
    lineage: Option<&str>,
    view: &Session<B>,
    parts: &MemoryParts,
) -> Result<(), Mismatch> {
    let records = verify.records;
    let bind = verify.session;
    let wal: Vec<WalRecord> = records.iter().map(|frame| frame.record.clone()).collect();
    let mode3 = broca_core::is_mode3_log(&wal);
    let ownership = broca_core::AdmissionCheckpointV1::from_frames(records)
        .map_err(|error| mismatch(ENTRIES, format!("today's ownership: {error}")))?
        .ownership
        .unwrap_or_default();
    let manifest = ownership
        .latest_manifest
        .as_deref()
        .or_else(|| ownership.first_episode.as_ref()?.manifest.as_deref());
    let provider = manifest
        .map(broca_core::compaction::item)
        .transpose()
        .map_err(|error| mismatch(ENTRIES, format!("today's compaction item: {error}")))?
        .flatten()
        .map(|item| item.0);
    let serve = |caller: String, method: &'static str, params: Value| async move {
        let params = serde_json::value::to_raw_value(&params).expect("params encode");
        outcome(
            broca_module::store_entries::serve_compaction(
                parts,
                Some(view),
                bind,
                Some(&caller),
                method,
                &params,
                // Compared with today's projection, which keeps every entry's attribution.
                &broca_module::store_entries::EntryAttribution::Known(true),
            )
            .await
            .map(|reply| serde_json::from_str::<Value>(reply.get()).expect("a reply is JSON")),
        )
    };
    let own = provider.clone().unwrap_or_else(|| "ck-mc".into());
    let mut admitted = false;
    for caller in [own.clone(), "another-provider".to_owned()] {
        let today = outcome(
            broca_core::compaction_v2::authority::check_bind(
                mode3,
                provider.as_deref(),
                Some(&caller),
            )
            .map(|()| json!({})),
        );
        if caller == own {
            admitted = today.get("ok").is_some();
        }
        let store = serve(caller.clone(), "bind", json!({})).await;
        same(ENTRIES, &format!("bind by {caller}"), &store, &today)?;
    }
    if !admitted {
        return Ok(());
    }
    let lineage = lineage.ok_or_else(|| {
        mismatch(
            ENTRIES,
            "a session on the compaction contract has no lineage",
        )
    })?;
    let transcript = broca_core::compaction_v2::transcript::project(records, lineage)
        .map_err(|error| mismatch(ENTRIES, format!("today's entries: {error}")))?;
    let entries = transcript.entries;
    let state = broca_core::compaction_v2::session::load(bind, records)
        .map_err(|error| mismatch(ENTRIES, format!("today's compaction state: {error}")))?;
    let applied = match state.applied_received() {
        Some(view) => json!({ "ok": { "view": view } }),
        None => json!({ "ok": {} }),
    };
    same(
        ENTRIES,
        "session.applied_view",
        &serve(own.clone(), "session.applied_view", json!({})).await,
        &applied,
    )?;
    let page = |request: &ReadRequest| {
        let mut head = Head::new();
        head.newest = broca_core::compaction_v2::views::newest(&entries);
        head.applied_view = state.applied_identity();
        json!({ "ok": broca_core::compaction_v2::views::page(&entries, request, head) })
    };
    let mut requests: Vec<ReadRequest> = Vec::new();
    for detail in [Detail::Full, Detail::Compact] {
        let mut after = None;
        loop {
            let request = ReadRequest::new(
                Select::After {
                    after_ordinal: after,
                    limit: Some(PAGE),
                },
                detail,
            );
            let next = page(&request)["ok"]["next_after"].as_u64();
            requests.push(request);
            match next {
                Some(next) if Some(next) != after => after = Some(next),
                _ => break,
            }
        }
    }
    let tags: Vec<u64> = entries
        .iter()
        .flat_map(|entry| entry.parts.iter().filter_map(|part| part.tag))
        .chain([u64::MAX >> 12])
        .collect();
    for tags in tags.chunks(LOOKUP) {
        requests.push(ReadRequest::new(
            Select::Tags {
                tags: tags.to_vec(),
            },
            Detail::Compact,
        ));
    }
    let mids: Vec<String> = entries
        .iter()
        .map(|entry| entry.mid.clone())
        .chain(["no-such-message".to_owned()])
        .collect();
    for mids in mids.chunks(LOOKUP) {
        requests.push(ReadRequest::new(
            Select::Mids {
                mids: mids.to_vec(),
            },
            Detail::Full,
        ));
    }
    for request in &requests {
        let params = serde_json::to_value(request).expect("a request encodes");
        same(
            ENTRIES,
            &format!("session.entries {params}"),
            &serve(own.clone(), "session.entries", params.clone()).await,
            &page(request),
        )?;
    }
    Ok(())
}

/// A head as compared: all of it but the members a moved session changes by design, with
/// `mode3`, which isn't on the wire, under its name.
fn head_members(reply: &impl Serialize, mode3: bool) -> Value {
    let mut value = serde_json::to_value(reply).expect("a head serializes");
    let members = value.as_object_mut().expect("a head is an object");
    for changed in ["incarnation", "history_version", "head"] {
        members.remove(changed);
    }
    members.insert("mode3".into(), json!(mode3));
    value
}

/// Each caller whose sender grant is compared: an anonymous one, every principal the session's
/// runs recorded, every module a plan of the session names as a sender, and a module nothing
/// names.
fn principals(records: &[FramedRecord]) -> Vec<Option<BindPrincipal>> {
    let mut out = vec![None];
    let mut add = |principal: BindPrincipal| {
        if !out.contains(&Some(principal.clone())) {
            out.push(Some(principal));
        }
    };
    for frame in records {
        if let WalRecord::RunStarted {
            principal,
            manifest,
            ..
        } = &frame.record
        {
            if let Some(principal) = principal {
                add(principal.clone());
            }
            for sender in manifest
                .as_deref()
                .map(broca_core::head::head_senders)
                .unwrap_or_default()
            {
                add(BindPrincipal::Reserved {
                    module_id: sender.module,
                });
            }
        }
    }
    add(BindPrincipal::Reserved {
        module_id: "a-module-nothing-names".into(),
    });
    out
}

/// The reason the latest end of `run_id` recorded for interrupting it.
fn recorded_interrupt_reason(records: &[FramedRecord], run_id: &RunId) -> Option<String> {
    let mut current = None;
    let mut reason = None;
    for frame in records {
        match &frame.record {
            WalRecord::RunStarted { run_id, .. } => current = Some(run_id),
            WalRecord::RunFinished {
                reason: broca_protocol::RunFinishReason::Interrupted,
                interrupt_reason,
                ..
            } if current == Some(run_id) => reason = interrupt_reason.clone(),
            _ => {}
        }
    }
    reason
}

/// The `media` member of the render config the latest run froze, or the one a resume refroze
/// after it: the session's frozen media profile.
fn frozen_media(records: &[FramedRecord]) -> Option<Value> {
    let mut media = None;
    for frame in records {
        match &frame.record {
            WalRecord::RunStarted { config, .. } => {
                media = config.frozen_render_config.get("media").cloned();
            }
            WalRecord::RunConfigRefrozen {
                frozen_render_config,
                ..
            } => media = frozen_render_config.get("media").cloned(),
            _ => {}
        }
    }
    media
}

/// `session.head`: the reply, the input profile, and the sender grant each principal is told.
/// Today's head is built from the WAL as the module's head builds it, its last update
/// `updated_at`, the one today's head serves (`broca_module::head_time`).
async fn head<B: SessionBackend>(
    records: &[FramedRecord],
    updated_at: u64,
    view: &Session<B>,
    parts: &MemoryParts,
) -> Result<(), Mismatch> {
    let mut facts = broca_core::head::HeadProjection::default();
    for frame in records {
        facts.observe(&frame.record);
    }
    let page = broca_core::read_session_page(
        &broca_core::StorePageSnapshot::empty(),
        records,
        None,
        Some(1),
    )
    .map_err(|error| mismatch(HEAD, format!("today's page: {error}")))?;
    let state = page.lineage_state;
    let interrupt_reason = match (&state.last_run_id, state.state) {
        (Some(run_id), broca_core::SessionReadStateKind::Interrupted) => {
            recorded_interrupt_reason(records, run_id)
        }
        _ => None,
    };
    let input = broca_module::store_head::input_source::<String>(
        state.last_run_id.as_ref().map(|_| state.state.as_str()),
        || Ok(frozen_media(records)),
    )
    .map_err(|error| mismatch(HEAD, format!("today's input profile: {error}")))?;
    let mode3 = facts
        .mode3
        .unwrap_or_else(|| broca_core::is_mode3_log(records.iter().map(|frame| &frame.record)));
    let today = broca_module::store_head::head_reply(HeadFacts {
        lineage_state: state,
        prompt_len: page.prompt_len,
        interrupt_reason,
        selection: facts.selection.clone(),
        run_model: facts.model.clone(),
        rich_ui: facts.last_run_rich_ui.clone(),
        usage: facts.usage.clone(),
        title: records
            .iter()
            .rev()
            .find_map(|frame| frame.record.title())
            .map(str::to_owned),
        mode3,
        updated_at: Some(updated_at),
    });
    let built = broca_module::store_head::build(view, parts, true)
        .await
        .map_err(|error| mismatch(HEAD, format!("the staged head: {error:?}")))?;
    same(
        HEAD,
        "the reply",
        &head_members(&built.reply, built.reply.mode3),
        &head_members(&today, today.mode3),
    )?;
    if built.input != input {
        return Err(mismatch(
            HEAD,
            format!(
                "the input profile: {} served, {} today",
                input_name(&built.input),
                input_name(&input)
            ),
        ));
    }
    let scope = SendScope::default();
    for principal in principals(records) {
        same(
            HEAD,
            &format!("the sender grant of {principal:?}"),
            &serde_json::to_value(built.sender(principal.as_ref(), &scope))
                .expect("a sender serializes"),
            &serde_json::to_value(facts.sender(
                principal.as_ref(),
                &scope,
                broca_module::store_head::DELIVERY_MODES,
            ))
            .expect("a sender serializes"),
        )?;
    }
    Ok(())
}

fn input_name(input: &InputSource) -> &'static str {
    match input {
        InputSource::Unknown => "unknown",
        InputSource::Catalog => "catalog",
        InputSource::Frozen(None) => "frozen without media",
        InputSource::Frozen(Some(_)) => "frozen media",
    }
}

/// A model step whose request the WAL recorded: where it started, its run and config, and the
/// digest of its first attempt's request body.
struct RecordedStep {
    index: usize,
    run: String,
    step: u64,
    config: broca_wal::RunConfig,
    digest: String,
}

/// Every step of the WAL with a recorded request, in order.
fn recorded_steps(records: &[FramedRecord]) -> Vec<RecordedStep> {
    let mut steps = Vec::new();
    let mut active: Option<(String, broca_wal::RunConfig)> = None;
    for (index, frame) in records.iter().enumerate() {
        match &frame.record {
            WalRecord::RunStarted { run_id, config, .. } => {
                active = Some((run_id.0.clone(), (**config).clone()))
            }
            WalRecord::StepStarted { step_id } => {
                let Some((run, config)) = &active else {
                    continue;
                };
                let digest = records[index + 1..]
                    .iter()
                    .take_while(|frame| {
                        !matches!(
                            frame.record,
                            WalRecord::StepStarted { .. } | WalRecord::RunStarted { .. }
                        )
                    })
                    .find_map(|frame| match &frame.record {
                        WalRecord::ModelAttemptFinished {
                            step_id: attempted,
                            request_body_sha256,
                            ..
                        } if attempted == step_id => Some(request_body_sha256.clone()),
                        _ => None,
                    });
                if let Some(digest) = digest {
                    steps.push(RecordedStep {
                        index,
                        run: run.clone(),
                        step: step_id.0,
                        config: config.clone(),
                        digest,
                    });
                }
            }
            _ => {}
        }
    }
    steps
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest as _;
    broca_session_store::codec::hex(&sha2::Sha256::digest(bytes))
}

/// The request body `opts` renders to under today's frozen render config `frozen`.
fn render(
    frozen: &broca_provider::wire::FrozenRenderConfig,
    opts: &broca_protocol::CallOptions,
) -> Result<Vec<u8>, String> {
    let family = broca_provider::registry::wire_family(frozen.family_id)
        .ok_or("no wire family for the frozen family id")?;
    Ok(family.render_with_provenance(&frozen.for_step(opts), opts, None))
}

/// Today's render of the request step `step` of run `run` sends, from the WAL `prefix` through
/// the step's start, with `cap` as the step's lowered output cap: the request a copy of the
/// session must render byte for byte. Where the move found today's media store for the session
/// (`verify.media_root`), it renders as today's offline render does
/// (`wal_render::render_step_with_media`), preparing the request's media from that store read
/// only, and refusing by name a medium it can't read. Without that store a request whose prefix
/// names media (`holds_media`) is refused.
#[allow(clippy::too_many_arguments)]
fn todays_request(
    verify: &Verify<'_>,
    lineage: Option<&str>,
    prefix: &[WalRecord],
    config: &broca_wal::RunConfig,
    run: &str,
    step: u64,
    cap: Option<u32>,
    holds_media: bool,
) -> Result<Vec<u8>, String> {
    match verify.media_root {
        Some(root) => broca_module::wal_render::render_step_with_media(
            &broca_module::wal_render::OfflineMediaContext {
                state_root: root.state_root,
                identity: verify.session,
                class: root.class,
                lineage,
            },
            prefix,
            config,
            run,
            step,
            cap,
        )
        .map(|(_, _, body)| body)
        .map_err(|error| error.to_string()),
        None if holds_media => {
            Err("no media store of today's is open to render the request's media from".into())
        }
        None => broca_module::wal_render::render_step_with_lineage(
            lineage, prefix, config, run, step, cap, None,
        )
        .map(|(_, _, body)| body),
    }
}

/// The frozen render config of a run whose config is `config`.
fn frozen_of(
    config: &broca_wal::RunConfig,
) -> Result<broca_provider::wire::FrozenRenderConfig, String> {
    serde_json::from_value(config.frozen_render_config.clone())
        .map_err(|error| format!("decode the frozen render config: {error}"))
}

/// The request the new run loop builds for step `step` of `session` and renders under `frozen`,
/// with `cap` as the step's output cap. A session holding media (`holds_media`) renders it as a
/// run of a session in the new storage does, through the media renderer over the session's own
/// media objects, read from the backend root `objects`.
#[allow(clippy::too_many_arguments)]
async fn store_request<B: SessionBackend>(
    session: &Session<B>,
    objects: &Path,
    parts: &MemoryParts,
    bind: &BindIdentity,
    step: u64,
    config: &broca_wal::RunConfig,
    cap: Option<u32>,
    holds_media: bool,
) -> Result<Vec<u8>, String> {
    let frozen = frozen_of(config)?;
    if holds_media {
        return broca_module::store_media::offline_render(
            session,
            LocalBackend::new(objects),
            parts,
            bind,
            step,
            &frozen,
            config.render_tags,
            cap,
        )
        .await;
    }
    let mut opts = broca_session_loop::run::offline_request(session, parts, bind, step)
        .await
        .map_err(|error| format!("the new run loop: {error:?}"))?;
    if let Some(context) = opts.step_context.as_mut() {
        context.max_output_tokens = cap;
    }
    render(&frozen, &opts)
}

/// The recorded step `recorded`, rebuilt by the new run loop from the converted WAL up to its
/// start, must render to the bytes today's renderer builds for it, and those to the bytes the
/// provider was sent.
async fn step(
    verify: &Verify<'_>,
    lineage: Option<&str>,
    recorded: &RecordedStep,
    check: &'static str,
) -> Result<(), Mismatch> {
    let records = verify.records;
    let wal: Vec<WalRecord> = records.iter().map(|frame| frame.record.clone()).collect();
    let contract = broca_core::is_mode3_log(&wal);
    let at = |what: String| {
        mismatch(
            check,
            format!("step {} of {}: {what}", recorded.step, recorded.run),
        )
    };
    // The step's own lowered output cap, if it recorded one, follows its start.
    let mut end = match wal.get(recorded.index + 1) {
        Some(WalRecord::OutputReservationClamped { .. }) => recorded.index + 1,
        _ => recorded.index,
    };
    // A step on the current contract renders from the views recorded up to its request.
    if contract {
        end = wal[end + 1..]
            .iter()
            .position(|record| matches!(record, WalRecord::ModelAttemptFinished { .. }))
            .map_or(wal.len(), |offset| end + 1 + offset)
            - 1;
    }
    let prefix = &wal[..=end];
    let cap =
        broca_core::output_reservation::recorded_cap(prefix, broca_protocol::StepId(recorded.step));
    let holds_media = !crate::stage::media_digests(&records[..=end]).is_empty();
    let converted = crate::convert(&records[..=end], lineage)
        .map_err(|error| at(format!("the conversion: {}", error.0)))?;
    let parts = MemoryParts {
        parts: converted.parts.clone(),
    };
    let addr = SessionAddr(broca_core::session_addr(verify.session));
    let triple = crate::stage::triple(verify.session);
    let rebuilt = if holds_media {
        // The media renderer reads the session's own objects through a backend of its own, so
        // the rebuild is written to a folder, with the bytes of every medium it names.
        let scratch = verify.staging.join("step-rebuild");
        let _ = std::fs::remove_dir_all(&scratch);
        let rebuilt = async {
            let media =
                crate::stage::read_media(&records[..=end], &converted.records, verify.media)
                    .map_err(|error| format!("the media: {error}"))?;
            let mut session = Session::create(
                LocalBackend::new(&scratch),
                addr,
                triple,
                verify.deployment,
                !contract,
            )
            .await
            .map_err(|error| format!("a session for the rebuild: {error:?}"))?;
            crate::stage::append_batch(&mut session, &converted.records, &media)
                .await
                .map_err(|error| format!("a session for the rebuild: {error}"))?;
            let rebuilt = store_request(
                &session,
                &scratch,
                &parts,
                verify.session,
                recorded.step,
                &recorded.config,
                cap,
                true,
            )
            .await;
            let _ = session.close().await;
            rebuilt
        }
        .await;
        let _ = std::fs::remove_dir_all(&scratch);
        rebuilt
    } else {
        let backend = MemoryBackend::default();
        async {
            let mut session = Session::create(&backend, addr, triple, verify.deployment, !contract)
                .await
                .map_err(|error| format!("a session for the rebuild: {error:?}"))?;
            session
                .append(&converted.records, false)
                .await
                .map_err(|error| format!("a session for the rebuild: {error:?}"))?;
            store_request(
                &session,
                verify.staging,
                &parts,
                verify.session,
                recorded.step,
                &recorded.config,
                cap,
                false,
            )
            .await
        }
        .await
    }
    .map_err(|error| at(format!("the new run loop's rebuild: {error}")))?;
    if sha256_hex(&rebuilt) != recorded.digest {
        return Err(at(
            "the request differs from the one the provider was sent".into()
        ));
    }
    let today = todays_request(
        verify,
        lineage,
        prefix,
        &recorded.config,
        &recorded.run,
        recorded.step,
        cap,
        holds_media,
    )
    .map_err(|error| at(format!("today's renderer: {error}")))?;
    if rebuilt != today {
        return Err(at("the rebuilt request differs from today's render".into()));
    }
    Ok(())
}

/// The run id the synthetic next send starts. No real run carries it: the copy that receives it
/// is deleted after the check.
const NEXT_RUN: &str = "store-move-next-request";

/// The WAL with one more send after it: the session's last run start again, under a new run id
/// with a fixed input, and its first step's start, as the next send to the session would begin.
fn with_next_send(records: &[FramedRecord]) -> Result<Vec<FramedRecord>, String> {
    let last = records.last().ok_or("the WAL is empty")?;
    let mut start = records
        .iter()
        .rev()
        .find_map(|frame| match &frame.record {
            WalRecord::RunStarted { .. } => Some(frame.record.clone()),
            _ => None,
        })
        .ok_or("the WAL starts no run")?;
    let at = records
        .iter()
        .rev()
        .find_map(|frame| broca_core::record_time_ms(&frame.record))
        .map(|at| at + 1);
    if let WalRecord::RunStarted {
        run_id,
        ts_ms,
        input,
        origin,
        submission_id,
        send_id,
        request_identity_digest,
        written_at_ms,
        ..
    } = &mut start
    {
        *run_id = RunId(NEXT_RUN.into());
        *ts_ms = at;
        *written_at_ms = at;
        *origin = broca_wal::RunOrigin::Fresh;
        *submission_id = None;
        *send_id = None;
        *request_identity_digest = None;
        *input = std::sync::Arc::new(vec![serde_json::from_value(json!({
            "role": "user",
            "content": [{"type": "text", "text": "The next message after the move."}],
        }))
        .map_err(|error| format!("the next send's input: {error}"))?]);
    }
    let mut extended = records.to_vec();
    for record in [
        start,
        WalRecord::StepStarted {
            step_id: broca_protocol::StepId(1),
        },
    ] {
        let seq = extended.last().map_or(last.seq, |frame| frame.seq) + 1;
        extended.push(FramedRecord {
            seq,
            fence: last.fence,
            record,
        });
    }
    Ok(extended)
}

/// Copies the staged session's own files (its head, log and objects) from the backend root
/// `from` to the backend root `to`, so a check can open and write the copy and leave the staged
/// session as it was.
fn copy_session(from: &Path, to: &Path, address: &str) -> std::io::Result<()> {
    fn copy_tree(from: &Path, to: &Path) -> std::io::Result<()> {
        std::fs::create_dir_all(to)?;
        for entry in std::fs::read_dir(from)? {
            let entry = entry?;
            let target = to.join(entry.file_name());
            if entry.file_type()?.is_dir() {
                copy_tree(&entry.path(), &target)?;
            } else {
                std::fs::copy(entry.path(), target)?;
            }
        }
        Ok(())
    }
    for folder in ["heads", "logs", "objects"] {
        let source = from.join(folder).join(address);
        if source.exists() {
            copy_tree(&source, &to.join(folder).join(address))?;
        }
    }
    Ok(())
}

/// Opens a copy of the staged session as its first writer would, and checks the open changes
/// nothing a writer finds: no run to recover, no billing owed, the same transcript and
/// operations. Then the session's next send, appended to the WAL and to that copy, must render
/// to the request today's renderer builds for it.
async fn next_request(
    verify: &Verify<'_>,
    lineage: Option<&str>,
    view: &Session<LocalBackend>,
    addr: &SessionAddr,
    triple: &broca_session_store::head::SessionTriple,
) -> Result<(), Mismatch> {
    let scratch = verify.staging.join("next-request");
    let _ = std::fs::remove_dir_all(&scratch);
    let result = async {
        copy_session(verify.staging, &scratch, &addr.0)
            .map_err(|error| mismatch(FIRST_OPEN, format!("copy the staged session: {error}")))?;
        let mut session = Session::load(
            LocalBackend::new(&scratch),
            addr.clone(),
            triple,
            verify.deployment,
        )
        .await
        .map_err(|error| mismatch(FIRST_OPEN, format!("open: {error:?}")))?;
        if session.conversation.encode() != view.conversation.encode()
            || session.operations.encode() != view.operations.encode()
        {
            return Err(mismatch(
                FIRST_OPEN,
                "opening the session changed what it holds",
            ));
        }
        if !session.operations.billing_owed.is_empty() {
            return Err(mismatch(FIRST_OPEN, "opening the session owes billing"));
        }
        let extended =
            with_next_send(verify.records).map_err(|error| mismatch(NEXT_REQUEST, error))?;
        let wal: Vec<WalRecord> = extended.iter().map(|frame| frame.record.clone()).collect();
        let config = match &wal[wal.len() - 2] {
            WalRecord::RunStarted { config, .. } => config,
            _ => unreachable!("with_next_send ends with a run start and its step"),
        };
        let holds_media = !crate::stage::media_digests(&extended).is_empty();
        let today = todays_request(
            verify,
            lineage,
            &wal,
            config,
            NEXT_RUN,
            1,
            None,
            holds_media,
        )
        .map_err(|error| mismatch(NEXT_REQUEST, format!("today's renderer: {error}")))?;
        let converted = crate::convert(&extended, lineage)
            .map_err(|error| mismatch(NEXT_REQUEST, format!("the conversion: {}", error.0)))?;
        let staged = &verify.converted.records;
        if !converted.records.starts_with(staged) {
            return Err(mismatch(
                NEXT_REQUEST,
                "converting the next send rewrites the session's earlier records",
            ));
        }
        session
            .append(&converted.records[staged.len()..], true)
            .await
            .map_err(|error| mismatch(NEXT_REQUEST, format!("append the next send: {error:?}")))?;
        let parts = MemoryParts {
            parts: converted.parts.clone(),
        };
        let rebuilt = store_request(
            &session,
            &scratch,
            &parts,
            verify.session,
            1,
            config,
            None,
            holds_media,
        )
        .await
        .map_err(|error| mismatch(NEXT_REQUEST, error))?;
        session
            .close()
            .await
            .map_err(|error| mismatch(NEXT_REQUEST, format!("close the copy: {error:?}")))?;
        if rebuilt != today {
            return Err(mismatch(
                NEXT_REQUEST,
                "the next request differs from the one today's renderer builds",
            ));
        }
        Ok(())
    }
    .await;
    let _ = std::fs::remove_dir_all(&scratch);
    result
}

/// The walks `session.history` is read by: each form, from the newest page by `older` cursors
/// and from the first message by `newer` cursors.
const HISTORY_WALKS: [(&str, bool); 4] = [
    ("raw", true),
    ("raw", false),
    ("display", true),
    ("display", false),
];

/// Every page of one walk of `session.history` over `handler`, as `broca_module::history::read`
/// serves them, ending at a refusal if one comes.
async fn history_pages(
    handler: &dyn broca_module::ServeHandler,
    session: &BindIdentity,
    form: &str,
    newest: bool,
) -> Vec<Value> {
    let mut params = json!({ "form": form, "limit": PAGE });
    if !newest {
        params["from_ordinal"] = json!(1);
    }
    let follow = if newest { "older" } else { "newer" };
    let mut pages = Vec::new();
    loop {
        let page = match serde_json::from_value(params.clone()) {
            Ok(request) => broca_module::history::read(handler, session, &request)
                .await
                .map_err(|error| format!("{error:?}")),
            Err(error) => Err(error.to_string()),
        };
        let page = match page {
            Ok(page) => page,
            Err(error) => {
                pages.push(json!({ "refused": error }));
                return pages;
            }
        };
        let next = page.get(follow).and_then(Value::as_str).map(str::to_owned);
        pages.push(page);
        match next {
            Some(cursor) => params = json!({ "form": form, "limit": PAGE, "cursor": cursor }),
            None => return pages,
        }
    }
}

/// `session.history` in both forms, every page of every walk, compared whole. Today's side is
/// given the staged session's incarnation and history version, which each storage keeps for
/// itself, so cursors compare as the positions they hold.
async fn history_walks(
    verify: &Verify<'_>,
    lineage: Option<&str>,
    view: &Session<LocalBackend>,
    parts: &MemoryParts,
) -> Result<(), Mismatch> {
    let head = view.head();
    let today = broca_module::store_read::history_sides::TodaysHistory::new(
        verify.records,
        lineage,
        broca_session_store::codec::hex(&head.incarnation.0),
        head.history_version,
    )
    .map_err(|error| mismatch(HISTORY_DISPLAY, format!("today's history: {error:?}")))?;
    let staged = crate::stored_history::StoredHistory {
        session: view,
        parts,
    };
    for (form, newest) in HISTORY_WALKS {
        let walk = format!(
            "{form} from the {}",
            if newest { "newest" } else { "first" }
        );
        same(
            HISTORY_DISPLAY,
            &walk,
            &Value::Array(history_pages(&staged, verify.session, form, newest).await),
            &Value::Array(history_pages(&today, verify.session, form, newest).await),
        )?;
    }
    Ok(())
}
