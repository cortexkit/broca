//! Serving llm-runner 0.8.42 §9.20 `fork_answer` on heads kept in the new storage: private
//! questions asked of a copy of a head's last served request, without touching the head.
//!
//! `session.fork_prepare` fixes a fork's base at the head's newest recorded attempt (its run,
//! step and request digest) and creates the fork's own short-lived session; `session.fork_ask`
//! rebuilds that request from the head's records, checks it against the digest, appends the
//! fork's earlier questions and answers and the new question, and hands one attempt to the
//! executor ([`crate::executor::RunExecutor::fork_call`]); `session.fork_end` ends the fork.
//!
//! The head is only read: through the same read-only view a read without an actor uses
//! (`StoreRuntime::current_view`), never its actor or its lease, so a fork runs while the head
//! runs, writes nothing of the head's and emits nothing on its stream. The base is rebuilt by the
//! code idle cache warming rebuilds a recorded request with (`store_actor::recorded_warm`).
//!
//! Each fork lives in a session of its own in the new storage's ephemeral root, named after the
//! head (its session name, `:fork:` and the fork id, under the head's project and harness) and
//! holding only `Fork` records, which [`ForkState`] folds. Its records say who prepared it, each
//! ask's attempts and stored answers, and its end and revocation; retention keeps the session at
//! least [`FORK_RETAIN_MS`] past the fork's last possible use. The fork's session is written only
//! under its own lease, held while an operation uses it, so retention never deletes it under a
//! running ask.
//!
//! Who may prepare is the plan member `fork_callers` of the head's plan at acceptance (the
//! plan the head's senders follow), matched against the route's attested module id. A fork
//! answers only its preparer; any other caller, and its preparer once revoked, reads it as
//! unknown. A preparer the head's plan no longer lists is revoked on the fork's next operation,
//! durably, so listing it again revives nothing.

use std::collections::HashMap;
use std::sync::Arc;

use broca_core::LeaseStore as _;
use broca_protocol::{BindIdentity, BindPrincipal};
use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_session_store::record::{Fork, ForkEvent, Record};
use broca_session_store::session::{Session, SessionError};
use broca_wal::StorageClass;
use cortexkit_role_llm_runner::fork_answer::{
    ForkAskReply, ForkAskRequest, ForkEndReply, ForkEndRequest, ForkFinish, ForkPrepareReply,
};
use cortexkit_role_llm_runner::usage::RunUsage;
use serde_json::{json, Value};

use crate::executor::{
    fork_admission_id, fork_conversation_key, ForkCallJob, ForkCallOutcome, ForkCallRefusal,
    ForkReleaseJob,
};
use crate::fork_state::{ForkState, FINISH_MODEL_ERROR};
use crate::store_open::StoreRuntime;

/// How long a fork lives after its prepare or its last counted ask: `fork_idle_s`, 600 s, as
/// Agent Runner's.
pub const FORK_IDLE_MS: u64 = 600_000;
/// How long a fork's stored answers are kept after it can last be used: the contract's minimum.
pub const FORK_RETAIN_MS: u64 = 3_600_000;
/// What a fork's session name puts between the head's session name and the fork id.
pub const FORK_SESSION_INFIX: &str = ":fork:";
/// The prefix of every fork id this runner mints, followed by 32 lowercase hex digits.
const FORK_ID_PREFIX: &str = "fork-";

/// Why a fork operation was refused. Nothing is stored for a refusal.
#[derive(Debug, Clone, PartialEq)]
pub enum ForkError {
    InvalidParams {
        field: String,
        message: String,
    },
    /// The caller may not prepare forks of this head.
    ScopeOwnerMismatch,
    /// No fork of that id answers this caller: none exists, it is another's, it is revoked, or
    /// it has expired.
    UnknownFork,
    ForkEnded,
    ForkBusy,
    AskIdReuse,
    BaseUnavailable(String),
    Unsupported(String),
    AccountHeld {
        routing_code: Option<String>,
        mismatch: Option<String>,
    },
    AccountBlocked {
        routing_code: String,
    },
    /// A store read or write failed; retrying may succeed.
    Transient(String),
}

impl ForkError {
    /// The error code the refusal is answered with.
    pub fn code(&self) -> &'static str {
        use cortexkit_role_llm_runner::errors;
        match self {
            Self::InvalidParams { .. } => errors::INVALID_PARAMS,
            Self::ScopeOwnerMismatch => errors::SCOPE_OWNER_MISMATCH,
            Self::UnknownFork => errors::UNKNOWN_FORK,
            Self::ForkEnded => errors::FORK_ENDED,
            Self::ForkBusy => errors::FORK_BUSY,
            Self::AskIdReuse => errors::ASK_ID_REUSE,
            Self::BaseUnavailable(_) => errors::FORK_BASE_UNAVAILABLE,
            Self::Unsupported(_) => errors::FORK_UNSUPPORTED,
            Self::AccountHeld { .. } => errors::FORK_ACCOUNT_HELD,
            Self::AccountBlocked { .. } => errors::FORK_ACCOUNT_BLOCKED,
            Self::Transient(_) => errors::TRANSIENT,
        }
    }

    /// The refusal's message, for a person reading it.
    pub fn message(&self) -> String {
        match self {
            Self::InvalidParams { message, .. } => message.clone(),
            Self::ScopeOwnerMismatch => "this caller may not fork the session".into(),
            Self::UnknownFork => "no such fork".into(),
            Self::ForkEnded => "the fork has ended".into(),
            Self::ForkBusy => "another ask is in flight on the fork".into(),
            Self::AskIdReuse => "the ask id was used for a different question".into(),
            Self::BaseUnavailable(reason) => {
                format!("the fork's base request is unavailable: {reason}")
            }
            Self::Unsupported(reason) => format!("forks are not served here: {reason}"),
            Self::AccountHeld { .. } => "the head's account is held; retry later".into(),
            Self::AccountBlocked { .. } => "routing refused the head's account".into(),
            Self::Transient(reason) => reason.clone(),
        }
    }

    /// The refusal's `detail`, when its code has one.
    pub fn detail(&self) -> Option<Value> {
        match self {
            Self::InvalidParams { field, .. } => Some(json!({ "field": field })),
            Self::AccountHeld {
                routing_code,
                mismatch,
            } => {
                let mut detail = serde_json::Map::new();
                if let Some(code) = routing_code {
                    detail.insert("routing_code".into(), json!(code));
                }
                if let Some(mismatch) = mismatch {
                    detail.insert("mismatch".into(), json!(mismatch));
                }
                Some(Value::Object(detail))
            }
            Self::AccountBlocked { routing_code } => Some(json!({ "routing_code": routing_code })),
            _ => None,
        }
    }
}

fn transient(what: &str) -> impl Fn(SessionError) -> ForkError + '_ {
    move |error| ForkError::Transient(format!("{what}: {error:?}"))
}

/// The session a fork of `head` with id `fork_id` lives in.
pub fn fork_session(head: &BindIdentity, fork_id: &str) -> BindIdentity {
    BindIdentity::new(
        head.project_root.clone(),
        head.harness.clone(),
        format!("{}{FORK_SESSION_INFIX}{fork_id}", head.session),
    )
}

/// Whether `fork_id` is one this runner could have minted. Any other id names no fork, and is
/// never used to name a session.
fn minted_fork_id(fork_id: &str) -> bool {
    fork_id.strip_prefix(FORK_ID_PREFIX).is_some_and(|hex| {
        hex.len() == 32
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn mint_fork_id() -> Result<String, ForkError> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes)
        .map_err(|error| ForkError::Transient(format!("no randomness for a fork id: {error}")))?;
    Ok(bytes
        .iter()
        .fold(String::from(FORK_ID_PREFIX), |mut id, byte| {
            id.push_str(&format!("{byte:02x}"));
            id
        }))
}

/// The principal fork admission matches: a module's id, as plan senders and readers are
/// matched. A caller that is no module matches no `fork_callers` entry.
fn caller_of(principal: Option<&BindPrincipal>) -> Option<&str> {
    match principal {
        Some(BindPrincipal::Reserved { module_id }) => Some(module_id),
        _ => None,
    }
}

/// An empty fork record of `event`, its other fields to be set by the caller.
fn record(fork_id: &str, event: ForkEvent, at_ms: u64) -> Fork {
    Fork {
        fork_id: fork_id.to_owned(),
        event,
        at_ms,
        ask_id: None,
        attempt: None,
        question: None,
        answer: None,
        finish: None,
        usage: None,
        principal: None,
        base: None,
        admission_id: None,
        idle_ms: None,
        retain_ms: None,
        detail: None,
    }
}

fn usage_value(usage: &RunUsage) -> Value {
    serde_json::to_value(usage).expect("usage encodes")
}

/// What a fork's prepare recorded about its base (`Fork::base`).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Base {
    run_id: String,
    step_id: u64,
    request_body_sha256: String,
    /// The head's config when the base was sent: what a restart releases the fork's routing
    /// admissions under, when the base can no longer be rebuilt.
    config: broca_wal::RunConfig,
}

/// A fork's session, open under its lease while an operation uses it.
struct OpenFork {
    key: BindIdentity,
    session: Session<LocalBackend>,
    records: Vec<Record>,
    state: ForkState,
    _lease: Box<dyn broca_core::LeaseHandle>,
    /// Set while an attempt of this process is with the executor. A fork opened from storage
    /// with an attempt in flight was cut by a restart.
    running: bool,
}

impl OpenFork {
    /// Appends `facts` durably and folds them into the fork's state.
    async fn append(&mut self, facts: Vec<Fork>) -> Result<(), ForkError> {
        let records: Vec<Record> = facts.into_iter().map(Record::Fork).collect();
        broca_session_loop::writer::Writer::append(&mut self.session, &records, true)
            .await
            .map_err(transient("write the fork's session"))?;
        self.records.extend(records);
        self.state = ForkState::fold(&self.records)
            .map_err(ForkError::Transient)?
            .ok_or_else(|| ForkError::Transient("the fork's session holds no fork".into()))?;
        Ok(())
    }
}

type Entry = Arc<tokio::sync::Mutex<OpenFork>>;

/// The head of a fork as an operation reads it: where it is kept, and its view.
struct Head {
    key: BindIdentity,
    class: StorageClass,
    view: crate::store_read::StoredLocalView,
}

/// Serves the fork operations of every head kept in the new storage.
pub struct ForkManager {
    runtime: Arc<StoreRuntime>,
    clock: broca_core::Clock,
    leases: broca_core::FileLeaseStore,
    /// The forks some operation holds open, by their session's address.
    open: tokio::sync::Mutex<HashMap<String, Entry>>,
}

impl ForkManager {
    /// Serves forks of the sessions `runtime` keeps, dated by `clock`: the clock retention
    /// decides by, so a test that moves it moves fork expiry too.
    pub fn new(runtime: Arc<StoreRuntime>, clock: broca_core::Clock) -> Self {
        let leases = broca_core::FileLeaseStore::new(runtime.state_root().join("leases"));
        Self {
            runtime,
            clock,
            leases,
            open: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    fn now_ms(&self) -> u64 {
        // A failed refresh keeps the last time read; it never moves the clock.
        let _ = self.clock.refresh();
        self.clock.now_ms()
    }

    /// The head `key` as it stands now, read-only. A head on today's storage serves no fork.
    async fn head(&self, key: &BindIdentity) -> Result<Option<Head>, ForkError> {
        let class = self
            .runtime
            .class_of(key)
            .map_err(|error| ForkError::Transient(format!("{error:?}")))?;
        let Some(class) = class else {
            return Err(ForkError::Unsupported(
                "the session isn't kept in the new storage".into(),
            ));
        };
        let view = self
            .runtime
            .current_view(key)
            .await
            .map_err(|error| ForkError::Transient(format!("read the head: {error:?}")))?;
        Ok(view.map(|view| Head {
            key: key.clone(),
            class,
            view,
        }))
    }

    fn parts<'a>(&'a self, head: &Head) -> &'a dyn broca_session_loop::parts::Parts {
        self.runtime
            .parts_for(head.class, head.view.head().cipher())
    }

    /// Whether the head's plan at acceptance lists `caller` in `fork_callers`.
    fn admits(&self, head: &Head, caller: &str) -> Result<bool, ForkError> {
        let manifest =
            broca_session_loop::plan::sender_manifest(&head.view.operations, self.parts(head))
                .map_err(ForkError::Transient)?;
        let Some(manifest) = manifest else {
            return Ok(false);
        };
        let plan: Value = serde_json::from_str(manifest.plan.as_json_text())
            .map_err(|error| ForkError::Transient(format!("the head's plan: {error}")))?;
        let Some(plan) = plan.as_object() else {
            return Ok(false);
        };
        // A plan admitted before its fork_callers were checked reads as admitting nobody.
        Ok(
            cortexkit_role_llm_runner::fork_answer::admit_prepare(plan, None, caller)
                .unwrap_or(false),
        )
    }

    /// The head request `run_id` sent at step `step`, rebuilt from the head's records, with the
    /// digest its bytes render to under the head's frozen render config. Refuses a request it
    /// can't rebuild or whose bytes differ from what the head's attempt recorded.
    async fn rebuild(
        &self,
        head: &Head,
        run_id: &str,
        step: u64,
    ) -> Result<broca_core::RecordedRequest, ForkError> {
        let request = crate::store_actor::recorded_warm::recorded_request(
            &**head.view,
            Some(self.runtime.class_backend(head.class)),
            self.parts(head),
            &head.key,
            run_id,
            Some(step),
        )
        .await
        .map_err(ForkError::BaseUnavailable)?;
        // Rendering media needs the head's media delivery, which only its actor holds.
        if request
            .opts
            .prompt
            .iter()
            .any(|message| !broca_session_store::session::message_media(message).is_empty())
        {
            return Err(ForkError::Unsupported(
                "the head's last request holds media".into(),
            ));
        }
        let digest =
            render_digest(&request.config, &request.opts).map_err(ForkError::BaseUnavailable)?;
        if digest != request.request_body_sha256 {
            return Err(ForkError::BaseUnavailable(format!(
                "the head's request rebuilds to {digest}, not the {} it sent",
                request.request_body_sha256
            )));
        }
        Ok(request)
    }

    /// The fork `fork_id` of `head`, open under its lease; `None` when there is none. A fork
    /// opened from storage with an attempt in flight, or an admission not released, was cut by
    /// a restart: it is settled here before anything else reads it.
    async fn entry(&self, head: &BindIdentity, fork_id: &str) -> Result<Option<Entry>, ForkError> {
        if !minted_fork_id(fork_id) {
            return Ok(None);
        }
        let key = fork_session(head, fork_id);
        let addr = broca_core::session_addr(&key);
        let mut open = self.open.lock().await;
        if let Some(entry) = open.get(&addr) {
            return Ok(Some(Arc::clone(entry)));
        }
        if self.runtime.class_of(&key).ok().flatten() != Some(StorageClass::Ephemeral) {
            return Ok(None);
        }
        let lease = self
            .leases
            .acquire(&broca_core::lease_key_for(&key))
            .map_err(|error| ForkError::Transient(format!("the fork's lease: {error:?}")))?;
        let session = match Session::load(
            self.runtime.class_backend(StorageClass::Ephemeral),
            SessionAddr(addr.clone()),
            &crate::store_open::triple(&key),
            self.runtime.deployment(),
        )
        .await
        {
            Ok(session) => session,
            Err(SessionError::Missing) => return Ok(None),
            Err(error) => return Err(transient("open the fork's session")(error)),
        };
        let records: Vec<Record> = session
            .records_from(0)
            .await
            .map_err(transient("read the fork's session"))?
            .into_iter()
            .map(|(_, record)| record)
            .collect();
        let Some(state) = ForkState::fold(&records).map_err(ForkError::Transient)? else {
            return Ok(None);
        };
        let mut fork = OpenFork {
            key,
            session,
            records,
            state,
            _lease: lease,
            running: false,
        };
        self.recover(&mut fork).await?;
        let entry = Arc::new(tokio::sync::Mutex::new(fork));
        open.insert(addr, Arc::clone(&entry));
        Ok(Some(entry))
    }

    /// Closes `entry` unless an attempt still runs on it, releasing its lease so retention may
    /// delete the fork's session once it is due.
    async fn settle(&self, entry: Entry) {
        let mut open = self.open.lock().await;
        let Ok(fork) = entry.try_lock() else {
            return;
        };
        if fork.running {
            return;
        }
        let addr = broca_core::session_addr(&fork.key);
        drop(fork);
        // The map's copy and this one: no other operation holds the fork.
        if Arc::strong_count(&entry) == 2 {
            open.remove(&addr);
        }
    }

    /// Stores an attempt a restart cut as `model_error` with the usage it recorded, and releases
    /// every routing admission no record says was released.
    async fn recover(&self, fork: &mut OpenFork) -> Result<(), ForkError> {
        let now = self.now_ms();
        if let Some(cut) = fork.state.in_flight.clone() {
            let mut finished = record(&fork.state.fork_id, ForkEvent::AskFinished, now);
            finished.ask_id = Some(cut.ask_id);
            finished.attempt = Some(cut.attempt);
            finished.answer = Some(String::new());
            finished.finish = Some(FINISH_MODEL_ERROR.into());
            finished.usage = Some(
                cut.usage
                    .clone()
                    .unwrap_or_else(|| usage_value(&RunUsage::default())),
            );
            finished.detail = Some(json!({"cause": "restart"}));
            fork.append(vec![finished]).await?;
        }
        let unreleased: Vec<String> = fork.state.unreleased.iter().cloned().collect();
        if unreleased.is_empty() {
            return Ok(());
        }
        let base: Base = serde_json::from_value(fork.state.base.clone())
            .map_err(|error| ForkError::Transient(format!("the fork's base: {error}")))?;
        let head = head_of(&fork.key, &fork.state.fork_id);
        for admission_id in unreleased {
            let job = ForkReleaseJob {
                head: head.clone(),
                config: base.config.clone(),
                run_id: admission_id.clone(),
                admission_id: admission_id.clone(),
            };
            match self.runtime.executor().fork_release(job).await {
                Ok(()) => {
                    let mut released = record(&fork.state.fork_id, ForkEvent::Released, now);
                    released.admission_id = Some(admission_id);
                    fork.append(vec![released]).await?;
                }
                Err(error) => tracing::warn!(
                    target: "fork",
                    fork = %fork.state.fork_id,
                    "a fork attempt's routing admission was not released: {error}"
                ),
            }
        }
        Ok(())
    }
}

/// The head a fork's session `key` names: its own session name without the fork's suffix.
fn head_of(key: &BindIdentity, fork_id: &str) -> BindIdentity {
    let suffix = format!("{FORK_SESSION_INFIX}{fork_id}");
    BindIdentity::new(
        key.project_root.clone(),
        key.harness.clone(),
        key.session
            .strip_suffix(&suffix)
            .unwrap_or(&key.session)
            .to_owned(),
    )
}

/// The digest of `opts` rendered by the wire family `config` froze, as the store actor learns a
/// warm's digest before the executor sends it.
fn render_digest(
    config: &broca_wal::RunConfig,
    opts: &broca_protocol::request::CallOptions,
) -> Result<String, String> {
    Ok(broca_core::render::request_digest_hex(&render(
        config, opts,
    )?))
}

/// `opts` rendered by the wire family `config` froze.
pub(crate) fn render(
    config: &broca_wal::RunConfig,
    opts: &broca_protocol::request::CallOptions,
) -> Result<Vec<u8>, String> {
    // A provider with no render state of its own renders the shared canonical bytes, and froze
    // only a marker saying so (`broca_core::Provider::render_config_blob`).
    if config.frozen_render_config == json!({ "render_config": "stateless" }) {
        return Ok(broca_core::render::canonical_request_bytes(opts));
    }
    let frozen: broca_provider::wire::FrozenRenderConfig =
        serde_json::from_value(config.frozen_render_config.clone())
            .map_err(|error| format!("the head's render config: {error}"))?;
    let family = broca_provider::registry::wire_family(frozen.family_id)
        .ok_or("the head's wire family isn't known")?;
    Ok(family.render_with_provenance(&frozen.for_step(opts), opts, None))
}

/// The request a fork's ask sends: the base's options with the fork's earlier questions and
/// answers and then `question` appended, and nothing else changed.
pub fn fork_request(
    base: &broca_protocol::request::CallOptions,
    branch: &[crate::fork_state::Turn],
    question: &str,
) -> broca_protocol::request::CallOptions {
    use broca_protocol::content::{ContentBlock, Message, Role};
    let text = |role, text: &str| {
        Message::new(
            role,
            vec![ContentBlock::Text {
                text: text.to_owned(),
            }],
        )
    };
    let mut opts = base.clone();
    for turn in branch {
        opts.prompt.push(text(Role::User, &turn.question));
        opts.prompt.push(text(Role::Assistant, &turn.answer));
    }
    opts.prompt.push(text(Role::User, question));
    opts
}

/// The head's routing conversation key, as its frozen routing decision recorded it.
fn head_conversation_key(config: &broca_wal::RunConfig) -> Option<&str> {
    config
        .auth_selection
        .as_ref()?
        .account_decision
        .as_ref()?
        .conversation_key
        .as_deref()
}

impl ForkManager {
    /// `session.fork_prepare` on `head`, for the route's attested `principal`.
    pub async fn prepare(
        &self,
        head: &BindIdentity,
        principal: Option<&BindPrincipal>,
    ) -> Result<ForkPrepareReply, ForkError> {
        let read = self.head(head).await?;
        let caller = caller_of(principal);
        let Some(read) = read else {
            // A session that was never written lists no caller.
            return Err(ForkError::ScopeOwnerMismatch);
        };
        let Some(caller) = caller.filter(|caller| self.admits(&read, caller).unwrap_or(false))
        else {
            return Err(ForkError::ScopeOwnerMismatch);
        };
        // The head's newest attempt, found by reading its ledger back from the end only as far
        // as that attempt, as idle cache warming finds it.
        let tail = read
            .view
            .ledger_tail_until(|record| matches!(record, Record::Attempt(_)))
            .await
            .map_err(transient("read the head's last request"))?;
        let Some((run_id, step_id)) = tail.iter().rev().find_map(|(_, record)| match record {
            Record::Attempt(attempt) => Some((attempt.run_id.clone(), attempt.step_id)),
            _ => None,
        }) else {
            return Err(ForkError::BaseUnavailable(
                "the head has sent no model request".into(),
            ));
        };
        let request = self.rebuild(&read, &run_id, step_id).await?;
        let base = Base {
            run_id,
            step_id,
            request_body_sha256: request.request_body_sha256,
            config: request.config,
        };
        let fork_id = mint_fork_id()?;
        let key = fork_session(head, &fork_id);
        let addr = broca_core::session_addr(&key);
        let lease = self
            .leases
            .acquire(&broca_core::lease_key_for(&key))
            .map_err(|error| ForkError::Transient(format!("the fork's lease: {error:?}")))?;
        let session = Session::create(
            self.runtime.class_backend(StorageClass::Ephemeral),
            SessionAddr(addr.clone()),
            crate::store_open::triple(&key),
            self.runtime.deployment(),
            false,
        )
        .await
        .map_err(transient("create the fork's session"))?;
        let mut prepared = record(&fork_id, ForkEvent::Prepared, self.now_ms());
        prepared.principal = Some(caller.to_owned());
        prepared.base = Some(serde_json::to_value(&base).expect("a fork's base encodes"));
        prepared.idle_ms = Some(FORK_IDLE_MS);
        prepared.retain_ms = Some(FORK_RETAIN_MS);
        let records = vec![Record::Fork(prepared)];
        let mut session = session;
        broca_session_loop::writer::Writer::append(&mut session, &records, true)
            .await
            .map_err(transient("write the fork's prepare"))?;
        drop(session);
        drop(lease);
        Ok(ForkPrepareReply::new(fork_id))
    }

    /// Refuses `caller` as `unknown_fork` unless it prepared the fork and the head's plan still
    /// lists it. A preparer the plan no longer lists is revoked here, durably.
    async fn check_caller(
        &self,
        fork: &mut OpenFork,
        head: &BindIdentity,
        caller: Option<&str>,
    ) -> Result<(), ForkError> {
        if caller != Some(fork.state.principal.as_str()) || fork.state.revoked_at_ms.is_some() {
            return Err(ForkError::UnknownFork);
        }
        // A head that is gone leaves its forks' stored answers readable.
        let Some(read) = self.head(head).await.or_else(|error| match error {
            ForkError::Unsupported(_) => Ok(None),
            error => Err(error),
        })?
        else {
            return Ok(());
        };
        if self.admits(&read, &fork.state.principal)? {
            return Ok(());
        }
        let revoked = record(&fork.state.fork_id, ForkEvent::Revoked, self.now_ms());
        fork.append(vec![revoked]).await?;
        Err(ForkError::UnknownFork)
    }

    /// `session.fork_ask` on a fork of `head`.
    pub async fn ask(
        &self,
        head: &BindIdentity,
        principal: Option<&BindPrincipal>,
        request: ForkAskRequest,
    ) -> Result<ForkAskReply, ForkError> {
        if let Err(field) = request.check() {
            return Err(ForkError::InvalidParams {
                field: field.into(),
                message: format!("{field} is out of bounds"),
            });
        }
        let Some(entry) = self.entry(head, &request.fork_id).await? else {
            return Err(ForkError::UnknownFork);
        };
        let result = self.ask_on(&entry, head, principal, request).await;
        self.settle(entry).await;
        result
    }

    async fn ask_on(
        &self,
        entry: &Entry,
        head: &BindIdentity,
        principal: Option<&BindPrincipal>,
        request: ForkAskRequest,
    ) -> Result<ForkAskReply, ForkError> {
        let caller = caller_of(principal);
        let mut fork = entry.lock().await;
        self.check_caller(&mut fork, head, caller).await?;
        if let Some(ask) = fork.state.asks.get(&request.ask_id) {
            if let Some(answer) = &ask.answer {
                if ask.question != request.question {
                    return Err(ForkError::AskIdReuse);
                }
                return stored_reply(answer);
            }
        }
        if fork.state.in_flight.is_some() {
            return Err(ForkError::ForkBusy);
        }
        if fork.state.expired(self.now_ms()) {
            return Err(ForkError::UnknownFork);
        }
        if fork.state.ended_at_ms.is_some() {
            return Err(ForkError::ForkEnded);
        }
        let base: Base = serde_json::from_value(fork.state.base.clone())
            .map_err(|error| ForkError::Transient(format!("the fork's base: {error}")))?;
        let read = self
            .head(head)
            .await?
            .ok_or_else(|| ForkError::BaseUnavailable("the head is gone".into()))?;
        let rebuilt = self.rebuild(&read, &base.run_id, base.step_id).await?;
        if rebuilt.request_body_sha256 != base.request_body_sha256 {
            return Err(ForkError::BaseUnavailable(
                "the head's recorded request is not the fork's base".into(),
            ));
        }
        drop(read);
        let fork_id = fork.state.fork_id.clone();
        let attempt = fork
            .state
            .asks
            .get(&request.ask_id)
            .map_or(0, |ask| ask.attempts)
            + 1;
        let admission_id = fork_admission_id(&fork_id, &request.ask_id, attempt);
        let mut started = record(&fork_id, ForkEvent::AskStarted, self.now_ms());
        started.ask_id = Some(request.ask_id.clone());
        started.attempt = Some(attempt);
        started.question = Some(request.question.clone());
        started.admission_id = Some(admission_id.clone());
        fork.append(vec![started]).await?;
        fork.running = true;
        let call = fork_request(&rebuilt.opts, &fork.state.branch, &request.question);
        let conversation_key =
            head_conversation_key(&rebuilt.config).map(|key| fork_conversation_key(&fork_id, key));
        drop(fork);

        // Usage the executor reports before the call ends is recorded as it comes, so a restart
        // stores the attempt with what it spent.
        let (usage_tx, mut usage_rx) = tokio::sync::mpsc::unbounded_channel::<RunUsage>();
        let usage_writer = {
            let entry = Arc::clone(entry);
            let fork_id = fork_id.clone();
            let ask_id = request.ask_id.clone();
            let clock = self.clock.clone();
            tokio::spawn(async move {
                while let Some(usage) = usage_rx.recv().await {
                    let mut fork = entry.lock().await;
                    // Reports that arrive after the attempt ended are dropped: its end records
                    // its whole usage.
                    if !fork.running {
                        break;
                    }
                    let mut fact = record(&fork_id, ForkEvent::AskUsage, clock.now_ms());
                    fact.ask_id = Some(ask_id.clone());
                    fact.attempt = Some(attempt);
                    fact.usage = Some(usage_value(&usage));
                    if let Err(error) = fork.append(vec![fact]).await {
                        tracing::warn!(target: "fork", fork = %fork_id, "{error:?}");
                    }
                }
            })
        };
        let job = ForkCallJob {
            head: head.clone(),
            fork_id: fork_id.clone(),
            ask_id: request.ask_id.clone(),
            attempt,
            run_id: admission_id.clone(),
            base: Box::new(rebuilt),
            request: call,
            admission_id: admission_id.clone(),
            conversation_key,
            on_usage: Some(Arc::new(move |usage| {
                let _ = usage_tx.send(usage);
            })),
        };
        let executor = Arc::clone(self.runtime.executor());
        let outcome =
            futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(executor.fork_call(job)))
                .await
                .unwrap_or_else(|_| ForkCallOutcome::ModelFailed {
                    usage: RunUsage::default(),
                    error: "the fork's model call panicked".into(),
                });
        // The usage writer ends on its own: it stops at the first report after `running` is
        // cleared below, or when the executor drops its callback.
        drop(usage_writer);

        let mut fork = entry.lock().await;
        fork.running = false;
        let now = self.now_ms();
        let mut facts = Vec::new();
        let reply = match outcome {
            ForkCallOutcome::Answered {
                answer,
                finish,
                usage,
            } => {
                let refused = finish == ForkFinish::ToolCallRefused.as_str();
                let mut finished = record(&fork_id, ForkEvent::AskFinished, now);
                finished.ask_id = Some(request.ask_id.clone());
                finished.attempt = Some(attempt);
                finished.answer = Some(answer.clone());
                finished.finish = Some(finish.clone());
                finished.usage = Some(usage_value(&usage));
                facts.push(finished);
                // A reply that called a tool ends the fork: its answer stands as given.
                if refused && fork.state.ended_at_ms.is_none() {
                    facts.push(record(&fork_id, ForkEvent::Ended, now));
                }
                Ok(ForkAskReply::new(answer, ForkFinish::parse(&finish), usage))
            }
            ForkCallOutcome::ModelFailed { usage, error } => {
                let mut finished = record(&fork_id, ForkEvent::AskFinished, now);
                finished.ask_id = Some(request.ask_id.clone());
                finished.attempt = Some(attempt);
                finished.answer = Some(String::new());
                finished.finish = Some(FINISH_MODEL_ERROR.into());
                finished.usage = Some(usage_value(&usage));
                finished.detail = Some(json!({ "error": error }));
                facts.push(finished);
                Ok(ForkAskReply::new("", ForkFinish::ModelError, usage))
            }
            ForkCallOutcome::Refused(refusal) => {
                let error = match refusal {
                    ForkCallRefusal::BaseUnavailable(reason) => ForkError::BaseUnavailable(reason),
                    ForkCallRefusal::AccountHeld {
                        routing_code,
                        mismatch,
                    } => ForkError::AccountHeld {
                        routing_code,
                        mismatch,
                    },
                    ForkCallRefusal::AccountBlocked { routing_code } => {
                        ForkError::AccountBlocked { routing_code }
                    }
                    ForkCallRefusal::Unsupported(reason) => ForkError::Unsupported(reason),
                };
                let mut refused = record(&fork_id, ForkEvent::AskRefused, now);
                refused.ask_id = Some(request.ask_id.clone());
                refused.attempt = Some(attempt);
                refused.detail = Some(json!({ "code": error.code() }));
                facts.push(refused);
                Err(error)
            }
        };
        // The executor released the attempt's admission, if routing granted one, before it
        // returned.
        let mut released = record(&fork_id, ForkEvent::Released, now);
        released.admission_id = Some(admission_id);
        facts.push(released);
        fork.append(facts).await?;
        // A caller revoked while its ask was in flight gets no answer, though it is stored.
        self.check_caller(&mut fork, head, caller).await?;
        reply
    }

    /// `session.fork_end` on a fork of `head`.
    pub async fn end(
        &self,
        head: &BindIdentity,
        principal: Option<&BindPrincipal>,
        request: ForkEndRequest,
    ) -> Result<ForkEndReply, ForkError> {
        let Some(entry) = self.entry(head, &request.fork_id).await? else {
            return Err(ForkError::UnknownFork);
        };
        let result = async {
            let mut fork = entry.lock().await;
            self.check_caller(&mut fork, head, caller_of(principal))
                .await?;
            if fork.state.ended_at_ms.is_none() {
                let ended = record(&fork.state.fork_id, ForkEvent::Ended, self.now_ms());
                fork.append(vec![ended]).await?;
            }
            Ok(ForkEndReply::new(request.fork_id.clone()))
        }
        .await;
        self.settle(entry).await;
        result
    }
}

/// A stored answer, replayed as it was given.
fn stored_reply(answer: &crate::fork_state::StoredAnswer) -> Result<ForkAskReply, ForkError> {
    let usage: RunUsage = serde_json::from_value(answer.usage.clone())
        .map_err(|error| ForkError::Transient(format!("a stored answer's usage: {error}")))?;
    Ok(ForkAskReply::new(
        answer.answer.clone(),
        ForkFinish::parse(&answer.finish),
        usage,
    ))
}

#[cfg(test)]
#[path = "fork_answer_tests.rs"]
mod tests;
