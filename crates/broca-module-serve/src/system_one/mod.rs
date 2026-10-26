//! System One: broca serving one-pass decision models (`classifier/v1`) beside its agent
//! sessions, in the same process.
//!
//! A System One model answers in one pass and bills on input only. A caller opens a route to
//! broca's management surface declaring the `classifier` role (see
//! [`broca_subc::CLASSIFIER_ROLE_VERSIONS`]) and sends `classify.run` batches: one model, a
//! set of questions, and items, each answered by one provider call. The role's rules are the
//! published contracts `cortexkit-role-classifier` (its `CONTRACT.md`) and
//! `cortexkit-model-batch` (its `RULES.md`).
//!
//! Sharing a process with agent sessions puts one hard requirement on everything here: this
//! path can never block, interrupt or corrupt a session. So:
//!
//! - It holds no session. Nothing here opens or creates a session, takes a session lease, or
//!   reads or writes a WAL or `run-index.db`. Its durable record is its own SQLite file
//!   (`broca_store::SystemOneStore`).
//! - Each batch call runs on its own task ([`SystemOne::spawn_call`]), never on the serve
//!   loop, and holds none of the connection's request slots, so a slow or hung provider waits
//!   only on itself. Concurrency is capped per process (batch calls) and per model (provider
//!   calls).
//! - A panic while serving a call answers that call with an error and releases its batch;
//!   nothing else sees it.
//! - Each running batch call raises broca's busy gauge, so a drain waits for it within the
//!   existing grace, and never longer.

mod classify;
pub mod credential;
pub mod typesafe;
pub mod workers_ai;

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use broca_catalog::ModelBatchRow;
use broca_store::SystemOneStore;
use cortexkit_role_classifier::{
    describe::{ImageSupport, Limits, ModelEntry, Price, RoleDescribe},
    errors::{ErrorClass, Refusal, RefusalDetail},
};
use serde_json::value::RawValue;
use tokio::sync::Semaphore;

/// The store's role key for `classifier/v1` batches, and the role a [`SystemOne`] serves
/// under unless [`SystemOne::with_store_role`] names another.
pub(crate) const STORE_ROLE: &str = "classifier";

/// The store's role key for the `llm.classify/v1` tool's batches. The tool runs on its own
/// [`SystemOne`] under this role, so a tool call and a `classifier/v1` batch that happen to
/// share a `batch_id` are two batches: neither replays, holds or refuses the other.
pub const LLM_CLASSIFY_STORE_ROLE: &str = "llm.classify";

/// The method names this surface answers.
pub const OP_ROLE_DESCRIBE: &str = cortexkit_role_classifier::ops::ROLE_DESCRIBE;
pub const OP_CLASSIFY_RUN: &str = cortexkit_role_classifier::ops::CLASSIFY_RUN;

/// The catalog id of TypeSafe's Jev 1.13.0.
pub const JEV_1_13_0: &str = cortexkit_role_classifier::provider::catalog_ids::JEV_1_13_0;
/// The catalog id of Cloudflare Workers AI's Clef.
pub const CLEF: &str = cortexkit_role_classifier::provider::catalog_ids::CLEF;
/// The catalog id of Cloudflare Workers AI's Clef-flash.
pub const CLEF_FLASH: &str = cortexkit_role_classifier::provider::catalog_ids::CLEF_FLASH;

/// The code a call is answered with when serving it failed inside broca (a panic, or the
/// batch store refusing a write). Transient: the caller re-sends, and the stored outcomes
/// spare the provider calls already answered.
pub const INTERNAL_ERROR: &str = "internal_error";

/// How long a caller refused `batch_in_progress` is told to wait before re-sending.
pub const BATCH_IN_PROGRESS_RETRY_AFTER_MS: u64 = 1_000;

/// How often the retention sweep runs.
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// The limits this build serves under.
#[derive(Clone, Debug)]
pub struct Config {
    /// Most items in one batch. A call is synchronous, so a batch has to fit the caller's call
    /// deadline: items are sent one after another; each provider attempt is bounded by
    /// `attempt_timeout`, and a retry the provider asked for adds its attempts and wait.
    pub max_items: u64,
    /// Largest `classify.run` params, in bytes.
    pub max_request_bytes: u64,
    /// Batch calls this process runs at once; a call beyond it waits, on its own task.
    pub process_calls: usize,
    /// Provider calls one model gets at once.
    pub model_calls: usize,
    /// One provider attempt's whole deadline.
    pub attempt_timeout: Duration,
    /// The longest `Retry-After` the module waits out itself before a retry. A longer one
    /// ends the item with the provider's error, carrying the wait for the caller.
    pub max_retry_wait: Duration,
    /// Provider attempts per item, the first included.
    pub max_attempts: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_items: 16,
            max_request_bytes: 1024 * 1024,
            process_calls: 16,
            model_calls: 4,
            attempt_timeout: Duration::from_secs(30),
            max_retry_wait: Duration::from_secs(2),
            max_attempts: 3,
        }
    }
}

/// What the module reads from its catalog. The live implementation is
/// [`LiveCatalog`]; tests supply rows directly.
pub trait BatchCatalog: Send + Sync {
    /// The row for a catalog id, `None` when the catalog lists none.
    fn row(&self, catalog_id: &str) -> Option<ModelBatchRow>;
}

/// broca's live catalog: the process-global snapshot, read without IO.
pub struct LiveCatalog;

impl BatchCatalog for LiveCatalog {
    fn row(&self, catalog_id: &str) -> Option<ModelBatchRow> {
        broca_catalog::catalog().ok()?.model_batch_row(catalog_id)
    }
}

/// What one provider attempt came back with.
#[derive(Debug)]
pub enum ProviderReply {
    /// HTTP 200, with the body as received.
    Answered(Vec<u8>),
    /// An answer tied to the credential resolved for this attempt, not a later lookup.
    AttributedAnswer {
        body: Vec<u8>,
        auth_selection: Option<String>,
        account_ref: Option<String>,
    },
    /// Any other HTTP status, with the wait the provider asked for, if it asked.
    Status {
        status: u16,
        retry_after_ms: Option<u64>,
    },
    /// A Cloudflare Workers AI reply that carried no answer: its HTTP status, the first
    /// `errors[].code` of its body when it named one, and the wait it asked for. Mapped by
    /// that code first, then by the status (the classifier role's Workers AI table).
    WorkersAiError {
        status: u16,
        code: Option<u64>,
        retry_after_ms: Option<u64>,
    },
    /// No reply: the connection failed or the attempt ran out of time.
    Transport(String),
    /// The credential could not be read, so nothing was sent.
    Credential(String),
}

/// One provider's classification endpoint.
#[async_trait]
pub trait ClassifierProvider: Send + Sync {
    /// The host item text is sent to, as `role.describe` names it.
    fn egress_host(&self) -> String;
    /// The id of the credential this provider presents, for `auth_failed`'s detail. Never a
    /// token.
    fn credential_id(&self) -> Option<String>;
    /// Send one provider request body (`{model, state, questions, images?}`) and return what
    /// came back.
    async fn call(&self, body: String) -> ProviderReply;
}

/// One model broca routes, pinned in code: the catalog id, the provider's own model value,
/// and the module's limits for it. The catalog says whether the model is listed, its kind,
/// price and context; this says how broca reaches it.
#[derive(Clone)]
pub struct ModelPin {
    pub catalog_id: String,
    /// The `model` value the provider expects in its request body.
    pub provider_model: String,
    pub max_questions: u64,
    /// Images per item when the catalog row allows attachments; `None` for a text-only
    /// provider.
    pub images_max: Option<u64>,
    pub provider: Arc<dyn ClassifierProvider>,
}

/// The `model` value broca sends for the `typesafe/jev-1.13.0` row: the pinned version, never
/// TypeSafe's moving `jev-latest` alias, which already names a newer model and would let it
/// answer, and be billed, as 1.13.0.
const JEV_1_13_0_WIRE: &str = cortexkit_role_classifier::provider::models::JEV_1_13_0;

/// Consent uses only the catalog's human labels. A missing display fact is not permission
/// to show a provider or model identifier on the person's approval card.
pub(crate) fn consent_model_labels(row: &ModelBatchRow) -> Option<(&str, &str)> {
    let provider = row.provider_name.as_deref()?;
    let model = row.model_name.as_deref()?;
    if provider.trim().is_empty() || model.trim().is_empty() {
        return None;
    }
    Some((provider, model))
}

impl ModelPin {
    /// TypeSafe's Jev 1.13.0, text only. Its question limit is the role's own question-id
    /// space at the clef bound, since TypeSafe states none.
    pub fn jev(provider: Arc<dyn ClassifierProvider>) -> Self {
        Self {
            catalog_id: JEV_1_13_0.to_owned(),
            provider_model: JEV_1_13_0_WIRE.to_owned(),
            max_questions: cortexkit_role_classifier::provider::clef_limits::MAX_QUESTIONS as u64,
            images_max: None,
            provider,
        }
    }

    /// Cloudflare Workers AI's Clef, which takes `clef`: up to 64 questions and 4 images per
    /// item (Cloudflare's limits), images only where the catalog row allows attachments.
    pub fn clef(provider: Arc<dyn ClassifierProvider>) -> Self {
        Self::workers_ai(
            CLEF,
            cortexkit_role_classifier::provider::models::CLEF,
            provider,
        )
    }

    /// Cloudflare Workers AI's Clef-flash, which takes `clef-flash`, with Clef's limits.
    pub fn clef_flash(provider: Arc<dyn ClassifierProvider>) -> Self {
        Self::workers_ai(
            CLEF_FLASH,
            cortexkit_role_classifier::provider::models::CLEF_FLASH,
            provider,
        )
    }

    fn workers_ai(
        catalog_id: &str,
        provider_model: &str,
        provider: Arc<dyn ClassifierProvider>,
    ) -> Self {
        use cortexkit_role_classifier::provider::clef_limits;
        Self {
            catalog_id: catalog_id.to_owned(),
            provider_model: provider_model.to_owned(),
            max_questions: clef_limits::MAX_QUESTIONS as u64,
            images_max: Some(clef_limits::MAX_IMAGES as u64),
            provider,
        }
    }
}

/// The System One service: the batch store, the routing pins, and the concurrency caps.
pub struct SystemOne {
    store: Arc<SystemOneStore>,
    /// The store role every batch this service claims, records and releases is keyed under.
    role: String,
    /// This process's name on a batch it holds, so a hold left by another process is told
    /// apart and a write after a lost hold is refused.
    owner: String,
    catalog: Arc<dyn BatchCatalog>,
    pins: Vec<ModelPin>,
    config: Config,
    /// Batch ids a call in this process holds now: the in-process half of single flight.
    claims: Mutex<HashMap<String, broca_store::system_one::BatchScope>>,
    call_slots: Arc<Semaphore>,
    model_slots: Mutex<HashMap<String, Arc<Semaphore>>>,
    in_flight: Arc<AtomicUsize>,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    metrics: Mutex<Metrics>,
    export: Option<Arc<broca_store::SqliteStore>>,
    charge_basis: Arc<crate::billing_routing::ChargeBasisTable>,
    /// Tests only: when set, the next store release made from a hold's `Drop` (the fallback
    /// for a panicked or dropped call) waits until the sender side is dropped. A test holds
    /// it shut to prove that a call which ends normally never leaves its release to that
    /// fallback, since a release still pending after the reply refuses an immediate re-send.
    #[cfg(test)]
    fallback_release_gate: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    /// Tests hold publication after the outcome/outbox commit to expose premature replay.
    #[cfg(test)]
    pub(crate) fact_publish_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

/// Pricing accepted before private-content admission. Keeping this snapshot with a delayed
/// call prevents a catalog refresh from changing what was already admitted.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ToolAdmission {
    frozen_row: String,
}

/// Process-lifetime observations only. No batch ids, questions, states or answers enter health.
#[derive(Default, serde::Serialize)]
struct Metrics {
    batches_answered: u64,
    batches_failed: u64,
    items_answered: u64,
    items_failed: u64,
    spend_nanos_by_model: std::collections::BTreeMap<String, u64>,
    last_provider_error: std::collections::BTreeMap<String, String>,
}

/// The wall clock in milliseconds since the Unix epoch.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as i64)
}

impl SystemOne {
    /// A service over `store`. `in_flight` is the gauge each running batch call raises (the
    /// registry's `batches_in_flight_gauge`).
    pub fn new(
        store: Arc<SystemOneStore>,
        owner: impl Into<String>,
        catalog: Arc<dyn BatchCatalog>,
        pins: Vec<ModelPin>,
        config: Config,
        in_flight: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            store,
            role: STORE_ROLE.to_owned(),
            owner: owner.into(),
            catalog,
            pins,
            call_slots: Arc::new(Semaphore::new(config.process_calls.max(1))),
            config,
            claims: Mutex::new(HashMap::new()),
            model_slots: Mutex::new(HashMap::new()),
            in_flight,
            clock: Arc::new(now_ms),
            metrics: Mutex::new(Metrics::default()),
            export: None,
            charge_basis: Arc::new(crate::billing_routing::ChargeBasisTable::shipped_defaults()),
            #[cfg(test)]
            fallback_release_gate: Mutex::new(None),
            #[cfg(test)]
            fact_publish_hook: Mutex::new(None),
        }
    }

    /// Reuse the daemon-wide immutable export stream, without opening any session projection.
    pub fn with_export(
        mut self,
        export: Arc<broca_store::SqliteStore>,
        charge_basis: Arc<crate::billing_routing::ChargeBasisTable>,
    ) -> Self {
        self.export = Some(export);
        self.charge_basis = charge_basis;
        self
    }

    fn flush_facts(&self) -> Result<(), String> {
        let Some(export) = &self.export else {
            return Ok(());
        };
        for pending in self.store.pending_facts().map_err(|e| e.to_string())? {
            #[cfg(test)]
            {
                let hook = self.fact_publish_hook.lock().unwrap().clone();
                if let Some(hook) = hook {
                    hook();
                }
            }
            if export
                .emit_system_one_fact(&pending.fact)
                .map_err(|e| e.to_string())?
            {
                let mut metrics = self.metrics.lock().unwrap_or_else(|p| p.into_inner());
                let spend = metrics
                    .spend_nanos_by_model
                    .entry(pending.fact.model.clone())
                    .or_default();
                *spend = spend.saturating_add(pending.spend_nanos);
            }
            self.store
                .acknowledge_fact(&pending.fact.fact_id)
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    async fn publish_facts(self: &Arc<Self>) -> Result<(), Refusal> {
        let service = Arc::clone(self);
        tokio::task::spawn_blocking(move || service.flush_facts())
            .await
            .map_err(|_| internal_refusal("usage export task failed"))?
            .map_err(|_| internal_refusal("usage export is unavailable; re-send the batch"))
    }

    pub fn health(&self) -> serde_json::Value {
        let metrics = self.metrics.lock().unwrap_or_else(|p| p.into_inner());
        let mut value = serde_json::to_value(&*metrics).expect("health counters encode");
        value["batches_in_flight"] = self.in_flight.load(Ordering::Relaxed).into();
        value
    }

    /// Serve under another store role. Each `SystemOne` keeps its own in-process claims and its
    /// own process-call and per-model semaphores, so a second instance under another role (the
    /// `llm.classify/v1` tool's, [`LLM_CLASSIFY_STORE_ROLE`]) can share the batch store
    /// without sharing a batch id space or concurrency with `classifier/v1`.
    pub fn with_store_role(mut self, role: impl Into<String>) -> Self {
        self.role = role.into();
        self
    }

    /// The store role this service's batches are keyed under.
    pub fn store_role(&self) -> &str {
        &self.role
    }

    /// Share durable storage and provider clients, but not claims or concurrency permits.
    /// Startup releases abandoned claims once, before either instance starts serving.
    pub fn fork_for_tools(&self) -> Self {
        let mut service = Self::new(
            self.store.clone(),
            self.owner.clone(),
            self.catalog.clone(),
            self.pins.clone(),
            self.config.clone(),
            self.in_flight.clone(),
        )
        .with_store_role(LLM_CLASSIFY_STORE_ROLE)
        .with_clock(self.clock.clone());
        service.export = self.export.clone();
        service.charge_basis = self.charge_basis.clone();
        service
    }

    pub(crate) fn tool_now(&self) -> i64 {
        (self.clock)()
    }

    pub(crate) fn admit_tool(
        &self,
        params: &str,
        stored: Option<&broca_store::system_one::StoredBatch>,
    ) -> Result<ToolAdmission, Refusal> {
        classify::admit_tool(self, params, stored)
    }

    pub(crate) fn tool_model_row(&self, model: &str) -> Option<ModelBatchRow> {
        self.catalog.row(model)
    }

    pub(crate) async fn replay_tool(
        self: &Arc<Self>,
        params: &str,
        stored: &broca_store::system_one::StoredBatch,
    ) -> Result<Box<RawValue>, Refusal> {
        // The outcome and outbox commit together, but publication is a separate transaction.
        // A replay can see that outcome while its original execution is still exporting it.
        self.publish_facts().await?;
        classify::replay_tool(params, stored)
    }

    pub(crate) async fn execute_tool(
        self: Arc<Self>,
        params: String,
        scope: broca_store::system_one::BatchScope,
        args_digest: String,
        admission: ToolAdmission,
        admitted_at_ms: Option<i64>,
    ) -> Result<Box<RawValue>, Refusal> {
        self.sweep()
            .await
            .map_err(|_| internal_refusal("batch retention is unavailable"))?;
        let gauge = self.in_flight.clone();
        gauge.fetch_add(1, Ordering::Relaxed);
        let _lowered = GaugeGuard(gauge);
        let _slot = self.call_slots.clone().acquire_owned().await;
        classify::run_inner(
            &self,
            &params,
            &scope,
            admitted_at_ms,
            Some((&args_digest, &admission)),
        )
        .await
    }

    /// The batch recorded under this service's role for `batch_id`, with its outcomes; `None`
    /// when there is none. Reads only: nothing is claimed or written.
    pub async fn stored_batch(
        &self,
        batch_id: &str,
    ) -> Result<Option<broca_store::system_one::StoredBatch>, String> {
        let store = Arc::clone(&self.store);
        let (role, batch_id) = (self.role.clone(), batch_id.to_owned());
        tokio::task::spawn_blocking(move || store.lookup(&role, &batch_id))
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())
    }

    /// Replace the clock; tests drive retention with it.
    pub fn with_clock(mut self, clock: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        self.clock = clock;
        self
    }

    /// Release provably abandoned batches without disturbing a live owner. Call before serving.
    pub fn release_abandoned(&self) -> Result<usize, String> {
        self.flush_facts()?;
        self.store.release_abandoned().map_err(|e| e.to_string())
    }

    /// Delete batches past retention. Returns how many batches went.
    pub async fn sweep(&self) -> Result<usize, String> {
        let store = Arc::clone(&self.store);
        let now = (self.clock)();
        tokio::task::spawn_blocking(move || store.sweep(now))
            .await
            .map_err(|e| e.to_string())?
            .map(|counts| counts.batches)
            .map_err(|e| e.to_string())
    }

    /// Run the retention sweep every [`SWEEP_INTERVAL`], first at once, until `stop` fires.
    pub fn spawn_sweeper(
        self: &Arc<Self>,
        stop: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let service = Arc::clone(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(SWEEP_INTERVAL);
            loop {
                tokio::select! {
                    biased;
                    _ = stop.cancelled() => return,
                    _ = tick.tick() => {}
                }
                match service.sweep().await {
                    Ok(0) => {}
                    Ok(batches) => tracing::info!(target: "system_one", batches,
                        "retention removed batches first admitted over seven days ago"),
                    Err(error) => tracing::warn!(target: "system_one", %error,
                        "retention sweep failed; it runs again within the hour"),
                }
            }
        })
    }

    fn pin(&self, catalog_id: &str) -> Option<&ModelPin> {
        self.pins.iter().find(|pin| pin.catalog_id == catalog_id)
    }

    fn model_slot(&self, catalog_id: &str) -> Arc<Semaphore> {
        let mut slots = self.model_slots.lock().unwrap_or_else(|p| p.into_inner());
        Arc::clone(
            slots
                .entry(catalog_id.to_owned())
                .or_insert_with(|| Arc::new(Semaphore::new(self.config.model_calls.max(1)))),
        )
    }

    /// The `role.describe` entry for a pinned model whose catalog row says it is a classifier.
    fn model_entry(&self, pin: &ModelPin, row: &ModelBatchRow) -> ModelEntry {
        let mut entry = ModelEntry::new(
            pin.catalog_id.clone(),
            pin.max_questions,
            self.config.max_items,
            row.context_tokens.filter(|tokens| *tokens > 0),
            pin.provider.egress_host(),
        );
        if let (Some(max), true) = (pin.images_max, row.attachment) {
            entry = entry.with_images(ImageSupport::new(max));
        }
        let rate = |nanos: Option<i64>| {
            nanos
                .and_then(|nanos| u64::try_from(nanos).ok())
                .map(cortexkit_model_batch::estimate::nanos_to_usd)
        };
        let (input, output) = (rate(row.input_rate_nanos), rate(row.output_rate_nanos));
        if input.is_some() || output.is_some() {
            let mut price = Price::new();
            if let Some(input) = input {
                price = price.with_input_per_mtok_usd(input);
            }
            if let Some(output) = output {
                price = price.with_output_per_mtok_usd(output);
            }
            entry = entry.with_price(price);
        }
        entry
    }

    /// The `classifier/v1` `role.describe` answer: every pinned model the live catalog lists
    /// as a classifier, with the catalog's facts and this build's limits.
    pub fn describe(&self) -> RoleDescribe {
        let models = self
            .pins
            .iter()
            .filter_map(|pin| {
                let row = self.catalog.row(&pin.catalog_id)?;
                classify::is_classifier_row(&row).then(|| self.model_entry(pin, &row))
            })
            .collect();
        RoleDescribe::new(
            Limits::new(self.config.max_items, self.config.max_request_bytes),
            models,
        )
    }

    /// Answer one request on a classifier route. The reply is the `result` text, or the body
    /// of the `ERROR` frame. `role.describe` is answered at once; `classify.run` on the
    /// caller's task (see [`Self::spawn_call`]).
    pub async fn answer(
        self: Arc<Self>,
        body: &[u8],
        principal: Option<&broca_protocol::BindPrincipal>,
        send: &broca_protocol::SendScope,
    ) -> Result<Box<RawValue>, Vec<u8>> {
        #[derive(serde::Deserialize)]
        struct Envelope<'a> {
            method: String,
            #[serde(borrow, default)]
            params: Option<&'a RawValue>,
        }
        let envelope: Envelope<'_> = serde_json::from_slice(body).map_err(|e| {
            error_body(
                "invalid_request",
                &format!("request body not decodable: {e}"),
            )
        })?;
        let params = envelope.params.map_or("null", RawValue::get);
        match envelope.method.as_str() {
            OP_ROLE_DESCRIBE => {
                let empty = serde_json::from_str::<serde_json::Value>(params)
                    .ok()
                    .is_some_and(|value| {
                        value.is_null() || value.as_object().is_some_and(|map| map.is_empty())
                    });
                if !empty {
                    return Err(refusal_body(&Refusal::invalid_params(
                        "params",
                        "role.describe takes no params",
                    )));
                }
                serde_json::value::to_raw_value(&self.describe())
                    .map_err(|e| error_body(INTERNAL_ERROR, &e.to_string()))
            }
            OP_CLASSIFY_RUN => {
                let scope = caller_scope(principal, send).map_err(|e| {
                    self.metrics
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .batches_failed += 1;
                    refusal_body(&e)
                })?;
                self.spawn_call(params.to_owned(), scope).await
            }
            other => Err(error_body(
                "unknown_method",
                &format!("unknown method '{other}' on a classifier/v1 route"),
            )),
        }
    }

    /// Run one `classify.run` on a task of its own and wait for it. A panic inside answers
    /// this call `internal_error`; the batch it held is released by its hold's drop.
    pub async fn spawn_call(
        self: Arc<Self>,
        params: String,
        scope: broca_store::system_one::BatchScope,
    ) -> Result<Box<RawValue>, Vec<u8>> {
        self.spawn_call_admitted_at(params, scope, None).await
    }

    /// [`Self::spawn_call`] for a call admitted before it runs. When `admitted_at_ms` is given
    /// and the batch is new, its seven-day retention runs from that time rather than from
    /// now, so a call held for consent and executed later expires on the same day as the
    /// record of its admission. A batch already recorded keeps its first admission time.
    pub async fn spawn_call_admitted_at(
        self: Arc<Self>,
        params: String,
        scope: broca_store::system_one::BatchScope,
        admitted_at_ms: Option<i64>,
    ) -> Result<Box<RawValue>, Vec<u8>> {
        // Raised here and lowered by the task itself, so a caller that stops waiting (its
        // route closed) does not hide a call that is still running.
        let gauge = Arc::clone(&self.in_flight);
        gauge.fetch_add(1, Ordering::Relaxed);
        let lowered = GaugeGuard(gauge);
        let service = Arc::clone(&self);
        let task = tokio::spawn(async move {
            let _lowered = lowered;
            let _slot = Arc::clone(&service.call_slots).acquire_owned().await;
            classify::run(&service, &params, &scope, admitted_at_ms).await
        });
        let result = match task.await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(refusal)) => Err(refusal_body(&refusal)),
            Err(error) => {
                tracing::error!(target: "system_one", %error,
                    "a classify.run task ended abnormally; the call is answered internal_error");
                Err(refusal_body(&internal_refusal(
                    "serving this call failed inside broca; re-send the batch",
                )))
            }
        };
        let mut metrics = self.metrics.lock().unwrap_or_else(|p| p.into_inner());
        if result.is_ok() {
            metrics.batches_answered += 1;
        } else {
            metrics.batches_failed += 1;
        }
        result
    }
}

fn caller_scope(
    principal: Option<&broca_protocol::BindPrincipal>,
    send: &broca_protocol::SendScope,
) -> Result<broca_store::system_one::BatchScope, Refusal> {
    use broca_store::system_one::BatchScope;
    let stamp = match send {
        broca_protocol::SendScope::Stamped(bind) => Some(&bind.scope),
        _ => None,
    };
    if broca_core::scope_admission::is_flow_only(principal)
        && stamp.and_then(|s| s.flow_id.as_ref()).is_none()
    {
        return Err(Refusal::new(
            "flow_scope_required",
            "Basal classifier calls require a daemon-stamped flow scope",
        ));
    }
    if let Some(stamp) = stamp {
        if let Some(flow_id) = &stamp.flow_id {
            return Ok(BatchScope::Flow {
                flow_id: flow_id.clone(),
            });
        }
        if let (Some(agent_id), Some(run_id)) = (&stamp.agent_id, &stamp.run_id) {
            return Ok(BatchScope::Run {
                agent_id: agent_id.clone(),
                run_id: run_id.clone(),
            });
        }
    }
    Err(Refusal::new(
        "scope_required",
        "classifier calls require a daemon-stamped agent/run or flow scope",
    ))
}

/// Lowers the busy gauge when a call ends, however it ends.
struct GaugeGuard(Arc<AtomicUsize>);

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// A transient refusal for a failure inside broca, so the caller re-sends.
pub(crate) fn internal_refusal(message: &str) -> Refusal {
    Refusal::new(INTERNAL_ERROR, message).with_detail(RefusalDetail::new(ErrorClass::Transient))
}

pub(crate) fn refusal_body(refusal: &Refusal) -> Vec<u8> {
    serde_json::to_vec(refusal).unwrap_or_else(|_| error_body(INTERNAL_ERROR, &refusal.message))
}

fn error_body(code: &str, message: &str) -> Vec<u8> {
    serde_json::to_vec(&subc_protocol::ErrorBody::new(code, message)).unwrap_or_default()
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

#[cfg(test)]
#[path = "workers_ai_tests.rs"]
mod workers_ai_tests;

#[cfg(test)]
mod tool_admission_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicI64};

    struct Catalog {
        present: AtomicBool,
        reads: AtomicUsize,
    }
    impl BatchCatalog for Catalog {
        fn row(&self, _: &str) -> Option<ModelBatchRow> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.present.load(Ordering::SeqCst).then(|| ModelBatchRow {
                model_name: None,
                provider_name: None,
                kind: Some("decision".into()),
                context_tokens: None,
                attachment: false,
                input_rate_nanos: Some(42_000_000),
                output_rate_nanos: Some(0),
            })
        }
    }
    struct Answer;
    #[async_trait::async_trait]
    impl ClassifierProvider for Answer {
        fn egress_host(&self) -> String {
            "localhost".into()
        }
        fn credential_id(&self) -> Option<String> {
            None
        }
        async fn call(&self, _: String) -> ProviderReply {
            ProviderReply::Answered(br#"{"model":"jev-1.13.0","answers":{"q":{"type":"noul","noul":0.5}},"usage":{"input_tokens":100}}"#.to_vec())
        }
    }

    #[tokio::test]
    async fn frozen_admission_and_original_time_survive_delayed_execution() {
        let catalog = Arc::new(Catalog {
            present: AtomicBool::new(true),
            reads: AtomicUsize::new(0),
        });
        let clock = Arc::new(AtomicI64::new(1000 + 24 * 60 * 60 * 1000));
        let read_clock = clock.clone();
        let service = Arc::new(
            SystemOne::new(
                Arc::new(SystemOneStore::open_in_memory().unwrap()),
                "delayed-owner",
                catalog.clone(),
                vec![ModelPin::jev(Arc::new(Answer))],
                Config::default(),
                Arc::default(),
            )
            .with_store_role(LLM_CLASSIFY_STORE_ROLE)
            .with_clock(Arc::new(move || read_clock.load(Ordering::SeqCst))),
        );
        let params = serde_json::json!({"batch_id":"held","model":JEV_1_13_0,"questions":{"q":{"type":"noul","instructions":"Flag?"}},"items":[{"state":"invented"}],"max_cost_usd":0.01}).to_string();
        let admission = service.admit_tool(&params, None).unwrap();
        catalog.present.store(false, Ordering::SeqCst);
        let scope = broca_store::system_one::BatchScope::Run {
            agent_id: "agent".into(),
            run_id: "run".into(),
        };
        service
            .clone()
            .execute_tool(params, scope, "digest".into(), admission, Some(1000))
            .await
            .unwrap();
        let stored = service.stored_batch("held").await.unwrap().unwrap();
        assert_eq!(
            stored.admitted_at_ms, 1000,
            "execution must not reset the admission anchor"
        );
        assert_eq!(stored.args_digest, "digest");
        assert_eq!(stored.outcomes.len(), 1);
        assert_eq!(
            catalog.reads.load(Ordering::SeqCst),
            1,
            "execution must not reread the admitted catalog row"
        );
        clock.store(
            1000 + broca_store::system_one::RETENTION_MS - 1,
            Ordering::SeqCst,
        );
        assert_eq!(service.sweep().await.unwrap(), 0);
        clock.fetch_add(1, Ordering::SeqCst);
        assert_eq!(service.sweep().await.unwrap(), 1);
    }
}
