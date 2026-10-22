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

use std::collections::{HashMap, HashSet};
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

/// The store's role key for `classifier/v1` batches.
pub(crate) const STORE_ROLE: &str = "classifier";

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

impl ModelPin {
    /// TypeSafe's Jev 1.13.0, which takes `jev-latest`, text only. Its question limit is the
    /// role's own question-id space at the clef bound, since TypeSafe states none.
    pub fn jev(provider: Arc<dyn ClassifierProvider>) -> Self {
        Self {
            catalog_id: JEV_1_13_0.to_owned(),
            provider_model: cortexkit_role_classifier::provider::models::JEV_LATEST.to_owned(),
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
    /// This process's name on a batch it holds, so a hold left by another process is told
    /// apart and a write after a lost hold is refused.
    owner: String,
    catalog: Arc<dyn BatchCatalog>,
    pins: Vec<ModelPin>,
    config: Config,
    /// Batch ids a call in this process holds now: the in-process half of single flight.
    claims: Mutex<HashSet<String>>,
    call_slots: Arc<Semaphore>,
    model_slots: Mutex<HashMap<String, Arc<Semaphore>>>,
    in_flight: Arc<AtomicUsize>,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
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
            owner: owner.into(),
            catalog,
            pins,
            call_slots: Arc::new(Semaphore::new(config.process_calls.max(1))),
            config,
            claims: Mutex::new(HashSet::new()),
            model_slots: Mutex::new(HashMap::new()),
            in_flight,
            clock: Arc::new(now_ms),
        }
    }

    /// Replace the clock; tests drive retention with it.
    pub fn with_clock(mut self, clock: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        self.clock = clock;
        self
    }

    /// Release every batch a previous process left running. Call once, before serving.
    pub fn release_abandoned(&self) -> Result<usize, String> {
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
    pub async fn answer(self: Arc<Self>, body: &[u8]) -> Result<Box<RawValue>, Vec<u8>> {
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
            OP_CLASSIFY_RUN => self.spawn_call(params.to_owned()).await,
            other => Err(error_body(
                "unknown_method",
                &format!("unknown method '{other}' on a classifier/v1 route"),
            )),
        }
    }

    /// Run one `classify.run` on a task of its own and wait for it. A panic inside answers
    /// this call `internal_error`; the batch it held is released by its hold's drop.
    pub async fn spawn_call(self: Arc<Self>, params: String) -> Result<Box<RawValue>, Vec<u8>> {
        // Raised here and lowered by the task itself, so a caller that stops waiting (its
        // route closed) does not hide a call that is still running.
        let gauge = Arc::clone(&self.in_flight);
        gauge.fetch_add(1, Ordering::Relaxed);
        let lowered = GaugeGuard(gauge);
        let service = Arc::clone(&self);
        let task = tokio::spawn(async move {
            let _lowered = lowered;
            let _slot = Arc::clone(&service.call_slots).acquire_owned().await;
            classify::run(&service, &params).await
        });
        match task.await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(refusal)) => Err(refusal_body(&refusal)),
            Err(error) => {
                tracing::error!(target: "system_one", %error,
                    "a classify.run task ended abnormally; the call is answered internal_error");
                Err(refusal_body(&internal_refusal(
                    "serving this call failed inside broca; re-send the batch",
                )))
            }
        }
    }
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
