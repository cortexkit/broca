//! The `llm.complete/v1` tool: one completion for a codemode program or a flow, on a model the
//! caller never names.
//!
//! The caller states a demand (`iq`, `eq`). Routing picks a route for consumer
//! `broca.complete.<caller>.<run|flow>`, where `<caller>` is the module the daemon stamped on the
//! route's bind and the scope kind comes from its scope stamp, so no argument or prompt can
//! choose another caller's routing policy. broca checks the route's price against the caller's
//! ceiling before anything is sent, then runs the call as one run with no tools in a session of
//! its own, through the same send path a `session.send` takes: credentials, account routing,
//! at-most-once dispatch, crash recovery and attempt facts all come with it. The session is kept
//! 7 days and closes when its run ends.
//!
//! The call's admission (the frozen route and rates) and its final answer are recorded in the
//! System One batch store under [`STORE_ROLE`], keyed like a classify batch: by the caller, its
//! scope and its `call_key`. A repeat of the key answers the recorded result and is never billed
//! twice. A repeat that finds an admission without an answer (broca stopped while the call ran)
//! joins the run that admission started, which the session's own send id makes a no-op send.
//!
//! Each attempt runs in its own session, `llm-complete:<key>:<attempt>`, and every attempt's end
//! is recorded as its outcome. A transient failure is not final: a repeat of the key starts the
//! next attempt in a fresh session, rather than replaying the failure for the week the record is
//! kept or adding a second prompt to the failed attempt's context.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use broca_module::{LineageKey, SendRequest, SendResponse, ServeError};
use broca_protocol::{BindIdentity, BindPrincipal, RunId, SendScope};
use broca_store::system_one::{
    BatchClaim, BatchScope, Claimed, Outcome, StoredOutcome, SystemOneStore,
};
use broca_wire::RunResultResponse;
use cortexkit_llm_tools::complete;
use cortexkit_llm_tools::errors::{COST_UNMEASURED, COST_UNPRICEABLE, NO_ROUTE, OUTPUT_INVALID};
use cortexkit_model_batch::errors as batch;
use cortexkit_model_batch::estimate::{
    cost_nanos, nanos_to_usd, usd_to_nanos, RateNanosPerMtok, Rounding, Tokens,
};
use cortexkit_role_classifier::errors::{ErrorClass, Refusal};
use cortexkit_role_routing::price::{Price, PriceBasis};
use cortexkit_role_routing::select::{Requirements, SelectAnswer, SelectRequest};
use serde::{Deserialize, Serialize};
use serde_json::{json, value::RawValue, Value};
use tokio::sync::watch;

use super::spend::Spend;
use crate::account_routing::RoutingCallError;

/// The tool's catalog name. Codemode programs call tools as `tools.<name>`, so it is a plain
/// identifier; the contract's tag goes in `capabilities`.
pub const COMPLETE_TOOL_NAME: &str = "llm_complete";

/// Whether broca lists and answers the tool. Off until broca pins the llm-tools release whose
/// contract lets completion run on subscription accounts and names its routing consumer
/// `<serving module>.complete.<caller>.<scope kind>`, as this module does; the release pinned
/// now says per-token accounts only.
pub const SERVED: bool = false;

/// The System One store role completions are recorded under, apart from classify's batches.
pub const STORE_ROLE: &str = "llm.complete";

/// The routing consumer every caller's name hangs under.
const CONSUMER_ROOT: &str = "broca.complete";

/// The harness of a completion's session. The session name carries the call's key.
const SESSION_HARNESS: &str = "broca";

/// The send id of an attempt's one send. A repeat of the send after a restart finds the run the
/// first one started instead of starting another.
const SEND_ID: &str = "complete";

/// How long a completion's session is kept, in seconds: the same 7 days its answer is kept.
const RETENTION_SECS: u64 = 7 * 24 * 60 * 60;

/// The largest result broca answers, encoded: the 64 KiB a tool result may carry.
pub const MAX_ANSWER_BYTES: usize = 64 * 1024;

/// Tokens allowed for what broca adds to the caller's text in the request: role framing, and
/// for a JSON answer the sentence that introduces the schema. The prompt, system text and
/// schema are bounded by their byte lengths, which no tokenizer exceeds.
const OVERHEAD_TOKENS: u64 = 512;

/// How long a completion may run before broca stops it and answers `provider_error`.
const MAX_RUN: Duration = Duration::from_secs(15 * 60);

type Answer = Result<Box<RawValue>, Vec<u8>>;

/// Routing's `route.select`, which picks the completion's model.
#[async_trait]
pub trait Select: Send + Sync {
    async fn select(&self, request: &SelectRequest) -> Result<SelectAnswer, RoutingCallError>;
}

#[async_trait]
impl Select for crate::account_routing::RoutingClient {
    async fn select(&self, request: &SelectRequest) -> Result<SelectAnswer, RoutingCallError> {
        self.classifier_select(request).await
    }
}

/// Whether a call's scope may send its text to `provider`: the private-content admission classify
/// uses. A refusal body states its spend (none).
#[async_trait]
pub trait Gate: Send + Sync {
    async fn admit(&self, scope: &SendScope, provider: &str) -> Result<(), Vec<u8>>;
}

#[async_trait]
impl Gate for super::private_content::PrivateContent {
    async fn admit(&self, scope: &SendScope, provider: &str) -> Result<(), Vec<u8>> {
        super::private_content::PrivateContent::admit(self, scope, provider).await
    }
}

/// The session runtime a completion's run goes through: the same calls a `session.send`,
/// `run.result` and `run.cancel` reach.
#[async_trait]
pub trait Sessions: Send + Sync {
    async fn send(&self, request: SendRequest) -> Result<SendResponse, ServeError>;
    async fn run_result(
        &self,
        session: &LineageKey,
        run_id: RunId,
    ) -> Result<RunResultResponse, ServeError>;
    async fn cancel(&self, session: &LineageKey, run_id: RunId) -> Result<(), ServeError>;
}

#[async_trait]
impl Sessions for broca_module::LineageRegistry {
    async fn send(&self, request: SendRequest) -> Result<SendResponse, ServeError> {
        broca_module::ServeHandler::send(self, request).await
    }

    async fn run_result(
        &self,
        session: &LineageKey,
        run_id: RunId,
    ) -> Result<RunResultResponse, ServeError> {
        broca_module::ServeHandler::run_result(self, session, run_id).await
    }

    async fn cancel(&self, session: &LineageKey, run_id: RunId) -> Result<(), ServeError> {
        broca_module::ServeHandler::cancel(self, session, run_id)
            .await
            .map(|_| ())
    }
}

/// What a completion froze when it was first admitted: the route routing chose and its prices.
/// Every attempt and every repeat of the call runs and is priced by these.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Frozen {
    model: complete::Model,
    /// The select's decision, for tracing the call back to routing's record of it.
    decision_id: String,
    /// `metered` or `subscription_equivalent`, as routing priced the route.
    basis: String,
    /// Nanodollars per million tokens. Subscription-equivalent rates arrive already divided by
    /// the plan's multiplier.
    input_rate: RateNanosPerMtok,
    output_rate: RateNanosPerMtok,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cache_read_rate: Option<RateNanosPerMtok>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cache_write_rate: Option<RateNanosPerMtok>,
}

/// A call that passed admission: its checked request and where it is recorded.
struct Admitted {
    request: complete::Request,
    /// The arguments as the caller sent them, for the record's identity.
    identity: String,
    /// The JCS digest of the arguments: a repeat of the key with other arguments is refused.
    digest: String,
    /// The record key, derived from the caller, its scope and the `call_key`; random for a call
    /// without a key, which is never replayed.
    key: String,
    keyed: bool,
    scope: BatchScope,
    /// `broca.complete.<caller>.<run|flow>`.
    consumer: String,
    /// The caller's ceiling in nanodollars, rounded down so it is never raised.
    ceiling_nanos: u64,
}

pub struct Complete {
    store: Arc<SystemOneStore>,
    /// This process's name on a record it holds.
    owner: String,
    select: Arc<dyn Select>,
    sessions: Arc<dyn Sessions>,
    /// The runner module routing may answer: this broca.
    runner: String,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    /// How often a running call's run is read.
    poll: Duration,
    /// Calls running in this process, by key: a concurrent repeat of a key waits for the first.
    flights: Mutex<HashMap<String, watch::Receiver<Option<Answer>>>>,
}

impl Complete {
    pub fn new(
        store: Arc<SystemOneStore>,
        owner: impl Into<String>,
        select: Arc<dyn Select>,
        sessions: Arc<dyn Sessions>,
        runner: impl Into<String>,
    ) -> Self {
        Self {
            store,
            owner: owner.into(),
            select,
            sessions,
            runner: runner.into(),
            clock: Arc::new(crate::system_one::now_ms),
            poll: Duration::from_millis(250),
            flights: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_clock(mut self, clock: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        self.clock = clock;
        self
    }

    pub fn with_poll(mut self, poll: Duration) -> Self {
        self.poll = poll;
        self
    }

    /// The tool's input schema, as the catalog lists it and a pin's digest covers.
    pub fn input_schema() -> Value {
        complete::input_schema()
    }

    /// Answer one `llm_complete` call: the result object, or a refusal body stating its spend.
    /// `gate` decides whether the scope may reach the provider routing chose. A scope that read
    /// the person's private content is refused here; completion files no consent card yet.
    pub async fn answer(
        self: &Arc<Self>,
        principal: Option<&BindPrincipal>,
        scope: &SendScope,
        arguments: &RawValue,
        call_key: Option<&str>,
        gate: &dyn Gate,
    ) -> Answer {
        let call = admit(principal, scope, arguments, call_key).map_err(unspent)?;
        let flight = {
            let mut flights = self.flights.lock().unwrap_or_else(|p| p.into_inner());
            match flights.get(&call.key) {
                Some(running) => Err(running.clone()),
                None => {
                    let (sender, receiver) = watch::channel(None);
                    flights.insert(call.key.clone(), receiver);
                    Ok(sender)
                }
            }
        };
        let sender = match flight {
            Ok(sender) => sender,
            Err(running) => return await_flight(running).await,
        };
        let answer = self.run(&call, principal, scope, gate).await;
        self.flights
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&call.key);
        let _ = sender.send(Some(answer.clone()));
        answer
    }

    async fn run(
        &self,
        call: &Admitted,
        principal: Option<&BindPrincipal>,
        scope: &SendScope,
        gate: &dyn Gate,
    ) -> Answer {
        let now = (self.clock)();
        // The record is read before routing is asked anything: a recorded answer is final, and
        // a call admitted before keeps the route and prices it was admitted with.
        let stored = if call.keyed {
            let (store, key) = (Arc::clone(&self.store), call.key.clone());
            tokio::task::spawn_blocking(move || store.lookup_live(STORE_ROLE, &key, now))
                .await
                .map_err(|_| unspent(internal("the completion record is unavailable")))?
                .map_err(|_| unspent(internal("the completion record is unavailable")))?
        } else {
            None
        };
        let (frozen, attempt) = match &stored {
            Some(stored) => {
                if stored.args_digest != call.digest || stored.scope.as_ref() != Some(&call.scope) {
                    return Err(unspent(key_reused()));
                }
                if let Some(answer) = replay(&stored.outcomes) {
                    return answer;
                }
                let frozen: Frozen = serde_json::from_str(&stored.frozen_row)
                    .map_err(|_| unspent(internal("the completion record is unreadable")))?;
                (frozen, stored.outcomes.len() as u64)
            }
            None => (self.price(call).await.map_err(unspent)?, 0),
        };
        // Every call that may send text is gated, a later attempt too: a scope may have read
        // the person's private content since the call was first admitted. A replay is not.
        gate.admit(scope, &frozen.model.provider).await?;
        let frozen_row = serde_json::to_string(&frozen).expect("a frozen route encodes");
        let model_key = route_key(&frozen.model);
        let claim = BatchClaim {
            role: STORE_ROLE,
            batch_id: &call.key,
            model: &model_key,
            identity: &call.identity,
            body_digest: &call.digest,
            frozen_row: &frozen_row,
            scope: &call.scope,
            ceiling_nanos: Some(call.ceiling_nanos),
            owner: &self.owner,
            now_ms: now,
        };
        match self.store.claim(&claim) {
            Ok(Claimed::Fresh | Claimed::Resumed(_)) => {}
            Ok(Claimed::InProgress) => {
                return Err(unspent(refused(
                    batch::PROVIDER_ERROR,
                    "this call is still running; repeat it with the same call_key later",
                )))
            }
            Ok(Claimed::Reuse { .. } | Claimed::ScopeMismatch | Claimed::Expired) => {
                return Err(unspent(key_reused()))
            }
            Err(_) => return Err(unspent(internal("the completion record is unavailable"))),
        }
        let hold = Hold {
            store: &self.store,
            key: &call.key,
            owner: &self.owner,
        };
        let (answer, spend_nanos, tokens) =
            self.execute(call, &frozen, attempt, principal, scope).await;
        // Every attempt's end is recorded, a transient failure too: the next repeat must start
        // the next attempt, since this attempt's session already holds its ended run.
        let outcome = StoredOutcome {
            index: attempt,
            outcome: match &answer {
                Ok(result) => Outcome::Answered(result.get().to_owned()),
                Err(body) => Outcome::Failed(String::from_utf8_lossy(body).into_owned()),
            },
            input_tokens: tokens.0,
            output_tokens: tokens.1,
        };
        if let Err(error) = self.store.record(
            STORE_ROLE,
            &call.key,
            &self.owner,
            &outcome,
            spend_nanos,
            (self.clock)(),
        ) {
            tracing::warn!(target: "llm_tools", %error,
                "could not record a completion's outcome; a repeat of its key joins its run");
        }
        drop(hold);
        answer
    }

    /// Ask routing for a route meeting the demand, and check its price against the ceiling.
    async fn price(&self, call: &Admitted) -> Result<Frozen, Refusal> {
        let request = &call.request;
        let input_tokens = input_bound(request);
        let mut requirements = Requirements::iq(demand(request.demand.iq));
        requirements.eq = request.demand.eq.map(demand);
        requirements.max_output_tokens = Some(request.max_output_tokens);
        requirements.input_tokens = Some(input_tokens);
        requirements.max_cost_usd = Some(request.max_cost_usd.clone());
        requirements.structured_output = request.output_schema.is_some().then_some(true);
        let query = SelectRequest::new(call.consumer.clone(), requirements)
            .with_runners(vec![self.runner.clone()]);
        let answer = self.select.select(&query).await.map_err(select_refusal)?;
        let route = &answer.runner.model;
        let model = complete::Model {
            provider: route.provider.clone(),
            model: route.model.clone(),
            variant: route.variant.clone(),
        };
        let Some(price) = &answer.price else {
            return Err(refused(
                COST_UNPRICEABLE,
                format!(
                    "routing chose {}, which it could not price",
                    route_key(&model)
                ),
            ));
        };
        let frozen = freeze(model, answer.decision_id.clone(), price)?;
        let estimate = cost_nanos(input_tokens, frozen.input_rate)
            .ok()
            .zip(cost_nanos(request.max_output_tokens, frozen.output_rate).ok())
            .and_then(|(input, output)| input.checked_add(output))
            .unwrap_or(u64::MAX);
        if estimate > call.ceiling_nanos {
            let mut refusal = refused(
                batch::COST_EXCEEDED,
                format!(
                    "{} may cost up to ${} for this call, over max_cost_usd",
                    route_key(&frozen.model),
                    nanos_to_usd(estimate)
                ),
            );
            refusal.detail.estimate_usd = Some(nanos_to_usd(estimate));
            return Err(refusal);
        }
        Ok(frozen)
    }

    /// Run one attempt and turn its end into the answer, its spend in nanodollars and its
    /// measured input and output tokens.
    async fn execute(
        &self,
        call: &Admitted,
        frozen: &Frozen,
        attempt: u64,
        principal: Option<&BindPrincipal>,
        scope: &SendScope,
    ) -> (Answer, u64, (Option<u64>, Option<u64>)) {
        let session = session_of(&call.key, attempt);
        let send = match send_request(&session, call, frozen, principal, scope) {
            Ok(send) => send,
            Err(refusal) => return (Err(unspent(refusal)), 0, (None, None)),
        };
        let run_id = send.run_id.clone();
        match self.sessions.send(send).await {
            Ok(SendResponse::Pending { .. }) => {
                // A completion's session only ever holds its own run, so nothing can be ahead of
                // it. Nothing was sent to a provider.
                let _ = self.sessions.cancel(&session, run_id).await;
                return (
                    Err(unspent(internal("the completion's session was busy"))),
                    0,
                    (None, None),
                );
            }
            Ok(_) => {}
            Err(error) => return (Err(unspent(send_refusal(&error))), 0, (None, None)),
        }
        match self.wait(&session, &run_id).await {
            Ok((result, stop)) => finish(&call.request, frozen, &result, stop),
            // The run may have reached the provider, and nothing here measured it.
            Err(refusal) => (
                Err(serialize(Spend::Unknown.mark(refusal))),
                0,
                (None, None),
            ),
        }
    }

    /// Wait for the run to end, and say whether broca stopped it. A run that pauses is stopped,
    /// since a tool call can't wait on a person signing in or an account limit resetting, and so
    /// is one that runs past [`MAX_RUN`].
    async fn wait(
        &self,
        session: &LineageKey,
        run_id: &RunId,
    ) -> Result<(RunResultResponse, Option<Stop>), Refusal> {
        let deadline = tokio::time::Instant::now() + MAX_RUN;
        let mut stop = None;
        loop {
            let result = self
                .sessions
                .run_result(session, run_id.clone())
                .await
                .map_err(|error| {
                    refused(
                        batch::PROVIDER_ERROR,
                        format!("the completion's run could not be read: {error}"),
                    )
                })?;
            match (result.state.as_str(), &stop) {
                ("active" | "paused", Some(_)) => {}
                ("active", None) if tokio::time::Instant::now() < deadline => {}
                ("active", None) => {
                    stop = Some(Stop::TimedOut);
                    let _ = self.sessions.cancel(session, run_id.clone()).await;
                }
                ("paused", None) => {
                    stop = Some(Stop::Paused(result.reason.clone().unwrap_or_default()));
                    let _ = self.sessions.cancel(session, run_id.clone()).await;
                }
                _ => return Ok((result, stop)),
            }
            tokio::time::sleep(self.poll).await;
        }
    }
}

/// Why broca stopped a completion's run before it ended on its own.
#[derive(Debug, PartialEq)]
enum Stop {
    /// The run paused, for this reason.
    Paused(String),
    /// The run ran past [`MAX_RUN`].
    TimedOut,
}

/// Releases the record's hold when the call ends, whichever way it ends.
struct Hold<'a> {
    store: &'a SystemOneStore,
    key: &'a str,
    owner: &'a str,
}

impl Drop for Hold<'_> {
    fn drop(&mut self) {
        if let Err(error) = self.store.release(STORE_ROLE, self.key, self.owner) {
            tracing::warn!(target: "llm_tools", %error, "could not release a completion's record");
        }
    }
}

/// Scope, argument and schema admission, before anything is read or written.
fn admit(
    principal: Option<&BindPrincipal>,
    scope: &SendScope,
    arguments: &RawValue,
    call_key: Option<&str>,
) -> Result<Admitted, Refusal> {
    let (batch_scope, stamp) = super::admission::admit_scope(principal, scope)?;
    let value: Value = serde_json::from_str(arguments.get())
        .map_err(|error| Refusal::invalid_params("arguments", error.to_string()))?;
    let object = value
        .as_object()
        .ok_or_else(|| Refusal::invalid_params("arguments", "arguments must be an object"))?;
    const MEMBERS: &[&str] = &[
        "demand",
        "prompt",
        "system",
        "output_schema",
        "max_output_tokens",
        "max_cost_usd",
    ];
    if let Some(field) = object
        .keys()
        .find(|field| !MEMBERS.contains(&field.as_str()))
    {
        return Err(Refusal::invalid_params(
            field,
            format!("unknown argument {field}"),
        ));
    }
    for field in ["demand", "prompt", "max_output_tokens", "max_cost_usd"] {
        if !object.contains_key(field) {
            return Err(Refusal::invalid_params(
                field,
                format!("{field} is required"),
            ));
        }
    }
    if !object.get("max_cost_usd").is_some_and(Value::is_number) {
        return Err(Refusal::invalid_params(
            "max_cost_usd",
            "max_cost_usd is a required non-negative number",
        ));
    }
    let request: complete::Request = serde_json::from_value(value.clone())
        .map_err(|error| Refusal::invalid_params("arguments", error.to_string()))?;
    complete::validate_request(&request).map_err(validation_refusal)?;
    if let Some(schema) = &request.output_schema {
        let structured = broca_protocol::structured_output::StructuredOutput::new(
            broca_protocol::structured_output::Mode::FinalText,
            Value::Object(schema.clone()),
        );
        broca_protocol::structured_output::check_compiles(&structured).map_err(|_| {
            Refusal::invalid_params(
                "output_schema",
                "output_schema must be a draft 2020-12 JSON Schema broca can compile, with no \
                 reference outside itself",
            )
        })?;
    }
    let ceiling_nanos = usd_to_nanos(&request.max_cost_usd, Rounding::Down)
        .ok_or_else(|| Refusal::invalid_params("max_cost_usd", "max_cost_usd is out of range"))?;
    let (key, keyed) = super::admission::call_id(principal, stamp, call_key)?;
    let caller = match principal {
        Some(BindPrincipal::Reserved { module_id }) => module_id.as_str(),
        _ => "direct",
    };
    let kind = match batch_scope {
        BatchScope::Flow { .. } => "flow",
        BatchScope::Run { .. } => "run",
    };
    Ok(Admitted {
        identity: serde_json::to_string(&value).expect("JSON values encode"),
        digest: super::admission::hash(&value),
        request,
        key,
        keyed,
        scope: batch_scope,
        consumer: format!("{CONSUMER_ROOT}.{caller}.{kind}"),
        ceiling_nanos,
    })
}

fn validation_refusal(error: cortexkit_llm_tools::ValidationError) -> Refusal {
    use cortexkit_llm_tools::ValidationError as E;
    match error {
        E::DemandOutOfRange { field } => {
            Refusal::invalid_params(field, "a demand is an integer from 0 to 100")
        }
        E::EmptyPrompt => Refusal::invalid_params("prompt", "prompt must not be empty"),
        E::ZeroMaxOutputTokens => {
            Refusal::invalid_params("max_output_tokens", "max_output_tokens must be at least 1")
        }
        E::OutputSchemaTooLarge { size, limit } => Refusal::invalid_params(
            "output_schema",
            format!("output_schema is {size} bytes, over the {limit}-byte limit"),
        ),
        E::NegativeCost { field } => Refusal::invalid_params(field, "cost must not be negative"),
        E::Role(refusal) => refusal,
        other => Refusal::invalid_params("arguments", other.to_string()),
    }
}

/// A demand score as routing takes it; admission has checked it is in 0..=100.
fn demand(score: i64) -> u8 {
    u8::try_from(score.clamp(0, 100)).expect("a clamped demand fits")
}

/// An upper bound on the call's input tokens: the byte lengths of everything the caller sent
/// that reaches the model, plus broca's own framing.
fn input_bound(request: &complete::Request) -> u64 {
    let schema = request.output_schema.as_ref().map_or(0, |schema| {
        Tokens::bound_of_json(schema).map_or(u64::MAX, Tokens::count)
    });
    Tokens::bound_of_text(&request.prompt)
        .count()
        .saturating_add(
            request
                .system
                .as_deref()
                .map_or(0, |system| Tokens::bound_of_text(system).count()),
        )
        .saturating_add(schema)
        .saturating_add(OVERHEAD_TOKENS)
}

/// The select answer's price as integer rates, rounded up so a cost is never lowered.
fn freeze(model: complete::Model, decision_id: String, price: &Price) -> Result<Frozen, Refusal> {
    let unpriceable = || {
        refused(
            COST_UNPRICEABLE,
            format!("routing's price for {} is not usable", route_key(&model)),
        )
    };
    let rate = |usd: &serde_json::Number| {
        usd_to_nanos(usd, Rounding::Up).and_then(|nanos| RateNanosPerMtok::try_from(nanos).ok())
    };
    let rates = &price.rates_per_mtok;
    let basis = match price.basis {
        PriceBasis::Metered => "metered",
        PriceBasis::SubscriptionEquivalent => "subscription_equivalent",
        #[allow(unreachable_patterns)]
        _ => return Err(unpriceable()),
    };
    Ok(Frozen {
        input_rate: rate(&rates.input).ok_or_else(unpriceable)?,
        output_rate: rate(&rates.output).ok_or_else(unpriceable)?,
        cache_read_rate: rates.cache_read.as_ref().and_then(rate),
        cache_write_rate: rates.cache_write.as_ref().and_then(rate),
        basis: basis.to_owned(),
        decision_id,
        model,
    })
}

fn route_key(model: &complete::Model) -> String {
    match &model.variant {
        Some(variant) => format!("{}/{}:{variant}", model.provider, model.model),
        None => format!("{}/{}", model.provider, model.model),
    }
}

/// The session attempt `attempt` of call `key` runs in. It is in broca's folderless namespace:
/// a completion reads no files, and its spend is attributed by the caller's scope.
fn session_of(key: &str, attempt: u64) -> LineageKey {
    BindIdentity::new(
        broca_protocol::session::UNSCOPED_SESSION_ROOT,
        SESSION_HARNESS,
        format!("llm-complete:{key}:{attempt}"),
    )
}

/// The attempt's one send: the caller's text on the frozen route, no tools, the output cap,
/// no prompt cache, a JSON answer when the call has a schema, and the caller's principal and
/// scope, so the run's spend is the caller's.
fn send_request(
    session: &LineageKey,
    call: &Admitted,
    frozen: &Frozen,
    principal: Option<&BindPrincipal>,
    scope: &SendScope,
) -> Result<SendRequest, Refusal> {
    use crate::wire::{SendParams, SendParamsExt};
    let request = &call.request;
    let mut params = json!({
        "model": frozen.model,
        "prompt": request.prompt,
        "send_id": SEND_ID,
        "title": "llm.complete",
        "tools": [],
        "generation": {"max_output_tokens": request.max_output_tokens},
        "cache": {"ttl": "none"},
        "retention": RETENTION_SECS,
        "one_shot": true,
    });
    if let Some(system) = &request.system {
        params["system"] = json!(system);
    }
    if let Some(schema) = &request.output_schema {
        params["structured_output"] = json!({"mode": "final_text", "schema": schema});
    }
    let params: SendParams = serde_json::from_value(params)
        .map_err(|error| internal(&format!("the completion's send did not encode: {error}")))?;
    let run_id = crate::serve::run_id_for_send(session, Some(SEND_ID));
    let mut send = params
        .into_send_request(session.clone(), run_id)
        .map_err(|error| internal(&format!("the completion's send was refused: {error}")))?;
    send.request.principal = principal.cloned();
    send.request.scope = scope.clone();
    Ok(send)
}

/// The answer, spend and measured tokens of an attempt whose run ended.
fn finish(
    request: &complete::Request,
    frozen: &Frozen,
    result: &RunResultResponse,
    stop: Option<Stop>,
) -> (Answer, u64, (Option<u64>, Option<u64>)) {
    let usage = &result.usage;
    let tokens = (usage.input_tokens, usage.output_tokens);
    let cost = cost_of(usage, frozen);
    let spent = cost.unwrap_or(0);
    if stop.is_some() || result.state != "completed" {
        let (refusal, unbilled) = match stop {
            Some(Stop::Paused(reason)) => pause_refusal(&reason),
            Some(Stop::TimedOut) => (
                refused(
                    batch::PROVIDER_ERROR,
                    "the completion ran longer than broca allows and was stopped",
                ),
                false,
            ),
            None => run_refusal(result),
        };
        // A provider refusal it doesn't bill spent nothing, unless a model step was measured
        // before it; anything else measured is charged as measured, and the rest is unknown.
        let spend = match cost {
            Some(nanos) if usage.steps > 0 => Spend::Measured(usd(nanos)),
            _ if unbilled && usage.steps == 0 => Spend::Nothing,
            _ => Spend::Unknown,
        };
        return (Err(serialize(spend.mark(refusal))), spent, tokens);
    }
    let spend = match cost {
        Some(nanos) => Spend::Measured(usd(nanos)),
        None => Spend::Unknown,
    };
    let (text, json) = match &request.output_schema {
        None => (
            Some(
                result
                    .final_message
                    .as_ref()
                    .map(|message| message.text.clone())
                    .unwrap_or_default(),
            ),
            None,
        ),
        Some(_) => {
            let structured = result.structured_output.as_ref();
            match structured
                .filter(|so| so.validation == broca_protocol::structured_output::validation::PASSED)
            {
                Some(so) => (None, Some(so.value.clone().unwrap_or(Value::Null))),
                None => {
                    let first = structured
                        .and_then(|so| so.errors.as_ref())
                        .and_then(|errors| errors.first())
                        .map_or_else(|| "it gave no JSON value".to_owned(), Clone::clone);
                    let refusal = refused(
                        OUTPUT_INVALID,
                        format!("the model's answer does not match output_schema: {first}"),
                    );
                    return (Err(serialize(spend.mark(refusal))), spent, tokens);
                }
            }
        }
    };
    let Some(nanos) = cost else {
        // The provider answered, but a count the price needs is missing: the answer is withheld
        // rather than returned with a guessed cost.
        let refusal = refused(
            COST_UNMEASURED,
            "the provider did not report the token counts this call is priced by",
        );
        return (Err(serialize(refusal)), 0, tokens);
    };
    let model = usage
        .by_model
        .last()
        .map(|used| complete::Model {
            provider: used.provider.clone(),
            model: used.model.clone(),
            variant: used.variant.clone(),
        })
        .unwrap_or_else(|| frozen.model.clone());
    let answer = complete::Result {
        text,
        json,
        model,
        usage: tool_usage(usage),
        cost_usd: nanos_to_usd(nanos),
    };
    if let Err(error) = complete::validate_result_for_request(request, &answer) {
        let refusal = internal(&format!("the completion's result is malformed: {error}"));
        return (
            Err(serialize(Spend::Measured(usd(nanos)).mark(refusal))),
            nanos,
            tokens,
        );
    }
    let encoded = serde_json::to_string(&answer).expect("a result encodes");
    if encoded.len() > MAX_ANSWER_BYTES {
        let refusal = refused(
            batch::ANSWER_TOO_LARGE,
            format!(
                "the answer is {} bytes, over broca's {MAX_ANSWER_BYTES}-byte limit",
                encoded.len()
            ),
        );
        return (
            Err(serialize(Spend::Measured(usd(nanos)).mark(refusal))),
            nanos,
            tokens,
        );
    }
    (
        Ok(RawValue::from_string(encoded).expect("an encoded result is JSON")),
        nanos,
        tokens,
    )
}

/// The run's cost at the frozen rates, or `None` when a count the price needs is missing. A
/// cache count the provider didn't report contributes nothing: such a provider counts the whole
/// input as input. A cache count it did report needs that rate.
fn cost_of(usage: &broca_protocol::usage::RunUsage, frozen: &Frozen) -> Option<u64> {
    if usage.steps == 0 || usage.unreported_steps > 0 {
        return None;
    }
    let part = |tokens: Option<u64>, rate: Option<RateNanosPerMtok>| match tokens {
        None | Some(0) => Some(0),
        Some(tokens) => cost_nanos(tokens, rate?).ok(),
    };
    [
        cost_nanos(usage.input_tokens?, frozen.input_rate).ok(),
        cost_nanos(usage.output_tokens?, frozen.output_rate).ok(),
        part(usage.cached_input_tokens, frozen.cache_read_rate),
        part(usage.cache_write_tokens, frozen.cache_write_rate),
    ]
    .into_iter()
    .try_fold(0u64, |total, part| total.checked_add(part?))
}

fn tool_usage(usage: &broca_protocol::usage::RunUsage) -> cortexkit_llm_tools::Usage {
    cortexkit_llm_tools::Usage {
        input_tokens: usage.input_tokens,
        cached_input_tokens: usage.cached_input_tokens,
        cache_write_tokens: usage.cache_write_tokens,
        output_tokens: usage.output_tokens,
        // A breakdown of the output count only; a provider that reports it otherwise is left out.
        reasoning_tokens: usage.reasoning_tokens.filter(|reasoning| {
            usage
                .output_tokens
                .is_some_and(|output| reasoning <= &output)
        }),
    }
}

fn usd(nanos: u64) -> f64 {
    nanos_to_usd(nanos).as_f64().unwrap_or(f64::INFINITY)
}

/// The refusal for a run that ended without completing, and whether its cause is one a
/// provider doesn't bill.
fn run_refusal(result: &RunResultResponse) -> (Refusal, bool) {
    match result.state.as_str() {
        "context_ceiling" => (too_long(), true),
        "error" => match &result.error {
            Some(error) => provider_refusal(error),
            None => (
                refused(
                    batch::PROVIDER_ERROR,
                    "the completion's run ended in an error",
                ),
                false,
            ),
        },
        state => (
            refused(
                batch::PROVIDER_ERROR,
                format!("the completion's run ended {state}"),
            ),
            false,
        ),
    }
}

fn too_long() -> Refusal {
    Refusal::invalid_params(
        "prompt",
        "the prompt and system text don't fit the chosen model's context",
    )
}

/// The completion code for a provider error, by its code and class, and whether the provider
/// refused the request without billing it: a limit, a sign-in, an account or an oversized or
/// malformed request answered before any model work.
fn provider_refusal(error: &broca_protocol::caller::SharedProviderError) -> (Refusal, bool) {
    use cortexkit_role_llm_runner::errors::provider_codes as codes;
    let inner = error.as_inner();
    let message = inner.message.clone();
    let (code, unbilled) = match inner.provider_code.as_deref().unwrap_or_default() {
        codes::ACCOUNT_LIMIT_EXHAUSTED | codes::INSUFFICIENT_QUOTA => {
            (batch::ACCOUNT_LIMIT_EXHAUSTED, true)
        }
        codes::BILLING_EXHAUSTED => (batch::ACCOUNT_WALLED, true),
        codes::ACCESS_TERMINATED_ERROR => (batch::ACCOUNT_NOT_ENTITLED, true),
        codes::CONCURRENCY_FULL => (batch::RATE_LIMITED, true),
        codes::CONTEXT_OVERFLOW => return (too_long(), true),
        codes::OUTPUT_CEILING => (batch::ANSWER_TOO_LARGE, false),
        codes::CYBER_POLICY
        | codes::BIO_POLICY
        | codes::MISALIGNMENT_POLICY_VIOLATION
        | codes::CONTENT_FILTER
        | codes::HOST_POLICY_REFUSED => (batch::PROVIDER_REFUSED, false),
        _ => match error.class_wire() {
            "auth_required" => (batch::AUTH_FAILED, true),
            "context_overflow" => return (too_long(), true),
            "permanent" => (batch::INVALID_ITEM, true),
            _ => (batch::PROVIDER_ERROR, false),
        },
    };
    (refused(code, message), unbilled)
}

/// The refusal for a run broca stopped because it paused, and whether its cause is unbilled.
fn pause_refusal(reason: &str) -> (Refusal, bool) {
    match reason {
        "auth_required" => (
            refused(
                batch::AUTH_FAILED,
                "the chosen account needs signing in again",
            ),
            true,
        ),
        "account_limit" => (
            refused(
                batch::ACCOUNT_LIMIT_EXHAUSTED,
                "the chosen account is at its usage limit until it resets",
            ),
            true,
        ),
        other => (
            refused(
                batch::PROVIDER_ERROR,
                format!("the completion's run paused ({other}) and was stopped"),
            ),
            false,
        ),
    }
}

/// A send the session runtime refused wrote nothing and sent nothing.
fn send_refusal(error: &ServeError) -> Refusal {
    refused(
        batch::PROVIDER_ERROR,
        format!("the completion's run could not start: {error}"),
    )
}

fn select_refusal(error: RoutingCallError) -> Refusal {
    match error {
        RoutingCallError::Refused { code, message, .. } if code == "no_route" => {
            refused(NO_ROUTE, message)
        }
        RoutingCallError::Refused { code, message, .. } if code == "account_pool_exhausted" => {
            refused(batch::ACCOUNT_LIMIT_EXHAUSTED, message)
        }
        RoutingCallError::Refused { code, message, .. } => refused(
            batch::PROVIDER_ERROR,
            format!("routing refused the selection ({code}): {message}"),
        ),
        RoutingCallError::Unavailable(message) | RoutingCallError::Invalid(message) => refused(
            batch::PROVIDER_ERROR,
            format!("routing did not choose a model: {message}"),
        ),
    }
}

/// A refusal in the completion vocabulary, with its published class.
fn refused(code: &str, message: impl Into<String>) -> Refusal {
    cortexkit_llm_tools::errors::refusal(code, message)
        .unwrap_or_else(|| panic!("{code} is a completion code"))
}

fn internal(message: &str) -> Refusal {
    crate::system_one::internal_refusal(message)
}

fn key_reused() -> Refusal {
    Refusal::invalid_params(
        "call_key",
        "call_key names a call made with other arguments or in another scope",
    )
}

fn serialize(refusal: Refusal) -> Vec<u8> {
    serde_json::to_vec(&refusal).expect("a refusal encodes")
}

fn unspent(refusal: Refusal) -> Vec<u8> {
    serialize(Spend::Nothing.mark(refusal))
}

/// The recorded answer, if the record holds a final one: an answer, or a refusal that is
/// permanent or spent something. A transient attempt's failure is not final.
fn replay(outcomes: &[StoredOutcome]) -> Option<Answer> {
    let last = outcomes.iter().max_by_key(|outcome| outcome.index)?;
    match &last.outcome {
        Outcome::Answered(text) => Some(Ok(RawValue::from_string(text.clone()).ok()?)),
        Outcome::Failed(body) => {
            let refusal: Refusal = serde_json::from_str(body).ok()?;
            (refusal.detail.class != ErrorClass::Transient).then(|| Err(body.clone().into_bytes()))
        }
    }
}

async fn await_flight(mut receiver: watch::Receiver<Option<Answer>>) -> Answer {
    loop {
        if let Some(answer) = receiver.borrow().clone() {
            return answer;
        }
        if receiver.changed().await.is_err() {
            return receiver.borrow().clone().unwrap_or_else(|| {
                Err(serialize(Spend::Unknown.mark(internal(
                    "the call this repeat joined stopped without an answer",
                ))))
            });
        }
    }
}

#[cfg(test)]
#[path = "complete_tests.rs"]
mod tests;
