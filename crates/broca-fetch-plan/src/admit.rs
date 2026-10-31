//! Admission: fetch every provider a checked plan names in parallel under one
//! deadline, check what came back against the plan, and build the manifest the session's
//! `RunStarted` freezes.
//!
//! Nothing here writes. A refusal is returned before the caller writes anything, and an
//! admission's manifest is everything later episodes and every resume render from, so none of
//! them fetches again.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use broca_protocol::request::{ExecutionMode, Tool};
use broca_wal::{
    FetchOutcome, FrozenDeclaration, FrozenProviderDeclarations, FrozenResultFormat, OpaqueJson,
    PlanManifest, ProviderFetch,
};
use cortexkit_role_llm_runner::baseline::{AbsentItem, BaselineItem};
use cortexkit_role_step_transform_provider::subscription::{
    check_item_with_optional_preset, Declaration, DeclareRequest, ItemAdmissionRefusal,
};
use cortexkit_role_tool_provider::catalog::check_reply;
use cortexkit_role_tool_provider::catalog::{CatalogAnswer, CatalogRequest, SystemTextItem};
use serde_json::{json, Value};
use tokio::time::Instant;

use crate::plan::{sha256_hex, Item, Plan};
use crate::refusal::{Refusal, Unanswered};

#[derive(Debug, Clone)]
struct FetchMiss {
    unanswered: Unanswered,
    retryable: Option<bool>,
    detail: Option<FetchMissDetail>,
}

#[derive(Debug, Clone)]
enum FetchMissDetail {
    InvalidAnswer {
        serde_error_kind: Option<&'static str>,
    },
    ProviderRefusal {
        message: Option<String>,
        message_truncated: bool,
        field: Option<String>,
        field_truncated: bool,
    },
    ItemDigestMismatch {
        item_digest: String,
        computed_sha256: String,
        text_present: bool,
        text_bytes: usize,
    },
}

impl FetchMiss {
    fn from_error(error: FetchError) -> Self {
        let retryable = match &error {
            FetchError::Refused { retryable, .. }
            | FetchError::ProviderRefused { retryable, .. } => Some(*retryable),
            FetchError::UnknownModule | FetchError::Lost(_) => None,
        };
        let detail = match &error {
            FetchError::ProviderRefused {
                message,
                message_truncated,
                detail,
                ..
            } => {
                let (field, field_truncated) = provider_field(detail.as_ref());
                Some(FetchMissDetail::ProviderRefusal {
                    message: Some(message.clone()),
                    message_truncated: *message_truncated,
                    field,
                    field_truncated,
                })
            }
            FetchError::Refused { detail, .. } => {
                let (field, field_truncated) = provider_field(detail.as_ref());
                field.map(|field| FetchMissDetail::ProviderRefusal {
                    message: None,
                    message_truncated: false,
                    field: Some(field),
                    field_truncated,
                })
            }
            FetchError::UnknownModule | FetchError::Lost(_) => None,
        };
        Self {
            unanswered: unanswered(error),
            retryable,
            detail,
        }
    }

    fn provider_detail(&self, op: &str) -> Option<Value> {
        // Provider detail can echo catalog requests or prompts, so copy only the bounded fields
        // that explain the refusal rather than serializing the provider's whole detail object.
        let mut detail = json!({"op": op, "retryable": self.retryable.unwrap_or(false)});
        match &self.detail {
            Some(FetchMissDetail::ProviderRefusal {
                message,
                message_truncated,
                field,
                field_truncated,
            }) => {
                if let Unanswered::Refused { code } = &self.unanswered {
                    let (code, truncated) = bounded_diagnostic_text_with_truncation(code);
                    detail["provider_code"] = json!(code);
                    if truncated {
                        detail["provider_code_truncated"] = json!(true);
                    }
                }
                if let Some(message) = message {
                    detail["message"] = json!(message);
                    if *message_truncated {
                        detail["message_truncated"] = json!(true);
                    }
                }
                if let Some(field) = field {
                    detail["field"] = json!(field);
                    if *field_truncated {
                        detail["field_truncated"] = json!(true);
                    }
                }
                Some(detail)
            }
            Some(FetchMissDetail::InvalidAnswer { serde_error_kind }) => {
                if let Unanswered::Refused { code } = &self.unanswered {
                    let (code, truncated) = bounded_diagnostic_text_with_truncation(code);
                    detail["provider_code"] = json!(code);
                    if truncated {
                        detail["provider_code_truncated"] = json!(true);
                    }
                }
                if let Some(kind) = serde_error_kind {
                    detail["serde_error_kind"] = json!(kind);
                }
                Some(detail)
            }
            _ => None,
        }
    }

    fn invalid_answer(serde_error_kind: Option<&'static str>) -> Self {
        Self {
            unanswered: Unanswered::Refused {
                code: INVALID_ANSWER.into(),
            },
            retryable: None,
            detail: Some(FetchMissDetail::InvalidAnswer { serde_error_kind }),
        }
    }

    fn invalid_reply(tool: &str, member: &str) -> Self {
        Self {
            unanswered: Unanswered::InvalidReply {
                tool: tool.to_owned(),
                member: member.to_owned(),
            },
            retryable: None,
            detail: None,
        }
    }

    fn item_digest_mismatch(
        item_digest: String,
        computed_sha256: String,
        text_present: bool,
        text_bytes: usize,
    ) -> Self {
        Self {
            unanswered: Unanswered::Refused {
                code: ITEM_DIGEST_MISMATCH.into(),
            },
            retryable: None,
            detail: Some(FetchMissDetail::ItemDigestMismatch {
                item_digest,
                computed_sha256,
                text_present,
                text_bytes,
            }),
        }
    }

    /// Emit bounded refusal diagnostics and validation metadata; catalog requests can contain prompts.
    fn warn(
        &self,
        session: &broca_protocol::BindIdentity,
        plan_identity: &str,
        provider: &str,
        op: &str,
    ) {
        match (&self.unanswered, &self.detail) {
            (Unanswered::InvalidReply { tool, member }, _) => tracing::warn!(
                target: "admission",
                "invalid catalog reply session={} provider={} op={} tool={} member={}",
                session.session, provider, op, tool, member,
            ),
            (
                Unanswered::Refused { code },
                Some(FetchMissDetail::InvalidAnswer {
                    serde_error_kind: Some(serde_error_kind),
                }),
            ) if code == INVALID_ANSWER => tracing::warn!(
                target: "admission",
                "unanswered provider fetch session={} provider={} op={} code={} serde_error_kind={}",
                session.session,
                provider,
                op,
                code,
                serde_error_kind,
            ),
            (
                Unanswered::Refused { code },
                Some(FetchMissDetail::InvalidAnswer {
                    serde_error_kind: None,
                }),
            ) if code == INVALID_ANSWER => tracing::warn!(
                target: "admission",
                "unanswered provider fetch session={} provider={} op={} code={}",
                session.session,
                provider,
                op,
                code,
            ),
            (
                Unanswered::Refused { code },
                Some(FetchMissDetail::ItemDigestMismatch {
                    item_digest,
                    computed_sha256,
                    text_present,
                    text_bytes,
                }),
            ) if code == ITEM_DIGEST_MISMATCH => tracing::warn!(
                target: "admission",
                "unanswered provider fetch session={} provider={} op={} code={} item_digest={} computed_sha256={} text_present={} text_bytes={}",
                session.session,
                provider,
                op,
                code,
                item_digest,
                computed_sha256,
                text_present,
                text_bytes,
            ),
            (
                Unanswered::Refused { code },
                Some(FetchMissDetail::ProviderRefusal { message, field, .. }),
            ) if op == CATALOG_OP => tracing::warn!(
                target: "admission",
                "unanswered provider fetch session={} plan_identity={} provider={} op={} code=refused provider_code={} retryable={} refusal_message={:?} refusal_field={:?}",
                bounded_diagnostic_text(&session.session),
                bounded_diagnostic_text(plan_identity),
                bounded_diagnostic_text(provider),
                op,
                bounded_diagnostic_text(code),
                self.retryable.unwrap_or(false),
                message,
                field,
            ),
            (Unanswered::Refused { code }, _) => tracing::warn!(
                target: "admission",
                "unanswered provider fetch session={} provider={} op={} code=refused provider_code={} retryable={}",
                session.session,
                provider,
                op,
                code,
                self.retryable.unwrap_or(false),
            ),
            (unanswered, _) => tracing::warn!(
                target: "admission",
                "unanswered provider fetch session={} provider={} op={} code={}",
                session.session,
                provider,
                op,
                unanswered.reason(),
            ),
        }
    }
}

const MAX_PROVIDER_DIAGNOSTIC_BYTES: usize = 512;

fn bounded_diagnostic_text(value: &str) -> String {
    bounded_diagnostic_text_with_truncation(value).0
}

fn bounded_diagnostic_text_with_truncation(value: &str) -> (String, bool) {
    let mut bounded = String::with_capacity(value.len().min(MAX_PROVIDER_DIAGNOSTIC_BYTES));
    for character in value.chars().filter(|character| !character.is_control()) {
        if bounded.len() + character.len_utf8() > MAX_PROVIDER_DIAGNOSTIC_BYTES {
            return (bounded, true);
        }
        bounded.push(character);
    }
    (bounded, false)
}

fn provider_field(detail: Option<&Value>) -> (Option<String>, bool) {
    let Some(field) = detail
        .and_then(|detail| detail.get("field"))
        .and_then(Value::as_str)
    else {
        return (None, false);
    };
    let (field, truncated) = bounded_diagnostic_text_with_truncation(field);
    (Some(field), truncated)
}

#[derive(Debug)]
enum DeclarationOutcome {
    Answered {
        declaration: Box<Declaration>,
        answer: Value,
    },
    Unanswered(FetchMiss),
    RefusedPreset(FetchMiss),
}

/// How long admission waits, in all, for every provider's answer.
pub const ADMISSION_DEADLINE: Duration = Duration::from_secs(10);

/// Optional tools must leave time within admission for text to be recomposed without them.
pub const OPTIONAL_TOOL_BUDGET: Duration = Duration::from_secs(5);

/// The joining rule this build uses to make a session's system text from its fetched texts:
/// the texts in plan order, each non-empty one separated from the next by [`JOIN_SEPARATOR`].
/// It is recorded with every session so a later build that joins differently never changes the
/// bytes of a session admitted under this rule.
pub const JOIN_VERSION: u32 = 1;
/// The separator of join version 1.
pub const JOIN_SEPARATOR: &str = "\n\n";

/// The op that fetches a provider's catalog and system text, sent as a tool call.
pub const CATALOG_OP: &str = "tool.catalog";
/// The op that fetches a step-transform provider's subscription declaration, sent as a tool
/// call.
pub const DECLARE_OP: &str = cortexkit_role_step_transform_provider::ops::TRANSFORM_DECLARE;

/// The op admission sends a plan's compaction provider to learn whether it serves
/// `compaction-provider/v1`.
pub const DESCRIBE_OP: &str = cortexkit_role_compaction_provider::ops::ROLE_DESCRIBE;

/// The kind of an absent system-text item.
pub const ABSENT_KIND_SYSTEM_TEXT: &str = "system_text";
/// The kind of an absent tool item.
pub const ABSENT_KIND_TOOL: &str = "tool";

/// The provider code recorded when a provider's answer cannot be decoded.
pub const INVALID_ANSWER: &str = "invalid_answer";
/// The provider code recorded when a provider's system text does not hash to the item digest
/// it answered with.
pub const ITEM_DIGEST_MISMATCH: &str = "item_digest_mismatch";

/// Why one call to a provider did not answer.
#[derive(Debug, Clone, PartialEq)]
pub enum FetchError {
    /// The daemon knows no module by that id.
    UnknownModule,
    /// The provider refused. A retryable refusal is retried until the deadline.
    Refused {
        code: String,
        detail: Option<Value>,
        retryable: bool,
    },
    /// A provider refusal whose bounded human-readable explanation is retained for callers.
    ProviderRefused {
        code: String,
        detail: Option<Value>,
        message: String,
        message_truncated: bool,
        retryable: bool,
    },
    /// The call was lost in transport; retried until the deadline.
    Lost(String),
}

impl FetchError {
    fn refused_field(&self) -> Option<&str> {
        match self {
            Self::Refused { code, detail, .. } | Self::ProviderRefused { code, detail, .. } => {
                cortexkit_role_step_transform_provider::errors::refused_field(code, detail.as_ref())
            }
            Self::UnknownModule | Self::Lost(_) => None,
        }
    }

    /// Build a provider refusal after removing control characters from the diagnostic text and
    /// limiting it to 512 UTF-8 bytes.
    pub fn provider_refused(
        code: String,
        detail: Option<Value>,
        message: &str,
        retryable: bool,
    ) -> Self {
        let (message, message_truncated) = bounded_diagnostic_text_with_truncation(message);
        Self::ProviderRefused {
            code,
            detail,
            message,
            message_truncated,
            retryable,
        }
    }
}

/// Calls to the providers a plan names. Each call is one attempt; admission retries and bounds
/// them.
#[async_trait]
pub trait Fetcher: Send + Sync {
    /// A declaration from the provider's daemon catalog entry, never guessed from
    /// a version or learned by forwarding a request an older provider cannot read.
    async fn system_text_default_supported(&self, _provider: &str) -> Result<bool, FetchError> {
        Ok(false)
    }
    /// One `tool.catalog` call to `provider`.
    async fn catalog(&self, provider: &str, request: &CatalogRequest) -> Result<Value, FetchError>;
    /// One `transform.declare` call to `provider`.
    async fn declare(&self, provider: &str, request: &DeclareRequest) -> Result<Value, FetchError>;
    /// One `role.describe` call to `provider` on its `compaction-provider: v1` role route.
    /// Admission makes it only for a plan's compaction item, and sends no compaction op. A
    /// refusal that may clear on retry, or a describe that could not be asked at all, is an
    /// error; a refusal that will not clear is [`DescribeAnswer::Refused`].
    async fn describe_compaction(&self, provider: &str) -> Result<DescribeAnswer, FetchError>;
}

/// How a provider answered `role.describe`.
#[derive(Debug, Clone, PartialEq)]
pub enum DescribeAnswer {
    /// Its answer, unchecked.
    Answered(Value),
    /// The provider itself refused the describe with a typed code that does not clear on
    /// retry, such as `unknown_method`: it does not serve the role on that route. A retryable
    /// refusal, or a code the routing daemon answered in the provider's place, is a
    /// [`FetchError`] instead.
    Refused { code: String },
}

/// Admits plans for sessions: opens whatever a session's fetches need, runs [`admit`] under
/// [`ADMISSION_DEADLINE`], and releases what it opened. Each call fetches afresh; nothing
/// fetched for one session is reused for another.
#[async_trait]
pub trait PlanAdmitter: Send + Sync {
    /// Admit `plan` for `session`, fetching under the session's `scope` when it has one.
    async fn admit(
        &self,
        plan: &Plan,
        session: &broca_protocol::BindIdentity,
        scope: Option<&broca_protocol::SessionScope>,
    ) -> Result<Admitted, Refusal>;

    /// Whether sessions this admitter freezes may carry plan items opting into output
    /// references (llm-runner §9.13), so a send's `output_refs` member is accepted rather than
    /// refused. It must hold exactly when `role.describe` declares the `output_refs` group;
    /// broca does not declare it yet, so every production admitter answers false.
    fn admits_output_refs(&self) -> bool {
        false
    }
}

/// An admitted plan: the manifest to freeze.
#[derive(Debug, Clone, PartialEq)]
pub struct Admitted {
    pub manifest: PlanManifest,
}

/// The catalog request for `provider`: its tool item's preset and params (none and `{}` for a
/// text-only provider), the composition verbatim, and its system-text item when it has one.
pub fn catalog_request(
    plan: &Plan,
    tool_item: Option<&Item>,
    text_item: Option<&Item>,
) -> CatalogRequest {
    let mut request = tool_catalog_request(tool_item).with_composition(plan.composition.clone());
    if let Some(item) = text_item {
        request = request.with_system_text(SystemTextItem {
            preset: item.preset.clone(),
            params: item.params.clone(),
        });
    }
    request
}

fn tool_catalog_request(tool_item: Option<&Item>) -> CatalogRequest {
    let mut request = CatalogRequest::new(
        tool_item
            .map(|item| item.params.clone())
            .unwrap_or_default(),
    );
    if let Some(preset) = tool_item.and_then(|item| item.preset.clone()) {
        request = request.with_preset(preset);
    }
    request
}

/// Compose a tool-only plan by preflighting and decoding each provider's live catalog.
/// Admission fetches again with the resulting composition and validates it before a send.
/// Text and transform items are not served by this operator helper. No model request or
/// durable write is made.
pub async fn compose(
    items: &Value,
    fetcher: &dyn Fetcher,
    budget: Duration,
) -> Result<Value, Refusal> {
    let object = items.as_object().ok_or_else(|| Refusal::invalid("plan"))?;
    for field in ["composition", "composition_digest", "compaction_item"] {
        if object.contains_key(field) {
            return Err(Refusal::invalid(format!("plan.{field}")));
        }
    }
    for field in ["system_text_items", "step_transform_items"] {
        if !object
            .get(field)
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty)
        {
            return Err(Refusal::invalid(format!("plan.{field}")));
        }
    }
    let tools = crate::plan::items(object, "tool_items", true)?;
    let mut providers: BTreeMap<_, Value> = tools
        .iter()
        .map(|item| {
            (
                item.provider.clone(),
                serde_json::json!({"provider": item.provider, "tools": []}),
            )
        })
        .collect();
    let mut plan = items.clone();
    let set_composition =
        |plan: &mut Value, providers: &BTreeMap<String, Value>| -> Result<(), Refusal> {
            plan["composition"] = serde_json::json!({
                "providers": providers.values().collect::<Vec<_>>(), "step_transform_items": []
            });
            plan["composition_digest"] = serde_json::json!(
                cortexkit_role_tool_provider::catalog::composition_digest(&plan["composition"])
                    .map_err(|_| Refusal::invalid("plan.composition"))?
            );
            Ok(())
        };
    set_composition(&mut plan, &providers)?;
    let parsed = crate::parse_plan(&plan, crate::CompactionSupport::Unsupported)?;
    let deadline = Instant::now() + budget;
    for item in &parsed.tool_items {
        // Preflight omits composition and system text: tools establish the composition that
        // admission will send back to the provider when it freezes the catalog.
        let request = tool_catalog_request(Some(item));
        let fetched = within_deadline(deadline, || fetcher.catalog(&item.provider, &request))
            .await
            .map_err(FetchMiss::from_error)
            .and_then(|value| decode_answer(value, false))
            .and_then(|fetched| {
                if fetched.answer.composition_digest.is_some() {
                    Err(FetchMiss::invalid_answer(None))
                } else {
                    Ok(fetched)
                }
            })
            .map_err(|miss| Refusal::FetchUnavailable {
                provider: item.provider.clone(),
                provider_detail: miss.provider_detail(CATALOG_OP),
                unanswered: miss.unanswered,
            })?;
        let mut composed: Vec<_> = fetched
            .answer
            .tools
            .iter()
            .map(|tool| {
                let capabilities: BTreeSet<_> = tool.capabilities.iter().collect();
                serde_json::json!({"name": tool.name, "capabilities": capabilities})
            })
            .collect();
        composed.sort_by(|left, right| left["name"].as_str().cmp(&right["name"].as_str()));
        providers
            .get_mut(&item.provider)
            .expect("parsed provider is composed")["tools"] = Value::Array(composed);
    }
    set_composition(&mut plan, &providers)?;
    crate::parse_plan(&plan, crate::CompactionSupport::Unsupported)?;
    Ok(plan)
}

/// Check every item carrying `params.model` before opening or calling any provider.
///
/// Compare the caller's provider and model id strings and requested variant, not catalog aliases,
/// wire ids or renderer tiers: the persona provider receives these caller values verbatim.
pub fn check_plan_model(
    plan: &Plan,
    provider: &str,
    id: &str,
    variant: Option<&str>,
) -> Result<(), Refusal> {
    let mut send_model = json!({"provider": provider, "id": id});
    if let Some(variant) = variant {
        send_model["variant"] = json!(variant);
    }
    let items = plan
        .tool_items
        .iter()
        .enumerate()
        .map(|(i, item)| (format!("plan.tool_items[{i}]"), item))
        .chain(
            plan.system_text_items
                .iter()
                .enumerate()
                .map(|(i, item)| (format!("plan.system_text_items[{i}]"), item)),
        )
        .chain(
            plan.step_items
                .iter()
                .enumerate()
                .map(|(i, step)| (format!("plan.step_transform_items[{i}]"), &step.item)),
        );
    for (path, item) in items {
        let Some(plan_model) = item.params.get("model") else {
            continue;
        };
        let at = format!("{path}.params.model");
        let model = plan_model
            .as_object()
            .ok_or_else(|| Refusal::invalid(&at))?;
        let plan_provider = model
            .get("provider")
            .and_then(Value::as_str)
            .ok_or_else(|| Refusal::invalid(format!("{at}.provider")))?;
        let plan_id = model
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| Refusal::invalid(format!("{at}.id")))?;
        if plan_provider != provider
            || plan_id != id
            || model
                .get("variant")
                .is_some_and(|v| Some(v) != send_model.get("variant"))
        {
            return Err(Refusal::PlanModelMismatch {
                provider: item.provider.clone(),
                plan_model: plan_model.clone(),
                send_model: send_model.clone(),
            });
        }
    }
    Ok(())
}

/// Join fetched system texts in plan order under [`JOIN_VERSION`]. An empty text adds no bytes
/// and no separator.
pub fn join_system_texts<'a>(texts: impl IntoIterator<Item = &'a str>) -> String {
    let mut joined = String::new();
    for text in texts {
        if text.is_empty() {
            continue;
        }
        if !joined.is_empty() {
            joined.push_str(JOIN_SEPARATOR);
        }
        joined.push_str(text);
    }
    joined
}

/// Call `attempt` until it answers, a non-retryable outcome arrives, or `deadline` passes.
async fn within_deadline<T, F, Fut>(deadline: Instant, mut attempt: F) -> Result<T, FetchError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, FetchError>>,
{
    let mut backoff = Duration::from_millis(50);
    let mut last = FetchError::Lost("no answer before the admission deadline".into());
    loop {
        match tokio::time::timeout_at(deadline, attempt()).await {
            Err(_) => return Err(last),
            Ok(Ok(answer)) => return Ok(answer),
            Ok(Err(
                error @ (FetchError::UnknownModule
                | FetchError::Refused {
                    retryable: false, ..
                }
                | FetchError::ProviderRefused {
                    retryable: false, ..
                }),
            )) => return Err(error),
            Ok(Err(error)) => last = error,
        }
        let wake = Instant::now() + backoff;
        if wake >= deadline {
            tokio::time::sleep_until(deadline).await;
            return Err(last);
        }
        tokio::time::sleep_until(wake).await;
        backoff = (backoff * 2).min(Duration::from_secs(1));
    }
}

/// How a fetch that ended without an answer is reported.
fn unanswered(error: FetchError) -> Unanswered {
    match error {
        FetchError::UnknownModule => Unanswered::UnknownModule,
        // A retryable refusal still refusing at the deadline is reported as that refusal.
        FetchError::Refused { code, .. } | FetchError::ProviderRefused { code, .. } => {
            Unanswered::Refused { code }
        }
        FetchError::Lost(_) => Unanswered::Timeout,
    }
}

/// A provider's decoded catalog answer.
#[derive(Debug, Clone)]
struct Fetched {
    answer: CatalogAnswer,
    /// The answered system text (`""` when the answer carries none), when a system-text item
    /// was asked of the provider.
    text: Option<String>,
}

fn serde_error_kind(error: &serde_json::Error) -> &'static str {
    match error.classify() {
        serde_json::error::Category::Io => "io",
        serde_json::error::Category::Syntax => "syntax",
        serde_json::error::Category::Data => "data",
        serde_json::error::Category::Eof => "eof",
    }
}

/// Decode `value` as the answer to a fetch that asked for system text when `text_asked`.
fn decode_answer(value: Value, text_asked: bool) -> Result<Fetched, FetchMiss> {
    // Check the wire types before typed decoding loses the offending member's path.
    // Range and schema validation remain the shared contract's responsibility below.
    for tool in value["tools"].as_array().into_iter().flatten() {
        let name = tool["name"].as_str().unwrap_or("<unnamed>");
        if let Some(reply) = tool.get("reply").filter(|reply| !reply.is_null()) {
            let invalid = |member| FetchMiss::invalid_reply(name, member);
            if !reply.is_object() {
                return Err(invalid("reply"));
            }
            if reply.get("max_ms").and_then(Value::as_u64).is_none() {
                return Err(invalid("reply.max_ms"));
            }
            if let Some(hold) = reply.get("hold_argument").filter(|hold| !hold.is_null()) {
                if !hold.is_object() {
                    return Err(invalid("reply.hold_argument"));
                }
                if hold.get("name").and_then(Value::as_str).is_none() {
                    return Err(invalid("reply.hold_argument.name"));
                }
                for (field, member) in [
                    ("default_ms", "reply.hold_argument.default_ms"),
                    ("grace_ms", "reply.hold_argument.grace_ms"),
                ] {
                    if hold.get(field).and_then(Value::as_u64).is_none() {
                        return Err(invalid(member));
                    }
                }
            }
        }
    }
    let answer: CatalogAnswer = serde_json::from_value(value)
        .map_err(|error| FetchMiss::invalid_answer(Some(serde_error_kind(&error))))?;
    for tool in &answer.tools {
        check_reply(tool)
            .map_err(|problem| FetchMiss::invalid_reply(&problem.tool, problem.member))?;
    }
    let text = if text_asked {
        let system_text = answer
            .system_text
            .as_ref()
            .ok_or_else(|| FetchMiss::invalid_answer(None))?;
        let text_present = system_text.text.is_some();
        let text = system_text.text.clone().unwrap_or_default();
        // The tool-provider contract defines `item_digest` as this exact hash of the returned
        // text, so check with the contract's own function rather than a local copy of it.
        let computed_sha256 = cortexkit_role_tool_provider::catalog::system_text_digest(&text);
        if computed_sha256 != system_text.item_digest {
            return Err(FetchMiss::item_digest_mismatch(
                system_text.item_digest.clone(),
                computed_sha256,
                text_present,
                text.len(),
            ));
        }
        Some(text)
    } else {
        None
    };
    Ok(Fetched { answer, text })
}

/// Refuse a catalog whose reference slots break the catalog's own rules
/// (`check_reference_slots`), naming the tool and member, when the provider's tool item opts
/// into output references. A provider that does not opt in is not checked: its slots are never
/// read or frozen, so its admission is exactly what it was before slots existed.
fn check_opted_in_slots(fetched: Fetched, tool_item: Option<&Item>) -> Result<Fetched, FetchMiss> {
    if tool_item.is_some_and(|item| item.output_refs) {
        for tool in &fetched.answer.tools {
            cortexkit_role_tool_provider::catalog::check_reference_slots(&tool.reference_slots)
                .map_err(|problem| FetchMiss::invalid_reply(&tool.name, problem.field))?;
        }
    }
    Ok(fetched)
}

/// The reference slots to freeze for `provider`: every tool's declared slots by tool name, when
/// the provider's tool item opts into output references and some tool declares any. `None`
/// otherwise, which writes no bytes.
fn frozen_reference_slots(
    plan: &Plan,
    provider: &str,
    fetched: &Fetched,
) -> Option<BTreeMap<String, Vec<broca_wal::FrozenReferenceSlot>>> {
    plan.tool_items
        .iter()
        .find(|item| item.provider == provider)
        .filter(|item| item.output_refs)?;
    let slots: BTreeMap<_, _> = fetched
        .answer
        .tools
        .iter()
        .filter(|tool| !tool.reference_slots.is_empty())
        .map(|tool| {
            (
                tool.name.clone(),
                tool.reference_slots
                    .iter()
                    .map(|slot| broca_wal::FrozenReferenceSlot {
                        name: slot.name.clone(),
                        kind: slot.kind.clone(),
                    })
                    .collect(),
            )
        })
        .collect();
    (!slots.is_empty()).then_some(slots)
}

/// The tool a fetched catalog tool becomes in the session: routed to `provider`, with the
/// provider's description and schema.
fn session_tool(provider: &str, tool: &cortexkit_role_tool_provider::catalog::CatalogTool) -> Tool {
    Tool {
        name: tool.name.clone(),
        description: tool.description.clone().unwrap_or_default(),
        input_schema: tool.input_schema.clone(),
        module: Some(provider.to_owned()),
        execution_mode: ExecutionMode::default(),
        reply: None,
    }
}

/// Copy validated catalog fields into the WAL's independently owned storage shape.
fn freeze_reply(reply: &cortexkit_role_tool_provider::catalog::Reply) -> broca_wal::FrozenReply {
    broca_wal::FrozenReply {
        max_ms: reply.max_ms,
        hold_argument: reply
            .hold_argument
            .as_ref()
            .map(|hold| broca_wal::FrozenHoldArgument {
                name: hold.name.clone(),
                default_ms: hold.default_ms,
                grace_ms: hold.grace_ms,
            }),
    }
}

/// Freeze catalog identities and reply declarations at admission so dispatch and
/// display never depend on a provider's later catalog.
fn frozen_provider_declarations(
    answer: &cortexkit_role_tool_provider::catalog::CatalogAnswer,
) -> Option<FrozenProviderDeclarations> {
    let result_formats: BTreeMap<String, FrozenResultFormat> = answer
        .tools
        .iter()
        .filter_map(|tool| {
            tool.result_format.as_ref().map(|format| {
                (
                    tool.name.clone(),
                    FrozenResultFormat(format.as_str().to_owned()),
                )
            })
        })
        .collect();
    let declarations = FrozenProviderDeclarations {
        schema_pins: (!answer.tools.is_empty()).then(|| {
            answer
                .tools
                .iter()
                .map(|tool| {
                    (
                        tool.name.clone(),
                        broca_wal::DispatchSchemaPin {
                            schema_digest: tool.schema_digest.clone(),
                            semantics: tool.semantics,
                        },
                    )
                })
                .collect()
        }),
        result_formats: (!result_formats.is_empty()).then_some(result_formats),
        late_results: answer
            .declares(cortexkit_role_tool_provider::catalog::session_capabilities::LATE_RESULTS)
            .then_some(true),
    };
    (!declarations.is_empty()).then_some(declarations)
}

/// The declarations one provider froze into `manifest`. These are the only declarations a run
/// may route or decode by: a later catalog answer can differ from what the session admitted.
pub fn frozen_provider_declarations_of<'a>(
    manifest: &'a PlanManifest,
    provider: &str,
) -> Option<&'a FrozenProviderDeclarations> {
    manifest
        .fetches
        .iter()
        .find(|fetch| fetch.provider == provider)
        .and_then(|fetch| match &fetch.outcome {
            FetchOutcome::Answered {
                declarations_frozen,
                ..
            } => declarations_frozen.as_ref(),
            _ => None,
        })
}

/// Whether `provider` froze the `late_results` session capability into `manifest`.
pub fn frozen_declares_late_results(manifest: &PlanManifest, provider: &str) -> bool {
    frozen_provider_declarations_of(manifest, provider)
        .and_then(|declarations| declarations.late_results)
        .unwrap_or(false)
}

/// The result format `provider` froze for `tool` into `manifest`, if it opted in.
pub fn frozen_result_format<'a>(
    manifest: &'a PlanManifest,
    provider: &str,
    tool: &str,
) -> Option<&'a FrozenResultFormat> {
    frozen_provider_declarations_of(manifest, provider)?
        .result_formats
        .as_ref()?
        .get(tool)
}

fn names_missing<'a>(from: &'a [String], against: &[String]) -> Vec<&'a str> {
    from.iter()
        .filter(|name| !against.contains(name))
        .map(String::as_str)
        .collect()
}

/// The differences between what `provider` answered and the plan: its composition digest, its
/// tool names and capability tags against its composition entry, and the tool names its text
/// was composed for against the tools it answered.
fn differences_of(plan: &Plan, provider: &str, fetched: &Fetched) -> Vec<Value> {
    let mut differences = Vec::new();
    let expected = &plan.composition_digest;
    let answered_digest = fetched.answer.composition_digest.as_deref();
    let text_digest = fetched
        .text
        .as_ref()
        .and(fetched.answer.system_text.as_ref())
        .map(|text| text.composition_digest.as_deref());
    let wrong_digest = if answered_digest != Some(expected.as_str()) {
        Some(answered_digest)
    } else {
        text_digest.filter(|digest| *digest != Some(expected.as_str()))
    };
    if let Some(got) = wrong_digest {
        differences.push(json!({
            "kind": "composition_digest",
            "provider": provider,
            "expected": expected,
            "got": got,
        }));
    }
    let composed = plan
        .composed
        .get(provider)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let composed_names: Vec<String> = composed.iter().map(|tool| tool.name.clone()).collect();
    let fetched_names: Vec<String> = fetched
        .answer
        .tools
        .iter()
        .map(|tool| tool.name.clone())
        .collect();
    let missing = names_missing(&composed_names, &fetched_names);
    let unexpected = names_missing(&fetched_names, &composed_names);
    if !missing.is_empty() || !unexpected.is_empty() {
        differences.push(json!({
            "kind": "tools",
            "provider": provider,
            "missing": missing,
            "unexpected": unexpected,
        }));
    }
    for tool in composed {
        let Some(answered) = fetched.answer.tools.iter().find(|t| t.name == tool.name) else {
            continue;
        };
        let got: BTreeSet<&str> = answered.capabilities.iter().map(String::as_str).collect();
        let want: BTreeSet<&str> = tool.capabilities.iter().map(String::as_str).collect();
        if got != want {
            differences.push(json!({
                "kind": "capabilities",
                "provider": provider,
                "tool": tool.name,
                "expected": tool.capabilities,
                "got": answered.capabilities,
            }));
        }
    }
    // The tool names the provider's text was composed for, which must be the tools it answered.
    let text_names = fetched
        .text
        .as_ref()
        .and(fetched.answer.system_text.as_ref())
        .and_then(|text| text.tool_names.as_ref());
    if let Some(text_names) = text_names {
        let missing = names_missing(&fetched_names, text_names);
        let unexpected = names_missing(text_names, &fetched_names);
        if !missing.is_empty() || !unexpected.is_empty() {
            differences.push(json!({
                "kind": "text_tool_names",
                "provider": provider,
                "missing": missing,
                "unexpected": unexpected,
            }));
        }
    }
    differences
}

/// One provider's catalog fetch: which of its plan items it serves.
struct CatalogFetch<'a> {
    provider: &'a str,
    tool_item: Option<&'a Item>,
    text_item: Option<&'a Item>,
}

impl CatalogFetch<'_> {
    /// An optional tool's absence also removes any text from that provider.
    fn optional(&self) -> bool {
        self.tool_item.map_or_else(
            || self.text_item.is_some_and(|item| item.optional),
            |item| item.optional,
        )
    }
}

/// What a plan's compaction provider said about the role, once its `role.describe` arrived.
#[derive(Debug)]
enum CompactionDiscovery {
    /// It serves `compaction-provider/v1` with every required op.
    Served,
    /// It said it does not serve `compaction-provider/v1`. Each problem is an object whose
    /// `problem` member names it: the role crate's `DescribeProblem::name` for a readable
    /// describe (`missing_major` with the majors `found`, or `missing_op` with the `op`), or
    /// [`DESCRIBE_REFUSED`] with the refusal's `code`.
    NotServed(Vec<Value>),
}

/// The problem naming a non-retryable refusal of `role.describe` itself.
pub const DESCRIBE_REFUSED: &str = "describe_refused";

/// Classify a compaction provider's answer to `role.describe`. Proof that the provider does
/// not serve the role is either a describe that decodes and then lacks the
/// `compaction-provider/v1` major or a required op, or the provider refusing the describe with
/// a code that does not clear on retry. An answer that is not a describe at all (not an
/// object, or missing a required member) is unreadable, so it is a miss like any other
/// unusable fetch answer: it leaves admission unable to tell what the provider serves, which
/// is not the same as the provider saying no.
fn classify_compaction_describe(answer: &DescribeAnswer) -> Result<CompactionDiscovery, FetchMiss> {
    use cortexkit_role_compaction_provider::describe::{check_describe, DescribeProblem};
    let raw = match answer {
        DescribeAnswer::Answered(raw) => raw,
        DescribeAnswer::Refused { code } => {
            return Ok(CompactionDiscovery::NotServed(vec![
                json!({"problem": DESCRIBE_REFUSED, "code": code}),
            ]));
        }
    };
    let problems = match check_describe(raw) {
        Ok(_) => return Ok(CompactionDiscovery::Served),
        Err(problems) => problems,
    };
    let mut named = Vec::with_capacity(problems.len());
    for problem in &problems {
        named.push(match problem {
            DescribeProblem::NotAnObject | DescribeProblem::Undecodable(_) => {
                return Err(FetchMiss::invalid_answer(None));
            }
            DescribeProblem::MissingMajor { found } => {
                json!({"problem": problem.name(), "found": found})
            }
            DescribeProblem::MissingOp(op) => json!({"problem": problem.name(), "op": op}),
        });
    }
    Ok(CompactionDiscovery::NotServed(named))
}

/// Admit `plan`: fetch every provider it names through `fetcher`, all within `deadline` of now,
/// and check the answers. `session` identifies warning records for unanswered fetches. Returns
/// the manifest to freeze, or the refusal.
///
/// Refusals, in the order they are decided: `invalid_params` naming `plan.compaction_item` when
/// the plan's compaction provider answered a describe that does not serve the role (a
/// permanent answer, so it outranks a miss elsewhere that a retry might clear);
/// `fetch_unavailable` for the first provider (by id) whose required item, or whose compaction
/// describe, did not answer; `plan_stale` listing every difference between what was answered
/// and the plan; `tool_name_collision` for the first model-facing name offered twice.
pub async fn admit(
    plan: &Plan,
    fetcher: &dyn Fetcher,
    session: &broca_protocol::BindIdentity,
    deadline: Duration,
) -> Result<Admitted, Refusal> {
    let started = Instant::now();
    let deadline = started + deadline;
    for item in plan
        .system_text_items
        .iter()
        .filter(|item| item.preset.is_none())
    {
        let supported = within_deadline(deadline, || {
            fetcher.system_text_default_supported(&item.provider)
        })
        .await
        .map_err(|error| {
            let missed = FetchMiss::from_error(error);
            Refusal::FetchUnavailable {
                provider: item.provider.clone(),
                unanswered: missed.unanswered.clone(),
                provider_detail: missed.provider_detail("catalog.list"),
            }
        })?;
        if !supported {
            return Err(Refusal::SystemTextPresetRequired {
                provider: item.provider.clone(),
            });
        }
    }
    let mut catalog_fetches: Vec<CatalogFetch<'_>> = Vec::new();
    for item in plan.tool_items.iter().chain(&plan.system_text_items) {
        if catalog_fetches.iter().all(|f| f.provider != item.provider) {
            catalog_fetches.push(CatalogFetch {
                provider: &item.provider,
                tool_item: plan.tool_items.iter().find(|t| t.provider == item.provider),
                text_item: plan
                    .system_text_items
                    .iter()
                    .find(|t| t.provider == item.provider),
            });
        }
    }
    let catalogs = futures::future::join_all(catalog_fetches.iter().map(|fetch| {
        let request = catalog_request(plan, fetch.tool_item, fetch.text_item);
        let fetch_deadline = if fetch.tool_item.is_some_and(|item| item.optional) {
            deadline.min(started + OPTIONAL_TOOL_BUDGET)
        } else {
            deadline
        };
        async move {
            within_deadline(fetch_deadline, || fetcher.catalog(fetch.provider, &request))
                .await
                .map_err(FetchMiss::from_error)
        }
    }));
    let declarations = futures::future::join_all(plan.step_items.iter().map(|step| {
        let mut request = DeclareRequest::new(step.item.params.clone())
            .with_composition(plan.composition.clone());
        if let Some(preset) = &step.item.preset {
            request = request.with_preset(preset.clone());
        }
        async move {
            match within_deadline(deadline, || fetcher.declare(&step.item.provider, &request)).await
            {
                Ok(answer) => match serde_json::from_value::<Declaration>(answer.clone()) {
                    Ok(declaration) => DeclarationOutcome::Answered {
                        declaration: Box::new(declaration),
                        answer,
                    },
                    Err(error) => DeclarationOutcome::Unanswered(FetchMiss::invalid_answer(Some(
                        serde_error_kind(&error),
                    ))),
                },
                // A provider refusing the preset leaves the item with no declaration: a named
                // preset it no longer knows makes the plan stale, and a presetless item for a
                // provider with no default is a planner error (`invalid_params`), both decided
                // by the shared item check below.
                Err(error) if error.refused_field() == Some("preset") => {
                    DeclarationOutcome::RefusedPreset(FetchMiss::from_error(error))
                }
                Err(error) => DeclarationOutcome::Unanswered(FetchMiss::from_error(error)),
            }
        }
    }));
    // Only a plan that names a compaction provider asks one anything; a plan without one opens
    // no compaction route. Discovery sends `role.describe` alone, never Setup.
    let compaction = async {
        let item = plan.compaction_item.as_ref()?;
        Some(
            within_deadline(deadline, || fetcher.describe_compaction(&item.provider))
                .await
                .map_err(FetchMiss::from_error)
                .and_then(|answer| classify_compaction_describe(&answer)),
        )
    };
    let (catalogs, declarations, compaction) = futures::join!(catalogs, declarations, compaction);
    let mut raw_answers = BTreeMap::new();
    let mut outcomes: BTreeMap<&str, Result<Fetched, FetchMiss>> = BTreeMap::new();
    let mut absent_tools = BTreeSet::new();
    for (fetch, outcome) in catalog_fetches.iter().zip(&catalogs) {
        let mut decoded = match outcome {
            Ok(answer) => decode_answer(answer.clone(), fetch.text_item.is_some())
                .and_then(|fetched| check_opted_in_slots(fetched, fetch.tool_item)),
            Err(_) => continue,
        };
        if let Ok(answer) = outcome {
            raw_answers.insert(fetch.provider, answer.clone());
        }
        if fetch.tool_item.is_some_and(|item| item.optional)
            && decoded
                .as_ref()
                .is_ok_and(|fetched| !differences_of(plan, fetch.provider, fetched).is_empty())
        {
            decoded = Err(FetchMiss {
                unanswered: Unanswered::PlanStale,
                retryable: None,
                detail: None,
            });
        }
        outcomes.insert(fetch.provider, decoded);
    }
    for (fetch, outcome) in catalog_fetches.iter().zip(catalogs) {
        if let Err(missed) = outcome {
            outcomes.insert(fetch.provider, Err(missed));
        }
        if fetch.tool_item.is_some_and(|item| item.optional) && outcomes[fetch.provider].is_err() {
            absent_tools.insert(fetch.provider);
        }
    }

    // Keep the planned round unchanged on healthy launches. Only an absent optional tool
    // changes the composition and requires discarding every first-round text.
    let mut admitted_plan = plan.clone();
    let mut differences = Vec::new();
    while !absent_tools.is_empty() {
        let absent_count = absent_tools.len();
        admitted_plan = plan.clone();
        differences.clear();
        for fetch in &catalog_fetches {
            if absent_tools.contains(fetch.provider) || fetch.tool_item.is_none() {
                continue;
            }
            if let Some(answer) = raw_answers.get(fetch.provider) {
                // Ignore first-round text validation after the tool set changes. Required
                // tool catalogs still have to match the original planned tool set.
                let tools = decode_answer(answer.clone(), false)
                    .and_then(|fetched| check_opted_in_slots(fetched, fetch.tool_item));
                if let Ok(fetched) = &tools {
                    differences.extend(differences_of(plan, fetch.provider, fetched));
                }
                outcomes.insert(fetch.provider, tools);
            }
        }
        admitted_plan.composition["providers"]
            .as_array_mut()
            .expect("checked composition providers")
            .retain(|entry| {
                !absent_tools.contains(entry["provider"].as_str().expect("checked provider"))
            });
        admitted_plan.composition_digest =
            cortexkit_role_tool_provider::catalog::composition_digest(&Value::Object(
                admitted_plan.composition.clone(),
            ))
            .expect("a filtered checked composition is JSON");
        for provider in &absent_tools {
            admitted_plan.composed.remove(*provider);
        }
        let texts = futures::future::join_all(catalog_fetches.iter().filter_map(|fetch| {
            let text_item = fetch.text_item?;
            if absent_tools.contains(fetch.provider) {
                return None;
            }
            let request = catalog_request(&admitted_plan, None, Some(text_item));
            Some(async move {
                let outcome =
                    within_deadline(deadline, || fetcher.catalog(fetch.provider, &request))
                        .await
                        .map_err(FetchMiss::from_error)
                        .and_then(|answer| decode_answer(answer, true));
                (fetch.provider, outcome)
            })
        }))
        .await;
        // Text-only catalog requests must answer no tools, even when this provider also
        // supplied tools in the first round. Validate them against the admitted digest.
        let mut text_plan = admitted_plan.clone();
        text_plan.composed.values_mut().for_each(Vec::clear);
        for (provider, text) in texts {
            let optional_tool = plan
                .tool_items
                .iter()
                .any(|item| item.provider == provider && item.optional);
            match text {
                Ok(text) => {
                    let stale = differences_of(&text_plan, provider, &text);
                    if !stale.is_empty() {
                        if optional_tool {
                            absent_tools.insert(provider);
                            outcomes.insert(
                                provider,
                                Err(FetchMiss {
                                    unanswered: Unanswered::PlanStale,
                                    retryable: None,
                                    detail: None,
                                }),
                            );
                        } else {
                            differences.extend(stale);
                        }
                        continue;
                    }
                    // A text-only provider's entire first answer is superseded. For a
                    // mixed provider, a text answer cannot repair a failed tool catalog.
                    if !plan.tool_items.iter().any(|item| item.provider == provider) {
                        outcomes.insert(provider, Ok(text));
                    } else if let Some(Ok(fetched)) = outcomes.get_mut(provider) {
                        fetched.text = text.text;
                        fetched.answer.system_text = text.answer.system_text;
                    }
                }
                Err(missed) => {
                    if optional_tool {
                        absent_tools.insert(provider);
                    }
                    outcomes.insert(provider, Err(missed));
                }
            }
        }
        if absent_tools.len() == absent_count {
            break;
        }
        // A remaining optional tool provider can fail during its text fetch too. Remove it
        // and recompose surviving text again; each retry removes at least one provider.
    }

    // Log only terminal outcomes, after retries have either answered or exhausted the deadline.
    for fetch in &catalog_fetches {
        let outcome = &outcomes[fetch.provider];
        if let Err(missed) = outcome {
            missed.warn(session, &plan.identity, fetch.provider, CATALOG_OP);
        }
    }
    for (step, outcome) in plan.step_items.iter().zip(&declarations) {
        match outcome {
            DeclarationOutcome::Unanswered(missed) | DeclarationOutcome::RefusedPreset(missed) => {
                missed.warn(session, &plan.identity, &step.item.provider, DECLARE_OP);
            }
            DeclarationOutcome::Answered { .. } => {}
        }
    }
    let compaction_provider = plan
        .compaction_item
        .as_ref()
        .map(|item| item.provider.as_str());
    if let (Some(provider), Some(Err(missed))) = (compaction_provider, &compaction) {
        missed.warn(session, &plan.identity, provider, DESCRIBE_OP);
    }

    // A compaction provider that said it does not serve the role will say so on every retry.
    if let (Some(provider), Some(Ok(CompactionDiscovery::NotServed(problems)))) =
        (compaction_provider, &compaction)
    {
        tracing::warn!(
            target: "admission",
            "compaction provider does not serve the role session={} provider={} op={}",
            session.session,
            provider,
            DESCRIBE_OP,
        );
        return Err(Refusal::CompactionRoleNotServed {
            provider: provider.to_owned(),
            problems: problems.clone(),
        });
    }

    // A required item that did not answer refuses admission; an optional one is absent.
    let mut failures: BTreeMap<&str, (FetchMiss, &'static str)> = BTreeMap::new();
    for fetch in &catalog_fetches {
        if let Err(missed) = &outcomes[fetch.provider] {
            if !fetch.optional() {
                failures
                    .entry(fetch.provider)
                    .or_insert_with(|| (missed.clone(), CATALOG_OP));
            }
        }
    }
    for (step, outcome) in plan.step_items.iter().zip(&declarations) {
        if let DeclarationOutcome::Unanswered(missed) = outcome {
            failures
                .entry(&step.item.provider)
                .or_insert_with(|| (missed.clone(), DECLARE_OP));
        }
    }
    if let (Some(provider), Some(Err(missed))) = (compaction_provider, &compaction) {
        failures
            .entry(provider)
            .or_insert_with(|| (missed.clone(), DESCRIBE_OP));
    }
    if let Some((provider, (missed, op))) = failures.into_iter().next() {
        return Err(Refusal::FetchUnavailable {
            provider: provider.to_owned(),
            unanswered: missed.unanswered.clone(),
            provider_detail: missed.provider_detail(op),
        });
    }

    if absent_tools.is_empty() {
        for (provider, outcome) in &outcomes {
            if let Ok(fetched) = outcome {
                differences.extend(differences_of(plan, provider, fetched));
            }
        }
    }
    // Validate declarations against the same reduction owner that the plan freezes.
    let reduction_owner = plan
        .compaction_item
        .as_ref()
        .map(|item| item.provider.as_str());
    let mut frozen_declarations = Vec::with_capacity(plan.step_items.len());
    for (index, (step, declaration)) in plan.step_items.iter().zip(declarations).enumerate() {
        let declaration = match declaration {
            DeclarationOutcome::Answered {
                declaration,
                answer,
            } => {
                // Keep the provider's JSON, including unknown members and omitted defaults,
                // rather than round-tripping it through the typed admission view.
                frozen_declarations.push(FrozenDeclaration {
                    provider: step.item.provider.clone(),
                    answer: OpaqueJson::from_json_text(answer.to_string()).expect("JSON"),
                });
                Some(declaration)
            }
            DeclarationOutcome::RefusedPreset(_) => None,
            DeclarationOutcome::Unanswered(_) => {
                unreachable!("an unanswered declaration refused admission above")
            }
        };
        // The provider role's own item check, the one every runner and the plan composer use,
        // so all sides give the same admission answer for the role's copied cases.
        let checked = check_item_with_optional_preset(
            index,
            &step.item.provider,
            step.item.preset.as_deref(),
            declaration.as_deref(),
            &step.subscriptions,
            reduction_owner,
        );
        match checked {
            Ok(_) => {}
            Err(ItemAdmissionRefusal::PlanStale(stale)) => differences.extend(
                stale
                    .into_iter()
                    .map(|difference| serde_json::to_value(difference).expect("JSON")),
            ),
            // The role's other refusals (a malformed subscription, a presetless item for a
            // provider with no default) are `invalid_params`; the detail is the role's own body,
            // so a missing default reads exactly `{field: "preset"}`. The role marks the enum
            // non-exhaustive, and the exact version pin means a new variant only arrives with a
            // deliberate upgrade whose changelog names it.
            Err(refusal) => {
                return Err(Refusal::InvalidParams {
                    detail: refusal.refusal_body()["detail"].clone(),
                });
            }
        }
    }
    if !differences.is_empty() {
        return Err(Refusal::PlanStale { differences });
    }

    // The tools offered to the model: each tool item's provider in plan order, each provider's
    // tools in the order it answered them.
    let mut tools = Vec::new();
    let mut offered_by: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    let mut names_in_order: Vec<&str> = Vec::new();
    for item in &plan.tool_items {
        let Some(Ok(fetched)) = outcomes.get(item.provider.as_str()) else {
            continue;
        };
        for tool in &fetched.answer.tools {
            let providers = offered_by.entry(tool.name.as_str()).or_default();
            if providers.is_empty() {
                names_in_order.push(tool.name.as_str());
            }
            providers.push(&item.provider);
            tools.push(session_tool(&item.provider, tool));
        }
    }
    if let Some(name) = names_in_order
        .into_iter()
        .find(|name| offered_by[name].len() > 1)
    {
        let mut providers: Vec<String> = offered_by[name].iter().map(|p| (*p).to_owned()).collect();
        providers.sort();
        providers.dedup();
        return Err(Refusal::ToolNameCollision {
            name: name.to_owned(),
            providers,
        });
    }

    let texts: Vec<&str> = plan
        .system_text_items
        .iter()
        .filter_map(|item| match outcomes.get(item.provider.as_str()) {
            Some(Ok(fetched)) => fetched.text.as_deref(),
            _ => None,
        })
        .collect();
    let system_text = join_system_texts(texts);
    let stamps: Vec<_> = plan
        .system_text_items
        .iter()
        .filter_map(|item| {
            let fetched = outcomes.get(item.provider.as_str())?.as_ref().ok()?;
            let stamp = fetched.answer.system_text.as_ref()?.rich_ui.as_ref()?;
            let stamp = broca_protocol::RichUiStamp::new(stamp.dialect(), stamp.catalog_digest())
                .expect("the provider stamp was validated when decoding its answer");
            Some((item.provider.as_str(), stamp))
        })
        .collect();
    let rich_ui = cortexkit_role_llm_runner::rich_ui::plan_stamp(
        stamps
            .iter()
            .map(|(provider, stamp)| (*provider, Some(stamp))),
    )
    .map_err(|conflict| Refusal::PlanStale {
        differences: conflict.detail()["differences"]
            .as_array()
            .expect("conflict differences")
            .clone(),
    })?;
    let fetches = outcomes
        .iter()
        .map(|(provider, outcome)| ProviderFetch {
            provider: (*provider).to_owned(),
            outcome: match outcome {
                Ok(fetched) => FetchOutcome::Answered {
                    catalog_digest: fetched.answer.catalog_digest.clone(),
                    tools: Arc::new(
                        fetched
                            .answer
                            .tools
                            .iter()
                            .map(|tool| session_tool(provider, tool))
                            .collect(),
                    ),
                    text: fetched.text.clone(),
                    reply: {
                        let replies: BTreeMap<_, _> = fetched
                            .answer
                            .tools
                            .iter()
                            .filter_map(|tool| {
                                tool.reply
                                    .as_ref()
                                    .map(|reply| (tool.name.clone(), freeze_reply(reply)))
                            })
                            .collect();
                        (!replies.is_empty()).then_some(replies)
                    },
                    reference_slots: frozen_reference_slots(plan, provider, fetched),
                    // Resolve the catalog's omitted-means-all default now. An absent frozen
                    // permission is deliberately ungranted on legacy/non-catalog tools.
                    result_ops: Some(
                        fetched
                            .answer
                            .tools
                            .iter()
                            .map(|tool| {
                                (
                                    tool.name.clone(),
                                    tool.effective_result_ops()
                                        .into_iter()
                                        .map(str::to_owned)
                                        .collect(),
                                )
                            })
                            .collect(),
                    ),
                    declarations_frozen: frozen_provider_declarations(&fetched.answer),
                },
                Err(missed) => FetchOutcome::Absent {
                    reason: missed.unanswered.absence(),
                },
            },
        })
        .collect();
    let mut role_versions = BTreeMap::new();
    role_versions.insert("tool-provider".to_owned(), "v1".to_owned());
    if plan.compaction_item.is_some() {
        role_versions.insert("compaction-provider".to_owned(), "v1".to_owned());
    }
    if !plan.step_items.is_empty() {
        role_versions.insert("step-transform-provider".to_owned(), "v1".to_owned());
    }
    Ok(Admitted {
        manifest: PlanManifest {
            rich_ui,
            declarations: frozen_declarations,
            plan: OpaqueJson::from_json_text(plan.text.clone())
                .expect("a plan serialized from a JSON value is JSON"),
            plan_identity: plan.identity.clone(),
            composition_digest: admitted_plan.composition_digest.clone(),
            fetches,
            tools: Arc::new(tools),
            system_digest: sha256_hex(system_text.as_bytes()),
            system_text,
            join_version: JOIN_VERSION,
            role_versions,
        },
    })
}

/// The `session.baseline` facts a manifest froze, read from the manifest alone so the admission
/// reply and every later `session.baseline` answer the same.
#[derive(Debug, Clone, PartialEq)]
pub struct BaselineFacts {
    /// One per answered provider, its item digest being the provider's catalog digest, which
    /// covers its system text too.
    pub items: Vec<BaselineItem>,
    /// One per absent tool or system-text item, including a removed tool provider's text.
    pub absent_items: Vec<AbsentItem>,
    pub composition_digest: String,
    pub tool_names: Vec<String>,
}

/// The [`BaselineFacts`] of `manifest`.
pub fn baseline_facts(manifest: &PlanManifest) -> BaselineFacts {
    let mut items = Vec::new();
    let mut absent_items = Vec::new();
    let plan: Value = serde_json::from_str(manifest.plan.as_json_text()).expect("frozen JSON plan");
    for fetch in &manifest.fetches {
        match &fetch.outcome {
            FetchOutcome::Answered { catalog_digest, .. } => {
                items.push(BaselineItem::new(&fetch.provider, catalog_digest))
            }
            FetchOutcome::Absent { reason } => {
                let (reason, provider_code) = match Unanswered::from_absence(reason) {
                    crate::refusal::AbsenceReason::Known(unanswered) => (
                        unanswered.reason().to_owned(),
                        unanswered.provider_code().map(str::to_owned),
                    ),
                    crate::refusal::AbsenceReason::Other(reason) => (reason, None),
                };
                for (list, kind) in [
                    ("tool_items", ABSENT_KIND_TOOL),
                    ("system_text_items", ABSENT_KIND_SYSTEM_TEXT),
                ] {
                    if plan[list].as_array().is_some_and(|items| {
                        items.iter().any(|item| item["provider"] == fetch.provider)
                    }) {
                        absent_items.push(AbsentItem {
                            provider: fetch.provider.clone(),
                            kind: kind.to_owned(),
                            reason: reason.clone(),
                            provider_code: provider_code.clone(),
                        });
                    }
                }
            }
            FetchOutcome::Unknown => {}
        }
    }
    BaselineFacts {
        items,
        absent_items,
        composition_digest: manifest.composition_digest.clone(),
        tool_names: manifest
            .tools
            .iter()
            .map(|tool| tool.name.clone())
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::plan::parse_plan;
    use async_trait::async_trait;

    struct ScriptedFetcher {
        catalog_result: Result<Value, FetchError>,
        declaration_result: Result<Value, FetchError>,
    }

    #[async_trait]
    impl Fetcher for ScriptedFetcher {
        async fn catalog(
            &self,
            _provider: &str,
            _request: &CatalogRequest,
        ) -> Result<Value, FetchError> {
            self.catalog_result.clone()
        }

        async fn declare(
            &self,
            _provider: &str,
            _request: &DeclareRequest,
        ) -> Result<Value, FetchError> {
            self.declaration_result.clone()
        }

        async fn describe_compaction(&self, _provider: &str) -> Result<DescribeAnswer, FetchError> {
            panic!("these plans carry no compaction item")
        }
    }

    struct Preflight {
        answer: Result<Value, FetchError>,
        requests: std::sync::Mutex<Vec<Value>>,
    }

    #[async_trait]
    impl Fetcher for Preflight {
        async fn catalog(
            &self,
            provider: &str,
            request: &CatalogRequest,
        ) -> Result<Value, FetchError> {
            assert_eq!(provider, "aft");
            self.requests
                .lock()
                .unwrap()
                .push(serde_json::to_value(request).unwrap());
            self.answer.clone()
        }
        async fn declare(&self, _: &str, _: &DeclareRequest) -> Result<Value, FetchError> {
            panic!("tool-only composition must not declare transforms")
        }
        async fn describe_compaction(&self, _: &str) -> Result<DescribeAnswer, FetchError> {
            panic!("tool-only composition must not describe a compactor")
        }
    }

    fn composition_items() -> Value {
        json!({"tool_items": [{"provider": "aft", "preset": "reader", "params": {"scope": "read"}}],
               "system_text_items": [], "step_transform_items": []})
    }

    fn preflight_answer() -> Value {
        json!({"generation": "live", "catalog_digest": "opaque", "tools": [
            {"name": "zoom", "description": "Zoom", "input_schema": {}, "schema_digest": "z",
             "semantics": 1, "capabilities": ["z/v1", "a/v1"]},
            {"name": "read", "description": "Read", "input_schema": {}, "schema_digest": "r",
             "semantics": 1, "capabilities": ["code.read/v1"]}
        ]})
    }

    #[tokio::test]
    async fn composition_preflights_live_tools_and_canonical_digest() {
        let fetcher = Preflight {
            answer: Ok(preflight_answer()),
            requests: Default::default(),
        };
        let plan = compose(&composition_items(), &fetcher, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(
            *fetcher.requests.lock().unwrap(),
            vec![json!({"preset": "reader", "params": {"scope": "read"}})]
        );
        assert_eq!(plan["tool_items"], composition_items()["tool_items"]);
        assert_eq!(
            plan["composition"],
            json!({"providers": [{"provider": "aft", "tools": [
            {"name": "read", "capabilities": ["code.read/v1"]},
            {"name": "zoom", "capabilities": ["a/v1", "z/v1"]}
        ]}], "step_transform_items": []})
        );
        assert_eq!(
            plan["composition_digest"],
            "9881838597e87f35274e5d544969fb8f626aeee745ea81057a2f03909bf25fd8"
        );
    }

    #[tokio::test]
    async fn composition_refuses_invalid_items_before_fetching() {
        let fetcher = Preflight {
            answer: Ok(preflight_answer()),
            requests: Default::default(),
        };
        for (field, value) in [
            ("composition", json!({})),
            ("composition_digest", json!("old")),
            ("system_text_items", json!([{"provider": "persona"}])),
            ("step_transform_items", json!([{"provider": "transform"}])),
            ("compaction_item", json!({"provider": "compactor"})),
            ("tool_items", json!([{"provider": "aft", "params": []}])),
        ] {
            let mut items = composition_items();
            items[field] = value;
            assert_eq!(
                compose(&items, &fetcher, Duration::from_secs(1))
                    .await
                    .unwrap_err()
                    .code(),
                "invalid_params"
            );
        }
        assert!(fetcher.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn composition_fetch_failures_and_invalid_answers_name_the_provider() {
        let mut answered_composition = preflight_answer();
        answered_composition["composition_digest"] = json!("not-a-preflight");
        for answer in [
            Err(FetchError::UnknownModule),
            Ok(json!({"tools": "not-an-array"})),
            Ok(answered_composition),
        ] {
            let fetcher = Preflight {
                answer,
                requests: Default::default(),
            };
            let refusal = compose(&composition_items(), &fetcher, Duration::from_secs(1))
                .await
                .unwrap_err();
            assert_eq!(refusal.code(), "fetch_unavailable");
            assert_eq!(refusal.detail()["provider"], "aft");
        }
    }

    async fn capture_admission_logs<F: std::future::Future>(future: F) -> (F::Output, Vec<String>) {
        let (output, events) = broca_test_support::capture_logs(future).await;
        let logs = events
            .into_iter()
            .filter(|event| event.target == "admission" && event.level == tracing::Level::WARN)
            .map(|event| event.message().to_owned())
            .collect();
        (output, logs)
    }

    fn plan_for(provider: &str) -> Plan {
        let composition = json!({
            "providers": [{"provider": provider, "tools": []}]
        });
        let digest = cortexkit_role_tool_provider::catalog::composition_digest(&composition)
            .expect("composition digest");
        parse_plan(
            &json!({
                "tool_items": [],
                "system_text_items": [{
                    "provider": provider,
                    "preset": "persona",
                    "params": {}
                }],
                "step_transform_items": [],
                "composition": composition,
                "composition_digest": digest
            }),
            crate::plan::CompactionSupport::Unsupported,
        )
        .expect("valid test plan")
    }

    fn plan_with_declaration(provider: &str) -> Plan {
        Plan {
            render_tags: false,
            text: "{}".into(),
            identity: "test-plan".into(),
            tool_items: Vec::new(),
            system_text_items: Vec::new(),
            compaction_item: None,
            step_items: vec![crate::plan::StepItem {
                item: Item {
                    provider: provider.into(),
                    preset: Some("worker".into()),
                    params: serde_json::Map::new(),
                    optional: false,
                    output_refs: false,
                },
                subscriptions: Vec::new(),
            }],
            composition: serde_json::Map::new(),
            composition_digest: String::new(),
            composed: BTreeMap::new(),
            senders: Vec::new(),
        }
    }

    fn catalog_answer(plan: &Plan, text: Option<&str>, item_digest: &str) -> Value {
        let mut system_text = json!({
            "item_digest": item_digest,
            "preflight_digest": "preflight",
            "composition_digest": plan.composition_digest,
            "tool_names": []
        });
        if let Some(text) = text {
            system_text["text"] = json!(text);
        }
        json!({
            "generation": "1",
            "catalog_digest": "catalog",
            "composition_digest": plan.composition_digest,
            "tools": [],
            "system_text": system_text
        })
    }

    struct StampedFetcher(BTreeMap<String, Value>);

    #[async_trait]
    impl Fetcher for StampedFetcher {
        async fn catalog(&self, provider: &str, _: &CatalogRequest) -> Result<Value, FetchError> {
            Ok(self.0[provider].clone())
        }
        async fn declare(&self, _: &str, _: &DeclareRequest) -> Result<Value, FetchError> {
            unreachable!("no transforms")
        }
        async fn describe_compaction(&self, _: &str) -> Result<DescribeAnswer, FetchError> {
            unreachable!("no compaction")
        }
    }

    #[tokio::test]
    async fn rich_ui_stamp_freezes_only_the_teaching_answer_and_refuses_conflicts_in_plan_order() {
        let mut value: Value = serde_json::from_str(&plan_for("z-teacher").text).unwrap();
        value["system_text_items"]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "provider":"a-teacher", "preset":"persona", "params":{}
            }));
        value["composition"]["providers"]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "provider":"a-teacher", "tools":[]
            }));
        value["composition"]["providers"]
            .as_array_mut()
            .unwrap()
            .reverse();
        value["composition_digest"] = json!(
            cortexkit_role_tool_provider::catalog::composition_digest(&value["composition"])
                .unwrap()
        );
        let stamp = json!({"dialect":"1.0", "catalog_digest":"a".repeat(64)});
        let other = json!({"dialect":"2.0", "catalog_digest":"b".repeat(64)});
        value["rich_ui"] = other.clone();
        let plan = parse_plan(&value, crate::plan::CompactionSupport::Unsupported).unwrap();
        let text_digest = cortexkit_role_tool_provider::catalog::system_text_digest("teach");
        let mut answer = catalog_answer(&plan, Some("teach"), &text_digest);
        answer["system_text"]["rich_ui"] = stamp.clone();
        let mut fetcher = StampedFetcher(BTreeMap::from([
            ("z-teacher".into(), answer.clone()),
            ("a-teacher".into(), answer),
        ]));
        let admitted = admit(
            &plan,
            &fetcher,
            &broca_protocol::BindIdentity::new("/synthetic/stamps", "runner", "stamped"),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(
            serde_json::to_value(admitted.manifest.rich_ui).unwrap(),
            stamp
        );
        fetcher.0.get_mut("a-teacher").unwrap()["system_text"]["rich_ui"] = other.clone();
        let refusal = admit(
            &plan,
            &fetcher,
            &broca_protocol::BindIdentity::new("/synthetic/stamps", "runner", "conflict"),
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert_eq!(refusal.code(), "plan_stale");
        assert_eq!(
            refusal.detail(),
            json!({"differences":[
                {"kind":"rich_ui", "provider":"z-teacher", "rich_ui":stamp},
                {"kind":"rich_ui", "provider":"a-teacher", "rich_ui":other}
            ]})
        );
        for answer in fetcher.0.values_mut() {
            answer["system_text"]
                .as_object_mut()
                .unwrap()
                .remove("rich_ui");
        }
        let unstamped = admit(
            &plan,
            &fetcher,
            &broca_protocol::BindIdentity::new("/synthetic/stamps", "runner", "unstamped"),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert!(unstamped.manifest.rich_ui.is_none());
        assert!(serde_json::to_value(unstamped.manifest)
            .unwrap()
            .get("rich_ui")
            .is_none());
    }

    #[test]
    fn catalog_schema_pin_is_frozen_instead_of_recomputed_from_a_later_catalog() {
        let digest = "634bdb1fe768fce0974f9c89e6f15f889470a8288277c6433e4711ab6a5b9335";
        let mut answer: cortexkit_role_tool_provider::catalog::CatalogAnswer = serde_json::from_value(json!({
            "generation":"one", "catalog_digest":"one", "tools":[{
                "name":"work", "input_schema":{"type":"object"}, "schema_digest":digest, "semantics":7, "capabilities":[]
            }]
        })).unwrap();
        let frozen = frozen_provider_declarations(&answer).unwrap();
        answer.tools[0].schema_digest = "0".repeat(64);
        answer.tools[0].semantics = 99;
        let round_trip: FrozenProviderDeclarations =
            serde_json::from_slice(&serde_json::to_vec(&frozen).unwrap()).unwrap();
        let pin = &round_trip.schema_pins.as_ref().unwrap()["work"];
        assert_eq!(pin.schema_digest, digest);
        assert_eq!(pin.semantics, 7);
        assert_eq!(
            cortexkit_role_tool_provider::call::SchemaPin::new(
                "work",
                &pin.schema_digest,
                pin.semantics
            )
            .encode()
            .unwrap(),
            format!("tp1:work:{digest}:7")
        );
        assert!(FrozenProviderDeclarations::default().schema_pins.is_none());
    }

    struct DefaultTextFetcher {
        supported: bool,
        answer: Value,
        requests: std::sync::Mutex<Vec<Value>>,
    }

    #[async_trait]
    impl Fetcher for DefaultTextFetcher {
        async fn system_text_default_supported(&self, _: &str) -> Result<bool, FetchError> {
            Ok(self.supported)
        }
        async fn catalog(&self, _: &str, request: &CatalogRequest) -> Result<Value, FetchError> {
            self.requests
                .lock()
                .unwrap()
                .push(serde_json::to_value(request).unwrap());
            Ok(self.answer.clone())
        }
        async fn declare(&self, _: &str, _: &DeclareRequest) -> Result<Value, FetchError> {
            unreachable!()
        }
        async fn describe_compaction(&self, _: &str) -> Result<DescribeAnswer, FetchError> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn presetless_system_text_is_catalog_gated_and_frozen() {
        let composition =
            json!({"providers":[{"provider":"persona","tools":[]}],"step_transform_items":[]});
        let raw = json!({"tool_items":[],"system_text_items":[{"provider":"persona","params":{}}],"step_transform_items":[],
            "composition_digest":cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),"composition":composition});
        let plan = crate::parse_plan(&raw, crate::CompactionSupport::Unsupported).unwrap();
        let text = "frozen provider default";
        let session =
            broca_protocol::BindIdentity::new("/synthetic/default-text", "runner", "default-text");
        for supported in [false, true] {
            let mut fetcher = DefaultTextFetcher {
                supported,
                answer: catalog_answer(
                    &plan,
                    Some(text),
                    &cortexkit_role_tool_provider::catalog::system_text_digest(text),
                ),
                requests: Default::default(),
            };
            let result = admit(&plan, &fetcher, &session, Duration::from_secs(1)).await;
            if !supported {
                let refusal = result.unwrap_err();
                assert_eq!(refusal.code(), "system_text_preset_required");
                assert_eq!(refusal.detail(), json!({"provider":"persona"}));
                assert!(!refusal.retryable());
                assert!(fetcher.requests.lock().unwrap().is_empty());
            } else {
                let admitted = result.unwrap();
                let requests = fetcher.requests.lock().unwrap();
                assert_eq!(requests.len(), 1);
                assert!(requests[0]["system_text"].get("preset").is_none());
                assert_eq!(admitted.manifest.system_text, text);
                fetcher.answer["system_text"]["text"] = json!("later catalog text");
                let stored = serde_json::to_vec(&admitted.manifest).unwrap();
                let restored: broca_wal::PlanManifest = serde_json::from_slice(&stored).unwrap();
                assert_eq!(restored.system_text, text);
                assert_eq!(requests.len(), 1);
            }
        }
    }

    async fn run_fetch(plan: &Plan, answer: Result<Value, FetchError>, session: &str) {
        let fetcher = ScriptedFetcher {
            catalog_result: answer,
            declaration_result: Ok(Value::Null),
        };
        let refusal = admit(
            plan,
            &fetcher,
            &broca_protocol::BindIdentity::new("/tmp/log-test", "runner", session),
            Duration::from_millis(100),
        )
        .await
        .expect_err("the scripted fetch is unanswered");
        assert_eq!(
            refusal.code(),
            cortexkit_role_llm_runner::errors::FETCH_UNAVAILABLE
        );
    }

    async fn run_declare(plan: &Plan, answer: Result<Value, FetchError>, session: &str) {
        let fetcher = ScriptedFetcher {
            catalog_result: Ok(Value::Null),
            declaration_result: answer,
        };
        let refusal = admit(
            plan,
            &fetcher,
            &broca_protocol::BindIdentity::new("/tmp/log-test", "runner", session),
            Duration::from_millis(100),
        )
        .await
        .expect_err("the scripted declaration is unanswered");
        assert_eq!(
            refusal.code(),
            cortexkit_role_llm_runner::errors::FETCH_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn declaration_provider_refusal_reaches_fetch_unavailable_detail() {
        let plan = plan_with_declaration("magic-context");
        let fetcher = ScriptedFetcher {
            catalog_result: Ok(Value::Null),
            declaration_result: Err(FetchError::provider_refused(
                "invalid_request".into(),
                Some(json!({"field": "step_transform_items[0].preset"})),
                "the declaration preset is not available",
                false,
            )),
        };
        let refusal = admit(
            &plan,
            &fetcher,
            &broca_protocol::BindIdentity::new("/tmp/log-test", "runner", "declare-refusal"),
            Duration::from_millis(100),
        )
        .await
        .expect_err("the provider refusal prevents admission");

        assert_eq!(refusal.code(), "fetch_unavailable");
        assert_eq!(
            refusal.message(),
            "provider magic-context refused the fetch: invalid_request"
        );
        assert_eq!(
            refusal.detail(),
            json!({
                "provider": "magic-context",
                "reason": "refused",
                "provider_code": "invalid_request",
                "op": DECLARE_OP,
                "message": "the declaration preset is not available",
                "field": "step_transform_items[0].preset",
                "retryable": false,
            })
        );
    }

    #[tokio::test]
    async fn provider_refused_declared_preset_stays_plan_stale() {
        let plan = plan_with_declaration("magic-context");
        let fetcher = ScriptedFetcher {
            catalog_result: Ok(Value::Null),
            declaration_result: Err(FetchError::provider_refused(
                "invalid_params".into(),
                Some(json!({"field": "preset"})),
                "the declaration preset is no longer available",
                false,
            )),
        };
        let refusal = admit(
            &plan,
            &fetcher,
            &broca_protocol::BindIdentity::new("/tmp/log-test", "runner", "preset-stale"),
            Duration::from_millis(100),
        )
        .await
        .expect_err("the provider no longer knows the planned preset");

        assert_eq!(refusal.code(), "plan_stale");
        assert_eq!(
            refusal.detail(),
            json!({"differences":[{
                "kind":"preset_missing",
                "provider":"magic-context",
                "preset":"worker"
            }]})
        );
    }

    struct DefaultDeclaration(Result<Value, FetchError>);
    #[async_trait::async_trait]
    impl Fetcher for DefaultDeclaration {
        async fn catalog(&self, _: &str, _: &CatalogRequest) -> Result<Value, FetchError> {
            panic!("a step-only plan fetches no catalogs")
        }
        async fn declare(&self, _: &str, request: &DeclareRequest) -> Result<Value, FetchError> {
            assert!(serde_json::to_value(request)
                .unwrap()
                .get("preset")
                .is_none());
            self.0.clone()
        }
        async fn describe_compaction(&self, _: &str) -> Result<DescribeAnswer, FetchError> {
            panic!("a step-only plan names no compaction provider")
        }
    }

    #[tokio::test]
    async fn default_declaration_omits_preset_and_a_provider_without_a_default_is_invalid_params() {
        let mut plan = plan_with_declaration("magic-context");
        plan.step_items[0].item.preset = None;
        let session =
            broca_protocol::BindIdentity::new("/tmp/default-declaration", "runner", "session");
        admit(
            &plan,
            &DefaultDeclaration(Ok(json!({"subscriptions":[]}))),
            &session,
            ADMISSION_DEADLINE,
        )
        .await
        .unwrap();
        let refuse = |code: &str| {
            DefaultDeclaration(Err(FetchError::provider_refused(
                code.into(),
                Some(json!({"field":"preset"})),
                "no default selector",
                false,
            )))
        };
        // step-transform-provider 0.4.1: the provider's `invalid_params {field: "preset"}` for
        // a presetless item means it has no default. That is a planner error, refused exactly
        // `invalid_params {field: "preset"}`, never `plan_stale` and never a fetch failure.
        let refusal = admit(
            &plan,
            &refuse("invalid_params"),
            &session,
            ADMISSION_DEADLINE,
        )
        .await
        .unwrap_err();
        assert_eq!(refusal.code(), "invalid_params");
        assert_eq!(refusal.detail(), json!({"field":"preset"}));
        // Any other refusal code is the provider's own failure, not the role's no-default
        // answer, so admission reports it as unanswered with the provider's detail intact.
        let refusal = admit(
            &plan,
            &refuse("invalid_request"),
            &session,
            ADMISSION_DEADLINE,
        )
        .await
        .unwrap_err();
        assert_eq!(refusal.code(), "fetch_unavailable");
        assert_eq!(
            refusal.detail(),
            json!({"provider":"magic-context","reason":"refused","provider_code":"invalid_request",
            "op":DECLARE_OP,"message":"no default selector","field":"preset","retryable":false})
        );
        assert!(!refusal.detail().to_string().contains("preset_missing"));
    }

    #[tokio::test]
    async fn default_declaration_rechecks_reduction_and_subscription_bounds() {
        let mut plan = plan_with_declaration("other-context");
        plan.step_items[0].item.preset = None;
        plan.step_items[0].subscriptions = vec![serde_json::from_value(json!({
            "hook":"pre_user","ops":["replace"],"budget_ms":1000,"on_unavailable":"pass"
        }))
        .unwrap()];
        let session = broca_protocol::BindIdentity::new("/tmp/default-bounds", "runner", "session");
        let empty = DefaultDeclaration(Ok(json!({"subscriptions":[]})));
        let refusal = admit(&plan, &empty, &session, ADMISSION_DEADLINE)
            .await
            .unwrap_err();
        assert_eq!(
            refusal.detail(),
            json!({"field":"plan.step_transform_items","item":0,"subscription":0,"problem":"replace_not_reduction_owner"})
        );
        plan.step_items[0].subscriptions[0] = serde_json::from_value(json!({
            "hook":"pre_user","ops":["append"],"budget_ms":1000,"on_unavailable":"pass"
        }))
        .unwrap();
        let refusal = admit(&plan, &empty, &session, ADMISSION_DEADLINE)
            .await
            .unwrap_err();
        assert_eq!(refusal.code(), "plan_stale");
        assert_eq!(
            refusal.detail(),
            json!({"differences":[{"kind":"subscription_missing","provider":"other-context","hook":"pre_user","phase":null}]})
        );
    }

    #[tokio::test]
    async fn declaration_request_preserves_pre_bump_json_bytes() {
        struct Capture {
            requests: Mutex<Vec<Vec<u8>>>,
        }

        #[async_trait]
        impl Fetcher for Capture {
            async fn catalog(
                &self,
                _provider: &str,
                _request: &CatalogRequest,
            ) -> Result<Value, FetchError> {
                panic!("a step-only plan must not fetch a catalog");
            }

            async fn declare(
                &self,
                provider: &str,
                request: &DeclareRequest,
            ) -> Result<Value, FetchError> {
                assert_eq!(provider, "magic-context");
                self.requests
                    .lock()
                    .unwrap()
                    .push(serde_json::to_vec(request).unwrap());
                Ok(json!({"subscriptions": []}))
            }

            async fn describe_compaction(
                &self,
                _provider: &str,
            ) -> Result<DescribeAnswer, FetchError> {
                panic!("a plan without a compaction item must not describe one")
            }
        }

        let mut plan = plan_with_declaration("magic-context");
        plan.step_items[0].item.params = json!({"tags": "on"}).as_object().unwrap().clone();
        plan.composition = json!({
            "providers": [{"provider": "aft", "tools": ["read"]}],
            "step_transform_items": [{"provider": "magic-context"}]
        })
        .as_object()
        .unwrap()
        .clone();
        let fetcher = Capture {
            requests: Mutex::new(Vec::new()),
        };
        admit(
            &plan,
            &fetcher,
            &broca_protocol::BindIdentity::new("/tmp/declare-test", "runner", "session"),
            Duration::from_millis(100),
        )
        .await
        .expect("the provider's empty declaration covers the empty subscriptions");

        // Pin the struct-literal request's bytes so migrating to the shared
        // constructor cannot silently drop the item's config or composition.
        let expected = br#"{"preset":"worker","params":{"tags":"on"},"composition":{"providers":[{"provider":"aft","tools":["read"]}],"step_transform_items":[{"provider":"magic-context"}]}}"#;
        assert_eq!(*fetcher.requests.lock().unwrap(), vec![expected.to_vec()]);
    }

    #[tokio::test]
    async fn unanswered_provider_fetch_logs_only_safe_diagnostics() {
        const TEXT_MARKER: &str = "UNIQUE_PROMPT_TEXT_MUST_NOT_REACH_LOGS_7e2c";

        let (results, lines) = capture_admission_logs(async {
            let present = plan_for("text-present");
            let computed = "11e2b7d02217997771deae57eccbfb56e60e42dfd654eead61388369e55fe82e";
            run_fetch(
                &present,
                Ok(catalog_answer(
                    &present,
                    Some(TEXT_MARKER),
                    "answered-digest",
                )),
                "session-present",
            )
            .await;

            let absent = plan_for("text-absent");
            run_fetch(
                &absent,
                Ok(catalog_answer(&absent, None, "answered-empty-digest")),
                "session-absent",
            )
            .await;

            let refused = plan_with_declaration("refusing");
            run_declare(
                &refused,
                Err(FetchError::Refused {
                    code: "provider_busy".into(),
                    detail: None,
                    retryable: true,
                }),
                "session-refused",
            )
            .await;

            let refused_catalog = plan_for("catalog-refusing");
            let refused_plan_identity = refused_catalog.identity.clone();
            run_fetch(
                &refused_catalog,
                Err(FetchError::provider_refused(
                    "invalid_request".into(),
                    Some(json!({"field": "preset"})),
                    "The requested preset is unexpected",
                    false,
                )),
                "session-catalog-refused",
            )
            .await;

            let invalid = plan_for("invalid");
            let invalid_answer = json!({
                "generation": "1",
                "catalog_digest": "catalog",
                "composition_digest": invalid.composition_digest,
                "tools": [TEXT_MARKER]
            });
            run_fetch(&invalid, Ok(invalid_answer), "session-invalid").await;
            (
                computed.to_owned(),
                TEXT_MARKER.len(),
                refused_plan_identity,
            )
        })
        .await;

        assert_eq!(lines.len(), 5, "one warning per unanswered provider fetch");
        let present = lines
            .iter()
            .find(|line| line.contains("provider=text-present"))
            .unwrap();
        assert!(present.contains("session=session-present"));
        assert!(present.contains("op=tool.catalog"));
        assert!(present.contains("code=item_digest_mismatch"));
        assert!(present.contains("item_digest=answered-digest"));
        assert!(present.contains(&format!("computed_sha256={}", results.0)));
        assert!(present.contains("text_present=true"));
        assert!(present.contains(&format!("text_bytes={}", results.1)));

        let absent = lines
            .iter()
            .find(|line| line.contains("provider=text-absent"))
            .unwrap();
        assert!(absent.contains("session=session-absent"));
        assert!(absent.contains("op=tool.catalog"));
        assert!(absent.contains("code=item_digest_mismatch"));
        assert!(absent.contains("item_digest=answered-empty-digest"));
        assert!(absent.contains(
            "computed_sha256=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        ));
        assert!(absent.contains("text_present=false"));
        assert!(absent.contains("text_bytes=0"));

        let refused = lines
            .iter()
            .find(|line| line.contains("provider=refusing"))
            .unwrap();
        assert!(refused.contains("session=session-refused"));
        assert!(refused.contains("op=transform.declare"));
        assert!(refused.contains("code=refused"));
        assert!(refused.contains("provider_code=provider_busy"));
        assert!(refused.contains("retryable=true"));

        let catalog_refused = lines
            .iter()
            .find(|line| line.contains("provider=catalog-refusing"))
            .unwrap();
        assert!(catalog_refused.contains("session=session-catalog-refused"));
        assert!(catalog_refused.contains(&format!("plan_identity={}", results.2)));
        assert!(catalog_refused.contains("op=tool.catalog"));
        assert!(catalog_refused.contains("provider_code=invalid_request"));
        assert!(catalog_refused
            .contains("refusal_message=Some(\"The requested preset is unexpected\")"));
        assert!(catalog_refused.contains("refusal_field=Some(\"preset\")"));

        let invalid = lines
            .iter()
            .find(|line| line.contains("provider=invalid"))
            .unwrap();
        assert!(invalid.contains("session=session-invalid"));
        assert!(invalid.contains("op=tool.catalog"));
        assert!(invalid.contains("code=invalid_answer"));
        assert!(invalid.contains("serde_error_kind=data"));

        assert!(lines.iter().all(|line| !line.contains(TEXT_MARKER)));
    }

    #[test]
    fn provider_refusal_wire_detail_is_bounded_and_control_free() {
        let message = format!("{}\u{1b}x", "é".repeat(257));
        let field = format!("system_text.{}\u{0}", "f".repeat(600));
        let miss = FetchMiss::from_error(FetchError::provider_refused(
            "invalid_request".into(),
            Some(json!({"field": field})),
            &message,
            false,
        ));

        assert_eq!(
            miss.provider_detail(CATALOG_OP),
            Some(json!({
                "op": "tool.catalog",
                "provider_code": "invalid_request",
                "retryable": false,
                "message": "é".repeat(256),
                "message_truncated": true,
                "field": format!("system_text.{}", "f".repeat(500)),
                "field_truncated": true,
            }))
        );
    }

    #[test]
    fn invalid_answer_diagnostic_reaches_wire_detail() {
        let miss = FetchMiss::invalid_answer(Some("invalid_type"));
        assert_eq!(
            miss.provider_detail(DECLARE_OP),
            Some(json!({
                "op": DECLARE_OP,
                "provider_code": INVALID_ANSWER,
                "retryable": false,
                "serde_error_kind": "invalid_type",
            }))
        );
    }

    #[tokio::test]
    async fn provider_refusal_diagnostic_strings_are_truncated_to_512_bytes() {
        let plan = plan_for("long-refusal");
        let message = "é".repeat(257);
        let field = "f".repeat(513);
        let error = FetchError::provider_refused(
            "invalid_request".into(),
            Some(json!({"field": field.clone()})),
            &message,
            false,
        );
        let (_, lines) = capture_admission_logs(run_fetch(&plan, Err(error), "session-long")).await;

        assert_eq!(lines.len(), 1);
        let expected_message = "é".repeat(256);
        let expected_field = "f".repeat(512);
        assert!(
            lines[0].contains(&format!("refusal_message=Some(\"{expected_message}\")")),
            "message was not truncated on a UTF-8 boundary at 512 bytes: {}",
            lines[0]
        );
        assert!(
            !lines[0].contains(&format!("refusal_message=Some(\"{message}\")")),
            "the full refusal message reached the warning"
        );
        assert!(
            lines[0].contains(&format!("refusal_field=Some(\"{expected_field}\")")),
            "field was not truncated at 512 bytes: {}",
            lines[0]
        );
        assert!(
            !lines[0].contains(&format!("refusal_field=Some(\"{field}\")")),
            "the full refusal field reached the warning"
        );
    }
}
