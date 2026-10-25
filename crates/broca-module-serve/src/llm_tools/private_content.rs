//! Private-content admission for the model-tools classify route.
//!
//! Before this route calls a third-party provider it has to know whether the call may carry the
//! person's private content, and it has to be right the first time: a run that read the person's
//! Chrome must not reach a provider broca has no permission for. Two questions answer that.
//!
//! - *Marked?* Cerebellum says whether a scope read the person's Chrome, a desktop app or a site
//!   where they are signed in. Only a "marked" answer is cached, keyed by the full scope
//!   identity (owner, reference and epoch, plus the flow id for a flow), together with the
//!   source kinds a card would name. No answer, an error or a timeout counts as marked and is
//!   not cached, so the next call asks again.
//! - *Which providers?* An unmarked run may call any provider. A marked run may call local
//!   models and the providers on the operator's configured list. (A live grant will also admit
//!   one once grants are consulted; this code does not look at grants yet.) A flow never raises
//!   a card: its extra providers come from Basal's answer for the flow's approved version, a
//!   flow Basal has not approved is refused by name, and a marked flow asking for a provider
//!   outside its set is refused `provider_not_allowed`.
//!
//! This policy files no consent cards. A marked run asking for a provider it may not call ends
//! `not_started` with `detail.consent: "consent_required"` and writes nothing at all, and a
//! marked scope with no recognized source kind ends `private_content_unknown` because there is
//! no source a card could name.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use broca_protocol::SendScope;
use cortexkit_role_classifier::errors::{ErrorClass, Refusal, RefusalDetail};
use serde::Serialize;
use subc_protocol::Principal;

use crate::config::LlmToolsConfig;

/// `detail.consent` for a marked scope that may not call this provider. No consent card is
/// filed, so this is the call's end: nothing is written or held.
pub const CONSENT_REQUIRED: &str = "consent_required";

/// `detail.consent` for a scope broca counts as marked but cannot name a source for.
pub const PRIVATE_CONTENT_UNKNOWN: &str = "private_content_unknown";

/// The scope identity an answer belongs to. Owner, reference and epoch together: an answer for
/// one epoch says nothing about the next run under the same reference.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct ScopeIdentity {
    owner: String,
    scope_ref: String,
    scope_epoch: u64,
    flow_id: Option<String>,
}

impl ScopeIdentity {
    /// The stamp this route froze at admission. A call without a stamp never reaches this
    /// policy: admission refuses it with `scope_required` first.
    pub(super) fn of(scope: &SendScope) -> Option<Self> {
        let SendScope::Stamped(bind) = scope else {
            return None;
        };
        Some(Self {
            owner: bind.scope.owner.as_owner_string(),
            scope_ref: bind.scope.scope_ref.clone(),
            scope_epoch: bind.scope.scope_epoch,
            flow_id: bind.scope.flow_id.clone(),
        })
    }

    /// The daemon's owner principal, for the questions that name an owner.
    fn owner_principal(&self) -> Principal {
        owner_principal(&self.owner)
    }
}

/// The daemon principal a frozen scope names, from the stamp's owner spelling
/// (`reserved:<module>`, `direct`).
pub(super) fn owner_principal(owner: &str) -> Principal {
    match owner.strip_prefix("reserved:") {
        Some(module_id) => Principal::Reserved {
            module_id: module_id.to_owned(),
        },
        None if owner == "direct" => Principal::Direct,
        None => Principal::Unverified,
    }
}

/// One scope's cached answer: whether it is marked and which source kinds it named.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct MarkedScope {
    pub(super) marked: bool,
    pub(super) sources: Vec<broca_subc::SourceKind>,
}

impl MarkedScope {
    fn same_as(&self, other: &Self) -> bool {
        self.marked == other.marked && self.sources == other.sources
    }
}

/// The questions this policy asks, abstracted so tests drive the decision without a daemon.
#[async_trait::async_trait]
pub trait Asks: Send + Sync {
    /// Cerebellum's answer for the scope, or `None` when cerebellum did not answer.
    async fn marked(&self, owner: Principal, scope_ref: &str)
        -> Option<broca_subc::PrivateContent>;

    /// Basal's answer for the flow's approved version, or why there is none.
    async fn flow_providers(
        &self,
        owner: Principal,
        flow_id: &str,
    ) -> Result<broca_subc::FlowPolicy, broca_subc::FlowProvidersError>;
}

/// The real asks: cerebellum and Basal over broca's module connection.
pub struct SubcAsks {
    private_content: Arc<broca_subc::PrivateContentClient>,
    flow_providers: Arc<broca_subc::FlowProvidersClient>,
}

impl SubcAsks {
    pub fn new(
        private_content: Arc<broca_subc::PrivateContentClient>,
        flow_providers: Arc<broca_subc::FlowProvidersClient>,
    ) -> Self {
        Self {
            private_content,
            flow_providers,
        }
    }
}

#[async_trait::async_trait]
impl Asks for SubcAsks {
    async fn marked(
        &self,
        owner: Principal,
        scope_ref: &str,
    ) -> Option<broca_subc::PrivateContent> {
        match self.private_content.query(&owner, scope_ref, None).await {
            Ok(answer) => Some(answer),
            Err(error) => {
                // A question broca could not fully ask is answered in the only safe direction,
                // so an operator can tell a refused question from an unmarked scope.
                tracing::warn!(target: "llm_tools", scope = scope_ref,
                    "private_content.query did not answer ({error}); the scope counts as marked");
                None
            }
        }
    }

    async fn flow_providers(
        &self,
        owner: Principal,
        flow_id: &str,
    ) -> Result<broca_subc::FlowPolicy, broca_subc::FlowProvidersError> {
        self.flow_providers.query(&owner, flow_id).await
    }
}

/// The refusals only this policy returns. They use the same `{code, message, detail}` shape as
/// every other model-tool refusal; `detail.consent` is what a caller checks to tell a call that
/// needs the person's approval from one that failed.
#[derive(Serialize)]
struct ConsentRefusal<'a> {
    code: &'a str,
    message: String,
    detail: ConsentDetail<'a>,
}

#[derive(Serialize)]
struct ConsentDetail<'a> {
    class: ErrorClass,
    provider: &'a str,
    consent: &'a str,
}

/// The decisions and the cache they produce, for the life of one `LlmTools`.
pub(super) struct PrivateContent {
    config: LlmToolsConfig,
    asks: Arc<dyn Asks>,
    marked: Mutex<BTreeMap<ScopeIdentity, MarkedScope>>,
    flows: Mutex<BTreeMap<ScopeIdentity, Vec<String>>>,
}

impl PrivateContent {
    pub(super) fn new(config: LlmToolsConfig, asks: Arc<dyn Asks>) -> Self {
        Self {
            config,
            asks,
            marked: Mutex::new(BTreeMap::new()),
            flows: Mutex::new(BTreeMap::new()),
        }
    }

    /// Whether the call may reach `provider`. Runs once per admitted call, before any provider
    /// attempt, after replay and pricing.
    pub(super) async fn admit(&self, scope: &SendScope, provider: &str) -> Result<(), Vec<u8>> {
        let Some(identity) = ScopeIdentity::of(scope) else {
            // Admission refuses an unstamped scope before this point.
            return Err(encoded_refusal(
                "scope_required",
                "model tools require a daemon-stamped run or flow scope".into(),
                None,
            ));
        };
        if is_local(provider) || self.listed(provider) {
            return Ok(());
        }
        match identity.flow_id.clone() {
            Some(flow_id) => self.admit_flow(&identity, &flow_id, provider).await,
            None => self.admit_run(&identity, provider).await,
        }
    }

    fn listed(&self, provider: &str) -> bool {
        self.config
            .private_content_providers
            .iter()
            .any(|listed| listed == provider)
    }

    async fn admit_run(&self, identity: &ScopeIdentity, provider: &str) -> Result<(), Vec<u8>> {
        // A cached "marked" still counts as marked until cerebellum answers again; a cached
        // "unmarked" is never trusted, because a mark written between two calls must be seen.
        let marked = match self.cached(identity) {
            Some(marked) => marked,
            None => self.ask(identity).await,
        };
        if !marked.marked {
            return Ok(());
        }
        // A live grant would also admit here; this code does not consult grants yet.
        if marked.sources.is_empty() {
            return Err(consent_refusal(provider, PRIVATE_CONTENT_UNKNOWN));
        }
        Err(consent_refusal(provider, CONSENT_REQUIRED))
    }

    async fn admit_flow(
        &self,
        identity: &ScopeIdentity,
        flow_id: &str,
        provider: &str,
    ) -> Result<(), Vec<u8>> {
        let allowed = match self.cached_flow(identity) {
            Some(allowed) => allowed,
            None => match self
                .asks
                .flow_providers(identity.owner_principal(), flow_id)
                .await
            {
                Ok(broca_subc::FlowPolicy::Allowed(providers)) => {
                    self.flows
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .insert(identity.clone(), providers.clone());
                    providers
                }
                // Basal's refusal is the call's refusal. Nothing is written and nothing is
                // cached: a flow's approved version can change, and only a new question sees it.
                Ok(broca_subc::FlowPolicy::NotApproved { message }) => {
                    return Err(encoded_refusal(
                        broca_subc::flow_providers::FLOW_NOT_APPROVED,
                        message,
                        None,
                    ))
                }
                // No Basal, no such operation, or no answer: the base set only, not cached.
                Err(error) => {
                    tracing::info!(target: "llm_tools", flow = flow_id,
                        "flow providers unavailable ({error}); the flow may call the base set only");
                    Vec::new()
                }
            },
        };
        if allowed.iter().any(|listed| listed == provider) {
            return Ok(());
        }
        // A flow broca asked about is one cerebellum may have marked. An unmarked flow may call
        // any provider; Basal's list adds providers to the base set for a marked one.
        let marked = match self.cached(identity) {
            Some(marked) => marked,
            None => self.ask(identity).await,
        };
        if !marked.marked {
            return Ok(());
        }
        Err(encoded_refusal(
            "provider_not_allowed",
            format!("the flow's approved version does not allow the provider {provider}"),
            None,
        ))
    }

    /// Ask cerebellum and cache a marked answer. An unmarked answer, an error and a timeout all
    /// leave the cache alone, so a failed question is asked again on the next call.
    async fn ask(&self, identity: &ScopeIdentity) -> MarkedScope {
        let Some(answer) = self
            .asks
            .marked(identity.owner_principal(), &identity.scope_ref)
            .await
        else {
            // No answer counts as marked, and nothing is cached, so no source can be named.
            return MarkedScope {
                marked: true,
                sources: Vec::new(),
            };
        };
        let fresh = recognized(answer);
        if fresh.marked {
            self.remember(identity.clone(), fresh.clone());
        }
        fresh
    }

    /// Record a marked answer under the full scope identity. An unmarked answer never replaces
    /// a marked one, and an answer equal to the cached one changes nothing.
    fn remember(&self, identity: ScopeIdentity, scope: MarkedScope) {
        let mut marked = self.marked.lock().unwrap_or_else(|p| p.into_inner());
        let keep = marked
            .get(&identity)
            .is_some_and(|cached| cached.same_as(&scope) || (cached.marked && !scope.marked));
        if !keep {
            marked.insert(identity, scope);
        }
    }

    fn cached(&self, identity: &ScopeIdentity) -> Option<MarkedScope> {
        self.marked
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(identity)
            .filter(|scope| scope.marked)
            .cloned()
    }

    fn cached_flow(&self, identity: &ScopeIdentity) -> Option<Vec<String>> {
        self.flows
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(identity)
            .cloned()
    }
}

/// The answer, keeping only the source kinds the approved card text can name. An answer whose
/// kinds broca does not know leaves nothing for a card to show, so the call ends
/// `private_content_unknown` rather than with a card that names no source.
fn recognized(answer: broca_subc::PrivateContent) -> MarkedScope {
    MarkedScope {
        marked: answer.marked,
        sources: answer
            .sources
            .into_iter()
            .filter(|kind| !matches!(kind, broca_subc::SourceKind::Other(_)))
            .collect(),
    }
}

/// A local model runs on this machine, so private content never leaves it. These are the
/// provider ids broca's catalog gives models it serves locally.
fn is_local(provider: &str) -> bool {
    matches!(provider, "ollama" | "llama-cpp" | "local")
}

/// The refusal body for a call that ends before any provider attempt.
fn encoded_refusal(code: &str, message: String, consent: Option<(&str, &str)>) -> Vec<u8> {
    match consent {
        None => {
            let mut detail = RefusalDetail::new(ErrorClass::Permanent);
            detail.field = Some("model".into());
            let refusal = Refusal::new(code, message).with_detail(detail);
            serde_json::to_vec(&refusal).expect("a refusal encodes")
        }
        Some((provider, consent)) => serde_json::to_vec(&ConsentRefusal {
            code,
            message,
            detail: ConsentDetail {
                class: ErrorClass::Permanent,
                provider,
                consent,
            },
        })
        .expect("a consent refusal encodes"),
    }
}

fn consent_refusal(provider: &str, consent: &str) -> Vec<u8> {
    encoded_refusal(
        "not_started",
        format!("the provider {provider} needs the person's approval before this call may run"),
        Some((provider, consent)),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use serde_json::Value;

    use super::*;

    /// What the stub answers, and how often it was asked.
    #[derive(Default)]
    struct Stub {
        marked: Mutex<Option<broca_subc::PrivateContent>>,
        marked_fails: std::sync::atomic::AtomicBool,
        flow: Mutex<Option<Result<broca_subc::FlowPolicy, broca_subc::FlowProvidersError>>>,
        marked_asks: AtomicUsize,
        flow_asks: AtomicUsize,
    }

    impl Stub {
        fn answering(marked: broca_subc::PrivateContent) -> Arc<Self> {
            let stub = Arc::new(Self::default());
            *stub.marked.lock().unwrap() = Some(marked);
            stub
        }

        fn silent() -> Arc<Self> {
            let stub = Arc::new(Self::default());
            stub.marked_fails.store(true, Ordering::SeqCst);
            stub
        }

        fn answering_flows(self: &Arc<Self>, policy: broca_subc::FlowPolicy) -> Arc<Self> {
            *self.flow.lock().unwrap() = Some(Ok(policy));
            Arc::clone(self)
        }
    }

    #[async_trait::async_trait]
    impl Asks for Stub {
        async fn marked(
            &self,
            _owner: Principal,
            _scope_ref: &str,
        ) -> Option<broca_subc::PrivateContent> {
            self.marked_asks.fetch_add(1, Ordering::SeqCst);
            if self.marked_fails.load(Ordering::SeqCst) {
                return None;
            }
            self.marked.lock().unwrap().clone()
        }

        async fn flow_providers(
            &self,
            _owner: Principal,
            _flow_id: &str,
        ) -> Result<broca_subc::FlowPolicy, broca_subc::FlowProvidersError> {
            self.flow_asks.fetch_add(1, Ordering::SeqCst);
            self.flow.lock().unwrap().clone().unwrap_or(Err(
                broca_subc::FlowProvidersError::Unanswered("no answer".into()),
            ))
        }
    }

    fn policy(config: LlmToolsConfig, asks: Arc<Stub>) -> PrivateContent {
        PrivateContent::new(config, asks)
    }

    fn listed() -> LlmToolsConfig {
        LlmToolsConfig {
            advertise_tags: false,
            private_content_providers: vec!["anthropic".into(), "anthropic-platform".into()],
        }
    }

    fn run(epoch: u64) -> SendScope {
        SendScope::Stamped(broca_protocol::BindScope {
            scope: broca_protocol::SessionScope {
                owner: broca_protocol::BindPrincipal::Reserved {
                    module_id: "owner".into(),
                },
                scope_ref: "run".into(),
                scope_epoch: epoch,
                flow_id: None,
                sender: None,
                agent_id: Some("agent".into()),
                run_id: Some("run".into()),
            },
            owner_authorized: true,
        })
    }

    fn flow(epoch: u64) -> SendScope {
        SendScope::Stamped(broca_protocol::BindScope {
            scope: broca_protocol::SessionScope {
                owner: broca_protocol::BindPrincipal::Reserved {
                    module_id: "owner".into(),
                },
                scope_ref: "flow".into(),
                scope_epoch: epoch,
                flow_id: Some("tool-flow".into()),
                sender: None,
                agent_id: None,
                run_id: None,
            },
            owner_authorized: true,
        })
    }

    /// The cache key a scope produces.
    fn identity(scope: &SendScope) -> ScopeIdentity {
        ScopeIdentity::of(scope).expect("a stamped scope has an identity")
    }

    fn marked(kinds: &[&str]) -> broca_subc::PrivateContent {
        broca_subc::PrivateContent {
            marked: true,
            sources: kinds
                .iter()
                .map(|kind| match *kind {
                    "person_browser" => broca_subc::SourceKind::PersonBrowser,
                    "desktop_app" => broca_subc::SourceKind::DesktopApp,
                    "signed_in_site" => broca_subc::SourceKind::SignedInSite,
                    other => broca_subc::SourceKind::Other(other.into()),
                })
                .collect(),
        }
    }

    fn error_of(body: Vec<u8>) -> Value {
        serde_json::from_slice(&body).expect("a refusal is JSON")
    }

    fn refused(verdict: Result<(), Vec<u8>>) -> Value {
        match verdict {
            Ok(_) => panic!("the call must be refused"),
            Err(body) => error_of(body),
        }
    }

    fn admitted(verdict: Result<(), Vec<u8>>) {
        assert!(verdict.is_ok(), "the call must be admitted");
    }

    #[tokio::test]
    async fn an_unmarked_run_calls_any_provider() {
        let stub = Stub::answering(broca_subc::PrivateContent::unmarked());
        let policy = policy(LlmToolsConfig::default(), stub.clone());
        admitted(policy.admit(&run(1), "typesafe").await);
        assert_eq!(stub.marked_asks.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_marked_run_needs_approval_for_an_unlisted_provider_and_files_nothing() {
        let stub = Stub::answering(marked(&["person_browser"]));
        let policy = policy(listed(), stub.clone());
        let error = refused(policy.admit(&run(1), "typesafe").await);
        assert_eq!(error["code"], "not_started");
        assert_eq!(error["detail"]["consent"], CONSENT_REQUIRED);
        assert_eq!(error["detail"]["provider"], "typesafe");
        // The marker is cached, so the second call does not ask cerebellum again.
        refused(policy.admit(&run(1), "typesafe").await);
        assert_eq!(stub.marked_asks.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_marked_run_calls_local_models_and_the_configured_list() {
        let stub = Stub::answering(marked(&["desktop_app"]));
        let policy = policy(listed(), stub);
        admitted(policy.admit(&run(1), "ollama").await);
        admitted(policy.admit(&run(1), "anthropic").await);
    }

    #[tokio::test]
    async fn a_marked_answer_with_only_unknown_kinds_has_no_source_to_name() {
        let stub = Stub::answering(marked(&["future_source"]));
        let policy = policy(listed(), stub);
        let error = refused(policy.admit(&run(1), "typesafe").await);
        assert_eq!(error["detail"]["consent"], PRIVATE_CONTENT_UNKNOWN);
    }

    #[tokio::test]
    async fn no_answer_counts_as_marked_and_is_asked_again() {
        let stub = Stub::silent();
        let policy = policy(LlmToolsConfig::default(), stub.clone());
        for _ in 0..2 {
            let error = refused(policy.admit(&run(1), "typesafe").await);
            assert_eq!(error["detail"]["consent"], PRIVATE_CONTENT_UNKNOWN);
        }
        assert_eq!(stub.marked_asks.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn only_a_marked_answer_is_cached_and_the_cache_is_keyed_by_epoch() {
        let stub = Arc::new(Stub::default());
        *stub.marked.lock().unwrap() = Some(broca_subc::PrivateContent::unmarked());
        let policy = policy(listed(), stub.clone());
        admitted(policy.admit(&run(1), "typesafe").await);
        // Nothing is remembered: a cached answer would be served without asking again, and an
        // unmarked answer is never trusted as one.
        let after_arm = format!("{:?}", policy.marked.lock().unwrap());
        assert!(policy.marked.lock().unwrap().is_empty(), "{after_arm}");
        admitted(policy.admit(&run(1), "typesafe").await);
        assert_eq!(stub.marked_asks.load(Ordering::SeqCst), 2);

        // A new epoch asks again, and this marked answer is cached for that epoch.
        *stub.marked.lock().unwrap() = Some(marked(&["signed_in_site"]));
        refused(policy.admit(&run(2), "typesafe").await);
        assert!(policy
            .marked
            .lock()
            .unwrap()
            .contains_key(&identity(&run(2))));
        assert!(!policy
            .marked
            .lock()
            .unwrap()
            .contains_key(&identity(&run(1))));
        refused(policy.admit(&run(2), "typesafe").await);
        assert_eq!(stub.marked_asks.load(Ordering::SeqCst), 3);
        // The epoch after that is new work: the earlier mark does not apply to it.
        refused(policy.admit(&run(3), "typesafe").await);
        assert_eq!(stub.marked_asks.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn an_unmarked_flow_unknown_to_basal_is_served() {
        let stub = Stub::answering(broca_subc::PrivateContent::unmarked())
            .answering_flows(broca_subc::FlowPolicy::Allowed(Vec::new()));
        let policy = policy(listed(), stub.clone());
        admitted(policy.admit(&flow(1), "cloudflare-workers-ai").await);
        // Basal is asked once for the life of the flow's scope identity, and an unmarked flow
        // may then call a provider Basal never named.
        admitted(policy.admit(&flow(1), "typesafe").await);
        assert_eq!(stub.flow_asks.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn an_unapproved_flow_is_refused_by_name_and_nothing_is_cached() {
        let stub = Stub::answering(broca_subc::PrivateContent::unmarked()).answering_flows(
            broca_subc::FlowPolicy::NotApproved {
                message: "this flow has no approved version".into(),
            },
        );
        let policy = policy(listed(), stub.clone());
        for _ in 0..2 {
            let error = refused(policy.admit(&flow(1), "typesafe").await);
            assert_eq!(error["code"], "flow_not_approved");
        }
        assert_eq!(stub.flow_asks.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn an_absent_basal_operation_narrows_a_marked_flow_to_the_base_set() {
        // The stub serves no `flow.private_content_providers` at all, which is what an older
        // Basal or an unapproved one looks like here: the base set, and nothing cached.
        let stub = Stub::answering(marked(&["person_browser"]));
        let policy = policy(listed(), stub.clone());
        let error = refused(policy.admit(&flow(1), "typesafe").await);
        assert_eq!(error["code"], "provider_not_allowed");
        for _ in 0..2 {
            refused(policy.admit(&flow(2), "typesafe").await);
        }
        assert_eq!(stub.flow_asks.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn a_marked_flow_calls_the_base_set_and_basals_list() {
        let stub = Stub::answering(marked(&["person_browser"])).answering_flows(
            broca_subc::FlowPolicy::Allowed(vec!["cloudflare-workers-ai".into()]),
        );
        let policy = policy(listed(), stub.clone());
        admitted(policy.admit(&flow(1), "anthropic").await);
        admitted(policy.admit(&flow(1), "cloudflare-workers-ai").await);
        let error = refused(policy.admit(&flow(1), "typesafe").await);
        assert_eq!(error["code"], "provider_not_allowed");
        // Basal is asked once for the life of the flow's scope identity.
        assert_eq!(stub.flow_asks.load(Ordering::SeqCst), 1);
    }
}
