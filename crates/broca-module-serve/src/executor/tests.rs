use super::*;
use std::time::Duration;

/// The catalog the resolver reads is one process-wide slot, and libtest runs this binary's tests
/// on parallel threads. A test that installs its own catalog holds the catalog test lock for as
/// long as it keeps this guard, so another test's install can't replace that catalog between
/// the install and the lookups it answers. A test that installs again, a second revision, takes
/// the lock only once.
#[must_use = "the installed catalog is this test's only while the guard lives"]
struct TestCatalog {
    _gate: Option<std::sync::MutexGuard<'static, ()>>,
}

thread_local! {
    static CATALOGS_HELD: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn install_catalog(cache: Arc<broca_catalog::CatalogCache>) -> TestCatalog {
    let gate = (CATALOGS_HELD.get() == 0).then(|| {
        broca_catalog::runtime::test_catalog_lock()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    });
    CATALOGS_HELD.set(CATALOGS_HELD.get() + 1);
    broca_catalog::install_global_catalog_cache(cache);
    TestCatalog { _gate: gate }
}

impl Drop for TestCatalog {
    fn drop(&mut self) {
        CATALOGS_HELD.set(CATALOGS_HELD.get() - 1);
    }
}
include!("../provider_override_tests.rs");
include!("../recorded_resume_tests.rs");
include!("../store_run_tests.rs");
include!("../store_cache_evidence_tests.rs");
include!("../store_run_cancel_tests.rs");
include!("../tool_name_alias_tests.rs");
include!("../queued_service_tier_tests.rs");
include!("../service_tier_writer_tests.rs");
include!("../default_effort_head_tests.rs");
include!("../auth_hint_writer_tests.rs");
include!("../vault_request_tests.rs");
include!("../store_key_tests.rs");
include!("../role_plane_tests.rs");

include!("../credential_source_guard_tests.rs");
include!("../account_move_tests.rs");
mod signed_out {
    use super::*;
    include!("../signed_out_tests.rs");
    include!("../signed_out_audit_tests.rs");
    include!("../signed_out_reaudit_tests.rs");
}
include!("../fork_executor_tests.rs");

/// A new episode whose request has `inherit_frozen_cache` set (a steer or interrupt
/// that omitted `cache`) takes the cache policy and caller cache key frozen in the
/// session's latest render config, never a policy resolved from its own absent cache
/// request. A resume, a send that named its cache, and a session with no episode take
/// none.
#[test]
fn an_inheriting_episode_takes_the_frozen_cache_policy_and_key() {
    let policy = marker_policy(broca_provider::wire::CacheTtl::OneHour);
    let mut frozen = broca_provider::families::anthropic_frozen_config("claude-test");
    frozen.cache = Some(policy.clone());
    frozen.prompt_cache_key = Some("frozen-key".into());
    let mut req = drift_request(std::path::Path::new("/inherit-cache"));
    let config = broca_wal::RunConfig {
        account_route: None,
        global_system: None,
        route_decision_id: None,
        served_provider: None,
        account_limit_pause: false,
        turn_directives_v1: None,
        render_tags: false,
        computed_tags: None,
        model: req.model.clone(),
        tools: req.tools.clone().into(),
        tool_choice: req.tool_choice.clone(),
        generation: req.generation.clone(),
        response_format: Default::default(),
        provider_options: serde_json::Value::Null,
        cache_tiers: serde_json::Value::Null,
        context_epoch: 0,
        renderer_version: frozen.renderer_version,
        frozen_render_config: serde_json::to_value(&frozen).unwrap(),
        auth_selection: None,
        tools_were_omitted: false,
        budget: None,
        context_limit: None,
        work_class: None,
        variant: None,
        service_tier: None,
        stop_when: vec![],
        context_management: None,
    };
    req.inherit_frozen_cache = true;
    assert_eq!(
        inherited_cache(&req, true, Some(&config)),
        Some((Some(policy), Some("frozen-key".into())))
    );
    assert_eq!(
        inherited_cache(&req, false, Some(&config)),
        None,
        "a resume"
    );
    assert_eq!(inherited_cache(&req, true, None), None, "no episode");
    req.inherit_frozen_cache = false;
    assert_eq!(
        inherited_cache(&req, true, Some(&config)),
        None,
        "a named cache"
    );
}

fn marker_policy(ttl: broca_provider::wire::CacheTtl) -> broca_provider::wire::CachePolicy {
    broca_provider::wire::CachePolicy {
        version: 1,
        class: broca_provider::wire::CacheClass::Markers {
            syntax: broca_provider::wire::MarkerSyntax::AnthropicCacheControl,
            ttl,
            max_breakpoints: 4,
            lookback_blocks: 20,
            tail: None,
            omit_ttl: false,
        },
    }
}

fn a_tool() -> broca_protocol::request::Tool {
    broca_protocol::request::Tool {
        name: "read".into(),
        description: String::new(),
        input_schema: serde_json::json!({"type": "object"}),
        module: None,
        execution_mode: broca_protocol::request::ExecutionMode::Parallel,
        reply: None,
    }
}

#[test]
fn context_limit_precedence_is_caller_then_catalog_then_none() {
    assert_eq!(
        context_limit_for_episode(true, Some(64_000), Some(128_000), None),
        Some(64_000),
        "a caller override wins over the catalog"
    );
    assert_eq!(
        context_limit_for_episode(true, None, Some(128_000), None),
        Some(128_000),
        "an omitted override falls back to the catalog"
    );
    assert_eq!(
        context_limit_for_episode(true, None, None, None),
        None,
        "two absent sources remain unknown rather than becoming zero"
    );
}

#[test]
fn cache_markers_warn_only_when_the_run_cannot_read_them_back() {
    let ttl = broca_provider::wire::CacheTtl::FiveMinutes;

    // The reportable shape: paid breakpoints on a run that makes one provider call.
    let warning = single_call_cache_marker_warning(&Some(marker_policy(ttl)), &[])
        .expect("markers with no tools are paid for without an in-run read");
    assert!(warning.contains("no tools"), "warning names the cause");

    // THE CLAIM THE OLD TEXT GOT WRONG, pinned so it cannot come back. The scope is this
    // run, not this prefix: a later request re-sending the same leading bytes reads the
    // cache within its ttl, because the provider keys on content and not on a session.
    // Asserting on absence rather than presence -- the failure being guarded is a
    // confident sentence about the future, and any spelling of it is wrong.
    for forbidden in ["cannot be read back", "unreadable", "wasted write"] {
        assert!(
            !warning.contains(forbidden),
            "the warning must not claim the write is unrecoverable, found {forbidden:?} \
                     in {warning:?}"
        );
    }
    assert!(
        warning.contains("later request") && warning.contains("ttl"),
        "the warning must name the condition that repays the premium: {warning:?}"
    );

    // A run that can dispatch may reach a second step, so the write may be read back.
    assert!(
        single_call_cache_marker_warning(&Some(marker_policy(ttl)), &[a_tool()]).is_none(),
        "a tool-bearing run can read its own cache write"
    );

    // Hints carry a cache key rather than paid breakpoints, so a single-step run is free.
    let hints = broca_provider::wire::CachePolicy {
        version: 1,
        class: broca_provider::wire::CacheClass::Hints {
            cache_key: "k".into(),
            retention: None,
        },
    };
    assert!(
        single_call_cache_marker_warning(&Some(hints), &[]).is_none(),
        "hint policies cost nothing extra on a single-step run"
    );

    // No policy at all is the common case and must stay silent.
    assert!(single_call_cache_marker_warning(&None, &[]).is_none());
}

/// A mock launch must not read the developer's auth-methods config.
///
/// The rule once lived in a comment inside `async fn main`, where nothing
/// could call it, so a refactor could drop the branch with every test still green. The
/// loader here PANICS if reached, which is what makes the mock arm mean something -- a
/// test that merely checked the returned config was empty would pass even if the file
/// had been read and happened to be empty.
#[test]
fn a_mock_launch_does_not_load_any_method_defaults() {
    let path = fresh_vault_root("mock-obsolete-methods");
    std::fs::create_dir_all(&path).unwrap();
    let mocked =
        crate::method_config_for_launch(true, || broca_module_serve::MethodConfig::load(&path))
            .expect("checking an obsolete directory does not read it");
    std::fs::remove_dir_all(path).unwrap();
    // An empty config overrides nothing, so the caller's fallback survives. Uses the
    // real resolver rather than an accessor so the assertion is about the behaviour
    // that matters at admission.
    let resolved = broca_module_serve::auth_routing::resolve_credential_id(
        "anthropic",
        None,
        None,
        &mocked,
        broca_module_serve::AuthMethod::ApiKey,
    );
    assert_eq!(
        resolved.method,
        broca_module_serve::AuthMethod::ApiKey,
        "the mock arm must carry no provider defaults to override the caller's"
    );

    // The real arm must actually CALL the loader, or the mock assertion is vacuous:
    // returning empty unconditionally would satisfy it too.
    let mut loaded = false;
    let _ = crate::method_config_for_launch(false, || {
        loaded = true;
        Ok(broca_module_serve::MethodConfig::empty())
    });
    assert!(
        loaded,
        "a real launch must load the config rather than skipping it"
    );
}

#[test]
fn family_for_method_drives_wire_changing_families() {
    // Header-only methods keep the catalog family (None); wire-changing methods name
    // their family — the single authority that drives resolve_provider AND the guard.
    assert_eq!(family_for_method(AuthMethod::ApiKey), None);
    assert_eq!(family_for_method(AuthMethod::Oauth), None);
    assert_eq!(
        family_for_method(AuthMethod::Antigravity),
        Some(WireFamilyId::GoogleCodeAssist)
    );
    assert_eq!(
        family_for_method(AuthMethod::Chatgpt),
        Some(WireFamilyId::OpenAiResponsesChatgpt)
    );
}

fn load_method_config(_label: &str, defaults: &str) -> broca_module_serve::MethodConfig {
    let defaults: std::collections::BTreeMap<String, String> =
        serde_json::from_str(defaults).unwrap();
    let cache = Arc::new(broca_module_serve::account_routing::ListingCache::default());
    let credentials = defaults.into_iter().map(|(provider, method)| serde_json::from_value(serde_json::json!({
                "id":format!("{method}:{provider}"), "type":method, "auth_method":method,
                "provider_ids":[provider], "state":"active", "categories":[], "serves":[], "operations":["read"], "record_version":1
            })).unwrap()).collect();
    cache.store(broca_subc::vault::ScopedList {
        credentials,
        grants: 1,
        grant_tuples: vec![],
        view: "methods-fixture".into(),
    });
    broca_module_serve::MethodConfig::from_listing(cache)
}

#[test]
fn caller_auth_pin_wins_over_vault_default_at_admission() {
    let methods = load_method_config("caller-pin", r#"{"openai":"oauth"}"#);
    let auth = AuthRequest {
        pool: None,
        credential_id: None,
        method: "apikey".into(),
        account: None,
    };
    let (pinned, account) =
        caller_auth_for_episode(Some(&auth), episode_auth_context(true, false)).unwrap();

    let selected = broca_module_serve::auth_routing::select_credential_id(
        None,
        "openai",
        pinned,
        account,
        &methods,
        FALLBACK_METHOD,
    );
    assert_eq!(selected.to_id_string(), "apikey:openai");
}

#[test]
fn caller_auth_account_rides_through_to_credential_id() {
    let methods = load_method_config("caller-account", r#"{"openai":"oauth"}"#);
    let auth = AuthRequest {
        pool: None,
        credential_id: None,
        method: "apikey".into(),
        account: Some("astro".into()),
    };
    let (pinned, account) =
        caller_auth_for_episode(Some(&auth), episode_auth_context(true, false)).unwrap();

    let selected = broca_module_serve::auth_routing::select_credential_id(
        None,
        "openai",
        pinned,
        account,
        &methods,
        FALLBACK_METHOD,
    );
    assert_eq!(selected.to_id_string(), "apikey:openai:astro");
}

#[test]
fn caller_auth_pin_is_refused_on_a_genuine_resume() {
    let auth = AuthRequest {
        pool: None,
        credential_id: None,
        method: "apikey".into(),
        account: None,
    };
    let error =
        caller_auth_for_episode(Some(&auth), episode_auth_context(false, true)).unwrap_err();

    assert!(matches!(
        error,
        RunError::Config(message)
            if message.contains("genuine resume of an unfinished episode")
                && message.contains("new episode after the prior episode finishes")
    ));
}

#[test]
fn caller_auth_pin_is_honoured_for_a_new_episode_with_session_history() {
    let methods = load_method_config("new-episode-pin", r#"{"openai":"oauth"}"#);
    let auth = AuthRequest {
        pool: None,
        credential_id: None,
        method: "apikey".into(),
        account: Some("astro".into()),
    };
    let context = episode_auth_context(true, true);
    assert_eq!(context, EpisodeAuthContext::NewEpisode);
    let (pinned, account) = caller_auth_for_episode(Some(&auth), context).unwrap();

    let selected = broca_module_serve::auth_routing::select_credential_id(
        None,
        "openai",
        pinned,
        account,
        &methods,
        FALLBACK_METHOD,
    );
    assert_eq!(selected.to_id_string(), "apikey:openai:astro");
}

#[test]
fn unknown_caller_auth_method_is_a_typed_config_refusal() {
    let auth = AuthRequest {
        pool: None,
        credential_id: None,
        method: "api-key".into(),
        account: None,
    };
    let error =
        caller_auth_for_episode(Some(&auth), episode_auth_context(true, false)).unwrap_err();

    assert!(matches!(
        error,
        RunError::Config(message)
            if message.contains("unknown auth method 'api-key'")
                && message.contains("'apikey', 'oauth', 'antigravity', and 'chatgpt'")
    ));
}

fn install_test_catalog() -> TestCatalog {
    let catalog = broca_catalog::Catalog::from_json_str(
        include_str!("../../../broca-catalog/data/models.dev.json"),
        include_str!("../../../broca-catalog/data/overlay.json"),
    )
    .expect("legacy test snapshot is well formed");
    let cache = Arc::new(broca_catalog::CatalogCache::empty());
    cache.install_parsed_for_test(&catalog, 0, broca_catalog::CatalogSource::Fusiform);
    install_catalog(cache)
}

fn resolve_for_test(
    model: &str,
    method: Option<AuthMethod>,
    family_override: Option<WireFamilyId>,
) -> broca_catalog::live::ProviderResolution {
    let _catalog = install_test_catalog();
    let result = broca_catalog::live::resolve_provider(
        model,
        None,
        2048,
        &AuthMode::Bearer(
            "eyJhbGciOiJub25lIn0.eyJjaGF0Z3B0X2FjY291bnRfaWQiOiAiYWNjdC10ZXN0In0.sig".into(),
        ),
        &broca_catalog::live::ResolveOptions {
            credential_hosts: Default::default(),
            tool_name_aliases: None,
            media: false,
            media_profile: None,
            claude_code_persona: None,
            anthropic_server_side_fallback: false,
            auth_method: method,
            family_override,
            project_id: None,
            session_id: Some("opencode-test"),
            account_id: None,
            cache_policy: None,
            prompt_cache_key: None,
            variant: None,
            service_tier: None,
            queued_service_tier: None,
            secret_refresher: None,
            structured_output: None,
            frozen_account_ref: None,
            clock: None,
            refusal_fallback_model: None,
            quota_observer: None,
        },
    );
    match result {
        Ok(resolved) => resolved,
        Err(error) => panic!("provider resolution failed: {error}"),
    }
}

#[test]
fn chatgpt_pause_only_construction_is_gated_and_repaired_resume_is_strict() {
    let _catalog = install_test_catalog();
    let options = broca_catalog::live::ResolveOptions {
        credential_hosts: Default::default(),
        tool_name_aliases: None,
        media: false,
        media_profile: None,
        claude_code_persona: None,
        anthropic_server_side_fallback: false,
        auth_method: Some(AuthMethod::Chatgpt),
        family_override: Some(WireFamilyId::OpenAiResponsesChatgpt),
        project_id: None,
        session_id: Some("chatgpt-pause-test"),
        account_id: None,
        cache_policy: None,
        prompt_cache_key: None,
        variant: None,
        service_tier: None,
        queued_service_tier: None,
        secret_refresher: None,
        structured_output: None,
        frozen_account_ref: None,
        clock: None,
        refusal_fallback_model: None,
        quota_observer: None,
    };
    let paused = AuthResolution {
        credential_hosts: Default::default(),
        mode: AuthMode::None,
        pause: Some(ProviderError {
            provider_details: None,
            cause: None,
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Auth,
            message: "needs reauth".into(),
            status: None,
            retry_after_secs: None,
            provider_code: Some("needs_reauth".into()),
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
            account_continue: None,
        }),
        selection: Some(broca_core::AuthSelection {
            billing_vault_facts: None,
            auth_method: None,
            credential_id: "chatgpt:openai".into(),
            account_ref: None,
            resolved_credential_id: None,
            vault_request: None,
            account_decision: None,
        }),
        method: Some(AuthMethod::Chatgpt),
        project_id: None,
        account_id: None,
        record_version: None,
        in_run_refresh: None,
    };
    let pause_provider = resolve_provider_for_episode(
        "openai/gpt-5.3-codex",
        None,
        Some(2048),
        &paused,
        &options,
        broca_provider::build_provider_http_client(),
        broca_catalog::live::EpisodeRenderConfig::default(),
    )
    .unwrap_or_else(|error| panic!("pause-only provider must resolve: {error}"));
    assert_eq!(pause_provider.auth_desc, "chatgpt-pause-only(openai)");

    let repaired_options = broca_catalog::live::ResolveOptions {
        account_id: Some("acct-repaired"),
        ..options.clone()
    };
    let repaired = AuthResolution {
        credential_hosts: Default::default(),
        mode: AuthMode::Secret("repaired-chatgpt-token".into()),
        pause: None,
        selection: paused.selection.clone(),
        method: Some(AuthMethod::Chatgpt),
        project_id: None,
        account_id: Some("acct-repaired".into()),
        record_version: Some(9),
        in_run_refresh: None,
    };
    let resumed_provider = resolve_provider_for_episode(
        "openai/gpt-5.3-codex",
        None,
        Some(2048),
        &repaired,
        &repaired_options,
        broca_provider::build_provider_http_client(),
        broca_catalog::live::EpisodeRenderConfig::default(),
    )
    .unwrap_or_else(|error| panic!("repaired resume must resolve strictly: {error}"));
    assert_eq!(resumed_provider.auth_desc, "chatgpt(openai)");
    assert_eq!(
        resumed_provider.provider.render_config_blob(),
        pause_provider.provider.render_config_blob(),
        "auth headers stay outside RunConfig while repaired resume rebuilds them strictly"
    );

    let unpaused_missing_auth = AuthResolution {
        pause: None,
        ..paused
    };
    let error = match resolve_provider_for_episode(
        "openai/gpt-5.3-codex",
        None,
        Some(2048),
        &unpaused_missing_auth,
        &options,
        broca_provider::build_provider_http_client(),
        broca_catalog::live::EpisodeRenderConfig::default(),
    ) {
        Ok(_) => panic!("AuthMode::None without a pause must fail"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("no ChatGPT token"),
        "unexpected error: {error}"
    );
}

#[test]
fn generic_methods_keep_catalog_family_render_bytes() {
    let opts = broca_protocol::request::CallOptions::new(Vec::new());
    let baseline = resolve_for_test("google/gemini-2.5-flash", None, None);
    let baseline_bytes = baseline.provider.render_request(&opts);
    let baseline_version = baseline.provider.renderer_version();
    assert_eq!(baseline.family_id, WireFamilyId::Gemini);

    for method in [AuthMethod::Oauth, AuthMethod::ApiKey] {
        let resolved = resolve_for_test("google/gemini-2.5-flash", Some(method), None);
        assert_eq!(resolved.family_id, WireFamilyId::Gemini);
        assert_eq!(resolved.provider.renderer_version(), baseline_version);
        assert_eq!(resolved.provider.render_request(&opts), baseline_bytes);
    }
}

#[test]
fn vault_success_proceeds_with_secret() {
    let token = VaultToken {
        hosts: Some(vec!["127.0.0.1".into(), "localhost".into()]),
        token: "tok-123".into(),
        expires_at_ms: Some(1),
        record_version: 1,
        project_id: None,
        account_id: None,
        resolved_credential_id: None,
    };
    match vault_outcome_to_decision(Ok(token)) {
        AuthDecision::Proceed(AuthMode::Secret(s)) => assert_eq!(s, "tok-123"),
        _ => panic!("a successful token must Proceed with AuthMode::Secret"),
    }
}

#[test]
fn needs_reauth_maps_to_pause_for_reauth() {
    match vault_outcome_to_decision(Err(VaultError::NeedsReauth)) {
        AuthDecision::PauseForReauth(e) => {
            assert_eq!(e.class, ErrorClass::Auth);
            assert_eq!(e.provider_code.as_deref(), Some("needs_reauth"));
        }
        _ => panic!("needs_reauth must map to PauseForReauth"),
    }
}

#[test]
fn connectivity_vault_errors_fail_admission_transiently() {
    for err in [
        VaultError::Transport("connection reset".into()),
        VaultError::VaultLocked,
        VaultError::RefreshFailed,
    ] {
        match vault_outcome_to_decision(Err(err.clone())) {
            AuthDecision::FailAdmissionTransient(message) => {
                assert!(message.contains("transient — retry"));
                assert!(!message.contains("credential not configured"));
            }
            _ => panic!("{err:?} must fail admission transiently"),
        }
    }
}

#[test]
fn configuration_vault_errors_fail_admission_permanently() {
    for err in [
        VaultError::NotFound,
        VaultError::Corrupt,
        VaultError::RefreshUnsupported,
        VaultError::Protocol("bad reply".into()),
    ] {
        match vault_outcome_to_decision(Err(err.clone())) {
            AuthDecision::FailAdmission(message) => {
                assert!(message.contains("credential not configured or unusable"));
            }
            _ => panic!("{err:?} must fail admission permanently"),
        }
    }
}

/// AN UNSATISFIABLE TTL DEMAND MUST NOT BE RETRYABLE, and this is the one arm where the
/// wrong classification is self-sustaining rather than merely wrong.
///
/// The vault evaluates `min_ttl_ms` against a FRESHLY MINTED token, so a retry asks the
/// same impossible question and spends another upstream mint doing it. Classified
/// transient, broca would loop against the provider's mint budget forever; classified
/// permanent, the run fails once with a message naming the repair.
///
/// This is a SEPARATE TEST from `configuration_vault_errors_fail_admission_permanently`
/// rather than a fifth entry in its list, because that list is about config faults and
/// this is about a demand THIS CODEBASE chose. Someone pruning that list would have no
/// reason to keep a TTL case in it.
///
/// Before the variant existed, `ttl_unsatisfiable` failed to DECODE and reached this same
/// permanent classification via `VaultError::Protocol` -- the safe answer, by accident,
/// one `#[serde(other)] Unknown => RefreshFailed` edit away from inverting.
#[test]
fn an_unsatisfiable_ttl_demand_fails_admission_permanently() {
    match vault_outcome_to_decision(Err(VaultError::TtlUnsatisfiable)) {
        AuthDecision::FailAdmission(message) => {
            assert!(
                message.contains("reduce the min_ttl_ms demand"),
                "the refusal must name the repair, not just fail: {message}"
            );
            assert!(
                !message.contains("transient"),
                "a retry cannot succeed, so the message must not invite one: {message}"
            );
        }
        // AuthDecision has no Debug (it carries credential-adjacent text), so name the
        // arms that would be wrong rather than printing the value.
        AuthDecision::FailAdmissionTransient(message) => panic!(
            "an unsatisfiable ttl demand classified TRANSIENT -- this is the retry loop \
                     against the provider's mint budget: {message}"
        ),
        _ => panic!("an unsatisfiable ttl demand must fail admission permanently, got another arm"),
    }
}

fn cached_token(expires_at_ms: Option<i64>) -> VaultToken {
    VaultToken {
        hosts: Some(vec!["127.0.0.1".into(), "localhost".into()]),
        token: "secret-token".into(),
        expires_at_ms,
        record_version: 7,
        project_id: None,
        account_id: None,
        resolved_credential_id: None,
    }
}

/// AN UNREACHABLE VAULT SERVED FROM CACHE RECORDS NO REQUEST -- and still serves the token.
///
/// This is the production case the record was wrong for: a warm cache, a vault that does
/// not answer, and the LKG fallback serving the cached token. The run authenticates, so
/// it writes a RunStarted and an export fact -- which used to carry `vault_request`,
/// claiming a credential.get the vault never received.
///
/// BOTH HALVES ARE ASSERTED, because each alone matches a wrong fix: recording nothing
/// ever would pass the first half, and a fix that stopped the cache fallback from serving
/// would pass the second. Only the right fix keeps the token AND drops the claim.
#[test]
fn an_unreachable_vault_served_from_cache_records_no_request() {
    let request = broca_core::VaultRequest {
        min_ttl_ms: Some(VAULT_MIN_TTL_MS),
        force_refresh: None,
        op: Some(broca_core::VaultReadOp::GetScoped),
    };
    let cache = VaultLkgCache::default();
    cache.insert("apikey:openai".to_string(), cached_token(None));
    let gauge = broca_module_serve::serve::VaultGauge::not_in_use();
    let raw = VaultGet::no_reply(VaultError::Transport("route never opened".into()));

    let recorded = recorded_vault_request(&raw, request);
    let served = resolve_vault_outcome(
        &cache,
        &gauge,
        "apikey:openai",
        raw.result,
        1_000,
        VAULT_MIN_TTL_MS,
    );

    assert!(
        served.is_ok(),
        "the cache fallback must still serve the token when the vault is unreachable"
    );
    assert_eq!(
        recorded, None,
        "no reply came back, so the run must not claim a request the vault may never \
                 have received"
    );
}

/// EVERY REPLY IS RECORDED, INCLUDING A REFUSAL -- the vault has a row for each.
///
/// The control for the test above: without it, recording nothing at all would pass. A
/// served token, an authoritative refusal, a locked vault and an undecodable reply all
/// mean the request reached the vault, so the join against the vault's record must find
/// broca's half.
#[test]
fn every_vault_reply_records_the_request_including_a_refusal() {
    let request = broca_core::VaultRequest {
        min_ttl_ms: Some(VAULT_MIN_TTL_MS),
        force_refresh: None,
        op: Some(broca_core::VaultReadOp::GetScoped),
    };
    let replies: Vec<(&str, VaultGet)> = vec![
        ("served", VaultGet::replied(Ok(cached_token(None)))),
        ("not found", VaultGet::replied(Err(VaultError::NotFound))),
        (
            "needs reauth",
            VaultGet::replied(Err(VaultError::NeedsReauth)),
        ),
        ("locked", VaultGet::replied(Err(VaultError::VaultLocked))),
        (
            "undecodable",
            VaultGet::replied(Err(VaultError::Protocol("bad reply".into()))),
        ),
    ];
    for (label, reply) in replies {
        assert_eq!(
            recorded_vault_request(&reply, request.clone()),
            Some(request.clone()),
            "the vault replied ({label}), so the request must be recorded"
        );
    }
}

/// A REVOKED HANDLE MUST NOT COME BACK WHEN THE VAULT GOES DOWN.
///
/// The sequence is three real steps, and only the third is surprising: a good `get`
/// caches a token; the operator revokes the handle, so the next `get` answers `NotFound`
/// and admission refuses; the vault then becomes unreachable, and the LKG path serves the
/// cached token for the credential broca ALREADY KNOWS IS GONE.
///
/// `NotFound` used to fall through to `Other`, which does not evict. So the same
/// credential was REFUSED while the vault was reachable and ACCEPTED once it was not --
/// an incoherence, not merely a liberal cache. API keys make it worst: they carry no
/// expiry, so there is no TTL that eventually ends the resurrection.
///
/// The `NeedsReauth` arm is the CONTROL. It already evicted, so if this test ever passes
/// for both arms without the NotFound mapping, it is asserting the control and nothing
/// else.
#[test]
fn an_authoritative_negative_evicts_so_an_outage_cannot_resurrect_it() {
    for authoritative in [VaultError::NotFound, VaultError::NeedsReauth] {
        let cache = VaultLkgCache::default();
        cache.insert(
            "apikey:openai".to_string(),
            VaultToken {
                hosts: Some(vec!["127.0.0.1".into(), "localhost".into()]),
                token: "cached-key".into(),
                // No expiry: an API key. Nothing times this out on its own.
                expires_at_ms: None,
                record_version: 1,
                project_id: None,
                account_id: None,
                resolved_credential_id: None,
            },
        );

        let class = lkg_error_class(&authoritative);
        evict_if_vault_authoritative(&cache, "apikey:openai", class);

        assert_eq!(
            class,
            LkgErrorClass::CredentialUnusable,
            "{authoritative} is an authoritative negative and must classify as such"
        );
        assert!(
            cache.get("apikey:openai").is_none(),
            "{authoritative} left the token cached, so a later outage would serve it"
        );

        // The outage that used to resurrect it now finds nothing to serve.
        let outage = lkg_error_class(&VaultError::Transport("vault down".into()));
        assert_eq!(outage, LkgErrorClass::Connectivity);
        assert_eq!(
            lkg_decision(None, 0, VAULT_MIN_TTL_MS, outage),
            LkgDecision::Refuse,
            "with the entry evicted, an outage must refuse rather than fall back"
        );
    }
}

/// The other half of the rule: a refusal ABOUT THE REQUEST OR THE VAULT is not a verdict
/// on the credential, so it must NOT evict. Widening the eviction to every non-connectivity
/// error would discard tokens that are still good -- `RefreshFailed` is transient by its
/// own definition, and `TtlUnsatisfiable` is about a number this code chose.
#[test]
fn a_request_shaped_refusal_does_not_evict_a_good_token() {
    for benign in [
        VaultError::RefreshFailed,
        VaultError::TtlUnsatisfiable,
        VaultError::TooManyItems,
    ] {
        let cache = VaultLkgCache::default();
        cache.insert(
            "apikey:openai".to_string(),
            VaultToken {
                hosts: Some(vec!["127.0.0.1".into(), "localhost".into()]),
                token: "cached-key".into(),
                expires_at_ms: None,
                record_version: 1,
                project_id: None,
                account_id: None,
                resolved_credential_id: None,
            },
        );
        evict_if_vault_authoritative(&cache, "apikey:openai", lkg_error_class(&benign));
        assert!(
            cache.get("apikey:openai").is_some(),
            "{benign} is not a verdict on the credential and must not evict it"
        );
    }
}

#[test]
fn lkg_decision_enforces_connectivity_and_ttl_margin() {
    let now_ms = 1_000_000;
    let expiring = CachedCredentialMeta {
        expires_at_ms: Some(now_ms + VAULT_MIN_TTL_MS),
    };
    assert_eq!(
        lkg_decision(
            Some(&expiring),
            now_ms,
            VAULT_MIN_TTL_MS,
            LkgErrorClass::Connectivity,
        ),
        LkgDecision::Serve
    );

    let too_close = CachedCredentialMeta {
        expires_at_ms: Some(now_ms + VAULT_MIN_TTL_MS - 1),
    };
    assert_eq!(
        lkg_decision(
            Some(&too_close),
            now_ms,
            VAULT_MIN_TTL_MS,
            LkgErrorClass::Connectivity,
        ),
        LkgDecision::Refuse
    );

    let no_expiry = CachedCredentialMeta {
        expires_at_ms: None,
    };
    assert_eq!(
        lkg_decision(
            Some(&no_expiry),
            now_ms,
            VAULT_MIN_TTL_MS,
            LkgErrorClass::Connectivity,
        ),
        LkgDecision::Serve
    );
    assert_eq!(
        lkg_decision(
            Some(&no_expiry),
            now_ms,
            VAULT_MIN_TTL_MS,
            LkgErrorClass::CredentialUnusable,
        ),
        LkgDecision::Refuse
    );
}

#[test]
fn successful_resolve_populates_lkg_for_connectivity_fallback() {
    let cache = VaultLkgCache::default();
    let gauge = broca_module_serve::serve::VaultGauge::disconnected();
    let now_ms = 1_000_000;
    let token = cached_token(Some(now_ms + VAULT_MIN_TTL_MS));

    let resolved = resolve_vault_outcome(
        &cache,
        gauge.as_ref(),
        "apikey:openai",
        Ok(token.clone()),
        now_ms,
        VAULT_MIN_TTL_MS,
    )
    .unwrap();
    assert_eq!(resolved, token);

    let fallback = resolve_vault_outcome(
        &cache,
        gauge.as_ref(),
        "apikey:openai",
        Err(VaultError::Transport("credentials offline".into())),
        now_ms,
        VAULT_MIN_TTL_MS,
    )
    .unwrap();
    assert_eq!(fallback, token);
}

#[test]
fn late_older_vault_reply_cannot_regress_lkg() {
    let credential_id = "apikey:openai";
    let cache = VaultLkgCache::default();
    let gauge = broca_module_serve::serve::VaultGauge::disconnected();
    let now_ms = 1_000_000;
    let mut older = cached_token(None);
    older.record_version = 1;
    let mut newer = cached_token(Some(now_ms + VAULT_MIN_TTL_MS));
    newer.record_version = 2;

    // The newer read finishes first; the older reply still belongs to its run,
    // but must not replace the shared fallback for subsequent admissions.
    for reply in [newer.clone(), older] {
        let version = reply.record_version;
        let served = resolve_vault_outcome(
            &cache,
            gauge.as_ref(),
            credential_id,
            Ok(reply),
            now_ms,
            VAULT_MIN_TTL_MS,
        )
        .unwrap();
        assert_eq!(served.record_version, version);
    }
    let fallback = resolve_vault_outcome(
        &cache,
        gauge.as_ref(),
        credential_id,
        Err(VaultError::Transport("credentials offline".into())),
        now_ms,
        VAULT_MIN_TTL_MS,
    )
    .unwrap();
    assert_eq!(fallback.record_version, 2);

    // An equal-version expiry update is useful even without a token rotation.
    newer.expires_at_ms = Some(now_ms + 2 * VAULT_MIN_TTL_MS);
    cache.insert(credential_id.into(), newer);
    assert_eq!(
        cache.get(credential_id).unwrap().expires_at_ms,
        Some(now_ms + 2 * VAULT_MIN_TTL_MS)
    );
    super::evict_for_provider_rejection(&cache, credential_id, 401, 1);
    assert_eq!(cache.get(credential_id).unwrap().record_version, 2);
    super::evict_for_provider_rejection(&cache, credential_id, 401, 2);
    assert!(cache.get(credential_id).is_none());
    assert!(resolve_vault_outcome(
        &cache,
        gauge.as_ref(),
        credential_id,
        Err(VaultError::Transport("credentials offline".into())),
        now_ms,
        VAULT_MIN_TTL_MS,
    )
    .is_err());
}

#[test]
fn provider_rejection_evicts_only_the_failed_record_version() {
    let credential_id = "apikey:openai";
    let cache = VaultLkgCache::default();
    let mut current = cached_token(None);
    current.record_version = 8;
    cache.insert(credential_id.into(), current);

    super::evict_for_provider_rejection(&cache, credential_id, 500, 8);
    assert_eq!(
        cache.get(credential_id).map(|token| token.record_version),
        Some(8),
        "a provider outage is not a verdict on the credential"
    );

    super::evict_for_provider_rejection(&cache, credential_id, 401, 7);
    assert_eq!(
        cache.get(credential_id).map(|token| token.record_version),
        Some(8)
    );

    super::evict_for_provider_rejection(&cache, credential_id, 401, 8);
    assert!(cache.get(credential_id).is_none());
}

fn err_with_status(status: Option<u16>) -> ProviderError {
    ProviderError {
        provider_details: None,
        cause: None,
        provider_subcode: None,
        refusal: None,
        class: ErrorClass::Auth,
        message: "x".into(),
        status,
        retry_after_secs: None,
        provider_code: None,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
        account_continue: None,
    }
}

/// A provider's refusal of a requested service tier on a 403, classified exactly as the
/// transport classifies it, is permanent with the stable tier code and is NOT reported to
/// the vault as a dead credential. A genuine auth 403 on the same tier-carrying request,
/// whose error names the key, is still `auth_required` and still reported.
#[test]
fn a_service_tier_refusal_403_is_never_reported_to_the_vault() {
    let tier = broca_provider::service_tier::ServiceTierRequest {
        tier: broca_catalog::live::ServiceTier::Priority,
        provider: "anthropic".into(),
    };
    let family = WireFamilyId::AnthropicMessages;
    let refusal = broca_provider::transport::classify_http_error(
        403,
        r#"{"type":"error","error":{"type":"permission_error","message":"Fast mode is not available for your organization."}}"#,
        None,
        Some((family, &tier)),
    );
    assert_eq!(refusal.class, ErrorClass::Permanent);
    assert_eq!(
        refusal.provider_code.as_deref(),
        Some(broca_provider::service_tier::SERVICE_TIER_UNAVAILABLE)
    );
    assert_eq!(wire_auth_status(&refusal), None);

    let dead_key = broca_provider::transport::classify_http_error(
        403,
        r#"{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}"#,
        None,
        Some((family, &tier)),
    );
    assert_eq!(dead_key.class, ErrorClass::Auth);
    assert_eq!(wire_auth_status(&dead_key), Some(403));
}

/// An exhausted account is a rate limit on a healthy credential. Reporting it would force
/// a refresh of a token that works, and on a subscription account that rotates the
/// refresh token for nothing.
#[test]
fn an_account_limit_refusal_is_never_reported_to_the_vault() {
    let exhausted = ProviderError {
        provider_details: None,
        cause: None,
        provider_subcode: None,
        refusal: None,
        class: ErrorClass::Transient,
        message: "429: account limit".into(),
        status: Some(429),
        retry_after_secs: Some(352_835.0),
        provider_code: Some(broca_protocol::error::ACCOUNT_LIMIT_EXHAUSTED.into()),
        resets_at_ms: Some(1_790_740_800_000),
        resets_at_source: None,
        limit_window: Some("seven_day".into()),
        account_ref: Some("d4bc9dbc".into()),
        credential_id: None,
        affordable_max_tokens: None,
        account_continue: None,
    };
    assert_eq!(wire_auth_status(&exhausted), None);
}

/// An Anthropic API key that has run out of prepaid credit is a billing problem, not an
/// invalid key, so it must never be reported to the vault as an auth failure. The refusal
/// goes through the transport's real `classify_http_error`, and no auth status is derived
/// from it. The
/// body is Anthropic's public shape, assumed rather than captured (see
/// `anthropic_credit_balance_too_low` in the transport).
#[test]
fn an_anthropic_credit_refusal_is_never_reported_to_the_vault() {
    let refusal = broca_provider::transport::classify_http_error(
        400,
        r#"{"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits."}}"#,
        None,
        None,
    );
    assert!(refusal.is_billing_exhausted());
    assert_eq!(wire_auth_status(&refusal), None);
}

fn run_outcome(
    reason: RunFinishReason,
    paused: Option<PauseReason>,
) -> Result<RunSummary, RunError> {
    Ok(RunSummary {
        reason,
        paused,
        steps: 0,
        usage: broca_protocol::Usage::default(),
        retries_used: 0,
        final_step_finish_reason: None,
    })
}

#[test]
fn wire_auth_wal_check_is_limited_to_auth_capable_outcomes() {
    let cases = [
        (
            "completed",
            run_outcome(RunFinishReason::Completed, None),
            false,
        ),
        (
            "auth pause",
            run_outcome(
                RunFinishReason::Interrupted,
                Some(PauseReason::AuthRequired),
            ),
            true,
        ),
        (
            "account-limit pause",
            run_outcome(
                RunFinishReason::Interrupted,
                Some(PauseReason::AccountLimit),
            ),
            false,
        ),
        (
            "terminal error",
            run_outcome(RunFinishReason::Error, None),
            true,
        ),
        (
            "transform unavailable",
            run_outcome(RunFinishReason::TransformUnavailable, None),
            false,
        ),
        (
            "max steps",
            run_outcome(RunFinishReason::MaxSteps, None),
            false,
        ),
        (
            "cancelled",
            run_outcome(RunFinishReason::Cancelled, None),
            false,
        ),
        (
            "interrupted",
            run_outcome(RunFinishReason::Interrupted, None),
            false,
        ),
        (
            "infrastructure error",
            Err(RunError::Wal("disk unavailable".into())),
            false,
        ),
    ];

    // An account-limit pause does not authorize a vault auth-failure report.
    for (name, outcome, expected) in cases {
        assert_eq!(should_check_wire_auth(&outcome), expected, "{name}");
    }
}

struct TerminalRouting {
    reports: std::sync::Mutex<Vec<serde_json::Value>>,
    supported: bool,
}

#[async_trait::async_trait]
impl broca_module_serve::account_routing::RoutingTransport for TerminalRouting {
    async fn observe_backoff_supported(&self) -> Result<bool, String> {
        Ok(true)
    }

    async fn observe_credential_refused_supported(&self) -> Result<bool, String> {
        Ok(self.supported)
    }

    async fn request(&self, body: Vec<u8>) -> broca_module_serve::account_routing::RoutingReply {
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        self.reports.lock().unwrap().push(body["params"].clone());
        Ok(Ok(br#"{"result":{}}"#.to_vec()))
    }
}

async fn terminal_backoff_reports(
    method: &str,
    path: &str,
    outcome: Result<RunSummary, RunError>,
    error: ProviderError,
) -> Vec<serde_json::Value> {
    let attempt_status = if outcome
        .as_ref()
        .is_ok_and(|summary| summary.reason == RunFinishReason::Completed)
    {
        Some(200)
    } else {
        error.status
    };
    terminal_backoff_reports_with_attempt_status(method, path, outcome, error, attempt_status).await
}

async fn terminal_backoff_reports_with_attempt_status(
    method: &str,
    path: &str,
    outcome: Result<RunSummary, RunError>,
    error: ProviderError,
    attempt_status: Option<u16>,
) -> Vec<serde_json::Value> {
    use broca_module_serve::account_routing::{RoutingClient, RunReporter};
    // A run reports its attempts by record position alone.
    let lineage: Option<broca_wal::WalLineageId> = None;
    let routing = Arc::new(TerminalRouting {
        reports: Default::default(),
        supported: true,
    });
    let decision: broca_wal::AccountDecision = serde_json::from_value(serde_json::json!({
        "path": path,
        "admission_id": "adm-transient",
    }))
    .unwrap();
    // A run names its reports by its session and record position. Only an
    // account routing chose is reported to routing at all.
    let Some(reporter) = RunReporter::start_for_decision(
        Arc::new(RoutingClient::new(routing.clone(), Default::default())),
        &decision,
        "run-transient".into(),
        None,
        Arc::new(std::sync::atomic::AtomicU64::new(4)),
        Some("session-transient".into()),
    ) else {
        return Vec::new();
    };
    for (seq, receipt) in [(2, 1_000), (3, 42_000)] {
        let status = if seq == 2 { Some(503) } else { attempt_status };
        let mut attempt: broca_protocol::ModelAttempt = serde_json::from_value(serde_json::json!({
            "attempt":seq - 2,"request_body_len":10,"request_body_sha256":"aa",
            "http_status":status,"response_body_len":0,
            "response_body_sha256":"bb","ts_ms":receipt,
            "outcome":match status {
                Some(200..=299) => "complete",
                Some(_) => "http_error",
                None => "connect_failed",
            }
        }))
        .unwrap();
        attempt.account_observation.failure_received_at_ms = Some(receipt);
        broca_core::AccountReporter::attempt_recorded(reporter.as_ref(), lineage, seq, &attempt);
    }
    let selection: broca_core::AuthSelection = serde_json::from_value(serde_json::json!({
        "auth_method":method,"credential_id":"vault-key",
        "account_decision":{"path":path,"admission_id":"adm-transient"}
    }))
    .unwrap();
    report_routed_terminal_error(
        &reporter,
        Some(&selection),
        &outcome,
        async { Some(error) },
        (lineage, 4),
    )
    .await;
    if let Ok(summary) = outcome {
        if summary.paused.is_none() {
            reporter.finish(lineage, 4, summary.reason);
        }
    }
    drop(reporter);
    broca_test_support::wait_until("terminal backoff reports drained", || async {
        if Arc::strong_count(&routing) == 1 {
            Ok(())
        } else {
            Err("report task still owns routing".into())
        }
    })
    .await;
    let reports = routing.reports.lock().unwrap().clone();
    reports
}

#[tokio::test]
async fn routed_chatgpt_terminal_overload_reports_default_backoff() {
    let mut error = err_with_status(Some(503));
    error.class = ErrorClass::Transient;
    error.provider_code = Some("server_is_overloaded".into());
    let reports = terminal_backoff_reports(
        "chatgpt",
        "routed",
        run_outcome(RunFinishReason::Error, None),
        error,
    )
    .await;
    assert_eq!(
        reports
            .iter()
            .map(|body| body["kind"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["attempt", "attempt", "transient_backoff", "run_finished"]
    );
    assert_eq!(reports[2]["retry_at_ms"], 47_000);
    assert_eq!(reports[2]["admission_id"], "adm-transient");
    assert_eq!(
        reports[2]["event_id"],
        format!("{}:backoff", reports[3]["event_id"].as_str().unwrap())
    );
}

#[tokio::test]
async fn routed_internal_transient_after_successful_attempt_does_not_queue_backoff() {
    let mut error = err_with_status(None);
    error.class = ErrorClass::Transient;
    let reports = terminal_backoff_reports_with_attempt_status(
        "chatgpt",
        "routed",
        run_outcome(RunFinishReason::Error, None),
        error,
        Some(200),
    )
    .await;
    assert!(reports
        .iter()
        .all(|body| body["kind"] != "transient_backoff"));
}

#[tokio::test]
async fn routed_terminal_transients_report_structured_or_default_backoff_for_every_method() {
    for method in ["oauth", "chatgpt", "apikey"] {
        for (status, code, retry, expected, class) in [
            (
                Some(429),
                Some("rate_limited"),
                Some(1.125),
                43_125,
                ErrorClass::Transient,
            ),
            (
                Some(503),
                Some("server_is_overloaded"),
                Some(2.25),
                44_250,
                ErrorClass::Transient,
            ),
            (
                Some(503),
                Some("server_is_overloaded"),
                None,
                47_000,
                ErrorClass::Transient,
            ),
            (None, None, None, 47_000, ErrorClass::Transient),
            (None, None, None, 47_000, ErrorClass::ProviderUnavailable),
        ] {
            let mut error = err_with_status(status);
            error.class = class;
            error.provider_code = code.map(str::to_owned);
            error.retry_after_secs = retry;
            error.message = "retry after 999 seconds".into();
            let reports = terminal_backoff_reports(
                method,
                "routed",
                run_outcome(RunFinishReason::Error, None),
                error,
            )
            .await;
            assert_eq!(reports.len(), 4, "{method}: {status:?}");
            assert_eq!(reports[2]["kind"], "transient_backoff");
            assert_eq!(reports[2]["retry_at_ms"], expected, "{method}: {status:?}");
            assert_eq!(reports[2]["admission_id"], "adm-transient");
            assert_eq!(reports[3]["kind"], "run_finished");
        }
    }
}

#[tokio::test]
async fn routed_recovery_account_pause_and_credential_refusal_do_not_queue_backoff() {
    for method in ["oauth", "chatgpt", "apikey"] {
        for (reason, pause, class, code, status) in [
            (
                RunFinishReason::Completed,
                None,
                ErrorClass::Transient,
                "server_is_overloaded",
                503,
            ),
            (
                RunFinishReason::Interrupted,
                Some(PauseReason::AccountLimit),
                ErrorClass::Transient,
                "account_limit_exhausted",
                429,
            ),
            (
                RunFinishReason::Error,
                None,
                ErrorClass::Transient,
                "account_limit_exhausted",
                429,
            ),
            (
                RunFinishReason::Error,
                None,
                ErrorClass::Permanent,
                "billing_exhausted",
                402,
            ),
            (
                RunFinishReason::Interrupted,
                Some(PauseReason::AuthRequired),
                ErrorClass::Auth,
                "invalid_api_key",
                401,
            ),
        ] {
            let mut error = err_with_status(Some(status));
            error.class = class;
            error.provider_code = Some(code.into());
            error.retry_after_secs = Some(1.0);
            let reports =
                terminal_backoff_reports(method, "routed", run_outcome(reason, pause), error).await;
            assert!(
                reports
                    .iter()
                    .all(|body| body["kind"] != "transient_backoff"),
                "{method}: {code}: {reason:?}"
            );
            if pause.is_some() {
                assert!(reports.iter().all(|body| body["kind"] != "run_finished"));
            }
        }
    }
}

#[tokio::test]
async fn unrouted_terminal_transients_do_not_queue_backoff() {
    for method in ["oauth", "chatgpt", "apikey"] {
        for path in ["legacy_static", "caller"] {
            let mut error = err_with_status(Some(503));
            error.class = ErrorClass::Transient;
            let reports = terminal_backoff_reports(
                method,
                path,
                run_outcome(RunFinishReason::Error, None),
                error,
            )
            .await;
            // An account routing didn't choose is never reported to routing, so it
            // can't be held back from it either.
            assert_eq!(reports, Vec::<serde_json::Value>::new(), "{method}: {path}");
        }
    }
}

#[test]
fn only_auth_class_401_403_are_reportable_wire_auth_failures() {
    // An Auth-class 401/403 means the provider rejected the credential → report it to the
    // vault. A Permanent 403 means the credential is valid but forbidden → do not report.
    assert_eq!(wire_auth_status(&err_with_status(Some(401))), Some(401));
    assert_eq!(wire_auth_status(&err_with_status(Some(403))), Some(403));
    assert_eq!(
        wire_auth_status(&ProviderError {
            provider_details: None,
            cause: None,
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Permanent,
            message: "forbidden".into(),
            status: Some(403),
            retry_after_secs: None,
            provider_code: None,
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
            account_continue: None,
        }),
        None
    );
    // Everything else is NOT a dead-credential signal — a transient hiccup or no
    // status must never be reported (it would wrongly invalidate a live credential).
    for s in [None, Some(429), Some(500), Some(503), Some(400), Some(200)] {
        assert_eq!(wire_auth_status(&err_with_status(s)), None, "status {s:?}");
    }
}
