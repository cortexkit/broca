use crate::{admit, parse_plan, CompactionSupport, DescribeAnswer, FetchError, Fetcher, Plan};
use async_trait::async_trait;
use cortexkit_role_step_transform_provider::subscription::DeclareRequest;
use cortexkit_role_tool_provider::catalog::{composition_digest, CatalogRequest};
use serde_json::{json, Value};

fn raw_plan(provider: &str, owner: Option<&str>, op: &str) -> Value {
    let steps = json!([{
        "provider": provider, "preset": "transform", "params": {"mode": "replace"},
        "subscriptions": [
            {"hook": "pre_user", "ops": [op], "budget_ms": 1000, "on_unavailable": "pass"},
            {"hook": "post_assistant", "ops": [op], "budget_ms": 1000, "on_unavailable": "pass"}
        ]
    }]);
    let mut providers = vec![provider];
    if let Some(owner) = owner {
        providers.push(owner);
    }
    providers.sort();
    providers.dedup();
    let composition = json!({
        "providers": providers.iter().map(|provider| json!({"provider": provider, "tools": []})).collect::<Vec<_>>(),
        "step_transform_items": steps,
        "extra": {"opaque": [null, false, 7]}
    });
    let mut plan = json!({
        "tool_items": [], "system_text_items": [], "step_transform_items": steps,
        "composition_digest": composition_digest(&composition).unwrap(), "composition": composition
    });
    if let Some(owner) = owner {
        plan["compaction_item"] = json!({"provider": owner, "preset": "head",
            "params": {"opaque": [null, false, {"limit": 12}]}});
    }
    plan
}

#[test]
fn compaction_support_is_explicit_and_unsupported_keeps_the_refusal() {
    let raw = raw_plan("magic-context", Some("magic-context"), "replace");
    let supported = parse_plan(&raw, CompactionSupport::Supported).unwrap();
    let item = supported.compaction_item.as_ref().unwrap();
    assert_eq!(item.provider, "magic-context");
    assert_eq!(item.preset, "head");
    assert_eq!(
        serde_json::to_value(&item.params).unwrap(),
        raw["compaction_item"]["params"]
    );
    assert_eq!(
        parse_plan(&raw, CompactionSupport::Unsupported)
            .unwrap_err()
            .detail(),
        json!({"field": "plan.compaction_item"})
    );
}

#[test]
fn parser_allows_only_the_compaction_owners_replace_subscriptions() {
    for hook in ["pre_user", "post_assistant"] {
        for owner in [Some("magic-context"), None] {
            let mut raw = raw_plan("other-context", owner, "replace");
            raw["step_transform_items"][0]["subscriptions"] = json!([
                {"hook": hook, "ops": ["replace"], "budget_ms": 1000, "on_unavailable": "pass"}
            ]);
            raw["composition"]["step_transform_items"] = raw["step_transform_items"].clone();
            raw["composition_digest"] = json!(composition_digest(&raw["composition"]).unwrap());
            let refusal = parse_plan(&raw, CompactionSupport::Supported).unwrap_err();
            assert_eq!(refusal.code(), "invalid_params");
            assert_eq!(
                refusal.detail(),
                json!({"field": "plan.step_transform_items",
                "item": 0, "subscription": 0, "problem": "replace_not_reduction_owner"})
            );
        }
    }
    parse_plan(
        &raw_plan("magic-context", Some("magic-context"), "replace"),
        CompactionSupport::Supported,
    )
    .unwrap();
}

/// A `role.describe` answer serving `compaction-provider/v1` with every required op.
fn serving_describe() -> Value {
    json!({"majors": [{"version": "compaction-provider/v1",
        "ops": ["role.describe", "compaction.setup", "compaction.step"], "stability": "alpha"}],
        "implementation_version": "test", "capabilities": []})
}

/// Answers the plan's step declaration, and the compaction provider's describe with `describe`,
/// counting every describe asked of it.
struct Declaration {
    describe: Result<DescribeAnswer, FetchError>,
    describes: std::sync::Mutex<Vec<String>>,
}

impl Declaration {
    fn answering(describe: Result<Value, FetchError>) -> Self {
        Self::describing(describe.map(DescribeAnswer::Answered))
    }

    fn describing(describe: Result<DescribeAnswer, FetchError>) -> Self {
        Self {
            describe,
            describes: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn describes(&self) -> Vec<String> {
        self.describes.lock().unwrap().clone()
    }
}

#[async_trait]
impl Fetcher for Declaration {
    async fn catalog(&self, _: &str, _: &CatalogRequest) -> Result<Value, FetchError> {
        panic!("a compaction-only provider must not be fetched as a tool provider")
    }
    async fn declare(&self, _: &str, request: &DeclareRequest) -> Result<Value, FetchError> {
        assert_eq!(request.preset.as_deref(), Some("transform"));
        assert_eq!(
            request.params,
            json!({"mode": "replace"}).as_object().unwrap().clone()
        );
        Ok(json!({"subscriptions": [
            {"hook": "pre_user", "ops": ["replace", "append"], "budget_ms": 1000, "on_unavailable": "pass"},
            {"hook": "post_assistant", "ops": ["replace", "append"], "budget_ms": 1000, "on_unavailable": "pass"}
        ]}))
    }
    async fn describe_compaction(&self, provider: &str) -> Result<DescribeAnswer, FetchError> {
        self.describes.lock().unwrap().push(provider.to_owned());
        self.describe.clone()
    }
}

async fn admission_with(
    plan: &Plan,
    fetcher: &Declaration,
) -> Result<crate::Admitted, crate::Refusal> {
    admit(
        plan,
        fetcher,
        &broca_protocol::BindIdentity::new("/tmp/compaction-admission", "caller", "session"),
        crate::ADMISSION_DEADLINE,
    )
    .await
}

async fn admission(plan: &Plan) -> Result<crate::Admitted, crate::Refusal> {
    admission_with(plan, &Declaration::answering(Ok(serving_describe()))).await
}

#[tokio::test]
async fn admission_freezes_compaction_config_and_role_version() {
    let raw = raw_plan("magic-context", Some("magic-context"), "replace");
    let plan = parse_plan(&raw, CompactionSupport::Supported).unwrap();
    let fetcher = Declaration::answering(Ok(serving_describe()));
    let manifest = admission_with(&plan, &fetcher).await.unwrap().manifest;
    assert_eq!(fetcher.describes(), ["magic-context"]);
    assert_eq!(manifest.plan.as_json_text(), raw.to_string());
    assert_eq!(
        manifest
            .role_versions
            .get("compaction-provider")
            .map(String::as_str),
        Some("v1")
    );
    assert_eq!(
        manifest
            .role_versions
            .get("step-transform-provider")
            .map(String::as_str),
        Some("v1")
    );
    assert!(manifest.fetches.is_empty());
}

#[tokio::test]
async fn admission_rechecks_replace_ownership_independently_of_the_parser() {
    for owner in [Some("magic-context"), None] {
        let mut plan = parse_plan(
            &raw_plan("other-context", owner, "append"),
            CompactionSupport::Supported,
        )
        .unwrap();
        for hook in ["pre_user", "post_assistant"] {
            plan.step_items[0].subscriptions = vec![serde_json::from_value(json!({
                "hook": hook, "ops": ["replace"], "budget_ms": 1000, "on_unavailable": "pass"
            }))
            .unwrap()];
            assert_eq!(
                admission(&plan).await.unwrap_err().detail(),
                json!({
                    "field": "plan.step_transform_items", "item": 0, "subscription": 0,
                    "problem": "replace_not_reduction_owner"
                })
            );
        }
    }
}

#[tokio::test]
async fn a_noncompaction_manifest_does_not_claim_the_compaction_role() {
    let plan = parse_plan(
        &raw_plan("other-context", None, "append"),
        CompactionSupport::Unsupported,
    )
    .unwrap();
    let fetcher = Declaration::answering(Ok(serving_describe()));
    let manifest = admission_with(&plan, &fetcher).await.unwrap().manifest;
    assert!(!manifest.role_versions.contains_key("compaction-provider"));
    assert!(
        fetcher.describes().is_empty(),
        "a plan without a compaction item must ask no provider about the compaction role"
    );
}

fn compaction_plan() -> Plan {
    parse_plan(
        &raw_plan("magic-context", Some("magic-context"), "replace"),
        CompactionSupport::Supported,
    )
    .unwrap()
}

#[tokio::test]
async fn a_provider_whose_describe_lacks_the_role_is_refused_invalid_params_naming_it() {
    let cases = [
        (
            json!({"majors": [{"version": "step-transform-provider/v1",
                "ops": ["role.describe", "transform.declare", "transform.hook"]}],
                "implementation_version": "mc"}),
            json!([{"problem": "missing_major", "found": ["step-transform-provider/v1"]}]),
        ),
        (
            json!({"majors": [{"version": "compaction-provider/v1",
                "ops": ["role.describe", "compaction.setup"]}], "implementation_version": "mc"}),
            json!([{"problem": "missing_op", "op": "compaction.step"}]),
        ),
    ];
    for (describe, problems) in cases {
        let fetcher = Declaration::answering(Ok(describe));
        let refusal = admission_with(&compaction_plan(), &fetcher)
            .await
            .unwrap_err();
        assert_eq!(refusal.code(), "invalid_params");
        assert_eq!(
            refusal.detail(),
            json!({"field": "plan.compaction_item", "provider": "magic-context",
                "problems": problems})
        );
        assert!(
            !refusal.retryable(),
            "a provider that said no says no again"
        );
        assert_eq!(fetcher.describes(), ["magic-context"]);
    }
}

/// A provider that refuses `role.describe` itself with a typed code that does not clear on retry
/// (live Magic Context answers `unknown_method`) has said it does not serve the role: the same
/// permanent refusal, naming the code it refused with.
#[tokio::test]
async fn a_provider_refusing_its_describe_for_good_is_refused_invalid_params_naming_the_code() {
    for code in ["unknown_method", "invalid_request"] {
        let fetcher = Declaration::describing(Ok(DescribeAnswer::Refused { code: code.into() }));
        let refusal = admission_with(&compaction_plan(), &fetcher)
            .await
            .unwrap_err();
        assert_eq!(refusal.code(), "invalid_params");
        assert_eq!(
            refusal.detail(),
            json!({"field": "plan.compaction_item", "provider": "magic-context",
                "problems": [{"problem": "describe_refused", "code": code}]})
        );
        assert!(!refusal.retryable(), "{code}");
        assert_eq!(
            fetcher.describes(),
            ["magic-context"],
            "asked once, not retried"
        );
    }
}

/// A provider that could not be asked, that refused its describe with a code that may clear, or
/// whose answer is not a describe, has not said it lacks the role. Each is the runner role's
/// retryable `fetch_unavailable`, naming the provider, and never the permanent refusal above.
#[tokio::test(start_paused = true)]
async fn a_provider_that_could_not_be_asked_is_fetch_unavailable_not_a_role_refusal() {
    let cases = [
        (
            Err(FetchError::Lost("route closed".into())),
            json!({"provider": "magic-context", "reason": "timeout"}),
        ),
        (
            Err(FetchError::Refused {
                code: "module_warming".into(),
                detail: None,
                retryable: true,
            }),
            json!({"provider": "magic-context", "reason": "refused",
                "provider_code": "module_warming"}),
        ),
        (
            Err(FetchError::Refused {
                code: "transient".into(),
                detail: None,
                retryable: true,
            }),
            json!({"provider": "magic-context", "reason": "refused",
                "provider_code": "transient"}),
        ),
        (
            Err(FetchError::UnknownModule),
            json!({"provider": "magic-context", "reason": "unknown_module"}),
        ),
        (
            Ok(json!("not a describe")),
            json!({"provider": "magic-context", "reason": "refused",
                "provider_code": "invalid_answer"}),
        ),
        (
            Ok(json!({"majors": []})),
            json!({"provider": "magic-context", "reason": "refused",
                "provider_code": "invalid_answer"}),
        ),
    ];
    for (describe, detail) in cases {
        let fetcher = Declaration::answering(describe);
        let refusal = admission_with(&compaction_plan(), &fetcher)
            .await
            .unwrap_err();
        assert_eq!(refusal.code(), "fetch_unavailable", "{detail}");
        assert_eq!(refusal.detail(), detail);
        assert!(refusal.retryable(), "{detail}");
    }
}
