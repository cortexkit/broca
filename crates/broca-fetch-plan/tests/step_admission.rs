//! Admission freezes provider answers, not the typed view used to validate them.
use async_trait::async_trait;
use broca_fetch_plan::{
    admit, parse_plan, DescribeAnswer, FetchError, Fetcher, ADMISSION_DEADLINE,
};
use cortexkit_role_step_transform_provider::subscription::DeclareRequest;
use cortexkit_role_tool_provider::catalog::{composition_digest, schema_digest, CatalogRequest};
use serde_json::{json, Value};

fn plan(steps: Value, tool: bool) -> Value {
    let providers = if tool {
        json!([{"provider":"aft", "tools":[{"name":"edit", "capabilities":[]}]}])
    } else {
        json!([{"provider":"a-second", "tools":[]}, {"provider":"z-first", "tools":[]}])
    };
    let composition = json!({"providers":providers, "step_transform_items":steps});
    json!({"tool_items":if tool {json!([{"provider":"aft", "preset":"worker", "params":{}}])} else {json!([])},
        "system_text_items":[], "step_transform_items":steps,
        "composition_digest":composition_digest(&composition).unwrap(), "composition":composition})
}

fn session() -> broca_protocol::BindIdentity {
    broca_protocol::BindIdentity::new("/tmp/step-admission", "caller", "frozen")
}

struct Declarations;
#[async_trait]
impl Fetcher for Declarations {
    async fn catalog(&self, _: &str, _: &CatalogRequest) -> Result<Value, FetchError> {
        panic!("step-only providers must not have catalog fetches")
    }
    async fn describe_compaction(&self, _: &str) -> Result<DescribeAnswer, FetchError> {
        panic!("these plans carry no compaction item")
    }

    async fn declare(&self, provider: &str, request: &DeclareRequest) -> Result<Value, FetchError> {
        assert_eq!(request.preset.as_deref(), Some("worker"));
        assert_eq!(
            request.params,
            json!({"nested":[1,null,{"x":false}]})
                .as_object()
                .unwrap()
                .clone()
        );
        if provider == "z-first" {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        Ok(answer(provider))
    }
}
fn answer(provider: &str) -> Value {
    // Omitted on_unavailable and unknown members must survive typed validation unchanged.
    json!({"subscriptions":[{"hook":"post_assistant", "ops":["append"], "budget_ms":1000}],
        "provider_extra":{"provider":provider, "nested":[null,false,1.25]}})
}

#[tokio::test]
async fn declarations_are_frozen_verbatim_in_plan_order_without_catalog_fetches() {
    let steps = json!(["z-first", "a-second"]).as_array().unwrap().iter().map(|provider| json!({
        "provider":provider, "preset":"worker", "params":{"nested":[1,null,{"x":false}]},
        "subscriptions":[{"hook":"post_assistant", "ops":["append"], "on_unavailable":"pass", "budget_ms":1000}]
    })).collect::<Vec<_>>();
    let raw = plan(json!(steps), false);
    let parsed = parse_plan(&raw, broca_fetch_plan::CompactionSupport::Unsupported).unwrap();
    let manifest = admit(&parsed, &Declarations, &session(), ADMISSION_DEADLINE)
        .await
        .unwrap()
        .manifest;
    assert!(manifest.fetches.is_empty());
    assert_eq!(manifest.declarations.len(), 2);
    for (frozen, provider) in manifest.declarations.iter().zip(["z-first", "a-second"]) {
        assert_eq!(frozen.provider, provider);
        assert_eq!(frozen.answer.as_json_text(), answer(provider).to_string());
    }
    assert_eq!(
        serde_json::from_str::<Value>(manifest.plan.as_json_text()).unwrap(),
        raw
    );
}

fn approve_plan() -> Value {
    let steps = ["z-first", "a-second"].map(|provider| {
        json!({
            "provider":provider, "preset":"worker", "params":{},
            "subscriptions":[{"hook":"pre_tool", "phase":"approve", "ops":[],
                "on_unavailable":"refuse", "budget_ms":1000}]
        })
    });
    plan(json!(steps), false)
}

struct ApprovalDeclarations;
#[async_trait]
impl Fetcher for ApprovalDeclarations {
    async fn catalog(&self, _: &str, _: &CatalogRequest) -> Result<Value, FetchError> {
        panic!("step-only providers must not have catalog fetches")
    }
    async fn describe_compaction(&self, _: &str) -> Result<DescribeAnswer, FetchError> {
        panic!("these plans carry no compaction item")
    }

    async fn declare(&self, provider: &str, request: &DeclareRequest) -> Result<Value, FetchError> {
        assert_eq!(request.preset.as_deref(), Some("worker"));
        assert!(request.params.is_empty());
        Ok(approve_answer(provider))
    }
}

fn approve_answer(provider: &str) -> Value {
    // Admission must preserve the validate hook's omitted default and unknown provider_extra
    // data in the stored declaration, rather than replacing the provider's JSON with typed JSON.
    json!({"subscriptions":[
        {"hook":"pre_tool", "phase":"validate", "ops":[], "budget_ms":1000},
        {"hook":"pre_tool", "phase":"approve", "ops":[], "on_unavailable":"pass", "budget_ms":1000}
    ], "provider_extra":{"provider":provider, "nested":[null,false,1.25]}})
}

#[tokio::test]
async fn approve_subscription_is_admitted_with_its_declaration_frozen() {
    let raw = approve_plan();
    let parsed = parse_plan(&raw, broca_fetch_plan::CompactionSupport::Unsupported).unwrap();
    let manifest = admit(
        &parsed,
        &ApprovalDeclarations,
        &session(),
        ADMISSION_DEADLINE,
    )
    .await
    .unwrap()
    .manifest;
    assert!(manifest.fetches.is_empty());
    assert_eq!(manifest.declarations.len(), 2);
    for (frozen, provider) in manifest.declarations.iter().zip(["z-first", "a-second"]) {
        assert_eq!(frozen.provider, provider);
        assert_eq!(
            frozen.answer.as_json_text(),
            approve_answer(provider).to_string()
        );
    }
    assert_eq!(manifest.plan.as_json_text(), raw.to_string());
    let decoded: broca_wal::PlanManifest =
        serde_json::from_slice(&serde_json::to_vec(&manifest).unwrap()).unwrap();
    assert_eq!(decoded, manifest);
}

#[tokio::test]
async fn approve_on_unavailable_pass_refusal_matches_wire_golden() {
    let mut raw = approve_plan();
    raw["step_transform_items"][0]["subscriptions"] = json!([]);
    raw["step_transform_items"][1]["subscriptions"] = json!([
        {"hook":"pre_tool", "phase":"validate", "ops":[], "on_unavailable":"refuse", "budget_ms":1000},
        {"hook":"pre_tool", "phase":"approve", "ops":[], "on_unavailable":"pass", "budget_ms":1000}
    ]);
    raw["composition"]["step_transform_items"] = raw["step_transform_items"].clone();
    raw["composition_digest"] = json!(composition_digest(&raw["composition"]).unwrap());

    // Admit a complete plan so the golden pins the actual approve/pass refusal from parse_plan,
    // not a hand-built Refusal; if parsing accepts it, declaration admission must run as usual.
    let admitted = match parse_plan(&raw, broca_fetch_plan::CompactionSupport::Unsupported) {
        Ok(parsed) => {
            admit(
                &parsed,
                &ApprovalDeclarations,
                &session(),
                ADMISSION_DEADLINE,
            )
            .await
        }
        Err(refusal) => Err(refusal),
    };
    let refusal = admitted.unwrap_err();
    assert_eq!(refusal.code(), "invalid_params", "{refusal:?}");
    let detail: cortexkit_role_step_transform_provider::subscription::InvalidSubscriptionDetail =
        serde_json::from_value(refusal.detail()).unwrap();
    assert_eq!(detail.item, 1);
    assert_eq!(detail.subscription, 1);
    assert_eq!(detail.problem, "approve_on_unavailable_pass");
    assert!(!refusal.retryable());

    // Model the wire body that broca-module-serve builds from Refusal::code, message and detail.
    // This pins admission's output fields without needing the daemon's transport or runtime.
    #[derive(serde::Serialize)]
    struct ErrorBody {
        code: &'static str,
        message: String,
        detail: Value,
    }
    let bytes = serde_json::to_vec(&ErrorBody {
        code: refusal.code(),
        message: refusal.message(),
        detail: refusal.detail(),
    })
    .unwrap();
    assert_eq!(
        bytes,
        include_str!("fixtures/approve_on_unavailable_pass.json")
            .trim_end()
            .as_bytes()
    );
}

struct Catalog {
    result_ops: Option<Value>,
    reply: Option<Value>,
    schema: Option<Value>,
}
#[async_trait]
impl Fetcher for Catalog {
    async fn catalog(&self, _: &str, request: &CatalogRequest) -> Result<Value, FetchError> {
        let schema = self
            .schema
            .clone()
            .unwrap_or_else(|| json!({"type":"object"}));
        let mut tool = json!({"name":"edit", "input_schema":schema, "schema_digest":schema_digest(&schema).unwrap(), "semantics":1});
        if let Some(ops) = &self.result_ops {
            tool["result_ops"] = ops.clone();
        }
        if let Some(reply) = &self.reply {
            tool["reply"] = reply.clone();
        }
        Ok(
            json!({"generation":"1", "catalog_digest":"catalog", "composition_digest":composition_digest(&json!(request.composition)).unwrap(), "tools":[tool]}),
        )
    }
    async fn declare(&self, _: &str, _: &DeclareRequest) -> Result<Value, FetchError> {
        panic!("no transform items")
    }

    async fn describe_compaction(&self, _: &str) -> Result<DescribeAnswer, FetchError> {
        panic!("these plans carry no compaction item")
    }
}

#[tokio::test]
async fn catalog_result_ops_freeze_the_explicit_list_or_resolved_default() {
    let parsed = parse_plan(
        &plan(json!([]), true),
        broca_fetch_plan::CompactionSupport::Unsupported,
    )
    .unwrap();
    for (result_ops, expected) in [
        (None, json!(["prepend", "append", "replace"])),
        (Some(json!(["prepend"])), json!(["prepend"])),
        (Some(json!([])), json!([])),
        (
            Some(json!(["prepend", "rotate"])),
            json!(["prepend", "rotate"]),
        ),
    ] {
        let manifest = admit(
            &parsed,
            &Catalog {
                result_ops,
                reply: None,
                schema: None,
            },
            &session(),
            ADMISSION_DEADLINE,
        )
        .await
        .unwrap()
        .manifest;
        let broca_wal::FetchOutcome::Answered {
            result_ops: Some(permissions),
            ..
        } = &manifest.fetches[0].outcome
        else {
            panic!("resolved permissions must be frozen")
        };
        assert_eq!(json!(permissions["edit"]), expected);
        assert!(manifest.declarations.is_empty());
        assert!(serde_json::to_value(&manifest)
            .unwrap()
            .get("declarations")
            .is_none());
    }
}

#[tokio::test]
async fn reply_validation_vectors_freeze_valid_declarations() {
    let vectors: Value = serde_json::from_str(include_str!(
        "fixtures/tool-provider-v1/reply-deadline.json"
    ))
    .unwrap();
    let parsed = parse_plan(
        &plan(json!([]), true),
        broca_fetch_plan::CompactionSupport::Unsupported,
    )
    .unwrap();
    for case in vectors["valid"].as_array().unwrap() {
        let manifest = admit(
            &parsed,
            &Catalog {
                result_ops: None,
                reply: Some(case["reply"].clone()),
                schema: Some(
                    case.get("input_schema")
                        .unwrap_or(&vectors["input_schema"])
                        .clone(),
                ),
            },
            &session(),
            ADMISSION_DEADLINE,
        )
        .await
        .unwrap()
        .manifest;
        let broca_wal::FetchOutcome::Answered { reply, .. } = &manifest.fetches[0].outcome else {
            panic!("answered fetch")
        };
        if case["reply"].is_null() {
            assert!(reply.is_none());
            assert!(serde_json::to_value(&manifest).unwrap()["fetches"][0]
                .get("reply")
                .is_none());
        } else {
            assert_eq!(
                json!(reply.as_ref().unwrap()["edit"]),
                case["reply"],
                "{}",
                case["name"]
            );
        }
        let decoded: broca_wal::PlanManifest =
            serde_json::from_slice(&serde_json::to_vec(&manifest).unwrap()).unwrap();
        assert_eq!(decoded, manifest);
    }
}
