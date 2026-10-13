//! Admission driven by the vendored fetch-plan vectors (`tests/fixtures/fetch-plan-v1`).
//!
//! Every `admission/*.json` case is run: its plan (named by file, inline, or inside a send), the
//! fetch answers it simulates, and the outcome it expects, admitted or refused with exactly the
//! code and detail it names. A provider a case does not mention answers exactly what the plan
//! composed for it, and declares exactly the subscriptions the plan froze for it. The plan and
//! composition files' canonical bytes and digests are checked against broca's own
//! canonicalisation, and the catalog requests broca builds against `catalog-requests.json`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use broca_fetch_plan::{
    baseline_facts, check_later_plan, check_send_members, parse_plan, plan_identity,
    DescribeAnswer, FetchError, Fetcher, Plan, Refusal, ADMISSION_DEADLINE,
};
use cortexkit_role_step_transform_provider::subscription::DeclareRequest;
use cortexkit_role_tool_provider::catalog::{schema_digest, CatalogRequest};
use serde_json::{json, Value};

/// Plan identities pinned as literals, copied from the vendored vectors' own records, so a
/// canonicaliser and a vector set that drifted together would still fail. Re-vendoring a newer
/// vector set updates these.
const PINNED_IDENTITIES: &[(&str, &str)] = &[
    (
        "plans/broca-head",
        "8f3ac238ef62c3ea5018135445beb0db6b568cdeba8674a2edadb5de03490022",
    ),
    (
        "compositions/broca-head",
        "e5d6c646c78f48fe16b1e3f3d1ee8a656f7bbd5447ba6b8b02a4837d44c4d3a7",
    ),
    (
        "plans/broca-head-no-compaction",
        "8f3ac238ef62c3ea5018135445beb0db6b568cdeba8674a2edadb5de03490022",
    ),
    (
        "compositions/broca-head-no-compaction",
        "e5d6c646c78f48fe16b1e3f3d1ee8a656f7bbd5447ba6b8b02a4837d44c4d3a7",
    ),
    (
        "plans/broca-head-text-reordered",
        "14531e80b0af6cc5340b03c5722a6499873cfe1fd31688ac39731c29986c8b92",
    ),
    (
        "compositions/broca-head-text-reordered",
        "e5d6c646c78f48fe16b1e3f3d1ee8a656f7bbd5447ba6b8b02a4837d44c4d3a7",
    ),
    (
        "plans/broca-worker",
        "479ae0f6a1fc32bff6bf61fa703812ca0f8065a9f185daee3a8f500721bc176a",
    ),
    (
        "plans/broca-worker-policy-absent",
        "27ca6fc3b94dd5339a1838f38e60b4ebaa695762e883f4ce4634e1b93e915de8",
    ),
    (
        "plans/broca-worker-policy-disabled",
        "a2c73f3cffcd89490d7436ca5a07c59324ac6ea2ee1e1e7d9dfb8c5c6d8ded70",
    ),
    (
        "compositions/broca-worker",
        "ae5f5d0a9238f443ea4676e31000c9233381a4c07ee962bd941c3d51fe933fb4",
    ),
    (
        "plans/broca-reader",
        "db88ab09d8fa1b168c25b44993adafa2e3f22b62014c337956ce0a9fafb000d2",
    ),
    (
        "compositions/broca-reader",
        "437daa8dd11309877f49113a646ac30242d36975fd0aa444b218b3fb198c9f3e",
    ),
    (
        "plans/text-only-and-hook-only",
        "c46f61d81594b3dd7fbd5161a3ed6f5c2ea97c015bee3d6c530cee7d2b89d60b",
    ),
    (
        "compositions/text-only-and-hook-only",
        "4fbb7040e057600736798e3def78aa85af81305cb1b1d217638f7bad8457b623",
    ),
    (
        "plans/broca-head-plexus-direct",
        "05766eb539d5c21cada0f403049c705639be4749915803d85952485713b86785",
    ),
    (
        "compositions/broca-head-plexus-direct",
        "9ce573eb40d0f5915ad1b1dd25a3e69c5c07713cefbdeac92f1135192367afd1",
    ),
    (
        "plans/broca-head-plexus-no-exclude",
        "b8445bb61d5aecf28a5c874e7c11edc0127a9db176a4906fb9160fb7860cb9f3",
    ),
    (
        "compositions/broca-head-plexus-no-exclude",
        "9c3f75fe0ab282e77cbb3f2da80041e425818be953ad286f7edbd65c0d5854c5",
    ),
    (
        "plans/broca-head-optional-text",
        "2364a41294deba1c7a283d4be2847b1e267badeac526eb584ef336e235b102b2",
    ),
    (
        "compositions/broca-head-optional-text",
        "c5128fb525aa4af8f05b3a7ee015b506e83ea26820aa04d3ab1ab2b437e56723",
    ),
    (
        "plans/broca-worker-unknown-provider",
        "618d80714127e85fd6181a90d4ac4a07578c8d358c2479533cbdbb226a9b0f9c",
    ),
    (
        "compositions/broca-worker-unknown-provider",
        "9255606990e8b29239d1920645b91078a9cb71d65076ede9f4b724b448cae376",
    ),
    (
        "plans/pre-tool-declared",
        "93cb52994ad60bdee3feac440532f51f2584861643b8732613f9feee6b87af1a",
    ),
    (
        "compositions/pre-tool-declared",
        "0482cc96fe3b2a69a8a81a10d43a23a18151ed81450d39c3d3d24fa82cb2189c",
    ),
    (
        "plans/pre-tool-tightened",
        "4bc9fbe2e74b02ac92ceac4ff7315f2fabe60ac85adf41856442bac1d5c0ae96",
    ),
    (
        "compositions/pre-tool-tightened",
        "12107bb71f007a0eb61bc98ebdee4bd0c6299967ccd576fe3807a8935f350dd9",
    ),
    (
        "plans/pre-tool-two-phase",
        "b9f8da8516ca579ac8a026df2e319fb976aaa99390edb80f40cfb37cf926fc42",
    ),
    (
        "compositions/pre-tool-two-phase",
        "269eea6f25a2e32d586de78e255fa49aa6debe8ce3d5545a6be2052298e59745",
    ),
];

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("fetch-plan-v1")
}

fn load(relative: &str) -> Value {
    let path = relative
        .split('/')
        .fold(root(), |path, part| path.join(part));
    serde_json::from_slice(&std::fs::read(&path).unwrap_or_else(|e| panic!("{path:?}: {e}")))
        .unwrap()
}

/// `plan` without its `compaction_item`. broca does not take a compaction provider, so it is
/// sent the same plan without one; a vector plan naming one stands for a runner that does.
fn without_compaction(mut plan: Value) -> Value {
    plan.as_object_mut().unwrap().remove("compaction_item");
    plan
}

fn carries_compaction(plan: &Value) -> bool {
    plan.get("compaction_item").is_some()
}

/// The vector plan at `relative`, as broca is sent it: without a compaction item.
fn broca_plan(relative: &str) -> Plan {
    parse_plan(
        &without_compaction(load(relative)),
        broca_fetch_plan::CompactionSupport::Unsupported,
    )
    .unwrap()
}

/// The refusal broca answers a well-formed plan that names a compaction provider with.
fn compaction_refusal() -> Value {
    json!({"field": "plan.compaction_item"})
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    sha2::Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Every `.json` with a `.jcs` and `.sha256` beside it: the `.jcs` is broca's RFC 8785 bytes of
/// the `.json`, and the `.sha256` their digest (a plan's identity, a composition's digest).
#[test]
fn every_digested_vector_matches_broca_canonicalisation() {
    let mut checked = 0;
    for folder in ["plans", "compositions"] {
        let mut stems: Vec<String> = std::fs::read_dir(root().join(folder))
            .unwrap()
            .flatten()
            .filter_map(|entry| {
                let name = entry.file_name().to_string_lossy().into_owned();
                name.strip_suffix(".json").map(str::to_owned)
            })
            .collect();
        stems.sort();
        for stem in stems {
            let base = root().join(folder);
            let value = load(&format!("{folder}/{stem}.json"));
            let jcs = std::fs::read(base.join(format!("{stem}.jcs"))).unwrap();
            let digest = std::fs::read_to_string(base.join(format!("{stem}.sha256"))).unwrap();
            assert_eq!(serde_jcs::to_vec(&value).unwrap(), jcs, "{folder}/{stem}");
            assert_eq!(sha256_hex(&jcs), digest, "{folder}/{stem}");
            assert_eq!(plan_identity(&value).unwrap(), digest, "{folder}/{stem}");
            if folder == "plans" {
                if value["composition"].get("compaction").is_some() {
                    assert_eq!(
                        parse_plan(&value, broca_fetch_plan::CompactionSupport::Unsupported)
                            .unwrap_err()
                            .detail(),
                        json!({"field": "plan.composition.compaction"}),
                        "{stem}"
                    );
                    checked += 1;
                    continue;
                }
                if carries_compaction(&value) {
                    assert_eq!(
                        parse_plan(&value, broca_fetch_plan::CompactionSupport::Unsupported)
                            .unwrap_err()
                            .detail(),
                        compaction_refusal(),
                        "{stem}"
                    );
                }
                let plan = parse_plan(
                    &without_compaction(value.clone()),
                    broca_fetch_plan::CompactionSupport::Unsupported,
                )
                .unwrap_or_else(|r| panic!("{stem}: {r:?}"));
                // A plan that only reorders another's texts shares that plan's composition and
                // has no composition file of its own.
                if root()
                    .join("compositions")
                    .join(format!("{stem}.json"))
                    .exists()
                {
                    let composition = load(&format!("compositions/{stem}.json"));
                    assert_eq!(
                        Value::Object(plan.composition.clone()),
                        composition,
                        "{stem}"
                    );
                }
            }
            checked += 1;
        }
    }
    assert!(checked >= 20, "only {checked} digested vectors found");
    for (stem, digest) in PINNED_IDENTITIES {
        let vector = root().join(format!("{stem}.json"));
        if vector.exists() {
            assert_eq!(
                plan_identity(&load(&format!("{stem}.json"))).unwrap(),
                *digest,
                "{stem}"
            );
        } else {
            let plan = load(&format!("plans/{}.json", stem.replace("compositions/", "")));
            assert_eq!(
                cortexkit_role_tool_provider::catalog::composition_digest(&plan["composition"])
                    .unwrap(),
                *digest,
                "{stem}"
            );
        }
    }
}

#[test]
fn reordering_system_texts_changes_the_identity_not_the_composition() {
    let original = load("plans/broca-head.json");
    let reordered = load("plans/broca-head-text-reordered.json");
    assert_eq!(
        original["composition_digest"],
        reordered["composition_digest"]
    );
    assert_ne!(
        plan_identity(&original).unwrap(),
        plan_identity(&reordered).unwrap()
    );
}

/// The catalog request broca builds for each `catalog-requests.json` case, from the plan whose
/// items name that provider with those values, is exactly the recorded request.
#[test]
fn catalog_requests_are_built_as_recorded() {
    let cases = load("catalog-requests.json");
    let plans: Vec<Plan> = std::fs::read_dir(root().join("plans"))
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with(".json"))
        .filter_map(|e| {
            let relative = format!("plans/{}", e.file_name().to_string_lossy());
            parse_plan(
                &without_compaction(load(&relative)),
                broca_fetch_plan::CompactionSupport::Unsupported,
            )
            .ok()
        })
        .collect();
    for (name, case) in cases.as_object().unwrap() {
        if case["request"]["composition"]["step_transform_items"]
            .as_array()
            .is_some_and(|items| !items.is_empty())
        {
            let plan = load("plans/pre-tool-declared.json");
            let refusal = parse_plan(
                &without_compaction(plan),
                broca_fetch_plan::CompactionSupport::Unsupported,
            )
            .unwrap_err();
            assert_eq!(refusal.code(), "invalid_params", "{name}");
            assert_eq!(
                refusal.detail(),
                json!({"field": "plan.composition.compaction"}),
                "{name}"
            );
            continue;
        }
        let provider = case["provider"].as_str().unwrap();
        let found = plans.iter().any(|plan| {
            let tool = plan.tool_items.iter().find(|i| i.provider == provider);
            let text = plan
                .system_text_items
                .iter()
                .find(|i| i.provider == provider);
            let request = broca_fetch_plan::admit::catalog_request(plan, tool, text);
            serde_json::to_value(request).unwrap() == case["request"]
        });
        assert!(found, "{name}: no plan builds the recorded request");
    }
}

/// Every admissible plan's tool-item params reach the catalog request as the plan wrote them,
/// nested objects and arrays included. Worker plans carry `remote_exec` (an object holding a
/// command list) and `siblings` (an array), which the tool provider reads to decide what a
/// worker may run remotely, so a dropped or reshaped member would change what the worker is
/// allowed to do.
#[test]
fn tool_item_params_reach_the_catalog_request_unchanged() {
    let mut nested = 0;
    let mut entries: Vec<_> = std::fs::read_dir(root().join("plans"))
        .unwrap()
        .flatten()
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".json"))
        .collect();
    entries.sort();
    for name in entries {
        let raw = load(&format!("plans/{name}"));
        let Ok(plan) = parse_plan(
            &without_compaction(raw.clone()),
            broca_fetch_plan::CompactionSupport::Unsupported,
        ) else {
            continue;
        };
        for (index, item) in raw["tool_items"]
            .as_array()
            .into_iter()
            .flatten()
            .enumerate()
        {
            let parsed = &plan.tool_items[index];
            let request = broca_fetch_plan::admit::catalog_request(&plan, Some(parsed), None);
            let sent = serde_json::to_value(&request).unwrap();
            assert_eq!(sent["params"], item["params"], "{name} tool item {index}");
            if item["params"].get("remote_exec").is_some() {
                nested += 1;
            }
        }
    }
    assert!(
        nested >= 2,
        "expected the vendored worker plans to carry remote_exec params, found {nested}"
    );
}

/// The send vectors carry plans broca accepts beside the send's other members.
#[test]
fn send_vectors_carry_admissible_plans() {
    for send in [
        "sends/broca-head-first-send.json",
        "sends/broca-worker-first-send.json",
    ] {
        let send = load(send);
        check_send_members(send.as_object().unwrap()).unwrap();
        parse_plan(
            &without_compaction(send["plan"].clone()),
            broca_fetch_plan::CompactionSupport::Unsupported,
        )
        .unwrap();
    }
    assert_eq!(
        load("sends/broca-head-first-send.json")["plan"],
        load("plans/broca-head.json")
    );
}

/// Each worker plan and the worker's first send resolve exactly the tool list frozen in the vector.
#[tokio::test]
async fn broca_worker_plans_and_send_resolve_the_vector_tool_list() {
    const WORKER_TOOLS: [&str; 8] = [
        "bash",
        "edit",
        "read",
        "ask",
        "browser_use",
        "computer_use",
        "knowhow",
        "sidekick",
    ];
    let mut plans = vec![
        load("plans/broca-worker.json"),
        load("plans/broca-worker-policy-absent.json"),
        load("plans/broca-worker-policy-disabled.json"),
        load("plans/broca-worker-unknown-provider.json"),
    ];
    let send = load("sends/broca-worker-first-send.json");
    check_send_members(send.as_object().unwrap()).unwrap();
    assert_eq!(send["plan"], load("plans/broca-worker.json"));
    plans.push(send["plan"].clone());

    for raw in plans {
        let plan = parse_plan(&raw, broca_fetch_plan::CompactionSupport::Unsupported).unwrap();
        let case = json!({});
        let fetcher = Simulated::new(plan.clone(), &case);
        let admitted = admit(&plan, &fetcher, ADMISSION_DEADLINE)
            .await
            .unwrap_or_else(|refusal| panic!("worker vector refused: {refusal:?}"));
        let tools: Vec<&str> = admitted
            .manifest
            .tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect();
        assert_eq!(tools, WORKER_TOOLS);
        assert!(tools.contains(&"sidekick"));
    }
}

/// Answers the fetches of one admission case.
struct Simulated {
    plan: Plan,
    fetches: BTreeMap<String, Value>,
    declarations: BTreeMap<String, Value>,
}

impl Simulated {
    fn new(plan: Plan, case: &Value) -> Self {
        let by_provider = |key: &str| {
            case.get(key)
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .map(|entry| {
                    (
                        entry["provider"].as_str().unwrap().to_owned(),
                        entry.clone(),
                    )
                })
                .collect()
        };
        Self {
            plan,
            fetches: by_provider("fetches"),
            declarations: by_provider("declarations"),
        }
    }

    /// The answer a provider gives when it answers exactly what the plan composed for it, with
    /// any member the case overrides replaced.
    fn answer(&self, provider: &str, request: &CatalogRequest, overrides: &Value) -> Value {
        let composed = &self.plan.composed[provider];
        let names: Vec<String> = match overrides.get("tool_names") {
            Some(names) => serde_json::from_value(names.clone()).unwrap(),
            None => composed.iter().map(|t| t.name.clone()).collect(),
        };
        let tools: Vec<Value> = names
            .iter()
            .map(|name| {
                let capabilities = overrides
                    .get("tools")
                    .and_then(Value::as_array)
                    .and_then(|tools| tools.iter().find(|t| t["name"] == name.as_str()))
                    .map(|t| t["capabilities"].clone())
                    .unwrap_or_else(|| {
                        json!(composed
                            .iter()
                            .find(|t| &t.name == name)
                            .map(|t| t.capabilities.clone())
                            .unwrap_or_default())
                    });
                let schema = json!({"type": "object", "properties": {}});
                json!({
                    "name": name,
                    "schema_digest": schema_digest(&schema).unwrap(),
                    "semantics": 1,
                    "capabilities": capabilities,
                    "description": format!("{provider} {name}"),
                    "input_schema": schema,
                })
            })
            .collect();
        let digest = overrides
            .get("composition_digest")
            .cloned()
            .unwrap_or_else(|| json!(self.plan.composition_digest));
        let mut answer = json!({
            "generation": "1",
            "catalog_digest": sha256_hex(provider.as_bytes()),
            "composition_digest": digest,
            "tools": tools,
        });
        if request.system_text.is_some() {
            let text = format!("{provider} system text");
            let text_overrides = overrides.get("system_text").cloned().unwrap_or(json!({}));
            let mut tool_names = names.clone();
            tool_names.sort();
            answer["system_text"] = json!({
                "text": text,
                "item_digest": sha256_hex(text.as_bytes()),
                "preflight_digest": "preflight",
                "composition_digest": text_overrides
                    .get("composition_digest")
                    .cloned()
                    .unwrap_or_else(|| json!(self.plan.composition_digest)),
                "tool_names": text_overrides.get("tool_names").cloned().unwrap_or(json!(tool_names)),
            });
        }
        answer
    }
}

#[async_trait]
impl Fetcher for Simulated {
    async fn catalog(&self, provider: &str, request: &CatalogRequest) -> Result<Value, FetchError> {
        let Some(fetch) = self.fetches.get(provider) else {
            return Ok(self.answer(provider, request, &json!({})));
        };
        if fetch.get("route_open").and_then(Value::as_str) == Some("unknown_module") {
            return Err(FetchError::UnknownModule);
        }
        match fetch.get("reason").and_then(Value::as_str) {
            Some("timeout") => std::future::pending().await,
            Some("refused") => Err(FetchError::Refused {
                code: fetch["provider_code"].as_str().unwrap().to_owned(),
                detail: None,
                retryable: false,
            }),
            Some(other) => panic!("unknown simulated reason {other}"),
            None => Ok(self.answer(provider, request, &fetch["answer"])),
        }
    }

    async fn describe_compaction(&self, _: &str) -> Result<DescribeAnswer, FetchError> {
        panic!("broca's vector plans are admitted without their compaction item")
    }

    async fn declare(&self, provider: &str, request: &DeclareRequest) -> Result<Value, FetchError> {
        let step = self
            .plan
            .step_items
            .iter()
            .find(|step| step.item.provider == provider)
            .unwrap();
        match self.declarations.get(provider) {
            None => Ok(json!({"subscriptions": step.subscriptions})),
            Some(declaration) if declaration["preset"].as_str() != request.preset.as_deref() => {
                Err(FetchError::Refused {
                    code: "invalid_params".into(),
                    detail: Some(json!({"field": "preset"})),
                    retryable: false,
                })
            }
            Some(declaration) => Ok(declaration["answer"].clone()),
        }
    }
}

/// The plan value a case admits: its named plan file, its inline plan, or its send's plan. A
/// case naming a send first checks the send's other members.
fn case_plan(case: &Value) -> Result<Value, Refusal> {
    if let Some(send) = case.get("send_inline") {
        check_send_members(send.as_object().unwrap())?;
        return Ok(send["plan"].clone());
    }
    if let Some(plan) = case.get("plan_inline") {
        return Ok(plan.clone());
    }
    Ok(load(case["plan"].as_str().unwrap()))
}

/// What admission answers for a first-send case: the manifest, or the refusal.
async fn run_first_send(case: &Value) -> Result<broca_fetch_plan::Admitted, Refusal> {
    run_first_send_of(case, case_plan(case)?).await
}

/// [`run_first_send`] with the case's plan replaced by `plan`.
async fn run_first_send_of(
    case: &Value,
    plan: Value,
) -> Result<broca_fetch_plan::Admitted, Refusal> {
    let plan = parse_plan(&plan, broca_fetch_plan::CompactionSupport::Unsupported)?;
    if let Some(composition) = case.get("composition").and_then(Value::as_str) {
        if case.get("plan_inline").is_none() {
            assert_eq!(Value::Object(plan.composition.clone()), load(composition));
        }
    }
    let fetcher = Simulated::new(plan.clone(), case);
    admit(&plan, &fetcher, ADMISSION_DEADLINE).await
}

fn test_session() -> broca_protocol::BindIdentity {
    broca_protocol::BindIdentity::new("/tmp/fetch-plan-vectors", "test", "vectors")
}

async fn admit(
    plan: &Plan,
    fetcher: &dyn Fetcher,
    deadline: Duration,
) -> Result<broca_fetch_plan::Admitted, Refusal> {
    broca_fetch_plan::admit(plan, fetcher, &test_session(), deadline).await
}

/// What a later-send case answers: the frozen plan is the first send's (none for a plan-less
/// first send), and the later plan is checked against it.
fn run_later_send(case: &Value) -> Result<(), Refusal> {
    let frozen = match case.get("first_send") {
        Some(send) => Some(plan_identity(&load(send.as_str().unwrap())["plan"]).unwrap()),
        None => {
            assert!(case["first_send_inline"].get("plan").is_none());
            None
        }
    };
    let sent = plan_identity(&load(case["later_plan"].as_str().unwrap())).unwrap();
    check_later_plan(frozen.as_deref(), &sent)
}

fn check_refusal(name: &str, refusal: &Refusal, expected: &Value) {
    assert_eq!(refusal.code(), expected["code"], "{name}: {refusal:?}");
    assert_eq!(refusal.detail(), expected["detail"], "{name}");
    if let Some(class) = expected.get("class") {
        assert_eq!(class, "transient", "{name}");
        assert!(
            refusal.retryable(),
            "{name}: a transient refusal is retryable"
        );
    } else {
        assert!(!refusal.retryable(), "{name}");
    }
    assert_eq!(expected["writes"], "nothing", "{name}");
}

/// The commons hook vectors include compaction composition data for runners that support
/// it. Remove only that unsupported member; the subscriptions and expected differences stay
/// exactly as recorded.
#[tokio::test]
async fn loosened_subscriptions_match_the_exact_commons_differences() {
    for name in [
        "refuse-subscription-loosened-budget_ms.json",
        "refuse-subscription-loosened-on_unavailable.json",
        "refuse-subscription-loosened-ops.json",
        "refuse-subscription-loosened-tools.json",
        "refuse-subscription-loosened-validate-phase.json",
        "refuse-declared-hook-gone.json",
        "refuse-declared-preset-gone.json",
        "refuse-declared-preset-gone-two-subscriptions.json",
    ] {
        let case = load(&format!("admission/{name}"));
        let mut raw = without_compaction(case_plan(&case).unwrap());
        raw["composition"]
            .as_object_mut()
            .unwrap()
            .remove("compaction");
        raw["composition_digest"] = json!(
            cortexkit_role_tool_provider::catalog::composition_digest(&raw["composition"]).unwrap()
        );
        let plan = parse_plan(&raw, broca_fetch_plan::CompactionSupport::Unsupported).unwrap();
        let fetcher = Simulated::new(plan.clone(), &case);
        let refusal = admit(&plan, &fetcher, ADMISSION_DEADLINE)
            .await
            .unwrap_err();
        check_refusal(name, &refusal, &case["expected"]);
    }
}

/// Each JSON admission case for a runner that supports neither hooks nor compaction matches its expected result.
#[tokio::test(start_paused = true)]
async fn every_admission_vector_holds() {
    let mut names: Vec<String> = std::fs::read_dir(root().join("admission"))
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".json"))
        .collect();
    names.sort();
    assert!(names.len() >= 30, "only {} admission vectors", names.len());
    let mut skipped = Vec::new();
    for name in names {
        let case = load(&format!("admission/{name}"));
        if case["runner_declaration"] != json!({"hooks": false, "compaction": false}) {
            skipped.push(name);
            continue;
        }
        let expected = &case["expected"];
        if case.get("later_plan").is_some() {
            // A first send whose plan names a compaction provider is refused by broca before
            // anything is frozen; the identity comparison the case records is checked as is.
            if let Some(send) = case.get("first_send") {
                let first = &load(send.as_str().unwrap())["plan"];
                if carries_compaction(first) {
                    assert_eq!(
                        parse_plan(first, broca_fetch_plan::CompactionSupport::Unsupported)
                            .unwrap_err()
                            .detail(),
                        compaction_refusal(),
                        "{name}"
                    );
                }
            }
            match run_later_send(&case) {
                Ok(()) => {
                    assert_eq!(expected["admitted"], true, "{name}");
                    assert_eq!(expected["no_op"], true, "{name}");
                }
                Err(refusal) => check_refusal(&name, &refusal, expected),
            }
            continue;
        }
        // The vectors record what a runner that takes a compaction provider answers. broca does
        // not take one, so a well-formed plan naming one is refused `plan.compaction_item`
        // (a plan malformed in itself, or a send carrying a member the plan replaces, is refused
        // for that first, as recorded). Everything else the case pins is checked on the same
        // plan without its compaction item.
        let plan = case_plan(&case);
        let refused_first_as_recorded = expected["code"] == "invalid_params";
        if let Ok(plan) = &plan {
            if carries_compaction(plan) && !refused_first_as_recorded {
                let refusal = run_first_send(&case).await.unwrap_err();
                assert_eq!(refusal.code(), "invalid_params", "{name}");
                assert_eq!(refusal.detail(), compaction_refusal(), "{name}");
            }
        }
        let started = tokio::time::Instant::now();
        let outcome = match plan {
            Ok(plan) if !refused_first_as_recorded => {
                run_first_send_of(&case, without_compaction(plan)).await
            }
            _ => run_first_send(&case).await,
        };
        assert!(
            started.elapsed() <= ADMISSION_DEADLINE,
            "{name}: admission ran past its deadline"
        );
        match outcome {
            Ok(admitted) => {
                assert_eq!(expected["admitted"], true, "{name}");
                let facts = baseline_facts(&admitted.manifest);
                let absent = expected.get("absent_items").cloned().unwrap_or(json!([]));
                assert_eq!(json!(facts.absent_items), absent, "{name}");
                if let Some(frozen) = expected.get("frozen_step_transform_items") {
                    let plan: Value =
                        serde_json::from_str(admitted.manifest.plan.as_json_text()).unwrap();
                    assert_eq!(&plan["step_transform_items"], frozen, "{name}");
                }
            }
            Err(refusal) => {
                assert_eq!(expected["admitted"], false, "{name}: {refusal:?}");
                check_refusal(&name, &refusal, expected);
            }
        }
    }
    for twin in [
        "refuse-runner-without-hooks-pre-tool-declared.json",
        "refuse-runner-without-hooks-pre-tool-tightened.json",
        "refuse-runner-without-hooks-pre-tool-two-phase.json",
        "refuse-runner-without-hooks-text-only-and-hook-only.json",
    ] {
        assert!(
            skipped.iter().any(|name| name == twin),
            "missing skipped twin {twin}; skipped: {skipped:?}"
        );
    }
    assert!(
        !skipped.is_empty(),
        "runner-declaration filter selected every vector; skipped: {skipped:?}"
    );
}

/// A timed-out fetch takes the whole admission deadline and no longer.
#[tokio::test(start_paused = true)]
async fn a_timeout_waits_out_exactly_the_deadline() {
    let case = load("admission/refuse-required-timeout.json");
    let plan = case_plan(&case).unwrap();
    let started = tokio::time::Instant::now();
    let refusal = run_first_send_of(&case, without_compaction(plan))
        .await
        .unwrap_err();
    assert_eq!(refusal.code(), "fetch_unavailable");
    assert_eq!(started.elapsed(), ADMISSION_DEADLINE);
}

/// A retryable refusal is retried inside the deadline until the provider answers.
#[tokio::test(start_paused = true)]
async fn a_retryable_refusal_is_retried_inside_the_deadline() {
    struct Flaky {
        inner: Simulated,
        refusals_left: std::sync::Mutex<u32>,
    }
    #[async_trait]
    impl Fetcher for Flaky {
        async fn catalog(
            &self,
            provider: &str,
            request: &CatalogRequest,
        ) -> Result<Value, FetchError> {
            {
                let mut left = self.refusals_left.lock().unwrap();
                if provider == "aft" && *left > 0 {
                    *left -= 1;
                    return Err(FetchError::Refused {
                        code: "transient".into(),
                        detail: None,
                        retryable: true,
                    });
                }
            }
            self.inner.catalog(provider, request).await
        }
        async fn declare(
            &self,
            provider: &str,
            request: &DeclareRequest,
        ) -> Result<Value, FetchError> {
            self.inner.declare(provider, request).await
        }
        async fn describe_compaction(&self, provider: &str) -> Result<DescribeAnswer, FetchError> {
            self.inner.describe_compaction(provider).await
        }
    }
    let plan = broca_plan("plans/broca-head-no-compaction.json");
    let flaky = Flaky {
        inner: Simulated::new(plan.clone(), &json!({})),
        refusals_left: std::sync::Mutex::new(3),
    };
    admit(&plan, &flaky, ADMISSION_DEADLINE).await.unwrap();
    assert_eq!(*flaky.refusals_left.lock().unwrap(), 0);

    // A provider that keeps refusing with a retryable code until the deadline: admission ends
    // `fetch_unavailable` carrying that last refusal's code.
    let always = Flaky {
        inner: Simulated::new(plan.clone(), &json!({})),
        refusals_left: std::sync::Mutex::new(u32::MAX),
    };
    let refusal = admit(&plan, &always, Duration::from_secs(2))
        .await
        .unwrap_err();
    assert_eq!(
        refusal.detail(),
        json!({"provider": "aft", "reason": "refused", "provider_code": "transient"})
    );
}

#[tokio::test(start_paused = true)]
async fn hook_only_manifest_vector_refuses_before_fetch() {
    let value = load("plans/text-only-and-hook-only.json");
    let refusal = parse_plan(&value, broca_fetch_plan::CompactionSupport::Unsupported).unwrap_err();
    assert_eq!(refusal.code(), "invalid_params");
    assert_eq!(
        refusal.detail(),
        json!({"field": "plan.composition.compaction"})
    );
}

/// Texts are joined in plan order with the fixed separator. An empty text adds neither bytes nor
/// a separator, wherever it falls.
#[test]
fn an_empty_text_adds_no_bytes_and_no_separator() {
    use broca_fetch_plan::join_system_texts;
    assert_eq!(join_system_texts(["a", "b"]), "a\n\nb");
    assert_eq!(join_system_texts(["a", "", "b"]), "a\n\nb");
    assert_eq!(join_system_texts(["", "a"]), "a");
    assert_eq!(join_system_texts(["a", ""]), "a");
    assert_eq!(join_system_texts(["", ""]), "");
}

/// An empty fetched text is recorded as present and empty, distinct from an absent item.
/// (That neither adds bytes to the joined text is `an_empty_text_adds_no_bytes_and_no_separator`.)
#[tokio::test(start_paused = true)]
async fn an_empty_text_is_recorded_empty_and_an_absent_one_absent() {
    struct EmptyText(Simulated);
    #[async_trait]
    impl Fetcher for EmptyText {
        async fn catalog(
            &self,
            provider: &str,
            request: &CatalogRequest,
        ) -> Result<Value, FetchError> {
            let mut answer = self.0.catalog(provider, request).await?;
            if provider == "aft" {
                answer["system_text"]["text"] = json!("");
                answer["system_text"]["item_digest"] = json!(sha256_hex(b""));
            }
            Ok(answer)
        }
        async fn declare(
            &self,
            provider: &str,
            request: &DeclareRequest,
        ) -> Result<Value, FetchError> {
            self.0.declare(provider, request).await
        }
        async fn describe_compaction(&self, provider: &str) -> Result<DescribeAnswer, FetchError> {
            self.0.describe_compaction(provider).await
        }
    }
    let case = load("admission/admit-optional-text-timeout.json");
    let plan = broca_plan(case["plan"].as_str().unwrap());
    let fetcher = EmptyText(Simulated::new(plan.clone(), &case));
    let manifest = admit(&plan, &fetcher, ADMISSION_DEADLINE)
        .await
        .unwrap()
        .manifest;
    let outcome = |provider: &str| {
        manifest
            .fetches
            .iter()
            .find(|f| f.provider == provider)
            .unwrap()
            .outcome
            .clone()
    };
    assert!(matches!(
        outcome("aft"),
        broca_wal::FetchOutcome::Answered { text: Some(text), .. } if text.is_empty()
    ));
    assert_eq!(
        outcome("notes-provider"),
        broca_wal::FetchOutcome::Absent {
            reason: "timeout".into()
        }
    );
}

/// A system text that does not hash to the item digest it was answered with is not used: the
/// required item's fetch is refused naming the provider.
#[tokio::test(start_paused = true)]
async fn a_text_that_does_not_match_its_item_digest_refuses() {
    struct WrongDigest(Simulated);
    #[async_trait]
    impl Fetcher for WrongDigest {
        async fn catalog(
            &self,
            provider: &str,
            request: &CatalogRequest,
        ) -> Result<Value, FetchError> {
            let mut answer = self.0.catalog(provider, request).await?;
            if provider == "aft" {
                answer["system_text"]["item_digest"] = json!(sha256_hex(b"other"));
            }
            Ok(answer)
        }
        async fn declare(
            &self,
            provider: &str,
            request: &DeclareRequest,
        ) -> Result<Value, FetchError> {
            self.0.declare(provider, request).await
        }
        async fn describe_compaction(&self, provider: &str) -> Result<DescribeAnswer, FetchError> {
            self.0.describe_compaction(provider).await
        }
    }
    let plan = broca_plan("plans/broca-head-no-compaction.json");
    let refusal = admit(
        &plan,
        &WrongDigest(Simulated::new(plan.clone(), &json!({}))),
        ADMISSION_DEADLINE,
    )
    .await
    .unwrap_err();
    assert_eq!(
        refusal.detail(),
        json!({"provider": "aft", "reason": "refused", "provider_code": "item_digest_mismatch"})
    );
}

/// The persona item is never optional, and only a system-text item may carry `optional`.
#[test]
fn the_persona_item_is_never_optional() {
    let mut plan = load("plans/broca-head.json");
    assert_eq!(plan["system_text_items"][0]["preset"], "head");
    plan["system_text_items"][0]["optional"] = json!(true);
    assert_eq!(
        parse_plan(&plan, broca_fetch_plan::CompactionSupport::Unsupported)
            .unwrap_err()
            .detail(),
        json!({"field": "plan.system_text_items[0].optional"})
    );
}

/// Fetched texts are joined byte for byte: a text's own leading and trailing whitespace,
/// including a final newline, is kept as fetched (no trimming, no newline normalisation), the
/// separator is added only between texts, and the system digest covers exactly the joined
/// bytes. Each text is also recorded exactly as fetched.
#[tokio::test(start_paused = true)]
async fn texts_are_joined_exactly_as_fetched() {
    struct Spaced(Simulated);
    fn text_of(provider: &str) -> String {
        match provider {
            "prefrontal-core" => "You are the head.\n".into(),
            "aft" => "  Read before you edit.\r\n\n".into(),
            other => format!("{other} text\n"),
        }
    }
    #[async_trait]
    impl Fetcher for Spaced {
        async fn catalog(
            &self,
            provider: &str,
            request: &CatalogRequest,
        ) -> Result<Value, FetchError> {
            let mut answer = self.0.catalog(provider, request).await?;
            if answer.get("system_text").is_some() {
                let text = text_of(provider);
                answer["system_text"]["item_digest"] = json!(sha256_hex(text.as_bytes()));
                answer["system_text"]["text"] = json!(text);
            }
            Ok(answer)
        }
        async fn declare(
            &self,
            provider: &str,
            request: &DeclareRequest,
        ) -> Result<Value, FetchError> {
            self.0.declare(provider, request).await
        }
        async fn describe_compaction(&self, provider: &str) -> Result<DescribeAnswer, FetchError> {
            self.0.describe_compaction(provider).await
        }
    }
    let plan = broca_plan("plans/broca-head-no-compaction.json");
    let fetcher = Spaced(Simulated::new(plan.clone(), &json!({})));
    let manifest = admit(&plan, &fetcher, ADMISSION_DEADLINE)
        .await
        .unwrap()
        .manifest;
    let expected = "You are the head.\n\n\n  Read before you edit.\r\n\n\n\nmagic-context text\n";
    assert_eq!(manifest.system_text, expected);
    assert_eq!(manifest.system_digest, sha256_hex(expected.as_bytes()));
    for fetch in &manifest.fetches {
        let broca_wal::FetchOutcome::Answered { text, .. } = &fetch.outcome else {
            panic!("{} answered", fetch.provider);
        };
        assert_eq!(text.as_deref(), Some(text_of(&fetch.provider).as_str()));
    }
}
