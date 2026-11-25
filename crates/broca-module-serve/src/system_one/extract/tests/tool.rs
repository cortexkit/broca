use std::{collections::BTreeMap, sync::atomic::Ordering};

use broca_store::grants::{GrantsStore, HeldState};
use broca_subc::{ConsentRecord, ConsentState};
use cortexkit_llm_tools::{errors::RefusalSpend, extract as contract};

use super::*;
use crate::{
    config::LlmToolsConfig,
    llm_tools::{
        consent::Consent,
        held::HeldCalls,
        held_images_tests::{principal, stamp, Io, Marked, Source},
        ConsumerFormat, LlmTools, NoPrivateContent, TOOL_RESULT_V1,
    },
};

const TOOL: &str = "llm_extract";
const FORMAT: ConsumerFormat = ConsumerFormat {
    result_format: Some(TOOL_RESULT_V1),
    held_reply: true,
};

fn arguments(state: &str, cap: f64) -> Value {
    json!({
        "model":CLAIRIC, "fields":{"total":{"type":"number"}},
        "item":{"state":state}, "options":{"state_text":true}, "max_cost_usd":cap
    })
}

fn tools(rig: &Rig) -> Arc<LlmTools> {
    Arc::new(LlmTools::new(
        Arc::new(rig.service.fork_for_tools()),
        LlmToolsConfig {
            private_content_providers: vec!["cloudflare-workers-ai".into()],
            advertise_tags: true,
            ..Default::default()
        },
        Arc::new(NoPrivateContent),
    ))
}

async fn route(tools: &Arc<LlmTools>, body: Value) -> Result<Box<RawValue>, Vec<u8>> {
    tools
        .answer_for_consumer(
            &serde_json::to_vec(&body).unwrap(),
            Some(&principal()),
            &stamp(),
            FORMAT,
        )
        .await
}

async fn call(
    tools: &Arc<LlmTools>,
    key: &str,
    arguments: Value,
) -> Result<Box<RawValue>, Vec<u8>> {
    route(
        tools,
        json!({"name":TOOL,"call_key":key,"arguments":arguments}),
    )
    .await
}

fn refusal(body: &[u8], code: &str, spend: RefusalSpend) -> Refusal {
    let refusal: Refusal = serde_json::from_slice(body).unwrap();
    assert_eq!(refusal.code, code, "{}", String::from_utf8_lossy(body));
    assert_eq!(
        cortexkit_llm_tools::errors::refusal_spend(&refusal),
        Ok(spend)
    );
    refusal
}

#[tokio::test]
async fn llm_extract_route_replies_and_samples() {
    let rig = Rig::new().await;
    let tools = tools(&rig);
    let catalog = route(&tools, json!({"name":"tool.catalog", "arguments":{}}))
        .await
        .unwrap();
    let catalog: Value = serde_json::from_str(catalog.get()).unwrap();
    let listing = catalog["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == TOOL)
        .expect("catalog must list llm_extract when Clairic is served");
    assert_eq!(listing["capabilities"], json!([contract::TAG]));
    assert_eq!(listing["modes"], json!(["codemode", "flow"]));
    assert_eq!(listing["result_format"], TOOL_RESULT_V1);
    assert_eq!(listing["input_schema"], contract::input_schema());

    const STATE: &str = "The invented total is 123.";
    const USAGE: &str = r#", "usage":{"input_tokens":10,"output_tokens":5,"passes":1}"#;
    rig.script_ok(
        STATE,
        r#"{"total":{"type":"number","value":1.2300e+2,"new_member":0.0100}}"#,
        USAGE,
    );
    let args = arguments(STATE, 0.02);
    let success = call(&tools, "extract-success", args.clone())
        .await
        .unwrap_or_else(|body| panic!("extract success: {}", String::from_utf8_lossy(&body)));
    #[derive(Deserialize)]
    struct ToolResult<'a> {
        #[serde(borrow, rename = "structuredContent")]
        structured: &'a RawValue,
    }
    let wrapped: ToolResult<'_> = serde_json::from_str(success.get()).unwrap();
    let result: contract::Result = serde_json::from_str(wrapped.structured.get()).unwrap();
    contract::validate_result(&result).unwrap();
    assert!(wrapped
        .structured
        .get()
        .contains(r#""value":1.2300e+2,"new_member":0.0100"#));
    assert_eq!(result.cost_usd.as_f64(), Some(0.0000011));
    assert_eq!(result.truncated, Some(false));
    assert_eq!(result.state_text.as_deref(), Some(STATE));
    assert_eq!(result.usage.input_tokens, Some(10));
    assert_eq!(result.usage.output_tokens, Some(5));

    let large = "7".repeat(MAX_ANSWER_BYTES as usize + 1);
    rig.script_ok(
        "paid oversized answer",
        &format!(r#"{{"total":{{"type":"number","value":{large}}}}}"#),
        USAGE,
    );
    let paid = call(
        &tools,
        "extract-paid",
        arguments("paid oversized answer", 0.02),
    )
    .await
    .unwrap_err();
    let marked = refusal(&paid, "answer_too_large", RefusalSpend::Measured(0.0000011));
    assert_eq!(marked.detail.class, errors::ErrorClass::Permanent);
    assert_eq!(rig.cloudflare.calls("paid oversized answer"), 1);
    let preflight = call(&tools, "extract-ceiling", arguments("unused", 0.0))
        .await
        .unwrap_err();
    let marked = refusal(&preflight, "cost_exceeded", RefusalSpend::NoSpend);
    assert_eq!(marked.detail.class, errors::ErrorClass::Permanent);
    assert_eq!(rig.cloudflare.calls("unused"), 0);

    rig.rows.0.lock().unwrap().clear();
    let replay = call(&tools, "extract-success", args).await.unwrap();
    assert_eq!(success.get(), replay.get());
    assert_eq!(
        call(
            &tools,
            "extract-paid",
            arguments("paid oversized answer", 0.02)
        )
        .await
        .unwrap_err(),
        paid
    );
    assert_eq!(
        call(&tools, "extract-ceiling", arguments("unused", 0.0))
            .await
            .unwrap_err(),
        preflight
    );
    assert_eq!(rig.cloudflare.received().len(), 2);
    let facts = rig.export.read_export_page(None, 100).unwrap().facts;
    assert_eq!(facts.len(), 2);
    for fact in facts {
        let fact = serde_json::to_value(fact.segment).unwrap();
        assert_eq!(fact["run_id"], "run");
        assert!(fact.get("flow_id").is_none());
        assert_eq!(fact["usage"], json!({"input_tokens":10,"output_tokens":5}));
    }

    if let Some(dir) = std::env::var_os("BROCA_EXTRACT_REPLY_SAMPLES") {
        let dir = std::path::PathBuf::from(dir);
        std::fs::create_dir_all(&dir).unwrap();
        for (name, body) in [
            ("extract-success.ok.json", success.get().as_bytes()),
            ("extract-paid.refused.json", paid.as_slice()),
            ("extract-ceiling.refused.json", preflight.as_slice()),
            ("extract-replay.ok.json", replay.get().as_bytes()),
        ] {
            std::fs::write(dir.join(name), body).unwrap();
        }
    }
}

#[tokio::test]
async fn llm_extract_holds_a_marked_run() {
    let mut rig = Rig::new().await;
    let source = Arc::new(Source {
        present: true.into(),
        calls: 0.into(),
        scopes: Mutex::default(),
    });
    rig.service = rig.service.with_image_resolver(source.clone());
    {
        let mut rows = rig.rows.0.lock().unwrap();
        let clairic = rows.get_mut(CLAIRIC).unwrap();
        clairic.model_name = Some("Clairic".into());
        clairic.provider_name = Some("Cloudflare Workers AI".into());
    }
    let store = Arc::new(GrantsStore::open_in_memory().unwrap());
    let stop = tokio_util::sync::CancellationToken::new();
    // Deliver the consent answer explicitly rather than racing a card watcher.
    stop.cancel();
    let io = Arc::new(Io::default());
    let consent = Arc::new(Consent::new(store.clone(), io.clone()));
    let tools = Arc::new(
        LlmTools::new(
            Arc::new(rig.service.fork_for_tools()),
            LlmToolsConfig::default(),
            Arc::new(Marked),
        )
        .with_held(Arc::new(HeldCalls::new(store.clone(), "test".into())))
        .with_consent(consent.clone(), stop),
    );
    let catalog = route(&tools, json!({"name":"tool.catalog","arguments":{}}))
        .await
        .unwrap();
    let catalog: Value = serde_json::from_str(catalog.get()).unwrap();
    let listing = catalog["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == TOOL)
        .unwrap();
    assert_eq!(
        listing["input_schema"],
        contract::input_schema_with_image_references()
    );
    let mut args = arguments("Private content", 0.02);
    args["item"]["images"] = json!([{"handle":"shot:1"}]);
    let response = call(&tools, "extract-hold", args.clone())
        .await
        .unwrap_or_else(|body| panic!("extract consent: {}", String::from_utf8_lossy(&body)));
    let held: Value = serde_json::from_str(response.get()).unwrap();
    assert_eq!(held["held"]["reason"], "consent");
    assert_eq!(
        held["held"]["detail"],
        json!({"call_key":"extract-hold", "ask_id":"card"})
    );
    assert_eq!(
        call(&tools, "extract-hold", args.clone())
            .await
            .unwrap()
            .get(),
        response.get()
    );
    assert!(rig.cloudflare.received().is_empty());

    source.present.store(false, Ordering::SeqCst);
    rig.script_ok(
        "Private content",
        r#"{"total":{"type":"number","value":1.2300e+2}}"#,
        r#", "usage":{"input_tokens":10}"#,
    );
    let row = store
        .recovering(rig.service.tool_now())
        .unwrap()
        .pop()
        .unwrap();
    *io.answer.lock().unwrap() = Some(ConsentRecord {
        elicitation_id: "card".into(),
        state: ConsentState::Answered,
        fields: BTreeMap::from([
            ("requester_ref".into(), json!(row.dedup_key)),
            ("answered_choice_id".into(), json!("allow_run")),
        ]),
    });
    consent
        .wait_once(&row.dedup_key, rig.service.tool_now())
        .await
        .unwrap();
    rig.rows.0.lock().unwrap().clear();
    tools.recover_authorized().unwrap();
    broca_test_support::wait_until("the approved extraction to settle", || async {
        let current = store.lookup(&row.batch_id, rig.service.tool_now()).unwrap();
        if current
            .as_ref()
            .is_some_and(|row| row.state == HeldState::Final)
        {
            Ok(())
        } else {
            Err(format!("current state: {:?}", current.map(|row| row.state)))
        }
    })
    .await;
    let late = route(&tools, json!({"name":"late_results","arguments":{}}))
        .await
        .unwrap();
    #[derive(Deserialize)]
    struct Page<'a> {
        #[serde(borrow)]
        entries: Vec<Entry<'a>>,
    }
    #[derive(Deserialize)]
    struct Entry<'a> {
        #[serde(borrow)]
        result: &'a RawValue,
    }
    let page: Page<'_> = serde_json::from_str(late.get()).unwrap();
    assert_eq!(page.entries.len(), 1);
    let replay = call(&tools, "extract-hold", args).await.unwrap();
    assert_eq!(page.entries[0].result.get(), replay.get());
    assert!(replay.get().contains(r#""value":1.2300e+2"#));
    let received = rig.cloudflare.received();
    assert_eq!(received.len(), 1);
    let sent: Value = serde_json::from_str(&received[0].body).unwrap();
    assert_eq!(sent["images"][0]["content_type"], "image/png");
    assert!(sent["images"][0]["base64"]
        .as_str()
        .is_some_and(|encoded| encoded.starts_with("iVBORw0KGgo")));
    assert!(!received[0].body.contains("shot:1"));
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn llm_extract_admits_only_valid_contract_vector_requests() {
    let rig = Rig::new().await;
    // The date-estimate vector uses a zero cap; a measured zero price is allowed.
    rig.rows
        .0
        .lock()
        .unwrap()
        .get_mut(CLAIRIC)
        .unwrap()
        .input_rate_nanos = Some(0);
    let tools = tools(&rig);
    let vectors: Value = serde_json::from_str(cortexkit_llm_tools::vectors::EXTRACT).unwrap();
    for case in vectors["invalid_requests"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let mut args = case["value"].clone();
        if args.get("model").is_some() {
            args["model"] = json!(CLAIRIC);
        }
        let body = call(&tools, name, args).await.unwrap_err();
        let marked = refusal(&body, "invalid_params", RefusalSpend::NoSpend);
        if name.starts_with("missing-cost-ceiling") {
            assert_eq!(marked.detail.field.as_deref(), Some("max_cost_usd"));
        }
    }
    assert!(rig.cloudflare.received().is_empty());
    for case in vectors["valid_requests"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let mut args = case["value"].clone();
        args["model"] = json!(CLAIRIC);
        let (field, spec) = args["fields"].as_object().unwrap().iter().next().unwrap();
        let state = args["item"]["state"].as_str().unwrap();
        let answers = json!({field: {"type":spec["type"], "value":null}});
        rig.script_ok(
            state,
            &answers.to_string(),
            r#", "usage":{"input_tokens":1}"#,
        );
        let answer = call(&tools, name, args)
            .await
            .unwrap_or_else(|body| panic!("{name}: {}", String::from_utf8_lossy(&body)));
        let reply: Value = serde_json::from_str(answer.get()).unwrap();
        assert_eq!(reply["structuredContent"]["answers"], answers, "{name}");
    }
    assert_eq!(rig.cloudflare.received().len(), 4);

    #[derive(Deserialize)]
    struct Vectors {
        valid_results: Vec<Case>,
        invalid_results: Vec<Case>,
    }
    #[derive(Deserialize)]
    struct Case {
        name: String,
        value: Box<RawValue>,
    }
    let vectors: Vectors = serde_json::from_str(cortexkit_llm_tools::vectors::EXTRACT).unwrap();
    let conforms = |case: &Case| {
        serde_json::from_str::<contract::Result>(case.value.get())
            .is_ok_and(|result| contract::validate_result(&result).is_ok())
    };
    for case in vectors.valid_results {
        assert!(conforms(&case), "{}", case.name);
    }
    for case in vectors.invalid_results {
        assert!(!conforms(&case), "{}", case.name);
    }
}
