use std::{
    collections::HashMap,
    sync::{atomic::AtomicUsize, Mutex},
};

use broca_store::{SqliteStore, SystemOneStore};
use cortexkit_role_extractor::{describe::check_describe, request::REPLY_FRAME_BYTES, vectors};
use serde::Deserialize;
use serde_json::{json, Value};

use super::*;
use crate::system_one::{
    workers_ai::CREDENTIAL_ID,
    workers_ai_tests::{self, Answer, Cloudflare, Vaulted, ACCOUNT},
    BatchCatalog, Config,
};

mod tool;

const RATE: i64 = 110_000_000;

struct Rows(Mutex<HashMap<String, ModelBatchRow>>);

impl BatchCatalog for Rows {
    fn row(&self, catalog_id: &str) -> Option<ModelBatchRow> {
        self.0.lock().unwrap().get(catalog_id).cloned()
    }
}

fn row(kind: Option<String>) -> ModelBatchRow {
    ModelBatchRow {
        model_name: None,
        provider_name: None,
        kind,
        context_tokens: Some(42),
        attachment: true,
        input_rate_nanos: Some(RATE),
        output_rate_nanos: Some(0),
    }
}

struct Rig {
    service: SystemOne,
    rows: Arc<Rows>,
    cloudflare: Arc<Cloudflare>,
    export: Arc<SqliteStore>,
    scope: BatchScope,
}

impl Rig {
    async fn new() -> Self {
        let cloudflare = Arc::new(Cloudflare::default());
        let base = workers_ai_tests::start(cloudflare.clone()).await;
        let config = Config::default();
        let provider = WorkersAi::new(
            Some(&base),
            super::super::CLAIRIC_MODEL_PATH,
            Arc::new(Vaulted {
                account_id: Some(ACCOUNT.into()),
            }),
            config.attempt_timeout,
        )
        .unwrap();
        let rows = Arc::new(Rows(Mutex::new(HashMap::from([(
            CLAIRIC.into(),
            row(Some("extraction".into())),
        )]))));
        let export = Arc::new(SqliteStore::open_in_memory().unwrap());
        let service = SystemOne::new(
            Arc::new(SystemOneStore::open_in_memory().unwrap()),
            "extractor-test",
            rows.clone(),
            vec![],
            config,
            Arc::new(AtomicUsize::new(0)),
        )
        .with_export(
            export.clone(),
            Arc::new(crate::billing_routing::ChargeBasisTable::shipped_defaults()),
        )
        .with_clairic(Arc::new(provider));
        Self {
            service,
            rows,
            cloudflare,
            export,
            scope: BatchScope::Run {
                agent_id: "agent".into(),
                run_id: "run".into(),
            },
        }
    }

    fn extractor(&self) -> &Extractor {
        self.service.extractor.as_ref().unwrap()
    }

    async fn run(&self, params: &str) -> Result<Box<RawValue>, Refusal> {
        self.extractor()
            .run(params, &self.scope, &SendScope::Internal)
            .await
    }

    async fn send(&self, request: &impl serde::Serialize) -> ExtractReply {
        serde_json::from_str(
            self.run(&serde_json::to_string(request).unwrap())
                .await
                .unwrap()
                .get(),
        )
        .unwrap()
    }

    fn script_ok(&self, state: &str, answers: &str, usage: &str) {
        self.cloudflare.script(
            state,
            vec![Answer::Result(format!(
                r#"{{"answers":{answers},"truncated":false{usage}}}"#
            ))],
        );
    }
}

#[tokio::test]
async fn request_vectors_reach_clairic_in_field_order() {
    #[derive(Deserialize)]
    struct File {
        requests: Vec<Case>,
    }
    #[derive(Deserialize)]
    struct Case {
        name: String,
        request: ExtractRequest,
        provider_requests: Vec<ProviderRequest>,
    }
    let rig = Rig::new().await;
    let file: File = serde_json::from_str(vectors::REQUESTS_JSON).unwrap();
    for mut case in file.requests {
        case.request.batch_id = case.name.clone();
        let before = rig.cloudflare.received().len();
        for item in &case.request.items {
            rig.script_ok(&item.state, "{}", "")
        }
        let sent = rig.send(&case.request).await;
        assert_eq!(
            sent.items.len(),
            case.provider_requests.len(),
            "{}",
            case.name
        );
        let received = rig.cloudflare.received();
        assert_eq!(
            received.len() - before,
            case.provider_requests.len(),
            "{}",
            case.name
        );
        for (got, expected) in received[before..].iter().zip(&case.provider_requests) {
            let got: ProviderRequest = serde_json::from_str(&got.body).unwrap();
            assert_eq!(&got, expected, "{}", case.name);
            assert_eq!(
                got.fields.keys().collect::<Vec<_>>(),
                expected.fields.keys().collect::<Vec<_>>()
            );
        }
    }
}

#[tokio::test]
async fn field_path_and_admission_vectors_refuse_before_writes_or_dispatch() {
    #[derive(Deserialize)]
    struct File {
        catalog: Vec<Catalog>,
        refusals: Vec<Case>,
    }
    #[derive(Deserialize)]
    struct Catalog {
        model: String,
        kind: Option<String>,
    }
    #[derive(Deserialize)]
    struct Case {
        name: String,
        request: Box<RawValue>,
        refusal: Refusal,
    }
    let rig = Rig::new().await;
    let store = &rig.extractor().service.store;
    for source in [vectors::FIELD_PATHS_JSON, vectors::ADMISSION_REFUSALS_JSON] {
        let file: File = serde_json::from_str(source).unwrap();
        for model in file.catalog {
            rig.rows
                .0
                .lock()
                .unwrap()
                .insert(model.model, row(model.kind));
        }
        for mut case in file.refusals {
            match case.refusal.code.as_str() {
                errors::BATCH_ID_REUSE
                | errors::ACCOUNT_WALLED
                | errors::BATCH_IN_PROGRESS
                | errors::AUTH_FAILED
                | errors::MODEL_UNAVAILABLE
                | errors::ACCOUNT_LIMIT_EXHAUSTED
                | errors::ACCOUNT_NOT_ENTITLED
                | errors::IMAGE_UNAVAILABLE
                | errors::IMAGE_SOURCE_UNAVAILABLE => continue,
                _ => {}
            }
            // These vectors use illustrative limits rather than broca's published limits.
            match case.name.as_str() {
                "too_many_fields" => {
                    let mut params: Value = serde_json::from_str(case.request.get()).unwrap();
                    let fields = params["fields"].as_object_mut().unwrap();
                    while fields.len() <= MAX_FIELDS as usize {
                        fields.insert(format!("extra_{}", fields.len()), json!({"type":"text"}));
                    }
                    case.request = to_raw_value(&params).unwrap();
                    case.refusal.detail.limit = Some(MAX_FIELDS);
                }
                "instructions_over_the_module_bound" => {
                    let mut params: Value = serde_json::from_str(case.request.get()).unwrap();
                    params["fields"]["vendor"]["instructions"] =
                        json!("x".repeat(MAX_INSTRUCTION_BYTES as usize + 1));
                    case.request = to_raw_value(&params).unwrap();
                    case.refusal.detail.limit = Some(MAX_INSTRUCTION_BYTES);
                }
                "too_many_images" => case.refusal.detail.limit = Some(2),
                _ => {}
            }
            let refusal = rig
                .run(case.request.get())
                .await
                .err()
                .unwrap_or_else(|| panic!("{} admitted", case.name));
            assert_eq!(
                refusal.code, case.refusal.code,
                "{}: {refusal:?}",
                case.name
            );
            assert_eq!(
                refusal.detail.class, case.refusal.detail.class,
                "{}",
                case.name
            );
            assert_eq!(refusal.field(), case.refusal.field(), "{}", case.name);
            assert_eq!(
                refusal.detail.limit, case.refusal.detail.limit,
                "{}",
                case.name
            );
            assert_eq!(
                refusal.detail.model, case.refusal.detail.model,
                "{}",
                case.name
            );
            assert_eq!(
                refusal.detail.kind, case.refusal.detail.kind,
                "{}",
                case.name
            );
            if let Some(id) =
                serde_json::from_str::<Value>(case.request.get()).unwrap()["batch_id"].as_str()
            {
                assert!(
                    store.lookup(STORE_ROLE, id).unwrap().is_none(),
                    "{}",
                    case.name
                );
            }
        }
    }
    assert!(rig.cloudflare.received().is_empty());
}

#[test]
fn identity_vectors_use_the_retained_digest_and_report_the_first_difference() {
    #[derive(Deserialize)]
    struct File {
        cases: Vec<Case>,
    }
    #[derive(Deserialize)]
    struct Case {
        name: String,
        recorded: ExtractRequest,
        sent: ExtractRequest,
        difference: Option<String>,
    }
    let file: File = serde_json::from_str(vectors::IDENTITY_JSON).unwrap();
    for case in file.cases {
        let recorded = serde_json::to_string(&BatchIdentity::of(&case.recorded).unwrap()).unwrap();
        let current = BatchIdentity::of(&case.sent).unwrap();
        let check = check_identity(&current, &recorded);
        if let Some(field) = case.difference {
            let refusal = check.unwrap_err();
            assert_eq!(refusal.code, errors::BATCH_ID_REUSE, "{}", case.name);
            assert_eq!(refusal.field(), Some(field.as_str()), "{}", case.name);
        } else {
            check.unwrap();
            assert_eq!(
                serde_json::to_string(&current).unwrap(),
                recorded,
                "{}",
                case.name
            );
        }
    }
}

#[tokio::test]
async fn describe_and_reply_frame_vectors_use_the_admission_bounds() {
    #[derive(Deserialize)]
    struct File {
        at_the_frame: ExtractRequest,
        over_the_frame: ExtractRequest,
        refusal: Refusal,
    }
    let file: File = serde_json::from_str(vectors::REPLY_SIZE_JSON).unwrap();
    let rig = Rig::new().await;
    let described = rig.extractor().describe();
    check_describe(&described).unwrap();
    let model = &described.models[0];
    assert_eq!(model.max_items, Config::default().max_items);
    assert_eq!(model.max_fields, 64);
    assert_eq!(model.max_answer_bytes, 8192);
    assert_eq!(model.context_tokens, None);
    assert_eq!(
        model.images.as_ref().unwrap().max_bytes_each,
        4 * 1024 * 1024
    );
    assert_eq!(
        model.images.as_ref().unwrap().max_bytes_total,
        8 * 1024 * 1024
    );
    assert_eq!(described.limits.max_instruction_bytes, 60_000);
    assert_eq!(
        model.price.as_ref().unwrap().output_per_mtok_usd,
        Some(0.into())
    );

    // The contract's exact-frame vectors use an illustrative per-answer cap of 65,480 bytes.
    let mut fixture_model = model.clone();
    fixture_model.max_answer_bytes = 65_480;
    check_request_limits(&file.at_the_frame, &fixture_model, &described.limits, false).unwrap();
    assert_eq!(
        check_request_limits(
            &file.over_the_frame,
            &fixture_model,
            &described.limits,
            false
        )
        .unwrap_err(),
        file.refusal
    );

    // Nine items asking for 64 answers cannot fit even at broca's lower per-answer cap.
    let mut over = file.at_the_frame;
    over.items = vec![over.items[0].clone(); 9];
    for i in 0..64 {
        over.fields.insert(
            format!("field_{i}"),
            cortexkit_role_extractor::field::Field::new(FieldType::Text),
        );
    }
    over.fields.retain(|name, _| name.starts_with("field_"));
    let refusal = rig
        .run(&serde_json::to_string(&over).unwrap())
        .await
        .unwrap_err();
    assert_eq!(refusal.field(), Some("items"));
    assert_eq!(refusal.detail.limit, Some(REPLY_FRAME_BYTES));
    assert!(rig.cloudflare.received().is_empty());
    assert!(rig
        .extractor()
        .service
        .store
        .lookup(STORE_ROLE, &over.batch_id)
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn spend_vectors_use_byte_bounds_and_check_each_item_against_recorded_spend() {
    #[derive(Deserialize)]
    struct File {
        cases: Vec<Case>,
    }
    #[derive(Deserialize)]
    struct Case {
        name: String,
        request: ExtractRequest,
        exact_state_tokens: Option<u64>,
    }
    let file: File = serde_json::from_str(vectors::SPEND_JSON).unwrap();
    let rig = Rig::new().await;
    for mut case in file.cases {
        // Exact counts and the illustrative vector module's token bounds are not Clairic's.
        // Rebase the two adjacent ceilings on 27 field bytes + 512 schema + 9 state = 548 tokens.
        if case.exact_state_tokens.is_some() {
            continue;
        }
        let code = match case.name.as_str() {
            "bounded_within_the_ceiling" => {
                case.request.max_cost_usd = Some(nanos_to_usd(60_280));
                None
            }
            "bounded_over_the_ceiling" => {
                case.request.max_cost_usd = Some(nanos_to_usd(60_279));
                Some(errors::COST_EXCEEDED)
            }
            "every_field_may_need_its_own_model_call" => {
                // Three field descriptions alone cost more than 100,000 nanodollars.
                case.request.max_cost_usd = Some(nanos_to_usd(100_000));
                Some(errors::COST_EXCEEDED)
            }
            _ => None,
        };
        case.request.batch_id = case.name.clone();
        let state = &case.request.items[0].state;
        rig.script_ok(
            state,
            "{}",
            r#", "usage":{"input_tokens":548,"output_tokens":100000000}"#,
        );
        let before = rig.cloudflare.calls(state);
        let reply = rig.send(&case.request).await;
        assert_eq!(
            reply.items[0].error.as_ref().map(|e| e.code.as_str()),
            code,
            "{}",
            case.name
        );
        assert_eq!(
            rig.cloudflare.calls(state) - before,
            usize::from(code.is_none()),
            "{}",
            case.name
        );
    }

    let request = json!({"batch_id":"remaining", "model":CLAIRIC, "fields":{"total":{"type":"number"}},
        "items":[{"state":"first-one"},{"state":"secondone"}], "max_cost_usd":nanos_to_usd(120_559)});
    rig.script_ok(
        "first-one",
        "{}",
        r#", "usage":{"input_tokens":548,"output_tokens":100000000}"#,
    );
    let reply = rig.send(&request).await;
    assert!(reply.items[0].answers.is_some());
    assert_eq!(
        reply.items[1].error.as_ref().unwrap().code,
        errors::COST_EXCEEDED
    );
    assert_eq!(reply.cost_usd, Some(nanos_to_usd(60_280)));
    assert_eq!(rig.cloudflare.calls("secondone"), 0);
}

#[tokio::test]
async fn captured_clairic_replies_preserve_raw_answers_spans_and_reported_usage() {
    #[derive(Deserialize)]
    struct File {
        replies: Vec<Case>,
    }
    #[derive(Deserialize)]
    struct Case {
        name: String,
        status: u16,
        body: String,
        item: ItemResult,
        sent: Option<Sent>,
    }
    #[derive(Deserialize)]
    struct Sent {
        state: String,
        fields: Box<RawValue>,
    }
    let file: File = serde_json::from_str(vectors::PROVIDER_REPLIES_JSON).unwrap();
    let rig = Rig::new().await;
    let mut checked = 0;
    for case in file.replies {
        let json = |value: &str| serde_json::to_string(value).unwrap();
        let (state, fields) = match case.name.as_str() {
            "01-all-types" | "02-span-offsets" | "03-list-decimals" => {
                let sent = case.sent.unwrap();
                (sent.state, sent.fields.get().to_owned())
            }
            "08-unknown-type" | "09-unknown-option" | "11-empty-state" => {
                (case.name.clone(), r#"{"text":{"type":"text"}}"#.into())
            }
            _ => continue,
        };
        let state_text = case.item.state_text.is_some();
        let quantiles = case.name == "01-all-types";
        let params = format!(
            r#"{{"batch_id":{},"model":"{CLAIRIC}","fields":{fields},"items":[{{"state":{}}}],"options":{{"state_text":{state_text},"quantiles":{quantiles}}}}}"#,
            json(&case.name),
            json(&state)
        );
        rig.cloudflare
            .script(&state, vec![Answer::Raw(case.status, case.body)]);
        let reply = rig.run(&params).await.unwrap();
        let reply: ExtractReply = serde_json::from_str(reply.get()).unwrap();
        assert_eq!(
            serde_json::to_string(&reply.items[0]).unwrap(),
            serde_json::to_string(&case.item).unwrap(),
            "{}",
            case.name
        );
        if case.name == "03-list-decimals" {
            // The capture bills 304 input tokens. No answer text or output tokens are charged.
            assert_eq!(reply.cost_usd, Some(nanos_to_usd(33_440)));
        }
        checked += 1;
    }
    assert_eq!(checked, 6);
}

#[tokio::test]
async fn answer_too_large_is_retained_and_its_measured_spend_is_exported_once() {
    #[derive(Deserialize)]
    struct File {
        request: ExtractRequest,
        reply: ExtractReply,
    }
    let file: File = serde_json::from_str(vectors::ANSWER_TOO_LARGE_JSON).unwrap();
    let rig = Rig::new().await;
    let first = serde_json::to_string(file.reply.items[0].answers.as_ref().unwrap()).unwrap();
    rig.script_ok(
        &file.request.items[0].state,
        &first,
        r#", "usage":{"input_tokens":7}"#,
    );
    let detail = file.reply.items[1]
        .error
        .as_ref()
        .unwrap()
        .detail
        .as_ref()
        .unwrap();
    let size = MAX_ANSWER_BYTES + detail.size.unwrap() - detail.limit.unwrap();
    let fixed = r#"{"type":"text","value":""}"#.len() as u64;
    let value = "x".repeat((size - fixed) as usize);
    let answers = format!(r#"{{"summary":{{"type":"text","value":"{value}"}}}}"#);
    rig.script_ok(
        &file.request.items[1].state,
        &answers,
        r#", "usage":{"input_tokens":20,"output_tokens":12}"#,
    );
    let params = serde_json::to_string(&file.request).unwrap();
    let original = rig.run(&params).await.unwrap();
    let reply: ExtractReply = serde_json::from_str(original.get()).unwrap();
    assert_eq!(reply.items[0].answers, file.reply.items[0].answers);
    assert_eq!(
        reply.items[1].error,
        Some(ItemError::answer_too_large(
            "fields.summary",
            MAX_ANSWER_BYTES,
            size
        ))
    );
    assert!(reply.items[1].answers.is_none());
    assert_eq!(reply.usage.input_tokens, Some(7));
    assert_eq!(reply.cost_usd, Some(nanos_to_usd(770)));
    assert_eq!(
        rig.extractor()
            .service
            .store
            .lookup(STORE_ROLE, &file.request.batch_id)
            .unwrap()
            .unwrap()
            .spent_nanos,
        2_970
    );
    let facts = rig.export.read_export_page(None, 100).unwrap().facts;
    assert_eq!(facts.len(), 2);
    let paid = serde_json::to_value(&facts[1].segment).unwrap();
    assert_eq!(paid["usage"], json!({"input_tokens":20,"output_tokens":12}));
    assert_eq!(paid["auth_selection"], format!("{CREDENTIAL_ID}:resolved"));
    assert_eq!(paid["account_ref"], ACCOUNT);

    // Even removing the live catalog row cannot cause a second call for either final item.
    rig.rows.0.lock().unwrap().clear();
    assert_eq!(rig.run(&params).await.unwrap().get(), original.get());
    assert_eq!(rig.cloudflare.received().len(), 2);
    assert_eq!(
        rig.export.read_export_page(None, 100).unwrap().facts.len(),
        2
    );
}

#[tokio::test]
async fn batch_level_refusal_writes_no_batch_and_a_resend_can_be_admitted() {
    let rig = Rig::new().await;
    let state = "provider access restored";
    rig.cloudflare.script(
        state,
        vec![
            Answer::Error {
                status: 401,
                code: Some(10000),
                retry_after_s: None,
            },
            Answer::Result(r#"{"answers":{"total":{"type":"number","value":1.000}}}"#.into()),
        ],
    );
    let params = json!({"batch_id":"access", "model":CLAIRIC, "fields":{"total":{"type":"number"}}, "items":[{"state":state}]});
    let refusal = rig.run(&params.to_string()).await.unwrap_err();
    assert_eq!(refusal.code, errors::AUTH_FAILED);
    assert_eq!(refusal.detail.credential.as_deref(), Some(CREDENTIAL_ID));
    assert!(rig
        .extractor()
        .service
        .store
        .lookup(STORE_ROLE, "access")
        .unwrap()
        .is_none());
    assert!(rig
        .export
        .read_export_page(None, 100)
        .unwrap()
        .facts
        .is_empty());
    let reply = rig.send(&params).await;
    assert!(reply.items[0].answers.is_some());
    assert_eq!(reply.cost_usd, None);
    let facts = rig.export.read_export_page(None, 100).unwrap().facts;
    assert_eq!(facts.len(), 1);
    assert_eq!(
        serde_json::to_value(&facts[0].segment).unwrap()["usage"],
        json!({})
    );
    assert_eq!(rig.cloudflare.calls(state), 2);
}

#[tokio::test]
async fn runtime_admission_vectors_apply_to_held_batches_and_provider_refusals() {
    #[derive(Deserialize)]
    struct File {
        refusals: Vec<Case>,
    }
    #[derive(Deserialize)]
    struct Case {
        request: Box<RawValue>,
        refusal: Refusal,
    }
    let file: File = serde_json::from_str(vectors::ADMISSION_REFUSALS_JSON).unwrap();
    let rig = Rig::new().await;
    let service = &rig.extractor().service;
    let mut dispatched = 0;
    for mut case in file.refusals {
        let expected = &mut case.refusal;
        if expected.code == errors::BATCH_IN_PROGRESS {
            let request = request::parse_request(case.request.get()).unwrap();
            let id = &request.batch_id;
            assert_eq!(
                service
                    .store
                    .claim(&BatchClaim {
                        role: STORE_ROLE,
                        batch_id: id,
                        model: CLAIRIC,
                        identity: "{}",
                        body_digest: "held",
                        frozen_row: "null",
                        scope: &rig.scope,
                        ceiling_nanos: None,
                        owner: "another-process",
                        now_ms: (service.clock)(),
                    })
                    .unwrap(),
                Claimed::Fresh
            );
            assert_eq!(&rig.run(case.request.get()).await.unwrap_err(), expected);
            assert_eq!(rig.cloudflare.received().len(), dispatched);
            service
                .store
                .discard_fresh(STORE_ROLE, id, "another-process")
                .unwrap();
            continue;
        }
        let (status, code) = match expected.code.as_str() {
            errors::AUTH_FAILED => (401, 10000),
            errors::MODEL_UNAVAILABLE => (404, 5007),
            errors::ACCOUNT_LIMIT_EXHAUSTED => (429, 3036),
            errors::ACCOUNT_NOT_ENTITLED => (403, 5035),
            _ => continue,
        };
        let mut request: Value = serde_json::from_str(case.request.get()).unwrap();
        // Give each script its own state; the stand-in counts calls by state, not by batch id.
        request["items"][0]["state"] = json!(expected.code);
        rig.cloudflare.script(
            &expected.code,
            vec![Answer::Error {
                status,
                code: Some(code),
                retry_after_s: None,
            }],
        );
        let refusal = rig.run(&request.to_string()).await.unwrap_err();
        if expected.code == errors::AUTH_FAILED {
            expected.detail.credential = Some(CREDENTIAL_ID.into());
            assert!(refusal.message.contains("10000"));
        }
        assert_eq!(refusal.code, expected.code);
        assert_eq!(refusal.detail, expected.detail);
        assert!(service
            .store
            .lookup(STORE_ROLE, request["batch_id"].as_str().unwrap())
            .unwrap()
            .is_none());
        dispatched += 1;
        assert_eq!(rig.cloudflare.received().len(), dispatched);
    }
    assert_eq!(dispatched, 4);
    assert!(rig
        .export
        .read_export_page(None, 100)
        .unwrap()
        .facts
        .is_empty());
}
