use broca_wire::agent_data::{self, errors::ErrorBody, errors::RefusalCode, request::Request, Op};
use cortexkit_role_agent_data::vectors;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

const SESSION_REFS: &str = include_str!("fixtures/llm-runner-v1/session-ref.json");

fn vectors(name: &str) -> Value {
    serde_json::from_str(vectors::file(name).expect("published vector file")).unwrap()
}

#[test]
fn session_ref_fixture_bytes_are_pinned() {
    assert_eq!(
        format!("{:x}", Sha256::digest(SESSION_REFS.as_bytes())),
        "4bc23abb3e5e82db7f02472244c196e97a38a4bea5c37c25e737f4601b32af0c"
    );
}

#[test]
fn session_reference_matches_shared_vectors() {
    let file: Value = serde_json::from_str(SESSION_REFS).unwrap();
    let cases = file["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 4);
    for case in cases {
        let bind = &case["bind"];
        let reference = agent_data::session_reference(
            bind["harness"].as_str().unwrap(),
            bind["project_root"].as_str().unwrap(),
            bind["session"].as_str().unwrap(),
        );
        assert_eq!(reference.ref_kind, "broca-session", "{}", case["name"]);
        assert_eq!(reference.r#ref, case["session_ref"], "{}", case["name"]);
        assert!(agent_data::refs::is_session_ref(&reference.r#ref));
    }
}

#[test]
fn broca_describe_names_its_agent_owned_wal_store() {
    assert_eq!(
        serde_json::to_value(agent_data::describe_reply()).unwrap(),
        json!({
            "ref_kinds": ["broca-session"],
            "stores": [{"name": "broca/session-wals", "sync_class": "agent"}],
            "external": false
        })
    );
}

#[test]
fn service_identity_and_all_ten_methods_are_pinned() {
    assert_eq!(agent_data::PROVIDES, "agent-data/v1");
    assert_eq!(agent_data::SERVICE_ID, "agent-data");
    assert_eq!(agent_data::ADMITTED_CALLER, "reserved:entorhinal");
    assert_eq!(
        agent_data::REQUIRED_OPS,
        [
            "agent_data.describe",
            "agent_data.busy",
            "agent_data.status",
            "agent_data.prepare",
            "agent_data.export",
            "agent_data.diverged",
            "agent_data.import",
            "agent_data.activate",
            "agent_data.abort",
            "agent_data.release"
        ]
    );
    assert!(agent_data::caller_admitted("reserved:entorhinal"));
    assert!(!agent_data::caller_admitted("direct"));
}

#[test]
fn shared_request_vectors_use_the_contract_decoder() {
    let file = vectors("requests.json");
    let mut seen = BTreeSet::new();
    for case in file["requests"].as_array().unwrap() {
        let method = case["method"].as_str().unwrap();
        let request = Request::decode(method, &case["params"]).unwrap();
        assert_eq!(request.op().method(), method, "{}", case["name"]);
        assert_eq!(request.params(), case["params"], "{}", case["name"]);
        seen.insert(method);
    }
    assert_eq!(seen, agent_data::REQUIRED_OPS.iter().copied().collect());
    for case in file["refused"].as_array().unwrap() {
        let mut params = case["params"].clone();
        if let Some(count) = case["expand_refs"].as_u64() {
            params["refs"] = Value::Array(
                (0..count)
                    .map(|i| json!({"ref_kind": "broca-session", "ref": format!("ses_{i}")}))
                    .collect(),
            );
        }
        let body = Request::decode(case["method"].as_str().unwrap(), &params).unwrap_err();
        assert_eq!(body.code.as_str(), case["code"], "{}", case["name"]);
    }
}

fn reply<T: DeserializeOwned + Serialize>(value: &Value) -> Result<Value, serde_json::Error> {
    let decoded: T = serde_json::from_value(value.clone())?;
    serde_json::to_value(decoded)
}

fn mapped_reply(op: Op, value: &Value) -> Result<Value, serde_json::Error> {
    use agent_data::*;
    match op {
        Op::Describe => reply::<describe::DescribeReply>(value),
        Op::Busy => reply::<busy::BusyReply>(value),
        Op::Status => reply::<status::StatusReply>(value),
        Op::Prepare => reply::<prepare::PrepareReply>(value),
        Op::Export => reply::<export::ExportReply>(value),
        Op::Diverged => reply::<diverged::DivergedReply>(value),
        Op::Import => reply::<import::ImportReply>(value),
        Op::Activate => reply::<activate::ActivateReply>(value),
        Op::Abort => reply::<abort::AbortReply>(value),
        Op::Release => reply::<release::ReleaseReply>(value),
    }
}

#[test]
fn shared_reply_vectors_use_the_contract_types() {
    let file = vectors("replies.json");
    let mut seen = BTreeSet::new();
    for case in file["replies"].as_array().unwrap() {
        let method = case["method"].as_str().unwrap();
        let op = Op::from_method(method).unwrap();
        assert_eq!(
            mapped_reply(op, &case["reply"]).unwrap(),
            case["reply"],
            "{}",
            case["name"]
        );
        seen.insert(method);
    }
    assert_eq!(seen, agent_data::REQUIRED_OPS.iter().copied().collect());
    for case in file["refused"].as_array().unwrap() {
        let op = Op::from_method(case["method"].as_str().unwrap()).unwrap();
        assert!(
            mapped_reply(op, &case["reply"]).is_err(),
            "{}",
            case["name"]
        );
    }
}

#[test]
fn shared_refusal_vectors_preserve_codes_details_and_retryability() {
    let file = vectors("refusals.json");
    let mut known = BTreeSet::new();
    for case in file["refusals"].as_array().unwrap() {
        let body: ErrorBody = serde_json::from_value(case["body"].clone()).unwrap();
        // A vector may add fields beside code/message/detail that the decoder ignores,
        // so compare the contract-defined code and detail rather than the whole body.
        assert_eq!(body.code.as_str(), case["body"]["code"], "{}", case["name"]);
        assert_eq!(body.code.retryable(), case["retryable"], "{}", case["name"]);
        assert_eq!(body.code.is_known(), case["named"], "{}", case["name"]);
        let encoded = serde_json::to_value(&body).unwrap();
        assert_eq!(
            encoded["detail"], case["body"]["detail"],
            "{}",
            case["name"]
        );
        if body.code.is_known() {
            known.insert(body.code.as_str().to_owned());
        }
    }
    assert_eq!(
        known,
        agent_data::errors::codes::ALL
            .iter()
            .map(|s| s.to_string())
            .collect()
    );
    assert_eq!(RefusalCode::KNOWN.len(), 14);
}

#[test]
fn shared_step_idempotency_and_ref_set_helpers_are_available() {
    use agent_data::{
        idempotency::Repeat,
        refs::{CutId, Refs},
        request::CutRequest,
        step::Step,
    };
    let a = agent_data::session_reference("runner", "/tmp/project", "a");
    let b = agent_data::session_reference("runner", "/tmp/project", "b");
    let refs = Refs::new(vec![a.clone(), b.clone()]).unwrap();
    let reordered = Refs::new(vec![b, a.clone()]).unwrap();
    let fewer = Refs::new(vec![a]).unwrap();
    let cut = CutId::new("0123456789abcdef0123456789abcdef").unwrap();
    let first = Request::Prepare(CutRequest::new(refs.clone(), cut.clone()));
    let again = Request::Prepare(CutRequest::new(reordered.clone(), cut.clone()));
    let changed = Request::Prepare(CutRequest::new(fewer.clone(), cut));
    assert!(refs.same_set(&reordered));
    assert_eq!(
        agent_data::idempotency::compare(&first, &again),
        Repeat::Replay
    );
    assert_eq!(
        agent_data::idempotency::compare(&first, &changed).refusal(),
        Some(RefusalCode::IdReused)
    );
    assert_eq!(
        agent_data::step::advance(Op::Prepare, Step::None),
        Ok(Step::Prepared)
    );
    assert_eq!(
        agent_data::step::advance(Op::Export, Step::None)
            .unwrap_err()
            .code(),
        RefusalCode::CutUnknown
    );
    assert!(
        agent_data::step::check_ref_set(Op::Export, Step::Prepared, Some(&refs), &reordered)
            .is_ok()
    );
    let mismatch = agent_data::step::check_ref_set(Op::Export, Step::Prepared, Some(&refs), &fewer)
        .unwrap_err();
    assert_eq!(mismatch.step, Step::Prepared);
    assert_eq!(mismatch.refs, Some(refs.into_vec()));
}
