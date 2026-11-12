use super::super::conditions;
use super::*;
use cortexkit_role_compaction::{
    notice::Event,
    ride::{RideCause, RideRequest},
    tokens::SurfaceTokens,
};

fn stamped_conditions(seq: u64) -> cortexkit_role_compaction::notice::Conditions {
    let model = broca_protocol::request::ModelHandle::new("example-provider", "example-model");
    let render = json!({});
    super::super::host::conditions_of(
        &super::super::host::Frozen {
            model: &model,
            variant: Some("standard"),
            service_tier: None,
            frozen_render_config: &render,
            tools: &[],
            global_system: None,
            auth_selection: None,
            context_limit: Some(32768),
            max_output_tokens: Some(4096),
        },
        &[],
        false,
        None,
        None,
        &|_| None,
        Some(seq),
    )
    .unwrap()
}

/// The catalog declaration controls the actual outgoing calls, not just a wire constructor.
/// Re-reading it at Setup must also remove a stamp that an earlier Setup allowed.
#[tokio::test]
async fn compaction_v2_statement_seq_gates_setup_notice_and_ride_and_matches_contract_vectors() {
    let declarations = [
        crate::ProviderCapabilities::Declared(vec![
            cortexkit_role_compaction::COMPACTION_PROVIDES.into(),
            cortexkit_role_compaction::COMPACTION_STATEMENT_SEQ_PROVIDES.into(),
            cortexkit_role_compaction::COMPACTION_NOTES_TOKENS_PROVIDES.into(),
        ]),
        crate::ProviderCapabilities::NotInCatalog,
        crate::ProviderCapabilities::Unknown,
    ];
    let plane = CatalogPlane {
        inner: RecordingPlane::default(),
        capabilities: std::sync::Mutex::new(declarations.into()),
    };
    let bind = broca_protocol::BindIdentity::new("/synthetic", "test", "statement-seq");
    let bound = runtime::BoundPlane {
        plane: Some(&plane),
        provider: "magic",
        bind: &bind,
        scope: None,
    };
    let runtime = runtime::Runtime::default();
    let mut conditions = stamped_conditions(42);
    conditions.last_prompt_tokens = Some(1024);
    conditions.last_prompt_view = Some(1);
    conditions.system_tokens = Some(SurfaceTokens::new(100, true));
    conditions.tools_tokens = Some(SurfaceTokens::new(50, true));
    conditions.notes_tokens = Some(SurfaceTokens::new(25, true));
    let setup = conditions::setup_request("example", &conditions, 1700000000200, true);
    let ride = RideRequest::new(vec![RideCause::OverLimit], 1500, conditions.clone());
    for _ in 0..3 {
        plane.inner.answers.lock().unwrap().extend([
            json!({"answer":"ready"}),
            json!({"answer":"received"}),
            json!({"answer":"nothing"}),
        ]);
        assert!(runtime.setup(&bound, &setup).await);
        runtime.event(Event::TranscriptChanged { from_ordinal: 1 });
        runtime.flush(&bound, &setup, &conditions, None).await;
        runtime.ride(&bound, &setup, &ride).await;
    }
    let calls = plane.inner.calls.lock().unwrap();
    assert_eq!(calls.len(), 9);
    for (index, (method, params)) in calls.iter().enumerate() {
        let bytes = serde_json::to_vec(params).unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let state = if method == "compaction.setup" {
            &body
        } else {
            &body["conditions"]
        };
        if index < 3 {
            assert_eq!(state["statement_seq"], json!(42), "{method}");
        } else {
            assert!(!state.as_object().unwrap().contains_key("statement_seq"));
            assert!(!String::from_utf8(bytes).unwrap().contains("statement_seq"));
        }
    }
    let vectors = [
        include_str!("test_vectors/setup-statement-seq-request.json"),
        include_str!("test_vectors/notice-statement-seq-request.json"),
        include_str!("test_vectors/ride-statement-seq-request.json"),
    ];
    for ((method, params), vector) in calls.iter().take(3).zip(vectors) {
        let mut expected: serde_json::Value = serde_json::from_str(vector).unwrap();
        // The synthetic session has no applied view, newest entry, or signed floor. Broca
        // detects more causes than the example runner, explicitly names its default service
        // tier, and this is its first notice.
        expected["params"]
            .as_object_mut()
            .unwrap()
            .remove("signed_floor");
        if method == "compaction.setup" {
            expected["params"]["model"]["service_tier"] = json!("standard");
            expected["params"]
                .as_object_mut()
                .unwrap()
                .remove("applied_view");
            expected["params"].as_object_mut().unwrap().remove("newest");
            expected["params"]["detects"] = serde_json::to_value(runtime::detects(true)).unwrap();
        } else {
            expected["params"]["conditions"]["model"]["service_tier"] = json!("standard");
            expected["params"]["conditions"]
                .as_object_mut()
                .unwrap()
                .remove("signed_floor");
        }
        if method == "compaction.notice" {
            expected["params"]["seq"] = json!(1);
            expected["params"]["events"] = json!([{"event":"transcript_changed","from_ordinal":1}]);
        }
        let actual = json!({"method":method,"params":params});
        assert_eq!(
            serde_json::to_string(&actual).unwrap(),
            serde_json::to_string(&expected).unwrap()
        );
    }
    if let Some(directory) = std::env::var_os("BROCA_STATEMENT_SEQ_SAMPLES") {
        let directory = std::path::PathBuf::from(directory);
        std::fs::create_dir_all(&directory).unwrap();
        for (index, (method, params)) in calls.iter().take(6).enumerate() {
            let declaration = if index < 3 { "declared" } else { "undeclared" };
            let op = method.strip_prefix("compaction.").unwrap();
            let mut bytes =
                serde_json::to_vec_pretty(&json!({"method":method,"params":params})).unwrap();
            bytes.push(b'\n');
            std::fs::write(directory.join(format!("{op}-{declaration}.json")), bytes).unwrap();
        }
    }
}
