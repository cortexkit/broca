use super::*;

fn capture(name: &str, bytes: &[u8]) {
    if let Some(directory) = std::env::var_os("BROCA_ROUTING_SAMPLE_DIR") {
        let directory = std::path::PathBuf::from(directory);
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join(format!("{name}.json")), bytes).unwrap();
    }
}

async fn fresh(code: &'static str, detail: Value, sample: &str) -> Value {
    let rig = routed_rig(true, true);
    *rig.router.refused.lock().unwrap() = Some((code, "scripted routing refusal"));
    *rig.router.refusal_detail.lock().unwrap() = detail;
    let mut request: Value = serde_json::from_str(&routed_request(&["never"])).unwrap();
    request["model"] = json!(JEV);
    let bytes = rig
        .service
        .clone()
        .spawn_call(request.to_string(), test_scope())
        .await
        .unwrap_err();
    capture(sample, &bytes);
    assert!(rig.store.lookup(STORE_ROLE, "routed").unwrap().is_none());
    assert!(rig.router.bodies("route.report").is_empty());
    assert!(rig.router.bodies("route.account_for").is_empty());
    assert_eq!(rig.provider.calls("never"), 0);
    let refusal: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(refusal["detail"]["routing_code"], code);
    assert_eq!(
        refusal["detail"]["routing_reason"],
        "scripted routing refusal"
    );
    assert_eq!(refusal["detail"]["model"], JEV);
    refusal
}

#[tokio::test]
async fn routing_retry_transient_admission_is_deferred_without_writes() {
    let refusal = fresh(
        "account_selection_unavailable",
        json!({}),
        "fresh-transient",
    )
    .await;
    assert_eq!(refusal["code"], "routing_deferred");
    assert_eq!(refusal["detail"]["class"], "transient");
    assert!(refusal["detail"].get("retry_after_ms").is_none());
}

#[tokio::test(start_paused = true)]
async fn routing_retry_timed_admission_uses_delay_and_saturates() {
    let _clock = ManualClock::new();
    for (code, key) in [
        ("account_hold", "until_ms"),
        ("pin_unavailable", "reset_at_ms"),
        ("no_route", "reset_at_ms"),
        ("account_pool_exhausted", "reset_at_ms"),
    ] {
        for (timestamp, delay) in [(1_001_234, 1_234), (0, 0)] {
            let refusal = fresh(
                code,
                json!({key:timestamp}),
                &format!("fresh-timed-{code}-{timestamp}"),
            )
            .await;
            assert_eq!(refusal["code"], "routing_deferred", "{code}");
            assert_eq!(refusal["detail"]["class"], "transient");
            assert_eq!(refusal["detail"]["retry_after_ms"], delay);
        }
    }
}

#[tokio::test]
async fn routing_retry_permanent_admission_preserves_all_answered_codes() {
    for (code, detail) in [
        ("auth_required", json!({})),
        ("invalid_request", json!({})),
        ("admission_conflict", json!({})),
        ("future_refusal", json!({"until_ms":0})),
        ("account_hold", json!({"until_ms":"1001234"})),
        ("no_route", json!({})),
    ] {
        let refusal = fresh(code, detail, &format!("fresh-permanent-{code}")).await;
        assert_eq!(refusal["code"], "routing_refused", "{code}");
        assert_eq!(refusal["detail"]["class"], "permanent");
        assert!(refusal["detail"].get("retry_after_ms").is_none());
    }
}

async fn renewal(code: &'static str, detail: Value) -> (Value, Value) {
    let rig = routed_rig(true, true);
    rig.router.lease_ms.store(12_000, Ordering::SeqCst);
    *rig.router.renewal_refusal.lock().unwrap() = Some((code, "scripted routing refusal"));
    *rig.router.refusal_detail.lock().unwrap() = detail;
    let gate = Arc::new(Semaphore::new(0));
    rig.provider
        .script("paid", vec![Script::Held(gate.clone(), answered(Some(10)))]);
    let entered = rig.provider.entered.notified();
    let service = rig.service.clone();
    let task = tokio::spawn(async move {
        service
            .spawn_call(routed_request(&["paid", "never"]), test_scope())
            .await
    });
    broca_test_support::event_timeout("paid provider entered", entered, || {
        "provider did not enter".into()
    })
    .await
    .unwrap();
    let journal: Value = serde_json::from_str(
        &rig.store
            .routing(STORE_ROLE, "routed")
            .unwrap()
            .unwrap()
            .runs,
    )
    .unwrap();
    let due = journal["runs"][0]["renew_at_ms"].as_u64().unwrap();
    let renewed = rig.router.renewed.notified();
    tokio::time::advance(std::time::Duration::from_millis(due - rig.router.now())).await;
    broca_test_support::event_timeout("renewal refused", renewed, || "router did not renew".into())
        .await
        .unwrap();
    gate.add_permits(1);
    let reply = task.await.unwrap().unwrap();
    capture(&format!("renewal-{code}"), reply.get().as_bytes());
    let reply: Value = serde_json::from_str(reply.get()).unwrap();
    assert!(reply["items"][0].get("answers").is_some());
    assert_eq!(reply["items"][0]["usage"]["input_tokens"], 10);
    assert_eq!(rig.provider.calls("paid"), 1);
    assert_eq!(rig.provider.calls("never"), 0);
    assert_eq!(rig.router.bodies("route.select").len(), 1);
    assert_eq!(rig.router.bodies("route.account_for").len(), 2);
    let journal: Value = serde_json::from_str(
        &rig.store
            .routing(STORE_ROLE, "routed")
            .unwrap()
            .unwrap()
            .runs,
    )
    .unwrap();
    (reply, journal)
}

#[tokio::test(start_paused = true)]
async fn routing_retry_renewal_items_keep_code_class_detail_and_paid_answer() {
    let _clock = ManualClock::new();
    for (code, detail, expected_code, class, delay) in [
        (
            "account_selection_unavailable",
            json!({}),
            "routing_deferred",
            "transient",
            None,
        ),
        (
            "account_hold",
            json!({"until_ms":1_010_000}),
            "routing_deferred",
            "transient",
            Some(2_000),
        ),
        (
            "auth_required",
            json!({}),
            "routing_refused",
            "permanent",
            None,
        ),
    ] {
        let (reply, journal) = renewal(code, detail).await;
        let error = &reply["items"][1]["error"];
        assert_eq!(error["code"], expected_code);
        assert_eq!(error["class"], class);
        assert_eq!(error["detail"]["routing_code"], code);
        assert_eq!(error.get("retry_after_ms").and_then(Value::as_u64), delay);
        assert_eq!(
            journal["runs"][0]["renewal_refusal"]["detail"]["routing_code"],
            code
        );
        assert_eq!(
            journal["runs"][0]["renewal_refusal"]["detail"]["routing_reason"],
            "scripted routing refusal"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn routing_retry_renewal_does_not_embed_routing_code_in_message() {
    let _clock = ManualClock::new();
    let (reply, _) = renewal("account_hold", json!({"until_ms":1_010_000})).await;
    let message = reply["items"][1]["error"]["message"].as_str().unwrap();
    assert!(!message.contains("routing refused renewal:"));
    assert!(!message.contains("account_hold"));
}
