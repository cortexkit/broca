use super::*;
use std::path::PathBuf;

fn journal(rig: &RoutedRig) -> Value {
    serde_json::from_str(
        &rig.store
            .routing(STORE_ROLE, "routed")
            .unwrap()
            .unwrap()
            .runs,
    )
    .unwrap()
}

pub(super) fn pinned(states: &[&str]) -> String {
    let mut request: Value = serde_json::from_str(&routed_request(states)).unwrap();
    request["model"] = json!(JEV);
    request.to_string()
}

#[tokio::test]
async fn audit_exit_committed_selection_survives_first_admission_refusal() {
    let rig = routed_rig(true, true);
    *rig.router.admission_refused.lock().unwrap() = Some(("auth_required", "sign in"));
    let refusal = call(&rig, &pinned(&["x"])).await.unwrap_err();
    assert_eq!(refusal["code"], "routing_refused");
    assert_eq!(refusal["detail"]["routing_code"], "auth_required");
    let stored = rig.store.routing(STORE_ROLE, "routed").unwrap();
    assert!(
        stored.is_some(),
        "a committed selection must survive first-admission refusal"
    );
    assert_eq!(rig.router.selects.load(Ordering::SeqCst), 1);
    *rig.router.admission_refused.lock().unwrap() = None;
    rig.provider
        .script("x", vec![Script::Answer(answered(Some(7)))]);
    call(&rig, &pinned(&["x"])).await.unwrap();
    assert_eq!(
        rig.router.selects.load(Ordering::SeqCst),
        1,
        "retry must reuse the committed decision"
    );
}

#[tokio::test]
async fn audit_exit_readmission_refusal_keeps_shared_class_and_verbatim_cause() {
    for code in [
        "account_selection_unavailable",
        "unknown_decision",
        "future_account_refusal",
    ] {
        let rig = routed_rig(true, true);
        rig.provider.script("x", vec![Script::Status(503, None)]);
        call(&rig, &routed_request(&["x"])).await.unwrap();
        *rig.router.admission_refused.lock().unwrap() = Some((code, "answer from router"));
        let refusal = call(&rig, &routed_request(&["x"])).await.unwrap_err();
        let transient = code == "account_selection_unavailable";
        assert_eq!(
            refusal["code"],
            if transient {
                "routing_deferred"
            } else {
                "routing_refused"
            },
            "{code}"
        );
        assert_eq!(
            refusal["detail"]["class"],
            if transient { "transient" } else { "permanent" }
        );
        assert_eq!(refusal["detail"]["routing_code"], code);
        assert_eq!(refusal["detail"]["routing_reason"], "answer from router");
        assert_eq!(rig.provider.calls("x"), 1);
        assert_eq!(rig.router.selects.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn audit_exit_describe_contract_refusal_is_not_an_outage() {
    for code in ["invalid_request", "admission_conflict"] {
        let rig = routed_rig(true, true);
        *rig.router.override_refusal.lock().unwrap() = Some(("role.describe", code));
        let refusal = call(&rig, &routed_request(&["x"])).await.unwrap_err();
        assert_eq!(refusal["code"], "routing_refused", "{code}");
        assert_eq!(refusal["detail"]["routing_code"], code);
        assert_eq!(refusal["detail"]["class"], "permanent");
        assert!(rig.store.lookup(STORE_ROLE, "routed").unwrap().is_none());
        assert_eq!(rig.provider.calls("x"), 0);
    }
}

#[tokio::test]
async fn audit_exit_transient_reselection_does_not_spend_the_allowance() {
    let rig = routed_rig(true, true);
    rig.provider.script(
        "x",
        vec![Script::AccountError, Script::Answer(answered(Some(7)))],
    );
    rig.router
        .reselect_unreachable
        .store(true, Ordering::SeqCst);
    let refusal = call(&rig, &routed_request(&["x"])).await.unwrap_err();
    assert_eq!(refusal["code"], "routing_unavailable");
    assert_eq!(journal(&rig)["reselected"], false);
    rig.router
        .reselect_unreachable
        .store(false, Ordering::SeqCst);
    let reply: Value =
        serde_json::from_str(call(&rig, &routed_request(&["x"])).await.unwrap().get()).unwrap();
    assert_eq!(reply["decision_id"], "decision-1");
    assert!(reply["items"][0].get("answers").is_some());
    assert_eq!(rig.router.selects.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn audit_exit_account_error_dominates_an_earlier_provider_error() {
    for run_reports in [false, true] {
        let rig = routed_rig(run_reports, true);
        rig.provider
            .script("temporary", vec![Script::Status(503, None)]);
        rig.provider.script(
            "account",
            vec![Script::AccountError, Script::Answer(answered(Some(7)))],
        );
        let reply: Value = serde_json::from_str(
            call(&rig, &routed_request(&["temporary", "account"]))
                .await
                .unwrap()
                .get(),
        )
        .unwrap();
        assert_eq!(
            rig.router.selects.load(Ordering::SeqCst),
            2,
            "the account failure, not an earlier 503, determines reselection"
        );
        assert_eq!(reply["decision_id"], "decision-1");
        let reports = rig.router.bodies("route.report");
        assert_eq!(reports[0]["runner_code"], "account_not_entitled");
    }
}

#[tokio::test(start_paused = true)]
async fn audit_exit_expired_live_reads_refuse_before_recovery_or_dispatch() {
    let _clock = ManualClock::new();
    let rig = routed_rig(true, true);
    rig.provider
        .script("private", vec![Script::Answer(answered(Some(7)))]);
    *rig.router.report_refused.lock().unwrap() = Some("store_error");
    call(&rig, &routed_request(&["private"])).await.unwrap();
    tokio::time::advance(std::time::Duration::from_millis(
        broca_store::system_one::RETENTION_MS as u64,
    ))
    .await;
    assert!(
        rig.service.stored_batch("routed").await.unwrap().is_none(),
        "public live snapshots must not expose expired content"
    );
    rig.router.clear();
    let refusal = call(&rig, &routed_request(&["private"])).await.unwrap_err();
    assert_eq!(refusal["code"], "routing_unavailable");
    assert_eq!(rig.provider.calls("private"), 1);
    assert!(
        rig.router.calls.lock().unwrap().is_empty(),
        "refuse before router lookup or delivery"
    );
}

#[tokio::test(start_paused = true)]
async fn audit_exit_granted_but_lost_renewal_releases_both_ids_and_resumes() {
    let _clock = ManualClock::new();
    let rig = routed_rig(true, true);
    rig.router.lease_ms.store(12_000, Ordering::SeqCst);
    rig.router
        .lose_granted_renewal_replies
        .store(2, Ordering::SeqCst);
    let gate = Arc::new(Semaphore::new(0));
    rig.provider
        .script("paid", vec![Script::Held(gate.clone(), answered(Some(7)))]);
    rig.provider
        .script("unsent", vec![Script::Answer(answered(Some(9)))]);
    let service = rig.service.clone();
    let task = tokio::spawn(async move {
        service
            .spawn_call(routed_request(&["paid", "unsent"]), test_scope())
            .await
    });
    wait_for("provider entered", || rig.provider.calls("paid") == 1).await;
    tokio::time::advance(std::time::Duration::from_secs(8)).await;
    wait_for("both granted renewal replies lost", || {
        rig.router
            .lose_granted_renewal_replies
            .load(Ordering::SeqCst)
            == 0
    })
    .await;
    gate.add_permits(1);
    let reply: Value = serde_json::from_str(task.await.unwrap().unwrap().get()).unwrap();
    assert!(reply["items"][0].get("answers").is_some());
    assert_eq!(reply["items"][1]["error"]["code"], "provider_error");
    assert_eq!(reply["items"][1]["error"]["class"], "transient");
    assert_eq!(rig.provider.calls("unsent"), 0);
    let admissions = rig.router.bodies("route.account_for");
    assert_eq!(admissions.len(), 3);
    assert_eq!(
        admissions[1], admissions[2],
        "transport retry reuses the granted renewal's identity"
    );
    assert_eq!(
        rig.router.admissions.lock().unwrap().len(),
        2,
        "the router really granted the renewal before losing its replies"
    );
    let finishes: Vec<_> = rig
        .router
        .bodies("route.account_observe")
        .into_iter()
        .filter(|body| body["kind"] == "run_finished")
        .collect();
    assert_eq!(finishes.len(), 2);
    assert_eq!(finishes[0]["admission_id"], admissions[0]["admission_id"]);
    assert_eq!(finishes[1]["admission_id"], admissions[1]["admission_id"]);
    assert!(!rig.router.pin_held.load(Ordering::SeqCst));
    let replay: Value = serde_json::from_str(
        call(&rig, &routed_request(&["paid", "unsent"]))
            .await
            .unwrap()
            .get(),
    )
    .unwrap();
    assert!(replay["items"][1].get("answers").is_some());
    assert_eq!(rig.provider.calls("paid"), 1);
    assert_eq!(rig.provider.calls("unsent"), 1);
    assert_eq!(rig.router.selects.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn audit_exit_answered_renewal_refusal_preserves_partial_reply_and_metadata() {
    let _clock = ManualClock::new();
    for code in [
        "pin_generation_conflict",
        "account_selection_unavailable",
        "future_refusal",
    ] {
        let rig = routed_rig(true, true);
        rig.router.lease_ms.store(12_000, Ordering::SeqCst);
        let gate = Arc::new(Semaphore::new(0));
        rig.provider
            .script("paid", vec![Script::Held(gate.clone(), answered(Some(7)))]);
        rig.provider
            .script("unsent", vec![Script::Answer(answered(Some(9)))]);
        let service = rig.service.clone();
        let task = tokio::spawn(async move {
            service
                .spawn_call(routed_request(&["paid", "unsent"]), test_scope())
                .await
        });
        wait_for("provider entered", || rig.provider.calls("paid") == 1).await;
        *rig.router.override_refusal.lock().unwrap() = Some(("route.account_for", code));
        tokio::time::advance(std::time::Duration::from_secs(8)).await;
        wait_for("renewal failure committed", || {
            journal(&rig)["runs"][0]["renew_failed"] == true
        })
        .await;
        gate.add_permits(1);
        let reply: Value = serde_json::from_str(task.await.unwrap().unwrap().get()).unwrap();
        assert!(reply["items"][0].get("answers").is_some());
        let transient = code == "account_selection_unavailable";
        let expected_code = if transient {
            "routing_deferred"
        } else {
            "routing_refused"
        };
        assert_eq!(reply["items"][1]["error"]["code"], expected_code, "{code}");
        assert_eq!(
            reply["items"][1]["error"]["class"],
            if transient { "transient" } else { "permanent" }
        );
        assert_eq!(reply["items"][1]["error"]["detail"]["routing_code"], code);
        assert!(!reply["items"][1]["error"]["message"]
            .as_str()
            .unwrap()
            .contains(&format!("routing refused renewal: {code}")));
        let durable = journal(&rig);
        let refusal = &durable["runs"][0]["renewal_refusal"];
        assert_eq!(refusal["code"], expected_code);
        assert_eq!(refusal["detail"]["routing_code"], code);
        assert_eq!(refusal["detail"]["routing_reason"], "scripted refusal");
        let report = durable["outbox"]
            .as_array()
            .unwrap()
            .iter()
            .find(|rpc| rpc["method"] == "route.report")
            .unwrap();
        assert_eq!(
            report["routing_refusal"], *refusal,
            "full refusal is local durable report metadata, not a new routing wire member"
        );
        *rig.router.override_refusal.lock().unwrap() = None;
        let reply: Value = serde_json::from_str(
            call(&rig, &routed_request(&["paid", "unsent"]))
                .await
                .unwrap()
                .get(),
        )
        .unwrap();
        if transient {
            assert!(reply["items"][1].get("answers").is_some());
        } else {
            assert_eq!(reply["items"][1]["error"]["code"], expected_code);
            assert_eq!(rig.provider.calls("unsent"), 0);
        }
        assert_eq!(rig.provider.calls("paid"), 1);
        assert_eq!(rig.router.selects.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn audit_exit_invalid_renewal_does_not_poison_stored_answer_replay() {
    let _clock = ManualClock::new();
    let rig = routed_rig(true, true);
    rig.router.lease_ms.store(12_000, Ordering::SeqCst);
    let gate = Arc::new(Semaphore::new(0));
    rig.provider
        .script("paid", vec![Script::Held(gate.clone(), answered(Some(7)))]);
    let service = rig.service.clone();
    let task = tokio::spawn(async move {
        service
            .spawn_call(routed_request(&["paid"]), test_scope())
            .await
    });
    wait_for("provider entered", || rig.provider.calls("paid") == 1).await;
    rig.router.invalid_account.store(true, Ordering::SeqCst);
    tokio::time::advance(std::time::Duration::from_secs(8)).await;
    wait_for("renewal failure committed", || {
        journal(&rig)["runs"][0]["renew_failed"] == true
    })
    .await;
    gate.add_permits(1);
    let reply = task
        .await
        .unwrap()
        .expect("a renewal contract error cannot hide a paid in-flight answer");
    let parsed: Value = serde_json::from_str(reply.get()).unwrap();
    assert!(parsed["items"][0].get("answers").is_some());
    assert!(journal(&rig)["terminal_error"].is_null());
    rig.router.clear();
    assert_eq!(
        call(&rig, &routed_request(&["paid"])).await.unwrap().get(),
        reply.get()
    );
    assert!(rig.router.calls.lock().unwrap().is_empty());
    assert_eq!(rig.provider.calls("paid"), 1);
    assert!(!rig.router.pin_held.load(Ordering::SeqCst));
    assert_eq!(
        rig.router.reports.lock().unwrap().values().next().unwrap()["outcome"],
        "completed",
        "all dispatched work answered despite the unnecessary renewal failing"
    );
}

#[tokio::test]
async fn audit_exit_recovery_retains_an_ambiguous_pending_admission() {
    let rig = routed_rig(true, true);
    rig.router.panic_on_admission.store(true, Ordering::SeqCst);
    assert!(call(&rig, &routed_request(&["x"])).await.is_err());
    wait_for("abandoned batch released", || {
        rig.store
            .lookup(STORE_ROLE, "routed")
            .unwrap()
            .is_some_and(|batch| !batch.running)
    })
    .await;
    let original = journal(&rig)["pending_admission"].clone();
    assert!(!original.is_null());
    *rig.router.override_refusal.lock().unwrap() =
        Some(("route.account_for", "account_selection_unavailable"));
    rig.service.recover_routing().await.unwrap();
    assert_eq!(
        journal(&rig)["pending_admission"],
        original,
        "a refusal while recovering an unknown grant is not proof it never happened"
    );
    *rig.router.override_refusal.lock().unwrap() = None;
    rig.service.recover_routing().await.unwrap();
    assert!(journal(&rig)["pending_admission"].is_null());
    assert!(!rig.router.pin_held.load(Ordering::SeqCst));
    assert_eq!(rig.provider.calls("x"), 0);
}

fn disk_rig(label: &str) -> (PathBuf, RoutedRig) {
    let path = broca_test_support::scratch_root().join(format!(
        "routed-exit-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&path).unwrap();
    let rig = routed_rig_with_store(
        Arc::new(SystemOneStore::open(path.join("system-one.db")).unwrap()),
        Router::new(true, true),
    );
    (path, rig)
}

#[tokio::test]
async fn audit_exit_disabled_validation_precedes_broken_store_lookup() {
    let (path, mut rig) = disk_rig("disabled-order");
    Arc::get_mut(&mut rig.service).unwrap().config.routed_models = false;
    let sql = rusqlite::Connection::open(path.join("system-one.db")).unwrap();
    sql.execute_batch("ALTER TABLE batch RENAME TO unavailable_batch;")
        .unwrap();
    let no_model = call(&rig, &routed_request(&["x"])).await.unwrap_err();
    assert_eq!(no_model["code"], "invalid_params");
    assert_eq!(
        no_model["detail"]["field"], "model",
        "base parser refuses before the unavailable batch table can be read"
    );
    let mut unknown: Value = serde_json::from_str(&pinned(&["x"])).unwrap();
    unknown["model"] = json!("unknown/model");
    let unknown = call(&rig, &unknown.to_string()).await.unwrap_err();
    assert_eq!(
        unknown["code"], "model_unknown",
        "base catalog validation precedes its claim"
    );
    let valid = call(&rig, &pinned(&["x"])).await.unwrap_err();
    assert_eq!(valid["code"], "internal_error");
    assert_eq!(
        valid["message"], "the batch store is unavailable; re-send the batch",
        "store failure occurs at the base claim boundary"
    );
    assert!(rig.router.calls.lock().unwrap().is_empty());
    assert_eq!(rig.provider.calls("x"), 0);
    drop(sql);
    drop(rig);
    broca_test_support::remove_rig_dir(&path);
}

#[tokio::test]
async fn audit_exit_disabled_malformed_legacy_scope_keeps_base_refusal_bytes() {
    let (path, mut rig) = disk_rig("disabled-scope");
    rig.provider
        .script("x", vec![Script::Answer(answered(Some(7)))]);
    call(&rig, &pinned(&["x"])).await.unwrap();
    let sql = rusqlite::Connection::open(path.join("system-one.db")).unwrap();
    sql.execute_batch("UPDATE batch SET scope_json='invalid JSON', routing_choice=NULL, routing_runs=NULL, routing_pending=NULL;").unwrap();
    Arc::get_mut(&mut rig.service).unwrap().config.routed_models = false;
    let refusal = rig
        .service
        .clone()
        .spawn_call(pinned(&["x"]), test_scope())
        .await
        .unwrap_err();
    // The legacy claim compares the stored scope bytes without decoding them.
    const BASE: &[u8] = br#"{"code":"batch_scope_mismatch","message":"batch_id belongs to a different scope; a retry cannot change its attribution","detail":{"class":"permanent"}}"#;
    assert_eq!(refusal, BASE);
    assert_eq!(rig.provider.calls("x"), 1);
    drop(sql);
    drop(rig);
    broca_test_support::remove_rig_dir(&path);
}

async fn rejected_replacement(case: &str) {
    let mut rig = routed_rig(true, true);
    *rig.router.models.lock().unwrap() = vec![CLEF.into(), JEV.into()];
    let mut request: Value = serde_json::from_str(&routed_request(&["x"])).unwrap();
    match case {
        "questions" => {
            Arc::get_mut(&mut rig.service).unwrap().pins[0].max_questions = 1;
            request["questions"]["second"] = request["questions"]["q"].clone();
        }
        "images" => {
            request["items"][0]["images"] = json!([{"content_type":"image/png","base64":"AQ=="}])
        }
        "ceiling" => {
            let mut rows = rig.catalog.rows.0.clone();
            rows.get_mut(CLEF).unwrap().input_rate_nanos = Some(1);
            rows.get_mut(JEV).unwrap().input_rate_nanos = Some(1_000_000_000_000_000);
            Arc::get_mut(&mut rig.service).unwrap().catalog = Arc::new(Rows(rows));
        }
        _ => unreachable!(),
    }
    rig.provider.script("x", vec![Script::AccountError]);
    let params = request.to_string();
    let reply = call(&rig, &params)
        .await
        .expect("an unusable replacement leaves the original frozen account failure replayable");
    let parsed: Value = serde_json::from_str(reply.get()).unwrap();
    assert_eq!(parsed["model"], CLEF, "{case}");
    assert_eq!(parsed["decision_id"], "decision-0", "{case}");
    assert_eq!(
        rig.provider.calls("x"),
        1,
        "replacement never dispatches: {case}"
    );
    assert_eq!(journal(&rig)["reselection"]["finished"], true, "{case}");
    let choice: Value = serde_json::from_str(
        &rig.store
            .routing(STORE_ROLE, "routed")
            .unwrap()
            .unwrap()
            .choice,
    )
    .unwrap();
    assert_eq!(choice["select"]["decision_id"], "decision-0");
    let reports = rig.router.bodies("route.report");
    let abandoned = reports
        .iter()
        .find(|report| report["decision_id"] == "decision-1")
        .expect("replacement launch must be reported");
    assert_eq!(abandoned["outcome"], "launch_failed");
    rig.router.clear();
    assert_eq!(call(&rig, &params).await.unwrap().get(), reply.get());
    assert!(rig.router.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn audit_exit_invalid_replacement_question_bounds_report_launch_failure() {
    rejected_replacement("questions").await;
}

#[tokio::test]
async fn audit_exit_invalid_replacement_images_report_launch_failure() {
    rejected_replacement("images").await;
}

#[tokio::test]
async fn audit_exit_invalid_replacement_ceiling_reports_launch_failure() {
    rejected_replacement("ceiling").await;
}

#[tokio::test(start_paused = true)]
async fn audit_exit_expiry_reports_an_unfinished_decision_on_batch_basis() {
    let _clock = ManualClock::new();
    let rig = routed_rig(false, true);
    rig.provider.script("x", vec![Script::Status(503, None)]);
    call(&rig, &routed_request(&["x"])).await.unwrap();
    assert!(
        rig.router.bodies("route.report").is_empty(),
        "batch usage is not final before retry or expiry"
    );
    tokio::time::advance(std::time::Duration::from_millis(
        broca_store::system_one::RETENTION_MS as u64 + 1,
    ))
    .await;
    rig.service.recover_routing().await.unwrap();
    let reports = rig.router.bodies("route.report");
    assert_eq!(
        reports.len(),
        1,
        "expiry closes the decision even when a provider transient left it non-final"
    );
    assert_eq!(reports[0]["decision_id"], "decision-0");
    assert_eq!(reports[0]["outcome"], "failed");
    assert!(reports[0]
        .get("usage")
        .is_none_or(|usage| usage.get("cost_usd").is_none()));
}

#[tokio::test]
async fn audit_exit_reselection_refusals_are_returned_and_replayed_verbatim() {
    for code in [
        "invalid_request",
        "admission_conflict",
        "no_route",
        "future_refusal",
    ] {
        let rig = routed_rig(true, true);
        rig.provider.script("x", vec![Script::AccountError]);
        *rig.router.reselection_refusal.lock().unwrap() = Some(code);
        let refusal = call(&rig, &routed_request(&["x"])).await.unwrap_err();
        assert_eq!(refusal["code"], "routing_refused");
        assert_eq!(refusal["detail"]["routing_code"], code);
        assert_eq!(refusal["detail"]["class"], "permanent");
        rig.router.clear();
        assert_eq!(
            call(&rig, &routed_request(&["x"])).await.unwrap_err(),
            refusal
        );
        assert!(
            rig.router.calls.lock().unwrap().is_empty(),
            "a definitive replacement refusal does not select again"
        );
    }
}

#[tokio::test]
async fn audit_exit_invalid_report_acknowledgement_keeps_the_immutable_debt() {
    let rig = routed_rig(true, true);
    rig.provider
        .script("x", vec![Script::Answer(answered(Some(7)))]);
    *rig.router.override_answer.lock().unwrap() = Some((
        "route.report",
        json!({"decision_id":"wrong-decision","report_id":"not-confirmed","outcome":"completed"}),
    ));
    call(&rig, &routed_request(&["x"])).await.unwrap();
    let report = rig.router.bodies("route.report")[0].clone();
    assert_eq!(
        rig.store.pending_routing_batches(STORE_ROLE).unwrap(),
        ["routed"],
        "an invalid acknowledgement proves neither acceptance nor a definitive rejection"
    );
    *rig.router.override_answer.lock().unwrap() = None;
    rig.service.recover_routing().await.unwrap();
    assert_eq!(rig.router.bodies("route.report")[1], report);
    assert_eq!(rig.router.reports.lock().unwrap().len(), 1);
    assert!(rig
        .store
        .pending_routing_batches(STORE_ROLE)
        .unwrap()
        .is_empty());
    assert_eq!(rig.provider.calls("x"), 1);
}

#[tokio::test(start_paused = true)]
async fn audit_exit_overhaul_cleanup_drains_debt_without_deleting_expired_content() {
    let _clock = ManualClock::new();
    let root = broca_test_support::scratch_root().join(format!(
        "overhaul-routing-exits-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("state-format"), b"broca-state/2\n").unwrap();
    assert_eq!(
        std::fs::read(root.join("state-format")).unwrap(),
        b"broca-state/2\n"
    );
    // Startup disables destructive retention for broca-state/2 roots.
    let retention = false;
    let sentinel = root.join("unrelated-wal-tail");
    std::fs::write(&sentinel, b"unpublished WAL bytes").unwrap();
    let mut rig = routed_rig_with_store(
        Arc::new(SystemOneStore::open(root.join("system-one.db")).unwrap()),
        Router::new(true, true),
    );
    Arc::get_mut(&mut rig.service)
        .unwrap()
        .routing_content_retention = retention;
    rig.provider
        .script("private", vec![Script::Answer(answered(Some(7)))]);
    *rig.router.report_refused.lock().unwrap() = Some("store_error");
    call(&rig, &routed_request(&["private"])).await.unwrap();
    let before = rig.store.lookup(STORE_ROLE, "routed").unwrap().unwrap();
    tokio::time::advance(std::time::Duration::from_millis(
        broca_store::system_one::RETENTION_MS as u64 + 1,
    ))
    .await;
    *rig.router.report_refused.lock().unwrap() = None;
    rig.service.recover_routing().await.unwrap();
    rig.service.sweep().await.unwrap();
    let after = rig
        .store
        .lookup(STORE_ROLE, "routed")
        .unwrap()
        .expect("overhaul cleanup cannot delete the expired batch");
    assert_eq!(after.identity, before.identity);
    assert_eq!(after.outcomes, before.outcomes);
    assert_eq!(std::fs::read(&sentinel).unwrap(), b"unpublished WAL bytes");
    assert_eq!(
        rig.router.reports.lock().unwrap().len(),
        1,
        "the owed report still drains"
    );
    assert!(rig
        .store
        .pending_routing_batches(STORE_ROLE)
        .unwrap()
        .is_empty());
    assert!(rig.service.stored_batch("routed").await.unwrap().is_none());
    rig.router.clear();
    let refusal = call(&rig, &routed_request(&["private"])).await.unwrap_err();
    assert_eq!(refusal["code"], "routing_unavailable");
    assert!(rig.router.calls.lock().unwrap().is_empty());
    assert_eq!(rig.provider.calls("private"), 1);
    drop(rig);
    broca_test_support::remove_rig_dir(&root);
}

#[tokio::test(start_paused = true)]
async fn audit_exit_provider_reply_is_polled_while_renewal_waits() {
    let _clock = ManualClock::new();
    let rig = routed_rig(true, true);
    rig.router.lease_ms.store(12_000, Ordering::SeqCst);
    let renewal = Arc::new(Semaphore::new(0));
    *rig.router.renewal_reply_gate.lock().unwrap() = Some(renewal.clone());
    let paid = Arc::new(Semaphore::new(0));
    rig.provider.script(
        "paid",
        vec![Script::TimedHeld(paid.clone(), answered(Some(7)))],
    );
    let service = rig.service.clone();
    let task = tokio::spawn(async move {
        service
            .spawn_call(routed_request(&["paid"]), test_scope())
            .await
    });
    wait_for("provider entered", || rig.provider.calls("paid") == 1).await;
    tokio::time::advance(std::time::Duration::from_secs(8)).await;
    wait_for("renewal grant recorded before reply gate", || {
        rig.router.admissions.lock().unwrap().len() == 2
    })
    .await;
    paid.add_permits(1);
    let provider = rig.provider.clone();
    tokio::task::spawn_blocking(move || {
        broca_test_support::wait_until_sync(
            "ready provider reply polled while renewal is blocked",
            || {
                (provider.held.load(Ordering::SeqCst) == 0)
                    .then_some(())
                    .ok_or_else(|| "provider future is still unpolled".into())
            },
        );
    })
    .await
    .unwrap();
    assert_eq!(renewal.available_permits(), 0);
    tokio::time::advance(std::time::Duration::from_secs(3)).await;
    renewal.add_permits(1);
    let reply: Value = serde_json::from_str(task.await.unwrap().unwrap().get()).unwrap();
    assert!(
        reply["items"][0].get("answers").is_some(),
        "the provider completed before its response deadline even though routing was slow"
    );
    assert_eq!(rig.provider.calls("paid"), 1);
}

#[test]
fn audit_exit_recovery_drop_does_not_remove_a_live_call_claim() {
    let rig = routed_rig(true, true);
    rig.service
        .claims
        .lock()
        .unwrap()
        .insert("routed".into(), test_scope());
    let recovery = crate::system_one::classify::Hold {
        service: &rig.service,
        batch_id: "routed".into(),
        stored: false,
        local: false,
    };
    drop(recovery);
    assert!(
        rig.service.claims.lock().unwrap().contains_key("routed"),
        "a recovery hold never owned the live call's in-process claim"
    );
}

#[tokio::test(start_paused = true)]
async fn audit_exit_expiry_does_not_depend_on_decoding_caller_scope() {
    let _clock = ManualClock::new();
    let (path, rig) = disk_rig("expired-scope");
    rig.provider
        .script("private", vec![Script::Answer(answered(Some(7)))]);
    *rig.router.report_refused.lock().unwrap() = Some("store_error");
    call(&rig, &routed_request(&["private"])).await.unwrap();
    let sql = rusqlite::Connection::open(path.join("system-one.db")).unwrap();
    sql.execute_batch("UPDATE batch SET scope_json='invalid JSON';")
        .unwrap();
    tokio::time::advance(std::time::Duration::from_millis(
        broca_store::system_one::RETENTION_MS as u64 + 1,
    ))
    .await;
    rig.service.recover_routing().await.unwrap();
    let identity: String = sql
        .query_row(
            "SELECT identity FROM batch WHERE batch_id='routed'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        identity, "{}",
        "irrelevant corrupt scope metadata cannot prevent content expiry"
    );
    *rig.router.report_refused.lock().unwrap() = None;
    rig.service.recover_routing().await.unwrap();
    assert!(rig.store.lookup(STORE_ROLE, "routed").unwrap().is_none());
    drop(sql);
    drop(rig);
    broca_test_support::remove_rig_dir(&path);
}

#[tokio::test(start_paused = true)]
async fn audit_exit_expired_unknown_first_admission_replays_then_releases_late_grant() {
    let _clock = ManualClock::new();
    let rig = routed_rig(true, true);
    rig.router
        .lose_first_admission_before_grant
        .store(true, Ordering::SeqCst);
    let refusal = call(&rig, &routed_request(&["private-late-admission"]))
        .await
        .unwrap_err();
    assert_eq!(refusal["code"], "routing_unavailable");
    assert!(
        rig.router.admissions.lock().unwrap().is_empty(),
        "the grant has not completed yet"
    );
    let body = rig.router.bodies("route.account_for")[0].clone();
    tokio::time::advance(std::time::Duration::from_millis(
        broca_store::system_one::RETENTION_MS as u64 + 1,
    ))
    .await;
    rig.router.unreachable.store(true, Ordering::SeqCst);
    rig.service.recover_routing().await.unwrap();
    let row = rig
        .store
        .lookup(STORE_ROLE, "routed")
        .unwrap()
        .expect("unknown grant remains a routing-only cleanup debt");
    assert_eq!(row.identity, "{}");
    assert!(row.outcomes.is_empty());
    let archive = rig.store.routing(STORE_ROLE, "routed").unwrap().unwrap();
    assert!(!archive.runs.contains("private-late-admission"));
    let archive: Value = serde_json::from_str(&archive.runs).unwrap();
    assert!(
        archive["outbox"]
            .as_array()
            .unwrap()
            .iter()
            .any(|rpc| rpc["method"] == "route.account_for"
                && rpc["params"] == body
                && rpc["closed"] == false),
        "TTL must preserve the exact unresolved admission request, not only an unbound finish"
    );
    rig.router.unreachable.store(false, Ordering::SeqCst);
    rig.service.recover_routing().await.unwrap();
    let bodies = rig.router.bodies("route.account_for");
    assert_eq!(bodies.len(), 3);
    assert!(bodies.iter().all(|replay| *replay == body));
    assert_eq!(
        rig.router.admissions.lock().unwrap().len(),
        1,
        "routing really granted the admission late"
    );
    assert!(!rig.router.pin_held.load(Ordering::SeqCst));
    assert!(rig
        .router
        .bodies("route.account_observe")
        .iter()
        .any(|event| event["kind"] == "run_finished"
            && event["admission_id"] == body["admission_id"]));
    assert!(rig.store.lookup(STORE_ROLE, "routed").unwrap().is_none());
    assert_eq!(rig.provider.calls("private-late-admission"), 0);
    assert_eq!(rig.router.selects.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn audit_exit_refused_or_invalid_replacement_admission_finishes_and_reports_its_decision() {
    for invalid in [false, true] {
        let rig = routed_rig(true, true);
        rig.provider.script("x", vec![Script::AccountError]);
        rig.router
            .refuse_reselection
            .store(!invalid, Ordering::SeqCst);
        rig.router
            .invalid_reselection_account
            .store(invalid, Ordering::SeqCst);
        let refusal = call(&rig, &routed_request(&["x"])).await.unwrap_err();
        if invalid {
            assert_eq!(refusal["code"], "internal_error");
        } else {
            assert_eq!(refusal["detail"]["routing_code"], "account_not_authorized");
        }
        assert_eq!(
            journal(&rig)["reselection"]["finished"],
            true,
            "a definitively refused replacement is not left pending forever"
        );
        let reports = rig.router.bodies("route.report");
        let report = reports
            .iter()
            .find(|report| report["decision_id"] == "decision-1")
            .expect("the committed replacement must be reported");
        assert_eq!(report["outcome"], "launch_failed");
        rig.router.clear();
        assert_eq!(
            call(&rig, &routed_request(&["x"])).await.unwrap_err(),
            refusal
        );
        assert!(rig.router.calls.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn audit_exit_account_failure_cascades_over_earlier_unanswered_errors_immediately() {
    for run_reports in [false, true] {
        let rig = routed_rig(run_reports, true);
        rig.provider
            .script("temporary", vec![Script::Status(503, None)]);
        rig.provider
            .script("paid", vec![Script::Answer(answered(Some(7)))]);
        rig.provider.script("account", vec![Script::AccountError]);
        let params = routed_request(&["temporary", "paid", "account"]);
        let reply = call(&rig, &params).await.unwrap();
        let value: Value = serde_json::from_str(reply.get()).unwrap();
        assert_eq!(
            value["items"][0]["error"]["code"], "account_not_entitled",
            "all unanswered work, not only later indices, gets the account failure"
        );
        assert!(value["items"][1].get("answers").is_some());
        assert_eq!(
            rig.router.bodies("route.report")[0]["runner_code"],
            "account_not_entitled"
        );
        rig.router.clear();
        assert_eq!(call(&rig, &params).await.unwrap().get(), reply.get());
        assert!(rig.router.calls.lock().unwrap().is_empty());
        assert_eq!(rig.provider.calls("temporary"), 1);
        assert_eq!(rig.provider.calls("paid"), 1);
        assert_eq!(rig.router.selects.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn audit_exit_every_first_admission_boundary_preserves_refusal_class_and_pin() {
    for method in ["role.describe", "route.select", "route.account_for"] {
        for code in [
            "invalid_request",
            "admission_conflict",
            "account_selection_unavailable",
            "future_refusal",
        ] {
            let rig = routed_rig(true, true);
            *rig.router.override_refusal.lock().unwrap() = Some((method, code));
            let refusal = call(&rig, &pinned(&["x"])).await.unwrap_err();
            let transient = code == "account_selection_unavailable";
            assert_eq!(
                refusal["detail"]["class"],
                if transient { "transient" } else { "permanent" },
                "{method}: {code}"
            );
            assert_eq!(
                refusal["code"],
                if transient {
                    "routing_deferred"
                } else {
                    "routing_refused"
                },
                "{method}: {code}"
            );
            assert_eq!(refusal["detail"]["routing_code"], code);
            assert_eq!(
                refusal["detail"]["model"], JEV,
                "the caller's pin survives every answered refusal boundary: {method}"
            );
            assert_eq!(rig.provider.calls("x"), 0);
        }
    }
}
