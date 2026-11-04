#[test]
fn namespace_spend_fact_omits_project_but_keeps_storage_identity_and_literal_id() {
    let mut start = started(1, "run", Some(10));
    let WalRecord::RunStarted { session, .. } = &mut start.record else { unreachable!() };
    session.project_root = "broca-unscoped-v1".into();
    let records = vec![start, step(2, 1, usage(12, 3), 0), terminal(3, Some(20), usage(12, 3))];
    let facts = compute_segments(&records, "source", &resolver);
    assert_eq!(facts.len(), 1);
    let fact = &facts[0];
    assert_eq!(fact.fact_id, "d06f97667872a914dc84ba42545ea52f5cfcae51bbb2bf8d8339d5387c4bc08f");
    assert_eq!(fact.session.project_root.as_os_str(), "broca-unscoped-v1");
    let value = serde_json::to_value(fact).unwrap();
    assert_eq!(value["session"], json!({"harness":"runner", "session":"session"}));
    assert_eq!(serde_json::from_value::<SpendSegment>(value).unwrap(), *fact);
    let mut invalid = serde_json::to_value(fact).unwrap();
    invalid["session"]["project_root"] = serde_json::Value::Null;
    assert!(serde_json::from_value::<SpendSegment>(invalid).is_err());
    assert_eq!(serde_json::to_value(&records[0].record).unwrap()["session"]["project_root"], "broca-unscoped-v1");
    assert_eq!(warm_fact_id("source", &fact.session, &RunId("run".into()), 4), "c132c2fe9705d2fcaa254fe8519db7cb7f315f7969877073808828d735eb212a");
    let other = BindIdentity::new("broca-unscoped-v1", "runner", "another-session");
    assert_ne!(fact_id("source", &other, &RunId("run".into()), 0), fact.fact_id);
}

#[test]
fn directory_spend_fact_session_bytes_keep_the_legacy_field_order_and_values() {
    for root in ["/stamped/worktree", "relative", "", "broca-unscoped-v1/", "./broca-unscoped-v1"] {
        let mut start = started(1, "run", Some(10));
        let WalRecord::RunStarted { session, .. } = &mut start.record else { unreachable!() };
        session.project_root = root.into();
        let records = vec![start, step(2, 1, usage(12, 3), 0), terminal(3, Some(20), usage(12, 3))];
        let fact = compute_segments(&records, "source", &resolver).remove(0);
        let bytes = serde_json::to_string(&fact).unwrap();
        let legacy_session = format!(r#""session":{{"project_root":{},"harness":"runner","session":"session"}}"#, serde_json::to_string(root).unwrap());
        assert!(bytes.contains(&legacy_session), "{bytes}");
        assert_eq!(serde_json::from_str::<SpendSegment>(&bytes).unwrap(), fact);
    }
}
