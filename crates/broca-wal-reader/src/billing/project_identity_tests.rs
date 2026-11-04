#[test]
fn namespace_attempt_session_is_projectless_only_at_the_reader_boundary() {
    for warm in [false, true] {
        let mut log = Log::new();
        log.open(W);
        let mut reserved = reserved(1, warm);
        reserved["reservation"]["session"]["project_root"] = json!("broca-unscoped-v1");
        let reservation = reserved["reservation"].clone();
        log.push(reserved, W);
        log.open(NEXT);
        let original_wal = log.raw.clone();
        let (items, checkpoint) = log.read(None);
        let first = charges(&items);
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].reservation["session"], json!({"harness":"runner", "session":"s", "session_ref":"ref"}));
        assert!(!first[0].typed_reservation().unwrap().session.contains_key("project_root"));
        assert_eq!(log.raw, original_wal);
        let mut finished = finished(1, warm);
        finished["reservation"] = reservation;
        log.push(finished, NEXT);
        let (items, _) = log.read(Some(&checkpoint));
        assert_eq!(charges(&items).len(), 1);
        assert!(charges(&items)[0].reservation["session"].get("project_root").is_none());
    }
}

#[test]
fn non_namespace_attempt_session_projects_remain_byte_identical() {
    for root in ["/stamped/worktree", "", "relative", "./broca-unscoped-v1", "broca-unscoped-v1/"] {
        let mut log = Log::new();
        log.open(W);
        let mut finished = finished(1, false);
        finished["reservation"]["session"]["project_root"] = json!(root);
        let expected = serde_json::to_vec(&finished["reservation"]).unwrap();
        log.push(finished, W);
        let (items, _) = log.read(None);
        assert_eq!(serde_json::to_vec(&charges(&items)[0].reservation).unwrap(), expected);
    }
}
