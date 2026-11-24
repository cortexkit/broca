use super::*;
use broca_wire::diagnostics::Finding;
use std::collections::BTreeMap;

fn finding(check: &str, key: &str, severity: Severity, count: u64) -> Finding {
    Finding {
        check: check.into(),
        key: key.into(),
        severity,
        count,
        first_seen_ms: count as i64,
        last_seen_ms: count as i64,
        summary: format!("{count} things"),
        hint: String::new(),
        samples: vec![json!({ "run_id": count })],
    }
}

fn reply(findings: Vec<Finding>) -> Reply {
    Reply {
        generated_at_ms: 0,
        window_ms: DEFAULT_WINDOW_MS,
        findings,
        coverage: BTreeMap::new(),
        findings_omitted: 0,
    }
}

/// The digest follows each finding's check, key and severity, in any order, and ignores counts,
/// times, summaries and samples; the worst severity is `ok` with no finding.
#[test]
fn the_digest_follows_which_findings_there_are_not_their_counts() {
    let a = finding("run_errors", "k1", Severity::Warn, 3);
    let b = finding("model_failing", "k2", Severity::Error, 3);
    let (digest, worst) = findings_digest(&reply(vec![a.clone(), b.clone()]));
    assert_eq!(worst, "error");
    let mut busier = (a.clone(), b.clone());
    busier.0.count = 9;
    busier.1.samples.clear();
    assert_eq!(
        findings_digest(&reply(vec![busier.1, busier.0])).0,
        digest,
        "order, counts and samples don't change the digest"
    );
    let mut worse = a.clone();
    worse.severity = Severity::Error;
    assert_ne!(findings_digest(&reply(vec![worse, b.clone()])).0, digest);
    assert_ne!(findings_digest(&reply(vec![b])).0, digest);
    assert_eq!(findings_digest(&reply(vec![a])).1, "warn");
    assert_eq!(findings_digest(&reply(Vec::new())).1, "ok");
}

fn store() -> &'static Arc<SqliteStore> {
    STORE.get_or_init(|| {
        let store = SqliteStore::open_in_memory().unwrap();
        store.enable_bus_events("box_test").unwrap();
        Arc::new(store)
    })
}

/// `events_get` answers a kept event with its stored body, the body's digest and when it was
/// recorded; a wrong key, name or version is `event_not_visible`, and an unknown member or a
/// missing one is `invalid_request`.
#[test]
fn events_get_answers_a_kept_event_and_refuses_everything_else() {
    let key = store()
        .record_findings(&format!("digest-{}", line!()), "error", 1_700_000_000_000)
        .unwrap()
        .unwrap();
    let body = store().findings_event(&key).unwrap().unwrap().body;
    let reply =
        events_get(json!({"event_key": key, "event_name": "findings_changed", "version": 1}))
            .unwrap();
    assert_eq!(
        reply,
        json!({
            "body": String::from_utf8(body.clone()).unwrap(),
            "digest": format!("{:x}", Sha256::digest(&body)),
            "event_key": key,
            "event_name": "findings_changed",
            "published_at": "2023-11-14T22:13:20.000Z",
        })
    );
    for params in [
        json!({"event_key": "unknown", "event_name": "findings_changed", "version": 1}),
        json!({"event_key": key, "event_name": "job_page", "version": 1}),
        json!({"event_key": key, "event_name": "findings_changed", "version": 2}),
    ] {
        assert_eq!(events_get(params).unwrap_err().code, "event_not_visible");
    }
    for params in [
        json!({"event_key": key, "event_name": "findings_changed", "version": 1, "extra": 1}),
        json!({"event_key": key, "event_name": "findings_changed"}),
    ] {
        assert_eq!(events_get(params).unwrap_err().code, "invalid_request");
    }
}

/// Writes reader samples, as synapse's `findings-changed-v1` samples are laid out, when
/// `BROCA_FINDINGS_SAMPLES_DIR` names a folder: the notice the drain hands the bus (with the two
/// headers the bus client adds, message id and digest) and `events_get` replies.
#[test]
fn write_findings_changed_samples() {
    use broca_store::bus_events::RunEventStore;
    let Some(dir) = std::env::var_os("BROCA_FINDINGS_SAMPLES_DIR") else {
        return;
    };
    let dir = std::path::PathBuf::from(dir);
    std::fs::create_dir_all(&dir).unwrap();
    let store = SqliteStore::open_in_memory().unwrap();
    store.enable_bus_events("box_test").unwrap();
    let key = store
        .record_findings("sample", "error", 1_700_000_000_000)
        .unwrap()
        .unwrap();
    let notice = store.pending_findings_event().unwrap().unwrap();
    assert_eq!(notice.event_key, key);
    let mut headers = serde_json::Map::new();
    headers.insert("Nats-Msg-Id".into(), json!(notice.event_key));
    headers.insert(
        "Ck-Content-Digest".into(),
        json!(notice.digest().to_string()),
    );
    for (name, value) in &notice.headers {
        headers.insert(name.clone(), json!(value));
    }
    let write = |name: &str, value: Value| {
        let bytes = format!("{}\n", serde_json::to_string_pretty(&value).unwrap());
        std::fs::write(dir.join(format!("{name}.json")), bytes).unwrap();
    };
    write(
        "notice",
        json!({"name": "notice", "subject": notice.subject, "headers": headers, "body": ""}),
    );
    let _ = STORE.set(Arc::new(store));
    let store = STORE.get().unwrap();
    // A second event, as after the findings clear, so the samples show the generation advance.
    let cleared = store
        .record_findings("cleared", "ok", 1_700_000_060_000)
        .unwrap()
        .unwrap();
    for (name, params) in [
        (
            "events-get-error",
            json!({"event_key": key, "event_name": "findings_changed", "version": 1}),
        ),
        (
            "events-get-cleared",
            json!({"event_key": cleared, "event_name": "findings_changed", "version": 1}),
        ),
        (
            "events-get-unknown-key",
            json!({"event_key": "unknown", "event_name": "findings_changed", "version": 1}),
        ),
        (
            "events-get-wrong-name",
            json!({"event_key": key, "event_name": "job_page", "version": 1}),
        ),
        (
            "events-get-wrong-version",
            json!({"event_key": key, "event_name": "findings_changed", "version": 2}),
        ),
    ] {
        let response = match events_get(params.clone()) {
            Ok(result) => json!({ "result": result }),
            Err(error) => json!({ "error": error }),
        };
        write(
            name,
            json!({"name": name, "request": {"method": OP_EVENTS_GET, "params": params}, "response": response}),
        );
    }
}
