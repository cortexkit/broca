use super::*;

/// The fork cases that route the head's account: the kit installs a routing peer on their head,
/// so the head runs on its own vault-backed runner ([`routed_accounts::session_live`]).
const ROUTED_CASES: [&str; 6] = [
    "fork_answer_ask_id_replays",
    "fork_answer_survives_restart",
    "fork_answer_account_for_pins_head_credential",
    "fork_answer_conversation_key_bounded",
    "fork_answer_account_held_refusal",
    "fork_answer_account_blocked_refusal",
];

/// Labels the kit opens fork-case heads under, besides the case names themselves.
const FIXTURE_LABELS: [&str; 5] = [
    "fork_answer_",
    "fork-idle",
    "fork-isolation",
    "fork-callers",
    "invalid-fork-plan",
];

/// Whether `session` is the head of one of the kit's fork cases (`cf-<label>-<n>`).
pub(super) fn fixture_session(session: &str) -> bool {
    FIXTURE_LABELS.iter().any(|label| session.contains(label))
}

/// Whether `session` is the head of a fork case that routes the head's account. The kit names
/// a case's sessions after the case (`cf-<case>-<n>`).
pub(super) fn routed_session(session: &str) -> bool {
    ROUTED_CASES.iter().any(|case| session.contains(case))
}

/// Plan members the runner serves only for a head kept in the session store. A head starts there
/// when the send creating it names the head session class.
pub(super) const NEW_STORAGE_PLAN_MEMBERS: [&str; 2] = ["fork_callers", "readers"];

pub(super) type PendingAsks = HashMap<String, HashMap<(String, String), String>>;
pub(super) type RawRequests = HashMap<String, Vec<(Option<String>, Vec<u8>)>>;

/// Correlate a fixture ask with model ingress without adding anything to the provider bytes.
pub(super) struct AskCapture {
    shared: Arc<Shared>,
    session: String,
    key: (String, String),
}

impl AskCapture {
    pub(super) fn new(
        shared: &Arc<Shared>,
        session: &str,
        method: &str,
        params: &Value,
    ) -> Option<Self> {
        if method != "session.fork_ask" {
            return None;
        }
        let key = (
            params["fork_id"].as_str()?.to_owned(),
            params["ask_id"].as_str()?.to_owned(),
        );
        let question = params["question"].as_str()?.to_owned();
        shared
            .fork_asks
            .lock()
            .unwrap()
            .entry(session.into())
            .or_default()
            .insert(key.clone(), question);
        Some(Self {
            shared: shared.clone(),
            session: session.into(),
            key,
        })
    }
}

impl Drop for AskCapture {
    fn drop(&mut self) {
        if let Some(asks) = self.shared.fork_asks.lock().unwrap().get_mut(&self.session) {
            asks.remove(&self.key);
        }
    }
}

impl Shared {
    pub(super) fn record_model_ingress(&self, session: &str, request: &Value, bytes: &[u8]) {
        let messages = request
            .get("messages")
            .or_else(|| request.get("input"))
            .or_else(|| request.get("contents"));
        let last_user = messages
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .rev()
            .find(|message| message["role"] == "user");
        let text = last_user.map(|message| {
            let content = message
                .get("content")
                .or_else(|| message.get("parts"))
                .unwrap_or(&Value::Null);
            if let Some(text) = content.as_str() {
                return text.to_owned();
            }
            content
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|part| part["text"].as_str())
                .collect::<String>()
        });
        // Only an outstanding fixture ask with the received final question labels a fork.
        // The original session can make a model call while a fork's earlier call is held.
        let fork = self
            .fork_asks
            .lock()
            .unwrap()
            .get(session)
            .and_then(|asks| {
                asks.iter()
                    .find(|(_, question)| {
                        text.as_deref()
                            .is_some_and(|text| text == question.as_str())
                    })
                    .map(|((fork, _), _)| fork.clone())
            });
        self.fork_raw_requests
            .lock()
            .unwrap()
            .entry(session.into())
            .or_default()
            .push((fork, bytes.to_vec()));
        self.raw_bodies
            .lock()
            .unwrap()
            .entry(session.into())
            .or_default()
            .push(bytes.to_vec());
    }
}

#[test]
fn fork_raw_capture_preserves_bytes_and_distinguishes_concurrent_head() {
    let shared = Arc::new(Shared::default());
    let params = json!({"fork_id":"issued-fork", "ask_id":"ask", "question":"private question"});
    let capture = AskCapture::new(&shared, "head", "session.fork_ask", &params).unwrap();
    let head = br#"{ "messages" : [{"role":"user","content":"head question"}],"tools":[] }"#;
    let fork = br#"{"messages":[{"role":"user","content":[{"type":"text","text":"private question"}]}], "tools" : []}"#;
    for bytes in [head.as_slice(), fork.as_slice()] {
        shared.record_model_ingress("head", &serde_json::from_slice(bytes).unwrap(), bytes);
    }
    drop(capture);
    assert!(shared.fork_asks.lock().unwrap()["head"].is_empty());
    let captures = shared.fork_raw_requests.lock().unwrap();
    assert_eq!(
        captures["head"],
        vec![
            (None, head.to_vec()),
            (Some("issued-fork".into()), fork.to_vec())
        ]
    );
}

#[test]
fn heads_whose_plan_needs_the_new_storage_are_created_as_heads() {
    let subject = super::held_call_tests::subject(Arc::new(Shared::default()));
    let first = |members: Value| {
        subject
            .send_fields_with_plan(
                "cf-fork_answer_never_writes_head-55",
                true,
                members.as_object().unwrap(),
            )
            .unwrap()
    };
    for members in [
        json!({"fork_callers":["aft"]}),
        json!({"readers":[{"module":"magic-context"}]}),
    ] {
        assert_eq!(first(members.clone())["session_class"], "head", "{members}");
    }
    assert!(first(json!({})).get("session_class").is_none());
    // A later send never names a class: the session already exists.
    let later = subject
        .send_fields_with_plan(
            "cf-fork_answer_never_writes_head-55",
            false,
            json!({"fork_callers":["aft"]}).as_object().unwrap(),
        )
        .unwrap();
    assert!(later.get("session_class").is_none());
}

#[test]
fn every_fork_case_head_is_served_turns_in_request_order() {
    for session in [
        "cf-fork_answer_base_matches_digest-32",
        "cf-fork_answer_survives_restart-83",
        "cf-fork-idle-50",
        "cf-fork-isolation-55",
        "cf-fork-callers-60",
        "cf-invalid-fork-plan-61",
    ] {
        assert!(fixture_session(session), "{session}");
    }
    for session in [
        "cf-plan-view-3",
        "conformance-default",
        "cf-session-class-9",
    ] {
        assert!(!fixture_session(session), "{session}");
    }
}
