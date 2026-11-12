use super::*;

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
