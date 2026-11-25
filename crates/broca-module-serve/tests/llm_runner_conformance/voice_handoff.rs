//! Untouched wire bodies from a named sender's two ways into a head session.
use super::*;
use broca_subc::SubscribeStream;
use cortexkit_role_llm_runner_conformance::{ScriptedToolCall, ScriptedTurn};
use cortexkit_role_tool_provider::catalog::{composition_digest, schema_digest};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const SESSION: &str = "voice-handoff-head";
const PROVIDER: &str = "voice-say-fixture";
const STEER: &str = "A voice caller asks about the weather.";
const IDLE: &str = "A voice caller asks for the answer now.";
const SPOKEN: &str = "The forecast is sunny.";

fn schema() -> Value {
    json!({"type":"object","properties":{"text":{"type":"string"},"reply_to":{"type":"integer"}},"required":["text","reply_to"]})
}

#[derive(Clone, Default)]
struct Say {
    calls: Arc<Mutex<Vec<ToolExchange>>>,
}

struct ToolExchange {
    request: Vec<u8>,
    reply: Vec<u8>,
}

#[async_trait]
impl subc_client_rs::ModuleHandler for Say {
    async fn handle(
        &self,
        _: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        let request = decode(&body);
        // An empty compaction view keeps all messages while enabling request-scoped tag reads.
        let reply = match request["method"].as_str() {
            Some("role.describe") => {
                let fixture = decode(include_bytes!(
                    "../../../broca-subc/tests/fixtures/compaction-provider-v1/role-describe.json"
                ));
                fixture["canonical"][0]["answer"].clone()
            }
            Some("compaction.setup") => {
                json!({"answer":"ready", "request_id":request["params"]["request_id"],
                "initial":{"compaction_id":"empty", "version":1, "range":{"from":0,"to":0}, "replacement":[]}})
            }
            Some("compaction.notice") => json!({"answer":"received"}),
            Some("compaction.ride") => json!({"answer":"nothing"}),
            Some(method) => panic!("unexpected compaction request {method}: {request}"),
            None => match request["name"].as_str() {
                Some("tool.catalog") => {
                    let schema = schema();
                    json!({"generation":"1","catalog_digest":"voice-say-fixture",
                    "composition_digest":composition_digest(&request["arguments"]["composition"]).unwrap(),
                    "tools":[{"name":"say","description":"Speak to the voice caller",
                    "schema_digest":schema_digest(&schema).unwrap(),"semantics":1,"capabilities":[],"input_schema":schema}]})
                }
                Some("say") => json!({"content":[{"type":"text","text":"sent"}]}),
                name => panic!("unexpected voice tool request {name:?}: {request}"),
            },
        };
        let bytes = serde_json::to_vec(&reply).unwrap();
        if request["name"] == "say" {
            self.calls.lock().unwrap().push(ToolExchange {
                request: body,
                reply: bytes.clone(),
            });
        }
        subc_client_rs::HandlerOutcome::Response(bytes)
    }

    async fn on_bind(&self, _: &subc_client_rs::RouteBindRequest) -> subc_client_rs::BindDecision {
        subc_client_rs::BindDecision::accept()
    }
}

fn manifest() -> subc_protocol::manifest::ModuleManifest {
    use subc_protocol::manifest::{Concurrency, ExecutionMode, IdentityScope, ProviderRole, Tool};
    subc_protocol::manifest::ModuleManifest::builder(PROVIDER, env!("CARGO_PKG_VERSION"))
        .provides(vec![ProviderRole::ToolProvider {
            tools: vec![Tool {
                name: "say".into(),
                description: Some("Speak to the voice caller".into()),
                execution_mode: ExecutionMode::Pure,
                schema: schema(),
            }],
            identity_scope: vec![IdentityScope::Project, IdentityScope::Session],
            concurrency: Concurrency::ModuleManaged,
            emits_push: false,
            sub_supervises: false,
        }])
        .build()
}

fn decode(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes).expect("the captured body is JSON")
}

fn request(method: &str, params: Value) -> Vec<u8> {
    serde_json::to_vec(&json!({"method":method,"params":params})).unwrap()
}

async fn terminal(route: &BrocaRoute, body: Vec<u8>) -> Result<Vec<u8>, Vec<u8>> {
    route
        .connection
        .route_request_terminal(&route.route, body)
        .await
        .unwrap()
}

async fn response(route: &BrocaRoute, body: Vec<u8>) -> Vec<u8> {
    terminal(route, body)
        .await
        .unwrap_or_else(|error| panic!("request refused: {}", String::from_utf8_lossy(&error)))
}

async fn next(stream: &mut SubscribeStream) -> Vec<u8> {
    tokio::time::timeout(broca_test_support::EVENT_HANG_GUARD, stream.next())
        .await
        .expect("the live subscription produced its next body")
        .unwrap()
        .expect("the subscription is still attached")
}

async fn through_run(stream: &mut SubscribeStream, run: &Value) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    loop {
        let bytes = next(stream).await;
        let body = decode(&bytes);
        frames.push(bytes);
        if body["event"]["type"] == "run_finished" && body["event"]["run_id"] == *run {
            assert_eq!(body["event"]["reason"], "completed", "{body}");
            return frames;
        }
    }
}

fn text_turn(text: &str) -> ScriptedTurn {
    ScriptedTurn::new(vec![ScriptedPart::Text(text.into())]).with_finish_reason("stop")
}

fn tool_turn(id: &str, tool: &str, arguments: Value, result: Value) -> ScriptedTurn {
    ScriptedTurn::new(vec![ScriptedPart::ToolCall(ScriptedToolCall {
        tool_call_id: id.into(),
        tool: tool.into(),
        arguments,
        result,
        hold: false,
    })])
    .with_finish_reason("tool_calls")
}

fn handoff(send_id: &str, text: &str) -> Vec<u8> {
    request(
        "session.send",
        json!({"send_id":send_id,"prompt":text,"delivery":["steer","idle_start"]}),
    )
}

fn handoff_entry<'a>(page: &'a Value, text: &str) -> &'a Value {
    let entry = page["result"]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["message"]["content"][0]["text"] == text)
        .unwrap_or_else(|| panic!("the hand-off is missing: {page}"));
    assert_eq!(entry["message"]["role"], "user");
    assert_eq!(entry["sender"], SENDER_MODULE, "{entry}");
    entry
}

fn assert_stream_message(frames: &[Vec<u8>], entry: &Value) {
    assert!(
        frames.iter().any(|bytes| {
            let body = decode(bytes);
            body["kind"] == "control" && body["event"]["mid"] == entry["mid"]
        }),
        "the live subscription missed {entry}; frames: {:?}",
        frames.iter().map(|bytes| decode(bytes)).collect::<Vec<_>>()
    );
}

fn prompt_tag(bytes: &[u8], text: &str) -> u64 {
    let body = decode(bytes);
    let messages = body["messages"].as_array().unwrap();
    messages
        .iter()
        .filter(|message| message["role"] == "user")
        .flat_map(|message| match &message["content"] {
            Value::Array(parts) => parts
                .iter()
                .filter_map(|part| part["text"].as_str())
                .collect(),
            value => value.as_str().into_iter().collect::<Vec<_>>(),
        })
        .find_map(|line| {
            let (tag, rest) = line.strip_prefix('\u{a7}')?.split_once("\u{a7} ")?;
            (rest == text).then(|| tag.parse().unwrap())
        })
        .unwrap_or_else(|| panic!("no model-visible tag for {text:?}: {messages:?}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn capture_voice_handoff_reader_frames() {
    let Some(output) = std::env::var_os("BROCA_VOICE_HANDOFF_SAMPLES").map(PathBuf::from) else {
        return;
    };
    for (name, variable) in [
        ("ck-subc", "SUBC_CORE_BIN"),
        ("fake-aft-stub", "FAKE_AFT_STUB_BIN"),
    ] {
        let binary = sibling_bin(name, variable);
        assert!(binary.is_file(), "missing sample rig binary: {binary:?}");
    }
    let scratch = broca_test_support::scratch_root()
        .join(format!("broca-voice-handoff-{}", std::process::id()));
    broca_test_support::remove_rig_dir(&scratch);
    let subject = BrocaSubject::new(&scratch).await;
    let live = subject.spawn(&scratch.join("root")).await.unwrap();
    let say = Say::default();
    let (_handle, serve) =
        subc_client_rs::serve_with_handle(&live.connection_file, manifest(), say.clone())
            .await
            .unwrap();
    let provider = tokio::spawn(serve);
    let owner = subject
        .session_route(&live, SESSION, &subject.owner_stamp())
        .await
        .unwrap();
    let sender = subject.plan_sender().unwrap();
    let voice = subject
        .session_route(&live, SESSION, &sender.stamp)
        .await
        .unwrap();
    let stream_route = subject
        .open(&owner.connection, &owner.session)
        .await
        .unwrap();
    let subscribe = request("session.subscribe", json!({"from":"live"}));
    let mut stream = owner
        .connection
        .subscribe(&stream_route, subscribe.clone())
        .await
        .unwrap();
    let attached = next(&mut stream).await;
    assert_eq!(decode(&attached)["event"]["type"], "display_reset");

    // A tool call gives the active run a step boundary at which to deliver the steer.
    let first = || {
        tool_turn(
            "voice-prepare-1",
            SCRIPTED_TOOL,
            json!({"action":"prepare"}),
            json!("ready"),
        )
    };
    let steered = || text_turn("I can answer the caller next.");
    subject
        .install_script(
            SESSION,
            Script {
                turns: vec![first(), steered()],
            },
        )
        .await
        .unwrap();
    subject.hold_next_model_step(SESSION).await.unwrap();
    let composition = json!({"providers":[
        {"provider":TOOL_MODULE,"tools":[{"name":SCRIPTED_TOOL,"capabilities":[]}]},
        {"provider":PROVIDER,"tools":[{"name":"say","capabilities":[]}]}
    ],"step_transform_items":[],"compaction":{"provider":PROVIDER}});
    let start = request(
        "session.send",
        json!({
            "prompt":"Get ready to answer questions.","send_id":"head-start",
            "model":{"provider":"mock","model":format!("{PLAN_MODEL_PREFIX}{SESSION}")},
            "session_class":"head",
        "context_limit":32000,
            "plan":{
                "tool_items":[
                    {"provider":TOOL_MODULE,"preset":"conformance","params":{}},
                    {"provider":PROVIDER,"preset":"voice","params":{}}
                ],
                "system_text_items":[],"step_transform_items":[],"render_tags":true,
            "compaction_item":{"provider":PROVIDER,"preset":"head","params":{}},
                "composition_digest":composition_digest(&composition).unwrap(),"composition":composition,
                "senders":[{"module": SENDER_MODULE,"idle_start":true}],
            }
        }),
    );
    let started = decode(&response(&owner, start.clone()).await);
    let run1 = &started["result"]["run_id"];
    assert!(run1.is_string(), "{started}");
    subject.await_model_step(SESSION).await.unwrap();
    let steer_request = handoff("voice-steer", STEER);
    let steer_reply = response(&voice, steer_request.clone()).await;
    let joined = decode(&steer_reply);
    assert_eq!(joined["result"]["state"], "pending", "{joined}");
    assert!(joined["result"]["submission_id"].is_string(), "{joined}");
    subject.release_model_step(SESSION).await.unwrap();
    let steer_frames = through_run(&mut stream, run1).await;

    let read = request("session.read", json!({"include_display_facts":true}));
    let steer_read = response(&owner, read.clone()).await;
    let steer_page = decode(&steer_read);
    let steer_entry = handoff_entry(&steer_page, STEER);
    assert_eq!(steer_entry["send_id"], "voice-steer");
    assert_eq!(steer_entry["delivered_as"]["mode"], "steer");
    assert_eq!(steer_entry["run"]["run_id"], *run1);
    assert_stream_message(&steer_frames, steer_entry);
    // Use the number actually sent to the model, not the message's ordinal or part key.
    let (tag, say_request_index) = {
        let captured = subject.shared.raw_bodies.lock().unwrap();
        let bodies = &captured[SESSION];
        (prompt_tag(bodies.last().unwrap(), STEER), bodies.len())
    };
    let arguments = json!({"text":SPOKEN,"reply_to":tag});
    subject
        .install_script(
            SESSION,
            Script {
                turns: vec![
                    first(),
                    steered(),
                    tool_turn("voice-say-1", "say", arguments.clone(), json!("sent")),
                    text_turn("I spoke the forecast."),
                ],
            },
        )
        .await
        .unwrap();
    broca_test_support::wait_until("head session idle before next hand-off", || async {
        let status = decode(&response(&owner, request("run.status", json!({}))).await);
        if status["result"]["state"] == "idle" {
            Ok(())
        } else {
            Err(status.to_string())
        }
    })
    .await;
    let idle_request = handoff("voice-idle", IDLE);
    let idle_reply = response(&voice, idle_request.clone()).await;
    let idle = decode(&idle_reply);
    let run2 = &idle["result"]["run_id"];
    assert_eq!(idle["result"]["state"], "active", "{idle}");
    assert!(run2.is_string() && run2 != run1, "{idle}");
    let idle_frames = through_run(&mut stream, run2).await;
    drop(stream);

    let idle_read = response(&owner, read.clone()).await;
    let idle_page = decode(&idle_read);
    let idle_entry = handoff_entry(&idle_page, IDLE);
    assert_eq!(idle_entry["send_id"], "voice-idle");
    assert_eq!(idle_entry["delivered_as"]["mode"], "idle_start");
    assert_eq!(idle_entry["run"]["run_id"], *run2);
    assert_stream_message(&idle_frames, idle_entry);
    let blocks = idle_page["result"]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|entry| entry["message"]["content"].as_array().unwrap());
    let say_blocks: Vec<_> = blocks
        .filter(|block| block["tool_call_id"] == "voice-say-1")
        .collect();
    assert_eq!(say_blocks.len(), 2, "{idle_page}");
    assert_eq!(say_blocks[0]["tool_name"], "say");
    assert_eq!(say_blocks[0]["input"], arguments);
    assert_eq!(say_blocks[1]["output"]["text"], "sent");

    let exchanges = std::mem::take(&mut *say.calls.lock().unwrap());
    assert_eq!(exchanges.len(), 1);
    let exchange = exchanges.into_iter().next().unwrap();
    let dispatched = decode(&exchange.request);
    assert_eq!(dispatched["arguments"], arguments);
    let say_model_request =
        subject.shared.raw_bodies.lock().unwrap()[SESSION][say_request_index].clone();
    assert_eq!(prompt_tag(&say_model_request, STEER), tag);
    let call_key = idle_page["result"]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|entry| entry["tool_calls"].as_array().into_iter().flatten())
        .find(|call| call["tool_call_id"] == "voice-say-1")
        .and_then(|call| call["call_key"].as_str())
        .unwrap_or_else(|| panic!("say has no durable call key: {idle_page}"));
    for event_type in ["tool_started", "tool_finished"] {
        assert!(
            idle_frames.iter().any(|bytes| {
                let body = decode(bytes);
                body["kind"] == "control"
                    && body["event"]["type"] == event_type
                    && body["event"]["call_key"] == call_key
            }),
            "missing {event_type} for {call_key}"
        );
    }
    // A tag read resolves against one dispatched call, so it can only run after say has a key.
    let tag_request = request("session.read", json!({"tag":tag,"call_key":call_key}));
    let sender_refusal = terminal(&voice, tag_request.clone())
        .await
        .expect_err("a named sender is not the tag-read owner");
    assert_eq!(decode(&sender_refusal)["code"], "scope_owner_mismatch");
    let tag_reply = terminal(&owner, tag_request.clone()).await;
    let tag_filename = match &tag_reply {
        Ok(reply) => {
            let body = decode(reply);
            assert_eq!(body["result"]["entry"], *steer_entry, "{body}");
            assert_eq!(body["result"]["part"], 0);
            "tag-read.reply.json"
        }
        Err(error) => {
            let body = decode(error);
            assert_eq!(body["code"], "unknown_tag", "{body}");
            "tag-read.refusal.json"
        }
    };
    let mut samples = BTreeMap::from([
        ("head-start.request.json".into(), start),
        ("subscribe.request.json".into(), subscribe),
        ("subscribe.attach.json".into(), attached),
        ("steer.send.request.json".into(), steer_request),
        ("steer.send.reply.json".into(), steer_reply),
        ("idle.send.request.json".into(), idle_request),
        ("idle.send.reply.json".into(), idle_reply),
        ("read.request.json".into(), read),
        ("steer.read.reply.json".into(), steer_read),
        ("idle.read.reply.json".into(), idle_read),
        ("tag-read.request.json".into(), tag_request),
        (tag_filename.into(), tag_reply.unwrap_or_else(|error| error)),
        ("tag-read.sender-refusal.json".into(), sender_refusal),
        ("say.model.request.json".into(), say_model_request),
        ("say.tool.request.json".into(), exchange.request),
        ("say.tool.reply.json".into(), exchange.reply),
    ]);
    for (phase, frames) in [("steer", steer_frames), ("idle", idle_frames)] {
        for (index, bytes) in frames.into_iter().enumerate() {
            let body = decode(&bytes);
            let lane = body["kind"].as_str().unwrap();
            let event = body["event"]["type"]
                .as_str()
                .or_else(|| body["unit"]["type"].as_str())
                .unwrap_or("snapshot");
            samples.insert(
                format!("{phase}.subscribe.{index:02}.{lane}.{event}.json"),
                bytes,
            );
        }
    }
    write_samples(&output, samples);
    drop(live);
    provider.abort();
    broca_test_support::remove_rig_dir(&scratch);
}

fn write_samples(output: &Path, samples: BTreeMap<String, Vec<u8>>) {
    std::fs::create_dir_all(output).unwrap();
    assert!(
        std::fs::read_dir(output).unwrap().next().is_none(),
        "sample directory must be empty: {output:?}"
    );
    let mut sums = String::new();
    for (name, bytes) in samples {
        std::fs::write(output.join(&name), &bytes).unwrap();
        use std::fmt::Write;
        writeln!(sums, "{:x}  {name}", Sha256::digest(&bytes)).unwrap();
    }
    std::fs::write(output.join("SHA256SUMS"), sums).unwrap();
}
