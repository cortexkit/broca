//! Opt-in real Magic Context compaction. Every module and database is private to this rig.
//! Run with test-clock, BROCA_MC_E2E_BIN and BROCA_MC_E2E_DB; no external model is called.
use super::*;
use broca_core::WalStore;
use broca_wal::{CompactionMessageKind, WalRecord};
use sha2::{Digest, Sha256};

const SESSION: &str = "mc-e2e";
const BIN_SHA: &str = "8eeeaae339d2db3d1942f5b6b151ba9735a9b202cb59a598a99f41104ff9bfdc";
const DB_SHA: &str = "c84e26b0d4e394dcc2288dc9290539633e890c0c564bd7dba7309d5e5edd2060";

#[derive(Clone)]
pub(super) struct McConfig {
    binary: PathBuf,
    root: PathBuf,
}

impl McConfig {
    fn prepare(root: &Path, binary: &Path, database: &Path) -> Self {
        let root = root.join("mc");
        let data = root.join("data/cortexkit/magic-context");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::create_dir_all(root.join("config/cortexkit")).unwrap();
        std::fs::create_dir_all(root.join("home")).unwrap();
        for (source, destination, digest) in [
            (binary, root.join("ckdev-mc"), BIN_SHA),
            (database, data.join("context.db"), DB_SHA),
        ] {
            let bytes =
                std::fs::read(source).unwrap_or_else(|e| panic!("{}: {e}", source.display()));
            assert_eq!(
                format!("{:x}", Sha256::digest(&bytes)),
                digest,
                "{}",
                source.display()
            );
            std::fs::copy(source, &destination).unwrap();
            assert_eq!(
                format!("{:x}", Sha256::digest(std::fs::read(&destination).unwrap())),
                digest
            );
        }
        std::fs::write(
            root.join("config/cortexkit/magic-context.jsonc"),
            serde_json::to_vec_pretty(
                &json!({"historian":{"broca":{"model":"scripted/historian"}},
                "execute_threshold_percentage":20}),
            )
            .unwrap(),
        )
        .unwrap();
        Self {
            binary: root.join("ckdev-mc"),
            root,
        }
    }

    /// The storage override takes precedence over XDG paths, so pin both to private state.
    pub(super) fn module(&self) -> Value {
        json!({"program":self.binary,"args":[],"enabled":true,"env":{
            "HOME":self.root.join("home"),
            "XDG_CONFIG_HOME":self.root.join("config"),
            "XDG_DATA_HOME":self.root.join("data"),
            "MAGIC_CONTEXT_STORAGE_DIR":self.root.join("data/cortexkit/magic-context"),
            "MAGIC_CONTEXT_TEST_DATA_DIR":self.root.join("data/cortexkit/magic-context"),
        }})
    }

    fn assert_private_databases(&self, live: &Live) -> String {
        let output = std::process::Command::new("lsof")
            .arg("-t")
            .arg(&self.binary)
            .output()
            .expect("lsof must identify the process running the private MC executable");
        let pids: Vec<i32> = String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        assert_eq!(
            pids.len(),
            1,
            "expected one process running {}: {pids:?}; daemon children={:?}",
            self.binary.display(),
            live.children()
        );
        let pid = pids[0];
        let output = std::process::Command::new("lsof")
            .args(["-Fn", "-p", &pid.to_string()])
            .output()
            .expect("lsof is required to verify MC isolation");
        assert!(
            output.status.success(),
            "lsof: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8(output.stdout).unwrap();
        let databases: Vec<_> = text
            .lines()
            .filter_map(|s| s.strip_prefix('n'))
            .filter(|s| s.ends_with(".db") || s.ends_with(".db-wal") || s.ends_with(".db-shm"))
            .collect();
        assert!(
            databases.iter().any(|s| s.ends_with("context.db")),
            "MC has no context.db open: {text}"
        );
        let root = self.root.canonicalize().unwrap();
        for path in &databases {
            assert!(
                Path::new(path).starts_with(&root),
                "MC opened a database outside scratch: {path}"
            );
        }
        format!("MC pid={pid}; open databases={databases:?}")
    }
}

/// Rust port of the supplied model-free responder; reference pairs pin its exact output.
fn historian_response(prompt: &str) -> Option<String> {
    fn digits(s: &str) -> Option<u64> {
        (!s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
            .then(|| s.parse().ok())
            .flatten()
    }
    fn block(line: &str) -> bool {
        let Some((range, role)) = line.strip_prefix('[').and_then(|s| s.split_once("] ")) else {
            return false;
        };
        let valid = match range.split_once('-') {
            Some((a, b)) => digits(a).is_some() && digits(b).is_some(),
            None => digits(range).is_some(),
        };
        let letters = role.bytes().take_while(u8::is_ascii_uppercase).count();
        valid && letters > 0 && role.as_bytes().get(letters) == Some(&b':')
    }
    let (_, rest) = prompt.split_once("<new_messages>\n\nMessages ")?;
    let (header, _) = rest.split_once(":\n\n")?;
    let (start, end) = header.split_once('-')?;
    let (start, end) = (digits(start)?, digits(end)?);
    if end < start {
        return None;
    }
    let close = rest.rfind("\n\n</new_messages>")?;
    let body = rest.get(header.len() + 3..close).unwrap_or_default();
    let blocks = body.lines().filter(|s| block(s)).count();
    let next = end.checked_add(1)?;
    Some(format!(concat!("<output>\n<compartments>\n",
        "<compartment start=\"{}\" end=\"{}\" title=\"Scripted fold {}-{}\" episode_type=\"feature\" importance=\"50\">\n",
        "<p1>Scripted summary of raw messages {}-{}: {} transcript blocks.</p1>\n",
        "<p2>Scripted summary of raw messages {}-{}.</p2>\n<p3>Messages {}-{}.</p3>\n<p4 />\n",
        "</compartment>\n</compartments>\n<meta><messages_processed>{}-{}</messages_processed>",
        "<unprocessed_from>{}</unprocessed_from></meta>\n</output>\n"),
        start,end,start,end,start,end,blocks,start,end,start,end,start,end,next))
}

pub(super) async fn serve_historian(
    mut stream: tokio::net::TcpStream,
    request: &Value,
) -> std::io::Result<()> {
    let prompt = request["messages"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|m| m["role"] == "user")
        .filter_map(|m| m["content"].as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let answer =
        historian_response(&prompt).expect("MC historian prompt must match the supplied grammar");
    eprintln!(
        "MC scripted historian: prompt={} response={} bytes",
        prompt.len(),
        answer.len()
    );
    let chunk = json!({"id":"scripted-historian","object":"chat.completion.chunk","model":"historian",
        "choices":[{"index":0,"delta":{"role":"assistant","content":answer},"finish_reason":null}]});
    let end = json!({"id":"scripted-historian","object":"chat.completion.chunk","model":"historian",
        "choices":[{"index":0,"delta":{},"finish_reason":"stop"}]});
    let body = format!("data: {chunk}\n\ndata: {end}\n\ndata: [DONE]\n\n");
    stream.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len()).as_bytes()).await?;
    stream.shutdown().await
}

#[test]
fn historian_matches_mc_reference_pairs() {
    let root =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/scripted-historian/pairs");
    for i in 1..=3 {
        let prompt = std::fs::read_to_string(root.join(format!("pair-{i}.prompt.txt"))).unwrap();
        let expected =
            std::fs::read_to_string(root.join(format!("pair-{i}.response.xml"))).unwrap();
        assert_eq!(
            historian_response(&prompt).as_deref(),
            Some(expected.as_str()),
            "pair {i}"
        );
    }
    assert!(historian_response("ordinary conversation").is_none());
}

fn note(root: &Path, text: impl AsRef<str>) {
    use std::io::Write;
    let text = text.as_ref();
    println!("{text}");
    writeln!(
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(root.join("report.txt"))
            .unwrap(),
        "{text}"
    )
    .unwrap();
}

struct Evidence {
    root: PathBuf,
    events: Option<Arc<Mutex<Vec<async_nats::Message>>>>,
    identity: broca_protocol::BindIdentity,
}
impl Drop for Evidence {
    fn drop(&mut self) {
        if let Some(events) = &self.events {
            let _ = std::fs::write(
                self.root.join("bus-events.txt"),
                format!("{:#?}", events.lock().unwrap()),
            );
        }
        let records = broca_core::FileWalStore::new(self.root.join("broca-state/wal"))
            .read_records(&self.identity);
        if let Ok(records) = records {
            let values: Vec<_> = records
                .into_iter()
                .map(|f| serde_json::to_value(f.record).unwrap())
                .collect();
            let _ = std::fs::write(
                self.root.join("wal-records.json"),
                serde_json::to_vec_pretty(&values).unwrap(),
            );
            let kinds: Vec<_> = values
                .iter()
                .map(|r| r["type"].as_str().unwrap_or("?"))
                .collect();
            note(&self.root, format!("WAL record kinds in order: {kinds:?}"));
        }
        note(
            &self.root,
            format!(
                "Outcome: {}; module logs and private databases retained in {}",
                if std::thread::panicking() {
                    "FAILED (stop; do not work around provider refusals)"
                } else {
                    "PASSED"
                },
                self.root.display()
            ),
        );
    }
}

fn requests(subject: &BrocaSubject) -> Vec<Vec<u8>> {
    subject
        .shared
        .raw_bodies
        .lock()
        .unwrap()
        .get(SESSION)
        .cloned()
        .unwrap_or_default()
}

fn fields(subject: &BrocaSubject, first: bool, index: usize) -> Value {
    let mut fields = Value::Object(subject.send_fields(SESSION, first));
    fields["prompt"] = json!(format!(
        "Turn {index}: {}",
        "Private deterministic conversation evidence. ".repeat(100)
    ));
    fields["send_id"] = json!(format!("mc-turn-{index}"));
    fields["stop_when"] = json!({"max_steps":1});
    if first {
        let composition = json!({"providers":[{"provider":"magic-context","tools":[]}],
            "compaction":{"provider":"magic-context"},"step_transform_items":[]});
        fields["plan"] = json!({"tool_items":[],"system_text_items":[],"step_transform_items":[],
            "compaction_item":{"provider":"magic-context","preset":"head","params":{}},
            "composition_digest":cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),
            "composition":composition});
    }
    fields
}

async fn send(route: &BrocaRoute, fields: Value, root: &Path) {
    let reply = route
        .request("session.send", fields)
        .await
        .expect("session.send transport");
    note(root, format!("session.send reply: {reply:?}"));
    assert!(
        matches!(reply, Reply::Response(_)),
        "provider refused; see report and MC logs: {reply:?}"
    );
}

async fn finished(subject: &BrocaSubject, live: &Live, expected: usize) {
    broca_test_support::wait_until("MC E2E conversation run terminal", || async {
        let records = subject.records(live, SESSION);
        if records
            .iter()
            .filter(|r| matches!(r, WalRecord::RunFinished { .. }))
            .count()
            >= expected
        {
            Ok(())
        } else {
            Err(format!(
                "waiting for terminal {expected}; records={records:?}"
            ))
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_mc_async_compaction_end_to_end() {
    let Some(binary) = std::env::var_os("BROCA_MC_E2E_BIN").filter(|v| !v.is_empty()) else {
        println!("SKIP real_mc_async_compaction_end_to_end: BROCA_MC_E2E_BIN is unset; real MC was NOT exercised");
        return;
    };
    // Once opted in, every prerequisite is mandatory, including under BROCA_REQUIRE_SUBC.
    let database = std::env::var_os("BROCA_MC_E2E_DB")
        .expect("BROCA_MC_E2E_DB must name the staged v102 fixture");
    for (name, env) in [
        ("ck-subc", "SUBC_CORE_BIN"),
        ("fake-aft-stub", "FAKE_AFT_STUB_BIN"),
    ] {
        assert!(sibling_bin(name, env).exists(), "MC E2E requires {env}");
    }
    let root = std::env::var_os("BROCA_MC_E2E_ARTIFACT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../.cortexkit/internal-docs/investigations")
                .join(format!(
                    "mc-e2e-{}-{}",
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
                ))
        });
    std::fs::create_dir_all(&root).unwrap();
    let root = root.canonicalize().unwrap();
    assert!(
        !root.join("broca-state").exists(),
        "use a fresh artifact directory for each run"
    );
    let mc = McConfig::prepare(&root, Path::new(&binary), Path::new(&database));
    let mut subject = BrocaSubject::new(&root).await;
    subject.mc = Some(mc.clone());
    subject
        .shared
        .mc_historian
        .store(true, std::sync::atomic::Ordering::SeqCst);
    subject
        .shared
        .plan_sessions
        .lock()
        .unwrap()
        .insert(SESSION.into());
    let mut evidence = Evidence {
        events: None,
        root: root.clone(),
        identity: subject.bind_identity(SESSION),
    };
    note(&root, format!("Authenticated MC sha256={BIN_SHA}; fixture sha256={DB_SHA}; no billed models; artifacts={}", root.display()));
    subject
        .install_script(
            SESSION,
            Script {
                turns: vec![
                    cortexkit_role_llm_runner_conformance::ScriptedTurn::new(vec![
                        ScriptedPart::Text("Deterministic assistant evidence. ".repeat(80))
                    ]);
                    128
                ],
            },
        )
        .await
        .unwrap();
    let controls = root.join("broca-state/test-compaction").join(SESSION);
    std::fs::create_dir_all(&controls).unwrap();
    std::fs::write(
        controls.join("config"),
        serde_json::to_vec(&json!({
        "context_window":32000,"output_limit":4096,"call_timeout_ms":30000}))
        .unwrap(),
    )
    .unwrap();
    // `BusRig::new` has already reported the skip, or failed under CK_BUS_TESTKIT_REQUIRE.
    let Some(bus) = bus::BusRig::new(&root).await else {
        return;
    };
    let bus = Arc::new(bus);
    provision_consumers(&bus).await;
    let events = observe_events(&bus).await;
    evidence.events = Some(events.messages.clone());
    subject.bus = Some(bus.clone());
    let receipt_gate = root.join("broca-state/test-compaction-bus-gate");
    std::fs::write(&receipt_gate, "ViewReceiptRecorded").unwrap();
    let mut live = subject
        .start(&root)
        .await
        .expect("start private supervised stack");
    // Route readiness also waits for the module to finish opening its databases.
    let connection = SubcConnection::connect(&live.connection_file)
        .await
        .unwrap();
    let mc_route = connection
        .route_open_within(
            "magic-context",
            &subject.bind_identity(SESSION),
            broca_subc::RouteOpenRetry::within(Duration::from_secs(40), Duration::from_millis(100)),
        )
        .await
        .expect("MC readiness");
    let description = connection
        .route_request(
            &mc_route,
            serde_json::to_vec(&json!({"method":"role.describe","params":{}})).unwrap(),
        )
        .await
        .unwrap();
    note(&root, format!("MC role.describe: {}", description));
    broca_core::compaction_provider::describe::check_describe(&description)
        .expect("real MC must serve compaction-provider 0.3");
    connection.route_close(&mc_route).await.unwrap();
    note(&root, mc.assert_private_databases(&live));
    let route = subject
        .session_route(&live, SESSION, &subject.owner_stamp())
        .await
        .unwrap();
    let sent = std::sync::atomic::AtomicUsize::new(0);
    broca_test_support::wait_until("MC view_ready's durable receipt", || async {
        if root
            .join("broca-state/test-compaction-bus-reached")
            .exists()
        {
            return Ok(());
        }
        let index = sent.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        assert!(
            index < 40,
            "MC produced no view_ready after 40 turns; STOP and inspect MC logs"
        );
        send(&route, fields(&subject, index == 0, index), &root).await;
        finished(&subject, &live, index + 1).await;
        note(
            &root,
            format!(
                "turn={index} model_request_bytes={:?}",
                requests(&subject).last().map(Vec::len)
            ),
        );
        if root
            .join("broca-state/test-compaction-bus-reached")
            .exists()
        {
            Ok(())
        } else {
            Err(format!("turn {index} complete, no view_ready receipt yet"))
        }
    })
    .await;
    let mut sent = sent.into_inner();
    assert_eq!(
        std::fs::read_to_string(root.join("broca-state/test-compaction-bus-reached"))
            .ok()
            .as_deref(),
        Some("ViewReceiptRecorded"),
        "MC never published view_ready; STOP and inspect MC logs"
    );
    let store = broca_store::SqliteStore::open(live.state_root.join("run-index.db")).unwrap();
    let receipts = store.compaction_receipts("", 256).unwrap();
    assert_eq!(
        receipts.len(),
        1,
        "view_ready must have a durable receipt before ACK"
    );
    let ready = receipts[0].ready.clone();
    note(
        &root,
        format!("Durable receipt before ACK: {:?}", receipts[0]),
    );
    assert!(
        !subject
            .records(&live, SESSION)
            .iter()
            .any(|r| matches!(r, WalRecord::CompactionViewApplied { .. })),
        "receipt alone must not apply a view before inbox announcement"
    );
    drop(store);
    std::fs::remove_file(receipt_gate).unwrap();
    wait_for_ack(&bus).await;
    note(&root, "Broker m_broca durable confirms view_ready ACK");
    broca_test_support::wait_until("real MC view applied at the idle boundary", || async {
        let records = subject.records(&live, SESSION);
        if records
            .iter()
            .any(|r| matches!(r, WalRecord::CompactionViewApplied { .. }))
        {
            Ok(())
        } else {
            Err(format!("records={records:?}"))
        }
    })
    .await;
    let before = requests(&subject).last().unwrap().len();
    send(&route, fields(&subject, false, sent), &root).await;
    sent += 1;
    finished(&subject, &live, sent).await;
    let records = subject.records(&live, SESSION);
    let setups: Vec<_> = records
        .iter()
        .filter(|r| {
            matches!(
                r,
                WalRecord::CompactionMessage {
                    kind: CompactionMessageKind::Setup,
                    ..
                }
            )
        })
        .collect();
    assert_eq!(setups.len(), 1, "Setup must be recorded once");
    assert!(
        matches!(setups[0], WalRecord::CompactionMessage { provider, ceiling:Some(_), .. } if provider == "magic-context")
    );
    let view_at = records
        .iter()
        .position(|r| matches!(r, WalRecord::CompactionViewApplied { .. }))
        .expect("MC never produced an applicable async view; STOP and inspect MC logs");
    let view = records[view_at].clone();
    assert!(
        matches!(&view, WalRecord::CompactionViewApplied { provider, view_version, .. }
        if provider == "magic-context" && *view_version == ready.view_version),
        "applied view does not match the real bus announcement"
    );
    let WalRecord::CompactionViewApplied { view: raw, .. } = &view else {
        unreachable!();
    };
    let decoded: broca_core::compaction_provider::view::CompactionView =
        serde_json::from_str(raw.as_json_text()).unwrap();
    assert_eq!(
        broca_core::compaction_provider::notice::ViewReady::for_view(&decoded),
        ready
    );
    assert_bus_notices(&root, &bus, &events, &records, &ready).await;
    let after = requests(&subject).last().unwrap().len();
    note(&root, format!("Setup recorded once; view at WAL index {view_at}: {view:?}; request bytes before={before}, after={after}"));
    assert!(
        after < before,
        "view must shrink the next real model request: {before} -> {after}"
    );
    assert_rendered_view(requests(&subject).last().unwrap(), &decoded);
    assert!(
        records[view_at + 1..]
            .iter()
            .any(|r| matches!(r, WalRecord::StepStarted { .. })),
        "view must precede its consuming step"
    );

    subject.hold_next_model_step(SESSION).await.unwrap();
    let resume_fields = fields(&subject, false, sent);
    send(&route, resume_fields.clone(), &root).await;
    subject.await_model_step(SESSION).await.unwrap();
    let before_kill = requests(&subject).last().unwrap().clone();
    std::fs::write(root.join("request-before-kill.json"), &before_kill).unwrap();
    assert!(
        live.kill_all(),
        "real SIGKILL of broca is required; no simulated kill"
    );
    subject.shared.released.notify_waiters();
    drop(route);
    drop(live);
    note(
        &root,
        "SIGKILL delivered to broca; restarting private stack on the same WAL",
    );
    subject.hold_next_model_step(SESSION).await.unwrap();
    live = subject.start(&root).await.unwrap();
    let route = subject
        .session_route(&live, SESSION, &subject.owner_stamp())
        .await
        .unwrap();
    send(&route, resume_fields, &root).await;
    subject.await_model_step(SESSION).await.unwrap();
    let resumed = requests(&subject).last().unwrap().clone();
    std::fs::write(root.join("request-after-restart.json"), &resumed).unwrap();
    assert_eq!(
        before_kill, resumed,
        "resume changed rendered request bytes"
    );
    assert!(
        subject.records(&live, SESSION).contains(&view),
        "resume lost the applied view"
    );
    note(
        &root,
        format!(
            "Resume request byte-identical: {} bytes; applied view intact",
            resumed.len()
        ),
    );
    subject.release_model_step(SESSION).await.unwrap();
    finished(&subject, &live, sent + 1).await;
    note(&root, mc.assert_private_databases(&live));
    note(&root, "Ceiling wait/refusal not forced: this fixture's successful proactive compaction keeps usage below ceiling");
}

async fn provision_consumers(bus: &bus::BusRig) {
    let stream = async_nats::jetstream::new(bus.connection.client())
        .get_stream(&bus.names.streams().event)
        .await
        .unwrap();
    for spec in cortexkit_bus_naming::event_consumer_specs(&bus.names).unwrap() {
        if spec.consumer.durable == "m_broca" {
            continue;
        }
        stream
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                durable_name: Some(spec.consumer.durable),
                filter_subjects: spec.consumer.filter_subjects,
                ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                ack_wait: spec.ack_wait,
                max_ack_pending: spec.max_ack_pending,
                ..Default::default()
            })
            .await
            .unwrap();
    }
}

struct Events {
    messages: Arc<Mutex<Vec<async_nats::Message>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Events {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn observe_events(bus: &bus::BusRig) -> Events {
    use futures::StreamExt;
    let mut subscriber = bus
        .connection
        .client()
        .subscribe(bus.names.event_binding())
        .await
        .unwrap();
    bus.connection.client().flush().await.unwrap();
    let messages = Arc::new(Mutex::new(Vec::new()));
    let seen = messages.clone();
    let task = tokio::spawn(async move {
        while let Some(message) = subscriber.next().await {
            seen.lock().unwrap().push(message);
        }
    });
    Events { messages, task }
}
async fn wait_for_ack(bus: &bus::BusRig) {
    broca_test_support::wait_until("broker ACK for real MC view_ready", || async {
        let consumer = async_nats::jetstream::new(bus.connection.client())
            .get_consumer_from_stream::<async_nats::jetstream::consumer::pull::Config, _, _>(
                "m_broca",
                &bus.names.streams().event,
            )
            .await
            .map_err(|e| e.to_string())?;
        let info = consumer.cached_info();
        if info.ack_floor.stream_sequence > 0 && info.num_ack_pending == 0 {
            Ok(())
        } else {
            Err(format!("{info:?}"))
        }
    })
    .await;
}
async fn assert_bus_notices(
    root: &Path,
    bus: &bus::BusRig,
    events: &Events,
    records: &[WalRecord],
    ready: &broca_core::compaction_provider::notice::ViewReady,
) {
    use broca_core::compaction_provider::notice::{event_subject, VIEW_READY_EVENT};
    fn matches(
        message: &async_nats::Message,
        headers: &std::collections::BTreeMap<String, String>,
    ) -> bool {
        message.headers.as_ref().is_some_and(|actual| {
            headers.iter().all(|(key, value)| {
                actual
                    .get(key.as_str())
                    .is_some_and(|v| v.as_str() == value)
            })
        })
    }
    let notices: Vec<_> = records
        .iter()
        .filter_map(|r| match r {
            WalRecord::CompactionNotice { seq, headers } => Some((*seq, headers.clone())),
            _ => None,
        })
        .collect();
    assert!(notices.len() >= 2, "need multiple durable advance notices");
    assert_eq!(notices[0].0, 1);
    assert!(
        notices.windows(2).all(|w| w[1].0 == w[0].0 + 1),
        "advance sequences must rise"
    );
    broca_test_support::wait_until(
        "real MC view_ready and WAL-backed advance publications",
        || async {
            let seen = events.messages.lock().unwrap();
            let view = seen.iter().any(|m| {
                m.subject.as_str()
                    == event_subject(bus.names.account(), "magic-context", VIEW_READY_EVENT)
                    && matches(m, &ready.to_headers())
            });
            let all = notices
                .iter()
                .all(|(_, h)| seen.iter().any(|m| matches(m, h)));
            if view && all {
                Ok(())
            } else {
                Err(format!(
                    "view_ready={view}; all_notices={all}; seen={seen:?}"
                ))
            }
        },
    )
    .await;
    let seen = events.messages.lock().unwrap();
    let sequences: Vec<_> = seen
        .iter()
        .filter_map(|m| {
            notices
                .iter()
                .find(|(_, h)| matches(m, h))
                .map(|(seq, _)| *seq)
        })
        .collect();
    assert!(
        sequences.windows(2).all(|w| w[1] > w[0]),
        "bus advance notices out of order: {sequences:?}"
    );
    std::fs::write(root.join("bus-events.txt"), format!("{seen:#?}")).unwrap();
    note(root, format!("Real MC view_ready observed; durable receipts and ACK; published advance sequences={sequences:?}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replay_preserved_mc_read() {
    use broca_core::compaction_provider::notice::{event_subject, AdvanceNotice, ADVANCED_EVENT};
    use cortexkit_bus_trait::Stream;
    let Some(root) = std::env::var_os("BROCA_MC_E2E_REPLAY_ROOT") else {
        println!("SKIP replay_preserved_mc_read: BROCA_MC_E2E_REPLAY_ROOT is unset");
        return;
    };
    let root = PathBuf::from(root).canonicalize().unwrap();
    let mut subject = BrocaSubject::new(&root).await;
    subject.mc = Some(McConfig {
        binary: root.join("mc/ckdev-mc"),
        root: root.join("mc"),
    });
    subject
        .shared
        .mc_historian
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let replay_bus = root.join(format!("read-replay-bus-{}", std::process::id()));
    // `BusRig::new` has already reported the skip, or failed under CK_BUS_TESTKIT_REQUIRE.
    let Some(bus) = bus::BusRig::new(&replay_bus).await else {
        return;
    };
    let bus = Arc::new(bus);
    provision_consumers(&bus).await;
    subject.bus = Some(bus.clone());
    let live = subject.start(&root).await.unwrap();
    let route = subject
        .session_route(&live, SESSION, &subject.owner_stamp())
        .await
        .unwrap();
    let records = subject.records(&live, SESSION);
    let headers = records
        .iter()
        .rev()
        .find_map(|r| match r {
            WalRecord::CompactionNotice { headers, .. } => Some(headers.clone()),
            _ => None,
        })
        .unwrap();
    let notice = AdvanceNotice::from_headers(&headers).unwrap();
    let publication = bus
        .connection
        .stream(bus.names.streams().event.clone())
        .publish(
            &event_subject(bus.names.account(), "broca", ADVANCED_EVENT),
            &notice.message_id(),
            notice.digest().parse().unwrap(),
            headers,
        )
        .await
        .unwrap();
    assert!(!publication.duplicate());
    sleep(Duration::from_secs(2)).await;
    let request = serde_json::to_vec(
        &json!({"method":"session.read","params":{"from_ordinal":0,"max_bytes":1048576}}),
    )
    .unwrap();
    std::fs::write(root.join("read-replay.request.json"), &request).unwrap();
    let reply = route
        .connection
        .route_request_raw(&route.route, request.clone())
        .await
        .unwrap();
    std::fs::write(root.join("read-replay.reply.json"), &reply).unwrap();
    let value: Value = serde_json::from_slice(&reply).unwrap();
    println!(
        "Preserved session raw read: {} bytes; lineage_id={:?}; keys={:?}",
        reply.len(),
        value["result"].get("lineage_id"),
        value["result"]
            .as_object()
            .map(|m| m.keys().collect::<Vec<_>>())
    );
    assert!(value["result"]["lineage_id"].is_string());
    let other = broca_protocol::BindIdentity::new(&subject.project_root, "runner", SESSION);
    let other_route = route
        .connection
        .route_open_management("broca", &other)
        .await
        .unwrap();
    let other_reply = route
        .connection
        .route_request_raw(&other_route, request)
        .await
        .unwrap();
    std::fs::write(
        root.join("read-replay-runner-harness.reply.json"),
        &other_reply,
    )
    .unwrap();
    println!(
        "Same params under harness=runner: {}",
        String::from_utf8_lossy(&other_reply)
    );
    let other_value: Value = serde_json::from_slice(&other_reply).unwrap();
    assert_eq!(other_value["result"]["messages"], json!([]));
    assert!(other_value["result"].get("lineage_id").is_none());
}

fn assert_rendered_view(body: &[u8], view: &broca_core::compaction_provider::view::CompactionView) {
    use broca_core::compaction_provider::view::ViewRange;
    let request: Value = serde_json::from_slice(body).unwrap();
    let wire_text: Vec<&str> = request["messages"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|m| {
            if let Some(text) = m["content"].as_str() {
                vec![text]
            } else {
                m["content"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|p| p["text"].as_str())
                    .collect()
            }
        })
        .collect();
    let mut replacements = 0;
    for range in &view.ranges {
        if let ViewRange::Replace { bytes, .. } = range {
            let messages: Vec<broca_protocol::Message> = serde_json::from_str(bytes).unwrap();
            for message in messages {
                for part in message.content {
                    if let broca_protocol::ContentBlock::Text { text } = part {
                        if text.is_empty() {
                            continue;
                        }
                        replacements += 1;
                        assert!(
                            wire_text.iter().any(|wire| wire.contains(&text)),
                            "scripted model did not receive text from MC's applied view: {text:?}"
                        );
                    }
                }
            }
        }
    }
    assert!(
        replacements > 0,
        "real MC view must replace transcript text, not merely keep it"
    );
}
