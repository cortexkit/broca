//! Opt-in real Magic Context compaction. Every module and database is private to this rig.
//! Run with test-clock, BROCA_MC_E2E_BIN, BROCA_MC_E2E_BIN_SHA256, BROCA_MC_E2E_DB and
//! BROCA_MC_E2E_DB_SHA256; no external model is called. MC stages a new build and a matching
//! store fixture per change, so the operator states the sha256 MC published for each, and the
//! rig refuses any other bytes.
use super::*;
use broca_core::WalStore;
use broca_wal::{CompactionMessageKind, WalRecord};
use cortexkit_role_llm_runner_conformance::{ScriptedToolCall, ScriptedTurn};
use sha2::{Digest, Sha256};

const SESSION: &str = "mc-e2e";

#[derive(Clone)]
pub(super) struct McConfig {
    binary: PathBuf,
    root: PathBuf,
}

impl McConfig {
    fn prepare(
        root: &Path,
        binary: &Path,
        binary_sha: &str,
        database: &Path,
        database_sha: &str,
    ) -> Self {
        let root = root.join("mc");
        let data = root.join("data/cortexkit/magic-context");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::create_dir_all(root.join("config/cortexkit")).unwrap();
        std::fs::create_dir_all(root.join("home")).unwrap();
        for (source, destination, digest) in [
            (binary, root.join("ckdev-mc"), binary_sha),
            (database, data.join("context.db"), database_sha),
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
                // Magic Context reads this from its user-tier file only. The session runs a
                // worker compaction preset, which writes no summaries, so no historian model
                // is configured. The window the threshold applies to is the one broca reports
                // for the session: `context_window` in the rig's test-compaction `config` file.
                &json!({"execute_threshold_percentage":50}),
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

#[test]
fn compaction_log_reads_fleet_logger_lines() {
    let root =
        broca_test_support::scratch_root().join(format!("mc-log-parse-{}", std::process::id()));
    let logs = root.join("data-home/cortexkit/broca/logs");
    std::fs::create_dir_all(&logs).unwrap();
    // Lines as broca wrote them in a real run against Magic Context.
    std::fs::write(logs.join("broca.2026-10-22.log"), concat!(
        "2026-10-22T15:47:27.640Z INFO  broca.compaction: compaction consumer attached account=box_compaction_test durable=m_broca\n",
        "2026-10-22T15:47:32.497Z INFO  broca.compaction: compaction view_ready handled session_ref=d22579b49da13fb908f3c244f73c6776 view_version=1 outcome=receipted\n",
        "2026-10-22T15:47:32.545Z INFO  broca.compaction: compaction view_ready handled session_ref=d22579b49da13fb908f3c244f73c6776 view_version=2 outcome=receipted\n",
        "2026-10-22T15:47:32.600Z INFO  broca.compaction: compaction view_ready handled session_ref=00000000000000000000000000000000 view_version=9 outcome=receipted\n",
        "2026-10-22T15:47:32.649Z INFO  broca.compaction: compaction view decided at the idle boundary session_ref=d22579b49da13fb908f3c244f73c6776 view_version=2 choice=applied reason=\"\"\n",
    )).unwrap();
    let log = compaction_log(&root, "d22579b49da13fb908f3c244f73c6776");
    let summary: Vec<_> = log
        .iter()
        .map(|f| {
            (
                f["message"].as_str(),
                f["view_version"].as_str(),
                f.get("outcome").or(f.get("choice")).unwrap().as_str(),
            )
        })
        .collect();
    assert_eq!(
        summary,
        vec![
            ("compaction view_ready handled", "1", "receipted"),
            ("compaction view_ready handled", "2", "receipted"),
            (
                "compaction view decided at the idle boundary",
                "2",
                "applied"
            ),
        ]
    );
    assert_eq!(log[2]["reason"], "");
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
    fields["stop_when"] = json!({"max_steps":2});
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

/// Holds the next send until Magic Context can have read everything the previous run wrote:
/// that run's last advance notice is durable in the WAL and the rig's bus subscriber has seen
/// it published. Back-to-back turns otherwise outrun MC, which publishes its view while later
/// runs are already going. No `view_ready` is awaited: MC publishes one only when it decides
/// to compact.
async fn previous_notice_published(subject: &BrocaSubject, live: &Live, events: &Events) {
    broca_test_support::wait_until(
        "the previous run's last advance notice on the bus",
        || async {
            let records = subject.records(live, SESSION);
            let run_start = records
                .iter()
                .rposition(|r| matches!(r, WalRecord::RunStarted { .. }))
                .ok_or("no run has started")?;
            let Some(headers) = records[run_start..].iter().rev().find_map(|r| match r {
                WalRecord::CompactionNotice { headers, .. } => Some(headers),
                _ => None,
            }) else {
                return Err(format!(
                    "the last run recorded no advance notice; records={records:?}"
                ));
            };
            if events
                .messages
                .lock()
                .unwrap()
                .iter()
                .any(|m| published_with(m, headers))
            {
                Ok(())
            } else {
                Err(format!("advance notice not yet published: {headers:?}"))
            }
        },
    )
    .await;
}

/// Whether a published bus message carries every one of `headers`.
fn published_with(
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

/// The scope the conversation runs under. Its worker tool preset is confined by the scope on
/// its route, and broca refuses a worker preset on an unscoped route (`scope_required`).
const SCOPE_REF: &str = "mc-e2e-scope";
const SCOPE_EPOCH: u64 = 1;
/// The fake owner stub answers its nth `scope.sync` with correlation `base + n`.
const SCOPE_SYNC_CORR: u64 = 1_000_001;

fn scope_sync_path(root: &Path) -> PathBuf {
    root.join("owner-scopes.json")
}

fn owner_events_path(root: &Path) -> PathBuf {
    root.join("owner-events.jsonl")
}

/// Make the conformance owner the daemon's scope authority, have it sync the scope written by
/// [`write_scopes`], and make broca a reserved principal so the scope can list it as the carrier
/// that opens scoped provider routes for plan admission.
pub(super) fn scope_config(config: &mut Value, root: &Path) {
    config["scope_authority_owners"] = json!([OWNER_MODULE]);
    config["modules"]["broca"]["reserved"] = json!(true);
    let env = &mut config["modules"][OWNER_MODULE]["env"];
    env["FAKE_AFT_SCOPE_SYNC_PATH"] = json!(scope_sync_path(root));
    env["FAKE_AFT_EVENTS_PATH"] = json!(owner_events_path(root));
}

/// The owner stub syncs this file when it starts, so it is written before the stack starts.
fn write_scopes(root: &Path) {
    let record = json!([{
        "ref": SCOPE_REF,
        "scope_epoch": SCOPE_EPOCH,
        "kind": "head",
        "attributes": {},
        "carriers": [{"principal": {"kind": "reserved", "module_id": "broca"}}],
    }]);
    let path = scope_sync_path(root);
    let staging = path.with_extension("staging");
    std::fs::write(&staging, serde_json::to_vec(&record).unwrap()).unwrap();
    std::fs::rename(&staging, &path).unwrap();
}

/// Lines already in the owner's event log, so a restart waits for its own sync answer.
fn scope_sync_lines(root: &Path) -> usize {
    std::fs::read_to_string(owner_events_path(root))
        .unwrap_or_default()
        .lines()
        .count()
}

/// Wait for the daemon to accept the owner's scope sync, logged after line `after`.
async fn wait_scope_synced(root: &Path, after: usize) {
    broca_test_support::wait_until("the owner's scope.sync answer", || async {
        let text = std::fs::read_to_string(owner_events_path(root)).unwrap_or_default();
        let reply = text
            .lines()
            .skip(after)
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find(|event| {
                event["corr"] == SCOPE_SYNC_CORR
                    && (event["kind"] == "scope_sync_response" || event["kind"] == "error")
            });
        match reply {
            None => Err(format!("no scope.sync answer yet: {text}")),
            Some(reply) => {
                assert_eq!(
                    reply["kind"], "scope_sync_response",
                    "scope.sync refused: {reply}"
                );
                for result in reply["body_json"]["results"].as_array().unwrap() {
                    assert_ne!(
                        result["outcome"], "refused",
                        "the scope was refused: {result}"
                    );
                }
                Ok(())
            }
        }
    })
    .await;
}

/// The conversation's management route, opened by the scope owner under [`SCOPE_REF`].
async fn scoped_session_route(subject: &BrocaSubject, live: &Live) -> BrocaRoute {
    use subc_protocol::{scope::ScopeSelector, Principal};
    let stamp = subject.owner_stamp();
    let connection = subject.connection(live, &stamp).await.unwrap();
    let identity = subject.bind_identity(SESSION);
    subject
        .shared
        .session_roots
        .lock()
        .unwrap()
        .insert(SESSION.to_owned(), live.state_root.clone());
    subject
        .shared
        .session_bindings
        .lock()
        .unwrap()
        .insert(SESSION.into(), identity.clone());
    let selector = || {
        ScopeSelector::new(
            Principal::Reserved {
                module_id: OWNER_MODULE.into(),
            },
            SCOPE_REF,
            Some(SCOPE_EPOCH),
        )
    };
    let route = broca_test_support::wait_until("broca's scoped management route", || async {
        match connection
            .route_open_management_scoped_within(
                "broca",
                &identity,
                selector(),
                broca_subc::RouteOpenRetry::within(
                    Duration::from_secs(2),
                    Duration::from_millis(100),
                ),
            )
            .await
        {
            Ok(route) => Ok(route),
            // A scope refusal is an answer, not a module still starting.
            Err(error) if error.to_string().contains("scope_") => {
                panic!("broca refused the scoped route: {error}")
            }
            Err(error) => Err(error.to_string()),
        }
    })
    .await;
    BrocaRoute {
        subject: subject.clone(),
        connection,
        route,
        session: identity,
        describe_calls: std::sync::atomic::AtomicUsize::new(0),
        logs_dir: live.logs_dir.clone(),
    }
}

/// The plan the conversation is admitted with, the shape a worker ships with: Magic Context's
/// tool, system-text and compaction items all on preset `worker`, so it serves ctx_reduce,
/// ctx_expand and ctx_search with the worker guidance and writes no summaries. The scripted
/// tool beside it gives each turn a tool output to drop. Tags are rendered so ctx_reduce can
/// name outputs by their `§N§` tag.
fn worker_plan() -> Value {
    let item = json!({"provider":"magic-context","preset":"worker","params":{"compacting":true}});
    let composition = json!({
        "providers":[
            {"provider":"magic-context","tools":[
                {"name":"ctx_reduce","capabilities":["magic-context:context.reduce/v1"]},
                {"name":"ctx_expand","capabilities":["magic-context:history.expand/v1"]},
                {"name":"ctx_search","capabilities":["magic-context:archive.search/v1"]},
            ]},
            {"provider":TOOL_MODULE,"tools":[{"name":SCRIPTED_TOOL,"capabilities":[]}]},
        ],
        "compaction":{"provider":"magic-context"},
        "step_transform_items":[],
    });
    json!({
        "tool_items":[item, {"provider":TOOL_MODULE,"preset":"conformance","params":{}}],
        "system_text_items":[item],
        "step_transform_items":[],
        "compaction_item":{"provider":"magic-context","preset":"worker","params":{}},
        "render_tags":true,
        "composition_digest":cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),
        "composition":composition,
    })
}

/// The frozen plan of the conversation's first run carries the worker shape it was sent with.
fn assert_frozen_worker_plan(records: &[WalRecord]) {
    let manifest = records
        .iter()
        .find_map(|r| match r {
            WalRecord::RunStarted { manifest, .. } => manifest.clone(),
            _ => None,
        })
        .expect("the first run froze no plan manifest");
    let plan: Value = serde_json::from_str(manifest.plan.as_json_text()).unwrap();
    let worker = json!({"provider":"magic-context","preset":"worker","params":{"compacting":true}});
    let mc_tools: Vec<_> = plan["tool_items"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|item| item["provider"] == "magic-context")
        .collect();
    assert_eq!(mc_tools.len(), 1, "frozen plan: {plan}");
    for key in ["provider", "preset", "params"] {
        assert_eq!(mc_tools[0][key], worker[key], "frozen tool item: {plan}");
        assert_eq!(
            plan["system_text_items"][0][key], worker[key],
            "frozen system-text item: {plan}"
        );
    }
    assert_eq!(
        plan["compaction_item"]["provider"], "magic-context",
        "frozen compaction item: {plan}"
    );
    assert_eq!(
        plan["compaction_item"]["preset"], "worker",
        "frozen compaction item: {plan}"
    );
}

/// The large, distinct output the scripted tool answers for read `index`.
fn tool_output(index: usize) -> String {
    format!(
        "Tool output {index}: {}",
        "deterministic tool evidence line. ".repeat(700)
    )
}

/// The two model turns of one run: a call, then a short answer that ends the run.
fn run_turns(call: ScriptedToolCall, answer: &str) -> [ScriptedTurn; 2] {
    [
        ScriptedTurn::new(vec![ScriptedPart::ToolCall(call)]),
        ScriptedTurn::new(vec![ScriptedPart::Text(answer.into())]),
    ]
}

fn read_call(index: usize) -> ScriptedToolCall {
    ScriptedToolCall {
        tool_call_id: format!("read_{index}"),
        tool: SCRIPTED_TOOL.into(),
        arguments: json!({"read": index}),
        result: json!(tool_output(index)),
        hold: false,
    }
}

/// The `§N§` tags broca rendered on the tool outputs of `reads` in the model request `body`.
fn output_tags(body: &[u8], reads: &[usize]) -> Vec<u64> {
    let request: Value = serde_json::from_slice(body).unwrap();
    let mut tags = Vec::new();
    for message in request["messages"].as_array().into_iter().flatten() {
        if message["role"] != "tool" {
            continue;
        }
        let texts: Vec<&str> = match &message["content"] {
            Value::String(text) => vec![text.as_str()],
            Value::Array(parts) => parts.iter().filter_map(|p| p["text"].as_str()).collect(),
            _ => Vec::new(),
        };
        for text in texts {
            let Some(rest) = text.strip_prefix('§') else {
                continue;
            };
            let Some((tag, output)) = rest.split_once("§ ") else {
                continue;
            };
            let named = reads.iter().any(|read| {
                // The scripted result is a JSON string, so its text keeps the quotes.
                output
                    .trim_start_matches('"')
                    .starts_with(&format!("Tool output {read}:"))
            });
            if let (true, Ok(tag)) = (named, tag.parse()) {
                tags.push(tag);
            }
        }
    }
    tags
}

/// The newest durable advance notice: its sequence, estimated tokens and window.
fn last_notice(records: &[WalRecord]) -> Option<(u64, u64, Option<u64>)> {
    records.iter().rev().find_map(|r| match r {
        WalRecord::CompactionNotice { seq, headers } => {
            let notice =
                broca_core::compaction_provider::notice::AdvanceNotice::from_headers(headers)
                    .ok()?;
            Some((*seq, notice.est_tokens, notice.window))
        }
        _ => None,
    })
}

#[test]
fn output_tags_reads_the_tags_rendered_on_named_tool_outputs() {
    let body = json!({"messages":[
        {"role":"user","content":"§1§ question"},
        {"role":"tool","tool_call_id":"read_0","content":format!("§4§ \"{}\"", tool_output(0))},
        {"role":"tool","tool_call_id":"read_1","content":[{"type":"text","text":format!("§9§ \"{}\"", tool_output(1))}]},
        {"role":"tool","tool_call_id":"read_2","content":format!("§12§ \"{}\"", tool_output(2))},
    ]});
    let body = serde_json::to_vec(&body).unwrap();
    assert_eq!(output_tags(&body, &[0, 1]), vec![4, 9]);
    assert_eq!(output_tags(&body, &[2]), vec![12]);
    assert!(output_tags(&body, &[3]).is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_mc_async_compaction_end_to_end() {
    let Some(binary) = std::env::var_os("BROCA_MC_E2E_BIN").filter(|v| !v.is_empty()) else {
        println!("SKIP real_mc_async_compaction_end_to_end: BROCA_MC_E2E_BIN is unset; real MC was NOT exercised");
        return;
    };
    // Once opted in, every prerequisite is mandatory, including under BROCA_REQUIRE_SUBC.
    let database = std::env::var_os("BROCA_MC_E2E_DB")
        .expect("BROCA_MC_E2E_DB must name the store fixture MC staged with this build");
    let binary_sha = std::env::var("BROCA_MC_E2E_BIN_SHA256")
        .expect("BROCA_MC_E2E_BIN_SHA256 must name the sha256 MC published for this build");
    let database_sha = std::env::var("BROCA_MC_E2E_DB_SHA256")
        .expect("BROCA_MC_E2E_DB_SHA256 must name the sha256 MC published for that fixture");
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
    let mc = McConfig::prepare(
        &root,
        Path::new(&binary),
        &binary_sha,
        Path::new(&database),
        &database_sha,
    );
    let mut subject = BrocaSubject::new(&root).await;
    subject.mc = Some(mc.clone());
    subject
        .shared
        .plan_sessions
        .lock()
        .unwrap()
        .insert(SESSION.into());
    // An applied view can drop messages, so the scripted model counts the turns it served.
    subject
        .shared
        .counted_turns
        .lock()
        .unwrap()
        .insert(SESSION.into(), 0);
    let mut evidence = Evidence {
        events: None,
        root: root.clone(),
        identity: subject.bind_identity(SESSION),
    };
    let session_ref = broca_core::agent_session_ref(&subject.bind_identity(SESSION));
    note(&root, format!("Authenticated MC sha256={binary_sha}; fixture sha256={database_sha}; no billed models; session_ref={session_ref}; artifacts={}", root.display()));
    let controls = root.join("broca-state/test-compaction").join(SESSION);
    std::fs::create_dir_all(&controls).unwrap();
    std::fs::write(
        controls.join("config"),
        serde_json::to_vec(&json!({
        "context_window":128000,"output_limit":4096,"call_timeout_ms":30000}))
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
    write_scopes(&root);
    let mut live = subject
        .start(&root)
        .await
        .expect("start private supervised stack");
    wait_scope_synced(&root, 0).await;
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
    let route = scoped_session_route(&subject, &live).await;

    // The scripted model's turns, extended run by run. Each run is a tool call and an answer.
    let mut turns: Vec<ScriptedTurn> = Vec::new();
    let mut runs = 0;
    let mut reads = 0;
    // Run one turn of the conversation: install its two model turns, pace on the previous
    // run's published notice, send, and wait for its terminal.
    macro_rules! run {
        ($call:expr, $answer:expr) => {{
            turns.extend(run_turns($call, $answer));
            subject
                .install_script(
                    SESSION,
                    Script {
                        turns: turns.clone(),
                    },
                )
                .await
                .unwrap();
            if runs > 0 {
                previous_notice_published(&subject, &live, &events).await;
            }
            let mut sent = fields(&subject, runs == 0, runs);
            if runs == 0 {
                sent["plan"] = worker_plan();
            }
            send(&route, sent, &root).await;
            runs += 1;
            finished(&subject, &live, runs).await;
            let records = subject.records(&live, SESSION);
            note(
                &root,
                format!(
                    "run={} model_request_bytes={:?} last_notice(seq, est_tokens, window)={:?}",
                    runs - 1,
                    requests(&subject).last().map(Vec::len),
                    last_notice(&records)
                ),
            );
            records
        }};
    }
    // A few tool outputs, then ctx_reduce on the two oldest. The tags come from the request
    // the model just received, as a model would read them.
    for _ in 0..3 {
        let records = run!(read_call(reads), "Noted that output.");
        reads += 1;
        if runs == 1 {
            assert_frozen_worker_plan(&records);
            note(&root, "Frozen plan: Magic Context tool, system-text and compaction items on preset worker; tool and system text with params {\"compacting\":true}");
        }
    }
    let dropped = [0, 1];
    let tags = output_tags(requests(&subject).last().unwrap(), &dropped);
    assert_eq!(
        tags.len(),
        dropped.len(),
        "broca rendered no §N§ tag on the outputs of reads {dropped:?}; the plan sets render_tags"
    );
    let drop_tags = tags
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let records = run!(
        ScriptedToolCall {
            tool_call_id: "reduce_old_outputs".into(),
            tool: "ctx_reduce".into(),
            arguments: json!({"drop": drop_tags}),
            result: Value::Null,
            hold: false,
        },
        "Dropped the old outputs."
    );
    let reduce_result = records
        .iter()
        .find_map(|r| match r {
            WalRecord::ToolResult { result, .. }
                if result.tool_call_id.0 == "reduce_old_outputs" =>
            {
                Some(format!("{:?}", result.output))
            }
            _ => None,
        })
        .expect("ctx_reduce produced no tool result");
    note(
        &root,
        format!("ctx_reduce drop={drop_tags}: {reduce_result}"),
    );
    // More tool output until usage passes Magic Context's execute threshold, 50% of the window.
    let (crossed_seq, _, _) = loop {
        assert!(
            runs < 60,
            "usage never passed 50% of the window after {runs} runs"
        );
        let records = run!(read_call(reads), "Noted that output.");
        reads += 1;
        let notice = last_notice(&records).expect("no durable advance notice");
        let (_, est_tokens, window) = notice;
        if est_tokens * 2 > window.unwrap_or(128_000) {
            break notice;
        }
    };
    note(&root, format!("Usage passed 50% of the window at notice seq {crossed_seq}, with drops queued for reads {dropped:?}"));
    // Magic Context publishes its view on the step after the crossing.
    let seen = announcements(&bus, &events, &session_ref)
        .iter()
        .map(|a| a.view_version)
        .max()
        .unwrap_or(0);
    let records = run!(read_call(reads), "Noted that output.");
    reads += 1;
    let last_seq = last_notice(&records).map_or(crossed_seq, |(seq, _, _)| seq);
    let description = format!(
        "Magic Context's view_ready for session_ref={session_ref} above version {seen}, after usage passed 50% with drops queued (last advance notice seq={last_seq})"
    );
    let announced = broca_test_support::wait_until(&description, || async {
        let newer: Vec<_> = announcements(&bus, &events, &session_ref)
            .into_iter()
            .filter(|a| a.view_version > seen)
            .collect();
        newer.last().cloned().ok_or_else(|| {
            format!(
                "announced versions: {:?}",
                announcements(&bus, &events, &session_ref)
            )
        })
    })
    .await;
    note(&root, format!("Real MC view_ready: {announced:?}"));
    broca_test_support::wait_until("broca's durable receipt of the view_ready", || async {
        if root
            .join("broca-state/test-compaction-bus-reached")
            .exists()
        {
            Ok(())
        } else {
            Err("the receipt gate was not reached".to_string())
        }
    })
    .await;
    let store = broca_store::SqliteStore::open(live.state_root.join("run-index.db")).unwrap();
    let receipts = store.compaction_receipts("", 256).unwrap();
    assert_eq!(
        receipts.len(),
        1,
        "view_ready must have a durable receipt before ACK"
    );
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
    // The next model request, the first of the next run, against the one before it.
    let before = requests(&subject).last().unwrap().len();
    let served = requests(&subject).len();
    let records = run!(read_call(reads), "Noted that output.");
    let next = requests(&subject)[served].clone();
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
    let WalRecord::CompactionViewApplied {
        provider,
        view_version: applied_version,
        view: raw,
        ..
    } = &view
    else {
        unreachable!();
    };
    assert_eq!(provider, "magic-context");
    let decoded: broca_core::compaction_provider::view::CompactionView =
        serde_json::from_str(raw.as_json_text()).unwrap();
    let ready = applied_announcement(&root, &bus, &events, &decoded, *applied_version).await;
    assert_bus_notices(&root, &bus, &events, &records, &ready).await;
    note(&root, format!("Setup recorded once; view at WAL index {view_at}: {view:?}; request bytes before={before}, next={}", next.len()));
    assert!(
        next.len() < before,
        "the worker view must shrink the next model request: {before} -> {}",
        next.len()
    );
    assert_worker_view(&root, &next, &decoded, &dropped);
    assert!(
        records[view_at + 1..]
            .iter()
            .any(|r| matches!(r, WalRecord::StepStarted { .. })),
        "view must precede its consuming step"
    );

    // A real kill mid-step, then a resume with byte-identical request bytes. The held run is
    // answered with text only, and after the restart the scripted model serves its next turn.
    turns.extend([
        ScriptedTurn::new(vec![ScriptedPart::Text("Held answer.".into())]),
        ScriptedTurn::new(vec![ScriptedPart::Text("Resumed answer.".into())]),
    ]);
    subject
        .install_script(
            SESSION,
            Script {
                turns: turns.clone(),
            },
        )
        .await
        .unwrap();
    subject.hold_next_model_step(SESSION).await.unwrap();
    let resume_fields = fields(&subject, false, runs);
    previous_notice_published(&subject, &live, &events).await;
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
    let synced = scope_sync_lines(&root);
    live = subject.start(&root).await.unwrap();
    wait_scope_synced(&root, synced).await;
    let route = scoped_session_route(&subject, &live).await;
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
    finished(&subject, &live, runs + 1).await;
    note(&root, mc.assert_private_databases(&live));
    note(&root, "Not checked: ride views, reminder text, and the ceiling beyond Setup recording one. No historian is configured, so no summarizer child run can be made.");
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
/// The `view_ready` announcements of the rig's session seen on the bus, in arrival order.
fn announcements(
    bus: &bus::BusRig,
    events: &Events,
    session_ref: &str,
) -> Vec<broca_core::compaction_provider::notice::ViewReady> {
    use broca_core::compaction_provider::notice::{event_subject, ViewReady, VIEW_READY_EVENT};
    let subject = event_subject(bus.names.account(), "magic-context", VIEW_READY_EVENT);
    events
        .messages
        .lock()
        .unwrap()
        .iter()
        .filter(|m| m.subject.as_str() == subject)
        .filter_map(|m| {
            let headers: std::collections::BTreeMap<String, String> = m
                .headers
                .as_ref()?
                .iter()
                .filter_map(|(name, values)| {
                    let name: &str = name.as_ref();
                    Some((name.to_owned(), values.last()?.as_str().to_owned()))
                })
                .collect();
            ViewReady::from_headers(&headers).ok()
        })
        .filter(|ready| ready.session_ref == session_ref)
        .collect()
}

/// The `broca.compaction` lines of broca's log for `session_ref`, in the order written, each
/// as its `key=value` fields. The fleet logger quotes a value only when it holds a space or a
/// quote, which none of the fields read here do.
fn compaction_log(
    root: &Path,
    session_ref: &str,
) -> Vec<std::collections::BTreeMap<String, String>> {
    let mut files: Vec<_> = std::fs::read_dir(root.join("data-home/cortexkit/broca/logs"))
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| entry.path())
        .collect();
    files.sort();
    files
        .iter()
        .filter_map(|path| std::fs::read_to_string(path).ok())
        .flat_map(|text| {
            text.lines()
                .filter_map(|line| line.split_once(" broca.compaction: "))
                .map(|(_, rest)| {
                    let mut fields: std::collections::BTreeMap<String, String> = rest
                        .split(' ')
                        .filter_map(|token| token.split_once('='))
                        .map(|(k, v)| (k.to_owned(), v.trim_matches('"').to_owned()))
                        .collect();
                    let message = rest.split(" session_ref=").next().unwrap_or(rest);
                    fields.insert("message".into(), message.to_owned());
                    fields
                })
                .collect::<Vec<_>>()
        })
        .filter(|fields| fields.get("session_ref").map(String::as_str) == Some(session_ref))
        .collect()
}

/// The bus announcement of the view broca applied, after checking that broca applied the
/// newest view it had. Magic Context may publish several views before broca reaches an idle
/// or step boundary (in one real run, versions 1 and 2 arrived 48 ms apart), and broca then
/// applies only the newest, so the applied view is matched to its own announcement, not to
/// the first one seen. Checks:
/// - an announcement of the applied version is on the bus, with the applied view's digest;
/// - broca's log receipted no version above it before it logged applying it;
/// - every announcement seen on the bus was receipted.
async fn applied_announcement(
    root: &Path,
    bus: &bus::BusRig,
    events: &Events,
    view: &broca_core::compaction_provider::view::CompactionView,
    applied_version: u64,
) -> broca_core::compaction_provider::notice::ViewReady {
    let expected = broca_core::compaction_provider::notice::ViewReady::for_view(view);
    assert_eq!(expected.view_version, applied_version);
    let session_ref = expected.session_ref.clone();
    let version = |fields: &std::collections::BTreeMap<String, String>| {
        fields
            .get("view_version")
            .and_then(|v| v.parse::<u64>().ok())
    };
    let (announced, receipted, before_apply) = broca_test_support::wait_until(
        "broca's log of receipting every announced view and applying the newest",
        || async {
            let announced = announcements(bus, events, &session_ref);
            let log = compaction_log(root, &session_ref);
            let receipted: Vec<u64> = log
                .iter()
                .filter(|f| f.get("outcome").map(String::as_str) == Some("receipted"))
                .filter_map(version)
                .collect();
            let Some(applied_at) = log.iter().position(|f| {
                f["message"].starts_with("compaction view decided at ")
                    && f.get("choice").map(String::as_str) == Some("applied")
                    && version(f) == Some(applied_version)
            }) else {
                return Err(format!(
                    "no log line applying view {applied_version}; log={log:?}"
                ));
            };
            let unreceipted: Vec<u64> = announced
                .iter()
                .map(|a| a.view_version)
                .filter(|v| !receipted.contains(v))
                .collect();
            if !unreceipted.is_empty() {
                return Err(format!(
                    "announced but not yet receipted: {unreceipted:?}; log={log:?}"
                ));
            }
            let before_apply: Vec<u64> = log[..applied_at]
                .iter()
                .filter(|f| f.get("outcome").map(String::as_str) == Some("receipted"))
                .filter_map(version)
                .collect();
            Ok((announced, receipted, before_apply))
        },
    )
    .await;
    let ready = announced
        .iter()
        .find(|a| a.view_version == applied_version)
        .unwrap_or_else(|| {
            panic!("broca applied view {applied_version}, which MC never announced: {announced:?}")
        })
        .clone();
    assert_eq!(
        ready.digest, expected.digest,
        "applied view's digest differs from its bus announcement"
    );
    assert_eq!(
        ready, expected,
        "applied view does not match its announcement"
    );
    assert!(
        before_apply.contains(&applied_version),
        "broca applied view {applied_version} before receipting it: receipted first {before_apply:?}"
    );
    assert!(
        before_apply.iter().all(|v| *v <= applied_version),
        "broca applied view {applied_version} while it held a newer one: receipted first {before_apply:?}"
    );
    note(
        root,
        format!(
            "Announced view versions on the bus: {:?}; receipted: {receipted:?}; receipted before applying: {before_apply:?}; applied: {applied_version}",
            announced.iter().map(|a| a.view_version).collect::<Vec<_>>()
        ),
    );
    ready
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
    let matches = published_with;
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
    write_scopes(&root);
    let replay_bus = root.join(format!("read-replay-bus-{}", std::process::id()));
    // `BusRig::new` has already reported the skip, or failed under CK_BUS_TESTKIT_REQUIRE.
    let Some(bus) = bus::BusRig::new(&replay_bus).await else {
        return;
    };
    let bus = Arc::new(bus);
    provision_consumers(&bus).await;
    subject.bus = Some(bus.clone());
    let synced = scope_sync_lines(&root);
    let live = subject.start(&root).await.unwrap();
    wait_scope_synced(&root, synced).await;
    let route = scoped_session_route(&subject, &live).await;
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

/// What a worker view does: it drops or replaces parts of the transcript, never summarizes it
/// into a fold of its own. Checks that the view changes something, that every replacement text
/// it carries reaches the model, and that the tool outputs the model's ctx_reduce call named by
/// tag are no longer sent.
fn assert_worker_view(
    root: &Path,
    body: &[u8],
    view: &broca_core::compaction_provider::view::CompactionView,
    dropped: &[usize],
) {
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
    let (mut edits, mut replaced) = (0, 0);
    for range in &view.ranges {
        match range {
            ViewRange::Keep { edits: changed, .. } => edits += changed.len(),
            ViewRange::Replace { bytes, .. } => {
                replaced += 1;
                let messages: Vec<broca_protocol::Message> = serde_json::from_str(bytes).unwrap();
                for message in messages {
                    for part in message.content {
                        if let broca_protocol::ContentBlock::Text { text } = part {
                            if !text.is_empty() {
                                assert!(
                                    wire_text.iter().any(|wire| wire.contains(&text)),
                                    "the model did not receive text from MC's applied view: {text:?}"
                                );
                            }
                        }
                    }
                }
            }
            // A range kind this rig does not know yet changes nothing it can check.
            _ => {}
        }
    }
    assert!(
        edits + replaced > 0,
        "MC's applied view changes nothing: {view:?}"
    );
    let still_sent: Vec<usize> = dropped
        .iter()
        .copied()
        .filter(|read| {
            let marker = format!("Tool output {read}:");
            wire_text.iter().any(|wire| wire.contains(&marker))
        })
        .collect();
    assert!(
        still_sent.is_empty(),
        "tool outputs named in the ctx_reduce call are still sent after MC's view: reads {still_sent:?}"
    );
    note(
        root,
        format!("Worker view: {edits} part edits, {replaced} replaced ranges; stamped reads {dropped:?} no longer sent"),
    );
}
