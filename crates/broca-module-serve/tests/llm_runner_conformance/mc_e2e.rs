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
    // broca refuses a composition that is not in canonical order: providers sorted bytewise by
    // id, and each provider's tools by name.
    let composition = json!({
        "providers":[
            {"provider":TOOL_MODULE,"tools":[{"name":SCRIPTED_TOOL,"capabilities":[]}]},
            {"provider":"magic-context","tools":[
                {"name":"ctx_expand","capabilities":["magic-context:history.expand/v1"]},
                {"name":"ctx_reduce","capabilities":["magic-context:context.reduce/v1"]},
                {"name":"ctx_search","capabilities":["magic-context:archive.search/v1"]},
            ]},
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
    // The receipt gate is not armed here. Magic Context may publish a view after any advance
    // notice, the first one included, so a gate armed at startup would hold whichever view came
    // first rather than the one this test means to observe. It is armed later, just before the
    // step that should make Magic Context publish the view carrying the drops.
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
    // Start one turn of the conversation: install its two model turns, pace on the previous
    // run's published notice, and send. `end_run!` waits for its terminal.
    macro_rules! begin_run {
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
        }};
    }
    // Wait for the turn `begin_run!` started last to reach its terminal; returns the WAL.
    macro_rules! end_run {
        () => {{
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
    // One whole turn: start it and wait for its terminal.
    macro_rules! run {
        ($call:expr, $answer:expr) => {{
            begin_run!($call, $answer);
            end_run!()
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
    // Arm the receipt gate for the view this test observes: the one Magic Context publishes after
    // usage passed its threshold with drops queued, which in practice comes after the step that
    // follows the crossing. Views it published earlier (it may publish one after any advance
    // notice, with or without drops) were handled ungated; first wait until broca has logged
    // every one seen so far, so the gate cannot be credited to one of them.
    let applied_before = highest_applied(&subject.records(&live, SESSION));
    let settled_before = handled_announcements(&root, &bus, &events, &session_ref).await;
    let receipt_gate = root.join("broca-state/test-compaction-bus-gate");
    let reached = root.join("broca-state/test-compaction-bus-reached");
    match std::fs::remove_file(&reached) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => panic!("{}: {error}", reached.display()),
    }
    std::fs::write(&receipt_gate, "ViewReceiptRecorded").unwrap();
    note(
        &root,
        format!("Receipt gate armed: broca has applied up to view {applied_before}; view_ready versions handled before arming: {settled_before:?}"),
    );
    // The gate holds broca's view_ready consumer, not the conversation, so the run goes on
    // while the gate waits; its terminal is awaited only after the gate is released, because
    // the consumer gives up on a delivery it holds for 15 seconds and the broker redelivers it.
    begin_run!(read_call(reads), "Noted that output.");
    reads += 1;
    let held = gated_view(
        &root,
        &bus,
        &events,
        &session_ref,
        &settled_before,
        applied_before,
    )
    .await;
    note(
        &root,
        format!("Real MC view_ready held at the receipt gate: {held:?}"),
    );
    // The ACK is held, so the receipt must already be durable. The idle boundary may have
    // decided the view meanwhile from that durable receipt (applied, refused or passed over
    // for a newer one) and cleared it, so a recorded decision counts as the same proof.
    let proof = receipt_or_decision(&live, &subject, &held);
    note(
        &root,
        format!("Before ACK of view_ready v{}: {proof}", held.view_version),
    );
    std::fs::remove_file(&receipt_gate).unwrap();
    wait_for_ack(&bus).await;
    note(&root, "Broker m_broca durable confirms view_ready ACK");
    let records = end_run!();
    let last_seq = last_notice(&records).map_or(crossed_seq, |(seq, _, _)| seq);
    let awaited = broca_test_support::wait_until(
        &format!("broca applying Magic Context's view v{} or a newer one (last advance notice seq={last_seq})", held.view_version),
        || async {
            let records = subject.records(&live, SESSION);
            let newest = highest_applied(&records);
            let log = compaction_log(&root, &session_ref);
            if gated_view_applied(&log, held.view_version, newest) {
                return Ok(newest);
            }
            if let Some((seq, _, reason)) = view_refusals(&records)
                .into_iter()
                .find(|(_, refused, _)| *refused == held.view_version)
            {
                panic!("broca refused Magic Context's view v{} (advance notice seq {seq}, reason {reason}); see broca's log and MC's logs", held.view_version);
            }
            Err(format!(
                "newest applied view is v{newest}; log={log:?}; advance notice events: {:?}; WAL record kinds: {:?}",
                notice_events(&records),
                records.iter().map(record_kind).collect::<Vec<_>>()
            ))
        },
    )
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
    // The view `next` renders: the newest one broca applied before the first step of the run
    // that sent it. Earlier views (Magic Context may have published one before any drop) are
    // not this one, and it may be newer than the one awaited above if Magic Context published
    // again in between.
    let run_start = records
        .iter()
        .rposition(|r| matches!(r, WalRecord::RunStarted { .. }))
        .expect("the last run recorded no start");
    let first_step = run_start
        + records[run_start..]
            .iter()
            .position(|r| matches!(r, WalRecord::StepStarted { .. }))
            .expect("the last run recorded no step");
    let view_at = records[..first_step]
        .iter()
        .rposition(|r| matches!(r, WalRecord::CompactionViewApplied { .. }))
        .unwrap_or_else(|| panic!("the WAL records view v{awaited} as applied, yet no applied view precedes the next run's first step; STOP and inspect MC logs"));
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
    let applied_version = *applied_version;
    assert!(
        applied_version >= held.view_version,
        "the next request renders view v{applied_version}, older than the gated view v{}",
        held.view_version
    );
    assert_eq!(provider, "magic-context");
    // The WAL records the view exactly as broca's `view.get` returned it.
    let decoded: broca_core::compaction_provider::view::CompactionView =
        serde_json::from_str(raw.as_json_text()).unwrap();
    let ready = applied_announcement(
        &root,
        &bus,
        &events,
        &decoded,
        applied_version,
        held.view_version,
    )
    .await;
    assert_bus_notices(&root, &bus, &events, &records, &ready).await;
    note(&root, format!("Setup recorded once; view v{applied_version} at WAL index {view_at}: {view:?}; request bytes before={before}, next={}", next.len()));
    assert_mc_worker_drops(&root, &records, &dropped, &drop_tags);
    assert!(
        next.len() < before,
        "the worker view v{applied_version} must shrink the next model request: {before} -> {}",
        next.len()
    );
    assert_worker_view(&root, &next, &decoded, &dropped);
    assert!(
        records[view_at + 1..]
            .iter()
            .any(|r| matches!(r, WalRecord::StepStarted { .. })),
        "view v{applied_version} must precede its consuming step"
    );

    // A real kill mid-step, then a new episode after the restart. Broca's boot
    // crash-orphan seal ends the SIGKilled run `interrupted` (documented behaviour,
    // unchanged since the public release): resending its send_id only reads that
    // terminal back and can never resume the held model step. The held run is
    // answered with text only, and after the restart the scripted model serves its
    // next turn, so the new episode answers "Resumed answer.".
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
    // The hold below captures the new episode's first request. The resend of the
    // killed send_id must not reach the model, so the request count proves it.
    subject.hold_next_model_step(SESSION).await.unwrap();
    let synced = scope_sync_lines(&root);
    live = subject.start(&root).await.unwrap();
    wait_scope_synced(&root, synced).await;
    let route = scoped_session_route(&subject, &live).await;
    let killed_send_id = resume_fields["send_id"].as_str().unwrap().to_owned();
    // The killed run's id, from the WAL it started: the resend below must name it.
    let killed_run_id = subject
        .records(&live, SESSION)
        .iter()
        .rev()
        .find_map(|record| match record {
            WalRecord::RunStarted {
                run_id, send_id, ..
            } if send_id.as_deref() == Some(killed_send_id.as_str()) => Some(run_id.0.clone()),
            _ => None,
        })
        .expect("the killed run recorded no start");
    let sent_before_resend = requests(&subject).len();
    let resend = route
        .request("session.send", resume_fields.clone())
        .await
        .expect("session.send transport");
    note(
        &root,
        format!("resend of the killed send_id replied: {resend:?}"),
    );
    // The resend answers the sealed terminal instead of resuming: a resumed run
    // would answer `active` and reach the model, neither of which may happen.
    let Reply::Response(resend) = &resend else {
        panic!("the killed send_id was refused after the restart");
    };
    assert_eq!(
        resend["run_id"].as_str(),
        Some(killed_run_id.as_str()),
        "the resend named a different run than the killed one"
    );
    assert_eq!(
        resend["state"].as_str(),
        Some("finished"),
        "the killed run resumed instead of reading back its terminal: {resend}"
    );
    assert_eq!(
        resend["reason"].as_str(),
        Some("interrupted"),
        "the killed run did not read back interrupted: {resend}"
    );
    assert_eq!(
        requests(&subject).len(),
        sent_before_resend,
        "the resend of a sealed run reached the model"
    );
    // The terminal is `interrupted`, never `cancelled`: the seal ends a run the
    // daemon never cancelled. The boot pass may still be sealing it, so a
    // transient refusal means poll again, as the retention lane does.
    let status =
        broca_test_support::wait_until("the killed run reads back interrupted", || async {
            let reply = route
                .request("run.status", json!({ "run_id": killed_run_id }))
                .await
                .expect("run.status transport");
            match reply {
                Reply::Error(error)
                    if error
                        .detail
                        .as_ref()
                        .is_some_and(|detail| detail["class"] == "transient") =>
                {
                    Err(format!(
                        "run.status not ready: {} {}",
                        error.code, error.message
                    ))
                }
                Reply::Error(error) => panic!(
                    "run.status refused the killed run: {} {}",
                    error.code, error.message
                ),
                Reply::Response(status) if status["state"] == "active" => {
                    Err(format!("the killed run is still active: {status}"))
                }
                Reply::Response(status) => Ok(status),
            }
        })
        .await;
    assert_eq!(
        status["state"].as_str(),
        Some("interrupted"),
        "the killed run did not read back interrupted: {status}"
    );
    let records = subject.records(&live, SESSION);
    let start_at = records
        .iter()
        .position(|record| {
            matches!(record, WalRecord::RunStarted { run_id, .. } if run_id.0 == killed_run_id)
        })
        .expect("the killed run recorded no start");
    // The WAL agrees with the API above: the killed episode's terminal is
    // `interrupted`, so it was sealed, never cancelled.
    let terminal = records[start_at..]
        .iter()
        .find_map(|record| match record {
            WalRecord::RunFinished { reason, .. } => Some(reason),
            _ => None,
        })
        .expect("the killed run recorded no terminal");
    assert!(
        matches!(terminal, broca_protocol::RunFinishReason::Interrupted),
        "the killed run was not sealed interrupted: {terminal:?}"
    );
    // The held model step never answered: its step started, no step finished, so
    // there is no unfinished reply that could leak into the transcript.
    assert!(
        records[start_at..]
            .iter()
            .any(|record| matches!(record, WalRecord::StepStarted { .. })),
        "the killed run recorded no step"
    );
    assert!(
        !records[start_at..]
            .iter()
            .any(|record| matches!(record, WalRecord::ModelStepFinished { .. })),
        "the held model step finished after the kill"
    );
    let page = route
        .request("session.read", json!({}))
        .await
        .expect("session.read transport");
    let Reply::Response(page) = &page else {
        panic!("session.read refused after the restart");
    };
    // The killed run's user message stays, but its held reply never existed: the
    // model was held before answering and the seal finished the run without it.
    assert!(
        !page.to_string().contains("Held answer."),
        "the held step's unfinished reply is in the transcript: {page}"
    );
    // A NEW send_id starts a new episode: it must answer `active` under a fresh
    // run id rather than joining the sealed run.
    previous_notice_published(&subject, &live, &events).await;
    let next_fields = fields(&subject, false, runs + 1);
    assert_ne!(
        next_fields["send_id"].as_str(),
        Some(killed_send_id.as_str()),
        "the new send reused the killed send_id"
    );
    let next_send = route
        .request("session.send", next_fields)
        .await
        .expect("session.send transport");
    note(&root, format!("new send replied: {next_send:?}"));
    let Reply::Response(next_send) = &next_send else {
        panic!("the new send was refused after the restart");
    };
    assert_eq!(
        next_send["state"].as_str(),
        Some("active"),
        "the new send_id did not start a new episode: {next_send}"
    );
    let next_run_id = next_send["run_id"]
        .as_str()
        .expect("the new send names its run");
    assert_ne!(
        next_run_id, killed_run_id,
        "the new send joined the killed run instead of starting an episode"
    );
    subject.await_model_step(SESSION).await.unwrap();
    let after_restart = requests(&subject).last().unwrap().clone();
    std::fs::write(root.join("request-after-restart.json"), &after_restart).unwrap();
    // The new episode renders exactly the compaction-applied history the held
    // request had. Rendering is deterministic, so parsed-JSON equality is the
    // byte-exact check: everything but `messages` is unchanged (system text and
    // tools included), the held messages are the new one's prefix, and the only
    // addition is the new user message.
    let before: Value = serde_json::from_slice(&before_kill).unwrap();
    let after: Value = serde_json::from_slice(&after_restart).unwrap();
    let mut before_head = before.as_object().unwrap().clone();
    let mut after_head = after.as_object().unwrap().clone();
    let before_messages = before_head.remove("messages").unwrap();
    let after_messages = after_head.remove("messages").unwrap();
    assert_eq!(
        after_head, before_head,
        "the new episode changed the held request outside its messages"
    );
    assert_eq!(
        after_messages[0], before_messages[0],
        "the new episode changed the held system message"
    );
    let before_messages = before_messages.as_array().unwrap();
    let after_messages = after_messages.as_array().unwrap();
    assert_eq!(
        after_messages.len(),
        before_messages.len() + 1,
        "the new request is not the held request plus one message"
    );
    assert_eq!(
        &after_messages[..before_messages.len()],
        before_messages,
        "the held request's messages are not the new request's prefix"
    );
    let new_message = after_messages.last().unwrap();
    assert_eq!(
        new_message["role"].as_str(),
        Some("user"),
        "the new request does not end in the new user message: {new_message}"
    );
    assert!(
        new_message
            .to_string()
            .contains(&format!("Turn {}:", runs + 1)),
        "the new request does not end in the new user message: {new_message}"
    );
    let records = subject.records(&live, SESSION);
    assert!(records.contains(&view), "the restart lost the applied view");
    // The applied view still holds: the new episode renders its edits and the
    // dropped reads are still not sent.
    assert_worker_view(&root, &after_restart, &decoded, &dropped);
    note(
        &root,
        format!(
            "New episode {next_run_id} renders the held {}-byte history plus one user message; applied view v{applied_version} intact",
            before_kill.len(),
        ),
    );
    subject.release_model_step(SESSION).await.unwrap();
    finished(&subject, &live, runs + 2).await;
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
///
/// `gated` is the version the rig held at broca's receipt gate and proved durable before its
/// ACK. Broca logs a delivery's `receipted` line only after the gate releases it, so when the
/// idle boundary decided that view during the hold, from its durable receipt, the log shows the
/// decision first. The gate proof stands in for the log order for that one version.
async fn applied_announcement(
    root: &Path,
    bus: &bus::BusRig,
    events: &Events,
    view: &broca_core::compaction_provider::view::CompactionView,
    applied_version: u64,
    gated: u64,
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
            let mut before_apply: Vec<u64> = log[..applied_at]
                .iter()
                .filter(|f| f.get("outcome").map(String::as_str) == Some("receipted"))
                .filter_map(version)
                .collect();
            if !before_apply.contains(&gated) {
                before_apply.push(gated);
            }
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

/// The highest `view_version` broca's WAL records as applied, or 0 when none is.
fn highest_applied(records: &[WalRecord]) -> u64 {
    records
        .iter()
        .filter_map(|r| match r {
            WalRecord::CompactionViewApplied { view_version, .. } => Some(*view_version),
            _ => None,
        })
        .max()
        .unwrap_or(0)
}

/// A gated receipt is settled by its own application, or by a newer applied view
/// and an explicit superseded receipt. Refusal or merely announcing a newer view
/// does not prove application.
fn gated_view_applied(
    log: &[std::collections::BTreeMap<String, String>],
    gated: u64,
    newest: u64,
) -> bool {
    let version = |fields: &std::collections::BTreeMap<String, String>| {
        fields
            .get("view_version")
            .and_then(|v| v.parse::<u64>().ok())
    };
    let applied = |v| {
        log.iter().any(|f| {
            f["message"].starts_with("compaction view decided at ")
                && f.get("choice").map(String::as_str) == Some("applied")
                && version(f) == Some(v)
        })
    };
    newest >= gated
        && (applied(gated)
            || (newest > gated
                && applied(newest)
                && log.iter().any(|f| {
                    f["message"] == "compaction view receipt cleared"
                        && version(f) == Some(gated)
                        && f.get("reason").map(String::as_str) == Some("superseded")
                })))
}

#[test]
fn gated_view_wait_accepts_only_applied_or_explicitly_superseded() {
    let applied = |version: &str| {
        std::collections::BTreeMap::from([
            (
                "message".into(),
                "compaction view decided at a step boundary".into(),
            ),
            ("view_version".into(), version.into()),
            ("choice".into(), "applied".into()),
        ])
    };
    let mut cleared = std::collections::BTreeMap::from([
        ("message".into(), "compaction view receipt cleared".into()),
        ("view_version".into(), "2".into()),
        ("reason".into(), "superseded".into()),
    ]);
    assert!(gated_view_applied(&[applied("2")], 2, 2));
    assert!(gated_view_applied(&[applied("3"), cleared.clone()], 2, 3));
    assert!(!gated_view_applied(&[applied("3")], 2, 3));
    assert!(!gated_view_applied(&[cleared.clone()], 2, 3));
    assert!(!gated_view_applied(&[applied("1"), cleared.clone()], 2, 1));
    assert!(!gated_view_applied(&[applied("3"), cleared.clone()], 2, 2));
    cleared.insert("reason".into(), "refused".into());
    assert!(!gated_view_applied(&[applied("3"), cleared.clone()], 2, 3));
    cleared.insert("reason".into(), "superseded".into());
    cleared.insert("view_version".into(), "1".into());
    assert!(!gated_view_applied(&[applied("3"), cleared], 2, 3));
}

/// A WAL record's `type` tag, for failure messages that would be unreadable with whole records.
fn record_kind(record: &WalRecord) -> String {
    serde_json::to_value(record)
        .ok()
        .and_then(|v| v["type"].as_str().map(str::to_owned))
        .unwrap_or_else(|| "?".into())
}

/// Every `view_refused` event the WAL's advance notices carry, as `(notice seq, view_version,
/// reason)`.
fn view_refusals(records: &[WalRecord]) -> Vec<(u64, u64, String)> {
    use broca_core::compaction_provider::notice::{AdvanceNotice, Event};
    records
        .iter()
        .filter_map(|r| match r {
            WalRecord::CompactionNotice { seq, headers } => {
                Some((*seq, AdvanceNotice::from_headers(headers).ok()?))
            }
            _ => None,
        })
        .flat_map(|(seq, notice)| {
            notice
                .events
                .into_iter()
                .filter_map(move |event| match event {
                    Event::ViewRefused {
                        view_version,
                        reason,
                    } => Some((seq, view_version, reason)),
                    _ => None,
                })
        })
        .collect()
}

/// The `events` header of every advance notice in the WAL, in order: the decisions broca
/// announced on views (applied with a companion edit, deferred, refused, discarded).
fn notice_events(records: &[WalRecord]) -> Vec<String> {
    use broca_core::compaction_provider::notice::headers::EVENTS;
    records
        .iter()
        .filter_map(|r| match r {
            WalRecord::CompactionNotice { seq, headers } => headers
                .get(EVENTS)
                .map(|events| format!("seq {seq}: {events}")),
            _ => None,
        })
        .collect()
}

/// The WAL's decision on view `version`, the kind of record after which broca clears that
/// view's receipt: the view applied, or passed over because a newer one was applied (recorded
/// directly or announced in an advance notice's `applied_view_version`), or a refusal of it or a
/// newer view in an advance notice's events. `None` while the WAL records none.
fn view_decision(records: &[WalRecord], version: u64) -> Option<String> {
    use broca_core::compaction_provider::notice::AdvanceNotice;
    if let Some((seq, refused, reason)) = view_refusals(records)
        .into_iter()
        .find(|(_, refused, _)| *refused >= version)
    {
        return Some(format!(
            "advance notice seq {seq} reports view_refused:{refused}:{reason}"
        ));
    }
    records.iter().find_map(|r| match r {
        WalRecord::CompactionViewApplied { view_version, .. } if *view_version == version => {
            Some(format!("view {version} applied"))
        }
        WalRecord::CompactionViewApplied { view_version, .. } if *view_version > version => {
            Some(format!("passed over: view {view_version} applied"))
        }
        WalRecord::CompactionNotice { seq, headers } => AdvanceNotice::from_headers(headers)
            .ok()?
            .applied_view_version
            .filter(|applied| *applied >= version)
            .map(|applied| format!("advance notice seq {seq} reports view {applied} applied")),
        _ => None,
    })
}

/// The session's view versions broca's log reports as handled by its view_ready consumer, in
/// any outcome. Broca writes that line only after the receipt gate lets the delivery go.
fn handled_versions(root: &Path, session_ref: &str) -> Vec<u64> {
    compaction_log(root, session_ref)
        .iter()
        .filter(|f| f["message"] == "compaction view_ready handled")
        .filter_map(|f| f.get("view_version")?.parse().ok())
        .collect()
}

/// Waits until broca's log reports every `view_ready` of the session the rig has seen on the
/// bus as handled, and returns those versions: every delivery seen so far has passed the point
/// where the receipt gate would hold it.
async fn handled_announcements(
    root: &Path,
    bus: &bus::BusRig,
    events: &Events,
    session_ref: &str,
) -> Vec<u64> {
    broca_test_support::wait_until(
        "broca's log handling every view_ready seen on the bus so far",
        || async {
            let announced: Vec<u64> = announcements(bus, events, session_ref)
                .iter()
                .map(|a| a.view_version)
                .collect();
            let handled = handled_versions(root, session_ref);
            let pending: Vec<u64> = announced
                .iter()
                .copied()
                .filter(|v| !handled.contains(v))
                .collect();
            if pending.is_empty() {
                Ok(announced)
            } else {
                Err(format!(
                    "announced {announced:?}; not yet handled in broca's log: {pending:?}"
                ))
            }
        },
    )
    .await
}

/// The `view_ready` broca's receipt gate holds. Waits until the gate reports it was reached and
/// the rig's bus subscriber has seen an announcement that was not handled before the gate was
/// armed (`settled_before`) and that broca's log does not report as handled yet. The first such
/// announcement in bus order is the held one, because broca's consumer settles deliveries one
/// at a time in stream order. It must be newer than `applied_before`, the newest view broca had
/// applied when the gate was armed.
async fn gated_view(
    root: &Path,
    bus: &bus::BusRig,
    events: &Events,
    session_ref: &str,
    settled_before: &[u64],
    applied_before: u64,
) -> broca_core::compaction_provider::notice::ViewReady {
    let reached = root.join("broca-state/test-compaction-bus-reached");
    let held = broca_test_support::wait_until(
        &format!("a view_ready above version {applied_before} for session_ref={session_ref} held at broca's receipt gate"),
        || async {
            let announced = announcements(bus, events, session_ref);
            let handled = handled_versions(root, session_ref);
            let candidate = announced.iter().find(|a| {
                !settled_before.contains(&a.view_version) && !handled.contains(&a.view_version)
            });
            let gate = reached.exists();
            if let (true, Some(ready)) = (gate, candidate) {
                return Ok(ready.clone());
            }
            let versions: Vec<u64> = announced.iter().map(|a| a.view_version).collect();
            let early: Vec<u64> = versions
                .iter()
                .copied()
                .filter(|v| *v > applied_before && settled_before.contains(v))
                .collect();
            Err(format!(
                "gate reached={gate}; announced versions {versions:?}; handled in broca's log {handled:?}; handled before the gate was armed {settled_before:?}{}",
                if early.is_empty() {
                    String::new()
                } else {
                    format!("; Magic Context published {early:?} above version {applied_before} before the gate was armed, so only a later view can reach it")
                }
            ))
        },
    )
    .await;
    assert!(
        held.view_version > applied_before,
        "the receipt gate held view_ready version {}, which is not above version {applied_before}, the newest view broca had applied when the gate was armed: {held:?}",
        held.view_version
    );
    held
}

/// Proof, taken while the receipt gate holds the ACK of `held`, that broca made the view
/// durable before acknowledging it: a receipt for exactly `(session_ref, view_version)` in
/// broca's store, or a decision on that version in its WAL. The idle boundary can decide a view
/// from its durable receipt while the ACK is held, and a decision clears the receipt, so
/// either is the proof. Broca makes the decision durable in the WAL before it clears the
/// receipt, and the receipt is read first, so one snapshot cannot miss both.
fn receipt_or_decision(
    live: &Live,
    subject: &BrocaSubject,
    held: &broca_core::compaction_provider::notice::ViewReady,
) -> String {
    let receipts = broca_store::SqliteStore::open(live.state_root.join("run-index.db"))
        .unwrap()
        .compaction_receipts("", 256)
        .unwrap();
    if let Some(receipt) = receipts.iter().find(|r| {
        r.ready.session_ref == held.session_ref && r.ready.view_version == held.view_version
    }) {
        return format!("durable receipt {receipt:?}");
    }
    let records = subject.records(live, SESSION);
    if let Some(decision) = view_decision(&records, held.view_version) {
        return format!("receipt already cleared; the WAL records the decision: {decision}");
    }
    panic!(
        "view_ready version {} for session_ref={} must have a durable receipt before ACK, or a recorded decision: with its ACK held, broca's store has no receipt for that version and its WAL records no decision on it; receipts in the store: {receipts:?}; newest applied view: {}; advance notice events: {:?}",
        held.view_version,
        held.session_ref,
        highest_applied(&records),
        notice_events(&records)
    );
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

/// Magic Context worker-preset (mode 3) drop contract.
///
/// MC's investigation `mode3-v1-partial-drops.md` ("What BROCA's test should check
/// instead") found the old check wrong: the first gated view (v1) not holding every
/// stamped drop is by design. A stamped drop stays queued while its tool output sits
/// inside the persisted tool-tag protection window, and the worker preset applies it
/// on an execute pass once newer tool output pushes it out. This checker asserts
/// MC's four rules over every view broca applied, in WAL order:
///
/// 1. A drop outside the window lands on the first view after its stamp.
/// 2. A drop inside the window lands on the first execute pass after newer tool
///    output pushes it out (so it may be absent from early views).
/// 3. No view ever brings a dropped part back: once an edit or removal of a part
///    appears in view N, every later view carries it with identical bytes.
/// 4. Either legal drop shape counts: a removed arc (`replace` with `"[]"`) or a
///    skeleton (`keep` plus a `[dropped]` part edit). The engine picks the shape,
///    so the check never requires one shape for every tag.
///
/// The timing half (rules 1-2) replays MC's window from the test's own known
/// output sizes: every read output costs MC_READ_OUTPUT_TOKENS and the
/// ctx_reduce result MC_REDUCE_RESULT_TOKENS (both MC-measured in the report's
/// timeline table), and a drop is out once newer covered outputs reach
/// MC_PROTECTED_TOKENS across at least MC_PROTECTED_NEWEST_GROUPS groups. A view
/// covering below the stamp result was built before the stamp and is exempt from
/// the timing half (rules 1-2) while still bound by no-resurrection (rule 3).
/// That coverage set matches the tag rows the build's window walked: both
/// derive from the same notice stream (validated against the preserved run
/// replayed by `mc_drops_replay_run_1115b`).
///
/// The window's group leg (three newest tool-tag groups) and token leg
/// (newer output reaching `protected_tokens`) come from the MC report section
/// cited on each constant. A drop counts as released only when both legs agree
/// it is out, matching the report's `min` cutoff.
/// The context window the rig configures (the test-compaction `config` file
/// writes 128000; the MC report calls it the 128k window). Only the floor
/// below enters the window math; the debug assertion at the checker's entry
/// pins the reclaim.md:38 relation between the two, so a future window change
/// updates the floor instead of silently aging it.
const MC_CONTEXT_WINDOW_TOKENS: u64 = 128_000;
/// `protected_tokens_effective` in the MC report: the documented default for a
/// 128k window, `round(0.08 * window)` clamped as in reclaim.md:38
/// (`clamp(6400, min(16000, 10240), 64000)` is 10240).
const MC_PROTECTED_TOKENS: u64 = 10_240;
/// Each read output's cost in the MC report timeline table: every read output
/// row carries `token_count` 3505. The rig's outputs all share one shape
/// (`tool_output` repeats one line 700 times), so one measured size fits all.
const MC_READ_OUTPUT_TOKENS: u64 = 3_505;
/// The ctx_reduce result's cost in the same table (tag 11, `token_count` 5).
const MC_REDUCE_RESULT_TOKENS: u64 = 5;
/// The window's group leg: the three newest tool-tag groups (MC report,
/// `ProtectionWindow::from_persisted_rows_calibrated`).
const MC_PROTECTED_NEWEST_GROUPS: usize = 3;

/// One stamped drop: the read whose tool output the model's `ctx_reduce` call
/// named, and where the replayed transcript holds it.
struct StampedDrop {
    read: usize,
    part: String,
    ordinal: u64,
    marker: String,
}

/// One tool output for the window walk, in transcript order (the prompt is
/// append-only, so transcript order is mint order).
struct WindowOutput {
    ordinal: u64,
    tokens: u64,
}

/// How far a stamped drop is from leaving the window under a view covering
/// through `covers_through`.
struct ReleaseState {
    newer_tokens: u64,
    newer_groups: usize,
    released: bool,
}

/// Whether `drop` had left the protection window by the time a view covering
/// through `covers_through` was built: newer covered outputs reach
/// MC_PROTECTED_TOKENS across at least MC_PROTECTED_NEWEST_GROUPS groups.
fn mc_release_state(
    covers_through: u64,
    drop: &StampedDrop,
    outputs: &[WindowOutput],
) -> ReleaseState {
    let mut newer_tokens = 0u64;
    let mut newer_groups = 0usize;
    for output in outputs {
        if output.ordinal > drop.ordinal && output.ordinal <= covers_through {
            newer_tokens += output.tokens;
            newer_groups += 1;
        }
    }
    ReleaseState {
        newer_tokens,
        newer_groups,
        released: newer_tokens >= MC_PROTECTED_TOKENS && newer_groups >= MC_PROTECTED_NEWEST_GROUPS,
    }
}

/// How one applied view drops one stamped part: either legal shape from MC's
/// report. A part that is gone without either shape still counts as gone, but
/// carries no evidence for the identical-bytes half of rule 3.
#[derive(Debug, Clone, PartialEq, Eq)]
enum DropEvidence {
    /// A keep-range edit of the part: `drop: true` removes it outright, and
    /// `bytes` replaces it (the worker skeleton writes `[dropped]`).
    PartEdit { drop: bool, bytes: Option<String> },
    /// A replace range over the part's message holding no messages: the arc
    /// removed whole (`"[]"`).
    ArcRemoved { from: u64, to: u64, bytes: String },
}

/// The evidence with which `view` drops `drop`, when it carries either legal
/// shape over the part.
fn drop_evidence(
    view: &broca_core::compaction_provider::view::CompactionView,
    drop: &StampedDrop,
) -> Option<DropEvidence> {
    use broca_core::compaction_provider::view::ViewRange;
    for range in &view.ranges {
        match range {
            ViewRange::Keep { from, to, edits } if *from <= drop.ordinal && drop.ordinal < *to => {
                if let Some(edit) = edits.iter().find(|edit| edit.part == drop.part) {
                    return Some(DropEvidence::PartEdit {
                        drop: edit.drop,
                        bytes: edit.bytes.clone(),
                    });
                }
            }
            ViewRange::Replace { from, to, bytes }
                if *from <= drop.ordinal && drop.ordinal < *to =>
            {
                let empty = serde_json::from_str::<Vec<serde_json::Value>>(bytes)
                    .is_ok_and(|messages| messages.is_empty());
                if empty {
                    return Some(DropEvidence::ArcRemoved {
                        from: *from,
                        to: *to,
                        bytes: bytes.clone(),
                    });
                }
            }
            _ => {}
        }
    }
    None
}

/// Rules 1-4 over applied views in WAL order. Returns a one-line summary for
/// the run report. `views` holds `(view_version, view)` in the order broca
/// applied them; `stamp_ordinal` is the ordinal of the `ctx_reduce` result.
fn check_mc_worker_drops(
    prompt: &[broca_protocol::Message],
    drops: &[StampedDrop],
    outputs: &[WindowOutput],
    stamp_ordinal: u64,
    views: &[(u64, broca_core::compaction_provider::view::CompactionView)],
) -> Result<String, String> {
    debug_assert_eq!(
        MC_PROTECTED_TOKENS,
        (MC_CONTEXT_WINDOW_TOKENS * 8 / 100)
            .clamp(6_400, 64_000)
            .min(16_000),
        "the protected floor follows reclaim.md:38 for this rig's window"
    );
    if views.is_empty() {
        return Err("no applied views to check the stamped drops against".to_owned());
    }
    if drops.is_empty() {
        return Err("no stamped drops to check the applied views against".to_owned());
    }
    for pair in views.windows(2) {
        if pair[0].0 >= pair[1].0 {
            return Err(format!(
                "applied view versions out of order: v{} then v{}",
                pair[0].0, pair[1].0
            ));
        }
    }
    let holds = |messages: &[broca_protocol::Message], marker: &str| {
        messages
            .iter()
            .any(|m| serde_json::to_string(m).unwrap().contains(marker))
    };
    let mut assembled: Vec<Vec<broca_protocol::Message>> = Vec::with_capacity(views.len());
    for (version, view) in views {
        match broca_core::compaction::view::assemble(view, prompt) {
            Ok(history) => assembled.push(history),
            Err(problem) => {
                return Err(format!(
                    "broca cannot assemble Magic Context's view v{version} over the replayed transcript: {problem:?}"
                ));
            }
        }
    }
    let gone = |index: usize, drop: &StampedDrop| !holds(&assembled[index], &drop.marker);
    // Rule 3: no view ever brings a dropped part back, with identical bytes.
    for drop in drops {
        let mut first_gone: Option<u64> = None;
        let mut first_evidence: Option<(u64, DropEvidence)> = None;
        for (index, (version, view)) in views.iter().enumerate() {
            if gone(index, drop) {
                if first_gone.is_none() {
                    first_gone = Some(*version);
                }
                if let Some(evidence) = drop_evidence(view, drop) {
                    match &first_evidence {
                        Some((first_version, first)) if *first != evidence => {
                            return Err(format!(
                                "view v{version} changes how stamped part {} (read {}) drops: v{first_version} carried {first:?}, now {evidence:?}; no view ever brings a dropped part back, and every later view carries it with identical bytes",
                                drop.part, drop.read
                            ));
                        }
                        Some(_) => {}
                        None => first_evidence = Some((*version, evidence)),
                    }
                } else if let Some((first_version, first)) = &first_evidence {
                    return Err(format!(
                        "view v{version} drops the evidence v{first_version} carried for stamped part {} (read {}): {first:?}; every later view carries it with identical bytes",
                        drop.part, drop.read
                    ));
                }
            } else if let Some(first_version) = first_gone {
                return Err(format!(
                    "view v{version} still carries stamped part {} (read {}), gone since v{first_version}; no view ever brings a dropped part back",
                    drop.part, drop.read
                ));
            }
        }
    }
    // Rules 1-2: a drop the view's build had already pushed out of the window
    // is gone from it. A drop still inside the window may be held, so no
    // presence is required there: that hold is the late-inside case the old
    // check mistook for a defect.
    for (index, (version, view)) in views.iter().enumerate() {
        let covers = view.covers_through.ordinal;
        if covers < stamp_ordinal {
            continue;
        }
        for drop in drops {
            let state = mc_release_state(covers, drop, outputs);
            if state.released && !gone(index, drop) {
                return Err(format!(
                    "view v{version} (covers {covers}) still carries stamped part {} (read {}): {} newer tool-output tokens in {} groups reach the window ({MC_PROTECTED_TOKENS} tokens, {} newest groups), so the drop had left the window before this build; an outside drop lands on the first view after its stamp",
                    drop.part,
                    drop.read,
                    state.newer_tokens,
                    state.newer_groups,
                    MC_PROTECTED_NEWEST_GROUPS
                ));
            }
        }
    }
    // Every dropped part gone from the newest applied view.
    let (newest, _) = views.last().unwrap();
    let last = assembled.len() - 1;
    for drop in drops {
        if !gone(last, drop) {
            return Err(format!(
                "newest applied view v{newest} still carries stamped part {} (read {}); every dropped part is gone from the newest applied view",
                drop.part, drop.read
            ));
        }
    }
    let mut summary = format!(
        "checked {} applied views against {} stamped drops",
        views.len(),
        drops.len()
    );
    for drop in drops {
        let first = views
            .iter()
            .enumerate()
            .find(|(index, _)| gone(*index, drop))
            .map(|(_, (version, _))| version.to_string())
            .unwrap_or_else(|| "never".to_owned());
        summary.push_str(&format!(
            " read {} ({}): first gone v{first};",
            drop.read, drop.part
        ));
    }
    Ok(summary)
}

/// Replay the WAL into the checker's inputs: the stamped drops with their part
/// keys, the tool outputs for the window walk in transcript (mint) order, the
/// ordinal of the `ctx_reduce` result, and every applied view in WAL order.
/// The transcript is the one the WAL replays, with ordinals stamped as broca
/// stamps them; the raw transcript must hold every stamped output first, so the
/// check can fail.
#[allow(clippy::type_complexity)]
fn mc_worker_drop_inputs(
    records: &[WalRecord],
    dropped: &[usize],
    drop_tags: &str,
) -> (
    Vec<broca_protocol::Message>,
    Vec<StampedDrop>,
    Vec<WindowOutput>,
    u64,
    Vec<(u64, broca_core::compaction_provider::view::CompactionView)>,
) {
    use broca_protocol::ContentBlock;
    let mut prompt = broca_core::replay::replay(records)
        .expect("the session's WAL must replay")
        .prompt;
    broca_protocol::content::stamp_ordinals(&mut prompt);
    let holds = |messages: &[broca_protocol::Message], read: usize| {
        let marker = format!("Tool output {read}:");
        messages
            .iter()
            .any(|m| serde_json::to_string(m).unwrap().contains(&marker))
    };
    // Each dropped read's part key in the transcript, to name it in a failure.
    let drops: Vec<StampedDrop> = dropped
        .iter()
        .map(|&read| {
            let call = format!("read_{read}");
            let found = prompt.iter().find_map(|message| {
                let index = message.content.iter().position(|block| {
                    matches!(block, ContentBlock::ToolResult(result) if result.tool_call_id.0 == call)
                })?;
                let ordinal = message.ordinal?;
                Some((ordinal, broca_protocol::tags::part_key(ordinal, index)))
            });
            let (ordinal, part) = found.unwrap_or_else(|| {
                panic!("the replayed transcript holds no tool result for call {call}")
            });
            assert!(
                holds(&prompt, read),
                "the replayed transcript does not hold tool output {read} at part {part}"
            );
            StampedDrop {
                read,
                part,
                ordinal,
                marker: format!("Tool output {read}:"),
            }
        })
        .collect();
    let mut outputs = Vec::new();
    let mut stamp_ordinal = None;
    for message in &prompt {
        for block in &message.content {
            if let ContentBlock::ToolResult(result) = block {
                let ordinal = message.ordinal.expect("stamped prompt");
                let id = result.tool_call_id.0.as_str();
                if id == "reduce_old_outputs" {
                    stamp_ordinal = Some(ordinal);
                    outputs.push(WindowOutput {
                        ordinal,
                        tokens: MC_REDUCE_RESULT_TOKENS,
                    });
                } else if id.starts_with("read_") && id["read_".len()..].parse::<usize>().is_ok() {
                    outputs.push(WindowOutput {
                        ordinal,
                        tokens: MC_READ_OUTPUT_TOKENS,
                    });
                } else {
                    panic!(
                        "the window walk meets an unexpected tool result {id}; it only knows the rig's reads and the ctx_reduce result"
                    );
                }
            }
        }
    }
    let stamp_ordinal = stamp_ordinal.unwrap_or_else(|| {
        panic!("the replayed transcript holds no ctx_reduce result for tags {drop_tags}")
    });
    let views: Vec<(u64, broca_core::compaction_provider::view::CompactionView)> = records
        .iter()
        .filter_map(|record| match record {
            WalRecord::CompactionViewApplied {
                provider,
                view_version,
                view: raw,
                ..
            } => {
                assert_eq!(
                    provider, "magic-context",
                    "the session compacts only through Magic Context: {record:?}"
                );
                let decoded: broca_core::compaction_provider::view::CompactionView =
                    serde_json::from_str(raw.as_json_text()).unwrap_or_else(|error| {
                        panic!("broca recorded an undecodable view v{view_version}: {error}")
                    });
                Some((*view_version, decoded))
            }
            _ => None,
        })
        .collect();
    (prompt, drops, outputs, stamp_ordinal, views)
}

/// The worker-preset drop check for the real-MC lane: MC's mode-3 rules over
/// every view broca applied, in WAL order.
fn assert_mc_worker_drops(root: &Path, records: &[WalRecord], dropped: &[usize], drop_tags: &str) {
    let (prompt, drops, outputs, stamp_ordinal, views) =
        mc_worker_drop_inputs(records, dropped, drop_tags);
    match check_mc_worker_drops(&prompt, &drops, &outputs, stamp_ordinal, &views) {
        Ok(summary) => note(root, format!("Worker drops {drop_tags}: {summary}")),
        Err(problem) => panic!(
            "Magic Context's worker view breaks the mode-3 drop contract for tags {drop_tags}: {problem}"
        ),
    }
}

/// A synthetic transcript for the checker unit tests: a user message, three
/// read outputs, the ctx_reduce result, then two more reads, so one early drop
/// needs three newer reads to leave the window, exactly like the real lane.
fn synth_prompt() -> Vec<broca_protocol::Message> {
    use broca_protocol::ids::ToolCallId;
    use broca_protocol::{ContentBlock, Message, Role, ToolOutput, ToolResult};
    let mut prompt = vec![Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "synthetic start".into(),
        }],
    )];
    let read_output = |read: usize| {
        Message::new(
            Role::Tool,
            vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: ToolCallId(format!("read_{read}")),
                output: ToolOutput::Text {
                    text: format!("Synth output {read}: {}", "evidence line. ".repeat(10)),
                },
                is_error: false,
            })],
        )
    };
    for read in 0..3 {
        prompt.push(read_output(read));
    }
    prompt.push(Message::new(
        Role::Tool,
        vec![ContentBlock::ToolResult(ToolResult {
            tool_call_id: ToolCallId("reduce_old_outputs".into()),
            output: ToolOutput::Text {
                text: "2 tags stamped. ".into(),
            },
            is_error: false,
        })],
    ));
    for read in 3..5 {
        prompt.push(read_output(read));
    }
    broca_protocol::content::stamp_ordinals(&mut prompt);
    prompt
}

/// The checker's inputs over [`synth_prompt`] for `dropped` reads: part keys
/// and ordinals scanned from the stamped prompt, the window walk, and the
/// stamp ordinal of the ctx_reduce result.
fn synth_inputs(
    dropped: &[usize],
) -> (
    Vec<broca_protocol::Message>,
    Vec<StampedDrop>,
    Vec<WindowOutput>,
    u64,
) {
    use broca_protocol::ContentBlock;
    let prompt = synth_prompt();
    let drops: Vec<StampedDrop> = dropped
        .iter()
        .map(|&read| {
            let call = format!("read_{read}");
            let (ordinal, part) = prompt
                .iter()
                .find_map(|message| {
                    let index = message.content.iter().position(|block| {
                        matches!(block, ContentBlock::ToolResult(result) if result.tool_call_id.0 == call)
                    })?;
                    let ordinal = message.ordinal?;
                    Some((ordinal, broca_protocol::tags::part_key(ordinal, index)))
                })
                .expect("synthetic prompt holds every read");
            StampedDrop {
                read,
                part,
                ordinal,
                marker: format!("Synth output {read}:"),
            }
        })
        .collect();
    let mut outputs = Vec::new();
    let mut stamp_ordinal = None;
    for message in &prompt {
        for block in &message.content {
            if let ContentBlock::ToolResult(result) = block {
                let ordinal = message.ordinal.expect("stamped prompt");
                if result.tool_call_id.0 == "reduce_old_outputs" {
                    stamp_ordinal = Some(ordinal);
                    outputs.push(WindowOutput {
                        ordinal,
                        tokens: MC_REDUCE_RESULT_TOKENS,
                    });
                } else {
                    outputs.push(WindowOutput {
                        ordinal,
                        tokens: MC_READ_OUTPUT_TOKENS,
                    });
                }
            }
        }
    }
    (
        prompt,
        drops,
        outputs,
        stamp_ordinal.expect("synthetic prompt holds the stamp"),
    )
}

/// A synthetic applied view: `ranges` must tile `[0, covers_through + 1)`.
fn synth_view(
    version: u64,
    covers_through: u64,
    ranges: Vec<broca_core::compaction_provider::view::ViewRange>,
) -> (u64, broca_core::compaction_provider::view::CompactionView) {
    use broca_core::compaction_provider::lineage::MessageRef;
    use broca_core::compaction_provider::view::CompactionView;
    (
        version,
        CompactionView::new(
            "synth",
            version,
            "synth-lineage",
            MessageRef {
                ordinal: covers_through,
                mid: format!("m{covers_through}"),
            },
            ranges,
        ),
    )
}

/// The skeleton bytes for `call_id`: the tool-result part with its output
/// replaced by `[dropped]`, the shape the worker serves for a recent output.
fn synth_skeleton_bytes(call_id: &str) -> String {
    serde_json::to_string(&serde_json::json!({
        "is_error": false,
        "output": {"kind": "text", "text": "[dropped]"},
        "tool_call_id": call_id,
        "type": "tool_result",
    }))
    .unwrap()
}

/// A drop that lands late inside the window passes: v1 still holds the part
/// (only two newer reads plus the tiny stamp result are covered, 7,015 tokens
/// under the 10,240 floor), and v2 carries the drop once four newer reads are
/// covered.
#[test]
fn mc_drops_late_inside_window_passes() {
    use broca_core::compaction_provider::view::{PartEdit, ViewRange};
    let (prompt, drops, outputs, stamp_ordinal) = synth_inputs(&[0]);
    let views = vec![
        synth_view(1, 4, vec![ViewRange::keep(0, 5)]),
        synth_view(
            2,
            6,
            vec![ViewRange::keep_with_edits(
                0,
                7,
                vec![PartEdit::replace(
                    1,
                    "m1",
                    drops[0].part.clone(),
                    synth_skeleton_bytes("read_0"),
                )],
            )],
        ),
    ];
    let summary = check_mc_worker_drops(&prompt, &drops, &outputs, stamp_ordinal, &views)
        .expect("a late drop inside the window must pass");
    assert!(
        summary.contains("read 0"),
        "summary names the drop: {summary}"
    );
}

/// A drop that lands late outside the window fails: v1 already covers four
/// newer reads, so the part must be gone from it.
#[test]
fn mc_drops_late_outside_window_fails() {
    use broca_core::compaction_provider::view::ViewRange;
    let (prompt, drops, outputs, stamp_ordinal) = synth_inputs(&[0]);
    let views = vec![synth_view(1, 6, vec![ViewRange::keep(0, 7)])];
    let problem = check_mc_worker_drops(&prompt, &drops, &outputs, stamp_ordinal, &views)
        .expect_err("a late drop outside the window must fail");
    assert!(
        problem.contains("had left the window"),
        "the failure names the missed timing: {problem}"
    );
}

/// A part that is dropped and then reappears fails, even though the newest
/// view is the one that carries it again.
#[test]
fn mc_drops_reappearing_part_fails() {
    use broca_core::compaction_provider::view::{PartEdit, ViewRange};
    let (prompt, drops, outputs, stamp_ordinal) = synth_inputs(&[0]);
    let views = vec![
        synth_view(
            1,
            6,
            vec![ViewRange::keep_with_edits(
                0,
                7,
                vec![PartEdit::replace(
                    1,
                    "m1",
                    drops[0].part.clone(),
                    synth_skeleton_bytes("read_0"),
                )],
            )],
        ),
        synth_view(2, 6, vec![ViewRange::keep(0, 7)]),
    ];
    let problem = check_mc_worker_drops(&prompt, &drops, &outputs, stamp_ordinal, &views)
        .expect_err("a reappearing part must fail");
    assert!(
        problem.contains("brings a dropped part back"),
        "the failure names rule 3: {problem}"
    );
}

/// Both legal shapes pass together: a removed arc for one part and a skeleton
/// edit for the other, both released by the same coverage.
#[test]
fn mc_drops_accepts_both_shapes() {
    use broca_core::compaction_provider::view::{PartEdit, ViewRange};
    let (prompt, drops, outputs, stamp_ordinal) = synth_inputs(&[0, 1]);
    let views = vec![synth_view(
        1,
        6,
        vec![
            ViewRange::keep(0, 1),
            ViewRange::replace(1, 2, "[]"),
            ViewRange::keep_with_edits(
                2,
                3,
                vec![PartEdit::replace(
                    2,
                    "m2",
                    drops[1].part.clone(),
                    synth_skeleton_bytes("read_1"),
                )],
            ),
            ViewRange::keep(3, 7),
        ],
    )];
    check_mc_worker_drops(&prompt, &drops, &outputs, stamp_ordinal, &views)
        .expect("both legal drop shapes must pass");
}

/// The identical-bytes half of rule 3: the same part dropped two ways across
/// views fails, even though it never reappears.
#[test]
fn mc_drops_changed_bytes_fail() {
    use broca_core::compaction_provider::view::{PartEdit, ViewRange};
    let (prompt, drops, outputs, stamp_ordinal) = synth_inputs(&[0]);
    let views = vec![
        synth_view(
            1,
            6,
            vec![ViewRange::keep_with_edits(
                0,
                7,
                vec![PartEdit::replace(
                    1,
                    "m1",
                    drops[0].part.clone(),
                    synth_skeleton_bytes("read_0"),
                )],
            )],
        ),
        synth_view(
            2,
            6,
            vec![ViewRange::keep_with_edits(
                0,
                7,
                vec![PartEdit::replace(
                    1,
                    "m1",
                    drops[0].part.clone(),
                    synth_skeleton_bytes("read_0").replace("[dropped]", "[dropped again]"),
                )],
            )],
        ),
    ];
    let problem = check_mc_worker_drops(&prompt, &drops, &outputs, stamp_ordinal, &views)
        .expect_err("changed drop bytes must fail");
    assert!(
        problem.contains("identical bytes"),
        "the failure names the identical-bytes rule: {problem}"
    );
}

/// Offline replay of a preserved Magic Context end-to-end WAL through the new
/// checker: run 1115b is the failed lane run whose first view carried only one
/// of the two stamped drops, failing the old check. Point
/// `BROCA_MC_E2E_REPLAY_ROOT` at the preserved run directory (which holds
/// `wal-records.json`, written by the lane's evidence hook) and run
/// `mc_e2e::mc_drops_replay_run_1115b`. The run's three applied views must
/// satisfy the contract, and removing the newest view's skeleton edit must
/// fail the check, proving the timing half can fail. Skipped without the
/// directory: the 467 KB WAL carries operator-local paths, so it is not a
/// committed fixture.
#[test]
fn mc_drops_replay_run_1115b() {
    use broca_core::compaction_provider::view::ViewRange;
    let Some(root) = std::env::var_os("BROCA_MC_E2E_REPLAY_ROOT") else {
        println!("SKIP mc_drops_replay_run_1115b: BROCA_MC_E2E_REPLAY_ROOT is unset");
        return;
    };
    let bytes = std::fs::read(PathBuf::from(root).join("wal-records.json"))
        .expect("the replay root must hold wal-records.json");
    let values: Vec<Value> = serde_json::from_slice(&bytes).expect("wal-records.json must be JSON");
    let records: Vec<WalRecord> = values
        .into_iter()
        .map(|value| serde_json::from_value(value).expect("wal-records.json must hold WAL records"))
        .collect();
    let (prompt, drops, outputs, stamp_ordinal, views) =
        mc_worker_drop_inputs(&records, &[0, 1], "2,5");
    check_mc_worker_drops(&prompt, &drops, &outputs, stamp_ordinal, &views)
        .expect("run 1115b satisfies the mode-3 drop contract");
    let mut mutated = views.clone();
    let (_newest_version, newest) = mutated.last_mut().expect("run 1115b applied views");
    let mut removed = 0;
    for range in &mut newest.ranges {
        if let ViewRange::Keep { edits, .. } = range {
            let before = edits.len();
            edits.retain(|edit| !drops.iter().any(|drop| edit.part == drop.part));
            removed += before - edits.len();
        }
    }
    assert!(
        removed > 0,
        "the mutation must remove the newest view's drop edits"
    );
    let problem = check_mc_worker_drops(&prompt, &drops, &outputs, stamp_ordinal, &mutated)
        .expect_err("removing the newest view's drop edit must fail");
    assert!(
        problem.contains("still carries"),
        "the failure names the carried part: {problem}"
    );
}
