//! Real daemon exec and same-process Broca resume. The daemon image belongs to one case.

use super::*;
use sha2::{Digest, Sha256};
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::sync::atomic::{AtomicBool, Ordering};
use subc_protocol::{Flags, Frame, FrameType, Priority};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UnixListener, UnixStream};

const WAIT_FOR_EXEC: Duration = Duration::from_secs(40);
const AUTH: Duration = Duration::from_secs(2);
const TARGET: &str = "BROCA_UPGRADE_SUBC_CORE_BIN";
/// Set by the native release lane that names SUBC's signed test-support daemon. CI's
/// `BROCA_REQUIRE_SUBC` can't require it: CI builds an ordinary ck-subc, and the daemon refuses an
/// upgrade candidate signed by another team, so no CI-built daemon can stand in for it.
const REQUIRE_TARGET: &str = "BROCA_REQUIRE_UPGRADE_SUBC";

pub(crate) struct DaemonUpgradeRig {
    root: PathBuf,
    pub(crate) executable: PathBuf,
    port: u16,
    delay_resume_hello: bool,
    printed_failure: AtomicBool,
}

impl DaemonUpgradeRig {
    fn new(case: char, delay_resume_hello: bool) -> Option<Self> {
        if !rig_ready() {
            return None;
        }
        let Some(binary) = std::env::var_os(TARGET)
            .map(PathBuf::from)
            .filter(|path| path.is_file())
        else {
            assert!(
                std::env::var_os(REQUIRE_TARGET).is_none(),
                "{TARGET} must name a ck-subc build with test hooks"
            );
            eprintln!("skipping the daemon upgrade case: {TARGET} names no ck-subc test build");
            return None;
        };
        assert!(
            !delay_resume_hello || cfg!(feature = "test-hooks"),
            "the missed-resume native case requires broca-module-serve --features test-hooks"
        );
        // subc's local socket and the freeze checkpoint must fit sockaddr_un on macOS.
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = PathBuf::from(format!("/tmp/bup-{}-{case}-{nonce:x}", std::process::id()));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&root)
            .unwrap();
        let run = root.join("data-home/cortexkit/run");
        std::fs::create_dir_all(&run).unwrap();
        std::fs::set_permissions(&run, std::fs::Permissions::from_mode(0o700)).unwrap();
        let executable = root.join("ckdev-subc-upgrade");
        std::fs::copy(binary, &executable).unwrap();
        std::fs::write(root.join("approve"), b"approve\n").unwrap();
        let port = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        Some(Self {
            root,
            executable,
            port,
            delay_resume_hello,
            printed_failure: AtomicBool::new(false),
        })
    }

    pub(crate) fn configure(&self, command: &mut tokio::process::Command, connection: &Path) {
        command
            .arg("--config-store-root")
            .arg(self.root.join("authority"))
            .env("SUBC_TEST_CONFIG_GATE", "1")
            .env("SUBC_CONNECTION_FILE", connection)
            .env("SUBC_CGROUP_PLACEMENT", "disabled")
            .env("SUBC_PORT", self.port.to_string())
            .env("SUBC_TEST_OPERATOR_SCRIPT", self.root.join("approve"))
            .env_remove("SUBC_TEST_RESUME_PAUSE_MODULE")
            .env_remove("SUBC_TEST_UPGRADE_FREEZE_GATE");
        if !self.delay_resume_hello {
            command.env("SUBC_TEST_UPGRADE_FREEZE_GATE", self.root.join("freeze"));
        }
    }

    pub(crate) fn configure_broca(&self, env: &mut Value) {
        env["BROCA_TEST_HOOKS_RESUME_HELLO_DELAY_V1"] =
            json!(if self.delay_resume_hello { "1" } else { "0" });
    }

    fn stage(&self) -> String {
        let stage = self.root.join("replacement");
        std::fs::copy(&self.executable, &stage).unwrap();
        let mut digest = Sha256::new();
        std::io::copy(&mut std::fs::File::open(&stage).unwrap(), &mut digest).unwrap();
        std::fs::rename(stage, &self.executable).unwrap();
        format!("{:x}", digest.finalize())
    }

    async fn issue(&self, live: &Live) -> tokio::task::JoinHandle<(FrameType, Value)> {
        let checksum = self.stage();
        let path = live.connection_file.clone();
        let port = self.port;
        tokio::spawn(async move {
            tokio::time::timeout(
                WAIT_FOR_EXEC,
                control(
                    &path,
                    port,
                    json!({"op":"daemon.upgrade_in_place", "executable_sha256":checksum}),
                ),
            )
            .await
            .expect("daemon upgrade did not return its receipt")
            .unwrap()
        })
    }

    async fn resumed(&self, live: &Live) {
        tokio::time::timeout(WAIT_FOR_EXEC, async {
            loop {
                if let Ok((_, report)) = control(
                    &live.connection_file,
                    self.port,
                    json!({"op":"server.describe"}),
                )
                .await
                {
                    let upgrade = &report["counters"]["last_upgrade"];
                    if upgrade["outcome"] == "upgraded" {
                        assert!(
                            upgrade["modules"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .any(|entry| entry["id"] == "broca"
                                    && entry["disposition"] == "resumed"),
                            "{upgrade}"
                        );
                        return;
                    }
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("daemon did not report Broca resumed");
    }

    fn print_failure(&self) {
        if self.printed_failure.swap(true, Ordering::Relaxed) {
            return;
        }
        eprintln!(
            "retaining the failed daemon upgrade rig at {}",
            self.root.display()
        );
        let logs = self.root.join("data-home/cortexkit/broca/logs");
        match diagnostic_logs(&logs) {
            Ok(tail) if !tail.is_empty() => eprintln!("--- Broca's own log tail ---\n{tail}"),
            Ok(_) => eprintln!(
                "--- Broca's own log tail: {} (no log segments) ---",
                logs.display()
            ),
            Err(error) => eprintln!("--- Broca's own log tail: {} ({error}) ---", logs.display()),
        }
        broca_test_support::dump_rig_process_logs_on_failure(&self.root, "daemon");
    }
}

impl Drop for DaemonUpgradeRig {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.print_failure();
        } else {
            broca_test_support::remove_rig_dir(&self.root);
        }
    }
}

// Declare after `Live` so the tail is captured before teardown can add its own shutdown lines.
struct FailureLogGuard<'a>(&'a DaemonUpgradeRig);

impl Drop for FailureLogGuard<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.print_failure();
        }
    }
}

async fn authenticated_control(path: &Path, port: u16) -> std::io::Result<TcpStream> {
    let conn = subc_transport::read_for_client(path).map_err(std::io::Error::other)?;
    let mut stream = TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port)).await?;
    subc_transport::authenticate_client(&mut stream, &conn, AUTH)
        .await
        .map_err(std::io::Error::other)?;
    Ok(stream)
}

async fn control(path: &Path, port: u16, request: Value) -> std::io::Result<(FrameType, Value)> {
    control_on(&mut authenticated_control(path, port).await?, request).await
}

async fn control_on(stream: &mut TcpStream, request: Value) -> std::io::Result<(FrameType, Value)> {
    let frame = Frame::build(
        FrameType::Request,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        501,
        serde_json::to_vec(&request)?,
    )
    .unwrap();
    subc_transport::write_frame(stream, &frame)
        .await
        .map_err(std::io::Error::other)?;
    let reply = subc_transport::read_frame(stream)
        .await
        .map_err(std::io::Error::other)?
        .ok_or_else(|| std::io::Error::other("daemon closed before answering"))?;
    assert_eq!(reply.header.corr, 501);
    Ok((reply.header.ty, serde_json::from_slice(&reply.body)?))
}

async fn checkpoint(listener: &UnixListener, expected: u8) -> UnixStream {
    let (mut stream, _) = tokio::time::timeout(WAIT_FOR_EXEC, listener.accept())
        .await
        .expect("daemon did not reach its upgrade checkpoint")
        .unwrap();
    assert_eq!(stream.read_u8().await.unwrap(), expected);
    stream
}

fn turns(calls: impl IntoIterator<Item = ScriptedToolCall>) -> Script {
    let mut turns: Vec<_> = calls
        .into_iter()
        .map(|call| turn(vec![ScriptedPart::ToolCall(call)]))
        .collect();
    turns.push(turn(vec![ScriptedPart::Text(
        "done after the upgrade".into(),
    )]));
    Script { turns }
}

fn turn(parts: Vec<ScriptedPart>) -> ScriptedTurn {
    ScriptedTurn::new(parts).with_usage(cortexkit_role_llm_runner_conformance::ScriptedUsage::new(
        1, 0, 0, 1,
    ))
}

async fn subscribe(route: &BrocaRoute, from: Value) -> broca_subc::SubscribeStream {
    let handle = route
        .connection
        .route_open_management("broca", &route.session)
        .await
        .unwrap();
    route
        .connection
        .subscribe(
            &handle,
            serde_json::to_vec(&json!({"method":"session.subscribe", "params":{"from":from}}))
                .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn daemon_upgrade_preserves_broca_run_feed_and_subscription_cursor() {
    let Some(rig) = DaemonUpgradeRig::new('a', false) else {
        return;
    };
    let case = HeldCase::new("upgrade-cut-call").await;
    let fresh = ScriptedToolCall {
        tool_call_id: "fresh_call".into(),
        arguments: json!({"marker":"fresh after switch"}),
        result: json!("fresh tool answered"),
        ..case.call.clone()
    };
    let held = ScriptedToolCall {
        tool_call_id: "late_call".into(),
        arguments: json!({"marker":"held after switch"}),
        ..case.call.clone()
    };
    install_held_reply(
        &case.subject.shared,
        case.session,
        &held.arguments,
        "running",
    );
    case.subject
        .shared
        .held_feed
        .lock()
        .unwrap()
        .cut_replies
        .insert(case.call.arguments.to_string());
    case.subject
        .install_script(
            case.session,
            turns([case.call.clone(), fresh.clone(), held.clone()]),
        )
        .await
        .unwrap();

    let streaming = "upgrade-streaming";
    case.subject
        .install_script(
            streaming,
            Script {
                turns: vec![turn(vec![
                    ScriptedPart::Text("stream before switch".into()),
                    ScriptedPart::Text("stream after switch".into()),
                ])],
            },
        )
        .await
        .unwrap();
    case.subject
        .hold_next_model_stream(streaming, 1)
        .await
        .unwrap();
    let mut live = case
        .subject
        .start_configured_inner(&rig.root, None, Some(&rig))
        .await
        .unwrap();
    let _failure_logs = FailureLogGuard(&rig);
    let streaming_route = case
        .subject
        .session_route(&live, streaming, &case.subject.owner_stamp())
        .await
        .unwrap();
    let mut fields = case
        .subject
        .send_fields_with_plan(streaming, true, &Map::new())
        .unwrap();
    fields.insert("prompt".into(), json!("stream a reply"));
    fields.insert("send_id".into(), json!("streaming-send"));
    assert!(matches!(
        streaming_route
            .request("session.send", Value::Object(fields))
            .await
            .unwrap(),
        Reply::Response(_)
    ));
    case.subject.await_model_step(streaming).await.unwrap();

    let route = case
        .subject
        .session_route(&live, case.session, &case.subject.owner_stamp())
        .await
        .unwrap();
    let mut subscriber = subscribe(&route, json!("start")).await;
    case.send_on(&route).await;
    let cut_key = await_held_reply(&case.subject.shared, &case.call.arguments)
        .await
        .unwrap();
    // The in-process test provider re-registers after exec, unlike a carried supervised child.
    // Hold the next model response so that its fresh tool call sees the replacement provider.
    case.subject
        .hold_next_model_step(case.session)
        .await
        .unwrap();
    let mut prefix = Vec::new();
    capture_through(&mut subscriber, &mut prefix, |frame| {
        frame["cursor"].is_object()
    })
    .await;
    let cursor = serde_json::from_slice::<Value>(prefix.last().unwrap()).unwrap()["cursor"].clone();
    let pid = supervised_broca_pid(&rig.root).expect("supervised Broca pid");

    let freeze = UnixListener::bind(rig.root.join("freeze")).unwrap();
    // Keep a raw authenticated control socket, not the old session route (which the daemon can
    // already have retired). A caller trying to send during the freeze must first open a route.
    let mut probe = authenticated_control(&live.connection_file, rig.port)
        .await
        .unwrap();
    let launch_nonce = std::fs::read_to_string(&live.owner_nonce).unwrap();
    let receipt = rig.issue(&live).await;
    let mut before = checkpoint(&freeze, 1).await;
    let open_during_switch = control_on(
        &mut probe,
        json!({
            "op":"route.open",
            "target":subc_protocol::RouteTarget::ManagementSurface { module_id: "broca".into() },
            "identity":subc_protocol::BindIdentity::new(HARNESS, case.session),
            "consumer_identity":ConsumerIdentity { module_id: OWNER_MODULE.into(), launch_nonce: launch_nonce.trim().to_owned() },
        }),
    );
    tokio::pin!(open_during_switch);
    let refused = tokio::time::timeout(WAIT_FOR_EXEC, async {
        loop {
            tokio::select! {
                answer = &mut open_during_switch => break answer.unwrap(),
                mut blocked = checkpoint(&freeze, 3) => { blocked.write_u8(0).await.unwrap(); }
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(refused.0, FrameType::Error, "{}", refused.1);
    assert_eq!(refused.1["code"], "module_reloading", "{}", refused.1);
    assert_eq!(
        refused.1["detail"]["reason"], "daemon_upgrade",
        "{}",
        refused.1
    );
    before.write_u8(0).await.unwrap();
    checkpoint(&freeze, 2).await.write_u8(0).await.unwrap();
    let (kind, receipt) = receipt.await.unwrap();
    assert_eq!(kind, FrameType::Response, "{receipt}");
    assert!(receipt["carried"]
        .as_array()
        .unwrap()
        .contains(&json!("broca")));
    rig.resumed(&live).await;
    assert_eq!(supervised_broca_pid(&rig.root), Some(pid));
    let old_stream = tokio::time::timeout(WAIT_FOR_EXEC, async {
        while let Ok(Some(_)) = subscriber.next().await {}
    })
    .await;
    assert!(old_stream.is_ok(), "the subscriber's old route ended");
    case.subject.release_model_step(streaming).await.unwrap();
    case.subject
        .start_tool_provider(&live.connection_file)
        .await
        .map(|tools| {
            live.tools.abort();
            live.tools = tools;
        })
        .unwrap();
    case.subject.release_model_step(case.session).await.unwrap();
    live.connections.lock().await.clear();
    let route = case
        .subject
        .session_route(&live, case.session, &case.subject.owner_stamp())
        .await
        .unwrap();
    case.send_on(&route).await;
    let mut replay = subscribe(&route, cursor).await;
    let records = case
        .wait_for(
            &live,
            "held call on the new generation or run end",
            |records| {
                records
                    .iter()
                    .any(|record| matches!(record, Record::ToolHeld(_) | Record::RunEnd(_)))
            },
        )
        .await;
    let held_key = records.iter().find_map(|record| match record {
        Record::ToolHeld(held) => Some(held.call_key.clone()),
        _ => None,
    }).unwrap_or_else(|| panic!("the run reached a terminal before the post-upgrade tool held its call: {records:#?}"));
    assert_eq!(
        await_held_reply(&case.subject.shared, &held.arguments)
            .await
            .unwrap(),
        held_key
    );
    case.publish(&cut_key);
    case.subject.shared.held_feed.lock().unwrap().publish(
        LateEntry::new(
            kinds::RESULT,
            "reserved:broca",
            "none",
            0,
            "reserved:broca",
            held_key,
            "event-2",
            1_761_220_000_000,
        )
        .with_result(json!({"content":[{"type":"text","text":"fresh late result"}]})),
    );
    let mut suffix = Vec::new();
    capture_through(&mut replay, &mut suffix, |frame| {
        frame["unit"]["type"] == "run_finished"
    })
    .await;
    broca_test_support::wait_until(
        "the resumed feed acknowledges both late entries",
        || async {
            (case.subject.shared.held_feed.lock().unwrap().acked >= 2)
                .then_some(())
                .ok_or_else(|| "late results not acknowledged yet".into())
        },
    )
    .await;
    let mut whole = subscribe(&route, json!("start")).await;
    let mut all = Vec::new();
    capture_through(&mut whole, &mut all, |frame| {
        frame["unit"]["type"] == "run_finished"
    })
    .await;
    prefix.extend(suffix);
    assert_eq!(
        durable_frames(&prefix),
        durable_frames(&all),
        "reattaching at the last cursor neither loses nor repeats durable deliveries"
    );

    let records = case.subject.records(&live, case.session).await;
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record, Record::RunStart(_)))
            .count(),
        1
    );
    assert!(records.iter().any(|record| matches!(record, Record::RunEnd(end) if end.reason == broca_protocol::RunFinishReason::Completed)));
    let cut: Vec<_> = records
        .iter()
        .filter_map(|record| match record {
            Record::ToolOutcome(outcome) if outcome.tool_call_id.0 == case.call.tool_call_id => {
                Some(outcome.outcome)
            }
            _ => None,
        })
        .collect();
    assert_eq!(cut, [broca_session_store::record::CallOutcome::Unknown]);
    // The mock records parsed HTTP requests as well as raw bytes. Check the text
    // that actually reached its Chat messages, not the debug representation of those bytes.
    let requests = case
        .subject
        .shared
        .bodies
        .lock()
        .unwrap()
        .get(case.session)
        .cloned()
        .expect("the mock model received the session's requests");
    let messages = requests.iter().flat_map(|request| {
        request["messages"]
            .as_array()
            .expect("a captured Chat request has messages")
    });
    let model_saw_tool_result = |call_id: &str, text: &str| {
        messages.clone().any(|message| {
            message["role"] == "tool"
                && message["tool_call_id"] == call_id
                && message["content"]
                    .as_str()
                    .is_some_and(|content| content.contains(text))
        })
    };
    assert!(
        model_saw_tool_result(&case.call.tool_call_id, "outcome_unknown"),
        "the cut call's unknown outcome must reach the model as a tool result"
    );
    assert!(
        model_saw_tool_result(&held.tool_call_id, "fresh late result"),
        "the held call's fresh late result must reach the model"
    );
    let stale = case.late_text();
    assert!(
        !messages.clone().any(|message| {
            let content = &message["content"];
            content.as_str().is_some_and(|text| text.contains(&stale))
                || content
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|part| part["text"].as_str())
                    .any(|text| text.contains(&stale))
        }),
        "the cut call's stale late result must never reach the model"
    );
    assert_eq!(
        case.subject.shared.invocations.lock().unwrap()[&case.call.arguments.to_string()],
        1
    );
    assert_eq!(
        case.subject.shared.invocations.lock().unwrap()[&fresh.arguments.to_string()],
        1
    );
    broca_test_support::wait_until("the HTTPS model stream completes", || async {
        case.subject.records(&live, streaming).await.iter().any(|record| matches!(record, Record::RunEnd(end) if end.reason == broca_protocol::RunFinishReason::Completed))
            .then_some(()).ok_or_else(|| "model run is still open".into())
    }).await;
    assert_eq!(case.subject.model_invocations(streaming).await.unwrap(), 1);
    drop(live);
    case.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn daemon_upgrade_resume_deadline_recovers_the_held_run_after_crash() {
    let Some(rig) = DaemonUpgradeRig::new('b', true) else {
        return;
    };
    let case = HeldCase::new("upgrade-missed-deadline").await;
    let mut live = case
        .subject
        .start_configured_inner(&rig.root, None, Some(&rig))
        .await
        .unwrap();
    let _failure_logs = FailureLogGuard(&rig);
    let _ = case.send(&live).await;
    let held_key = case.await_pause(&live).await;
    let pid = supervised_broca_pid(&rig.root).expect("supervised Broca pid");
    let receipt = rig.issue(&live).await;
    let (kind, receipt) = receipt.await.unwrap();
    assert_eq!(kind, FrameType::Response, "{receipt}");
    assert!(
        receipt["carried"]
            .as_array()
            .unwrap()
            .contains(&json!("broca")),
        "{receipt}"
    );
    broca_test_support::wait_until("Broca withholds its resume HELLO", || async {
        let logs = diagnostic_logs(&live.logs_dir)
            .map_err(|error| format!("reading Broca's log: {error}"))?;
        assert!(
            !logs.contains("resumed after daemon upgrade"),
            "Broca resumed despite the withheld HELLO"
        );
        logs.contains("test hook withholding resume HELLO")
            .then_some(())
            .ok_or_else(|| "Broca has not reached its resume attempt".into())
    })
    .await;
    let terminals = rig.root.join("data-home/cortexkit/run/terminals.jsonl");
    broca_test_support::wait_until("the daemon kills the Broca that missed resume", || async {
        let records = std::fs::read_to_string(&terminals)
            .map_err(|error| format!("reading daemon terminal records: {error}"))?;
        records
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find(|terminal| {
                terminal["module_id"] == "broca"
                    && terminal["exit_signal"] == libc::SIGKILL
                    && terminal["disposition_detail"] == "daemon_upgrade"
            })
            .ok_or_else(|| "no daemon-upgrade SIGKILL for Broca yet".into())
    })
    .await;
    broca_test_support::wait_until(
        "supervisor replaces the Broca that missed resume",
        || async {
            supervised_broca_pid(&rig.root)
                .filter(|new_pid| *new_pid != pid)
                .ok_or_else(|| "no replacement pid yet".into())
        },
    )
    .await;
    broca_test_support::wait_until("the replacement recovers the held run", || async {
        let logs = diagnostic_logs(&live.logs_dir)
            .map_err(|error| format!("reading Broca's log: {error}"))?;
        logs.lines()
            .any(|line| {
                line.contains("store restart resume candidate sweep complete")
                    && line.contains("recovered=1")
                    && line.contains("run_held=1")
            })
            .then_some(())
            .ok_or_else(|| "no held-run recovery yet".into())
    })
    .await;
    let provider = case
        .subject
        .start_tool_provider(&live.connection_file)
        .await
        .unwrap();
    live.tools.abort();
    live.tools = provider;
    case.publish(&held_key);
    case.assert_delivered_once(&live, true).await;

    live.connections.lock().await.clear();
    let route = case.send(&live).await;
    let records = case.subject.records(&live, case.session).await;
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record, Record::RunStart(_)))
            .count(),
        1,
        "resending the same send_id after recovery must not start another run: {records:#?}"
    );
    assert!(
        records.iter().any(|record| matches!(record,
        Record::RunEnd(end) if end.reason == broca_protocol::RunFinishReason::Completed)),
        "the recovered run must finish: {records:#?}"
    );
    assert_eq!(case.subject.tool_invocations(&case.call.arguments).await, 1);
    drop(route);
    drop(live);
    case.finish();
}
