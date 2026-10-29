//! Provider-held calls end to end: a real `ck-subc`, a supervised `ck-broca` whose tool routes
//! declare `content/v1` and `held-reply/v1` (a debug-only switch), and the scripted tool
//! provider answering one call `held` and settling it later through its `late_results` feed.
//!
//! What these cases hold broca to:
//! - the held reply parks the run durably (`ToolCallProviderHeld`, then `RunPaused {tool_held}`)
//!   with nothing sent to the model and the interim text never in any prompt;
//! - the late entry is pulled under broca's own principal, written as the held call's
//!   `ToolResult` in its original batch, synced, acked through the provider's cursor, and the run
//!   continues in the same turn;
//! - the held call is sent to the provider exactly once, whatever is killed when: a kill between
//!   the pause and the pull, or after the result is written and before its ack, neither writes the
//!   result twice nor runs the call again.

use super::*;
use cortexkit_role_llm_runner_conformance::{ScriptedToolCall, ScriptedTurn};
use cortexkit_role_tool_provider::late_results::{kinds, LateEntry};

/// The scripted provider's one incarnation: it lives in the test process, so a broca restart
/// does not restart it.
const INCARNATION: &str = "conformance-provider-1";
/// The interim content of every held reply. It must never reach the model.
const INTERIM: &str = "HELD INTERIM: the answer comes later";

/// The scripted provider's held calls and its `late_results` feed.
#[derive(Default)]
pub(super) struct FeedState {
    withdrawals: Vec<Value>,
    /// The catalog declares `late_results` and `content/v1`, and calls in `hold` are held.
    enabled: bool,
    /// Arguments (as JSON text) of the calls the provider holds.
    hold: BTreeSet<String>,
    /// Route channels whose bind declared both `content/v1` and `held-reply/v1`.
    declared_routes: BTreeSet<(u16, u32)>,
    /// The call key each held call carried, by its arguments' JSON text.
    held_keys: HashMap<String, String>,
    /// Every entry published, in order; an entry's position is its index plus one.
    entries: Vec<LateEntry>,
    /// The highest position acked.
    acked: u64,
    /// Every ack's position, in order.
    acks: Vec<u64>,
    /// Every ack that landed, with when it landed, for a failure's account.
    ack_log: Vec<(u64, std::time::Instant)>,
    /// When the provider published its first entry.
    published_at: Option<std::time::Instant>,
    /// How many acks arrived, including those held unanswered.
    ack_attempts: usize,
    /// Leave acks unanswered (and unrecorded), so the rig can kill broca while one is in flight.
    hold_acks: bool,
}

impl FeedState {
    fn publish(&mut self, entry: LateEntry) {
        self.published_at
            .get_or_insert_with(std::time::Instant::now);
        self.entries.push(entry);
    }

    /// Each ack that landed: its position, and how long before (negative) or after the first
    /// publication it landed.
    fn ack_account(&self) -> Vec<String> {
        self.ack_log
            .iter()
            .map(|(seq, at)| match self.published_at {
                Some(published) if *at >= published => {
                    format!(
                        "seq {seq} {} ms after publication",
                        (*at - published).as_millis()
                    )
                }
                Some(published) => {
                    format!(
                        "seq {seq} {} ms before publication",
                        (published - *at).as_millis()
                    )
                }
                None => format!("seq {seq} with nothing published"),
            })
            .collect()
    }
}

/// Add the provider-held declarations to a catalog answer while the feed is enabled: the
/// `late_results` session capability and the `content/v1` result format on every tool.
pub(super) fn declare_in_catalog(shared: &Shared, answer: &mut Value) {
    if !shared.held_feed.lock().unwrap().enabled {
        return;
    }
    answer["capabilities"] = json!({"late_results": true});
    if let Some(tools) = answer["tools"].as_array_mut() {
        for tool in tools {
            tool["result_format"] = json!("content/v1");
        }
    }
}

/// Remember whether a route's bind declared both held-reply capabilities: the provider holds a
/// call only on such a route (contract §4.1).
pub(super) fn note_bind(shared: &Shared, route: (u16, u32), capabilities: Option<&[String]>) {
    let declared = capabilities.is_some_and(|capabilities| {
        cortexkit_role_tool_provider::consumer_reads_held_reply_v1(Some(capabilities))
    });
    if declared {
        shared
            .held_feed
            .lock()
            .unwrap()
            .declared_routes
            .insert(route);
    }
}

/// Answer a call the provider holds with a held reply, counting it as an invocation. `None`
/// for every other call, which the scripted provider answers as usual.
pub(super) fn answer_held(
    shared: &Shared,
    route: (u16, u32),
    arguments: &str,
    request: &Value,
) -> Option<subc_client_rs::HandlerOutcome> {
    let mut feed = shared.held_feed.lock().unwrap();
    if !feed.enabled || !feed.hold.contains(arguments) || !feed.declared_routes.contains(&route) {
        return None;
    }
    // A provider may hold only a call that carried a key (contract §4.1).
    let key = request["call_key"].as_str()?.to_owned();
    feed.held_keys.insert(arguments.to_owned(), key);
    drop(feed);
    *shared
        .invocations
        .lock()
        .unwrap()
        .entry(arguments.to_owned())
        .or_default() += 1;
    Some(subc_client_rs::HandlerOutcome::Response(
        serde_json::to_vec(&json!({
            "content": [{"type": "text", "text": INTERIM}],
            "held": {"reason": "running", "detail": {"ticket": "t-1"}}
        }))
        .unwrap(),
    ))
}

/// Serve `late_results` and `late_results.ack`; `None` for any other request.
pub(super) async fn serve_feed(
    shared: &Shared,
    request: &Value,
) -> Option<subc_client_rs::HandlerOutcome> {
    match request["name"].as_str()? {
        "tool.withdraw" => {
            assert!(
                request.get("call_key").is_none(),
                "withdraw has no key of its own"
            );
            shared
                .held_feed
                .lock()
                .unwrap()
                .withdrawals
                .push(request.clone());
            Some(subc_client_rs::HandlerOutcome::Response(
                br#"{"answer":"withdrawn"}"#.to_vec(),
            ))
        }
        "late_results" => {
            let feed = shared.held_feed.lock().unwrap();
            let since = &request["arguments"]["since"];
            let from = if since.is_null() {
                0
            } else {
                let seq = since["seq"].as_u64().unwrap_or(u64::MAX);
                if since["provider_incarnation"] != INCARNATION || seq > feed.entries.len() as u64 {
                    return Some(subc_client_rs::HandlerOutcome::Error {
                        code:
                            cortexkit_role_tool_provider::late_results::CURSOR_INCARNATION_CHANGED
                                .into(),
                        message: "that cursor is not from this provider incarnation".into(),
                    });
                }
                seq
            };
            let start = from.max(feed.acked) as usize;
            let entries: Vec<&LateEntry> = feed.entries.iter().skip(start).collect();
            let reply = json!({
                "entries": entries,
                "cursor": {"provider_incarnation": INCARNATION, "seq": feed.entries.len() as u64},
                "more": false,
            });
            Some(subc_client_rs::HandlerOutcome::Response(
                serde_json::to_vec(&reply).unwrap(),
            ))
        }
        "late_results.ack" => {
            let seq = request["arguments"]["through"]["seq"].as_u64().unwrap_or(0);
            let held = {
                let mut feed = shared.held_feed.lock().unwrap();
                feed.ack_attempts += 1;
                feed.hold_acks
            };
            if held {
                shared.released.notified().await;
                return Some(subc_client_rs::HandlerOutcome::Error {
                    code: "conformance_killed".into(),
                    message: "the runner was killed while this ack was held".into(),
                });
            }
            let mut feed = shared.held_feed.lock().unwrap();
            feed.acked = feed.acked.max(seq);
            feed.acks.push(seq);
            feed.ack_log.push((seq, std::time::Instant::now()));
            Some(subc_client_rs::HandlerOutcome::Response(b"{}".to_vec()))
        }
        _ => None,
    }
}

/// Whether the real-daemon prerequisites are here; a missing one fails under
/// `BROCA_REQUIRE_SUBC` and is a printed skip otherwise.
fn rig_ready() -> bool {
    if !sibling_bin("ck-subc", "SUBC_CORE_BIN").exists()
        || !sibling_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN").exists()
    {
        assert!(!require_subc(), "the subc rig binaries are missing");
        println!("SKIP: the subc rig binaries are missing");
        return false;
    }
    std::env::remove_var("SUBC_MODULE_ID");
    std::env::remove_var("SUBC_LAUNCH_NONCE");
    true
}

/// One case's rig: a subject whose broca declares the held-reply capabilities and whose provider
/// holds `call`, and the session's script (one held call, then a closing text turn).
struct HeldCase {
    scratch: PathBuf,
    root: PathBuf,
    subject: BrocaSubject,
    session: &'static str,
    call: ScriptedToolCall,
}

impl HeldCase {
    async fn new(session: &'static str) -> Self {
        let scratch = broca_test_support::scratch_root()
            .join(format!("broca-held-{session}-{}", std::process::id()));
        broca_test_support::remove_rig_dir(&scratch);
        std::fs::create_dir_all(&scratch).unwrap();
        let mut held_subject = BrocaSubject::new(&scratch).await;
        held_subject.declare_held_reply = true;
        let root = scratch.join("root");
        std::fs::create_dir_all(&root).unwrap();
        let call = ScriptedToolCall {
            tool_call_id: "held_call".into(),
            tool: SCRIPTED_TOOL.into(),
            arguments: json!({"marker": format!("held-{session}")}),
            result: json!({"marker": "never answered synchronously"}),
            hold: false,
        };
        {
            let mut feed = held_subject.shared.held_feed.lock().unwrap();
            feed.enabled = true;
            feed.hold.insert(call.arguments.to_string());
        }
        let turn = |parts| {
            ScriptedTurn::new(parts).with_usage(
                cortexkit_role_llm_runner_conformance::ScriptedUsage::new(1, 0, 0, 1),
            )
        };
        held_subject
            .install_script(
                session,
                Script {
                    turns: vec![
                        turn(vec![ScriptedPart::ToolCall(call.clone())]),
                        turn(vec![ScriptedPart::Text(
                            "done after the late result".into(),
                        )]),
                    ],
                },
            )
            .await
            .unwrap();
        Self {
            scratch,
            root,
            subject: held_subject,
            session,
            call,
        }
    }

    /// Admit the session under a plan, so the provider's declarations freeze into its manifest,
    /// and send the turn whose one call the provider holds.
    async fn send(&self, live: &Live) -> BrocaRoute {
        let route = self
            .subject
            .session_route(live, self.session, &self.subject.owner_stamp())
            .await
            .unwrap();
        self.send_on(&route).await;
        route
    }

    async fn send_on(&self, route: &BrocaRoute) {
        let mut fields = self
            .subject
            .send_fields_with_plan(self.session, true, &Map::new())
            .expect("a plan send");
        fields.insert("prompt".into(), json!("ask and wait"));
        fields.insert("send_id".into(), json!("held-send"));
        match route
            .request("session.send", Value::Object(fields))
            .await
            .unwrap()
        {
            Reply::Response(_) => {}
            Reply::Error(error) => panic!("send refused: {} {}", error.code, error.message),
        }
    }

    /// Wait until the session's WAL satisfies `ready`, failing after a minute with `what`.
    async fn wait_for(
        &self,
        live: &Live,
        what: &str,
        ready: impl Fn(&[broca_wal::WalRecord]) -> bool,
    ) -> Vec<broca_wal::WalRecord> {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let records = self.subject.records(live, self.session);
            if ready(&records) {
                return records;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {what}; the WAL holds {records:#?}"
            );
            sleep(Duration::from_millis(50)).await;
        }
    }

    /// Wait for the held reply's durable pause and return the key the held call carried.
    async fn await_pause(&self, live: &Live) -> String {
        let records = self
            .wait_for(live, "the held pause", |records| {
                records.iter().any(|record| {
                    matches!(
                        record,
                        broca_wal::WalRecord::RunPaused {
                            reason: broca_protocol::PauseReason::ToolHeld,
                            ..
                        }
                    )
                })
            })
            .await;
        let held_at = records
            .iter()
            .position(|record| matches!(record, broca_wal::WalRecord::ToolCallProviderHeld { .. }))
            .expect("the held record precedes the pause");
        let paused_at = records
            .iter()
            .position(|record| matches!(record, broca_wal::WalRecord::RunPaused { .. }))
            .unwrap();
        assert!(
            held_at < paused_at,
            "the held record is durable before the pause"
        );
        assert_eq!(
            self.model_requests(),
            1,
            "nothing goes to the model while the call is held"
        );
        self.subject
            .shared
            .held_feed
            .lock()
            .unwrap()
            .held_keys
            .get(&self.call.arguments.to_string())
            .cloned()
            .expect("the held call carried its key")
    }

    /// Publish the held call's answer on the provider's feed.
    fn publish(&self, call_key: &str) {
        let entry = LateEntry::new(
            kinds::RESULT,
            "reserved:broca",
            "none",
            0,
            "reserved:broca",
            call_key,
            "event-1",
            1_761_220_000_000,
        )
        .with_result(json!({"content": [{"type": "text", "text": self.late_text()}]}));
        self.subject.shared.held_feed.lock().unwrap().publish(entry);
    }

    fn late_text(&self) -> String {
        format!("LATE ANSWER for {}", self.session)
    }

    fn model_requests(&self) -> usize {
        self.subject
            .shared
            .requests
            .lock()
            .unwrap()
            .get(self.session)
            .map_or(0, Vec::len)
    }

    /// Check the one outcome every case shares: the entry was acked through the provider's
    /// cursor; the held call has exactly one terminal, the late result, written in its original
    /// batch; the provider ran the call once; and no model request ever held the interim
    /// content. With `completes`, the run also finished the turn: the result precedes its batch's
    /// commit and the model's next request carries it.
    async fn assert_delivered_once(&self, live: &Live, completes: bool) {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let acks = self.subject.shared.held_feed.lock().unwrap().acks.clone();
            if acks.contains(&1) {
                // The feed's whole ack history, for the run's log.
                eprintln!(
                    "{} acks: {:?}",
                    self.session,
                    self.subject.shared.held_feed.lock().unwrap().ack_account()
                );
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the entry was never acked: {acks:?}"
            );
            sleep(Duration::from_millis(50)).await;
        }
        let records = if completes {
            self.wait_for(live, "the resumed run to finish", |records| {
                records
                    .iter()
                    .any(|record| matches!(record, broca_wal::WalRecord::RunFinished { .. }))
            })
            .await
        } else {
            self.subject.records(live, self.session)
        };
        let held_batch = records
            .iter()
            .find_map(|record| match record {
                broca_wal::WalRecord::ToolCallProviderHeld { batch_id, .. } => {
                    Some(batch_id.clone())
                }
                _ => None,
            })
            .expect("a held record");
        let terminals: Vec<(usize, &broca_wal::WalRecord)> = records
            .iter()
            .enumerate()
            .filter(|(_, record)| {
                matches!(record, broca_wal::WalRecord::ToolResult { result, .. }
                    if result.tool_call_id.0 == self.call.tool_call_id)
            })
            .collect();
        assert_eq!(
            terminals.len(),
            1,
            "the held call has exactly one terminal: {records:#?}"
        );
        let (at, terminal) = terminals[0];
        let broca_wal::WalRecord::ToolResult {
            batch_id, result, ..
        } = terminal
        else {
            unreachable!()
        };
        assert_eq!(batch_id, &held_batch, "written under the original batch");
        assert!(
            serde_json::to_string(result.as_ref())
                .unwrap()
                .contains(&self.late_text()),
            "the terminal is the late result: {result:?}"
        );
        assert_eq!(
            self.subject.tool_invocations(&self.call.arguments).await,
            1,
            "the held call is sent to its provider once and never again"
        );
        let requests = self
            .subject
            .shared
            .requests
            .lock()
            .unwrap()
            .get(self.session)
            .cloned()
            .unwrap_or_default();
        let text = Value::Array(requests.iter().flatten().cloned().collect()).to_string();
        assert!(
            !text.contains(INTERIM),
            "interim content reached the model: {text}"
        );
        if completes {
            let committed_at = records
                .iter()
                .position(|record| {
                    matches!(record, broca_wal::WalRecord::ToolBatchFinished { batch_id }
                        if batch_id == &held_batch)
                })
                .expect("the held batch commits");
            assert!(at < committed_at, "the result precedes its batch's commit");
            assert_eq!(requests.len(), 2, "one request before the hold, one after");
            assert!(text.contains(&self.late_text()), "{text}");
        }
    }

    fn finish(self) {
        broca_test_support::remove_rig_dir(&self.scratch);
    }
}

/// The flagship: a held reply parks the run with nothing sent to the model; the provider then
/// publishes the answer, broca pulls it, writes it as the call's result at the call's position,
/// acks it, and the run completes in the same turn.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_call_pauses_then_its_late_result_completes_the_turn() {
    if !rig_ready() {
        return;
    }
    let case = HeldCase::new("held-flagship").await;
    let live = case.subject.spawn(&case.root).await.unwrap();
    let route = case.send(&live).await;
    let call_key = case.await_pause(&live).await;
    case.publish(&call_key);
    case.assert_delivered_once(&live, true).await;
    drop(route);
    drop(live);
    case.finish();
}

/// Preserve StreamData bodies exactly as served; decoding is only for finding the cut.
async fn capture_through(
    stream: &mut broca_subc::SubscribeStream,
    frames: &mut Vec<Vec<u8>>,
    ready: impl Fn(&Value) -> bool,
) {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let body = stream
                .next()
                .await
                .unwrap()
                .expect("subscription stays open");
            let decoded: Value = serde_json::from_slice(&body).unwrap();
            frames.push(body);
            if ready(&decoded) {
                break;
            }
        }
    })
    .await
    .expect("the subscription reached the requested durable boundary");
}

fn save_status_sample(name: &str, frames: &[Vec<u8>]) {
    if let Some(directory) = std::env::var_os("BROCA_HELD_STATUS_SAMPLES") {
        let directory = PathBuf::from(directory);
        std::fs::create_dir_all(&directory).unwrap();
        let mut bytes = Vec::new();
        for frame in frames {
            bytes.extend(frame);
            bytes.push(b'\n');
        }
        std::fs::write(directory.join(format!("{name}.jsonl")), bytes).unwrap();
    }
}

fn durable_frames(frames: &[Vec<u8>]) -> Vec<Value> {
    frames
        .iter()
        .map(|body| serde_json::from_slice::<Value>(body).unwrap())
        .filter(|frame| frame["kind"] == "control")
        .map(|mut frame| {
            // Display deltas consume stream sequence numbers but are not replayed.
            frame.as_object_mut().unwrap().remove("seq");
            frame
        })
        .collect()
}

fn check_status_stream(frames: &[Vec<u8>]) {
    for (index, body) in frames.iter().enumerate() {
        let frame: cortexkit_role_llm_runner::subscribe::SubscribeEvent =
            serde_json::from_slice(body).unwrap();
        assert_eq!(frame.seq(), Some(index as u64 + 1));
        assert!(!matches!(
            frame.live_event(),
            Some(cortexkit_role_llm_runner::live::LiveEvent::RunPaused(_))
        ));
        if frame
            .body
            .get("unit")
            .is_some_and(|unit| unit["type"] == "status")
        {
            assert_eq!(frame.live_event(), None);
            assert!(!frame.body.contains_key("event"));
            assert_eq!(frame.body["state"], "active");
            assert_eq!(
                frame.body.contains_key("held_calls"),
                !frame.held_calls().unwrap().is_empty()
            );
        }
    }
}

/// The same real serve path supplies both the live reader and reconnecting readers.
/// Set BROCA_HELD_STATUS_SAMPLES to retain the untouched bodies as JSONL reader fixtures.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn held_status_live_and_replay_samples() {
    if !rig_ready() {
        return;
    }
    for (session, kind) in [
        ("status-late", Some(kinds::RESULT)),
        ("status-expired", Some(kinds::EXPIRED)),
        ("status-ordinary", None),
    ] {
        let case = HeldCase::new(session).await;
        if kind.is_none() {
            case.subject.shared.held_feed.lock().unwrap().hold.clear();
        }
        let live = case.subject.spawn(&case.root).await.unwrap();
        let route = case
            .subject
            .session_route(&live, session, &case.subject.owner_stamp())
            .await
            .unwrap();
        let subscription = route
            .connection
            .route_open_management("broca", &route.session)
            .await
            .unwrap();
        let mut stream = route
            .connection
            .subscribe(
                &subscription,
                br#"{"method":"session.subscribe","params":{"from":"live"}}"#.to_vec(),
            )
            .await
            .unwrap();
        let mut frames = Vec::new();
        capture_through(&mut stream, &mut frames, |frame| frame["kind"] == "display").await;
        case.send_on(&route).await;
        if let Some(kind) = kind {
            let call_key = case.await_pause(&live).await;
            capture_through(&mut stream, &mut frames, |frame| {
                frame["unit"]["type"] == "status"
            })
            .await;
            let frame: cortexkit_role_llm_runner::subscribe::SubscribeEvent =
                serde_json::from_slice(frames.last().unwrap()).unwrap();
            let Reply::Response(status) = route
                .request(
                    "run.status",
                    json!({"run_id": frame.body["unit"]["run_id"]}),
                )
                .await
                .unwrap()
            else {
                panic!("run.status refused");
            };
            let status: cortexkit_role_llm_runner::run::RunStatus =
                serde_json::from_value(status).unwrap();
            assert_eq!(status.state, "active");
            assert_eq!(frame.held_calls().unwrap(), status.held_calls());
            assert_eq!(status.held_calls().len(), 1);
            assert_eq!(status.held_calls()[0].call_key(), call_key);
            save_status_sample(&format!("{session}-listed-live"), &frames);
            let replay_route = route
                .connection
                .route_open_management("broca", &route.session)
                .await
                .unwrap();
            let mut replay = route
                .connection
                .subscribe(
                    &replay_route,
                    br#"{"method":"session.subscribe","params":{"from":"start"}}"#.to_vec(),
                )
                .await
                .unwrap();
            let mut replay_frames = Vec::new();
            capture_through(&mut replay, &mut replay_frames, |frame| {
                frame["unit"]["type"] == "status"
            })
            .await;
            save_status_sample(&format!("{session}-listed-replay"), &replay_frames);
            assert_eq!(durable_frames(&frames), durable_frames(&replay_frames));
            drop(replay);
            let _ = route.connection.route_close(&replay_route).await;
            let entry = LateEntry::new(
                kind,
                "reserved:broca",
                "none",
                0,
                "reserved:broca",
                call_key,
                "event-1",
                1_761_220_000_000,
            );
            let entry = if kind == kinds::RESULT {
                entry.with_result(json!({"content":[{"type":"text","text":case.late_text()}]}))
            } else {
                entry
            };
            case.subject.shared.held_feed.lock().unwrap().publish(entry);
        }
        capture_through(&mut stream, &mut frames, |frame| {
            frame["unit"]["type"] == "run_finished"
        })
        .await;
        save_status_sample(&format!("{session}-settled-live"), &frames);
        let replay_route = route
            .connection
            .route_open_management("broca", &route.session)
            .await
            .unwrap();
        let mut replay = route
            .connection
            .subscribe(
                &replay_route,
                br#"{"method":"session.subscribe","params":{"from":"start"}}"#.to_vec(),
            )
            .await
            .unwrap();
        let mut replay_frames = Vec::new();
        capture_through(&mut replay, &mut replay_frames, |frame| {
            frame["unit"]["type"] == "run_finished"
        })
        .await;
        save_status_sample(&format!("{session}-settled-replay"), &replay_frames);
        check_status_stream(&frames);
        check_status_stream(&replay_frames);
        let changes: Vec<cortexkit_role_llm_runner::subscribe::SubscribeEvent> = frames
            .iter()
            .map(|body| serde_json::from_slice(body).unwrap())
            .filter(
                |frame: &cortexkit_role_llm_runner::subscribe::SubscribeEvent| {
                    frame
                        .body
                        .get("unit")
                        .is_some_and(|unit| unit["type"] == "status")
                },
            )
            .collect();
        if kind.is_some() {
            assert_eq!(changes.len(), 2, "listed then cleared, once each");
            assert!(changes[1].held_calls().unwrap().is_empty());
            let Reply::Response(status) = route
                .request(
                    "run.status",
                    json!({"run_id": changes[1].body["unit"]["run_id"]}),
                )
                .await
                .unwrap()
            else {
                panic!("run.status refused");
            };
            let status: cortexkit_role_llm_runner::run::RunStatus =
                serde_json::from_value(status).unwrap();
            assert_eq!(changes[1].held_calls().unwrap(), status.held_calls());
        } else {
            assert!(
                changes.is_empty(),
                "ordinary sessions gain no status frames"
            );
            assert!(frames
                .iter()
                .all(|body| !String::from_utf8_lossy(body).contains("held_calls")));
        }
        assert_eq!(durable_frames(&frames), durable_frames(&replay_frames));
        assert_eq!(case.subject.tool_invocations(&case.call.arguments).await, 1);
        drop(replay);
        drop(stream);
        drop(route);
        drop(live);
        case.finish();
    }
}

/// Killed after the run paused and before the late result was pulled: the restarted daemon
/// keeps the run paused, pulls the entry, and delivers it once without re-sending the call.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_kill_between_the_pause_and_the_pull_still_delivers_once() {
    if !rig_ready() {
        return;
    }
    let case = HeldCase::new("held-kill-paused").await;
    let mut live = case.subject.spawn(&case.root).await.unwrap();
    let route = case.send(&live).await;
    let call_key = case.await_pause(&live).await;
    drop(route);
    assert!(live.kill_all(), "ck-broca was killed");
    drop(live);

    let live = case.subject.restart(&case.root).await.unwrap();
    // The answer arrives only after the restart.
    case.publish(&call_key);
    case.assert_delivered_once(&live, true).await;
    drop(live);
    case.finish();
}

/// Killed after the late result was written as the call's terminal and before the provider saw
/// the ack: the restarted daemon re-reads the entry, finds the call already answered, acks it as
/// a no-op, and the result is never written twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_kill_after_the_result_is_written_and_before_its_ack_never_writes_it_twice() {
    if !rig_ready() {
        return;
    }
    let case = HeldCase::new("held-kill-unacked").await;
    let mut live = case.subject.spawn(&case.root).await.unwrap();
    let route = case.send(&live).await;
    let call_key = case.await_pause(&live).await;
    case.subject.shared.held_feed.lock().unwrap().hold_acks = true;
    case.publish(&call_key);
    let tool_call_id = case.call.tool_call_id.clone();
    case.wait_for(&live, "the delivered terminal", |records| {
        records.iter().any(|record| {
            matches!(record, broca_wal::WalRecord::ToolResult { result, .. }
                if result.tool_call_id.0 == tool_call_id)
        })
    })
    .await;
    let deadline = Instant::now() + Duration::from_secs(60);
    while case.subject.shared.held_feed.lock().unwrap().ack_attempts == 0 {
        assert!(Instant::now() < deadline, "broca never tried to ack");
        sleep(Duration::from_millis(50)).await;
    }
    drop(route);
    assert!(live.kill_all(), "ck-broca was killed");
    drop(live);
    {
        let mut feed = case.subject.shared.held_feed.lock().unwrap();
        assert!(
            feed.acks.is_empty(),
            "the ack was held, so none landed: {:?}",
            feed.ack_account()
        );
        feed.hold_acks = false;
    }

    // The run may have gone on past its batch's commit before the kill, so how its turn ends
    // after the restart depends on where the kill landed; what must hold is that the late
    // result was written once and the call never ran again.
    let live = case.subject.restart(&case.root).await.unwrap();
    case.assert_delivered_once(&live, false).await;
    drop(live);
    case.finish();
}

/// After the cancel seal is durable, broca withdraws the held call through a real tool route.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn held_cancel_sends_one_real_withdraw_and_restart_does_not_repeat_it() {
    if !rig_ready() {
        return;
    }
    let case = HeldCase::new("held-cancel-withdraw").await;
    let live = case.subject.spawn(&case.root).await.unwrap();
    let route = case.send(&live).await;
    let key = case.await_pause(&live).await;
    let records = case
        .wait_for(&live, "run start", |records| {
            records
                .iter()
                .any(|record| matches!(record, broca_wal::WalRecord::RunStarted { .. }))
        })
        .await;
    let run_id = records
        .iter()
        .find_map(|record| match record {
            broca_wal::WalRecord::RunStarted { run_id, .. } => Some(run_id.clone()),
            _ => None,
        })
        .unwrap();
    assert!(matches!(
        route
            .request("run.cancel", json!({"run_id": run_id, "at": "now"}))
            .await
            .unwrap(),
        Reply::Response(_)
    ));
    case.wait_for(&live, "cancel seal", |records| {
        records.iter().any(|record| {
            matches!(
                record,
                broca_wal::WalRecord::RunFinished {
                    reason: broca_protocol::RunFinishReason::Cancelled,
                    ..
                }
            )
        })
    })
    .await;
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if !case
                .subject
                .shared
                .held_feed
                .lock()
                .unwrap()
                .withdrawals
                .is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cancel reaches tool.withdraw");
    assert_eq!(
        case.subject.shared.held_feed.lock().unwrap().withdrawals,
        [json!({"name": "tool.withdraw", "arguments": {"call_key": key}})]
    );
    // Receipt at the provider is not a durable reply at broca. Restart only after the
    // outbox committed completion; a lost reply is explicitly safe to withdraw again.
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let done = broca_module::late_results_store::LateResultsStore::open(
                &live.state_root.join("late-results.db"),
            )
            .and_then(|store| store.withdrawal_rows())
            .is_ok_and(|rows| rows.iter().all(|(call, _, _)| call.call_key != key));
            if done {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("withdrawal completion commits before restart");
    drop(route);
    drop(live);
    let live = case.subject.restart(&case.root).await.unwrap();
    let route = case
        .subject
        .session_route(&live, case.session, &case.subject.owner_stamp())
        .await
        .unwrap();
    assert!(matches!(
        route
            .request("run.result", json!({"run_id": run_id}))
            .await
            .unwrap(),
        Reply::Response(_)
    ));
    assert_eq!(
        case.subject
            .shared
            .held_feed
            .lock()
            .unwrap()
            .withdrawals
            .len(),
        1
    );
    assert_eq!(case.model_requests(), 1);
    drop(route);
    drop(live);
    case.finish();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn held_shutdown_and_restart_send_no_real_withdraw() {
    if !rig_ready() {
        return;
    }
    let case = HeldCase::new("held-shutdown-no-withdraw").await;
    let live = case.subject.spawn(&case.root).await.unwrap();
    let route = case.send(&live).await;
    let key = case.await_pause(&live).await;
    drop(route);
    drop(live);
    assert!(case
        .subject
        .shared
        .held_feed
        .lock()
        .unwrap()
        .withdrawals
        .is_empty());
    let live = case.subject.restart(&case.root).await.unwrap();
    case.publish(&key);
    case.assert_delivered_once(&live, true).await;
    assert!(case
        .subject
        .shared
        .held_feed
        .lock()
        .unwrap()
        .withdrawals
        .is_empty());
    drop(live);
    case.finish();
}
