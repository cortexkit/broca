//! The retired trial placement of rides: a run frozen with `ride_placement: after_results` on an
//! Anthropic Messages route shows each ride as its own text block after the tool turn's results,
//! which then hold only the tools' own output. Today's loop has no such placement, so these
//! tests run the new loop alone, each selected run beside an unselected twin of the same
//! scenario. Keep this coverage until stored runs admitted during the 12 November trial no longer
//! need rendering; new admissions never select the trial placement.

use super::*;
use broca_provider::wire::{FrozenRenderConfig, RidePlacement};

tokio::task_local! {
    /// The render config the runs in these tests freeze, when one is set.
    static ROUTE: Option<FrozenRenderConfig>;
}

/// The render config the scripted provider reports in a ride placement test, if one runs.
pub(super) fn frozen_route() -> Option<FrozenRenderConfig> {
    ROUTE.try_with(Clone::clone).unwrap_or(None)
}

/// A first-party Anthropic route with cache markers, as an unselected run freezes it.
fn anthropic() -> FrozenRenderConfig {
    cached_config()
}

/// `route` as a selected run freezes it.
fn selected(mut route: FrozenRenderConfig) -> FrozenRenderConfig {
    route.ride_placement = Some(RidePlacement::AfterResults);
    route
}

/// What one run of the new loop left: the requests it sent, its records (normalized), the
/// session's messages and compaction entries as reads serve them, and its raw records and parts
/// for a resume.
struct Run {
    requests: Vec<CallOptions>,
    records: Vec<Value>,
    raw: Vec<Record>,
    parts: MemoryParts,
    messages: Vec<Message>,
    entries: Vec<Value>,
}

/// The session's compaction entries, on the contract; none off it. Message ids, which each
/// session mints its own, are named by order of first appearance.
async fn served_entries(session: &Session<&MemoryBackend>) -> Vec<Value> {
    fn rename(value: &mut Value, names: &mut std::collections::HashMap<String, usize>) {
        match value {
            Value::Object(members) => {
                for (key, member) in members.iter_mut() {
                    match (key.as_str(), member.as_str()) {
                        ("mid", Some(text)) => {
                            let next = names.len();
                            let name = *names.entry(text.to_owned()).or_insert(next);
                            *member = json!(format!("mid#{name}"));
                        }
                        _ => rename(member, names),
                    }
                }
            }
            Value::Array(items) => items.iter_mut().for_each(|item| rename(item, names)),
            _ => {}
        }
    }
    if layout().is_none() {
        return Vec::new();
    }
    let mut names = Default::default();
    session
        .entries(1, usize::MAX)
        .await
        .expect("the session's entries read")
        .iter()
        .map(|entry| {
            let mut value =
                serde_json::to_value(broca_session_loop::entries::project(entry).unwrap()).unwrap();
            rename(&mut value, &mut names);
            value
        })
        .collect()
}

/// Runs `scenario` on the new loop under `route`, with the session's actor doing `steers`. With
/// `start`, the session first holds those records and parts and the loop resumes its run,
/// answered by `steps`.
async fn new_only(
    scenario: &Scenario,
    route: FrozenRenderConfig,
    steers: Vec<(usize, Action)>,
    start: Option<(&[Record], &MemoryParts)>,
    steps: Vec<Vec<NormalizedEvent>>,
) -> Run {
    ROUTE
        .scope(Some(route), async move {
            let backend = MemoryBackend::default();
            let triple = SessionTriple {
                project_root: "/project".into(),
                harness: "differential".into(),
                session: "s1".into(),
            };
            let mut session = Session::create(
                &backend,
                SessionAddr("0123456789abcdef".into()),
                triple,
                &DeploymentKey::new(1, &[7; 32]),
                false,
            )
            .await
            .unwrap();
            let mut parts = MemoryParts::default();
            if let Some((records, held_parts)) = start {
                session.append(records, true).await.unwrap();
                parts.parts = held_parts.parts.clone();
            }
            let injected: Arc<Mutex<Vec<Record>>> = Default::default();
            let stop = CancellationToken::new();
            let cancel = CancellationToken::new();
            let provider = SteeringProvider {
                inner: ScriptedProvider::new(steps),
                calls: Default::default(),
                reports_attempts: false,
                steers: Mutex::new(steers),
                sink: Arc::new(NewSink {
                    injected: Arc::clone(&injected),
                    stop: stop.clone(),
                    cancel: cancel.clone(),
                }),
            };
            let spy = compaction().map(|script| Arc::new(CompactionSpy::new(&script)));
            let plane = scenario.plane();
            let lifecycle = plane.lifecycle();
            let mut writer = Recording {
                session: &mut session,
                records: Vec::new(),
                injected,
                ended: Default::default(),
                media: Default::default(),
            };
            let now = harness_now;
            let bind = bind();
            let seams = Seams {
                provider: &provider,
                tools: &plane,
                bind: bind.clone(),
                out: None,
                cancel,
                stop,
                held: Some(&*lifecycle),
                approval: &HarnessPolicy,
                hook_plane: Some(harness_hooks()),
                now_ms: &now,
                session_address: Some(broca_core::wal_writer::session_addr(&bind)),
                compaction_plane: spy
                    .as_deref()
                    .map(|spy| spy as &dyn broca_core::CompactionPlane),
                compaction_session: None,
                compaction_changes: false,
                keep_warm: None,
                account_reports: None,
                max_same_account_wait: Default::default(),
                auth_selection: None,
                step_progress: None,
                model_call_ready: None,
                pause_for_auth_at_start: None,
                account_continue: None,
            };
            let finished = if start.is_some() {
                through!(writer, |w| finishes(Box::pin(resume(w, &parts, &seams)))
                    .await)
            } else {
                let mut admission = scenario.admission(&provider.inner);
                // A run the runner may resume after a restart, as the resume test cuts it.
                admission.on_restart = Some(OnRestart::Resume);
                through!(writer, |w| finishes(Box::pin(run(
                    w, &mut parts, admission, &seams
                )))
                .await)
            }
            .expect("the new loop runs");
            assert_eq!(finished.paused, None, "the run ends");
            let raw = writer.records.clone();
            let messages = writer.conversation().messages.clone();
            let entries = served_entries(writer.session).await;
            Run {
                requests: sent_requests(&provider.inner),
                records: normalized(&raw),
                raw,
                parts,
                messages,
                entries,
            }
        })
        .await
}

/// A run that asks for one `read` per path, then answers.
fn reading(paths: &[&str], manifest: bool) -> Scenario {
    let tools = vec![
        tool("read", ExecutionMode::Parallel),
        tool("big", ExecutionMode::Parallel),
    ];
    let calls = paths
        .iter()
        .enumerate()
        .map(|(index, path)| call(&format!("c{}", index + 1), "read", json!({ "path": path })))
        .collect();
    Scenario {
        input: vec![user("read them")],
        manifest: manifest.then(|| compacted_manifest(&tools)),
        tools,
        stop_when: Vec::new(),
        steps: vec![
            answer("a1", calls, FinishReason::ToolCalls),
            text_step("a2", "done"),
        ],
    }
}

/// The three ways a request of the new loop renders rides: off the compaction contract (each
/// ride rendered into its result's copy), on the contract without a compaction provider (from
/// the entries), and compacted (through the compaction view's render, with part numbers shown).
#[derive(Clone, Copy, Debug)]
enum Path {
    Plain,
    Contract,
    Compacted,
}

impl Path {
    const ALL: [Path; 3] = [Path::Plain, Path::Contract, Path::Compacted];

    fn name(self) -> &'static str {
        match self {
            Path::Plain => "plain",
            Path::Contract => "contract",
            Path::Compacted => "compacted",
        }
    }

    /// Runs `body` in this path's layout, with its compaction provider.
    async fn scope<F: std::future::Future>(self, body: F) -> F::Output {
        let (layout, script) = match self {
            Path::Plain => (None, None),
            Path::Contract => (Some(cortexkit_compaction_view::PLAN_VERSION), None),
            Path::Compacted => (
                Some(cortexkit_compaction_view::PLAN_VERSION),
                Some(Arc::new(CompactionScript {
                    context_limit: 100_000,
                    rides: Vec::new(),
                })),
            ),
        };
        LAYOUT.scope(layout, COMPACTION.scope(script, body)).await
    }
}

/// The request bodies `requests` render to on an Anthropic Messages route.
fn bodies(route: &FrozenRenderConfig, requests: &[CallOptions]) -> Vec<Value> {
    let renderer = broca_provider::LiveProvider::new(
        broca_provider::families::AnthropicMessages,
        route.clone(),
        broca_provider::wire::AuthHandle::None,
    );
    requests
        .iter()
        .map(|opts| {
            let bytes = Provider::try_render_request(&renderer, opts).expect("the request renders");
            serde_json::from_slice(&bytes).expect("an Anthropic body is JSON")
        })
        .collect()
}

/// Whether a rendered message is a tool turn: a user message carrying tool results.
fn is_tool_turn(message: &Value) -> bool {
    message["content"]
        .as_array()
        .is_some_and(|content| content.iter().any(|block| block["type"] == "tool_result"))
}

/// The newest tool turn of a rendered body.
fn tool_turn(body: &Value) -> Value {
    body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .rfind(|message| is_tool_turn(message))
        .cloned()
        .expect("the request holds a tool turn")
}

/// The texts of a tool result block's content.
fn result_texts(block: &Value) -> Vec<String> {
    match &block["content"] {
        Value::String(text) => vec![text.clone()],
        Value::Array(items) => items
            .iter()
            .filter_map(|item| item["text"].as_str().map(str::to_owned))
            .collect(),
        _ => Vec::new(),
    }
}

/// The `§N§` mark a text begins with, if any.
fn mark(text: &str) -> Option<String> {
    let rest = text.strip_prefix('§')?;
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    (!digits.is_empty() && rest[digits.len()..].starts_with('§')).then(|| format!("§{digits}§"))
}

/// Every numbered part's mark in `body`, in order: each text block and each tool result's texts.
/// With `skip_rides`, the text blocks a tool turn holds after its results (the held rides) are
/// left out, and each is checked to carry no number.
fn marks(body: &Value, skip_rides: bool) -> Vec<Option<String>> {
    let mut marks = Vec::new();
    for message in body["messages"].as_array().unwrap() {
        let tool_turn = is_tool_turn(message);
        for block in message["content"].as_array().into_iter().flatten() {
            match block["type"].as_str() {
                Some("tool_result") => {
                    marks.extend(result_texts(block).iter().map(|text| mark(text)))
                }
                Some("text") if tool_turn && skip_rides => {
                    let text = block["text"].as_str().unwrap();
                    assert_eq!(mark(text), None, "a held ride carries a number: {text}");
                }
                Some("text") => marks.push(mark(block["text"].as_str().unwrap())),
                _ => {}
            }
        }
    }
    marks
}

/// `records` with each render config part's digest masked: it is the one thing a selected run
/// records differently, its frozen render config holding the placement.
fn render_config_masked(records: &[Value]) -> Vec<Value> {
    fn mask(value: &mut Value) {
        match value {
            Value::Object(members) => {
                if members.get("kind").and_then(Value::as_str) == Some("RenderConfig") {
                    if let Some(digest) = members.get_mut("digest") {
                        *digest = json!("<render config>");
                    }
                }
                members.values_mut().for_each(mask);
            }
            Value::Array(items) => items.iter_mut().for_each(mask),
            _ => {}
        }
    }
    records
        .iter()
        .cloned()
        .map(|mut record| {
            mask(&mut record);
            record
        })
        .collect()
}

/// Checks `actual` against the golden file `name`, or writes it with `BROCA_UPDATE_GOLDEN` set.
fn golden(name: &str, actual: &Value) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden/ride_placement")
        .join(format!("{name}.json"));
    let rendered = serde_json::to_string_pretty(actual).unwrap() + "\n";
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &rendered).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("golden {} unreadable: {error}", path.display()));
    assert_eq!(rendered, expected, "golden {name} differs");
}

/// One case a path runs: its name, its scenario, and what the session's actor does meanwhile.
type Case = (&'static str, Scenario, Vec<(usize, Action)>);

/// The cases each path runs: one ride, and two rides delivered on one batch.
fn cases() -> Vec<Case> {
    vec![
        ("one_ride", reading(&["a"], false), vec![a_note("q1")]),
        (
            "two_rides",
            reading(&["a", "b"], false),
            vec![a_note("q1"), a_note("q2")],
        ),
    ]
}

fn on_path(mut scenario: Scenario, path: Path) -> Scenario {
    if let Path::Compacted = path {
        scenario.manifest = Some(compacted_manifest(&scenario.tools));
    }
    scenario
}

/// On every path a selected run sends each ride as its own text block after the tool turn's
/// results, the results hold only the tools' own output, and everything else in every request
/// is what its unselected twin sends: the same messages, the same part numbers, the cache
/// marker on the turn's last block. Its session reads, records and compaction entries are its
/// twin's. Both runs' tool turns are kept as golden files under `tests/golden/ride_placement`.
#[tokio::test]
async fn a_selected_run_shows_its_rides_after_the_tool_results() {
    for path in Path::ALL {
        for (case, scenario, steers) in cases() {
            let scenario = on_path(scenario, path);
            let label = format!("{}_{case}", path.name());
            let (twin, chosen) = path
                .scope(async {
                    let twin = new_only(
                        &scenario,
                        anthropic(),
                        steers.clone(),
                        None,
                        scenario.steps.clone(),
                    )
                    .await;
                    let chosen = new_only(
                        &scenario,
                        selected(anthropic()),
                        steers.clone(),
                        None,
                        scenario.steps.clone(),
                    )
                    .await;
                    (twin, chosen)
                })
                .await;
            // Nothing stored or read differs.
            assert_eq!(chosen.messages, twin.messages, "{label}: messages");
            assert_eq!(chosen.entries, twin.entries, "{label}: compaction entries");
            assert_eq!(
                render_config_masked(&chosen.records),
                render_config_masked(&twin.records),
                "{label}: records"
            );
            assert_eq!(chosen.requests.len(), twin.requests.len(), "{label}");
            let twin_bodies = bodies(&anthropic(), &twin.requests);
            let chosen_bodies = bodies(&selected(anthropic()), &chosen.requests);
            let rides: Vec<String> = (1..=steers.len())
                .map(|index| format!("note q{index}"))
                .collect();
            // The request after the batch carries the rides.
            let (twin_body, chosen_body) = (&twin_bodies[1], &chosen_bodies[1]);
            let (twin_turn, chosen_turn) = (tool_turn(twin_body), tool_turn(chosen_body));
            let twin_blocks = twin_turn["content"].as_array().unwrap();
            let chosen_blocks = chosen_turn["content"].as_array().unwrap();
            let results = twin_blocks.len();
            assert_eq!(
                chosen_blocks.len(),
                results + rides.len(),
                "{label}: {chosen_turn}"
            );
            for (at, ride) in rides.iter().enumerate() {
                let block = &chosen_blocks[results + at];
                assert_eq!(block["type"], "text", "{label}");
                assert_eq!(block["text"].as_str(), Some(ride.as_str()), "{label}");
            }
            // Each result holds what its twin's holds without the appended rides.
            let joined: String = rides.iter().map(|ride| format!("\n\n{ride}")).collect();
            let carrier = twin_blocks.len() - 1;
            for (at, (twin_block, chosen_block)) in
                twin_blocks.iter().zip(chosen_blocks).enumerate()
            {
                let mut expected = twin_block.clone();
                expected.as_object_mut().unwrap().remove("cache_control");
                if at == carrier {
                    let text = result_texts(twin_block).concat();
                    let own = text
                        .strip_suffix(&joined)
                        .unwrap_or_else(|| panic!("{label}: {text:?} ends in its rides"));
                    match &mut expected["content"] {
                        Value::String(content) => *content = own.to_owned(),
                        Value::Array(items) => {
                            let last = items.last_mut().unwrap();
                            last["text"] = json!(last["text"]
                                .as_str()
                                .unwrap()
                                .strip_suffix(&joined)
                                .unwrap());
                        }
                        _ => unreachable!(),
                    }
                }
                let mut chosen_block = chosen_block.clone();
                chosen_block
                    .as_object_mut()
                    .unwrap()
                    .remove("cache_control");
                assert_eq!(chosen_block, expected, "{label}: result {at}");
            }
            // The cache marker sits on the turn's last block in both.
            let marked = |blocks: &[Value]| {
                blocks
                    .iter()
                    .map(|block| block.get("cache_control").is_some())
                    .collect::<Vec<_>>()
            };
            let mut expected_marks = marked(twin_blocks);
            let twin_marks_last = expected_marks.pop().unwrap();
            expected_marks.push(false);
            expected_marks.extend(std::iter::repeat_n(false, rides.len() - 1));
            expected_marks.push(twin_marks_last);
            assert_eq!(
                marked(chosen_blocks),
                expected_marks,
                "{label}: cache marker"
            );
            assert!(twin_marks_last, "{label}: the twin marks its tool turn");
            // Every other message is its twin's, byte for byte.
            let others = |body: &Value| {
                let mut body = body.clone();
                let messages = body["messages"].as_array_mut().unwrap();
                let at = messages.iter().rposition(is_tool_turn).unwrap();
                messages.remove(at);
                body
            };
            assert_eq!(others(chosen_body), others(twin_body), "{label}");
            for (at, (chosen, twin)) in chosen_bodies.iter().zip(&twin_bodies).enumerate() {
                if at != 1 {
                    assert_eq!(chosen, twin, "{label}: request {at}");
                }
            }
            // Part numbers: every numbered part keeps its number, and the rides carry none.
            for (chosen, twin) in chosen_bodies.iter().zip(&twin_bodies) {
                assert_eq!(marks(chosen, true), marks(twin, false), "{label}: numbers");
            }
            if let Path::Compacted = path {
                assert!(
                    marks(twin_body, false).iter().any(Option::is_some),
                    "{label}: the compacted session shows part numbers"
                );
            }
            golden(
                &label,
                &json!({ "after_results": chosen_turn, "in_result": twin_turn }),
            );
        }
    }
}

/// A run frozen with the placement on a route other than Anthropic Messages renders every ride
/// inside its result, exactly as its unselected twin.
#[tokio::test]
async fn a_selected_run_on_another_route_keeps_rides_in_their_results() {
    let route = broca_provider::families::openai_chat_frozen_config("m");
    for path in Path::ALL {
        let (case, scenario, steers) = cases().remove(0);
        let scenario = on_path(scenario, path);
        let (twin, chosen) = path
            .scope(async {
                (
                    new_only(
                        &scenario,
                        route.clone(),
                        steers.clone(),
                        None,
                        scenario.steps.clone(),
                    )
                    .await,
                    new_only(
                        &scenario,
                        selected(route.clone()),
                        steers.clone(),
                        None,
                        scenario.steps.clone(),
                    )
                    .await,
                )
            })
            .await;
        assert_eq!(chosen.requests.len(), twin.requests.len());
        for (chosen, twin) in chosen.requests.iter().zip(&twin.requests) {
            assert_eq!(chosen.prompt, twin.prompt, "{path:?} {case}");
            assert!(
                chosen
                    .prompt
                    .iter()
                    .all(|message| message.tags.rides_after_results().is_empty()),
                "{path:?} {case}: no ride is held out of its result"
            );
        }
        let ContentBlock::ToolResult(result) = &chosen.requests[1]
            .prompt
            .iter()
            .rfind(|message| message.role == Role::Tool)
            .unwrap()
            .content[0]
        else {
            panic!("a tool message holds results")
        };
        let ToolOutput::Text { text } = &result.output else {
            panic!("a read answers text")
        };
        assert!(text.ends_with("\n\nnote q1"), "{path:?}: {text:?}");
    }
}

/// A selected run resumed mid-turn, after its ride was delivered and before the next request,
/// renders the request it first sent byte for byte: the placement comes from the run's frozen
/// config, never from anything read again at the resume.
#[tokio::test]
async fn a_resumed_selected_run_renders_the_bytes_it_first_sent() {
    for path in [Path::Plain, Path::Compacted] {
        let (_, scenario, steers) = cases().remove(0);
        let scenario = on_path(scenario, path);
        path.scope(async {
            let first = new_only(
                &scenario,
                selected(anthropic()),
                steers.clone(),
                None,
                scenario.steps.clone(),
            )
            .await;
            let sent = bodies(&selected(anthropic()), &first.requests);
            assert_eq!(
                tool_turn(&sent[1])["content"]
                    .as_array()
                    .unwrap()
                    .last()
                    .unwrap()["text"],
                "note q1",
                "{path:?}: the first run held its ride out"
            );
            let names: Vec<&str> = first.raw.iter().map(Record::name).collect();
            let delivered = names
                .iter()
                .rposition(|name| *name == "Delivery")
                .unwrap_or_else(|| panic!("{path:?}: the ride is delivered in {names:?}"));
            let ends = |records: &[&str]| records.iter().filter(|name| **name == "StepEnd").count();
            // The cuts between the ride's delivery and the next request's answer: the request is
            // sent again from what the run froze.
            let answered = delivered
                + names[delivered..]
                    .iter()
                    .position(|name| *name == "Assistant")
                    .expect("the next step answers");
            let mut resumed_any = false;
            for cut in delivered + 1..=answered {
                if ends(&names[..cut]) != 1 {
                    continue;
                }
                let resumed = new_only(
                    &scenario,
                    selected(anthropic()),
                    Vec::new(),
                    Some((&first.raw[..cut], &first.parts)),
                    scenario.steps[1..].to_vec(),
                )
                .await;
                let again = bodies(&selected(anthropic()), &resumed.requests);
                assert_eq!(
                    again.first(),
                    sent.get(1),
                    "{path:?}: cut at {cut} of {names:?}, the resume wrote {:?}",
                    resumed.records
                );
                resumed_any = true;
            }
            assert!(resumed_any, "{path:?}: no cut fell mid-turn in {names:?}");
        })
        .await;
    }
}
