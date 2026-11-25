//! Capture the replies, model hold transitions and durable record order behind a failed case.
//! Passing cases discard their snapshots. The dump omits transcript content and request bodies.
use super::*;
use cortexkit_role_llm_runner_conformance::SuiteProgress;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Write as _;
use std::time::{SystemTime, UNIX_EPOCH};

const STALL_POLLS: usize = 200;
const MODEL_EVENTS: usize = 16;
const HEADS: usize = 4;
const WAL_WINDOW: u64 = 128;
const WAL_LINES: usize = 32;
const FIELD_BYTES: usize = 768;
const SESSION_BYTES: usize = 16 * 1024;

pub(super) fn wall_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis())
}

fn bounded(text: &str, bytes: usize) -> &str {
    let mut end = text.len().min(bytes);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn field(value: &Value) -> String {
    let text = value.to_string();
    let prefix = bounded(&text, FIELD_BYTES);
    if prefix.len() == text.len() {
        text
    } else {
        format!("{prefix}… (truncated)")
    }
}

fn keep_last<T>(items: &mut VecDeque<T>, value: T, limit: usize) {
    if items.len() == limit {
        items.pop_front();
    }
    items.push_back(value);
}

#[derive(Default)]
pub(super) struct Cases {
    active: Option<Case>,
    failed: Vec<Case>,
}

struct Case {
    name: &'static str,
    sessions: BTreeMap<String, SessionEvidence>,
}

#[derive(Default)]
struct SessionEvidence {
    root: Option<PathBuf>,
    identity: Option<broca_protocol::BindIdentity>,
    result: Option<ResultObservation>,
    heads: VecDeque<(Option<String>, String)>,
    model: VecDeque<(u128, &'static str)>,
    stall: Option<String>,
}

struct ResultObservation {
    run_id: String,
    text: String,
    nonterminal_polls: usize,
}

impl Cases {
    pub(super) fn active_name(&self) -> Option<&'static str> {
        self.active.as_ref().map(|case| case.name)
    }

    pub(super) fn progress(&mut self, event: &SuiteProgress) {
        match event {
            SuiteProgress::CaseStarted { case, .. } => {
                self.active = Some(Case {
                    name: case,
                    sessions: BTreeMap::new(),
                });
            }
            SuiteProgress::CaseFinished { case, outcome, .. } => {
                let active = self
                    .active
                    .take()
                    .expect("the case started before finishing");
                assert_eq!(active.name, *case);
                if matches!(outcome, CaseOutcome::Failed { .. }) {
                    self.failed.push(active);
                }
            }
            _ => {}
        }
    }

    fn session(&mut self, session: &str) -> Option<&mut SessionEvidence> {
        Some(
            self.active
                .as_mut()?
                .sessions
                .entry(session.to_owned())
                .or_default(),
        )
    }
}

impl Shared {
    pub(super) fn evidence_session(
        &self,
        session: &str,
        root: &Path,
        identity: Option<&broca_protocol::BindIdentity>,
    ) {
        if let Some(evidence) = self.case_evidence.lock().unwrap().session(session) {
            if evidence.root.as_deref() != Some(root) {
                evidence.root = Some(root.to_owned());
                evidence.identity = None;
            }
            if let Some(identity) = identity {
                evidence.identity = Some(identity.clone());
            }
        }
    }

    pub(super) fn model_hold_event(&self, session: &str, event: &'static str) {
        // A held request may wake after its case finished. Do not attribute its wake to the
        // case now running unless that case has registered the same session.
        if let Some(evidence) = self
            .case_evidence
            .lock()
            .unwrap()
            .active
            .as_mut()
            .and_then(|case| case.sessions.get_mut(session))
        {
            keep_last(&mut evidence.model, (wall_ms(), event), MODEL_EVENTS);
        }
    }
}

impl BrocaRoute {
    pub(super) fn observe_head(&self, head: &Value) {
        let last = &head["last_run_state"];
        let run_id = last["run_id"].as_str().map(str::to_owned);
        if let Some(evidence) = self
            .subject
            .shared
            .case_evidence
            .lock()
            .unwrap()
            .session(&self.session.session)
        {
            evidence.heads.retain(|(id, _)| id != &run_id);
            let snapshot = format!(
                "last_run_state={} last_run_error={} history_version={} head={}",
                field(last),
                field(&head["last_run_error"]),
                field(&head["history_version"]),
                field(&head["head"])
            );
            keep_last(&mut evidence.heads, (run_id, snapshot), HEADS);
        }
    }

    /// Keep the latest answer even when the case rejected a terminal state.
    pub(super) async fn observe_run_result(&self, run_id: &str, result: &Value) {
        let session = &self.session.session;
        let terminal =
            serde_json::from_value::<cortexkit_role_llm_runner::run::RunResult>(result.clone())
                .is_ok_and(|result| result.run_state().is_terminal());
        let polls = {
            let mut cases = self.subject.shared.case_evidence.lock().unwrap();
            let Some(evidence) = cases.session(session) else {
                return;
            };
            let polls = if terminal {
                0
            } else {
                evidence
                    .result
                    .as_ref()
                    .filter(|previous| previous.run_id == run_id)
                    .map_or(1, |previous| previous.nonterminal_polls + 1)
            };
            evidence.result = Some(ResultObservation {
                run_id: run_id.to_owned(),
                text: format!(
                    "state={} error={} finish={} reason={}",
                    field(&result["state"]),
                    field(&result["error"]),
                    field(&result["finish"]),
                    field(&result["reason"])
                ),
                nonterminal_polls: polls,
            });
            polls
        };
        if polls == STALL_POLLS {
            let status = self
                .raw_request("run.status", json!({"run_id": run_id}))
                .await;
            let session_status = self.raw_request("run.status", json!({})).await;
            if let Some(evidence) = self
                .subject
                .shared
                .case_evidence
                .lock()
                .unwrap()
                .session(session)
            {
                evidence.stall = Some(format!(
                    "run {run_id} stalled at wall_ms={}: run.status={} session status={}",
                    wall_ms(),
                    bounded(&status, FIELD_BYTES),
                    bounded(&session_status, FIELD_BYTES)
                ));
            }
        }
    }

    pub(super) async fn raw_request(&self, method: &str, params: Value) -> String {
        let body = serde_json::to_vec(&json!({"method": method, "params": params})).unwrap();
        match self
            .connection
            .route_request_terminal(&self.route, body)
            .await
        {
            Ok(Ok(response)) => String::from_utf8_lossy(&response).into_owned(),
            Ok(Err(error)) => format!("refused: {}", String::from_utf8_lossy(&error)),
            Err(error) => format!("transport failure: {error}"),
        }
    }
}

impl SessionEvidence {
    async fn render(&self, session: &str, shared: &Shared) -> String {
        let mut output = format!("--- adapter view of session {session} ---\n");
        if let Some(result) = &self.result {
            let _ = writeln!(
                output,
                "final run.result run_id={} {}",
                bounded(&result.run_id, FIELD_BYTES),
                result.text
            );
        } else {
            output.push_str("final run.result: not observed\n");
        }
        let matching_head = self.result.as_ref().and_then(|result| {
            self.heads
                .iter()
                .find(|(id, _)| id.as_deref() == Some(result.run_id.as_str()))
        });
        if let Some((_, head)) = matching_head {
            let _ = writeln!(output, "matching session.head: {head}");
        } else if let Some((_, head)) = self.heads.back() {
            let _ = writeln!(
                output,
                "latest session.head (no matching run.result/head pair): {head}"
            );
        } else {
            output.push_str("session.head: not observed\n");
        }
        output.push_str("mock model hold events:\n");
        if self.model.is_empty() {
            output.push_str("  none observed\n");
        }
        for (at, event) in &self.model {
            let _ = writeln!(output, "  wall_ms={at} {event}");
        }
        output.push_str("durable WAL (sequence order):\n");
        match self.wal().await {
            Ok(lines) => output.push_str(&lines),
            Err(error) => {
                let _ = writeln!(output, "  unavailable: {}", bounded(&error, FIELD_BYTES));
            }
        }
        if let Some(stall) = &self.stall {
            let _ = writeln!(output, "{stall}");
        }
        if let Some(root) = &self.root {
            if let Ok(clock) = std::fs::read_to_string(root.join("test-clock")) {
                let _ = writeln!(output, "test-clock: {}", bounded(&clock, FIELD_BYTES));
            }
        }
        if let Some(requests) = shared.routed_http_requests.lock().unwrap().get(session) {
            let _ = writeln!(
                output,
                "routed HTTP requests: {}",
                bounded(&format!("{requests:?}"), FIELD_BYTES)
            );
        }
        let marker = format!(" session={session} ");
        let events = shared.evidence.lock().unwrap();
        let events: Vec<_> = events
            .iter()
            .rev()
            .filter(|event| event.contains(&marker))
            .take(MODEL_EVENTS)
            .collect();
        for event in events.into_iter().rev() {
            let _ = writeln!(output, "{}", bounded(event, FIELD_BYTES));
        }
        let prefix = bounded(
            &output,
            SESSION_BYTES - "\n[session dump truncated]\n".len(),
        );
        if prefix.len() == output.len() {
            output
        } else {
            format!("{prefix}\n[session dump truncated]\n")
        }
    }

    async fn wal(&self) -> Result<String, String> {
        let (Some(root), Some(identity)) = (&self.root, &self.identity) else {
            return Err("no resolved storage identity".into());
        };
        let Some(view) = stored_log::stored_view(root, identity).await? else {
            return Err("no stored session".into());
        };
        let first = view.next_seq().saturating_sub(WAL_WINDOW);
        let records = view
            .records_from(first)
            .await
            .map_err(|error| format!("{error:?}"))?;
        let relevant: Vec<_> = records
            .iter()
            .filter_map(|(seq, record)| {
                let (id, event) = match record {
                    Record::RunStart(start) => (&start.run_id, "RunStart".into()),
                    Record::CancelRequest(cancel) => {
                        (&cancel.run_id, format!("CancelRequest({:?})", cancel.at))
                    }
                    Record::Attempt(attempt) => (
                        &attempt.run_id,
                        format!(
                            "Attempt step={} attempt={} outcome={} ts_ms={}",
                            attempt.step_id,
                            attempt.attempt,
                            bounded(&attempt.outcome, FIELD_BYTES),
                            attempt.ts_ms
                        ),
                    ),
                    Record::RunEnd(end) => (
                        &end.run_id,
                        format!("RunEnd({:?}) ts_ms={:?}", end.reason, end.ts_ms),
                    ),
                    _ => return None,
                };
                Some(format!(
                    "  seq={seq} {event} run_id={}\n",
                    bounded(id, FIELD_BYTES)
                ))
            })
            .collect();
        let mut lines = String::new();
        if first > 0 || relevant.len() > WAL_LINES {
            let _ = writeln!(
                lines,
                "  showing at most the last {WAL_LINES} relevant records from seq {first} onward"
            );
        }
        if relevant.is_empty() {
            lines.push_str("  no run start, cancel, attempt or run end records in range\n");
        }
        for line in relevant
            .iter()
            .skip(relevant.len().saturating_sub(WAL_LINES))
        {
            lines.push_str(line);
        }
        Ok(lines)
    }
}

impl BrocaSubject {
    pub(super) async fn failure_evidence(&self) -> String {
        let failed = std::mem::take(&mut self.shared.case_evidence.lock().unwrap().failed);
        let mut output = String::from("=== llm-runner conformance failure evidence ===\n");
        for case in failed {
            let _ = writeln!(output, "=== failed case {} ===", case.name);
            for (session, evidence) in case.sessions {
                output.push_str(&evidence.render(&session, &self.shared).await);
            }
        }
        output
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_case_dumps_its_observed_replies_hold_and_ordered_wal() {
    assert!(
        sibling_bin("ck-subc", "SUBC_CORE_BIN").is_file(),
        "real daemon required"
    );
    assert!(
        sibling_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN").is_file(),
        "real provider rig required"
    );
    let scratch = broca_test_support::scratch_root().join("conformance-failure-evidence");
    let subject = BrocaSubject::new(&scratch).await;
    let live = subject.spawn(&scratch.join("rig")).await.unwrap();
    let progress = |event| {
        subject
            .shared
            .case_evidence
            .lock()
            .unwrap()
            .progress(&event)
    };
    progress(SuiteProgress::CaseStarted {
        case: "passing-control",
        index: 0,
        total: 2,
    });
    subject
        .shared
        .evidence_session("passing-session", &live.state_root, None);
    subject
        .shared
        .model_hold_event("passing-session", "passing hold event");
    progress(SuiteProgress::CaseFinished {
        case: "passing-control",
        outcome: CaseOutcome::Passed,
        elapsed_ms: 0,
    });
    assert!(!subject.failure_evidence().await.contains("passing-session"));

    progress(SuiteProgress::CaseStarted {
        case: "forced-failure",
        index: 1,
        total: 2,
    });
    let session = "failed-session";
    subject
        .install_script(
            session,
            Script {
                turns: vec![cortexkit_role_llm_runner_conformance::ScriptedTurn::new(
                    vec![ScriptedPart::Text("not the released answer".into())],
                )],
            },
        )
        .await
        .unwrap();
    let route = subject
        .session_route(&live, session, &subject.owner_stamp())
        .await
        .unwrap();
    subject.hold_next_model_step(session).await.unwrap();
    let mut fields = subject.send_fields(session, true);
    fields.insert("prompt".into(), json!("not for the dump"));
    fields.insert("send_id".into(), json!("cancel-evidence"));
    let Reply::Response(sent) = route
        .request("session.send", Value::Object(fields))
        .await
        .unwrap()
    else {
        panic!("sending failed");
    };
    let run = sent["run_id"].as_str().expect("the held run started");
    subject.await_model_step(session).await.unwrap();
    for at in ["step_boundary", "now"] {
        assert!(
            matches!(route.request("run.cancel", json!({"run_id": run, "at": at})).await.unwrap(),
            Reply::Response(value) if value == "ack")
        );
    }
    broca_test_support::wait_until("the held run's terminal head", || async {
        match route.request("session.head", json!({})).await {
            Ok(Reply::Response(value))
                if value["last_run_state"]["run_id"] == run
                    && value["last_run_state"]["state"]
                        .as_str()
                        .is_some_and(|state| {
                            cortexkit_role_llm_runner::run::RunState::parse(state).is_terminal()
                        }) =>
            {
                Ok(())
            }
            other => Err(format!("{other:?}")),
        }
    })
    .await;
    let Reply::Response(result) = route
        .request("run.result", json!({"run_id": run}))
        .await
        .unwrap()
    else {
        panic!("run.result failed");
    };
    subject.release_model_step(session).await.unwrap();
    // Force the same report path as a case whose expected terminal differs from its reply.
    progress(SuiteProgress::CaseFinished {
        case: "forced-failure",
        outcome: CaseOutcome::Failed {
            reason: "case expected a completed run".into(),
        },
        elapsed_ms: 0,
    });

    let dump = subject.failure_evidence().await;
    assert_eq!(result["state"], "cancelled", "{dump}");
    assert!(
        dump.contains("=== failed case forced-failure ==="),
        "{dump}"
    );
    assert!(
        dump.contains("--- adapter view of session failed-session ---"),
        "{dump}"
    );
    assert!(
        dump.contains(&format!(
            "final run.result run_id={run} state=\"cancelled\" error=null finish="
        )),
        "{dump}"
    );
    assert!(dump.contains("matching session.head:"), "{dump}");
    assert!(dump.contains("\"state\":\"cancelled\""), "{dump}");
    assert!(dump.lines().any(|line| line.contains("wall_ms=") && line.ends_with("held before response headers")), "{dump}");
    assert!(
        dump.lines()
            .any(|line| line.contains("wall_ms=") && line.ends_with("release requested by case")),
        "{dump}"
    );
    let boundary = dump.find("CancelRequest(StepBoundary)").expect(&dump);
    let now = dump.find("CancelRequest(Now)").expect(&dump);
    let attempt = dump.find("outcome=cancelled").expect(&dump);
    let end = dump.find("RunEnd(Cancelled)").expect(&dump);
    assert!(boundary < now && now < attempt && attempt < end, "{dump}");
    assert!(
        !dump.contains("passing-session") && !dump.contains("not for the dump"),
        "{dump}"
    );
    assert!(dump.len() <= SESSION_BYTES + 128, "{dump}");
}
