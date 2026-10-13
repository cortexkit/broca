//! Evidence for a failed suite case on this rig, collected while the daemon still answers and
//! printed only when a case fails. It covers what the adapter wrote to the test clock and why it
//! considered compaction work settled, the live `run.status` and `run.result` of a run that stays
//! non-terminal across many polls, and the daemon's log tails, watchdog stall files and decoded
//! session WALs.
//! It carries identifiers, timings and outcomes, never message content.
use super::*;
use std::time::{SystemTime, UNIX_EPOCH};

/// After this many consecutive non-terminal `run.result` answers for one run, take a live
/// snapshot. The suite gives up after 400 polls, so at half of that the daemon is still
/// running and can answer `run.status`.
pub(super) const STALL_POLLS: usize = 200;

pub(super) fn wall_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis())
}

impl Shared {
    /// Record one adapter event, in order, with its wall-clock time.
    pub(super) fn note(&self, session: &str, event: impl std::fmt::Display) {
        self.evidence
            .lock()
            .unwrap()
            .push(format!("wall_ms={} session={session} {event}", wall_ms()));
    }
}

/// The `cortexkit` directory under the rig's `XDG_DATA_HOME`. [`BrocaSubject::start`] puts the
/// state root and `data-home` side by side in the rig directory.
fn data_home(state_root: &Path) -> PathBuf {
    state_root
        .parent()
        .unwrap_or(state_root)
        .join("data-home/cortexkit")
}

fn clock_file(state_root: &Path) -> String {
    match std::fs::read_to_string(state_root.join("test-clock")) {
        Ok(text) => text,
        Err(error) => format!("unreadable: {error}"),
    }
}

impl BrocaRoute {
    /// Count non-terminal answers for `run_id`; at [`STALL_POLLS`] in a row, snapshot the run.
    pub(super) async fn observe_run_result(&self, run_id: &str, result: &Value) {
        let terminal =
            serde_json::from_value::<cortexkit_role_llm_runner::run::RunResult>(result.clone())
                .is_ok_and(|result| result.run_state().is_terminal());
        let polls = {
            let mut polls = self.subject.shared.run_polls.lock().unwrap();
            let entry = polls.entry(run_id.to_owned()).or_insert((0, Value::Null));
            entry.0 = if terminal { 0 } else { entry.0 + 1 };
            entry.1 = result.clone();
            entry.0
        };
        if polls != STALL_POLLS {
            return;
        }
        let status = self
            .raw_request("run.status", json!({"run_id": run_id}))
            .await;
        // The session-level form answers in broca's own, more detailed schema.
        let session_status = self.raw_request("run.status", json!({})).await;
        let session = &self.session.session;
        let mut snapshot = format!(
            "=== run {run_id} of session {session}: {polls} non-terminal run.result answers in a \
             row, wall_ms={} ===\nrun.result: {result}\nrun.status: {status}\n\
             session run.status: {session_status}\n",
            wall_ms()
        );
        snapshot.push_str(&self.subject.session_evidence(session, true));
        let state_root = self
            .subject
            .shared
            .session_roots
            .lock()
            .unwrap()
            .get(session)
            .cloned();
        if let Some(root) = state_root {
            snapshot.push_str(&session_client::rig_diagnostics(
                &data_home(&root),
                &root,
                session,
            ));
        }
        self.subject.shared.stalls.lock().unwrap().push(snapshot);
    }

    /// One request on this route, answered as text whatever its outcome.
    async fn raw_request(&self, method: &str, params: Value) -> String {
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

impl BrocaSubject {
    /// The adapter's view of one session: its rig's clock file, the adapter's own clock value
    /// for that rig, every provider call it received and, with `events`, its recorded events.
    pub(super) fn session_evidence(&self, session: &str, events: bool) -> String {
        let mut output = format!("--- adapter view of session {session} ---\n");
        let root = self
            .shared
            .session_roots
            .lock()
            .unwrap()
            .get(session)
            .cloned();
        if let Some(root) = &root {
            output.push_str(&format!(
                "state root {}: test-clock file {}; adapter clock {:?}\n",
                root.display(),
                clock_file(root),
                self.shared.compaction_clocks.lock().unwrap().get(root),
            ));
        }
        if let Some(state) = self.shared.compaction.lock().unwrap().get(session) {
            output.push_str(&state.describe());
        }
        if events {
            let marker = format!(" session={session} ");
            for event in self.shared.evidence.lock().unwrap().iter() {
                if event.contains(&marker) {
                    output.push_str(event);
                    output.push('\n');
                }
            }
        }
        output
    }

    /// Everything this rig can say about a failed suite: the adapter's events, the snapshots of
    /// runs that stayed non-terminal for [`STALL_POLLS`] polls, each rig's clock file and logs, and
    /// each compaction session's provider calls and decoded WAL.
    pub(super) fn failure_evidence(&self) -> String {
        let mut output = String::from("=== llm-runner conformance failure evidence ===\n");
        output.push_str("--- adapter events: clock writes and settle outcomes ---\n");
        for event in self.shared.evidence.lock().unwrap().iter() {
            output.push_str(event);
            output.push('\n');
        }
        let polls = self.shared.run_polls.lock().unwrap();
        let mut runs: Vec<_> = polls.iter().filter(|(_, (count, _))| *count > 0).collect();
        runs.sort_by(|left, right| left.0.cmp(right.0));
        output.push_str("--- runs whose latest run.result was not terminal ---\n");
        for (run_id, (count, result)) in runs {
            output.push_str(&format!("{run_id}: {count} in a row, latest {result}\n"));
        }
        drop(polls);
        for stall in self.shared.stalls.lock().unwrap().iter() {
            output.push_str(stall);
        }
        let roots: BTreeSet<PathBuf> = self
            .shared
            .session_roots
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect();
        for root in &roots {
            output.push_str(&format!(
                "=== rig state root {}: test-clock file {} ===\n",
                root.display(),
                clock_file(root)
            ));
            output.push_str(&session_client::rig_log_diagnostics(&data_home(root), root));
        }
        let mut sessions: Vec<String> = self
            .shared
            .compaction
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        sessions.sort();
        for session in sessions {
            output.push_str(&self.session_evidence(&session, false));
            let root = self
                .shared
                .session_roots
                .lock()
                .unwrap()
                .get(&session)
                .cloned();
            if let Some(root) = root {
                session_client::append_session_wal(&mut output, &root.join("wal"), &session);
            }
        }
        output
    }
}
