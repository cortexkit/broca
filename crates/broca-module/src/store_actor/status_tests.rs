//! `run.status`, `run.result`, `send.status` and the no-run `status` of a session kept in the new
//! storage, compared with today's replies for the same sends through the module's registry.
//!
//! Each scenario drives the same sends through a module on today's storage and one on the new
//! storage, then compares the replies as JSON with only what each storage keeps for itself taken
//! out: message ids, wall-clock times (kept as whether they are present), and an active run's
//! live `step`, which only today's actor keeps in memory. The crash scenarios read a session the
//! new storage holds before any actor recovers it, then let its open recover it, and require the
//! two reads to agree.

use std::sync::Arc;
use std::time::Duration;

use broca_core::{FileLeaseStore, FileWalStore};
use broca_protocol::{BindIdentity, NormalizedEvent, RunId, SubmissionId};
use serde_json::{json, Value};
use tokio::sync::Notify;

use super::differential::{BothLoops, TodayLoop, IDLE};
use super::tests::{executor, request, text_step, Gated};
use crate::actor::{SendKey, SendOutcome};
use crate::executor::{RunExecutor, RunKind};
use crate::registry::LineageRegistry;
use crate::serve::ServeHandler;

/// One module over a scratch folder: on today's storage, or on the new one (`store`).
struct Side {
    dir: std::path::PathBuf,
    registry: LineageRegistry,
    key: BindIdentity,
    /// Removes the folder when dropped.
    owns_dir: bool,
}

impl Drop for Side {
    fn drop(&mut self) {
        if self.owns_dir {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

fn scratch() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "store-status-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// A module over `dir` whose scripted provider answers `steps`, each step waiting for `gate`
/// when one is given. With `store`, a session that exists nowhere starts in the new storage.
async fn side_at(
    dir: std::path::PathBuf,
    store: bool,
    steps: Vec<Vec<NormalizedEvent>>,
    gate: Option<Arc<Notify>>,
) -> Side {
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let executor: Arc<dyn RunExecutor> = Arc::new(BothLoops {
        today: TodayLoop {
            provider: Gated {
                inner: broca_core::provider::StubProvider::new(steps.clone()),
                gate: gate.clone(),
            },
            wal: Arc::clone(&wal),
        },
        store: executor(steps, gate),
    });
    let runtime = crate::store_open::StoreRuntime::open(
        &dir,
        broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
        Arc::clone(&executor),
        IDLE,
    )
    .await
    .unwrap();
    let registry = LineageRegistry::new_with_reader(
        Arc::new(
            crate::opener::DefaultOpener::new(
                Arc::new(FileLeaseStore::new(dir.join("leases"))),
                wal.clone(),
                executor,
            )
            .with_idle_ttl(IDLE)
            .with_store_runtime(Arc::new(runtime)),
        ),
        wal,
        None,
    );
    let registry = if store {
        registry.with_store_new_sessions()
    } else {
        registry
    };
    let key = BindIdentity::new(dir.join("project"), "store-actor", "s1");
    Side {
        dir,
        registry,
        key,
        owns_dir: true,
    }
}

async fn side(store: bool, steps: Vec<Vec<NormalizedEvent>>, gate: Option<Arc<Notify>>) -> Side {
    side_at(scratch(), store, steps, gate).await
}

/// Today's side and the new storage's, over the same script.
async fn pair(
    steps: impl Fn() -> Vec<Vec<NormalizedEvent>>,
    gated: bool,
) -> [(Side, Option<Arc<Notify>>); 2] {
    let gate = || gated.then(|| Arc::new(Notify::new()));
    let today_gate = gate();
    let store_gate = gate();
    [
        (side(false, steps(), today_gate.clone()).await, today_gate),
        (side(true, steps(), store_gate.clone()).await, store_gate),
    ]
}

impl Side {
    async fn send_keyed(&self, run_id: &str, text: &str, key: SendKey) -> SendOutcome {
        self.registry
            .route_submission_with_order(
                &self.key,
                RunId(run_id.into()),
                request(text),
                RunKind::FreshOrResume,
                key,
                false,
                broca_protocol::Delivery::Queue,
                None,
                None,
                None,
                None,
                None,
                false,
                None,
            )
            .await
            .expect("the session opens")
    }

    async fn send(&self, run_id: &str, text: &str) -> SendOutcome {
        self.send_keyed(run_id, text, SendKey::Minted).await
    }

    /// Waits until the session's actor has closed and left the registry.
    async fn closed(&self) {
        for _ in 0..2000 {
            if self.registry.slot_state(&self.key).is_none() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("the session's actor never closed");
    }

    fn in_store(&self) -> bool {
        broca_wal::session_storage_head(&self.dir, &broca_core::session_addr(&self.key)).exists()
    }

    async fn run_status(&self, run_id: &str) -> Value {
        reply(
            self.registry
                .run_status_reply(&self.key, RunId(run_id.into()))
                .await,
        )
    }

    async fn run_result(&self, run_id: &str) -> Value {
        reply(
            self.registry
                .run_result(&self.key, RunId(run_id.into()))
                .await,
        )
    }

    async fn send_status(&self, run_id: &str, send_id: &str) -> Value {
        reply(
            self.registry
                .send_status(&self.key, RunId(run_id.into()), send_id, None)
                .await,
        )
    }

    async fn status(&self) -> Value {
        reply(self.registry.status(&self.key).await)
    }
}

/// A reply as JSON with what each storage keeps for itself taken out, or the refusal's variant.
fn reply<T: serde::Serialize>(reply: Result<T, crate::serve::ServeError>) -> Value {
    match reply {
        Ok(value) => masked(serde_json::to_value(value).unwrap()),
        Err(error) => json!({ "refused": format!("{error:?}") }),
    }
}

fn masked(mut value: Value) -> Value {
    fn walk(value: &mut Value) {
        match value {
            Value::Object(object) => {
                // A message's id takes each storage's own form; an active run's live step is
                // kept only by today's actor.
                object.remove("mid");
                if object.get("state") == Some(&json!("active")) {
                    object.remove("step");
                }
                for (name, member) in object.iter_mut() {
                    if name.ends_with("_ms") || name.ends_with("_at") {
                        *member = Value::Bool(!member.is_null());
                    } else {
                        walk(member);
                    }
                }
            }
            Value::Array(items) => items.iter_mut().for_each(walk),
            _ => {}
        }
    }
    walk(&mut value);
    value
}

/// A step the provider fails permanently.
fn failing_step() -> Vec<NormalizedEvent> {
    vec![NormalizedEvent::Control(
        broca_protocol::provider::ProviderControl::Error {
            error: serde_json::from_value(json!({
                "class": "permanent",
                "message": "the model refused the request",
                "status": 400
            }))
            .unwrap(),
            retries_used: 0,
        },
    )]
}

/// Asserts the store's reply equals today's, and that it says `state` when given, so a
/// comparison of two refusals never passes for a fact neither served.
fn same(today: &Value, store: &Value, what: &str, state: Option<&str>) {
    assert_eq!(store, today, "{what}");
    if let Some(state) = state {
        assert_eq!(store["state"], json!(state), "{what}: {store}");
    }
}

/// Two finished runs: each run's `run.status` and `run.result`, an id no run carries, and the
/// session's own status all read alike, and a finished run's facts are kept for the next poll.
#[tokio::test]
async fn finished_runs_report_alike() {
    let steps = || vec![text_step("a1", "hello"), text_step("a2", "again")];
    let [(today, _), (store, _)] = pair(steps, false).await;
    for side in [&today, &store] {
        assert!(matches!(
            side.send("r1", "first").await,
            SendOutcome::Active { .. }
        ));
        side.closed().await;
        assert!(matches!(
            side.send("r2", "second").await,
            SendOutcome::Active { .. }
        ));
        side.closed().await;
    }
    assert!(store.in_store() && !today.in_store());
    for run in ["r1", "r2"] {
        same(
            &today.run_status(run).await,
            &store.run_status(run).await,
            &format!("run.status {run}"),
            Some("completed"),
        );
        same(
            &today.run_result(run).await,
            &store.run_result(run).await,
            &format!("run.result {run}"),
            Some("completed"),
        );
    }
    assert_eq!(
        store.run_result("r2").await["final_message"]["text"],
        json!("again")
    );
    same(
        &today.run_status("nope").await,
        &store.run_status("nope").await,
        "an unknown run id",
        Some("unknown"),
    );
    same(
        &today.run_result("nope").await,
        &store.run_result("nope").await,
        "run.result of an unknown run id",
        None,
    );
    same(
        &today.status().await,
        &store.status().await,
        "status",
        Some("idle"),
    );
    let runtime = store.registry.store_runtime().expect("the new storage");
    assert!(
        runtime.settled_runs().len() >= 2,
        "a finished run's facts are kept"
    );
    // A kept answer is the one read from the records.
    same(
        &today.run_status("r1").await,
        &store.run_status("r1").await,
        "run.status r1 again",
        Some("completed"),
    );
}

/// A run its provider fails ends in error, with the provider's error, alike.
#[tokio::test]
async fn a_failed_run_reports_alike() {
    let [(today, _), (store, _)] = pair(|| vec![failing_step()], false).await;
    for side in [&today, &store] {
        side.send("r1", "first").await;
        side.closed().await;
    }
    assert!(store.in_store());
    same(
        &today.run_status("r1").await,
        &store.run_status("r1").await,
        "run.status of a failed run",
        Some("error"),
    );
    same(
        &today.run_result("r1").await,
        &store.run_result("r1").await,
        "run.result of a failed run",
        Some("error"),
    );
}

/// A run its caller cancels while its first model call is in flight ends cancelled, alike.
#[tokio::test]
async fn a_cancelled_run_reports_alike() {
    let [(today, today_gate), (store, store_gate)] =
        pair(|| vec![text_step("a1", "hello")], true).await;
    for (side, gate) in [(&today, &today_gate), (&store, &store_gate)] {
        assert!(matches!(
            side.send("r1", "first").await,
            SendOutcome::Active { .. }
        ));
        assert!(matches!(
            side.registry
                .cancel(&side.key, RunId("r1".into()))
                .await
                .unwrap(),
            crate::serve::CancelResponse::Ack
        ));
        gate.as_ref().unwrap().notify_one();
        side.closed().await;
    }
    assert!(store.in_store());
    same(
        &today.run_status("r1").await,
        &store.run_status("r1").await,
        "run.status of a cancelled run",
        Some("cancelled"),
    );
    same(
        &today.run_result("r1").await,
        &store.run_result("r1").await,
        "run.result of a cancelled run",
        Some("cancelled"),
    );
}

/// A send queued behind a running run: its run id reads as a pending send, the running run as
/// active, and once both ran, each as completed, alike. The session's own status names the
/// running run.
#[tokio::test]
async fn a_queued_send_reports_alike() {
    let steps = || vec![text_step("a1", "hello"), text_step("a2", "again")];
    let [(today, today_gate), (store, store_gate)] = pair(steps, true).await;
    let mut while_running = Vec::new();
    for side in [&today, &store] {
        assert!(matches!(
            side.send("r1", "first").await,
            SendOutcome::Active { .. }
        ));
        assert!(matches!(
            side.send("r2", "second").await,
            SendOutcome::Pending { .. }
        ));
        while_running.push([
            side.run_status("r2").await,
            side.run_status("r1").await,
            side.run_result("r1").await,
            side.status().await,
        ]);
    }
    assert!(store.in_store());
    assert_eq!(while_running[1], while_running[0], "while r1 runs");
    assert_eq!(while_running[1][0]["state"], json!("pending"));
    assert_eq!(while_running[1][1]["state"], json!("active"));
    assert_eq!(while_running[1][3]["run_id"], json!("r1"));
    for (side, gate) in [(&today, &today_gate), (&store, &store_gate)] {
        let gate = gate.as_ref().unwrap();
        gate.notify_one();
        gate.notify_one();
        side.closed().await;
    }
    for run in ["r1", "r2"] {
        same(
            &today.run_status(run).await,
            &store.run_status(run).await,
            &format!("run.status {run} after both ran"),
            Some("completed"),
        );
    }
}

/// A queued send its sender retracts reads as retracted under its run id, alike.
#[tokio::test]
async fn a_retracted_queued_send_reports_alike() {
    let steps = || vec![text_step("a1", "hello")];
    let [(today, today_gate), (store, store_gate)] = pair(steps, true).await;
    for (side, gate) in [(&today, &today_gate), (&store, &store_gate)] {
        side.send("r1", "first").await;
        assert!(matches!(
            side.send("r2", "second").await,
            SendOutcome::Pending { .. }
        ));
        assert!(matches!(
            side.registry
                .retract(&side.key, SubmissionId("sub-r2".into()))
                .await
                .unwrap(),
            crate::serve::RetractResponse::Retracted
        ));
        gate.as_ref().unwrap().notify_one();
        side.closed().await;
    }
    assert!(store.in_store());
    same(
        &today.run_status("r2").await,
        &store.run_status("r2").await,
        "run.status of a retracted send",
        Some("retracted"),
    );
}

/// Sends keyed by the caller's `send_id`: `send.status` reads a queued send as pending, a
/// finished one as finished with the session's baseline, and an unknown key as unknown, alike.
#[tokio::test]
async fn keyed_sends_report_their_status_alike() {
    let steps = || vec![text_step("a1", "hello"), text_step("a2", "again")];
    let [(today, today_gate), (store, store_gate)] = pair(steps, true).await;
    let mut pending = Vec::new();
    for (side, gate) in [(&today, &today_gate), (&store, &store_gate)] {
        assert!(matches!(
            side.send_keyed("r-k1", "first", SendKey::SendId("k1".into()))
                .await,
            SendOutcome::Active { .. }
        ));
        assert!(matches!(
            side.send_keyed("r-k2", "second", SendKey::SendId("k2".into()))
                .await,
            SendOutcome::Pending { .. }
        ));
        pending.push([
            side.send_status("r-k1", "k1").await,
            side.send_status("r-k2", "k2").await,
        ]);
        let gate = gate.as_ref().unwrap();
        gate.notify_one();
        gate.notify_one();
        side.closed().await;
    }
    assert!(store.in_store());
    assert_eq!(pending[1], pending[0], "while k1 runs and k2 waits");
    assert_eq!(
        pending[1][1]["state"],
        json!("pending"),
        "{}",
        pending[1][1]
    );
    for (run, key) in [("r-k1", "k1"), ("r-k2", "k2"), ("r-k9", "k9")] {
        same(
            &today.send_status(run, key).await,
            &store.send_status(run, key).await,
            &format!("send.status {key}"),
            None,
        );
    }
    assert_eq!(
        store.send_status("r-k1", "k1").await["state"],
        json!("finished"),
        "{}",
        store.send_status("r-k1", "k1").await
    );
}

/// Sends `r1`, whose first model call never answers, and `r2` queued behind it, in a module
/// over `dir`, then drops the module's runtime mid-run, as a killed process leaves the session:
/// no actor writes anything more.
async fn crash_with_a_queued_send(dir: std::path::PathBuf, store: bool) {
    tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let side = runtime.block_on(async {
            let mut side = side_at(dir, store, Vec::new(), Some(Arc::new(Notify::new()))).await;
            side.owns_dir = false;
            assert!(matches!(
                side.send("r1", "first").await,
                SendOutcome::Active { .. }
            ));
            assert!(matches!(
                side.send("r2", "second").await,
                SendOutcome::Pending { .. }
            ));
            side
        });
        // The runtime drops every task where it waits, the actor's included, so nothing seals
        // the run. The module itself is never dropped, so no actor is told to stop first.
        drop(runtime);
        std::mem::forget(side);
    })
    .await
    .unwrap();
}

/// A run a crash cut off, with a send queued behind it, on a session no actor holds. Today's
/// `run.status` opens the session, which seals the run; the new storage's reads it without an
/// actor, as the seal the next open writes: interrupted by the crash. The session's own status
/// then opens it, which seals the run and runs the queued send, and every answer read before
/// stays the same.
#[tokio::test(flavor = "multi_thread")]
async fn a_crashed_run_reads_as_its_seal_and_status_recovers_the_session() {
    let today_dir = scratch();
    let store_dir = scratch();
    crash_with_a_queued_send(today_dir.clone(), false).await;
    crash_with_a_queued_send(store_dir.clone(), true).await;
    let steps = || vec![text_step("a2", "again")];
    let today_gate = Arc::new(Notify::new());
    let store_gate = Arc::new(Notify::new());
    let today = side_at(today_dir, false, steps(), Some(Arc::clone(&today_gate))).await;
    let store = side_at(store_dir, true, steps(), Some(Arc::clone(&store_gate))).await;
    assert!(store.in_store() && !today.in_store());
    // Before anything opens the store session.
    let before = [
        store.run_status("r1").await,
        store.run_result("r1").await,
        store.run_status("r2").await,
    ];
    assert!(store.registry.slot_state(&store.key).is_none());
    assert_eq!(before[0]["state"], json!("interrupted"), "{}", before[0]);
    assert_eq!(before[0]["reason"], json!("runner_crash"), "{}", before[0]);
    assert_eq!(
        before[0]["error"]["provider_code"],
        json!("previous_process_ended_unsealed"),
        "{}",
        before[0]
    );
    assert_eq!(before[2]["state"], json!("pending"), "{}", before[2]);
    // Today's answers once its read sealed the run.
    same(
        &today.run_status("r1").await,
        &before[0],
        "run.status of the crashed run",
        Some("interrupted"),
    );
    same(
        &today.run_result("r1").await,
        &before[1],
        "run.result of the crashed run",
        Some("interrupted"),
    );
    // The session's own status recovers it; the queued send then runs, its model call held
    // until the status is read.
    let statuses = [today.status().await, store.status().await];
    today_gate.notify_one();
    store_gate.notify_one();
    today.closed().await;
    store.closed().await;
    let after = [store.run_status("r1").await, store.run_result("r1").await];
    assert_eq!(
        after[..],
        before[..2],
        "the crashed run's answers once it is sealed"
    );
    for run in ["r1", "r2"] {
        same(
            &today.run_status(run).await,
            &store.run_status(run).await,
            &format!("run.status {run} once recovered"),
            None,
        );
    }
    assert_eq!(store.run_status("r2").await["state"], json!("completed"));
    assert_eq!(
        statuses[1], statuses[0],
        "the session's status as it recovers"
    );
    assert_eq!(statuses[1]["run_id"], json!("r2"), "{}", statuses[1]);
}

mod open_recovery {
    //! The end a read serves for a run a crash cut off is the end the session's next open
    //! records, or the read is refused until the open records it. Each case reads the session
    //! before its actor reopens it and after, and the two answers must agree.

    use std::sync::Arc;

    use broca_session_loop::parts::{MemoryParts, SharedParts};
    use broca_session_store::backend::{MemoryBackend, SessionAddr};
    use broca_session_store::record::Record;
    use broca_session_store::session::Session;
    use serde_json::json;
    use tokio::sync::{oneshot, Notify};

    use super::super::tests::{
        actor_with, deployment, executor, idle, reopen, send, text_step, triple, ADDR,
    };
    use crate::actor::{CancelOutcome, LineageCmd, SendOutcome};
    use crate::store_status::{run_facts, status_response, StatusError};

    async fn view(
        backend: &'static MemoryBackend,
    ) -> broca_session_store::session::SessionView<&'static MemoryBackend> {
        Session::view(backend, SessionAddr(ADDR.into()), &triple(), &deployment())
            .await
            .unwrap()
    }

    /// `run.status` of `run_id` as a session no actor holds reads it.
    async fn read(
        backend: &'static MemoryBackend,
        parts: &SharedParts,
        run_id: &str,
    ) -> Result<serde_json::Value, String> {
        let session = view(backend).await;
        match run_facts(&session, parts, run_id, false, None).await {
            Ok(facts) => Ok(super::masked(
                serde_json::to_value(status_response(facts, run_id).unwrap()).unwrap(),
            )),
            Err(StatusError::EndUnrecorded { run_id }) => Err(format!("unrecorded {run_id}")),
            Err(error) => panic!("{error:?}"),
        }
    }

    /// Reopens the session's actor, lets its recovery finish, and stops it.
    async fn recover(backend: &'static MemoryBackend, parts: &SharedParts) {
        let (commands, served) = reopen(
            backend,
            Arc::new(executor(vec![text_step("a2", "again")], None)),
            parts,
        )
        .await;
        idle(&commands).await;
        drop(commands);
        served.await.unwrap();
    }

    /// A run whose last step ended it, cut off before its end was written: the read serves the
    /// end the open writes, completed, and the open writes exactly that.
    #[tokio::test]
    async fn a_run_cut_off_after_its_last_step_reads_as_the_end_its_open_records() {
        let parts = SharedParts::new(Box::new(MemoryParts::default()));
        let (commands, served, _) = actor_with(
            executor(vec![text_step("a1", "hello")], None),
            parts.clone(),
        )
        .await;
        assert!(matches!(
            send(&commands, "r1", "first").await,
            SendOutcome::Active { .. }
        ));
        idle(&commands).await;
        drop(commands);
        let finished = served.await.unwrap();
        // The same session with the run's end lost, as a crash between the step and the end
        // leaves it.
        let cut: &'static MemoryBackend = Box::leak(Box::default());
        let mut copy = Session::create(
            cut,
            SessionAddr(ADDR.into()),
            triple(),
            &deployment(),
            false,
        )
        .await
        .unwrap();
        for (_, record) in finished.open_log_records().await.unwrap() {
            if !matches!(record, Record::RunEnd(_) | Record::WriterOpened(_)) {
                copy.append(std::slice::from_ref(&record), false)
                    .await
                    .unwrap();
            }
        }
        copy.close().await.unwrap();
        let before = read(cut, &parts, "r1")
            .await
            .expect("the fold decides the end");
        assert_eq!(before["state"], json!("completed"), "{before}");
        recover(cut, &parts).await;
        let session = view(cut).await;
        assert!(session
            .operations
            .run
            .as_ref()
            .is_some_and(|run| run.finished.is_some()));
        assert_eq!(read(cut, &parts, "r1").await.unwrap(), before);
    }

    /// A run cut off with a cancel waiting at its next boundary: the open's resume checks the
    /// provider's renderer before it reaches the cancel, so no read can know the end, and the
    /// read is refused until the open records it, cancelled.
    #[tokio::test]
    async fn a_run_cut_off_with_a_cancel_waiting_is_refused_until_its_open() {
        let parts = SharedParts::new(Box::new(MemoryParts::default()));
        let (commands, served, backend) = actor_with(
            executor(
                vec![text_step("a1", "hello")],
                Some(Arc::new(Notify::new())),
            ),
            parts.clone(),
        )
        .await;
        assert!(matches!(
            send(&commands, "r1", "first").await,
            SendOutcome::Active { .. }
        ));
        let (reply, answer) = oneshot::channel();
        commands
            .send(LineageCmd::Cancel {
                run_id: broca_protocol::RunId("r1".into()),
                at: broca_wal::record::RunCancelAt::StepBoundary,
                reply,
            })
            .unwrap();
        assert_eq!(answer.await.unwrap(), CancelOutcome::Ack);
        served.abort();
        let _ = served.await;
        assert_eq!(
            read(backend, &parts, "r1").await,
            Err("unrecorded r1".into())
        );
        recover(backend, &parts).await;
        let after = read(backend, &parts, "r1").await.unwrap();
        assert_eq!(after["state"], json!("cancelled"), "{after}");
    }

    /// A run cut off mid-step, with no cancel, reads as the crash seal its open writes.
    #[tokio::test]
    async fn a_run_cut_off_mid_step_reads_as_its_crash_seal() {
        let parts = SharedParts::new(Box::new(MemoryParts::default()));
        let (commands, served, backend) = actor_with(
            executor(Vec::new(), Some(Arc::new(Notify::new()))),
            parts.clone(),
        )
        .await;
        assert!(matches!(
            send(&commands, "r1", "first").await,
            SendOutcome::Active { .. }
        ));
        served.abort();
        let _ = served.await;
        drop(commands);
        let before = read(backend, &parts, "r1").await.unwrap();
        assert_eq!(before["state"], json!("interrupted"), "{before}");
        assert_eq!(before["reason"], json!("runner_crash"), "{before}");
        recover(backend, &parts).await;
        assert_eq!(read(backend, &parts, "r1").await.unwrap(), before);
    }
}
