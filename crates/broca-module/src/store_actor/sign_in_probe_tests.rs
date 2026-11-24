use super::tests::{
    auth_failure, bind, deployment, executor, idle, send, text_step, triple, Commands, ADDR,
};
use super::*;
use broca_core::OutputEvent;
use broca_session_loop::parts::{MemoryParts, SharedParts};
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use std::sync::atomic::{AtomicUsize, Ordering};

struct CountAdmissions {
    inner: tests::LoopExecutor,
    calls: AtomicUsize,
    /// Whether a fresh run's account is one routing chose, the kind a sign-in pause can ask
    /// routing to move off.
    routed: bool,
}

#[async_trait::async_trait]
impl RunExecutor for CountAdmissions {
    async fn execute_on_store(
        &self,
        mut job: StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> StoreRunEnd {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if job.mode == StoreRunMode::Fresh && self.routed {
            job.control.auth_selection = Some(serde_json::from_value(serde_json::json!({
                "credential_id":"credential-a","account_ref":"account-a","auth_method":"oauth",
                "account_decision":{"path":"routed","admission_id":"r1","decision_id":"decision-a","conversation_key":"conversation"}
            })).unwrap());
        }
        self.inner.execute_on_store(job, out).await
    }
}

struct ProbeFixture {
    actor: StoreActor<&'static MemoryBackend>,
    runner: Arc<CountAdmissions>,
    registry: crate::registry::LineageRegistry,
    backend: &'static MemoryBackend,
}

async fn paused_probe(label: &str) -> ProbeFixture {
    paused_on(label, true).await
}

/// A session whose first run paused for sign-in, its account routed or not, and an actor over it
/// that hasn't opened yet.
async fn paused_on(label: &str, routed: bool) -> ProbeFixture {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let parts = SharedParts::new(Box::new(MemoryParts::default()));
    let runner = Arc::new(CountAdmissions {
        inner: executor(vec![auth_failure(), text_step("answer", "done")], None),
        calls: AtomicUsize::new(0),
        routed,
    });
    let session = Session::create(
        backend,
        SessionAddr(ADDR.into()),
        triple(),
        &deployment(),
        false,
    )
    .await
    .unwrap();
    let (tx, rx) = mpsc::unbounded_channel();
    let commands = Commands::over(tx, backend);
    let task = tokio::spawn(
        StoreActor::new(bind(), session, runner.clone(), Box::new(parts.clone())).serve(rx),
    );
    send(&commands, "r1", "first").await;
    idle(&commands).await;
    drop(commands);
    drop(task.await.unwrap());
    assert_eq!(runner.calls.load(Ordering::SeqCst), 1);

    let root =
        broca_test_support::scratch_root().join(format!("sign-in-{label}-{}", std::process::id()));
    let leases = Arc::new(broca_core::FileLeaseStore::new(root.join("leases")));
    let opener = crate::opener::DefaultOpener::new(
        leases,
        runner.clone(),
        std::sync::Arc::new(crate::store_open::StoreSlot::awaiting_key(
            std::path::Path::new("/nonexistent-broca-test-store"),
            1,
        )),
    );
    let registry = crate::registry::LineageRegistry::new(Arc::new(opener));
    let session = Session::load(backend, SessionAddr(ADDR.into()), &triple(), &deployment())
        .await
        .unwrap();
    let mut actor = StoreActor::new(bind(), session, runner.clone(), Box::new(parts));
    actor.registry = Some(registry.handle());
    ProbeFixture {
        actor,
        runner,
        registry,
        backend,
    }
}

/// A run paused for sign-in on an account routing didn't choose owes a boot nothing: opening
/// leaves the pause as it is, so the actor retires the session's recovery registration and the
/// next boot doesn't open it. The same pause on a routed account stays registered, since opening
/// asks routing to move the run.
#[tokio::test]
async fn only_a_routed_sign_in_pause_stays_registered_for_recovery() {
    for routed in [false, true] {
        let fixture = paused_on(&format!("registered-{routed}"), routed).await;
        let index = broca_test_support::scratch_root()
            .join(format!("sign-in-index-{routed}-{}", std::process::id()));
        // The actor registers and retires the session under its key's address.
        let addr = SessionAddr(broca_core::session_addr(&bind()));
        crate::store_open::recovery::register_candidate(&index, &addr).unwrap();
        let actor = fixture.actor.with_recovery_index(index.clone());
        let (tx, rx) = mpsc::unbounded_channel();
        let commands = Commands::over(tx, fixture.backend);
        let task = tokio::spawn(actor.serve(rx));
        idle(&commands).await;
        assert_eq!(
            crate::store_open::recovery_index::registered_for_test(&index, &addr),
            routed,
            "routed: {routed}: whether the session is still registered after its open"
        );
        drop(commands);
        drop(task.await.unwrap());
        drop(fixture.registry);
        let _ = std::fs::remove_dir_all(index);
    }
}

/// How long the probe tests' actors wait idle before their idle deadline.
const IDLE_TTL: std::time::Duration = std::time::Duration::from_secs(30);

#[tokio::test(start_paused = true)]
async fn actor_open_sign_in_probe_waits_for_dependency_readiness() {
    let ProbeFixture {
        actor,
        runner,
        registry,
        backend,
    } = paused_probe("ready").await;
    let (tx, rx) = mpsc::unbounded_channel();
    let commands = Commands::over(tx, backend);
    let task = tokio::spawn(actor.serve_idle(rx, IDLE_TTL));
    idle(&commands).await;
    assert_eq!(
        runner.calls.load(Ordering::SeqCst),
        1,
        "opening before readiness must not resolve credentials or ask"
    );
    // Two idle deadlines pass, well inside the two minutes an owed probe may hold the actor.
    tokio::time::advance(IDLE_TTL * 2).await;
    let (reply, response) = oneshot::channel();
    commands
        .send(LineageCmd::Status { reply })
        .expect("an idle deadline must not retire an actor with an owed probe");
    assert_eq!(
        response
            .await
            .expect("an idle deadline must preserve the owed probe"),
        crate::session_cmd::LineageStatus::Idle
    );
    assert!(!task.is_finished());
    registry.enable_restart_resumes();
    broca_test_support::wait_until("ready actor starts its owed sign-in probe", || async {
        if runner.calls.load(Ordering::SeqCst) == 2 {
            Ok(())
        } else {
            Err("probe not started".into())
        }
    })
    .await;
    drop(commands);
    drop(task.await.unwrap());
}

#[tokio::test]
async fn stopping_before_sign_in_probe_readiness_keeps_the_pause_without_writes() {
    let ProbeFixture {
        mut actor,
        runner,
        registry: _registry,
        backend,
    } = paused_probe("stop-before-ready").await;
    let before = actor.session.open_log_records().await.unwrap();
    let stop = CancellationToken::new();
    let stopping = stop.clone();
    let (tx, rx) = mpsc::unbounded_channel();
    let commands = Commands::over(tx, backend);
    let task = tokio::spawn(async move {
        let mut receiver = super::Commands::from(rx);
        let exit = actor
            .serve_until_exit(&mut receiver, Some(&stopping), None, None, None)
            .await;
        assert!(matches!(exit, Exit::Stop));
        actor.stop_for_shutdown().await;
        actor.collect_on_exit(&exit).await;
        actor.into_session()
    });
    idle(&commands).await;
    assert_eq!(runner.calls.load(Ordering::SeqCst), 1);
    stop.cancel();
    let session = task.await.unwrap();
    assert_eq!(runner.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        session.operations().run.as_ref().unwrap().paused,
        Some(broca_protocol::PauseReason::AuthRequired)
    );
    assert_eq!(
        serde_json::to_value(session.open_log_records().await.unwrap()).unwrap(),
        serde_json::to_value(before).unwrap()
    );
}

#[tokio::test(start_paused = true)]
async fn an_open_probe_readiness_hold_expires_two_minutes_after_open() {
    let ProbeFixture {
        actor,
        runner,
        registry: _registry,
        backend,
    } = paused_probe("readiness-deadline").await;
    let before = actor.session.open_log_records().await.unwrap();
    let (tx, rx) = mpsc::unbounded_channel();
    let commands = Commands::over(tx, backend);
    let mut task = tokio::spawn(actor.serve_idle(rx, IDLE_TTL));
    idle(&commands).await;
    tokio::time::advance(std::time::Duration::from_secs(119)).await;
    let (reply, answer) = oneshot::channel();
    commands.send(LineageCmd::Status { reply }).unwrap();
    answer
        .await
        .expect("the hold still applies before two minutes");
    assert_eq!(runner.calls.load(Ordering::SeqCst), 1);
    tokio::time::advance(std::time::Duration::from_secs(2)).await;
    let session = tokio::time::timeout(IDLE_TTL * 2, &mut task)
        .await
        .expect("an expired readiness hold must allow normal idle retirement")
        .unwrap();
    assert_eq!(runner.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        session.operations().run.as_ref().unwrap().paused,
        Some(broca_protocol::PauseReason::AuthRequired)
    );
    assert_eq!(
        serde_json::to_value(session.open_log_records().await.unwrap()).unwrap(),
        serde_json::to_value(before).unwrap()
    );
}

#[tokio::test(start_paused = true)]
async fn readiness_wakes_an_owed_open_probe_without_a_caller_command() {
    let ProbeFixture {
        actor,
        runner,
        registry,
        backend,
    } = paused_probe("readiness-poll").await;
    let (tx, rx) = mpsc::unbounded_channel();
    let commands = Commands::over(tx, backend);
    let task = tokio::spawn(actor.serve(rx));
    idle(&commands).await;
    registry.enable_restart_resumes();
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        broca_test_support::wait_until("ready probe wakes promptly", || async {
            if runner.calls.load(Ordering::SeqCst) == 2 {
                Ok(())
            } else {
                Err("probe is asleep".into())
            }
        }),
    )
    .await
    .expect("readiness must be noticed without a command or a long idle deadline");
    idle(&commands).await;
    assert_eq!(runner.calls.load(Ordering::SeqCst), 2);
    drop(commands);
    drop(task.await.unwrap());
}
