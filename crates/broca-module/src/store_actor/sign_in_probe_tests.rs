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
}

#[async_trait::async_trait]
impl RunExecutor for CountAdmissions {
    async fn execute(
        &self,
        job: crate::executor::RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<broca_core::RunSummary, broca_core::RunError> {
        self.inner.execute(job, out).await
    }

    async fn execute_on_store(
        &self,
        mut job: StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> StoreRunEnd {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if job.mode == StoreRunMode::Fresh {
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
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let parts = SharedParts::new(Box::new(MemoryParts::default()));
    let runner = Arc::new(CountAdmissions {
        inner: executor(vec![auth_failure(), text_step("answer", "done")], None),
        calls: AtomicUsize::new(0),
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
    let wal = Arc::new(broca_core::FileWalStore::new(root.join("wal")));
    let leases = Arc::new(broca_core::FileLeaseStore::new(root.join("leases")));
    let opener = crate::opener::DefaultOpener::new(leases, wal.clone(), runner.clone());
    let registry = crate::registry::LineageRegistry::new_with_reader(Arc::new(opener), wal, None);
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

#[tokio::test]
async fn actor_open_sign_in_probe_waits_for_dependency_readiness() {
    let ProbeFixture {
        actor,
        runner,
        registry,
        backend,
    } = paused_probe("ready").await;
    let (tx, rx) = mpsc::unbounded_channel();
    let commands = Commands::over(tx, backend);
    let task = tokio::spawn(actor.serve(rx));
    idle(&commands).await;
    assert_eq!(
        runner.calls.load(Ordering::SeqCst),
        1,
        "opening before readiness must not resolve credentials or ask"
    );
    commands.send(LineageCmd::IdleTick).unwrap();
    let (reply, response) = oneshot::channel();
    commands
        .send(LineageCmd::Status { reply })
        .expect("an idle tick must not retire an actor with an owed probe");
    assert_eq!(
        response
            .await
            .expect("an idle tick must preserve the owed probe"),
        crate::session_cmd::LineageStatus::Idle
    );
    assert!(!task.is_finished());
    registry.enable_restart_resumes();
    commands.send(LineageCmd::IdleTick).unwrap();
    idle(&commands).await;
    broca_test_support::wait_until("ready actor starts its owed sign-in probe", || async {
        if runner.calls.load(Ordering::SeqCst) == 2 {
            Ok(())
        } else {
            Err("probe not started".into())
        }
    })
    .await;
    idle(&commands).await;
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
