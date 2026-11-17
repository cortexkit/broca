//! Opening a head inserts missing spend only for warms after its latest finished run's Billed
//! record, without re-inserting facts for older runs.

use super::*;
use broca_core::{ExportError, ExportIndex};
use broca_wal::FramedRecord;

cipher_tests!(idle_open_recovery_never_reinserts_older_runs);

#[derive(Default)]
struct Inserts(Mutex<Vec<String>>);

impl ExportIndex for Inserts {
    fn missing_terminal_runs(&self) -> Result<Vec<broca_core::MissingExportRun>, ExportError> {
        Ok(Vec::new())
    }
    fn emit_run(
        &self,
        _: &[FramedRecord],
        run: &RunId,
        _: &dyn broca_core::ChargeBasisResolver,
    ) -> Result<(), ExportError> {
        self.0.lock().unwrap().push(run.0.clone());
        Ok(())
    }
    fn read_page(
        &self,
        _: Option<broca_core::ExportCursor>,
        _: u64,
    ) -> Result<broca_core::ExportPage, ExportError> {
        unreachable!("only insert calls are observed")
    }
    fn high_water(&self) -> u64 {
        0
    }
    fn source_epoch(&self) -> u64 {
        1
    }
    fn source_instance_id(&self) -> &str {
        "warm-recovery-test"
    }
}

async fn late_warm(actor: &mut StoreActor<LocalBackend>, run: &str) {
    let sent = broca_core::WarmSent {
        request_body_bytes: Some(100),
        warm_body_sha256: "22".repeat(32),
        sent_at_ms: crate::actor::epoch_ms(),
        usage: broca_protocol::Usage {
            input_tokens: Some(3),
            cached_input_tokens: Some(1_000),
            ..Default::default()
        },
        served_account_ref: None,
        ratelimit_headers: Vec::new(),
        http_status: 200,
    };
    let record = crate::store_actor::keep_warm::warmed(
        &RunId(run.into()),
        1,
        &"11".repeat(32),
        &sent,
        3_600_000,
        Some(3_600_000),
    )
    .unwrap();
    let (durable, written) = tokio::sync::oneshot::channel();
    actor
        .record_kept_warm(crate::store_actor::keep_warm::WarmRecord {
            record: Record::Warmed(record),
            finished_at_ms: sent.sent_at_ms,
            request_body_bytes: sent.request_body_bytes,
            durable,
        })
        .await;
    written.await.unwrap().unwrap();
}

async fn reopen(
    actor: StoreActor<LocalBackend>,
    side: &super::super::super::Side,
    cipher: Cipher,
    executor: Arc<Warming>,
    billing: crate::store_billing::StoreBilling,
) -> StoreActor<LocalBackend> {
    let parts = actor.parts.clone();
    drop(actor);
    let root = side.dir.join("resumed-store");
    let session = Session::load(
        LocalBackend::new(&root),
        SessionAddr(broca_core::session_addr(&bind())),
        &SessionTriple {
            project_root: "/project".into(),
            harness: "store-actor".into(),
            session: "s1".into(),
        },
        &DeploymentKey::new(1, &[7; 32]).creating(cipher),
    )
    .await
    .unwrap();
    let mut actor = StoreActor::new(bind(), session, executor, Box::new(parts))
        .with_readers(Arc::new(move || LocalBackend::new(&root)))
        .with_billing(billing);
    actor.registry = Some(side.registry.handle());
    actor
}

async fn idle_open_recovery_never_reinserts_older_runs(cipher: Cipher) {
    let side = super::super::super::side(false, cipher).await;
    let mut store = super::super::super::super::executor(
        vec![crate::store_actor::tests::text_step("next", "done")],
        None,
    );
    store.provider.render_config = Some(serde_json::to_value(warmable()).unwrap());
    store.provider.notes_sends = true;
    let executor = Arc::new(Warming {
        store: Some(store),
        ..Default::default()
    });
    let mut actor = head(
        &side,
        cipher,
        executor.clone(),
        converted("finished-head"),
        Arc::new(AtomicU64::new(crate::actor::epoch_ms())),
    )
    .await;
    let inserts = Arc::new(Inserts::default());
    let billing = crate::store_billing::StoreBilling {
        index: inserts.clone(),
        resolver: Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::MeteredApi, 4)),
    };
    late_warm(&mut actor, "r1").await;
    billing
        .insert(&actor.session, &bind(), &actor.parts, "r1")
        .await
        .unwrap();
    actor.billing = Some(billing.clone());
    let mut start = actor
        .recovery_start(actor.session.operations().run.as_ref().unwrap())
        .unwrap();
    start.run_id = RunId("r2".into());
    start.request = super::super::super::super::request("again");
    start.request.model = broca_protocol::ModelHandle::new("anthropic", "claude-opus-4-8");
    start.session_class = Some("head".into());
    actor
        .start_run(start, None, None, StoreRunMode::Fresh)
        .await;
    actor.run_current_to_end().await;
    assert!(actor.session.operations().billing_owed.is_empty());
    inserts.0.lock().unwrap().clear();

    let mut opened = reopen(actor, &side, cipher, executor.clone(), billing.clone()).await;
    opened.recover().await;
    assert!(
        inserts.0.lock().unwrap().is_empty(),
        "an open with no current late warm must not insert any older run's facts"
    );
    late_warm(&mut opened, "r2").await;
    let mut opened = reopen(opened, &side, cipher, executor, billing).await;
    opened.recover().await;
    assert_eq!(
        *inserts.0.lock().unwrap(),
        ["r2"],
        "a late warm recovers only the latest finished run"
    );
    side.registry.stop_and_seal().await;
}
