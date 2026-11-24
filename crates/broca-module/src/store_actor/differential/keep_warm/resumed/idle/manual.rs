//! A warm the caller asks for (`session.warm`) of a finished session: it re-sends the request the
//! session's last attempt recorded, its record is durable before it is reported, its spend is
//! exported like an automatic warm's, and closing the session waits for a warm still sending.

use super::*;
use broca_session_store::backend::MemoryBackend;

cipher_tests!(
    a_manual_warm_resends_the_recorded_request,
    a_manual_warm_is_exported_as_its_own_fact,
    opening_exports_a_manual_warm_its_insert_missed,
    a_manual_warm_whose_record_failed_is_not_reported_warmed,
    closing_records_a_warm_still_sending,
);

/// The converted history `name` in a session over a backend the test can make fail, with an
/// actor over it that warms through `executor`.
async fn in_memory(
    side: &super::super::super::Side,
    name: &str,
    cipher: Cipher,
    executor: Arc<Warming>,
) -> (StoreActor<&'static MemoryBackend>, &'static MemoryBackend) {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let converted = converted(name);
    let addr = SessionAddr(broca_core::session_addr(&bind()));
    let triple = SessionTriple {
        project_root: "/project".into(),
        harness: "store-actor".into(),
        session: "s1".into(),
    };
    let deployment = DeploymentKey::new(1, &[7; 32]).creating(cipher);
    let mut session = Session::create(backend, addr.clone(), triple.clone(), &deployment, true)
        .await
        .unwrap();
    session.append(&converted.records, true).await.unwrap();
    drop(session);
    let session = Session::load(backend, addr, &triple, &deployment)
        .await
        .unwrap();
    let mut actor = StoreActor::new(
        bind(),
        session,
        executor,
        Box::new(MemoryParts {
            parts: converted.parts,
        }),
    )
    .with_readers(Arc::new(move || backend));
    actor.registry = Some(side.registry.handle());
    (actor, backend)
}

/// Starts a manual warm and returns its reply, once its send has ended but before the actor
/// took that end.
async fn send_manual(
    actor: &mut StoreActor<&'static MemoryBackend>,
) -> (
    tokio::sync::oneshot::Receiver<crate::actor::WarmOutcome>,
    Result<Result<broca_core::WarmSent, WarmError>, tokio::task::JoinError>,
) {
    let (reply, result) = tokio::sync::oneshot::channel();
    actor.warm(None, reply, false).await;
    if let Some(build) = actor.media_build.take() {
        actor.media_built(build.await).await;
    }
    let pending = actor.warm.as_mut().expect("the warm is sending");
    let sent = (&mut pending.task).await;
    (result, sent)
}

async fn manual(actor: &mut StoreActor<&'static MemoryBackend>) -> crate::actor::WarmOutcome {
    let (result, sent) = send_manual(actor).await;
    actor.finish_warm(sent).await;
    result.await.unwrap()
}

async fn warms(
    actor: &StoreActor<&'static MemoryBackend>,
) -> Vec<broca_session_store::record::Warmed> {
    actor
        .session
        .open_log_records()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|(_, record)| match record {
            Record::Warmed(warm) => Some(warm),
            _ => None,
        })
        .collect()
}

fn export() -> (
    Arc<broca_store::SqliteStore>,
    crate::store_billing::StoreBilling,
) {
    let export = Arc::new(broca_store::SqliteStore::open_in_memory().unwrap());
    let billing = crate::store_billing::StoreBilling {
        index: export.clone(),
        resolver: Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::MeteredApi, 4)),
    };
    (export, billing)
}

/// Whether `export` holds a segment fact with the warm's measured cache reads.
fn holds_the_warm(export: &broca_store::SqliteStore) -> bool {
    export
        .read_export_page(None, 1000)
        .unwrap()
        .facts
        .iter()
        .any(|fact| {
            fact.segment.as_session().is_some_and(|segment| {
                serde_json::to_value(segment).unwrap()["usage"]["cached_input_tokens"] == 1000
            })
        })
}

/// The request warmed is the one the session's last attempt sent, rebuilt from its records with
/// the digest that attempt recorded, not the request the session would send next.
async fn a_manual_warm_resends_the_recorded_request(cipher: Cipher) {
    let side = super::super::super::side(cipher).await;
    let executor = Arc::new(Warming::default());
    let (mut actor, _) = in_memory(&side, "finished-head", cipher, executor.clone()).await;
    // The digest the session's last attempt recorded, for the request that attempt sent.
    let recorded = actor
        .recorded_warm_request()
        .await
        .unwrap()
        .request_body_sha256;
    let outcome = manual(&mut actor).await;
    let crate::actor::WarmOutcome::Warmed(reply) = outcome else {
        panic!("warmed: {outcome:?}");
    };
    let sent = executor.sent.lock().unwrap().clone();
    assert_eq!(sent.len(), 1, "one warm sent");
    assert_eq!(
        broca_core::render::request_digest_hex(&sent[0]),
        recorded,
        "the bytes sent are the recorded request's"
    );
    assert_eq!(reply.request_body_sha256, recorded);
    let warms = warms(&actor).await;
    assert_eq!(warms.len(), 1);
    assert_eq!(warms[0].trigger, None);
    assert_eq!(
        warms[0].step_id, 2,
        "the step that sent the recorded request"
    );
    assert_eq!(
        super::super::super::hex(&warms[0].request_body_sha256),
        recorded
    );
    side.registry.stop_and_seal().await;
}

/// A manual warm of a billed run is exported as its own spend fact, as an automatic one is.
async fn a_manual_warm_is_exported_as_its_own_fact(cipher: Cipher) {
    let side = super::super::super::side(cipher).await;
    let executor = Arc::new(Warming::default());
    let (mut actor, _) = in_memory(&side, "finished-head", cipher, executor).await;
    let (export, billing) = export();
    // The converted history already records its run billed; seed the export as that run did.
    billing
        .insert(&actor.session, &bind(), &actor.parts, "r1")
        .await
        .unwrap();
    let before = export.read_export_page(None, 1000).unwrap().facts.len();
    actor.billing = Some(billing);
    assert!(matches!(
        manual(&mut actor).await,
        crate::actor::WarmOutcome::Warmed(_)
    ));
    assert_eq!(
        export.read_export_page(None, 1000).unwrap().facts.len(),
        before + 1,
        "the warm inserts its own spend fact"
    );
    assert!(holds_the_warm(&export));
    side.registry.stop_and_seal().await;
}

/// A manual warm recorded without its export insert (the process stopped between them) is
/// exported when the session next opens.
async fn opening_exports_a_manual_warm_its_insert_missed(cipher: Cipher) {
    let side = super::super::super::side(cipher).await;
    let executor = Arc::new(Warming::default());
    let (mut actor, _) = in_memory(&side, "finished-head", cipher, executor).await;
    let (export, billing) = export();
    billing
        .insert(&actor.session, &bind(), &actor.parts, "r1")
        .await
        .unwrap();
    let before = export.read_export_page(None, 1000).unwrap().facts.len();
    assert!(matches!(
        manual(&mut actor).await,
        crate::actor::WarmOutcome::Warmed(_)
    ));
    assert_eq!(
        export.read_export_page(None, 1000).unwrap().facts.len(),
        before
    );
    actor.billing = Some(billing);
    actor.bill_recorded_idle_warms().await;
    assert_eq!(
        export.read_export_page(None, 1000).unwrap().facts.len(),
        before + 1,
        "opening inserts the missing warm fact"
    );
    assert!(holds_the_warm(&export));
    side.registry.stop_and_seal().await;
}

/// A warm the provider answered but whose record could not be written is answered as a failure:
/// the session's cache clock and spend don't show it.
async fn a_manual_warm_whose_record_failed_is_not_reported_warmed(cipher: Cipher) {
    let side = super::super::super::side(cipher).await;
    let executor = Arc::new(Warming::default());
    let (mut actor, backend) = in_memory(&side, "finished-head", cipher, executor).await;
    let (result, sent) = send_manual(&mut actor).await;
    assert!(sent.as_ref().is_ok_and(Result::is_ok), "{sent:?}");
    backend.crash_after(Some(0));
    actor.finish_warm(sent).await;
    let outcome = result.await.unwrap();
    assert!(
        matches!(outcome, crate::actor::WarmOutcome::Error(_)),
        "a warm whose record failed: {outcome:?}"
    );
    side.registry.stop_and_seal().await;
}

/// The actor closing while a warm is still sending waits for it, records it and answers it: the
/// warm may have reached the provider, and nothing records it once the actor is gone.
async fn closing_records_a_warm_still_sending(cipher: Cipher) {
    let side = super::super::super::side(cipher).await;
    let (started, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let executor = Arc::new(Warming {
        gate: Some((started.clone(), release.clone())),
        ..Default::default()
    });
    let (actor, _) = in_memory(&side, "finished-head", cipher, executor.clone()).await;
    let (commands, receiver) = mpsc::unbounded_channel();
    let served = tokio::spawn(actor.serve(receiver));
    let (reply, answer) = tokio::sync::oneshot::channel();
    commands
        .send(crate::actor::LineageCmd::Warm {
            lineage_id: None,
            idle_auto: false,
            reply,
        })
        .unwrap();
    started.notified().await;
    // The command channel closing ends the actor. The warm's release is only taken once the
    // actor has seen that close, since the actor was woken first.
    drop(commands);
    release.notify_one();
    let session = served.await.unwrap();
    let outcome = answer.await;
    assert!(
        matches!(outcome, Ok(crate::actor::WarmOutcome::Warmed(_))),
        "the closing actor answers its warm: {outcome:?}"
    );
    let warms = session
        .open_log_records()
        .await
        .unwrap()
        .into_iter()
        .filter(|(_, record)| matches!(record, Record::Warmed(_)))
        .count();
    assert_eq!(warms, 1, "the warm sent while closing is recorded");
    assert_eq!(executor.sent.lock().unwrap().len(), 1);
    side.registry.stop_and_seal().await;
}
