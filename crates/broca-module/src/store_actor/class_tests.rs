use super::tests::{
    bind, deployment, executor, idle, request, send_labelled, text_step, triple, Commands, ADDR,
};
use super::*;
use crate::actor::SendKey;
use broca_session_loop::parts::MemoryParts;
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::keys::Cipher;
use broca_wal::StorageClass;
use tokio::sync::Notify;

type MemorySession = Session<&'static MemoryBackend>;

async fn fresh(cipher: Cipher) -> (&'static MemoryBackend, MemorySession) {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let session = Session::create(
        backend,
        SessionAddr(ADDR.into()),
        triple(),
        &deployment().creating(cipher),
        false,
    )
    .await
    .unwrap();
    (backend, session)
}

fn actor(
    session: MemorySession,
    folder: StorageClass,
    parts: &SharedParts,
    steps: Vec<Vec<broca_protocol::NormalizedEvent>>,
    gate: Option<Arc<Notify>>,
) -> StoreActor<&'static MemoryBackend> {
    StoreActor::new(
        bind(),
        session,
        Arc::new(executor(steps, gate)),
        Box::new(parts.clone()),
    )
    .with_class_placement(folder, vec!["head".into()])
}

fn serve(
    actor: StoreActor<&'static MemoryBackend>,
    backend: &'static MemoryBackend,
) -> (Commands, tokio::task::JoinHandle<MemorySession>) {
    let actor = actor.with_readers(Arc::new(move || backend));
    let (commands, receiver) = mpsc::unbounded_channel();
    (
        Commands::over(commands, backend),
        tokio::spawn(actor.serve(receiver)),
    )
}

async fn labelled(
    commands: &Commands,
    run: &str,
    class: Option<&str>,
    key: SendKey,
) -> SendOutcome {
    send_labelled(
        commands,
        run,
        request(run),
        key,
        broca_protocol::Delivery::Queue,
        class,
    )
    .await
}

#[tokio::test]
async fn class_adoption_records_only_the_later_start_and_survives_reload_and_seal() {
    for cipher in [Cipher::None, Cipher::XChaCha20Poly1305] {
        let (backend, session) = fresh(cipher).await;
        let parts = SharedParts::new(Box::new(MemoryParts::default()));
        let gate = Arc::new(Notify::new());
        let (commands, served) = serve(
            actor(
                session,
                StorageClass::Durable,
                &parts,
                vec![
                    text_step("a1", "one"),
                    text_step("a2", "two"),
                    text_step("a3", "three"),
                ],
                Some(gate.clone()),
            ),
            backend,
        );
        assert!(matches!(
            labelled(&commands, "first", None, SendKey::Minted).await,
            SendOutcome::Active { .. }
        ));
        gate.notify_one();
        idle(&commands).await;
        let key = || SendKey::SendId("adopt-key".into());
        let first_reply = labelled(&commands, "adopt", Some("head"), key()).await;
        assert!(
            matches!(first_reply, SendOutcome::Active { .. }),
            "{first_reply:?}"
        );
        assert_eq!(
            labelled(&commands, "adopt", Some("head"), key()).await,
            first_reply
        );
        gate.notify_one();
        idle(&commands).await;
        assert_eq!(
            labelled(&commands, "conflict", Some("mason"), SendKey::Minted).await,
            SendOutcome::SessionClassInvalid(
                "session_class differs from the session's first send".into()
            )
        );
        assert!(matches!(
            labelled(&commands, "inherit", None, SendKey::Minted).await,
            SendOutcome::Active { .. }
        ));
        gate.notify_one();
        idle(&commands).await;
        drop(commands);
        let session = served.await.unwrap();
        let starts: Vec<_> = session
            .open_log_records()
            .await
            .unwrap()
            .into_iter()
            .filter_map(|(_, record)| {
                if let Record::RunStart(start) = record {
                    Some(start)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(starts.len(), 3, "retry and conflict start no runs");
        assert_eq!(starts[0].session_class, None);
        assert_eq!(starts[1].session_class.as_deref(), Some("head"));
        assert_eq!(starts[2].session_class.as_deref(), Some("head"));
        assert_eq!(
            session
                .operations()
                .owner
                .first_start
                .as_ref()
                .unwrap()
                .session_class,
            None
        );
        let reader = crate::store_status::Reader {
            labels: Default::default(),
            parts: &parts,
            live: false,
            settled: None,
            session_name: "s1",
            live_step: None,
        };
        assert_eq!(
            reader
                .run_admission(&session, &RunId("first".into()))
                .await
                .unwrap()
                .unwrap()
                .session_class,
            None
        );
        assert_eq!(
            reader
                .run_admission(&session, &RunId("adopt".into()))
                .await
                .unwrap()
                .unwrap()
                .session_class
                .as_deref(),
            Some("head")
        );
        drop(session);
        let session = Session::load(backend, SessionAddr(ADDR.into()), &triple(), &deployment())
            .await
            .unwrap();
        let mut loaded = actor(session, StorageClass::Durable, &parts, vec![], None);
        assert_eq!(loaded.frozen_session_class().as_deref(), Some("head"));
        loaded.session.seal(None, 1000).await.unwrap();
        assert_eq!(loaded.frozen_session_class().as_deref(), Some("head"));
        drop(loaded);
        let session = Session::load(backend, SessionAddr(ADDR.into()), &triple(), &deployment())
            .await
            .unwrap();
        let actor = actor(session, StorageClass::Durable, &parts, vec![], None);
        assert_eq!(actor.frozen_session_class().as_deref(), Some("head"));
    }
}

#[tokio::test]
async fn class_adoption_requires_the_existing_folder_and_preserves_labelled_refusals() {
    for cipher in [Cipher::None, Cipher::XChaCha20Poly1305] {
        for (folder, first, requested, expected) in [
            (StorageClass::Ordinary, None, "head", None),
            (StorageClass::Durable, None, "mason", None),
            (StorageClass::Ordinary, None, "mason", Some("mason")),
            (StorageClass::Durable, Some("head"), "mason", Some("head")),
        ] {
            let (backend, session) = fresh(cipher).await;
            let parts = SharedParts::new(Box::new(MemoryParts::default()));
            let (commands, served) = serve(
                actor(session, folder, &parts, vec![text_step("a1", "one")], None),
                backend,
            );
            labelled(&commands, "first", first, SendKey::Minted).await;
            idle(&commands).await;
            drop(commands);
            let session = served.await.unwrap();
            let mut actor = actor(session, folder, &parts, vec![text_step("a2", "two")], None);
            let capture = broca_test_support::LogCapture::new();
            let admitted = capture.in_scope(|| actor.session_class(Some(requested)));
            if first.is_some() {
                assert_eq!(
                    admitted.unwrap_err(),
                    SendOutcome::SessionClassInvalid(
                        "session_class differs from the session's first send".into()
                    )
                );
                assert_eq!(actor.frozen_session_class().as_deref(), expected);
                continue;
            }
            assert_eq!(admitted.unwrap().as_deref(), expected);
            if expected.is_none() {
                let events: Vec<_> = capture
                    .events()
                    .into_iter()
                    .filter(|event| event.target == "broca.session_class")
                    .collect();
                assert_eq!(events.len(), 1);
                assert_eq!(events[0].level, tracing::Level::INFO);
                assert_eq!(events[0].fields["session"], "s1");
                assert_eq!(events[0].fields["label"], requested);
                assert!(events[0].fields["message"].contains("folder differs"));
            }
            let (commands, served) = serve(actor, backend);
            labelled(&commands, "later", Some(requested), SendKey::Minted).await;
            idle(&commands).await;
            drop(commands);
            actor = super::class_tests::actor(served.await.unwrap(), folder, &parts, vec![], None);
            assert_eq!(actor.frozen_session_class().as_deref(), expected);
            assert_eq!(
                actor
                    .session
                    .operations()
                    .run
                    .as_ref()
                    .unwrap()
                    .start
                    .as_ref()
                    .unwrap()
                    .session_class
                    .as_deref(),
                expected
            );
        }
    }
}

#[tokio::test]
async fn class_adoption_queue_receipts_leave_other_sends_unchanged() {
    for (first, later, kept) in [
        (None, None, false),
        (Some("head"), None, false),
        (Some("head"), Some("head"), false),
        (None, Some("head"), true),
    ] {
        let (backend, session) = fresh(Cipher::None).await;
        let parts = SharedParts::new(Box::new(MemoryParts::default()));
        let (commands, served) = serve(
            actor(
                session,
                StorageClass::Durable,
                &parts,
                vec![text_step("a1", "one")],
                Some(Arc::new(Notify::new())),
            ),
            backend,
        );
        labelled(&commands, "first", first, SendKey::Minted).await;
        let outcome = labelled(&commands, "queued", later, SendKey::Minted).await;
        assert!(
            matches!(outcome, SendOutcome::Pending { .. }),
            "{outcome:?}"
        );
        served.abort();
        assert!(matches!(served.await, Err(error) if error.is_cancelled()));
        drop(commands);
        let session = Session::load(backend, SessionAddr(ADDR.into()), &triple(), &deployment())
            .await
            .unwrap();
        let queued = session
            .open_log_records()
            .await
            .unwrap()
            .into_iter()
            .find_map(|(_, record)| {
                if let Record::QueuedSend(send) = record {
                    Some(send)
                } else {
                    None
                }
            })
            .unwrap();
        let expected: &[u8] = if kept {
            br#"{"session_class":"head"}"#
        } else {
            b"null"
        };
        assert_eq!(serde_json::to_vec(&queued.receipt).unwrap(), expected);
    }
}

#[tokio::test]
async fn class_adoption_queued_send_is_kept_until_its_run_starts_after_reload() {
    for cipher in [Cipher::None, Cipher::XChaCha20Poly1305] {
        let (backend, session) = fresh(cipher).await;
        let parts = SharedParts::new(Box::new(MemoryParts::default()));
        let gate = Arc::new(Notify::new());
        let mut initial = actor(
            session,
            StorageClass::Durable,
            &parts,
            vec![text_step("a1", "one")],
            Some(gate),
        );
        initial.class_placement =
            Some((StorageClass::Durable, vec!["head".into(), "mason".into()]));
        let (commands, served) = serve(initial, backend);
        labelled(&commands, "first", None, SendKey::Minted).await;
        let queued = labelled(&commands, "adopt", Some("head"), SendKey::Minted).await;
        assert!(matches!(queued, SendOutcome::Pending { .. }), "{queued:?}");
        let queued = labelled(&commands, "conflict", Some("mason"), SendKey::Minted).await;
        assert!(matches!(queued, SendOutcome::Pending { .. }), "{queued:?}");
        let queued = labelled(&commands, "inherit", None, SendKey::Minted).await;
        assert!(matches!(queued, SendOutcome::Pending { .. }), "{queued:?}");
        served.abort();
        assert!(matches!(served.await, Err(error) if error.is_cancelled()));
        drop(commands);
        let session = Session::load(backend, SessionAddr(ADDR.into()), &triple(), &deployment())
            .await
            .unwrap();
        assert_eq!(session.operations().owner.adopted_session_class, None);
        let queued = session
            .open_log_records()
            .await
            .unwrap()
            .into_iter()
            .find_map(|(_, record)| {
                if let Record::QueuedSend(send) = record {
                    Some(send)
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(
            queued.receipt.as_ref().unwrap().parse()["session_class"],
            "head"
        );
        let (commands, served) = serve(
            actor(
                session,
                StorageClass::Durable,
                &parts,
                vec![text_step("a2", "two"), text_step("a3", "three")],
                None,
            ),
            backend,
        );
        idle(&commands).await;
        drop(commands);
        let session = served.await.unwrap();
        assert_eq!(
            session.operations().owner.adopted_session_class.as_deref(),
            Some("head")
        );
        assert_eq!(
            session
                .operations()
                .run
                .as_ref()
                .unwrap()
                .start
                .as_ref()
                .unwrap()
                .session_class
                .as_deref(),
            Some("head")
        );
        let starts = session
            .open_log_records()
            .await
            .unwrap()
            .into_iter()
            .filter(|(_, record)| matches!(record, Record::RunStart(_)))
            .count();
        assert_eq!(
            starts, 3,
            "a queued send cannot replace the class an earlier queued send adopted"
        );
        assert!(
            session.operations().queue.is_empty(),
            "a refused adoption must not stall the queue"
        );
    }
}

#[tokio::test]
async fn class_adoption_queued_send_rechecks_placement_after_reload() {
    let (backend, session) = fresh(Cipher::None).await;
    let parts = SharedParts::new(Box::new(MemoryParts::default()));
    let (commands, served) = serve(
        actor(
            session,
            StorageClass::Durable,
            &parts,
            vec![text_step("a1", "one")],
            Some(Arc::new(Notify::new())),
        ),
        backend,
    );
    labelled(&commands, "first", None, SendKey::Minted).await;
    labelled(&commands, "adopt", Some("head"), SendKey::Minted).await;
    served.abort();
    assert!(matches!(served.await, Err(error) if error.is_cancelled()));
    drop(commands);
    let session = Session::load(backend, SessionAddr(ADDR.into()), &triple(), &deployment())
        .await
        .unwrap();
    let reloaded = actor(
        session,
        StorageClass::Durable,
        &parts,
        vec![text_step("a2", "two")],
        None,
    )
    .with_class_placement(StorageClass::Durable, Vec::new());
    let (commands, served) = serve(reloaded, backend);
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    assert_eq!(session.operations().owner.adopted_session_class, None);
    assert_eq!(
        session
            .operations()
            .run
            .as_ref()
            .unwrap()
            .start
            .as_ref()
            .unwrap()
            .session_class,
        None
    );
}
