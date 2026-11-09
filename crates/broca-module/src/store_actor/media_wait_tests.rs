//! A run on a session kept in the new storage waits for the session's media fold before its
//! first model call, while the fold is built off the actor: the actor keeps answering commands
//! meanwhile, and a cancel during the wait ends the run as a cancel during a model call does.

use std::sync::Arc;

use broca_protocol::RunId;
use broca_session_loop::parts::{MemoryParts, SharedParts};
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::record::Record;
use broca_session_store::session::Session;
use tokio::sync::{mpsc, oneshot, Notify};

use super::tests::{
    bind, deployment, executor, idle, request, send, send_as, text_step, triple, ADDR,
};
use super::StoreActor;
use crate::actor::{CancelOutcome, LineageCmd, SendKey, SendOutcome};

#[tokio::test]
async fn a_steer_and_a_cancel_are_answered_while_the_media_fold_builds() {
    let mut executor = executor(
        vec![text_step("a1", "never sent"), text_step("a2", "never sent")],
        None,
    );
    executor.waits_for_media = true;
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let session = Session::create(
        backend,
        SessionAddr(ADDR.into()),
        triple(),
        &deployment(),
        false,
    )
    .await
    .unwrap();
    // The fold's build is held, as a long session's build takes seconds.
    let build = Arc::new(Notify::new());
    let actor = StoreActor::new(
        bind(),
        session,
        Arc::new(executor),
        Box::new(SharedParts::new(Box::new(MemoryParts::default()))),
    )
    .with_readers(Arc::new(move || backend))
    .with_media_build_gate(Arc::clone(&build));
    let (commands, receiver) = mpsc::unbounded_channel();
    let commands = super::tests::Commands::over(commands, backend);
    let served = tokio::spawn(actor.serve(receiver));

    // The run's start is durable while the fold builds: the send is answered.
    assert!(matches!(
        send(&commands, "r1", "first").await,
        SendOutcome::Active { .. }
    ));
    // A steer joins the run waiting for the fold.
    assert_eq!(
        send_as(
            &commands,
            "s1",
            request("steered"),
            SendKey::Minted,
            broca_protocol::Delivery::Steer
        )
        .await,
        SendOutcome::Active {
            run_id: RunId("r1".into()),
            delivered: None
        }
    );
    // A cancel is answered too, and ends the run before its first model call.
    let (reply, answer) = oneshot::channel();
    commands
        .send(LineageCmd::Cancel {
            run_id: RunId("r1".into()),
            at: broca_wal::record::RunCancelAt::Now,
            reply,
        })
        .unwrap();
    assert_eq!(answer.await.unwrap(), CancelOutcome::Ack);
    idle(&commands).await;
    // Everything above was answered while the build waited. It is let through only now, so the
    // test's runtime doesn't wait on it forever.
    build.notify_one();
    drop(commands);
    let session = served.await.unwrap();
    let run = session.operations.run.as_ref().unwrap();
    assert_eq!(
        run.finished,
        Some(broca_protocol::RunFinishReason::Cancelled)
    );
    let records = session.open_log_records().await.unwrap();
    assert!(
        records
            .iter()
            .all(|(_, record)| !matches!(record, Record::Assistant(_) | Record::Attempt(_))),
        "no model call was made"
    );
    assert!(records
        .iter()
        .any(|(_, record)| matches!(record, Record::Steer(_))));
}
