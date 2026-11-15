//! What a module answers about a session that exists nowhere: every read a caller may make before
//! the session's first send, and that none of them creates the session in either storage. Each
//! answer is pinned (see `broca_test_support::pins`).

use broca_core::WalStore as _;
use broca_protocol::{RunId, SendScope};

use super::{module, Module};
use crate::serve::{ServeHandler, SessionReadRequest};

async fn answers(module: &Module) {
    let key = &module.key;
    let run = || RunId("r1".into());
    broca_test_support::pin!(
        "send.status",
        format!(
            "{:?}",
            module.registry.send_status(key, run(), "k1", None).await
        )
    );
    broca_test_support::pin!(
        "session.read",
        format!(
            "{:?}",
            module
                .registry
                .read(SessionReadRequest::new(key.clone(), None, None))
                .await
        )
    );
    broca_test_support::pin!(
        "session.head",
        format!(
            "{:?}",
            module
                .registry
                .head_for(key, None, &SendScope::default())
                .await
        )
    );
    broca_test_support::pin!(
        "run.result",
        format!("{:?}", module.registry.run_result(key, run()).await)
    );
    let calls = serde_json::from_value(serde_json::json!({"run_id": "r1"})).unwrap();
    broca_test_support::pin!(
        "run.tool_calls",
        format!("{:?}", module.registry.run_tool_calls(key, calls).await)
    );
    broca_test_support::pin!(
        "session.baseline",
        format!(
            "{:?}",
            module
                .registry
                .baseline(key, None, &SendScope::default())
                .await
        )
    );
    broca_test_support::pin!(
        "run.status",
        format!("{:?}", module.registry.run_status_reply(key, run()).await)
    );
    broca_test_support::pin!(
        "run.admission",
        format!("{:?}", module.registry.run_admission(key, run()).await)
    );
}

/// The commands a caller may route to a session before its first send, other than a send.
async fn commands(module: &Module) {
    let key = &module.key;
    let run = || RunId("r1".into());
    let registry = &module.registry;
    broca_test_support::pin!("status", format!("{:?}", registry.route_status(key).await));
    broca_test_support::pin!(
        "cancel",
        format!("{:?}", registry.route_cancel(key, run()).await)
    );
    broca_test_support::pin!(
        "retract",
        format!(
            "{:?}",
            registry
                .route_retract(key, broca_protocol::SubmissionId("sub-r1".into()))
                .await
        )
    );
    broca_test_support::pin!(
        "warm",
        format!("{:?}", registry.route_warm(key, None).await)
    );
    broca_test_support::pin!(
        "flush prefix",
        format!(
            "{:?}",
            registry
                .route_session_flush_prefix(key, "flush-1".into())
                .await
        )
    );
    broca_test_support::pin!(
        "run state",
        format!("{:?}", registry.route_run_state(key, run()).await)
    );
    let (sink, _deliveries) = tokio::sync::mpsc::channel(64);
    broca_test_support::pin!(
        "live subscribe",
        format!("{:?}", registry.route_subscribe_live(key, sink).await)
    );
    broca_test_support::pin!(
        "tag location",
        format!("{:?}", registry.route_tag_location(key, 1).await)
    );
}

/// Every command but a send, routed to a session no send created, answers as a session that
/// doesn't exist, and leaves it so.
#[tokio::test]
async fn commands_to_a_session_that_exists_nowhere_create_nothing() {
    let module = module(true, Vec::new()).await;
    commands(&module).await;
    module.closed().await;
    assert_eq!(
        module
            .registry
            .store_holding(&module.key)
            .unwrap()
            .map(|_| ()),
        None,
        "the new storage holds nothing"
    );
    assert!(!module.head().exists(), "no head in the new storage");
    broca_test_support::pin!(
        "a log on today's storage",
        module.wal.wal_present(&module.key)
    );
}

/// Every read of a session no send created answers as a session that doesn't exist, and leaves
/// it so: no log in either storage, and no actor holding it.
#[tokio::test]
async fn reads_of_a_session_that_exists_nowhere_create_nothing() {
    let module = module(true, Vec::new()).await;
    answers(&module).await;
    module.closed().await;
    assert_eq!(
        module
            .registry
            .store_holding(&module.key)
            .unwrap()
            .map(|_| ()),
        None,
        "the new storage holds nothing"
    );
    assert!(!module.head().exists(), "no head in the new storage");
    broca_test_support::pin!(
        "a log on today's storage",
        module.wal.wal_present(&module.key)
    );
    // Read again: the answers don't depend on an earlier read.
    answers(&module).await;
}
