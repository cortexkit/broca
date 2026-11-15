//! A session's tools, frozen by its creating send, through the store actor, driven by the
//! session the ordered sends use ([`super::Pair`]). A later send that names tools is refused by
//! the frozen-tools rule, ahead of the other refusals it also meets; a retry of the creating send
//! names them again and is answered as the first; and a later send that leaves them out runs on
//! the frozen tools.

use std::sync::Arc;

use broca_protocol::SubmissionRequest;
use broca_session_store::keys::Cipher;

use super::{core, from, hello, plan, store, Pair};
use crate::actor::SendOutcome;

cipher_tests!(
    a_creating_sends_tools_are_frozen_alike,
    a_send_naming_tools_and_another_plan_is_refused_for_its_tools_alike,
);

/// Requires `outcome` to be the refusal of a send naming tools on a session whose tools are
/// frozen.
fn frozen(outcome: &SendOutcome) {
    assert_eq!(
        outcome,
        &crate::plan_episode::tools_frozen_refusal(),
        "the send naming tools is refused for them"
    );
}

/// A send of `text` naming the `read` tool, the tools of an unplanned session's creating send.
fn naming_tools(text: &str) -> SubmissionRequest {
    store::request(text)
}

/// The names of the tools each model call of `calls` was offered.
fn offered(calls: Vec<broca_protocol::request::CallOptions>) -> Vec<Vec<String>> {
    calls
        .iter()
        .map(|call| call.tools.iter().map(|tool| tool.name.clone()).collect())
        .collect()
}

/// On a session without a plan, the creating send, keyed by the caller's `send_id`, names the
/// `read` tool and freezes it, alike. Its retry names the same tools and is answered from its
/// run. A later send naming tools is refused for them, and so is one that also carries a plan the
/// session never had: the tools are checked first. A later send leaving tools out runs on the
/// `read` tool.
async fn a_creating_sends_tools_are_frozen_alike(cipher: Cipher) {
    let pair = Pair::new(hello(), false, cipher).await;
    let new = pair
        .send("r-k1", naming_tools("first"), None, None, Some("k1"))
        .await;
    assert!(matches!(new, SendOutcome::Active { .. }), "{new:?}");
    broca_test_support::pin!("the creating send", new);
    pair.idle().await;
    let new = pair
        .send("r-k1", naming_tools("first"), None, None, Some("k1"))
        .await;
    assert!(matches!(new, SendOutcome::Finished { .. }), "{new:?}");
    broca_test_support::pin!("the creating send's retry", new);
    let new = pair
        .send("r2", naming_tools("second"), None, None, None)
        .await;
    frozen(&new);
    broca_test_support::pin!("a later send naming tools", new);
    let new = pair
        .send("r3", naming_tools("third"), None, Some(plan()), None)
        .await;
    frozen(&new);
    broca_test_support::pin!("a later send naming tools and carrying a plan", new);
    let mut leaving = naming_tools("fourth");
    leaving.tools = None;
    let new = pair.send("r4", leaving, None, None, None).await;
    assert!(matches!(new, SendOutcome::Active { .. }), "{new:?}");
    broca_test_support::pin!("a later send leaving tools out", new);
    pair.idle().await;
    let offered = offered(pair.store.provider.inner.calls());
    assert_eq!(offered, [["read"], ["read"]], "the runs' tools");
    broca_test_support::pin!("the runs' tools", offered);
    pair.assert_alike().await;
}

/// On a planned session, a later send that names tools and carries another plan meets both
/// refusals; both sessions refuse it for its tools, which are checked before its plan.
async fn a_send_naming_tools_and_another_plan_is_refused_for_its_tools_alike(cipher: Cipher) {
    let pair = Pair::new(hello(), false, cipher).await;
    pair.first("r1").await;
    pair.idle().await;
    let mut another = (*plan()).clone();
    another.plan_identity = "plan-2".into();
    let mut named = from(core(), "second");
    named.tools = naming_tools("second").tools;
    let new = pair
        .send("r2", named, None, Some(Arc::new(another)), None)
        .await;
    frozen(&new);
    broca_test_support::pin!("a later send naming tools and another plan", new);
    let new = pair
        .send("r3", from(core(), "third"), None, Some(plan()), None)
        .await;
    assert!(matches!(new, SendOutcome::Active { .. }), "{new:?}");
    broca_test_support::pin!("a later send leaving tools out", new);
    pair.idle().await;
    pair.assert_alike().await;
}
