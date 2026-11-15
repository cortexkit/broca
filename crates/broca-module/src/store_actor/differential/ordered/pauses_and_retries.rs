//! An interrupt that ends a run paused for sign-in, keyed ordered sends after they left the
//! queue, a queued ordered send withdrawn, and a steer that leaves its model to the session under
//! a plan's model check, through today's actor and the store actor, driven by the same pair of
//! sessions the ordered sends use ([`super::Pair`]). Each reply is compared, and then what the
//! two sessions recorded.

use std::sync::Arc;

use broca_protocol::{Delivery, RunId, SubmissionId, SubmissionRequest};
use broca_session_store::keys::Cipher;
use serde_json::{json, Value};
use tokio::sync::oneshot;

use super::{core, from, reading, run_story, store, Pair};
use crate::actor::{LineageCmd, SendKey, SendOutcome, SendPlan};
use crate::executor::RunKind;

cipher_tests!(
    an_interrupt_ends_a_sign_in_pause_and_runs_alike,
    a_queued_ordered_send_is_retracted_alike,
    an_ended_or_delivered_ordered_sends_retry_is_answered_alike,
    a_steer_leaving_its_model_meets_the_plans_model_check_alike,
);

/// A run paused for sign-in owns the session, but an interrupt ends it: the interrupt is queued
/// and answered `pending`, the paused run ends `cancelled` without calling the model again, and
/// the interrupt's run answers on the next step of the script, alike. The paused run's step
/// announced a message id it never placed, which today's storage gives the interrupt's message
/// and the new storage doesn't, so message ids are set aside.
async fn an_interrupt_ends_a_sign_in_pause_and_runs_alike(cipher: Cipher) {
    let pair = Pair::new(
        vec![store::auth_failure(), store::text_step("a2", "done")],
        false,
        cipher,
    )
    .await;
    pair.first("r1").await;
    pair.idle().await;
    let interrupt = pair
        .alike_as("i1", from(core(), "stop and do this"), Delivery::Interrupt)
        .await;
    assert!(
        matches!(interrupt, SendOutcome::Pending { .. }),
        "{interrupt:?}"
    );
    pair.idle().await;
    let (ledger, queue, _) = pair.assert_alike_ids(false).await;
    assert!(queue.is_empty(), "{queue:?}");
    assert_eq!(
        run_story(&ledger),
        ["start r1", "end cancelled", "start i1", "end completed"]
    );
}

/// Retracts the queued send `submission_id` on both sides; the replies must be equal.
async fn retract_alike(pair: &Pair, submission_id: &str) -> crate::actor::RetractOutcome {
    let old = pair
        .today
        .registry
        .route_retract(&pair.today.key, SubmissionId(submission_id.into()))
        .await
        .expect("today's actor opens");
    let new = store::retract(&pair.commands, submission_id).await;
    alike!(new, old, "the retractions of {submission_id} differ");
    new
}

/// An ordered send waiting for a boundary of the running run is withdrawn before it is
/// delivered, and its retry is answered `retracted`; the run never delivers it. Alike.
async fn a_queued_ordered_send_is_retracted_alike(cipher: Cipher) {
    let pair = Pair::new(reading(), true, cipher).await;
    pair.first("r1").await;
    let order = json!({"modes": ["steer"]});
    let queued = pair
        .alike(
            "r-k1",
            from(core(), "also this"),
            Some(order.clone()),
            Some("k1"),
        )
        .await;
    assert!(matches!(queued, SendOutcome::Pending { .. }), "{queued:?}");
    assert_eq!(
        retract_alike(&pair, "sub-r-k1").await,
        crate::actor::RetractOutcome::Retracted
    );
    let retry = pair
        .alike("r-k1", from(core(), "also this"), Some(order), Some("k1"))
        .await;
    assert_eq!(
        retry,
        SendOutcome::Retracted {
            submission_id: SubmissionId("sub-r-k1".into()),
        }
    );
    pair.release();
    pair.release();
    pair.idle().await;
    let (ledger, queue, _) = pair.assert_alike().await;
    assert!(queue.is_empty(), "{queue:?}");
    assert!(
        !ledger.iter().any(|entry| entry["delivered"] == "sub-r-k1"),
        "the retracted send was never delivered: {ledger:#?}"
    );
}

/// The retry of a keyed ordered send that a later one under the same dedupe key superseded is
/// answered with that ending; the retries of sends a running run delivered, one riding on a tool
/// result and one steered in, are answered from the run that took them. Alike, but for the call
/// key a ride's receipt names, which each storage mints from its own ids (see [`keyless`]).
async fn an_ended_or_delivered_ordered_sends_retry_is_answered_alike(cipher: Cipher) {
    let pair = Pair::new(reading(), true, cipher).await;
    pair.first("r1").await;
    let riding = json!({"modes": ["tool_result"], "dedupe_key": "d"});
    let steering = json!({"modes": ["steer"]});
    let sends: [(&str, &str, &Value); 3] = [
        ("k1", "note one", &riding),
        ("k2", "note two", &riding),
        ("k3", "also this", &steering),
    ];
    for (key, text, order) in sends {
        let queued = pair
            .alike(
                &format!("r-{key}"),
                from(core(), text),
                Some(order.clone()),
                Some(key),
            )
            .await;
        assert!(matches!(queued, SendOutcome::Pending { .. }), "{queued:?}");
    }
    let superseded = pair
        .alike(
            "r-k1",
            from(core(), "note one"),
            Some(riding.clone()),
            Some("k1"),
        )
        .await;
    assert!(
        matches!(superseded, SendOutcome::SendEnded { .. }),
        "{superseded:?}"
    );
    pair.release();
    pair.release();
    pair.idle().await;
    for (key, text, order) in sends {
        let (old, new) = pair
            .send(
                &format!("r-{key}"),
                from(core(), text),
                Some(order.clone()),
                None,
                Some(key),
            )
            .await;
        alike!(
            keyless(new),
            keyless(old.clone()),
            "the retries of {key} differ"
        );
        assert!(
            !matches!(old, SendOutcome::Error(_) | SendOutcome::Pending { .. }),
            "{key}: {old:?}"
        );
    }
    pair.assert_alike().await;
}

/// `outcome` with the call key its delivery names kept only as whether it is there: each storage
/// keys a call by its own message ids, as the compared control streams rename them too.
fn keyless(mut outcome: SendOutcome) -> SendOutcome {
    if let SendOutcome::Finished {
        delivered: Some(delivered),
        ..
    }
    | SendOutcome::Active {
        delivered: Some(delivered),
        ..
    }
    | SendOutcome::Paused {
        delivered: Some(delivered),
        ..
    } = &mut outcome
    {
        if let Some(key) = delivered.call_key.as_mut() {
            *key = "<call key>".into();
        }
    }
    outcome
}

/// A plan whose one system-text item asks for the model `id` of the stub provider.
fn checking(id: &str) -> Arc<broca_fetch_plan::Plan> {
    Arc::new(broca_fetch_plan::Plan {
        text: "{}".into(),
        identity: "plan-1".into(),
        tool_items: Vec::new(),
        system_text_items: vec![broca_fetch_plan::plan::Item {
            provider: "persona".into(),
            preset: None,
            params: json!({"model": {"provider": "stub", "id": id}})
                .as_object()
                .cloned()
                .unwrap(),
            optional: false,
            output_refs: false,
        }],
        compaction_item: None,
        step_items: Vec::new(),
        composition: Default::default(),
        composition_digest: "composition".into(),
        composed: Default::default(),
        render_tags: false,
        senders: Vec::new(),
    })
}

/// A steer from the owner that leaves its model to the session, sent with the session's plan
/// checking the model `id`, on both sides. Returns today's reply and the store's.
async fn steer_checked(
    pair: &Pair,
    run_id: &str,
    text: &str,
    id: &str,
) -> (SendOutcome, SendOutcome) {
    let mut request: SubmissionRequest = from(core(), text);
    request.inherit_frozen = [broca_protocol::request::InheritedParam::Model]
        .into_iter()
        .collect();
    let plan = || SendPlan {
        identity: "plan-1".into(),
        manifest: None,
        model_check: Some(checking(id)),
    };
    let old = pair
        .today
        .registry
        .route_submission_with_order(
            &pair.today.key,
            RunId(run_id.into()),
            request.clone(),
            RunKind::FreshOrResume,
            SendKey::Minted,
            false,
            Delivery::Steer,
            Some(plan()),
            None,
            None,
            None,
            None,
            false,
            None,
        )
        .await
        .expect("today's actor opens");
    let (reply, answer) = oneshot::channel();
    pair.commands
        .send(LineageCmd::Send {
            session_class: None,
            retention: None,
            stop_sequences_named: false,
            conversation_key: None,
            title: None,
            run_id: RunId(run_id.into()),
            request: Box::new(request),
            kind: RunKind::FreshOrResume,
            key: SendKey::Minted,
            skip_materialization: false,
            delivery: Delivery::Steer,
            plan: Some(Box::new(plan())),
            order: None,
            reply,
        })
        .unwrap();
    (old, answer.await.unwrap())
}

/// A steer that leaves its model to the session joins the running run, whose model its plan's
/// model check is held to: a plan asking for another model refuses the steer by name, and one
/// asking for the run's model lets it join. Alike.
async fn a_steer_leaving_its_model_meets_the_plans_model_check_alike(cipher: Cipher) {
    let pair = Pair::new(reading(), true, cipher).await;
    pair.first("r1").await;
    let (old, new) = steer_checked(&pair, "s1", "use another", "another").await;
    alike!(new, old, "the refused steer's replies differ");
    assert!(
        matches!(
            new,
            SendOutcome::PlanRefused(broca_fetch_plan::Refusal::PlanModelMismatch { .. })
        ),
        "{new:?}"
    );
    let (old, new) = steer_checked(&pair, "s2", "also this", "m").await;
    alike!(new, old, "the admitted steer's replies differ");
    assert!(matches!(new, SendOutcome::Active { .. }), "{new:?}");
    pair.release();
    pair.release();
    pair.idle().await;
    pair.assert_alike().await;
}
