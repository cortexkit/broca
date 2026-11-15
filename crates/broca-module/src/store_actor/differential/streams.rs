//! The control streams of a comparison's two sides, as a client compares them: every unit a
//! subscriber from the session's start gets, its payload only. A resume cursor names each
//! storage's own records, so cursors aren't compared; message ids and call keys are minted by
//! each storage its own way, so each becomes its order of first appearance, and two streams
//! compare equal only if their ids line up one to one.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use broca_protocol::BindIdentity;
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};

use crate::actor::{LineageCmd, SubscribeOutcome};
use crate::fanout::{ControlEvent, Delivery, FromCursor};
use crate::registry::LineageRegistry;

/// Every control unit delivered until the stream stays quiet; display deltas are left out.
async fn drain(mut deliveries: mpsc::Receiver<Delivery>) -> Vec<Arc<ControlEvent>> {
    let mut out = Vec::new();
    while let Ok(Some(delivery)) =
        tokio::time::timeout(Duration::from_millis(300), deliveries.recv()).await
    {
        match delivery {
            Delivery::Control(event) => out.push(event),
            Delivery::Display(_) => {}
            other => panic!("a subscriber from the start got {other:?}"),
        }
    }
    out
}

/// The stream of `key` through `registry`'s subscribe route, to whichever actor serves it.
pub(super) async fn routed(registry: &LineageRegistry, key: &BindIdentity) -> Vec<Value> {
    let (sink, deliveries) = mpsc::channel(4096);
    let attached = registry
        .route_subscribe(key, FromCursor::Start, sink)
        .await
        .expect("today's actor opens");
    assert_eq!(attached, SubscribeOutcome::Attached);
    normalized(&drain(deliveries).await)
}

/// The stream of the store actor `commands` reach.
pub(super) async fn store(commands: &mpsc::UnboundedSender<LineageCmd>) -> Vec<Value> {
    let (sink, deliveries) = mpsc::channel(4096);
    let (reply, answer) = oneshot::channel();
    commands
        .send(LineageCmd::Subscribe {
            from: FromCursor::Start,
            live: false,
            sink,
            reply,
        })
        .unwrap();
    assert_eq!(answer.await.unwrap(), SubscribeOutcome::Attached);
    normalized(&drain(deliveries).await)
}

/// The two streams are equal, unit for unit; a difference is named by its first unit. The
/// store's stream is pinned (see `broca_test_support::pins`).
pub(super) fn assert_alike(store: &[Value], today: &[Value]) {
    broca_test_support::pin!(
        "control stream",
        broca_test_support::pins::Text(serde_json::to_string_pretty(store).unwrap())
    );
    let same = store
        .iter()
        .zip(today)
        .take_while(|(store, today)| store == today)
        .count();
    assert!(
        same == store.len() && same == today.len(),
        "the control streams differ at unit {same} of {} (store) and {} (today):\nstore: {:#}\ntoday: {:#}",
        store.len(),
        today.len(),
        store.get(same).cloned().unwrap_or_default(),
        today.get(same).cloned().unwrap_or_default(),
    );
}

mod cancel_ids;
pub(super) use cancel_ids::assert_alike_after_cancel;

fn normalized(events: &[Arc<ControlEvent>]) -> Vec<Value> {
    let mut names = HashMap::new();
    events
        .iter()
        .map(|event| {
            let mut value = json!({"unit": event.1, "event": event.2});
            rename(&mut value, &mut names);
            value
        })
        .collect()
}

/// Renames, anywhere in `value`, every message id and call key by its order of first
/// appearance, and marks every time as present.
fn rename(value: &mut Value, names: &mut HashMap<String, usize>) {
    match value {
        Value::Object(members) => {
            members.remove("awaits");
            if members.get("reason") == Some(&serde_json::json!("tool_held"))
                && members.contains_key("state")
            {
                members.remove("reason");
            }
            for (key, member) in members.iter_mut() {
                match (key.as_str(), member.as_str()) {
                    ("mid" | "call_key", Some(text)) => {
                        let next = names.len();
                        let name = *names.entry(text.to_owned()).or_insert(next);
                        *member = json!(format!("{key}#{name}"));
                    }
                    // A time each side's clock takes: whether one is there is still compared.
                    _ if member.is_number()
                        && (key.ends_with("_at") || key.ends_with("_at_ms")) =>
                    {
                        *member = json!("<time>");
                    }
                    _ => rename(member, names),
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(|item| rename(item, names)),
        _ => {}
    }
}

/// The durable controls preceding the display snapshot of one live attach.
async fn before_display_reset(mut deliveries: mpsc::Receiver<Delivery>) -> Vec<Arc<ControlEvent>> {
    let mut controls = Vec::new();
    loop {
        let delivery = broca_test_support::wait_until("live attach delivery", || {
            std::future::ready(deliveries.try_recv().map_err(|error| format!("{error:?}")))
        })
        .await;
        match delivery {
            Delivery::Control(event) => controls.push(event),
            Delivery::Display(event)
                if matches!(*event, broca_protocol::DisplayEvent::DisplayReset) =>
            {
                return controls
            }
            other => panic!("unexpected delivery before display_reset: {other:?}"),
        }
    }
}

async fn live_actor(commands: &mpsc::UnboundedSender<LineageCmd>) -> mpsc::Receiver<Delivery> {
    let (sink, deliveries) = mpsc::channel(4096);
    let (reply, answer) = oneshot::channel();
    commands
        .send(LineageCmd::Subscribe {
            from: FromCursor::Start,
            live: true,
            sink,
            reply,
        })
        .unwrap();
    assert_eq!(answer.await.unwrap(), SubscribeOutcome::Attached);
    deliveries
}

async fn live_routed(registry: &LineageRegistry, key: &BindIdentity) -> mpsc::Receiver<Delivery> {
    let (sink, deliveries) = mpsc::channel(4096);
    assert_eq!(
        registry.route_subscribe_live(key, sink).await.unwrap(),
        SubscribeOutcome::Attached
    );
    deliveries
}

async fn live_attach_during_step(seed_held: bool) {
    use super::super::tests as rig;
    use crate::serve::ServeHandler;
    use crate::store_reads::{StoreReadOutcome, StoreReadReply, StoreReadRequest};
    let steps = vec![rig::text_step("a1", "hello")];
    let old_gate = Arc::new(tokio::sync::Notify::new());
    let new_gate = Arc::new(tokio::sync::Notify::new());
    let seed_gate = seed_held.then(|| Arc::new(tokio::sync::Notify::new()));
    let today = super::today(steps.clone(), Some(Arc::clone(&old_gate)));
    let (commands, served, _) = rig::actor_with_seed_gate(
        rig::executor(steps, Some(Arc::clone(&new_gate))),
        broca_session_loop::parts::SharedParts::new(Box::new(
            broca_session_loop::parts::MemoryParts::default(),
        )),
        seed_gate.clone(),
    )
    .await;
    assert!(matches!(
        today
            .send(
                "r1",
                rig::request("first"),
                crate::actor::SendKey::Minted,
                broca_protocol::Delivery::Queue,
                None
            )
            .await,
        crate::actor::SendOutcome::Active { .. }
    ));
    assert!(matches!(
        rig::send(&commands, "r1", "first").await,
        crate::actor::SendOutcome::Active { .. }
    ));
    broca_test_support::wait_until("both model steps in flight", || async {
        let old = serde_json::to_value(
            today
                .registry
                .run_status_reply(&today.key, broca_protocol::RunId("r1".into()))
                .await
                .unwrap(),
        )
        .unwrap();
        let (reply, answer) = oneshot::channel();
        commands
            .send(LineageCmd::StoreRead {
                request: Box::new(StoreReadRequest::RunStatusReply(broca_protocol::RunId(
                    "r1".into(),
                ))),
                reply,
            })
            .unwrap();
        let StoreReadOutcome::Served(answer) = answer.await.unwrap() else {
            return Err("read not in memory".into());
        };
        let StoreReadReply::RunStatusReply(Ok(answer)) = *answer else {
            return Err("status refused".into());
        };
        let new = serde_json::to_value(answer).unwrap();
        if old["step"]["step_id"] == 1 && new["step"]["step_id"] == 1 {
            Ok(())
        } else {
            Err("step not in flight".into())
        }
    })
    .await;
    let old = before_display_reset(live_routed(&today.registry, &today.key).await).await;
    alike_today_first!(old.len(), 1);
    alike_today_first!(
        old[0].1,
        broca_protocol::ControlUnit::StepStarted {
            step_id: broca_protocol::StepId(1)
        }
    );
    let mut deliveries = live_actor(&commands).await;
    if let Some(seed_gate) = seed_gate {
        assert!(matches!(
            deliveries.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        // The run ends before seeding catches up. The attach must still receive its earlier H,
        // not the later empty step state.
        new_gate.notify_one();
        rig::idle(&commands).await;
        seed_gate.notify_one();
    }
    let new = before_display_reset(deliveries).await;
    assert_alike(&normalized(&new), &normalized(&old));
    old_gate.notify_one();
    new_gate.notify_one();
    today.idle().await;
    rig::idle(&commands).await;
    assert!(
        before_display_reset(live_routed(&today.registry, &today.key).await)
            .await
            .is_empty()
    );
    assert!(before_display_reset(live_actor(&commands).await)
        .await
        .is_empty());
    drop(commands);
    served.await.unwrap();
    assert!(today.registry.stop_and_seal().await);
}

#[tokio::test]
async fn a_mid_step_live_attach_replays_controls_before_display_reset_alike() {
    live_attach_during_step(false).await;
}

#[tokio::test]
async fn a_mid_step_live_attach_while_seeding_uses_its_exact_barrier() {
    live_attach_during_step(true).await;
}
