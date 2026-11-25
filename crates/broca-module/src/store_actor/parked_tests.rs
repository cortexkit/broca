use std::sync::Arc;
use std::time::Duration;

use broca_protocol::caller::ControlUnit;
use broca_protocol::display::DisplayEvent;
use tokio::sync::mpsc;

use super::new_heads_tests::{module, send, Module, IDLE};
use super::tests::{request, text_step};
use crate::actor::{SendOutcome, SubscribeOutcome};
use crate::fanout::{ControlEvent, Delivery, FromCursor};
use crate::registry::HEAD_SESSION_CLASS;

// Keep the expected delay independent of the actor's implementation.
const PARK_AFTER: Duration = Duration::from_secs(5 * 60);

async fn subscribe(module: &Module, live: bool, capacity: usize) -> mpsc::Receiver<Delivery> {
    let (sink, deliveries) = mpsc::channel(capacity);
    let registry = &module.registry;
    let attached = if live {
        registry.route_subscribe_live(&module.key, sink).await
    } else {
        registry
            .route_subscribe(&module.key, FromCursor::Start, sink)
            .await
    };
    assert_eq!(attached.unwrap(), SubscribeOutcome::Attached);
    deliveries
}

async fn send_turn(module: &Module, run: &str, retention: Option<u64>) {
    let mut later = request(run);
    if run != "r1" {
        later.tools = None;
    }
    let outcome = send(
        module,
        run,
        later,
        Some(HEAD_SESSION_CLASS),
        retention,
        None,
    )
    .await
    .unwrap();
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
}

async fn receive(deliveries: &mut mpsc::Receiver<Delivery>) -> Delivery {
    broca_test_support::wait_for("a subscription delivery", deliveries.recv(), String::new)
        .await
        .expect("the stream must stay open until the expected delivery")
}

async fn controls_through_run(
    deliveries: &mut mpsc::Receiver<Delivery>,
    run: &str,
) -> Vec<Arc<ControlEvent>> {
    let mut units = Vec::new();
    loop {
        match receive(deliveries).await {
            Delivery::Control(event) => {
                let finished = matches!(&event.1, ControlUnit::RunFinished { run_id: Some(id), .. } if id.0 == run);
                units.push(event);
                if finished {
                    return units;
                }
            }
            Delivery::Display(_) => {}
            other => panic!("unexpected subscription delivery: {other:?}"),
        }
    }
}

async fn reset(deliveries: &mut mpsc::Receiver<Delivery>) {
    let next = receive(deliveries).await;
    assert!(
        matches!(next, Delivery::Display(event) if *event == DisplayEvent::DisplayReset),
        "the attach must start with a display reset, not replayed controls"
    );
}

async fn subscribed_head(retention: Option<u64>) -> (Module, mpsc::Receiver<Delivery>) {
    let module = module(
        vec![text_step("a1", "first"), text_step("a2", "second")],
        None,
    )
    .await;
    if retention.is_some() {
        module.install_retention();
    }
    let deliveries = subscribe(&module, false, 64).await;
    send_turn(&module, "r1", retention).await;
    (module, deliveries)
}

async fn subscriber_holds(module: &Module) {
    broca_test_support::wait_until("only the subscriber to hold the idle actor", || async {
        let holds = module.registry.idle_holds(&module.key);
        if holds == ["live_subscriber"] {
            Ok(())
        } else {
            Err(format!("{holds:?}"))
        }
    })
    .await;
}

async fn age_idle(by: Duration) {
    tokio::time::pause();
    tokio::time::advance(by).await;
    // A blocked catch-up reader keeps Tokio from auto-advancing timers.
    tokio::time::advance(IDLE).await;
    tokio::time::resume();
}

async fn park(module: &Module) {
    subscriber_holds(module).await;
    age_idle(PARK_AFTER + IDLE).await;
    closed(module).await;
}

async fn closed(module: &Module) {
    broca_test_support::wait_until("the watched idle actor to park", || async {
        match module.registry.slot_state(&module.key) {
            None => Ok(()),
            state => Err(format!("actor slot: {state:?}")),
        }
    })
    .await;
}

async fn parked_counts(module: &Module) -> serde_json::Value {
    serde_json::to_value(module.registry.memory_report().await).unwrap()["parked_subscribers"]
        .clone()
}

#[tokio::test]
async fn watched_idle_actor_parks_and_leaves_the_memory_report() {
    let (module, mut deliveries) = subscribed_head(None).await;
    controls_through_run(&mut deliveries, "r1").await;
    subscriber_holds(&module).await;
    assert_eq!(module.registry.memory_report().await.largest.len(), 1);
    age_idle(PARK_AFTER / 2).await;
    assert_eq!(module.registry.slot_state(&module.key), Some("open"));
    age_idle(PARK_AFTER / 2 + IDLE).await;
    closed(&module).await;
    let report = module.registry.memory_report().await;
    assert!(report.sessions.is_empty() && report.largest.is_empty());
    assert_eq!(report.parked_subscribers.sessions, 1);
    assert_eq!(report.parked_subscribers.subscriptions, 1);
    assert_eq!(report.actor_close_reasons.get("parked"), Some(&1));
}

#[tokio::test]
async fn reopening_send_delivers_the_exact_control_suffix_to_the_parked_subscriber() {
    let (module, mut parked) = subscribed_head(None).await;
    let first = controls_through_run(&mut parked, "r1").await;
    // Unlike a cursor subscriber, a live subscriber cannot catch up work already in the log
    // when it attaches. Checking both streams detects attachment after the reopening send.
    let mut live = subscribe(&module, true, 64).await;
    reset(&mut live).await;
    park(&module).await;
    send_turn(&module, "r2", None).await;
    let suffix = controls_through_run(&mut parked, "r2").await;
    reset(&mut live).await;
    let live_suffix = controls_through_run(&mut live, "r2").await;
    let mut replay = subscribe(&module, false, 64).await;
    let history = controls_through_run(&mut replay, "r2").await;
    let positions = |events: &[Arc<ControlEvent>]| {
        events
            .iter()
            .map(|event| broca_wire::CursorWire::from(event.0))
            .collect::<Vec<_>>()
    };
    let last = first.last().unwrap().0;
    assert!(suffix[0].0 > last);
    assert_eq!(positions(&first), positions(&history[..first.len()]));
    assert_eq!(positions(&suffix), positions(&history[first.len()..]));
    assert_eq!(positions(&live_suffix), positions(&suffix));
    loop {
        match parked.try_recv() {
            Ok(Delivery::Display(_)) => {}
            Err(mpsc::error::TryRecvError::Empty) => break,
            other => {
                panic!("expected the subscription to remain open with no extra control: {other:?}")
            }
        }
    }
}

#[tokio::test]
async fn restore_refuses_a_parked_subscription_as_it_refuses_an_attached_one() {
    use crate::registry::store_move::FenceRefusal;

    let (module, mut deliveries) = subscribed_head(None).await;
    controls_through_run(&mut deliveries, "r1").await;
    subscriber_holds(&module).await;
    let attached = module
        .registry
        .restore_subscriber_busy(&module.key)
        .unwrap();
    park(&module).await;
    let refused = module
        .registry
        .fence_for_restore(&module.key, &module.leases())
        .await;
    assert!(matches!(refused, Err(FenceRefusal::Busy(reason)) if reason == attached));
    assert_eq!(
        parked_counts(&module).await,
        serde_json::json!({"sessions": 1, "subscriptions": 1})
    );
}

#[tokio::test]
async fn a_subscriber_without_a_cursor_parks_and_reattaches_live() {
    let (module, mut first) = subscribed_head(None).await;
    controls_through_run(&mut first, "r1").await;
    let mut live = subscribe(&module, true, 64).await;
    reset(&mut live).await;
    drop(first);
    park(&module).await;
    send_turn(&module, "r2", None).await;
    reset(&mut live).await;
    controls_through_run(&mut live, "r2").await;
}

#[tokio::test]
async fn a_subscriber_parks_only_after_catch_up_finishes() {
    let (module, mut first) = subscribed_head(None).await;
    controls_through_run(&mut first, "r1").await;
    let mut slow = subscribe(&module, false, 1).await;
    drop(first);
    subscriber_holds(&module).await;
    broca_test_support::wait_until("the catch-up to fill the subscriber's sink", || async {
        if slow.is_empty() {
            Err("no queued unit".into())
        } else {
            Ok(())
        }
    })
    .await;
    age_idle(PARK_AFTER + IDLE).await;
    assert_eq!(module.registry.slot_state(&module.key), Some("open"));
    controls_through_run(&mut slow, "r1").await;
    reset(&mut slow).await;
    park(&module).await;
}

#[tokio::test]
async fn retention_expires_a_parked_subscription() {
    let (module, mut deliveries) = subscribed_head(Some(3600)).await;
    controls_through_run(&mut deliveries, "r1").await;
    park(&module).await;
    assert_eq!(module.registry.sweep_once(u64::MAX).await.unwrap(), 1);
    loop {
        match receive(&mut deliveries).await {
            Delivery::Expired { expired_at_ms } => {
                assert!(expired_at_ms > 0);
                break;
            }
            Delivery::Display(_) => {}
            other => panic!("unexpected subscription delivery: {other:?}"),
        }
    }
    assert!(deliveries.recv().await.is_none());
}

#[tokio::test]
async fn the_parked_map_drops_a_subscription_whose_caller_left() {
    let (module, mut deliveries) = subscribed_head(None).await;
    controls_through_run(&mut deliveries, "r1").await;
    park(&module).await;
    assert_eq!(
        parked_counts(&module).await,
        serde_json::json!({"sessions": 1, "subscriptions": 1})
    );
    drop(deliveries);
    assert_eq!(
        parked_counts(&module).await,
        serde_json::json!({"sessions": 0, "subscriptions": 0})
    );
    assert_eq!(module.registry.slot_state(&module.key), None);
}
