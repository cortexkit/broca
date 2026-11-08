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

/// The two streams are equal, unit for unit; a difference is named by its first unit.
pub(super) fn assert_alike(store: &[Value], today: &[Value]) {
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
