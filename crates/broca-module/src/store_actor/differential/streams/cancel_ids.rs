//! A stream's message ids after a cancel, renamed by first appearance, with the check that the
//! store never reuses an id a cancelled run announced but never placed.

use std::collections::{HashMap, HashSet};

use serde_json::{json, Value};

/// Pins the store's stream after a cancel, its message ids renamed by first appearance; the
/// store never reuses an earlier message's id.
pub(in crate::store_actor::differential) fn pin_after_cancel(store: &[Value]) {
    super::pin(&identities(store));
}

fn identities(stream: &[Value]) -> Vec<Value> {
    let mut names = HashMap::new();
    let mut next = 0;
    let mut open = HashSet::new();
    let mut abandoned = HashSet::new();
    stream
        .iter()
        .map(|value| {
            let event = &value["event"];
            if event["type"] == "message_started" {
                let mid = event["mid"]
                    .as_str()
                    .expect("a message start names its message");
                if !abandoned.is_empty() {
                    assert!(
                        !names.contains_key(mid),
                        "the store reuses earlier message id {mid}"
                    );
                    abandoned.clear();
                }
                open.insert(mid.to_owned());
            } else if event["type"] == "message_finished" {
                open.remove(
                    event["mid"]
                        .as_str()
                        .expect("a message finish names its message"),
                );
            }
            if value["unit"]["type"] == "run_finished" {
                if value["unit"]["reason"] == "cancelled" {
                    abandoned = std::mem::take(&mut open);
                } else {
                    open.clear();
                    abandoned.clear();
                }
            }
            let mut value = value.clone();
            rename(&mut value, &mut names, &mut next);
            value
        })
        .collect()
}

fn rename(value: &mut Value, names: &mut HashMap<String, usize>, next: &mut usize) {
    match value {
        Value::Object(members) => {
            for (key, member) in members.iter_mut() {
                match (key.as_str(), member.as_str()) {
                    ("mid" | "call_key", Some(text)) => {
                        let id = *names.entry(text.to_owned()).or_insert_with(|| {
                            let id = *next;
                            *next += 1;
                            id
                        });
                        *member = json!(format!("{key}#{id}"));
                    }
                    _ => rename(member, names, next),
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(|item| rename(item, names, next)),
        _ => {}
    }
}

fn cancelled(next: &str) -> Vec<Value> {
    vec![
        json!({"unit":{"type":"message_boundary","mid":"old"}, "event":{"type":"message_started","mid":"old"}}),
        json!({"unit":{"type":"run_finished","reason":"cancelled"}, "event":null}),
        json!({"unit":{"type":"message_boundary","mid":next}, "event":{"type":"message_started","mid":next}}),
        json!({"unit":{"type":"message_boundary","mid":next}, "event":{"type":"message_finished","mid":next}}),
    ]
}

#[test]
fn a_fresh_message_after_a_cancel_is_named_anew() {
    let renamed = identities(&cancelled("fresh"));
    assert_eq!(renamed[2]["unit"]["mid"], "mid#1");
}

#[test]
#[should_panic(expected = "reuses earlier message id")]
fn a_message_cannot_reuse_the_abandoned_id() {
    identities(&cancelled("old"));
}
