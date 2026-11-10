//! Compare identities without treating a legacy position reused after cancel as a native alias.

use std::collections::{HashMap, HashSet};

use serde_json::{json, Value};

/// Every unit and identity agrees, except that today's next message may reuse the position of
/// an announced but unplaced message from a cancelled run. The native store must mint a fresh
/// identity there; references to every other message and call remain compared.
pub(in crate::store_actor::differential) fn assert_alike_after_cancel(
    store: &[Value],
    today: &[Value],
) {
    super::assert_alike(&identities(store, true), &identities(today, false));
}

fn identities(stream: &[Value], native: bool) -> Vec<Value> {
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
                    if native {
                        assert!(
                            !names.contains_key(mid),
                            "the native store reuses earlier message id {mid}"
                        );
                    } else if abandoned.contains(mid) {
                        // Only this first message after cancellation may take the abandoned position.
                        names.remove(mid);
                    }
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
fn only_the_abandoned_legacy_position_is_split() {
    assert_alike_after_cancel(&cancelled("fresh"), &cancelled("old"));
}

#[test]
#[should_panic(expected = "reuses earlier message id")]
fn a_native_message_cannot_reuse_the_abandoned_id() {
    assert_alike_after_cancel(&cancelled("old"), &cancelled("old"));
}

#[test]
#[should_panic(expected = "control streams differ")]
fn a_placed_message_cannot_be_aliased() {
    let mut today = cancelled("old");
    let mut store = cancelled("fresh");
    for stream in [&mut today, &mut store] {
        stream.insert(
            1,
            json!({"unit":{"type":"message_boundary","mid":"old"}, "event":{"type":"message_finished","mid":"old"}}),
        );
    }
    assert_alike_after_cancel(&store, &today);
}

#[test]
#[should_panic(expected = "control streams differ")]
fn cancellation_does_not_mask_other_payload_differences() {
    let mut store = cancelled("fresh");
    store[2]["unit"]["unexpected"] = json!(true);
    assert_alike_after_cancel(&store, &cancelled("old"));
}
