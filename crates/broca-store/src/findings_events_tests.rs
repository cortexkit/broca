use super::*;
use crate::bus_events::{drain_bus_event, BusPublisher, DrainResult, RunEventStore};
use cortexkit_bus_trait::{ContentDigest, PublishReceipt};
use std::sync::Mutex;

fn enabled() -> SqliteStore {
    let store = SqliteStore::open_in_memory().unwrap();
    store.enable_bus_events("box_test").unwrap();
    store
}

/// A bus that keeps what it is sent.
#[derive(Default)]
struct Sent(Mutex<Vec<Publication>>);

#[async_trait::async_trait]
impl BusPublisher for Sent {
    async fn publish(&self, event: &Publication) -> Result<PublishReceipt, String> {
        let mut sent = self.0.lock().unwrap();
        sent.push(event.clone());
        Ok(PublishReceipt::new(sent.len() as u64, false))
    }
}

/// A changed digest writes one event whose body names it and the worst severity, in synapse's
/// member order; the same digest again writes nothing, and a change back writes the next one.
#[test]
fn an_event_is_written_only_when_the_findings_change() {
    let store = enabled();
    let first = store.record_findings("a", "error", 10).unwrap().unwrap();
    assert_eq!(store.record_findings("a", "error", 20).unwrap(), None);
    let second = store.record_findings("b", "ok", 30).unwrap().unwrap();
    assert_ne!(first, second);
    let stored = store.findings_event(&first).unwrap().unwrap();
    assert_eq!(
        String::from_utf8(stored.body).unwrap(),
        format!(r#"{{"event_id":"{first}","generation":1,"worst_severity":"error"}}"#)
    );
    assert_eq!(stored.at_ms, 10);
    let stored = store.findings_event(&second).unwrap().unwrap();
    assert_eq!(
        String::from_utf8(stored.body).unwrap(),
        format!(r#"{{"event_id":"{second}","generation":2,"worst_severity":"ok"}}"#)
    );
    assert_eq!(store.findings_event("unknown").unwrap(), None);
}

/// Before the bus account is known nothing is recorded, so the change is still a change once it
/// is.
#[test]
fn nothing_is_recorded_before_the_bus_account_is_known() {
    let store = SqliteStore::open_in_memory().unwrap();
    assert_eq!(store.record_findings("a", "warn", 10).unwrap(), None);
    store.enable_bus_events("box_test").unwrap();
    assert!(store.record_findings("a", "warn", 20).unwrap().is_some());
}

/// Event keys are random version 4 UUIDs.
#[test]
fn event_keys_are_version_4_uuids() {
    let key = event_key().unwrap();
    let groups: Vec<_> = key.split('-').map(str::len).collect();
    assert_eq!(groups, [8, 4, 4, 4, 12]);
    assert!(key.bytes().all(|b| b == b'-' || b.is_ascii_hexdigit()));
    assert_eq!(&key[14..15], "4");
    assert!(matches!(&key[19..20], "8" | "9" | "a" | "b"));
    assert_ne!(key, event_key().unwrap());
}

/// The drain sends the event as a claim-check notice: broca's subject, the key as message id
/// and `event_key` header, an empty payload and the stored body's digest; once acknowledged it
/// isn't sent again, and its body stays readable.
#[tokio::test]
async fn the_drain_sends_an_event_once_as_a_claim_check_notice() {
    let store = enabled();
    let key = store.record_findings("a", "error", 10).unwrap().unwrap();
    let bus = Sent::default();
    assert!(matches!(
        drain_bus_event(&store, &bus, 11).await.unwrap(),
        DrainResult::Published
    ));
    assert!(matches!(
        drain_bus_event(&store, &bus, 12).await.unwrap(),
        DrainResult::Empty
    ));
    let sent = bus.0.into_inner().unwrap();
    assert_eq!(sent.len(), 1);
    let notice = &sent[0];
    assert_eq!(
        notice.subject,
        "ck.box_test.event.broca.findings_changed.v1"
    );
    assert_eq!(notice.event_key, key);
    assert_eq!(notice.headers.get(EVENT_KEY_HEADER), Some(&key));
    assert!(
        !notice.inline,
        "the payload is empty; the body is read with events_get"
    );
    let body = store.findings_event(&key).unwrap().unwrap().body;
    assert_eq!(notice.digest(), ContentDigest::of_bytes(&body));
    assert_eq!(
        store.pending_findings_event().unwrap().map(|p| p.event_key),
        None
    );
}

/// Events are kept seven days, then pruned whether sent or not.
#[test]
fn events_are_kept_seven_days() {
    let store = enabled();
    let key = store.record_findings("a", "error", 10).unwrap().unwrap();
    store.prune_bus_events(10 + FINDINGS_RETENTION_MS).unwrap();
    assert!(store.findings_event(&key).unwrap().is_some());
    store.prune_bus_events(11 + FINDINGS_RETENTION_MS).unwrap();
    assert_eq!(store.findings_event(&key).unwrap(), None);
}
