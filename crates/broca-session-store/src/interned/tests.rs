use std::sync::Arc;

use futures::executor::block_on;

use super::{Interned, Interner, SharedJson};
use crate::backend::{MemoryBackend, SessionAddr};
use crate::codec::Value as _;
use crate::head::SessionTriple;
use crate::keys::{Cipher, DeploymentKey};
use crate::record::{EntryFacts, Record, TurnFacts};
use crate::session::Session;
use crate::synthetic::{write, Shape};

fn triple() -> SessionTriple {
    SessionTriple {
        project_root: "/synthetic".into(),
        harness: "broca".into(),
        session: "interned".into(),
    }
}

fn deployment() -> DeploymentKey {
    DeploymentKey::new(1, &[5; 32]).creating(Cipher::XChaCha20Poly1305)
}

fn addr() -> SessionAddr {
    SessionAddr("1a7e2ed01a7e2ed0".into())
}

/// Every pair of equal values among `values` is the one copy.
fn one_copy_each<T: ?Sized + PartialEq>(values: &[&Interned<T>]) -> bool {
    values.iter().all(|a| {
        values
            .iter()
            .filter(|b| ***a == ****b)
            .all(|b| Interned::same(a, b))
    })
}

/// A session's facts, loaded and appended, hold one copy of each run id, sender, module and
/// schema pin they repeat; the kept records share each message and its facts with the fold.
#[test]
fn a_session_holds_one_copy_of_what_its_messages_and_facts_repeat() {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(), false)
            .await
            .unwrap();
        write(&mut session, &Shape::production(60)).await.unwrap();
        let appended = session.conversation.clone();
        session.close().await.unwrap();
        let loaded = Session::load(&backend, addr(), &triple(), &deployment())
            .await
            .unwrap();
        for conversation in [&appended, &loaded.conversation] {
            let facts: Vec<&EntryFacts> = conversation
                .entries
                .iter()
                .filter_map(|entry| entry.facts.as_deref())
                .collect();
            assert!(facts.len() > 20, "{} facts", facts.len());
            let runs: Vec<_> = facts.iter().filter_map(|f| f.run_id.as_ref()).collect();
            let senders: Vec<_> = facts.iter().filter_map(|f| f.sender.as_ref()).collect();
            assert!(runs.len() > 20 && senders.len() > 5);
            assert!(one_copy_each(&runs), "run ids held more than once");
            assert!(one_copy_each(&senders), "senders held more than once");
            let calls: Vec<_> = conversation
                .entries
                .iter()
                .filter_map(|entry| entry.turn.as_deref())
                .flat_map(|turn| &turn.calls)
                .collect();
            assert!(calls.len() > 5, "{} calls", calls.len());
            let modules: Vec<_> = calls
                .iter()
                .filter_map(|c| c.dispatched_to.as_ref())
                .collect();
            assert!(one_copy_each(&modules), "modules held more than once");
            let pins: Vec<_> = calls.iter().filter_map(|c| c.schema_pin.as_ref()).collect();
            assert!(pins.iter().all(|a| pins
                .iter()
                .filter(|b| a == *b)
                .all(|b| SharedJson::same(a, b))));
        }
        // The loaded writer keeps its latest run's records, sharing each message and its facts
        // with the conversation.
        let latest = &loaded.conversation.episodes.last().unwrap().run_id;
        let start = loaded.kept_run_start(latest).unwrap();
        let kept = loaded.kept_from(start).unwrap();
        let shared = loaded.conversation.shared_addresses();
        let mut messages = 0;
        let mut facts = 0;
        for (_, record) in &kept {
            match record {
                Record::Input(input) => {
                    messages += 1;
                    assert!(shared.contains(&(Arc::as_ptr(&input.message) as usize)));
                }
                Record::Assistant(assistant) => {
                    messages += 1;
                    assert!(shared.contains(&(Arc::as_ptr(&assistant.message) as usize)));
                }
                Record::EntryFacts(entry) => {
                    facts += 1;
                    assert!(shared.contains(&(Arc::as_ptr(entry) as usize)));
                }
                _ => {}
            }
        }
        assert!(messages > 0 && facts > 0, "{} kept records", kept.len());
    });
}

/// Equal JSON text read twice is one value, written back as the same bytes; reading it again
/// parses nothing.
#[test]
fn equal_json_text_is_read_as_one_shared_value() {
    let value = serde_json::json!({"tool": "read", "schema": {"type": "object"}, "v": 3});
    let mut text = Vec::new();
    value.write(&mut text);
    let first = SharedJson::read(crate::codec::Raw::Len(&text)).unwrap();
    let second = SharedJson::read(crate::codec::Raw::Len(&text)).unwrap();
    assert!(SharedJson::same(&first, &second));
    assert_eq!(*first, value);
    let mut written = Vec::new();
    second.write(&mut written);
    assert_eq!(written, text);
    // A value built in memory is the one read from the same text.
    assert!(SharedJson::same(&SharedJson::new(value), &first));
    // Text other than the compact form still refuses, as a plain JSON field does.
    assert!(SharedJson::read(crate::codec::Raw::Len(b"{ \"a\":1}")).is_err());
}

/// The interner lets go of a value no record holds once it has doubled since it last did.
#[test]
fn the_interner_keeps_only_what_records_hold() {
    let mut interner = Interner::default();
    let mut kept = Vec::new();
    for n in 0..200 {
        let mut record = Record::TurnFacts(Arc::new(TurnFacts {
            ordinal: n,
            calls: vec![crate::record::CallFacts {
                tool_call_id: broca_protocol::ids::ToolCallId(format!("c{n}")),
                call_key: format!("k{n}"),
                schema_pin: None,
                carrier: None,
                dispatched_to: Some(format!("module-{n}").into()),
                dispatched_at_ms: None,
                finished_at_ms: None,
                unanswered: None,
            }],
        }));
        interner.record(&mut record);
        // Only every tenth record stays alive.
        if n % 10 == 0 {
            kept.push(record);
        }
    }
    assert!(
        interner.len() < 100,
        "the interner holds {} values for {} records",
        interner.len(),
        kept.len()
    );
    let encoded = kept[0].encode_body();
    assert_eq!(Record::decode(kept[0].kind(), &encoded).unwrap(), kept[0]);
}
