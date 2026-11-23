use broca_protocol::caller::RunFinishReason;
use broca_protocol::content::{ContentBlock, MediaRef, Message, Role};
use broca_protocol::request::ModelHandle;
use futures::executor::block_on;
use sha2::{Digest as _, Sha256};

use super::{is_broca_id_form, NewInputMedia, MAX_BATCH_RECORDS};
use crate::backend::{MemoryBackend, SessionAddr, SessionBackend as _};
use crate::codec::{hex, DecodeErrorKind, Message as _};
use crate::head::SessionTriple;
use crate::keys::{Cipher, DeploymentKey};
use crate::record::{
    CallFacts, EntryFacts, ImportReceipt, ImportedIds, ImportedMove, Input, InputMedia, Placement,
    Record, RunEnd, RunStart, TurnFacts,
};
use crate::session::{Session, SessionError};

cipher_tests!(
    an_import_stores_its_media_and_resolves_its_ids_before_and_after_a_seal,
    a_damaged_id_map_reads_as_damage,
    turn_facts_attach_to_their_message,
    an_imported_tool_turn_reads_with_its_results_after_a_seal,
    an_import_larger_than_one_batch_reads_back_whole,
    a_replacement_keeps_the_session_and_takes_its_place_once_published,
);

fn triple() -> SessionTriple {
    SessionTriple {
        project_root: "/project".into(),
        harness: "broca".into(),
        session: "head".into(),
    }
}

/// The test deployment key, creating sessions in mode `cipher`.
fn deployment(cipher: Cipher) -> DeploymentKey {
    DeploymentKey::new(1, &[5; 32]).creating(cipher)
}

fn addr() -> SessionAddr {
    SessionAddr("0123456789abcdef".into())
}

fn model() -> ModelHandle {
    serde_json::from_value(
        serde_json::json!({"provider_module_id": "import", "model_id": "import"}),
    )
    .unwrap()
}

fn image(bytes: &[u8]) -> MediaRef {
    serde_json::from_value(serde_json::json!({
        "kind": "image",
        "mime": "image/png",
        "byte_size": bytes.len(),
        "sha256": hex(&Sha256::digest(bytes)),
        "width": 1,
        "height": 1,
    }))
    .unwrap()
}

fn start(run: &str, seeded: bool) -> Record {
    Record::RunStart(Box::new(RunStart {
        named_speaker: None,
        kept_notice: None,
        run_id: run.into(),
        model: model(),
        setup: Vec::new(),
        renderer_version: 0,
        submission_id: None,
        send_id: None,
        request_identity_digest: None,
        ts_ms: None,
        written_at_ms: None,
        session_class: None,
        retention_secs: None,
        title: None,
        context_limit: None,
        service_tier: None,
        route_decision_id: None,
        compaction_plan_version: None,
        auth_selection: None,
        run_choices: None,
        origin: None,
        principal: None,
        scope: None,
        hooks: None,
        keep_warm: None,
        on_restart: None,
        seeded: seeded.then_some(true),
        refusal_schedule: None,
        imported_ids: None,
        render_identity: None,
        message_marks: None,
    }))
}

fn end(run: &str) -> Record {
    Record::RunEnd(RunEnd {
        run_id: run.into(),
        reason: RunFinishReason::Completed,
        usage: None,
        ts_ms: None,
        retries_used: 0,
        interrupt_reason: None,
        error: None,
    })
}

fn input(run: &str, placement: Placement, role: Role, content: Vec<ContentBlock>) -> Record {
    Record::Input(Input {
        run_id: run.into(),
        placement,
        message: std::sync::Arc::new(Message::new(role, content)),
        written_at_ms: None,
        submission_id: None,
        notice: None,
        tags_v1: None,
        receipt_seq: None,
        pruned: Vec::new(),
        media: Vec::new(),
    })
}

fn text(text: &str) -> ContentBlock {
    ContentBlock::Text { text: text.into() }
}

fn facts(ordinal: u64, mid: &str) -> Record {
    Record::EntryFacts(std::sync::Arc::new(EntryFacts {
        send_id: None,
        named_speaker: None,
        kept_notice: None,
        ordinal,
        written_at_ms: Some(ordinal),
        tag_base: 0,
        notice: None,
        usage: None,
        generated_under: None,
        native: false,
        render_tags: false,
        clean_reply: false,
        mid: mid.into(),
        run_id: None,
        created_at_ms: None,
        delivery_mode: None,
        delivery_tool_call_id: None,
        client_message_id: None,
        sender: None,
        marks: None,
        no_display_facts: None,
        reasoning_summaries: Vec::new(),
        rich_ui: None,
        notices: Vec::new(),
        runner_tag_base: None,
        legacy_view_seq: None,
    }))
}

fn receipt() -> ImportReceipt {
    ImportReceipt {
        import_id: "imp-1".into(),
        digest: "d".into(),
        shape: "runner".into(),
        moves: vec![
            ImportedMove {
                mid: "msg_u1".into(),
                ordinal: Some(1),
                finished_summary: None,
            },
            ImportedMove {
                mid: "msg_sys".into(),
                ordinal: None,
                finished_summary: None,
            },
        ],
        imported_messages: 3,
        dropped_system_messages: 1,
    }
}

#[test]
fn import_receipts_without_finished_summary_remain_readable() {
    let old = receipt();
    let decoded = ImportReceipt::decode(&old.encode()).unwrap();
    assert_eq!(decoded, old);
    assert!(decoded
        .moves
        .iter()
        .all(|row| row.finished_summary.is_none()));
    let mut plugin = old.clone();
    plugin.moves[0].finished_summary = Some(true);
    plugin.moves[1].finished_summary = Some(false);
    assert_eq!(ImportReceipt::decode(&plugin.encode()).unwrap(), plugin);
    assert_ne!(plugin.encode(), old.encode());
}

/// An imported history of three messages, the first carrying an image, each message followed
/// by its facts under its source id.
fn import_batch(a: &[u8]) -> (Vec<Record>, Vec<NewInputMedia>, Vec<(String, u64)>) {
    let records = vec![
        start("seed", true),
        input(
            "seed",
            Placement::Imported,
            Role::User,
            vec![text("look"), ContentBlock::Media(image(a))],
        ),
        facts(1, "msg_u1"),
        input(
            "seed",
            Placement::Imported,
            Role::Assistant,
            vec![text("seen")],
        ),
        facts(2, "msg_a1"),
        input(
            "seed",
            Placement::Imported,
            Role::User,
            vec![text("thanks")],
        ),
        facts(3, "msg_u2"),
        end("seed"),
    ];
    let media = vec![NewInputMedia {
        record: 1,
        message: 0,
        block: 1,
        part: None,
        bytes: a.to_vec(),
    }];
    let ids = vec![
        ("msg_u2".to_owned(), 3),
        ("msg_u1".to_owned(), 1),
        ("msg_a1".to_owned(), 2),
    ];
    (records, media, ids)
}

/// An import's media is stored before its batch and reads back from the stored record, and its
/// source ids resolve to their ordinals through the id map: in the open log, after a seal at a
/// cut through the imported history, and after a reload that reads the map again.
fn an_import_stores_its_media_and_resolves_its_ids_before_and_after_a_seal(cipher: Cipher) {
    let backend = MemoryBackend::default();
    let a = b"an imported image".as_slice();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), true)
            .await
            .unwrap();
        let (records, media, ids) = import_batch(a);
        let first = session.next_seq();
        let (at, appended) = session
            .append_import(&records, media, ids, &receipt())
            .await
            .unwrap();
        assert_eq!(at, first);
        let Record::RunStart(seed) = &appended[0] else {
            unreachable!()
        };
        let pointer = seed
            .imported_ids
            .clone()
            .expect("the seed names the id map");
        assert_eq!(pointer.seq, first);
        assert_eq!(session.operations.imported_ids, Some(pointer));
        let Record::Input(with_image) = &appended[1] else {
            unreachable!()
        };
        assert_eq!(with_image.media.len(), 1);
        assert_eq!(session.input_media(with_image, 1, None).await.unwrap(), a);
        assert_eq!(session.window_media(&image(a)).await.unwrap(), a);
        assert_eq!(session.find_message("msg_a1").await.unwrap(), Some(2));
        assert_eq!(session.find_message("msg_nowhere").await.unwrap(), None);

        // A later run, then a seal whose cut keeps only the last imported message and after.
        let cut = first + 5;
        session.append(&[start("r1", false)], true).await.unwrap();
        let mid = session.message_id(1);
        session
            .append(
                &[
                    input("r1", Placement::RunInput, Role::User, vec![text("next")]),
                    facts(4, &mid),
                    end("r1"),
                ],
                true,
            )
            .await
            .unwrap();
        session.seal(Some(cut), 0).await.unwrap();
        assert_eq!(session.conversation.window_start(), 3);
        for (id, ordinal) in [("msg_u1", 1), ("msg_a1", 2), ("msg_u2", 3)] {
            assert_eq!(
                session.find_message(id).await.unwrap(),
                Some(ordinal),
                "{id}"
            );
        }
        assert_eq!(session.find_message(&mid).await.unwrap(), Some(4));
        assert_eq!(session.input_media(with_image, 1, None).await.unwrap(), a);
        session.close().await.unwrap();

        let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        assert_eq!(loaded.find_message("msg_u1").await.unwrap(), Some(1));
        assert_eq!(loaded.import_receipt().await.unwrap(), Some(receipt()));
        let entry = loaded.entries(1, 1).await.unwrap().remove(0);
        assert_eq!(entry.state.facts.unwrap().mid, "msg_u1");
    });
}

/// A map the session names but can't read is damage, never an id it doesn't hold.
fn a_damaged_id_map_reads_as_damage(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), true)
            .await
            .unwrap();
        let (records, media, ids) = import_batch(b"image");
        session
            .append_import(&records, media, ids, &receipt())
            .await
            .unwrap();
        session.close().await.unwrap();
        let name = backend
            .object_names(&addr())
            .into_iter()
            .find(|name| name.ends_with("-imported-ids"))
            .expect("the id map is stored");
        let mut sealed = backend.get_object(&addr(), &name).await.unwrap().unwrap();
        let last = sealed.len() - 1;
        sealed[last] ^= 1;
        backend.set_object(&addr(), &name, sealed);
        let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        assert!(matches!(
            loaded.find_message("msg_u1").await,
            Err(SessionError::ImportedIdsDamaged(_))
        ));
        // A broca id still resolves without the map.
        assert!(loaded.find_message("m0").await.is_ok());
    });
}

/// Ids of the forms broca mints or converts are recognised, so an import can refuse them.
#[test]
fn broca_id_forms() {
    for id in ["g0123456789abcdef.12", "g.0", "m7", "m.3", "mabc.9"] {
        assert!(is_broca_id_form(id), "{id}");
    }
    for id in [
        "msg_01J", "m", "mx", "g12", "gabc.x", "a1b2c3d4", "m01", "g1.01",
    ] {
        assert!(!is_broca_id_form(id), "{id}");
    }
}

crate::message! {
    /// `Input` as a reader from before input media decodes it.
    #[derive(Debug)]
    struct InputBeforeMedia {
        1 req run_id: String,
    }
}

crate::message! {
    /// `RunStart` as a reader from before imports decodes it.
    #[derive(Debug)]
    struct RunStartBeforeImports {
        1 req run_id: String,
    }
}

/// A reader that doesn't know input media or the id map refuses a record carrying either,
/// rather than render a message without its media or miss imported ids; records without them
/// read as before.
#[test]
fn an_older_reader_refuses_input_media_and_an_id_map() {
    let mut with_media = input("seed", Placement::Imported, Role::User, vec![text("x")]);
    let without = with_media.encode_body();
    assert!(InputBeforeMedia::decode(&without).is_ok());
    if let Record::Input(input) = &mut with_media {
        input.media.push(InputMedia {
            block: 0,
            part: None,
            log: crate::codec::Id128([1; 16]),
            generation: crate::codec::Id128([2; 16]),
            seq: 3,
        });
    }
    assert_eq!(
        InputBeforeMedia::decode(&with_media.encode_body())
            .unwrap_err()
            .kind,
        DecodeErrorKind::MustUnderstand { field: 10 }
    );
    let mut seed = start("seed", true);
    assert!(RunStartBeforeImports::decode(&seed.encode_body()).is_ok());
    if let Record::RunStart(start) = &mut seed {
        start.imported_ids = Some(ImportedIds {
            log: crate::codec::Id128([1; 16]),
            generation: crate::codec::Id128([2; 16]),
            seq: 3,
        });
    }
    assert_eq!(
        RunStartBeforeImports::decode(&seed.encode_body())
            .unwrap_err()
            .kind,
        DecodeErrorKind::MustUnderstand { field: 28 }
    );
}

/// A turn's call facts attach to the assistant message they name, in the window and through a
/// reload, beside its entry facts.
fn turn_facts_attach_to_their_message(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), true)
            .await
            .unwrap();
        let call: ContentBlock = serde_json::from_value(serde_json::json!({
            "type": "tool_call", "tool_call_id": "c1", "tool_name": "read", "input": {}
        }))
        .unwrap();
        let turn = TurnFacts {
            ordinal: 1,
            calls: vec![CallFacts {
                tool_call_id: broca_protocol::ids::ToolCallId("c1".into()),
                call_key: "msg_a/c1".into(),
                schema_pin: None,
                carrier: Some("thalamus-harness".into()),
                dispatched_to: None,
                dispatched_at_ms: None,
                finished_at_ms: None,
                unanswered: None,
            }],
        };
        let records = vec![
            start("seed", true),
            input("seed", Placement::Imported, Role::Assistant, vec![call]),
            Record::TurnFacts(std::sync::Arc::new(turn.clone())),
            facts(1, "msg_a"),
            end("seed"),
        ];
        session
            .append_import(&records, Vec::new(), vec![("msg_a".into(), 1)], &receipt())
            .await
            .unwrap();
        assert_eq!(session.conversation.entries[0].turn.as_deref(), Some(&turn));
        session.close().await.unwrap();
        let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        let entry = loaded.entries(1, 1).await.unwrap().remove(0);
        assert_eq!(entry.state.turn.as_deref(), Some(&turn));
        assert_eq!(entry.state.facts.unwrap().mid, "msg_a");
    });
}

/// An imported tool turn's results are their own input record. Sealed, the call and a large
/// result can't share a block unless the seal keeps them in one group; either way the call's
/// entry reads with its tool message, by ordinal, by id and after a reload, so its calls never
/// read as still running.
fn an_imported_tool_turn_reads_with_its_results_after_a_seal(cipher: Cipher) {
    let backend = MemoryBackend::default();
    let call: ContentBlock = serde_json::from_value(serde_json::json!({
        "type": "tool_call", "tool_call_id": "c1", "tool_name": "read", "input": {}
    }))
    .unwrap();
    // Larger than a sealed block, so the result can't join a block that already holds the call.
    let result: ContentBlock = serde_json::from_value(serde_json::json!({
        "type": "tool_result", "tool_call_id": "c1",
        "output": {"kind": "text", "text": "x".repeat(64 * 1024)}, "is_error": false
    }))
    .unwrap();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), true)
            .await
            .unwrap();
        let ids = ["msg_u1", "msg_a1", "msg_t1", "msg_u2", "msg_a2"];
        let records = vec![
            start("seed", true),
            input("seed", Placement::Imported, Role::User, vec![text("q")]),
            facts(1, ids[0]),
            input("seed", Placement::Imported, Role::Assistant, vec![call]),
            facts(2, ids[1]),
            input("seed", Placement::Imported, Role::Tool, vec![result]),
            facts(3, ids[2]),
            input("seed", Placement::Imported, Role::User, vec![text("next")]),
            facts(4, ids[3]),
            input(
                "seed",
                Placement::Imported,
                Role::Assistant,
                vec![text("done")],
            ),
            facts(5, ids[4]),
            end("seed"),
        ];
        let first = session.next_seq();
        let named = ids
            .iter()
            .enumerate()
            .map(|(index, id)| ((*id).to_owned(), index as u64 + 1))
            .collect();
        session
            .append_import(&records, Vec::new(), named, &receipt())
            .await
            .unwrap();
        // The log starts again at the second user input: the tool turn is sealed.
        session.seal(Some(first + 7), 0).await.unwrap();
        assert_eq!(session.conversation.window_start(), 4);
        let results = |entry: &crate::session::StoredEntry| {
            entry
                .next
                .as_deref()
                .map(|next| (next.role, next.content.len()))
        };
        let by_ordinal = session.entries(2, 1).await.unwrap();
        assert_eq!(by_ordinal[0].ordinal, 2);
        assert_eq!(results(&by_ordinal[0]), Some((Role::Tool, 1)), "by ordinal");
        let all = session.entries(1, 10).await.unwrap();
        assert_eq!(all.len(), 5);
        assert_eq!(results(&all[1]), Some((Role::Tool, 1)), "in a page");
        let by_id = session.entries_by_mid(&["msg_a1".into()]).await.unwrap();
        assert_eq!(results(&by_id[0]), Some((Role::Tool, 1)), "by id");
        session.close().await.unwrap();
        let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        let reloaded = loaded.entries(2, 1).await.unwrap();
        assert_eq!(
            results(&reloaded[0]),
            Some((Role::Tool, 1)),
            "after a reload"
        );
    });
}

/// An import of more records than one log batch holds is appended in as many batches as it
/// needs, and reads back whole, after a reload too.
fn an_import_larger_than_one_batch_reads_back_whole(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), true)
            .await
            .unwrap();
        // Each message is an input and its facts: one record more than a batch holds in all.
        let messages = MAX_BATCH_RECORDS as u64 / 2;
        let mut records = vec![start("seed", true)];
        let mut ids = Vec::new();
        for n in 1..=messages {
            let role = if n % 2 == 1 {
                Role::User
            } else {
                Role::Assistant
            };
            let content = vec![text(&format!("message {n}"))];
            records.push(input("seed", Placement::Imported, role, content));
            records.push(facts(n, &format!("msg_{n}")));
            ids.push((format!("msg_{n}"), n));
        }
        records.push(end("seed"));
        assert!(records.len() > MAX_BATCH_RECORDS);
        let first = session.next_seq();
        let (at, appended) = session
            .append_import(&records, Vec::new(), ids, &receipt())
            .await
            .unwrap();
        assert_eq!(at, first);
        assert_eq!(appended.len(), records.len());
        assert_eq!(session.next_seq(), first + records.len() as u64);
        assert_eq!(session.conversation.placed, messages);
        session.close().await.unwrap();

        let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        assert_eq!(loaded.conversation.placed, messages);
        assert!(loaded.operations.run.as_ref().unwrap().finished.is_some());
        assert_eq!(loaded.import_receipt().await.unwrap(), Some(receipt()));
        for n in [1, messages / 2, messages] {
            assert_eq!(
                loaded.find_message(&format!("msg_{n}")).await.unwrap(),
                Some(n)
            );
            let entry = loaded.entries(n, 1).await.unwrap().remove(0);
            assert_eq!(entry.message.content, vec![text(&format!("message {n}"))]);
            assert_eq!(entry.state.facts.unwrap().mid, format!("msg_{n}"));
        }
    });
}

/// A replacement staged elsewhere is the same session (id, triple, incarnation, mode) with a log
/// of its own, and lists the replaced session's log for deletion. Once its log, objects and head
/// are in place of the replaced session's, the writer takes the reloaded session in, and the
/// observer of its appends is told the history was replaced. A session that changed its log is
/// never replaced.
fn a_replacement_keeps_the_session_and_takes_its_place_once_published(cipher: Cipher) {
    use std::sync::{Arc, Mutex};
    let live = MemoryBackend::default();
    let staging = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&live, addr(), triple(), &deployment(cipher), true)
            .await
            .unwrap();
        let told = Arc::new(Mutex::new(Vec::new()));
        let observer = Arc::clone(&told);
        session.observe_appends(Box::new(move |appended| {
            observer.lock().unwrap().push(match appended {
                crate::session::Appended::Records { .. } => "records",
                crate::session::Appended::Synced => "synced",
                crate::session::Appended::Replaced => "replaced",
            })
        }));
        let (replaced, _) = session.open_log();
        let mut staged = session.stage_replacement(&staging, 77).await.unwrap();
        assert_eq!(staged.head().session_id, session.head().session_id);
        assert_eq!(staged.head().triple, session.head().triple);
        assert_eq!(staged.head().incarnation, session.head().incarnation);
        assert_eq!(staged.head().cipher, session.head().cipher);
        assert_ne!(staged.open_log().0, replaced);
        assert_eq!(staged.next_seq(), session.next_seq());
        let pending = &staged.head().pending;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].logs, vec![replaced]);
        assert_eq!(pending[0].unreferenced_at_ms, 77);
        let (records, media, ids) = import_batch(b"image");
        staged
            .append_import(&records, media, ids, &receipt())
            .await
            .unwrap();
        drop(staged);

        // Published by hand: the staged log and objects beside the live ones, then the head.
        for log in staging.log_names(&addr()) {
            let end = staging.log_end(&addr(), &log).await.unwrap();
            let bytes = staging.read_log(&addr(), &log, 0..end).await.unwrap();
            live.set_log(&addr(), &log, bytes);
        }
        for name in staging.object_names(&addr()) {
            let bytes = staging.get_object(&addr(), &name).await.unwrap().unwrap();
            live.set_object(&addr(), &name, bytes);
        }
        let (head, _) = staging.read_head(&addr()).await.unwrap().unwrap();
        let at = session.head_version();
        live.write_head(&addr(), &head, Some(at), at + 1)
            .await
            .unwrap();
        let reloaded = Session::load(&live, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        session.replace_with(reloaded);
        assert_eq!(*told.lock().unwrap(), ["replaced"]);
        assert_eq!(session.conversation.placed, 3);
        assert_eq!(session.find_message("msg_a1").await.unwrap(), Some(2));
        assert_eq!(session.import_receipt().await.unwrap(), Some(receipt()));
        assert_eq!(session.head().pending[0].logs, vec![replaced]);

        // The published session changed its log once sealed, so it is never replaced.
        session.seal(None, 0).await.unwrap();
        assert!(session.stage_replacement(&staging, 78).await.is_err());
    });
}
