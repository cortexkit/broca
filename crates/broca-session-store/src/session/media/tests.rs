use broca_protocol::caller::RunFinishReason;
use broca_protocol::content::{ContentBlock, MediaRef, Message, Role};
use broca_protocol::ids::MessageId;
use broca_protocol::request::ModelHandle;
use futures::executor::block_on;
use sha2::{Digest as _, Sha256};

use super::{MediaError, NewMedia};
use crate::backend::{MemoryBackend, SessionAddr, SessionBackend};
use crate::codec::hex;
use crate::head::SessionTriple;
use crate::keys::{Cipher, DeploymentKey};
use crate::record::{
    Assistant, EntryFacts, Input, MediaObject, Placement, QueuedSend, Record, RunEnd, RunStart,
    Steer, ToolReply,
};
use crate::session::{NewInputMedia, Reverted, Session, SessionError};

cipher_tests!(
    a_results_media_is_stored_once_and_reads_back_after_reload,
    media_that_doesnt_match_its_part_is_refused_before_anything_is_written,
    a_changed_or_moved_media_object_reads_as_corrupt,
    a_seal_keeps_the_windows_media_and_forgets_media_it_moved_out,
    a_revert_forgets_the_abandoned_branchs_media,
    a_named_medium_is_found_by_digest_in_sealed_history_and_the_open_log,
    a_sends_media_are_stored_under_its_record_and_its_copies_point_at_them,
    a_sends_media_that_dont_match_are_refused_before_anything_is_written,
    a_crash_while_storing_a_sends_media_never_leaves_a_record_without_its_bytes,
    a_changed_or_moved_input_medium_reads_as_corrupt,
    a_media_object_moved_onto_another_name_for_the_same_bytes_reads_as_corrupt,
);

fn triple() -> SessionTriple {
    SessionTriple {
        project_root: "/project".into(),
        harness: "broca".into(),
        session: "s".into(),
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
    serde_json::from_value(serde_json::json!({"provider_module_id": "p", "model_id": "m"})).unwrap()
}

/// An image's reference, as a capture would derive it from `bytes`.
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

fn start(run: &str, send: &str) -> Record {
    Record::RunStart(Box::new(RunStart {
        imported_ids: None,
        message_marks: None,
        run_id: run.into(),
        model: model(),
        setup: Vec::new(),
        renderer_version: 1,
        submission_id: None,
        send_id: Some(send.into()),
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
        seeded: None,
        refusal_schedule: None,
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

fn input(run: &str, text: &str) -> Record {
    Record::Input(Input {
        media: Vec::new(),
        run_id: run.into(),
        placement: Placement::RunInput,
        message: Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }]),
        written_at_ms: None,
        submission_id: None,
        notice: None,
        tags_v1: None,
        receipt_seq: None,
        pruned: Vec::new(),
    })
}

fn assistant(run: &str, content: Vec<ContentBlock>) -> Record {
    Record::Assistant(Assistant {
        run_id: run.into(),
        step_id: 1,
        provider_message_id: MessageId(format!("m-{run}")),
        message: Message::assistant_from(content, model()),
        usage: None,
        written_at_ms: None,
        last_content_at_ms: None,
        budget_finalize: None,
        tags_v1: None,
        pruned: Vec::new(),
    })
}

fn call(id: &str) -> ContentBlock {
    serde_json::from_value(serde_json::json!({
        "type": "tool_call", "tool_call_id": id, "tool_name": "photo", "input": {}
    }))
    .unwrap()
}

fn facts(ordinal: u64, mid: String) -> Record {
    Record::EntryFacts(EntryFacts {
        client_message_id: None,
        sender: None,
        marks: None,
        no_display_facts: None,
        reasoning_summaries: Vec::new(),
        rich_ui: None,
        runner_tag_base: None,
        legacy_view_seq: None,
        ordinal,
        written_at_ms: Some(ordinal),
        tag_base: 0,
        notice: None,
        usage: None,
        generated_under: None,
        native: true,
        render_tags: false,
        clean_reply: false,
        mid,
        run_id: None,
        created_at_ms: None,
        delivery_mode: None,
        delivery_tool_call_id: None,
        notices: Vec::new(),
    })
}

/// A tool result with a text part, then one media part per image.
fn reply(run: &str, id: &str, images: &[&[u8]]) -> Record {
    let mut parts = vec![serde_json::json!({"kind": "text", "text": "photos"})];
    for bytes in images {
        parts.push(serde_json::json!({"kind": "media", "media": image(bytes)}));
    }
    Record::ToolReply(ToolReply {
        run_id: run.into(),
        batch_id: None,
        result: serde_json::from_value(serde_json::json!({
            "tool_call_id": id, "output": {"kind": "parts", "parts": parts}, "is_error": false
        }))
        .unwrap(),
        finished_at_ms: None,
        tags_v1: None,
        media: Vec::new(),
    })
}

/// The media parts of a result, as `append_media` takes them: part 1 onward.
fn parts(record: usize, images: &[&[u8]]) -> Vec<NewMedia> {
    images
        .iter()
        .enumerate()
        .map(|(index, bytes)| NewMedia {
            record,
            part: index + 1,
            bytes: bytes.to_vec(),
        })
        .collect()
}

fn media_objects(backend: &MemoryBackend) -> Vec<String> {
    backend
        .object_names(&addr())
        .into_iter()
        .filter(|name| name.contains("/a/") && name.contains("-media-"))
        .collect()
}

/// One run that calls the photo tool once and gets `images`, then answers. Its input is
/// message `ordinal`. Returns the input's id and the result record as it was appended.
async fn photo_run<B: SessionBackend>(
    session: &mut Session<B>,
    run: &str,
    ordinal: u64,
    images: &[&[u8]],
) -> (String, Record) {
    let mid = session.message_id(2);
    session
        .append(
            &[
                start(run, &format!("send-{run}")),
                input(run, "take photos"),
                facts(ordinal, mid.clone()),
            ],
            true,
        )
        .await
        .unwrap();
    let id = format!("c-{run}");
    let call_mid = session.message_id(1);
    session
        .append(
            &[
                assistant(run, vec![call(&id)]),
                facts(ordinal + 1, call_mid),
            ],
            true,
        )
        .await
        .unwrap();
    let (_, mut appended) = session
        .append_media(&[reply(run, &id, images)], parts(0, images), true)
        .await
        .unwrap();
    let answer_mid = session.message_id(1);
    session
        .append(
            &[
                assistant(
                    run,
                    vec![ContentBlock::Text {
                        text: "nice".into(),
                    }],
                ),
                facts(ordinal + 3, answer_mid),
                end(run),
            ],
            true,
        )
        .await
        .unwrap();
    (mid, appended.remove(0))
}

fn objects_of(record: &Record) -> Vec<(u32, u64)> {
    let Record::ToolReply(reply) = record else {
        panic!("{record:?}")
    };
    reply
        .media
        .iter()
        .map(|object| (object.part, object.seq))
        .collect()
}

/// Each image is stored once, under the record that first names it: a later result naming the
/// same bytes points at that object. The bytes read back by reference after a reload, and the
/// stored record says where each part's bytes are.
fn a_results_media_is_stored_once_and_reads_back_after_reload(cipher: Cipher) {
    let backend = MemoryBackend::default();
    let (a, b) = (b"image a".as_slice(), b"image b".as_slice());
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let (_, first) = photo_run(&mut session, "r0", 1, &[a]).await;
        let (_, second) = photo_run(&mut session, "r1", 5, &[a, b]).await;
        assert_eq!(media_objects(&backend).len(), 2);
        let first_seq = objects_of(&first)[0].1;
        let second_objects = objects_of(&second);
        // Part 1 (a) points at the first result's object; part 2 (b) is its own.
        assert_eq!(second_objects[0], (1, first_seq));
        assert_ne!(second_objects[1].1, first_seq);
        session.close().await.unwrap();

        let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        assert_eq!(loaded.window_media(&image(a)).await.unwrap(), a);
        assert_eq!(loaded.window_media(&image(b)).await.unwrap(), b);
        let Record::ToolReply(second) = &second else {
            unreachable!()
        };
        assert_eq!(loaded.record_media(second, 1).await.unwrap(), a);
        assert_eq!(loaded.record_media(second, 2).await.unwrap(), b);
        assert!(matches!(
            loaded.window_media(&image(b"never stored")).await,
            Err(SessionError::Media(MediaError::Missing(_)))
        ));
    });
}

/// Bytes that don't match their part's reference, and a part that isn't media, are refused
/// before anything is written: no object, no record.
fn media_that_doesnt_match_its_part_is_refused_before_anything_is_written(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let before = session.next_seq();
        let record = reply("r0", "c0", &[b"image a"]);
        let wrong = vec![NewMedia {
            record: 0,
            part: 1,
            bytes: b"image b".to_vec(),
        }];
        assert!(matches!(
            session
                .append_media(std::slice::from_ref(&record), wrong, true)
                .await,
            Err(SessionError::Media(MediaError::Mismatch {
                record: 0,
                part: 1
            }))
        ));
        let text_part = vec![NewMedia {
            record: 0,
            part: 0,
            bytes: b"image a".to_vec(),
        }];
        assert!(matches!(
            session.append_media(&[record], text_part, true).await,
            Err(SessionError::Media(MediaError::NotMedia {
                record: 0,
                part: 0
            }))
        ));
        assert_eq!(session.next_seq(), before);
        assert!(media_objects(&backend).is_empty());
    });
}

/// An object whose bytes were changed, or that holds another object's bytes, reads as corrupt,
/// never as other bytes: a read decrypts the object and checks the bytes against the part's
/// reference.
fn a_changed_or_moved_media_object_reads_as_corrupt(cipher: Cipher) {
    let backend = MemoryBackend::default();
    let (a, b) = (b"image a".as_slice(), b"image b".as_slice());
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        photo_run(&mut session, "r0", 1, &[a, b]).await;
        let name_a = session.media_object_name(&image(a)).unwrap();
        let name_b = session.media_object_name(&image(b)).unwrap();
        let sealed_b = backend.get_object(&addr(), &name_b).await.unwrap().unwrap();
        backend.set_object(&addr(), &name_a, sealed_b.clone());
        assert!(matches!(
            session.window_media(&image(a)).await,
            Err(SessionError::Media(MediaError::Corrupt(_)))
        ));
        let mut flipped = sealed_b;
        let last = flipped.len() - 1;
        flipped[last] ^= 1;
        backend.set_object(&addr(), &name_b, flipped);
        assert!(matches!(
            session.window_media(&image(b)).await,
            Err(SessionError::Media(MediaError::Corrupt(_)))
        ));
    });
}

/// A seal keeps the window's media where it is, and forgets media it moved out of the open
/// log: a result naming those bytes again writes them again. The sealed record still reads its
/// own media.
fn a_seal_keeps_the_windows_media_and_forgets_media_it_moved_out(cipher: Cipher) {
    let backend = MemoryBackend::default();
    let (a, b) = (b"image a".as_slice(), b"image b".as_slice());
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let (_, sealed_away) = photo_run(&mut session, "r0", 1, &[a]).await;
        let cut = session.next_seq() + 1;
        photo_run(&mut session, "r1", 5, &[b]).await;
        session.seal(Some(cut), 0).await.unwrap();
        assert_eq!(session.conversation.window_start(), 5);
        assert_eq!(session.window_media(&image(b)).await.unwrap(), b);
        assert!(matches!(
            session.window_media(&image(a)).await,
            Err(SessionError::Media(MediaError::Missing(_)))
        ));
        let Record::ToolReply(sealed_away) = &sealed_away else {
            unreachable!()
        };
        assert_eq!(session.record_media(sealed_away, 1).await.unwrap(), a);

        let objects = media_objects(&backend).len();
        photo_run(&mut session, "r2", 9, &[a, b]).await;
        // a was written again; b is still the window's.
        assert_eq!(media_objects(&backend).len(), objects + 1);
        session.close().await.unwrap();
        let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        assert_eq!(loaded.window_media(&image(a)).await.unwrap(), a);
        assert_eq!(loaded.window_media(&image(b)).await.unwrap(), b);
    });
}

/// After a revert in the open log, the kept messages' media is still the window's, and the
/// abandoned branch's isn't: a result on the new branch naming those bytes writes them again.
fn a_revert_forgets_the_abandoned_branchs_media(cipher: Cipher) {
    let backend = MemoryBackend::default();
    let (a, b) = (b"image a".as_slice(), b"image b".as_slice());
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        photo_run(&mut session, "r0", 1, &[a]).await;
        let (abandoned, _) = photo_run(&mut session, "r1", 5, &[b]).await;
        let resend = vec![
            start("r9", "send-r9"),
            input("r9", "take one photo"),
            facts(0, String::new()),
        ];
        let reverted = session
            .revert(&abandoned, resend, None, 1_000)
            .await
            .unwrap();
        assert!(
            matches!(reverted, Reverted::Applied { ordinal: 5, .. }),
            "{reverted:?}"
        );
        assert_eq!(session.window_media(&image(a)).await.unwrap(), a);
        assert!(matches!(
            session.window_media(&image(b)).await,
            Err(SessionError::Media(MediaError::Missing(_)))
        ));
        let objects = media_objects(&backend).len();
        let id = "c-r9";
        let call_mid = session.message_id(1);
        session
            .append(&[assistant("r9", vec![call(id)]), facts(6, call_mid)], true)
            .await
            .unwrap();
        session
            .append_media(&[reply("r9", id, &[a, b])], parts(0, &[a, b]), true)
            .await
            .unwrap();
        assert_eq!(media_objects(&backend).len(), objects + 1);
        assert_eq!(session.window_media(&image(b)).await.unwrap(), b);
    });
}

/// A medium is found by its digest wherever a record of the session names it, sealed history
/// included, and its bytes read back from the object the record stored. A record naming a digest
/// without storing its bytes is found with nothing to read, and a digest no record names isn't
/// found. Reading the bytes touches storage, so a session answering from memory only refuses.
fn a_named_medium_is_found_by_digest_in_sealed_history_and_the_open_log(cipher: Cipher) {
    let backend = MemoryBackend::default();
    let (a, b, c) = (
        b"image a".as_slice(),
        b"image b".as_slice(),
        b"image c".as_slice(),
    );
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        photo_run(&mut session, "r0", 1, &[a]).await;
        let cut = session.next_seq() + 1;
        photo_run(&mut session, "r1", 5, &[b]).await;
        session.seal(Some(cut), 0).await.unwrap();
        // An input naming `c` in its content, whose record stored no bytes for it.
        let Record::Input(mut unstored) = input("r2", "look") else {
            unreachable!()
        };
        unstored.message.content.push(ContentBlock::Media(image(c)));
        let mid = session.message_id(2);
        session
            .append(
                &[
                    start("r2", "send-r2"),
                    Record::Input(unstored),
                    facts(9, mid),
                ],
                true,
            )
            .await
            .unwrap();
        for (bytes, place) in [(a, "sealed history"), (b, "the open log")] {
            let found = session
                .named_media(&image(bytes).sha256)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("found in {place}"));
            assert_eq!(found.reference, image(bytes), "{place}");
            assert!(found.stored(), "{place}");
            assert_eq!(
                session.named_media_bytes(&found).await.unwrap(),
                bytes,
                "{place}"
            );
            let memory = session.in_memory();
            assert!(matches!(
                session.named_media_bytes(&found).await,
                Err(SessionError::NotInMemory)
            ));
            drop(memory);
        }
        let named_only = session
            .named_media(&image(c).sha256)
            .await
            .unwrap()
            .unwrap();
        assert!(!named_only.stored());
        assert!(matches!(
            session.named_media_bytes(&named_only).await,
            Err(SessionError::Media(MediaError::Missing(_)))
        ));
        assert_eq!(
            session
                .named_media(&image(b"never named").sha256)
                .await
                .unwrap(),
            None
        );
    });
}

/// A user message holding `text` and then one media block per image.
fn photo_message(text: &str, images: &[&[u8]]) -> Message {
    let mut content = vec![ContentBlock::Text { text: text.into() }];
    content.extend(images.iter().map(|bytes| ContentBlock::Media(image(bytes))));
    Message::new(Role::User, content)
}

/// A queued send whose one message holds `images` after its text.
fn queued(submission: &str, images: &[&[u8]]) -> Record {
    Record::QueuedSend(QueuedSend {
        media: Vec::new(),
        submission_id: submission.into(),
        send_id: None,
        input: vec![photo_message("queued look", images)],
        setup: Vec::new(),
        written_at_ms: None,
        ts_ms: None,
        request: None,
        receipt: None,
        delivery: None,
        message_marks: None,
    })
}

/// The bytes of each image after a message's text, as an append takes them for the message at
/// `message` of record `record`.
fn blocks(record: usize, message: u32, images: &[&[u8]]) -> Vec<NewInputMedia> {
    (1u32..)
        .zip(images)
        .map(|(block, bytes)| NewInputMedia {
            record,
            message,
            block,
            part: None,
            bytes: bytes.to_vec(),
        })
        .collect()
}

/// A queued send's and a steer's media are stored under the record that accepts them, durable
/// before it, and the record says where. A later message naming the same bytes, as the copy that
/// places a queued send does, points at that object instead of writing another; a steer's
/// pointers reach the run state its boundary places copies from. Every medium reads back by
/// digest after a reload, and `stored_media` lists where each record keeps its bytes.
fn a_sends_media_are_stored_under_its_record_and_its_copies_point_at_them(cipher: Cipher) {
    let backend = MemoryBackend::default();
    let (a, b) = (b"image a".as_slice(), b"image b".as_slice());
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let mid = session.message_id(2);
        session
            .append(
                &[start("r0", "send-r0"), input("r0", "go"), facts(1, mid)],
                true,
            )
            .await
            .unwrap();
        let (queued_at, appended) = session
            .append_input_media(&[queued("sub-1", &[a])], blocks(0, 0, &[a]), true)
            .await
            .unwrap();
        let Record::QueuedSend(send) = &appended[0] else {
            unreachable!()
        };
        assert_eq!(send.media.len(), 1);
        assert_eq!((send.media[0].message, send.media[0].block), (0, 1));
        assert_eq!(send.media[0].seq, queued_at);
        assert_eq!(media_objects(&backend).len(), 1);

        let steer = Record::Steer(Steer {
            media: Vec::new(),
            input: vec![photo_message("steer look", &[b])],
            written_at_ms: None,
            tags_v1: None,
            receipt: None,
            message_marks: None,
        });
        let (steer_at, _) = session
            .append_input_media(&[steer], blocks(0, 0, &[b]), true)
            .await
            .unwrap();
        let pending = &session.operations.run.as_ref().unwrap().pending_steers[0];
        let placed = pending.media_at(0);
        assert_eq!(placed.len(), 1);
        assert_eq!(
            (placed[0].block, placed[0].part, placed[0].seq),
            (1, None, steer_at)
        );
        assert_eq!(media_objects(&backend).len(), 2);

        // The run that places the queued send names its image again: it points at the queued
        // record's object.
        let Record::Input(mut copy) = input("r0", "") else {
            unreachable!()
        };
        copy.message = photo_message("queued look", &[a]);
        let (_, placed) = session
            .append_input_media(&[Record::Input(copy)], blocks(0, 0, &[a]), true)
            .await
            .unwrap();
        let Record::Input(copy) = &placed[0] else {
            unreachable!()
        };
        assert_eq!(copy.media[0].seq, queued_at);
        assert_eq!(
            media_objects(&backend).len(),
            2,
            "no second object for the same bytes"
        );
        assert_eq!(
            super::stored_media(&appended[0]),
            vec![(
                image(a).sha256,
                MediaObject {
                    part: 0,
                    log: send.media[0].log,
                    generation: send.media[0].generation,
                    seq: queued_at,
                }
            )]
        );
        session.close().await.unwrap();

        let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        for bytes in [a, b] {
            let found = loaded
                .named_media(&image(bytes).sha256)
                .await
                .unwrap()
                .unwrap();
            assert!(found.stored());
            assert_eq!(loaded.named_media_bytes(&found).await.unwrap(), bytes);
            assert_eq!(loaded.window_media(&image(bytes)).await.unwrap(), bytes);
        }
    });
}

/// Bytes that don't match the medium their message names, and a block that isn't a medium, are
/// refused before anything is written: no object, no record.
fn a_sends_media_that_dont_match_are_refused_before_anything_is_written(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let before = session.next_seq();
        let (a, b) = (b"image a".as_slice(), b"image b".as_slice());
        let record = queued("sub-1", &[a]);
        let wrong = blocks(0, 0, &[b]);
        assert!(matches!(
            session
                .append_input_media(std::slice::from_ref(&record), wrong, true)
                .await,
            Err(SessionError::Media(MediaError::Mismatch {
                record: 0,
                part: 1
            }))
        ));
        let mut text = blocks(0, 0, &[a]);
        text[0].block = 0;
        assert!(matches!(
            session.append_input_media(&[record], text, true).await,
            Err(SessionError::Media(MediaError::NotMedia {
                record: 0,
                part: 0
            }))
        ));
        assert_eq!(session.next_seq(), before);
        assert!(media_objects(&backend).is_empty());
    });
}

/// A crash at any point of storing a send's media and appending its record leaves a session
/// whose every stored medium reads back: the record never lands before its bytes are durable.
fn a_crash_while_storing_a_sends_media_never_leaves_a_record_without_its_bytes(cipher: Cipher) {
    let image_a = b"image a".as_slice();
    let mut landed = 0;
    for crash in 0..6 {
        let backend = MemoryBackend::default();
        block_on(async {
            let mut session =
                Session::create(&backend, addr(), triple(), &deployment(cipher), false)
                    .await
                    .unwrap();
            backend.crash_after(Some(crash));
            let _ = session
                .append_input_media(
                    &[queued("sub-1", &[image_a])],
                    blocks(0, 0, &[image_a]),
                    true,
                )
                .await;
        });
        backend.crash_after(None);
        block_on(async {
            let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
                .await
                .unwrap();
            if let Some(found) = loaded.named_media(&image(image_a).sha256).await.unwrap() {
                landed += 1;
                assert!(found.stored(), "crash {crash}");
                assert_eq!(
                    loaded.named_media_bytes(&found).await.unwrap(),
                    image_a,
                    "crash {crash}: the record landed before its bytes"
                );
            }
        });
    }
    assert!((1..6).contains(&landed), "{landed}");
}

/// Whether `object` holds `bytes` as they are, which only a plain session's objects do: an
/// encrypted session's hold them encrypted.
fn holds_verbatim(object: &[u8], bytes: &[u8]) -> bool {
    object.windows(bytes.len()).any(|window| window == bytes)
}

/// A send's media object whose bytes were changed, or that holds another object's bytes, reads
/// as corrupt, never as other bytes, as a tool result's does. A plain session keeps the bytes as
/// they are, under a check over the object's name; an encrypted one keeps them encrypted.
fn a_changed_or_moved_input_medium_reads_as_corrupt(cipher: Cipher) {
    let backend = MemoryBackend::default();
    let (a, b) = (b"image a".as_slice(), b"image b".as_slice());
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        for (submission, bytes) in [("sub-1", a), ("sub-2", b)] {
            session
                .append_input_media(
                    &[queued(submission, &[bytes])],
                    blocks(0, 0, &[bytes]),
                    true,
                )
                .await
                .unwrap();
        }
        let found_a = session
            .named_media(&image(a).sha256)
            .await
            .unwrap()
            .unwrap();
        let found_b = session
            .named_media(&image(b).sha256)
            .await
            .unwrap()
            .unwrap();
        let name_a = session.media_object_name(&image(a)).unwrap();
        let name_b = session.media_object_name(&image(b)).unwrap();
        let stored_b = backend.get_object(&addr(), &name_b).await.unwrap().unwrap();
        assert_eq!(
            holds_verbatim(&stored_b, b),
            cipher == Cipher::None,
            "{cipher:?}"
        );

        backend.set_object(&addr(), &name_a, stored_b.clone());
        assert!(matches!(
            session.named_media_bytes(&found_a).await,
            Err(SessionError::Media(MediaError::Corrupt(_)))
        ));
        let mut changed = stored_b;
        changed[0] ^= 1;
        backend.set_object(&addr(), &name_b, changed);
        assert!(matches!(
            session.named_media_bytes(&found_b).await,
            Err(SessionError::Media(MediaError::Corrupt(_)))
        ));
    });
}

/// An object moved onto the name of another object of the same bytes reads as corrupt: its check
/// covers the name it was written under, so a copy is refused even where the bytes would match
/// the reference. The bytes are written twice because a seal forgets the first object, which the
/// sealed record still names.
fn a_media_object_moved_onto_another_name_for_the_same_bytes_reads_as_corrupt(cipher: Cipher) {
    let backend = MemoryBackend::default();
    let a = b"image a".as_slice();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let (_, sealed_away) = photo_run(&mut session, "r0", 1, &[a]).await;
        let cut = session.next_seq() + 1;
        photo_run(&mut session, "r1", 5, &[b"image b".as_slice()]).await;
        session.seal(Some(cut), 0).await.unwrap();
        photo_run(&mut session, "r2", 9, &[a]).await;
        let Record::ToolReply(sealed_away) = &sealed_away else {
            unreachable!()
        };
        let first = session.media_name(&sealed_away.media[0], &image(a).sha256);
        let second = session.media_object_name(&image(a)).unwrap();
        assert_ne!(first, second, "the bytes were written twice");
        assert_eq!(session.window_media(&image(a)).await.unwrap(), a);

        let moved = backend.get_object(&addr(), &first).await.unwrap().unwrap();
        backend.set_object(&addr(), &second, moved);
        assert!(matches!(
            session.window_media(&image(a)).await,
            Err(SessionError::Media(MediaError::Corrupt(_)))
        ));
        assert_eq!(session.record_media(sealed_away, 1).await.unwrap(), a);
    });
}
