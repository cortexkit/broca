use broca_protocol::content::{ContentBlock, Message, Role};
use broca_protocol::ids::MessageId;
use futures::executor::block_on;

use crate::backend::MemoryBackend;
use crate::head::SessionTriple;
use crate::keys::{Cipher, DeploymentKey};
use crate::record::{Assistant, EntryFacts, Input, Placement, Record, StepStart, ToolReply};
use crate::session::Session;

#[test]
fn sealed_read_cache_keeps_provenance_state_on_the_heap() {
    let cache_bytes = std::mem::size_of::<super::SealedCaches>();
    let session_bytes = std::mem::size_of::<Session<MemoryBackend>>();
    println!("sealed caches: {cache_bytes} bytes; session: {session_bytes} bytes");
    // Every session embeds its sealed cache, and read futures retain sessions across awaits.
    assert!(
        cache_bytes <= 128,
        "sealed caches carry {cache_bytes} bytes; keep provenance folds on the heap"
    );
    assert!(
        session_bytes <= 7 * 1024,
        "session carries {session_bytes} bytes; keep read caches small"
    );
}

cipher_tests!(
    every_message_reads_back_by_ordinal_after_two_cuts,
    message_ids_resolve_in_the_window_and_sealed_history,
    an_id_a_steps_start_announced_resolves_in_the_window_and_sealed_history,
    a_held_assistant_message_reads_last_at_its_reserved_ordinal,
    records_from_a_seq_read_both_streams_sealed_and_open,
    a_ledger_record_reads_back_by_its_seq_from_the_open_log_and_sealed_history,
    ledger_tail_stops_before_older_segments_and_reads_the_open_log_once,
    a_sealed_message_read_fetches_its_block_not_the_segment,
    the_entry_numbering_a_compaction_tag_is_found_by_a_search,
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

fn addr() -> crate::backend::SessionAddr {
    crate::backend::SessionAddr("0123456789abcdef".into())
}

fn input(text: String) -> Record {
    Record::Input(Input {
        media: Vec::new(),
        run_id: "r".into(),
        placement: Placement::RunInput,
        message: std::sync::Arc::new(Message::new(
            Role::User,
            vec![ContentBlock::Text { text: text.into() }],
        )),
        written_at_ms: None,
        submission_id: None,
        notice: None,
        tags_v1: None,
        receipt_seq: None,
        pruned: Vec::new(),
    })
}

fn assistant(content: Vec<ContentBlock>) -> Record {
    Record::Assistant(Assistant {
        run_id: "r".into(),
        step_id: 1,
        provider_message_id: MessageId("m".into()),
        message: std::sync::Arc::new(Message::assistant_from(
            content,
            serde_json::from_value(serde_json::json!({"provider_module_id": "p", "model_id": "m"}))
                .unwrap(),
        )),
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
        "type": "tool_call", "tool_call_id": id, "tool_name": "read", "input": {}
    }))
    .unwrap()
}

fn reply(id: &str, text: String) -> Record {
    Record::ToolReply(ToolReply {
        run_id: "r".into(),
        batch_id: None,
        result: serde_json::from_value(serde_json::json!({
            "tool_call_id": id, "output": {"kind": "text", "text": text}
        }))
        .unwrap(),
        finished_at_ms: None,
        tags_v1: None,
        media: Vec::new(),
    })
}

/// The entry facts of the message at `ordinal`. Its tag bases are the ordinal, so a read that
/// returns another message's facts shows it.
fn facts(ordinal: u64) -> Record {
    Record::EntryFacts(std::sync::Arc::new(EntryFacts {
        send_id: None,
        named_speaker: None,
        client_message_id: None,
        sender: None,
        marks: None,
        no_display_facts: None,
        reasoning_summaries: Vec::new(),
        rich_ui: None,
        runner_tag_base: Some(ordinal),
        legacy_view_seq: None,
        ordinal,
        written_at_ms: Some(ordinal),
        tag_base: ordinal,
        notice: None,
        usage: None,
        generated_under: None,
        native: true,
        render_tags: false,
        clean_reply: false,
        mid: format!("mfeedface.{}", ordinal - 1),
        run_id: None,
        created_at_ms: None,
        delivery_mode: None,
        delivery_tool_call_id: None,
        notices: Vec::new(),
    }))
}

/// Rounds of a user input, a 2 KiB tool turn, and a text answer. Each round places four
/// messages and fills about a quarter of a block, so the history spans many blocks. Each message
/// gets its entry facts in the batch that places it, or that reserves its ordinal.
fn round(n: usize) -> Vec<Vec<Record>> {
    let id = format!("c{n}");
    let first = 4 * n as u64 + 1;
    vec![
        vec![input(format!("ask {n}")), facts(first)],
        vec![assistant(vec![call(&id)]), facts(first + 1)],
        vec![reply(&id, format!("{n}:{}", "x".repeat(4096)))],
        vec![
            assistant(vec![ContentBlock::Text {
                text: format!("answer {n}").into(),
            }]),
            facts(first + 3),
        ],
    ]
}

/// Every message reads back by its ordinal from wherever it lives: the window, or a sealed
/// segment found through the directory, its index and one block. Two cuts make two segments,
/// each of several blocks. Ordinals outside the conversation read as `None`.
fn every_message_reads_back_by_ordinal_after_two_cuts(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let mut cuts = Vec::new();
        for n in 0..30 {
            for (part, batch) in round(n).into_iter().enumerate() {
                let seq = session.append(&batch, true).await.unwrap();
                if part == 0 && (n == 12 || n == 24) {
                    cuts.push(seq);
                }
            }
        }
        let expected = session.conversation.held_messages().unwrap().clone();
        assert_eq!(expected.len(), 120);
        assert_eq!(session.conversation.placed, 120);
        for cut in cuts {
            session.seal(Some(cut), 0).await.unwrap();
        }
        assert_eq!(session.conversation.window_start(), 97);
        session.close().await.unwrap();

        let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        assert_eq!(loaded.conversation.held_messages().unwrap().len(), 24);
        for (index, message) in expected.iter().enumerate() {
            let ordinal = index as u64 + 1;
            let read = loaded.message(ordinal).await.unwrap();
            assert_eq!(read.as_ref(), Some(message), "ordinal {ordinal}");
        }
        assert_eq!(loaded.message(0).await.unwrap(), None);
        assert_eq!(loaded.message(121).await.unwrap(), None);

        // Entries read the same messages, with each one's own facts, and a tool-calling
        // assistant message with its results, in pages that cross segment and block boundaries.
        let mut read = Vec::new();
        let mut from = 1;
        loop {
            let page = loaded.entries(from, 7).await.unwrap();
            assert!(page.len() <= 7);
            let Some(last) = page.last() else { break };
            from = last.ordinal + 1;
            read.extend(page);
        }
        assert_eq!(read.len(), 120);
        for (index, entry) in read.iter().enumerate() {
            let ordinal = index as u64 + 1;
            assert_eq!(entry.ordinal, ordinal);
            assert_eq!(&entry.message, &expected[index], "ordinal {ordinal}");
            // The tool-calling assistant message is every fourth, from the second.
            let results = (ordinal % 4 == 2).then(|| &expected[index + 1]);
            assert_eq!(entry.next.as_ref(), results, "ordinal {ordinal}");
            // Tool messages (every fourth, from the third) have no facts of their own.
            let base = entry.state.facts.as_ref().map(|facts| facts.tag_base);
            let expected_base = (ordinal % 4 != 3).then_some(ordinal);
            assert_eq!(base, expected_base, "ordinal {ordinal}");
            assert!(!entry.open);
        }
        assert!(loaded.entries(121, 5).await.unwrap().is_empty());

        // Each part number names the last message whose base is below it, sealed or in the
        // window: here the base is the ordinal, and tool messages have no facts, so they number
        // nothing.
        let expected_holder = |tag: u64| (1..tag.min(121)).rev().find(|ordinal| ordinal % 4 != 3);
        for tag in 1..=125 {
            let found = loaded.tagged_entry(tag).await.unwrap();
            assert_eq!(
                found.as_ref().map(|entry| entry.ordinal),
                expected_holder(tag),
                "tag {tag}"
            );
            if let Some(entry) = found {
                assert_eq!(
                    &entry.message,
                    &expected[entry.ordinal as usize - 1],
                    "tag {tag}"
                );
            }
        }
    });
}

/// Under the numbering `session.entries` serves (`EntryFacts::tag_base`), the entry holding a
/// part number is the last one whose base is below it, sealed or in the window, as under the
/// runner's own numbering; the two numberings are found apart. Here the compaction bases are ten
/// times the ordinal and no message carries a runner base, so only the compaction search finds an
/// entry. Finding one in sealed history reads directory pages and a few blocks, never every
/// block: a search, not a scan.
fn the_entry_numbering_a_compaction_tag_is_found_by_a_search(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let numbered = |record: Record| match record {
            Record::EntryFacts(facts) => {
                let mut facts = EntryFacts::clone(&facts);
                facts.runner_tag_base = None;
                facts.tag_base = 10 * facts.ordinal;
                Record::EntryFacts(std::sync::Arc::new(facts))
            }
            other => other,
        };
        let mut cut = 0;
        for n in 0..80usize {
            for (part, batch) in round(n).into_iter().enumerate() {
                let batch: Vec<Record> = batch.into_iter().map(numbered).collect();
                let seq = session.append(&batch, true).await.unwrap();
                if part == 0 && n == 70 {
                    cut = seq;
                }
            }
        }
        session.seal(Some(cut), 0).await.unwrap();
        session.close().await.unwrap();
        let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        let placed = loaded.conversation.placed;
        assert!(
            loaded.conversation.window_start() > 200,
            "history is sealed"
        );
        // Tool messages (every fourth, from the third) have no facts, so they number nothing.
        let holder = |tag: u64| {
            (1..=placed)
                .rev()
                .find(|ordinal| ordinal % 4 != 3 && 10 * ordinal < tag)
        };
        for tag in [
            1,
            10,
            11,
            25,
            31,
            1_000,
            2_000,
            2_811,
            3_201,
            10 * placed + 5,
        ] {
            let found = loaded.compaction_tagged_entry(tag).await.unwrap();
            assert_eq!(found.map(|entry| entry.ordinal), holder(tag), "tag {tag}");
            assert_eq!(
                loaded
                    .tagged_entry(tag)
                    .await
                    .unwrap()
                    .map(|entry| entry.ordinal),
                None,
                "no message carries a runner base, tag {tag}"
            );
        }
        // Each read gets its own freshly loaded session, so neither is served from the pages and
        // segment indexes the other read left cached in its session.
        let (triple, deployment) = (triple(), deployment(cipher));
        let fresh = || Session::load(&backend, addr(), &triple, &deployment);
        let searching = fresh().await.unwrap();
        let objects = backend.object_reads();
        let found = searching.compaction_tagged_entry(1_235).await.unwrap();
        assert_eq!(found.map(|entry| entry.ordinal), holder(1_235));
        let search = backend.object_reads() - objects;
        let scanning = fresh().await.unwrap();
        let objects = backend.object_reads();
        let all = scanning.entries(1, usize::MAX).await.unwrap();
        assert_eq!(all.len() as u64, placed);
        let scan = backend.object_reads() - objects;
        println!("a search read {search} objects; reading every entry read {scan}");
        assert!(
            search * 3 <= scan,
            "a search read {search} objects against {scan} for reading every entry"
        );
    });
}

/// Text that zstd can't shrink, so a segment's size on storage is about its content's.
fn noise(seed: u64, len: usize) -> String {
    let mut state = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    (0..len)
        .map(|_| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            char::from(b'a' + ((state >> 33) % 26) as u8)
        })
        .collect()
}

/// Reading one sealed message fetches the segment's layout (its first bytes, footer and index)
/// and the one block that holds the message, never the whole segment; a second message of the
/// same segment fetches only its block, since the session keeps the layout. A segment of a long
/// session is tens of megabytes, so fetching it whole made every older-history page cost that.
fn a_sealed_message_read_fetches_its_block_not_the_segment(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let mut cut = 0;
        for n in 0..80u64 {
            let id = format!("c{n}");
            let first = 4 * n + 1;
            let batches = vec![
                vec![input(format!("ask {n}")), facts(first)],
                vec![assistant(vec![call(&id)]), facts(first + 1)],
                vec![reply(&id, noise(n, 6000))],
                vec![
                    assistant(vec![ContentBlock::Text {
                        text: format!("answer {n}").into(),
                    }]),
                    facts(first + 3),
                ],
            ];
            for (part, batch) in batches.into_iter().enumerate() {
                let seq = session.append(&batch, true).await.unwrap();
                if part == 0 && n == 70 {
                    cut = seq;
                }
            }
        }
        let expected = session.conversation.held_messages().unwrap().clone();
        session.seal(Some(cut), 0).await.unwrap();
        session.close().await.unwrap();
        let segment = backend
            .object_names(&addr())
            .into_iter()
            .filter(|name| name.ends_with("/t"))
            .collect::<Vec<_>>();
        assert_eq!(segment.len(), 1, "one transcript segment");
        let segment_bytes =
            crate::backend::SessionBackend::get_object(&backend, &addr(), &segment[0])
                .await
                .unwrap()
                .unwrap()
                .len();
        assert!(segment_bytes > 200_000, "segment of {segment_bytes} bytes");

        let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        let before = backend.object_bytes_read();
        let read = loaded.message(3).await.unwrap();
        assert_eq!(read.as_ref(), Some(&expected[2]));
        let first = backend.object_bytes_read() - before;
        assert!(
            first < segment_bytes / 4,
            "the first read fetched {first} of the segment's {segment_bytes} bytes"
        );

        let (bytes, objects) = (backend.object_bytes_read(), backend.object_reads());
        let read = loaded.message(200).await.unwrap();
        assert_eq!(read.as_ref(), Some(&expected[199]));
        let directory_pages = loaded.head().transcript_dir.as_ref().unwrap().level as usize + 1;
        assert_eq!(
            backend.object_reads() - objects,
            directory_pages + 1,
            "a later read fetches the directory pages and one block"
        );
        let second = backend.object_bytes_read() - bytes;
        assert!(
            second < 4 * crate::segment::BLOCK_TARGET,
            "a later read fetched {second} bytes"
        );

        // A range of records, read by a freshly loaded session, fetches the layout and the
        // blocks holding the range, not the whole segment.
        let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        let before = backend.object_bytes_read();
        let records = loaded.records_between(9, 12).await.unwrap();
        assert!(!records.is_empty());
        assert!(records.iter().all(|(seq, _)| (9..=12).contains(seq)));
        let range = backend.object_bytes_read() - before;
        assert!(
            range < segment_bytes / 4,
            "the range read fetched {range} of the segment's {segment_bytes} bytes"
        );
    });
}

/// A ledger record reads back by its seq, from the open log and after seals have moved it into
/// sealed history, by a lookup: the ledger directory names the one segment that holds it, and no
/// other sealed segment is read, so an unreadable later segment doesn't matter. A transcript
/// record's seq, or one no record has, reads as `None`.
fn a_ledger_record_reads_back_by_its_seq_from_the_open_log_and_sealed_history(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let event = Record::Event(crate::record::Event {
            name: "compaction_view_applied".into(),
            run_id: Some("r".into()),
            body: Some(serde_json::json!({"view": "{}"})),
        });
        let mut cuts = Vec::new();
        let mut at = None;
        let mut transcript = None;
        for n in 0..30 {
            for (part, batch) in round(n).into_iter().enumerate() {
                let seq = session.append(&batch, true).await.unwrap();
                if part == 0 && (n == 12 || n == 24) {
                    cuts.push(seq);
                }
                if n == 5 && part == 0 {
                    transcript = Some(seq);
                    at = Some(
                        session
                            .append(std::slice::from_ref(&event), true)
                            .await
                            .unwrap(),
                    );
                }
            }
        }
        let (at, transcript) = (at.unwrap(), transcript.unwrap());
        assert_eq!(
            session.ledger_record(at).await.unwrap().as_ref(),
            Some(&event)
        );
        let segments = |backend: &MemoryBackend| -> Vec<String> {
            backend
                .object_names(&addr())
                .into_iter()
                .filter(|name| name.ends_with("/l"))
                .collect()
        };
        session.seal(Some(cuts[0]), 0).await.unwrap();
        let first = segments(&backend);
        assert_eq!(first.len(), 1);
        session.seal(Some(cuts[1]), 0).await.unwrap();
        session.close().await.unwrap();
        let later: Vec<String> = segments(&backend)
            .into_iter()
            .filter(|name| !first.contains(name))
            .collect();
        assert_eq!(later.len(), 1);
        for name in &later {
            backend.set_object(&addr(), name, b"unreadable".to_vec());
        }
        let loaded = Session::view(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        assert_eq!(
            loaded.ledger_record(at).await.unwrap().as_ref(),
            Some(&event)
        );
        assert_eq!(loaded.ledger_record(transcript).await.unwrap(), None);
        assert_eq!(loaded.ledger_record(u64::MAX).await.unwrap(), None);
        // The segment the directory names is the one read.
        backend.set_object(&addr(), &first[0], b"unreadable".to_vec());
        assert!(loaded.ledger_record(at).await.is_err());
    });
}

/// Message ids resolve to their ordinals, in the window and after sealing. Converted ids
/// (`m<prefix>.<position>`) resolve by position, new ids (`g<generation>.<seq>`) by the seq of
/// their facts record, and any id whose entry doesn't carry exactly that id resolves to nothing:
/// another prefix, a seq that isn't a facts record, a non-canonical number, an ordinal past
/// the end.
fn message_ids_resolve_in_the_window_and_sealed_history(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let mut cut = 0;
        for n in 0..10 {
            for (part, batch) in round(n).into_iter().enumerate() {
                let seq = session.append(&batch, true).await.unwrap();
                if part == 0 && n == 6 {
                    cut = seq;
                }
            }
        }
        // Two new messages with minted ids: a user input, then an answer.
        let mut minted = Vec::new();
        for message in [
            input("new".into()),
            assistant(vec![ContentBlock::Text { text: "a".into() }]),
        ] {
            let ordinal = session.conversation.placed + 1;
            let mid = session.message_id(1);
            let Record::EntryFacts(mut facts) = facts(ordinal) else {
                unreachable!()
            };
            std::sync::Arc::make_mut(&mut facts).mid = mid.clone();
            session
                .append(&[message, Record::EntryFacts(facts)], true)
                .await
                .unwrap();
            minted.push((mid, ordinal));
        }
        assert!(minted[0].0.starts_with('g'));

        async fn check(session: &Session<&MemoryBackend>, minted: &[(String, u64)]) {
            for ordinal in (1..=40).filter(|ordinal| ordinal % 4 != 3) {
                let mid = format!("mfeedface.{}", ordinal - 1);
                assert_eq!(
                    session.find_message(&mid).await.unwrap(),
                    Some(ordinal),
                    "{mid}"
                );
            }
            for (mid, ordinal) in minted {
                assert_eq!(
                    session.find_message(mid).await.unwrap(),
                    Some(*ordinal),
                    "{mid}"
                );
            }
            // A tool message has no facts, so a converted id can't name it.
            assert_eq!(session.find_message("mfeedface.2").await.unwrap(), None);
            for wrong in [
                "mdeadbeef.0",
                "mfeedface.00",
                "mfeedface.99",
                "m0",
                "g0000000000000000.1",
                "x1",
                "",
            ] {
                assert_eq!(session.find_message(wrong).await.unwrap(), None, "{wrong}");
            }
            // The minted id's seq with another generation names nothing.
            let (mid, _) = &minted[0];
            let other = format!("gffffffffffffffff{}", &mid[17..]);
            assert_eq!(session.find_message(&other).await.unwrap(), None);
        }
        check(&session, &minted).await;
        session.seal(Some(cut), 0).await.unwrap();
        assert!(session.conversation.window_start() > 20);
        check(&session, &minted).await;
        session.close().await.unwrap();
        let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        check(&loaded, &minted).await;
    });
}

/// An answer whose id its step's start announced (`StepStart::mid`, minted from that record's
/// seq) resolves by that id in the window, once sealed and after a reload. A step's start whose
/// step wrote no message announces an id that resolves to nothing.
fn an_id_a_steps_start_announced_resolves_in_the_window_and_sealed_history(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        // Two rounds, then a third whose final answer comes from a step that announced its id.
        for n in 0..3 {
            for (part, batch) in round(n).into_iter().enumerate() {
                if n < 2 || part < 3 {
                    session.append(&batch, true).await.unwrap();
                }
            }
        }
        let step_start = |mid: &String| {
            Record::StepStart(StepStart {
                run_id: "r".into(),
                step_id: 9,
                mid: Some(mid.clone()),
            })
        };
        // A step that wrote nothing, then one whose answer is the next message.
        let unused = session.message_id(0);
        session.append(&[step_start(&unused)], true).await.unwrap();
        let announced = session.message_id(0);
        let ordinal = session.conversation.placed + 1;
        let Record::EntryFacts(mut answer_facts) = facts(ordinal) else {
            unreachable!()
        };
        std::sync::Arc::make_mut(&mut answer_facts).mid = announced.clone();
        session
            .append(
                &[
                    step_start(&announced),
                    assistant(vec![ContentBlock::Text { text: "a".into() }]),
                    Record::EntryFacts(answer_facts),
                ],
                true,
            )
            .await
            .unwrap();
        let mut cut = 0;
        for n in 3..6 {
            for (part, batch) in round(n).into_iter().enumerate() {
                let seq = session.append(&batch, true).await.unwrap();
                if part == 0 && n == 4 {
                    cut = seq;
                }
            }
        }
        async fn check(
            session: &Session<&MemoryBackend>,
            announced: &str,
            ordinal: u64,
            unused: &str,
        ) {
            assert_eq!(
                session.find_message(announced).await.unwrap(),
                Some(ordinal)
            );
            assert_eq!(session.find_message(unused).await.unwrap(), None);
        }
        check(&session, &announced, ordinal, &unused).await;
        session.seal(Some(cut), 0).await.unwrap();
        assert!(session.conversation.window_start() > ordinal);
        check(&session, &announced, ordinal, &unused).await;
        session.close().await.unwrap();
        let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        check(&loaded, &announced, ordinal, &unused).await;
    });
}

/// An assistant message waiting for its tool results is read last, at its reserved ordinal,
/// with its facts and no next message.
fn a_held_assistant_message_reads_last_at_its_reserved_ordinal(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        session
            .append(&[input("ask".into()), facts(1)], true)
            .await
            .unwrap();
        session
            .append(&[assistant(vec![call("c")]), facts(2)], true)
            .await
            .unwrap();
        let entries = session.entries(1, 10).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[1].ordinal, 2);
        assert!(entries[1].open);
        assert!(entries[1].next.is_none());
        assert_eq!(entries[1].state.facts.as_ref().map(|f| f.tag_base), Some(2));
        assert_eq!(session.entries(2, 10).await.unwrap().len(), 1);
    });
}

/// Every record of both streams from a seq on reads back once, in seq order, wherever two cuts
/// left it: sealed ledger and transcript segments, a segment the read starts inside, and the
/// open log, which keeps the transcript after the last cut.
fn records_from_a_seq_read_both_streams_sealed_and_open(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let mut appended = Vec::new();
        let mut cuts = Vec::new();
        for n in 0..30 {
            for (part, mut batch) in round(n).into_iter().enumerate() {
                // A ledger record in every batch, so both streams are sealed.
                batch.insert(
                    0,
                    Record::StepStart(StepStart {
                        run_id: "r".into(),
                        step_id: (4 * n + part) as u64,
                        mid: None,
                    }),
                );
                let first = session.append(&batch, true).await.unwrap();
                if part == 0 && (n == 12 || n == 24) {
                    // A cut starts at the round's input, after its ledger record.
                    cuts.push(first + 1);
                }
                appended.extend((first..).zip(batch));
            }
        }
        for cut in &cuts {
            session.seal(Some(*cut), 0).await.unwrap();
        }
        session.close().await.unwrap();
        let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        for first in [
            1,
            cuts[0] - 7,
            cuts[0],
            cuts[1] + 3,
            appended.last().unwrap().0,
        ] {
            let read = loaded.records_from(first).await.unwrap();
            assert!(
                read.windows(2).all(|pair| pair[0].0 < pair[1].0),
                "from {first}: seqs ascend once each"
            );
            // The seals' own records are in the log too; every appended record reads back.
            let ours: Vec<&(u64, Record)> = read
                .iter()
                .filter(|(_, record)| {
                    !matches!(record, Record::ChangeIntent(_) | Record::WriterOpened(_))
                })
                .collect();
            let expected: Vec<&(u64, Record)> =
                appended.iter().filter(|(seq, _)| *seq >= first).collect();
            assert_eq!(ours, expected, "from {first}");
        }
    });
}

fn ledger_tail_stops_before_older_segments_and_reads_the_open_log_once(cipher: Cipher) {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(cipher), false)
            .await
            .unwrap();
        let mut older = Vec::new();
        let mut newest = Vec::new();
        let mut answers = Vec::new();
        for n in 0..30 {
            let mut cut = None;
            for (part, batch) in round(n).into_iter().enumerate() {
                let seq = session.append(&batch, true).await.unwrap();
                if part == 0 && (n == 12 || n == 24) {
                    cut = Some(seq);
                }
            }
            if n == 5 || n == 18 || n == 28 {
                let record = Record::Event(crate::record::Event {
                    name: format!("tail_answer_{n}"),
                    run_id: Some("r".into()),
                    body: None,
                });
                let seq = session
                    .append(std::slice::from_ref(&record), true)
                    .await
                    .unwrap();
                if n != 5 {
                    answers.push((seq, record));
                }
            }
            if let Some(cut) = cut {
                session.seal(Some(cut), 0).await.unwrap();
                let segments: Vec<_> = backend
                    .object_names(&addr())
                    .into_iter()
                    .filter(|name| name.ends_with("/l"))
                    .collect();
                if n == 12 {
                    older = segments;
                } else {
                    newest = segments
                        .into_iter()
                        .filter(|name| !older.contains(name))
                        .collect();
                }
            }
        }
        assert_eq!(older.len(), 1);
        assert_eq!(newest.len(), 1);
        session.close().await.unwrap();
        let loaded = Session::load(&backend, addr(), &triple(), &deployment(cipher))
            .await
            .unwrap();
        for name in older {
            backend.set_object(&addr(), &name, b"unreadable old segment".to_vec());
        }
        let logs = backend.log_reads();
        let objects = backend.object_reads();
        let open = loaded
            .ledger_tail_until(|record| record == &answers[1].1)
            .await
            .unwrap();
        assert_eq!(open.last(), Some(&answers[1]));
        assert_eq!(
            backend.object_reads() - objects,
            0,
            "an open-log answer fetches no sealed object"
        );
        assert!(
            backend.log_reads() - logs <= 1,
            "at most one open-log fallback read"
        );

        let logs = backend.log_reads();
        let objects = backend.object_reads();
        let sealed = loaded
            .ledger_tail_until(|record| record == &answers[0].1)
            .await
            .unwrap();
        assert_eq!(sealed.last(), Some(&answers[0]));
        assert!(sealed.windows(2).all(|pair| pair[0].0 > pair[1].0));
        let directory_pages = loaded.head().ledger_dir.as_ref().unwrap().level as usize + 1;
        assert_eq!(
            backend.object_reads() - objects,
            directory_pages + 1,
            "only directory pages and the newest ledger segment are fetched"
        );
        assert!(
            backend.log_reads() - logs <= 1,
            "the sealed search does not reread the open log"
        );
        // The counted segment is indispensable, not a cache or a synthesized answer.
        backend.set_object(&addr(), &newest[0], b"unreadable newest segment".to_vec());
        assert!(loaded
            .ledger_tail_until(|record| record == &answers[0].1)
            .await
            .is_err());
    });
}
