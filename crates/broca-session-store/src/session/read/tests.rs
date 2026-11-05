use broca_protocol::content::{ContentBlock, Message, Role};
use broca_protocol::ids::MessageId;
use futures::executor::block_on;

use crate::backend::MemoryBackend;
use crate::head::SessionTriple;
use crate::keys::DeploymentKey;
use crate::record::{Assistant, EntryFacts, Input, Placement, Record, ToolReply};
use crate::session::Session;

fn triple() -> SessionTriple {
    SessionTriple {
        project_root: "/project".into(),
        harness: "broca".into(),
        session: "s".into(),
    }
}

fn deployment() -> DeploymentKey {
    DeploymentKey::new(1, &[5; 32])
}

fn addr() -> crate::backend::SessionAddr {
    crate::backend::SessionAddr("0123456789abcdef".into())
}

fn input(text: String) -> Record {
    Record::Input(Input {
        run_id: "r".into(),
        placement: Placement::RunInput,
        message: Message::new(Role::User, vec![ContentBlock::Text { text }]),
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
        message: Message::assistant_from(
            content,
            serde_json::from_value(serde_json::json!({"provider_module_id": "p", "model_id": "m"}))
                .unwrap(),
        ),
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
    })
}

/// The entry facts of the message at `ordinal`. Its tag base is the ordinal, so a read that
/// returns another message's facts shows it.
fn facts(ordinal: u64) -> Record {
    Record::EntryFacts(EntryFacts {
        ordinal,
        written_at_ms: ordinal,
        tag_base: ordinal,
        notice: None,
        usage: None,
        generated_under: None,
        native: true,
        render_tags: false,
        clean_reply: false,
    })
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
                text: format!("answer {n}"),
            }]),
            facts(first + 3),
        ],
    ]
}

/// Every message reads back by its ordinal from wherever it lives: the window, or a sealed
/// segment found through the directory, its index and one block. Two cuts make two segments,
/// each of several blocks. Ordinals outside the conversation read as `None`.
#[test]
fn every_message_reads_back_by_ordinal_after_two_cuts() {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(), false)
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
        let expected = session.conversation.messages.clone();
        assert_eq!(expected.len(), 120);
        assert_eq!(session.conversation.placed, 120);
        for cut in cuts {
            session.seal(Some(cut), 0).await.unwrap();
        }
        assert_eq!(session.conversation.window_start(), 97);
        session.close().await.unwrap();

        let loaded = Session::load(&backend, addr(), &triple(), &deployment())
            .await
            .unwrap();
        assert_eq!(loaded.conversation.messages.len(), 24);
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
    });
}

/// An assistant message waiting for its tool results is read last, at its reserved ordinal,
/// with its facts and no next message.
#[test]
fn a_held_assistant_message_reads_last_at_its_reserved_ordinal() {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(), false)
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
