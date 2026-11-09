use std::path::{Path, PathBuf};

use broca_protocol::content::{ContentBlock, Message, Role};
use broca_protocol::{BindPrincipal, SessionScope};
use broca_session_store::record::{Placement, Record};
use broca_wire::head_import::{FileImportParams, ImportHarness, ImportOrigin, ImportSource};
use ck_session_import::head::{
    CallIdentity, HeadHistory, HeadMessage, HeadUsage, SendFacts, SourceRow,
};

use super::*;

fn scratch(name: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "store-import-{name}-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write(path: &Path, bytes: &[u8]) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
}

#[cfg(unix)]
#[test]
fn the_path_guard_admits_only_files_under_a_root_after_symlinks() {
    let dir = scratch("guard");
    let root = dir.join("root");
    let outside = dir.join("outside");
    write(&root.join("head.json"), b"{}");
    write(&outside.join("secret.json"), b"{}");
    std::os::unix::fs::symlink(outside.join("secret.json"), root.join("escape.json")).unwrap();
    std::os::unix::fs::symlink(root.join("head.json"), outside.join("inward.json")).unwrap();
    std::fs::create_dir_all(root.join("sub")).unwrap();
    let roots = ImportRoots::new(vec![root.clone()], Some(root.clone()));
    assert_eq!(
        roots.admit(&root.join("head.json")).unwrap(),
        std::fs::canonicalize(root.join("head.json")).unwrap()
    );
    // A link from outside to a file inside reads the file inside.
    assert!(roots.admit(&outside.join("inward.json")).is_ok());
    for refused in [
        outside.join("secret.json"),
        root.join("escape.json"),
        root.join("sub"),
        root.clone(),
        root.join("missing.json"),
        root.join("../outside/secret.json"),
        PathBuf::from("root/head.json"),
    ] {
        let refusal = roots.admit(&refused).unwrap_err();
        assert_eq!(refusal.code, "import_path_refused", "{}", refused.display());
    }
    // No configured root admits nothing.
    assert!(ImportRoots::new(Vec::new(), None)
        .admit(&root.join("head.json"))
        .is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(unix)]
#[test]
fn only_files_inside_the_import_folder_are_ever_deleted() {
    let dir = scratch("delete");
    let folder = dir.join("import-sources");
    let other = dir.join("other-root");
    write(&folder.join("head.json"), b"{}");
    write(&folder.join("head.sends.json"), b"[]");
    write(&other.join("elsewhere.json"), b"{}");
    write(&other.join("elsewhere.sends.json"), b"[]");
    std::os::unix::fs::symlink(other.join("elsewhere.json"), folder.join("link.json")).unwrap();
    let roots = ImportRoots::new(vec![folder.clone(), other.clone()], Some(folder.clone()));
    assert!(roots.deletable(&folder.join("head.json")));
    assert!(!roots.deletable(&other.join("elsewhere.json")));
    assert!(
        !roots.deletable(&folder.join("link.json")),
        "a link out of the folder"
    );
    assert!(!roots.deletable(&folder));
    // An OpenCode 2 export and its side file go; a file outside the folder stays.
    delete_sources(&roots, ImportHarness::OpenCode2, &folder.join("head.json"));
    assert!(!folder.join("head.json").exists());
    assert!(!folder.join("head.sends.json").exists());
    delete_sources(
        &roots,
        ImportHarness::OpenCode2,
        &other.join("elsewhere.json"),
    );
    assert!(other.join("elsewhere.json").exists());
    assert!(other.join("elsewhere.sends.json").exists());
    // A Pi journal is never deleted, even inside the folder; its side file is.
    write(&folder.join("pi.jsonl"), b"{}\n");
    write(&folder.join("pi.sends.json"), b"[]");
    delete_sources(&roots, ImportHarness::Pi, &folder.join("pi.jsonl"));
    assert!(folder.join("pi.jsonl").exists());
    assert!(!folder.join("pi.sends.json").exists());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_source_whose_digest_differs_is_refused_before_it_is_read() {
    let dir = scratch("digest");
    let path = dir.join("head.json");
    write(&path, b"not json at all");
    let roots = ImportRoots::new(vec![dir.clone()], None);
    let mut params = params(ImportHarness::OpenCode2, &path, "0".repeat(64));
    assert_eq!(
        read_source(&params, &roots).unwrap_err().code,
        "import_digest_mismatch"
    );
    params.source.sha256 = format!("{:x}", sha2::Sha256::digest(b"not json at all"));
    assert_eq!(
        read_source(&params, &roots).unwrap_err().code,
        "import_source_unreadable"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

fn params(harness: ImportHarness, path: &Path, sha256: String) -> FileImportParams {
    FileImportParams {
        import_id: "imp-1".into(),
        source: ImportSource {
            harness,
            path: path.display().to_string(),
            session: "ses_head".into(),
            sha256,
        },
        origin: ImportOrigin {
            kind: "import".into(),
            source_ref: "thalamus:ses_head".into(),
        },
        tag_floor: Some(100),
    }
}

fn core() -> BindPrincipal {
    BindPrincipal::Reserved {
        module_id: "prefrontal-core".into(),
    }
}

fn text(role: Role, text: &str) -> Message {
    Message::new(role, vec![ContentBlock::Text { text: text.into() }])
}

fn message(mid: Option<&str>, message: Message) -> HeadMessage {
    HeadMessage {
        mid: mid.map(str::to_owned),
        message,
        written_at_ms: mid.map(|_| 1_000),
        created_at_ms: mid.map(|_| 1_000),
        usage: None,
        media: Vec::new(),
        calls: Vec::new(),
        send: None,
        rides: Vec::new(),
    }
}

fn send(sender: &str, template: Option<&str>) -> SendFacts {
    SendFacts {
        client_message_id: Some(format!("cm-{sender}")),
        sender: sender.into(),
        template: template.map(str::to_owned),
        marks: Some(serde_json::json!({"notice": true})),
        delivery_mode: None,
        delivery_tool_call_id: None,
        text: None,
        send_id: "s".into(),
    }
}

/// A history of: the owner's plain prompt, the owner's templated prompt, a module's send, and
/// an assistant reply whose call's results came as a message broca makes, with a ride.
fn history() -> HeadHistory {
    let call: ContentBlock = serde_json::from_value(serde_json::json!({
        "type": "tool_call", "tool_call_id": "c1", "tool_name": "read", "input": {}
    }))
    .unwrap();
    let result: ContentBlock = serde_json::from_value(serde_json::json!({
        "type": "tool_result", "tool_call_id": "c1",
        "output": {"kind": "text", "text": "ok"}, "is_error": false
    }))
    .unwrap();
    let mut plain = message(Some("u1"), text(Role::User, "plain prompt"));
    plain.send = Some(send("prefrontal-core", None));
    let mut templated = message(Some("u2"), text(Role::User, "templated prompt"));
    templated.send = Some(send("prefrontal-core", Some("core-nudge")));
    let mut module = message(Some("u3"), text(Role::User, "from a module"));
    module.send = Some(send("magic-context", None));
    let mut assistant = message(
        Some("a1"),
        Message::new(
            Role::Assistant,
            vec![
                ContentBlock::Text {
                    text: "reading".into(),
                },
                call,
            ],
        ),
    );
    assistant.usage = Some(HeadUsage {
        prompt_tokens: Some(10),
        output_tokens: Some(2),
        ..HeadUsage::default()
    });
    assistant.calls = vec![CallIdentity {
        tool_call_id: "c1".into(),
        call_key: "a1/c1".into(),
        carrier: "thalamus-harness".into(),
    }];
    let mut results = message(None, Message::new(Role::Tool, vec![result]));
    let mut ride = send("occi", Some("occi-ride"));
    ride.delivery_mode = Some("tool_result".into());
    ride.delivery_tool_call_id = Some("c1".into());
    ride.text = Some("\n\nride".into());
    ride.send_id = "ride-1".into();
    results.rides = vec![ride];
    HeadHistory {
        messages: vec![plain, templated, module, assistant, results],
        rows: vec![
            SourceRow {
                mid: "u1".into(),
                message: Some(0),
            },
            SourceRow {
                mid: "sys".into(),
                message: None,
            },
            SourceRow {
                mid: "u2".into(),
                message: Some(1),
            },
            SourceRow {
                mid: "u3".into(),
                message: Some(2),
            },
            SourceRow {
                mid: "a1".into(),
                message: Some(3),
            },
        ],
        dropped_system_messages: 1,
    }
}

fn seeded(history: HeadHistory, floor: Option<u64>) -> Result<Seed, ImportRefusal> {
    let mut params = params(ImportHarness::OpenCode2, Path::new("/x"), "ab".into());
    params.tag_floor = floor;
    let scope = SessionScope {
        owner: core(),
        scope_ref: "agent:head".into(),
        scope_epoch: 1,
        flow_id: None,
        sender: None,
        agent_id: None,
        run_id: None,
        agent_id_frozen: false,
    };
    seed(
        history,
        &SeedFacts {
            params: &params,
            principal: &core(),
            scope: Some(&scope),
            now_ms: 5,
            prefix: "gfeed",
            first_seq: 10,
        },
    )
}

fn facts(seed: &Seed) -> Vec<&broca_session_store::record::EntryFacts> {
    seed.records
        .iter()
        .filter_map(|record| match record {
            Record::EntryFacts(facts) => Some(facts),
            _ => None,
        })
        .collect()
}

/// The source runner's notice rule: the owner's plain prompt has none, the owner's templated
/// prompt and a module's send have one, naming the sender and template.
#[test]
fn notices_follow_the_source_runners_rule() {
    let seed = seeded(history(), Some(100)).unwrap();
    let notices: Vec<_> = facts(&seed)
        .iter()
        .map(|facts| {
            facts
                .notice
                .as_ref()
                .map(|notice| (notice.sender.clone(), notice.template.clone()))
        })
        .collect();
    assert_eq!(
        notices,
        [
            None,
            Some(("prefrontal-core".into(), Some("core-nudge".into()))),
            Some(("magic-context".into(), None)),
            None,
            None,
        ]
    );
}

/// Part numbers count on from `tag_floor`; a floor beyond the protocol's integers is refused.
#[test]
fn part_numbers_start_above_the_tag_floor() {
    let seed = seeded(history(), Some(100)).unwrap();
    let bases: Vec<u64> = facts(&seed).iter().map(|facts| facts.tag_base).collect();
    assert_eq!(bases[0], 100);
    assert!(bases.windows(2).all(|pair| pair[0] <= pair[1]));
    assert!(bases[1] > 100, "the first message's text takes a number");
    assert_eq!(
        seeded(history(), Some(9_007_199_254_740_992))
            .err()
            .unwrap()
            .code,
        "import_tag_floor_refused"
    );
    assert_eq!(facts(&seeded(history(), None).unwrap())[0].tag_base, 0);
}

/// Each message keeps its source id; the results message broca makes takes an id it mints from
/// its facts record's seq; the batch is one seeded run with every message placed `Imported`.
#[test]
fn the_seed_keeps_source_ids_and_mints_only_for_messages_broca_makes() {
    let seed = seeded(history(), Some(0)).unwrap();
    assert!(matches!(&seed.records[0], Record::RunStart(start)
        if start.seeded == Some(true)
            && start.send_id.as_deref() == Some("imp-1")
            && start.compaction_plan_version.is_some()));
    assert!(matches!(seed.records.last(), Some(Record::RunEnd(_))));
    assert!(seed.records.iter().all(|record| match record {
        Record::Input(input) => input.placement == Placement::Imported,
        _ => true,
    }));
    let mids: Vec<_> = facts(&seed).iter().map(|facts| facts.mid.clone()).collect();
    let results_at = seed
        .records
        .iter()
        .rposition(|record| matches!(record, Record::EntryFacts(_)))
        .unwrap();
    assert_eq!(
        mids,
        [
            "u1",
            "u2",
            "u3",
            "a1",
            &format!("gfeed.{}", 10 + results_at)
        ]
    );
    assert_eq!(
        seed.ids,
        [
            ("u1".to_owned(), 1),
            ("u2".to_owned(), 2),
            ("u3".to_owned(), 3),
            ("a1".to_owned(), 4)
        ]
    );
    let moves: Vec<_> = seed
        .receipt
        .moves
        .iter()
        .map(|row| (row.mid.as_str(), row.ordinal))
        .collect();
    // The results broca split off `a1` are listed right after it, under the id minted for them.
    let minted = format!("gfeed.{}", 10 + results_at);
    assert_eq!(
        moves,
        [
            ("u1", Some(1)),
            ("sys", None),
            ("u2", Some(2)),
            ("u3", Some(3)),
            ("a1", Some(4)),
            (minted.as_str(), Some(5))
        ]
    );
    assert_eq!(seed.receipt.dropped_system_messages, 1);
}

/// The calls keep their source keys with their carrier; a ride becomes delivered text beside
/// the result with its sender, template and send id; a send's facts land on its message.
#[test]
fn call_keys_rides_and_send_facts_are_recorded() {
    let seed = seeded(history(), Some(0)).unwrap();
    let turn = seed
        .records
        .iter()
        .find_map(|record| match record {
            Record::TurnFacts(turn) => Some(turn),
            _ => None,
        })
        .unwrap();
    assert_eq!(turn.ordinal, 4);
    assert_eq!(turn.calls[0].call_key, "a1/c1");
    assert_eq!(turn.calls[0].carrier.as_deref(), Some("thalamus-harness"));
    let ride = seed
        .records
        .iter()
        .find_map(|record| match record {
            Record::ResultAppend(append) => Some(append),
            _ => None,
        })
        .unwrap();
    assert_eq!(ride.text, "\n\nride");
    assert_eq!(ride.send_id.as_deref(), Some("ride-1"));
    assert_eq!(ride.notice.as_ref().unwrap().sender, "occi");
    assert_eq!(
        ride.notice.as_ref().unwrap().template.as_deref(),
        Some("occi-ride")
    );
    let all = facts(&seed);
    assert_eq!(
        all[2].client_message_id.as_deref(),
        Some("cm-magic-context")
    );
    assert_eq!(all[2].sender.as_deref(), Some("magic-context"));
    assert_eq!(all[2].marks, Some(serde_json::json!({"notice": true})));
    assert_eq!(all[3].usage.unwrap().prompt_tokens, Some(10));
    assert_eq!(all[4].delivery_mode.as_deref(), Some("tool_result"));
    assert_eq!(all[4].delivery_tool_call_id.as_deref(), Some("c1"));
    // The results message has no time of its own and takes the one before it.
    assert_eq!(all[4].written_at_ms, Some(1_000));
    assert_eq!(all[4].created_at_ms, None);
    assert!(all
        .iter()
        .all(|facts| facts.run_id.is_none() && !facts.native));
}

#[test]
fn an_empty_history_or_a_broca_shaped_id_is_refused() {
    let mut empty = history();
    empty.messages.clear();
    assert_eq!(seeded(empty, None).err().unwrap().code, "import_empty");
    let mut shaped = history();
    shaped.messages[0].mid = Some("g0123456789abcdef.4".into());
    assert_eq!(
        seeded(shaped, None).err().unwrap().code,
        "import_id_refused"
    );
}
