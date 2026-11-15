//! Inline-import and output-reference replies of a session in the new storage, each pinned.

use super::*;
use crate::actor::LineageCmd;
use crate::serve::{ServeHandler, SessionImportRequest};
use broca_protocol::{ContentBlock, Message, Role};
use tokio::sync::oneshot;

fn imported(module: &Module, run: &str, messages: Vec<Message>) -> SessionImportRequest {
    SessionImportRequest {
        project_id: None,
        principal: None,
        scope: broca_protocol::SendScope::Internal,
        session: module.key.clone(),
        run_id: RunId(run.into()),
        messages,
        source_ref: "fixture:transcript".into(),
    }
}

fn text(role: Role, text: &str) -> Message {
    Message::new(role, vec![ContentBlock::Text { text: text.into() }])
}

#[tokio::test]
async fn inline_import_normalization_retry_reuse_and_nonempty_refusal_match_today() {
    let script = vec![super::super::tests::text_step("continued", "next answer")];
    let store = module(true, script).await;
    let messages = vec![text(Role::System, "discarded"), text(Role::User, "question"),
        serde_json::from_value(serde_json::json!({"role":"assistant","content":[{
            "type":"tool_call","tool_call_id":"c1","tool_name":"bash","input":{}
        }]})).unwrap(),
        serde_json::from_value(serde_json::json!({"role":"tool","content":[{
            "type":"tool_result","tool_call_id":"orphan","output":{"kind":"text","text":"discarded"},"is_error":false
        }]})).unwrap()];
    for (run, payload, expected) in [
        ("seed", messages.clone(), "ok"),
        ("seed", messages.clone(), "retry"),
        ("seed", vec![text(Role::User, "different")], "reuse"),
        ("other", messages, "nonempty"),
    ] {
        let new = store.registry.import(imported(&store, run, payload)).await;
        broca_test_support::pin!(expected, format!("{new:?}"));
        match expected {
            "ok" | "retry" => {
                let reply = new.unwrap();
                assert_eq!(reply.already_imported, expected == "retry");
                assert_eq!(reply.synthesized_tool_results, 1);
                assert_eq!(reply.dropped_orphan_tool_results, 1);
                assert_eq!(reply.dropped_system_messages, Some(1));
            }
            "reuse" => assert!(matches!(new, Err(crate::serve::ServeError::ImportIdReuse))),
            _ => assert!(new.is_err()),
        }
    }
    opted_reads(&store, "inline import").await;
    store.closed().await;
    assert!(store.head().exists());
    assert_eq!(store.wal.wal_present(&store.key), Some(false));
    let mut continued = Vec::new();
    for module in [&store] {
        let mut request = request("continue");
        // An inline import froze no tools. A later send inherits that empty tool set rather
        // than trying to replace it with the harness's default read tool.
        request.tools = None;
        let request = module.caller.shape("continued", &SendKey::Minted, request);
        continued.push(
            module
                .registry
                .route_submission_with_order(
                    &module.key,
                    RunId("continued".into()),
                    request,
                    RunKind::FreshOrResume,
                    SendKey::Minted,
                    false,
                    broca_protocol::Delivery::Queue,
                    None,
                    None,
                    None,
                    None,
                    None,
                    false,
                    None,
                )
                .await
                .unwrap(),
        );
    }
    broca_test_support::pin!("alike", continued[0]);
    assert!(
        matches!(continued[0], SendOutcome::Active { .. }),
        "{:?}",
        continued[0]
    );
    store.closed().await;
    opted_reads(&store, "continued inline import").await;
}

#[tokio::test]
async fn inline_import_invalid_and_empty_payloads_match_today_and_leave_a_fillable_session() {
    let store = module(true, vec![]).await;
    for messages in [
        vec![],
        vec![text(Role::System, "only system")],
        vec![Message::new(Role::User, vec![])],
    ] {
        let new = store
            .registry
            .import(imported(&store, "seed", messages))
            .await;
        broca_test_support::pin!("alike", format!("{new:?}"));
        assert!(new.is_err());
    }
    for module in [&store] {
        module
            .registry
            .import(imported(module, "seed", vec![text(Role::User, "usable")]))
            .await
            .unwrap();
    }
    opted_reads(&store, "valid import after refusals").await;
    store.closed().await;
}

#[tokio::test]
async fn conversation_key_sends_and_subsequent_sends_are_admitted_alike() {
    // These in-process loops have no account selection. This proves admission and read parity,
    // not routing; the real-daemon affinity differential compares the frozen auth selections.
    let script = vec![
        super::super::tests::text_step("a1", "answer"),
        super::super::tests::text_step("a2", "next"),
    ];
    let store = module(true, script).await;
    for (run, key) in [("r1", Some("shared-conversation")), ("r2", None)] {
        let new = store.send(run, "input", key).await;
        broca_test_support::pin!("alike", new);
        assert!(matches!(new, SendOutcome::Active { .. }));
        store.closed().await;
    }
    assert!(
        store.head().exists(),
        "a keyed creating send starts in the store"
    );
    opted_reads(&store, "conversation-key sends").await;
}

#[tokio::test]
async fn queued_conversation_keys_and_key_reuse_are_served_alike() {
    let new_gate = Arc::new(Notify::new());
    let new_reached = Arc::new(Notify::new());
    let script = vec![
        super::super::tests::read_step("a1"),
        super::super::tests::text_step("a2", "first answer"),
        super::super::tests::text_step("a3", "queued answer"),
    ];
    let parts = broca_session_loop::parts::SharedParts::new(Box::new(
        broca_session_loop::parts::MemoryParts::default(),
    ));
    let mut executor = executor(script, None);
    executor.planes.tool_gate = Some((new_reached.clone(), new_gate.clone()));
    let (commands, served, _) = super::super::tests::actor_with(executor, parts).await;
    let caller = super::super::tests::Caller::default();
    for (run, id, conversation) in [
        ("r1", "k1", "first"),
        ("r2", "k2", "queued"),
        ("r2", "k2", "queued"),
        ("r2", "k2", "changed"),
    ] {
        let key = SendKey::SendId(id.into());
        let request = caller.shape(run, &key, request("input"));
        let (reply, answer) = oneshot::channel();
        commands
            .send(LineageCmd::Send {
                session_class: None,
                retention: None,
                stop_sequences_named: false,
                conversation_key: Some(conversation.into()),
                title: None,
                run_id: RunId(run.into()),
                request: Box::new(request),
                kind: RunKind::FreshOrResume,
                key,
                skip_materialization: false,
                delivery: broca_protocol::Delivery::Queue,
                plan: None,
                order: None,
                reply,
            })
            .unwrap();
        let new = answer.await.unwrap();
        broca_test_support::pin!(format!("{id}: {conversation}"), new);
        if conversation == "changed" {
            assert!(matches!(
                new,
                SendOutcome::SendIdReuse {
                    differing: "conversation_key"
                }
            ));
        }
    }
    new_reached.notified().await;
    for messages in [vec![], vec![text(Role::User, "import while running")]] {
        let imported = broca_core::ImportRequest {
            project_id: None,
            principal: None,
            run_id: RunId("seed".into()),
            session: BindIdentity::new("/project", "store-actor", "s1"),
            messages,
            origin: broca_core::ImportOrigin {
                source_ref: "fixture:busy".into(),
            },
        };
        let (reply, answer) = oneshot::channel();
        commands
            .send(LineageCmd::Import {
                request: Box::new(imported),
                reply,
            })
            .unwrap();
        let new = answer.await.unwrap();
        broca_test_support::pin!("alike", new);
        assert!(matches!(new, crate::actor::ImportOutcome::Busy));
    }
    new_gate.notify_one();
    super::super::tests::idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    broca_test_support::pin!("alike", texts(&session));
}

async fn referencing(module: &Module) {
    let mut manifest =
        (*super::controls::manifest(serde_json::json!({"render_tags":true}), vec![])).clone();
    manifest.system_text.clear();
    let manifest = Arc::new(manifest);
    let sent = module
        .registry
        .route_submission_with_order(
            &module.key,
            RunId("refs".into()),
            module.caller.shape("refs", &SendKey::Minted, request("hi")),
            RunKind::FreshOrResume,
            SendKey::Minted,
            false,
            broca_protocol::Delivery::Queue,
            Some(crate::actor::SendPlan {
                identity: manifest.plan_identity.clone(),
                manifest: Some(manifest),
                model_check: None,
            }),
            None,
            None,
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
    module.closed().await;
}

fn reference_script() -> Vec<Vec<NormalizedEvent>> {
    vec![
        super::super::tests::read_step("a1"),
        super::super::tests::text_step("a2", "see $$tc(2), $$tc(1), $$tc(999)"),
    ]
}

async fn reference_set(module: &Module) -> crate::output_refs::OutputRefSet {
    let page = module
        .registry
        .read(crate::serve::SessionReadRequest::new(
            module.key.clone(),
            None,
            Some(100),
        ))
        .await
        .unwrap();
    let ordinal = page.messages.last().unwrap().ordinal - 1;
    crate::output_refs::message_output_refs(&module.registry, &module.key, ordinal, None)
        .await
        .unwrap()
        .expect("the tagged assistant reply has a set")
        .1
        .reply
}

async fn listed(module: &Module, set: &str) -> serde_json::Value {
    let request = serde_json::from_value(serde_json::json!({"set_id":set})).unwrap();
    serde_json::to_value(
        module
            .registry
            .output_refs(&module.key, &request)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap()
}

async fn read_ref(module: &Module, id: &str, offset: u64, length: u64) -> serde_json::Value {
    let request =
        serde_json::from_value(serde_json::json!({"ref_id":id,"offset":offset,"length":length}))
            .unwrap();
    serde_json::to_value(
        module
            .registry
            .output_ref_read(&module.key, &request)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap()
}

fn comparable_refs(mut page: serde_json::Value) -> serde_json::Value {
    for record in page["records"].as_array_mut().unwrap() {
        let record = record.as_object_mut().unwrap();
        for name in ["ref_id", "call_key", "part"] {
            record.remove(name);
        }
    }
    page
}

#[tokio::test]
async fn output_reference_list_and_byte_reads_match_today() {
    let store = module(true, reference_script()).await;
    referencing(&store).await;
    let new = reference_set(&store).await;
    let new_page = listed(&store, &new.descriptor.set_id).await;
    broca_test_support::pin!("the listed references", comparable_refs(new_page.clone()));
    assert_eq!(new_page["records"][0]["state"], "resolved");
    assert_eq!(new_page["records"][1]["reason"], "not_a_tool_output");
    assert_eq!(new_page["records"][2]["reason"], "unknown_tag");
    let new_id = new_page["records"][0]["ref_id"].as_str().unwrap();
    for (offset, length) in [(0, 131_072), (2, 3), (0, 0), (13, 1)] {
        broca_test_support::pin!(
            format!("{offset}+{length}"),
            read_ref(&store, new_id, offset, length).await
        );
    }
    assert_eq!(
        read_ref(&store, new_id, 0, 131_072).await,
        serde_json::json!({"data":"ZmlsZSBjb250ZW50cw==","size":13,"format":"text"})
    );
    let session = store.load().await;
    let namespace = store
        .registry
        .output_ref_namespace(&store.key, "unused")
        .await
        .unwrap();
    assert_eq!(
        namespace,
        broca_session_store::codec::hex(&session.head().incarnation.0)
    );
    // Another session's reference, to the same output of the same script.
    let other = module(true, reference_script()).await;
    referencing(&other).await;
    let other_set = reference_set(&other).await;
    let other_page = listed(&other, &other_set.descriptor.set_id).await;
    let other_id = other_page["records"][0]["ref_id"].as_str().unwrap();
    assert_ne!(other_id, new_id);
    let request =
        serde_json::from_value(serde_json::json!({"ref_id":other_id,"offset":0,"length":1}))
            .unwrap();
    assert!(
        store
            .registry
            .output_ref_read(&store.key, &request)
            .await
            .unwrap()
            .is_err(),
        "another session's reference cannot name this session's output"
    );
}

/// An output-reference set id on a session created on the new storage names the history version
/// the session stores, not the one its reads serve (`store_exposure`): a build from before the
/// exposure epoch served the stored version and scoped its ids by it, so an id it handed out
/// still resolves. An id scoped to the served version names nothing.
#[tokio::test]
async fn a_set_id_names_the_stored_history_version_and_resolves_across_the_raise() {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    let store = module(true, reference_script()).await;
    referencing(&store).await;
    let set = reference_set(&store).await;
    let stored = store.load().await.head().history_version;
    let page = store
        .registry
        .read(crate::serve::SessionReadRequest::new(
            store.key.clone(),
            None,
            Some(100),
        ))
        .await
        .unwrap();
    let served = crate::store_exposure::served_history_version(stored);
    assert_eq!(page.history_version, served);
    assert_ne!(served, stored);
    let namespace = store
        .registry
        .output_ref_namespace(&store.key, &page.incarnation)
        .await
        .unwrap();
    let scope = |version: u64| {
        format!(
            "h1.{}.{version}/",
            URL_SAFE_NO_PAD.encode(namespace.as_bytes())
        )
    };
    let id = &set.descriptor.set_id;
    assert!(id.starts_with(&scope(stored)), "{id}");
    let listed_page = listed(&store, id).await;
    assert_eq!(listed_page["records"][0]["state"], "resolved");
    let ref_id = listed_page["records"][0]["ref_id"].as_str().unwrap();
    assert_eq!(
        read_ref(&store, ref_id, 0, 131_072).await,
        serde_json::json!({"data":"ZmlsZSBjb250ZW50cw==","size":13,"format":"text"})
    );
    let at_served = id.replacen(&scope(stored), &scope(served), 1);
    let request = serde_json::from_value(serde_json::json!({"set_id": at_served})).unwrap();
    assert!(
        !matches!(
            store.registry.output_refs(&store.key, &request).await,
            Ok(Ok(_))
        ),
        "an id scoped to the served version names nothing"
    );
}
