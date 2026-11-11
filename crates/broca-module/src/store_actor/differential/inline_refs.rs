//! Public inline-import and reference replies compared across the two storage backends.

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
    let today = module(false, script.clone()).await;
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
        let old = today
            .registry
            .import(imported(&today, run, payload.clone()))
            .await;
        let new = store.registry.import(imported(&store, run, payload)).await;
        assert_eq!(format!("{new:?}"), format!("{old:?}"), "{expected}");
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
    opted_reads_alike(&today, &store, "inline import").await;
    store.closed().await;
    assert!(store.head().exists());
    assert_eq!(store.wal.wal_present(&store.key), Some(false));
    let mut continued = Vec::new();
    for module in [&today, &store] {
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
    assert_eq!(continued[1], continued[0]);
    assert!(
        matches!(continued[1], SendOutcome::Active { .. }),
        "{:?}",
        continued[1]
    );
    today.closed().await;
    store.closed().await;
    opted_reads_alike(&today, &store, "continued inline import").await;
}

#[tokio::test]
async fn inline_import_invalid_and_empty_payloads_match_today_and_leave_a_fillable_session() {
    let today = module(false, vec![]).await;
    let store = module(true, vec![]).await;
    for messages in [
        vec![],
        vec![text(Role::System, "only system")],
        vec![Message::new(Role::User, vec![])],
    ] {
        let old = today
            .registry
            .import(imported(&today, "seed", messages.clone()))
            .await;
        let new = store
            .registry
            .import(imported(&store, "seed", messages))
            .await;
        assert_eq!(format!("{new:?}"), format!("{old:?}"));
        assert!(new.is_err());
    }
    for module in [&today, &store] {
        module
            .registry
            .import(imported(module, "seed", vec![text(Role::User, "usable")]))
            .await
            .unwrap();
    }
    opted_reads_alike(&today, &store, "valid import after refusals").await;
    store.closed().await;
    assert!(today.registry.stop_and_seal().await);
}

#[tokio::test]
async fn conversation_key_sends_and_subsequent_sends_are_admitted_alike() {
    // These in-process loops have no account selection. This proves admission and read parity,
    // not routing; the real-daemon affinity differential compares the frozen auth selections.
    let script = vec![
        super::super::tests::text_step("a1", "answer"),
        super::super::tests::text_step("a2", "next"),
    ];
    let today = module(false, script.clone()).await;
    let store = module(true, script).await;
    for (run, key) in [("r1", Some("shared-conversation")), ("r2", None)] {
        let old = today.send(run, "input", key).await;
        let new = store.send(run, "input", key).await;
        assert_eq!(new, old);
        assert!(matches!(new, SendOutcome::Active { .. }));
        today.closed().await;
        store.closed().await;
    }
    assert!(
        store.head().exists(),
        "a keyed creating send starts in the store"
    );
    opted_reads_alike(&today, &store, "conversation-key sends").await;
}

#[tokio::test]
async fn queued_conversation_keys_and_key_reuse_are_served_alike() {
    let old_gate = Arc::new(Notify::new());
    let new_gate = Arc::new(Notify::new());
    let old_reached = Arc::new(Notify::new());
    let new_reached = Arc::new(Notify::new());
    let script = vec![
        super::super::tests::read_step("a1"),
        super::super::tests::text_step("a2", "first answer"),
        super::super::tests::text_step("a3", "queued answer"),
    ];
    let today = today_with(
        script.clone(),
        None,
        super::super::tests::Planes {
            tool_gate: Some((old_reached.clone(), old_gate.clone())),
            ..Default::default()
        },
    );
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
        let old = today
            .registry
            .route_submission_with_order(
                &today.key,
                RunId(run.into()),
                request.clone(),
                RunKind::FreshOrResume,
                key.clone(),
                false,
                broca_protocol::Delivery::Queue,
                None,
                Some(conversation.into()),
                None,
                None,
                None,
                false,
                None,
            )
            .await
            .unwrap();
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
        assert_eq!(new, old, "{id}: {conversation}");
        if conversation == "changed" {
            assert!(matches!(
                new,
                SendOutcome::SendIdReuse {
                    differing: "conversation_key"
                }
            ));
        }
    }
    old_reached.notified().await;
    new_reached.notified().await;
    for messages in [vec![], vec![text(Role::User, "import while running")]] {
        let imported = broca_core::ImportRequest {
            project_id: None,
            principal: None,
            run_id: RunId("seed".into()),
            session: today.key.clone(),
            messages,
            origin: broca_core::ImportOrigin {
                source_ref: "fixture:busy".into(),
            },
        };
        let old = today
            .registry
            .route_import(&today.key, imported.clone())
            .await
            .unwrap();
        let (reply, answer) = oneshot::channel();
        commands
            .send(LineageCmd::Import {
                request: Box::new(imported),
                reply,
            })
            .unwrap();
        let new = answer.await.unwrap();
        assert_eq!(new, old);
        assert!(matches!(new, crate::actor::ImportOutcome::Busy));
    }
    old_gate.notify_one();
    new_gate.notify_one();
    today.idle().await;
    super::super::tests::idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();
    assert_eq!(texts(&session), today.texts().await);
    assert!(today.registry.stop_and_seal().await);
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
    let today = module(false, reference_script()).await;
    let store = module(true, reference_script()).await;
    referencing(&today).await;
    referencing(&store).await;
    let old = reference_set(&today).await;
    let new = reference_set(&store).await;
    let old_page = listed(&today, &old.descriptor.set_id).await;
    let new_page = listed(&store, &new.descriptor.set_id).await;
    assert_eq!(
        comparable_refs(new_page.clone()),
        comparable_refs(old_page.clone())
    );
    assert_eq!(new_page["records"][0]["state"], "resolved");
    assert_eq!(new_page["records"][1]["reason"], "not_a_tool_output");
    assert_eq!(new_page["records"][2]["reason"], "unknown_tag");
    let old_id = old_page["records"][0]["ref_id"].as_str().unwrap();
    let new_id = new_page["records"][0]["ref_id"].as_str().unwrap();
    for (offset, length) in [(0, 131_072), (2, 3), (0, 0), (13, 1)] {
        assert_eq!(
            read_ref(&store, new_id, offset, length).await,
            read_ref(&today, old_id, offset, length).await
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
    let request =
        serde_json::from_value(serde_json::json!({"ref_id":old_id,"offset":0,"length":1})).unwrap();
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

#[tokio::test]
async fn an_output_reference_handed_out_before_conversion_still_resolves_after_reload() {
    let module = module(false, reference_script()).await;
    std::fs::create_dir_all(&module.dir).unwrap();
    let history =
        Arc::new(broca_store::SqliteStore::open(module.dir.join("run-index.db")).unwrap());
    module.wal.set_served_history(history);
    referencing(&module).await;
    let set = reference_set(&module).await;
    let before = listed(&module, &set.descriptor.set_id).await;
    let id = before["records"][0]["ref_id"].as_str().unwrap();
    let content = read_ref(&module, id, 0, 131_072).await;
    let records = module.wal.read_records(&module.key).unwrap();
    let page = module
        .registry
        .read(crate::serve::SessionReadRequest::new(
            module.key.clone(),
            None,
            Some(100),
        ))
        .await
        .unwrap();
    assert!(
        page.history_version > 1,
        "the source's served-history row was allocated"
    );
    let root = module.dir.join(broca_wal::SESSION_STORAGE_DIR);
    // The test publishes straight into its private scratch root; production publishes the staged
    // head only after verifying the copy and checking that the WAL backend no longer owns it.
    let staged = broca_session_convert::stage::stage(broca_session_convert::stage::Stage {
        source_history_version: page.history_version,
        records: &records,
        lineage: Some(&page.incarnation),
        session: &module.key,
        deployment: &broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
        store_root: &root,
        staging: &root,
        media: None,
        moved: &broca_session_store::record::Moved {
            move_id: "fixture-move".into(),
            source_activity_ms: 1,
            source_lineage: None,
        },
    })
    .await
    .unwrap();
    let runtime = module.registry.store_runtime().unwrap();
    let mut parts = runtime
        .parts_for(
            broca_wal::StorageClass::Ordinary,
            broca_session_store::keys::Cipher::XChaCha20Poly1305,
        )
        .clone();
    use broca_session_loop::parts::Parts as _;
    for part in staged
        .converted
        .records
        .iter()
        .flat_map(broca_session_convert::measure::named_parts)
    {
        let value = serde_json::from_slice(&staged.converted.parts[&part.digest]).unwrap();
        assert_eq!(parts.put(part.kind, &value).unwrap(), *part);
        parts.mark_stored(std::slice::from_ref(part));
    }
    let loaded = module.load().await;
    assert_eq!(
        loaded.operations.source_lineage.as_deref(),
        Some(page.incarnation.as_str())
    );
    assert_ne!(
        broca_session_store::codec::hex(&loaded.head().incarnation.0),
        page.incarnation
    );
    let after = module
        .registry
        .read(crate::serve::SessionReadRequest::new(
            module.key.clone(),
            None,
            Some(100),
        ))
        .await
        .unwrap();
    // The moved session serves the source's version raised by the new storage's exposure epoch:
    // it serves replies cleaned that today's storage served with their tags.
    assert_eq!(
        after.history_version,
        crate::store_exposure::served_history_version(page.history_version)
    );
    assert_eq!(listed(&module, &set.descriptor.set_id).await, before);
    assert_eq!(read_ref(&module, id, 0, 131_072).await, content);
}
