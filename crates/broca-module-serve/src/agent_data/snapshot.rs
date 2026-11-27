//! The answers a moved session must give the same on both machines: its pages, compaction
//! entries and head, and the bytes of the next request it would send. The receiver computes
//! them from the bytes it staged and compares them with the digests the sender computed from
//! its own copy, so a move that would change what the session answers is refused before
//! anything is activated.

use std::collections::BTreeMap;

use broca_module::serve::SessionReadRequest;
use broca_module::store_read::StoredView;
use broca_protocol::BindIdentity;
use broca_session_loop::parts::MemoryParts;
use broca_session_store::backend::{SessionAddr, SessionBackend};
use broca_session_store::keys::DeploymentKey;
use broca_session_store::record::{Input, PartKind, Placement, Record, StepStart};
use broca_session_store::session::moves::Export;
use broca_session_store::session::Session;
use broca_session_store::setup::SetupReader;
use serde_json::Value;
use sha2::{Digest as _, Sha256};

/// Digest keys. Both machines name each answer the same, so these strings are part of the move
/// protocol between builds: changing one makes every move refuse until both sides carry it.
pub const READ: &str = "read";
pub const ENTRIES: &str = "entries";
pub const HEAD: &str = "head";
pub const NEXT_REQUEST: &str = "next_request";

/// The run id the next-request check writes into the in-memory copy, so the checked request is
/// never confused with one the session sent.
const NEXT_RUN: &str = "store-move-next-request";

/// The read modes: whether each asks for display facts, tools and Rich UI.
const READ_MODES: [(&str, bool, bool, bool); 5] = [
    ("default", false, false, false),
    ("display_facts", true, false, false),
    ("tools", false, true, false),
    ("rich_ui", false, false, true),
    ("all", true, true, true),
];

/// How many messages a compared page asks for. Small, so a session's pages are many and their
/// boundaries are compared too.
const PAGE: u64 = 7;

fn sha256_hex(bytes: &[u8]) -> String {
    broca_session_store::codec::hex(&Sha256::digest(bytes))
}

/// The setup parts `record` names.
fn named_parts(record: &Record) -> &[broca_session_store::record::PartRef] {
    match record {
        Record::RunStart(start) => &start.setup,
        Record::SetupStub(stub) => &stub.parts,
        Record::SessionRefresh(refresh) => &refresh.setup,
        _ => &[],
    }
}

async fn read_digest<B: SessionBackend>(
    view: &StoredView<B>,
    parts: &MemoryParts,
    bind: &BindIdentity,
) -> Result<String, String> {
    let mut hash = Sha256::new();
    hash.update(b"[");
    let mut first = true;
    for (_, display_facts, tools, rich_ui) in READ_MODES {
        let mut from = 1;
        loop {
            let mut request = SessionReadRequest::new(bind.clone(), Some(from), Some(PAGE));
            request.include_display_facts = display_facts;
            request.include_tools = tools;
            request.include_rich_ui = rich_ui;
            let page = view
                .serve_page(parts, &request)
                .await
                .map_err(|e| format!("{e:?}"))?;
            // The move protocol hashes an array of JSON values, not the page structs: retain
            // the value's member order and the array delimiters, but only hold one page at a time.
            if !first {
                hash.update(b",");
            }
            first = false;
            let value = serde_json::to_value(&page).map_err(|e| e.to_string())?;
            hash.update(serde_json::to_vec(&value).map_err(|e| e.to_string())?);
            match page.next_from_ordinal {
                Some(next) if next > from => from = next,
                _ => break,
            }
        }
    }
    hash.update(b"]");
    Ok(broca_session_store::codec::hex(&hash.finalize()))
}

/// The digests of what `export` answers, read by the same store readers and renderer that serve
/// the session. The scratch writer lives only in memory, so checking the next send never changes
/// a file.
pub async fn store_snapshot(
    export: &Export,
    deployment: &DeploymentKey,
) -> Result<BTreeMap<String, String>, String> {
    let triple = &export.header.triple;
    let bind = BindIdentity {
        project_root: triple.project_root.clone().into(),
        harness: triple.harness.clone(),
        session: triple.session.clone(),
    };
    let addr = SessionAddr(broca_core::session_addr(&bind));
    let memory = export
        .memory(&addr, deployment)
        .await
        .map_err(|e| format!("{e:?}"))?;
    let view = Session::view(&memory, addr.clone(), triple, deployment)
        .await
        .map_err(|e| format!("{e:?}"))?;
    let records = view.records_from(0).await.map_err(|e| format!("{e:?}"))?;
    let setup = SetupReader::new(&memory, deployment, export.header.cipher);
    let mut parts = MemoryParts {
        parts: BTreeMap::new(),
    };
    for (_, record) in &records {
        for part in named_parts(record) {
            parts.parts.insert(
                part.digest,
                setup
                    .get(part)
                    .await
                    .map_err(|e| format!("{e:?}"))?
                    .to_vec(),
            );
        }
    }
    let position = view.position();
    let view = StoredView::new(Box::new(view), position).await;
    let mut digests = BTreeMap::new();
    digests.insert(READ.into(), read_digest(&view, &parts, &bind).await?);
    let entries = broca_module::store_entries::all_entries(&view)
        .await
        .map_err(|e| format!("{e:?}"))?;
    digests.insert(
        ENTRIES.into(),
        sha256_hex(&serde_json::to_vec(&entries).unwrap()),
    );
    let built = broca_module::store_head::build(&view, &parts, true)
        .await
        .map_err(|e| format!("{e:?}"))?;
    digests.insert(
        HEAD.into(),
        sha256_hex(&serde_json::to_vec(&built.reply).unwrap()),
    );
    let start = records.iter().rev().find_map(|(_, record)| match record {
        Record::RunStart(start) => Some((**start).clone()),
        _ => None,
    });
    drop(view);
    let request = if let Some(mut start) = start {
        let at = records
            .iter()
            .filter_map(|(_, record)| record.activity_ms())
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        let config = start
            .setup
            .iter()
            .find(|part| part.kind == PartKind::RenderConfig)
            .and_then(|part| parts.parts.get(&part.digest));
        if let Some(config) = config {
            let render_tags = start
                .run_choices
                .as_ref()
                .and_then(|choices| choices.get("config"))
                .and_then(|config| config.get("render_tags"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let part: Value =
                serde_json::from_slice(config).map_err(|e| format!("render config: {e}"))?;
            let render_config = std::sync::Arc::unwrap_or_clone(
                start
                    .render_config(std::sync::Arc::new(part))
                    .map_err(|e| format!("render config: {e}"))?,
            );
            let frozen = serde_json::from_value(render_config.clone())
                .map_err(|e| format!("render config: {e}"))?;
            // The next message gets its `EntryFacts` record (its id, time and part numbers) as
            // the run loop writes one: on a session on the compaction contract a message's
            // compaction entry, and so the request, can't be built without it. `tags` are the
            // numbering rules those facts follow.
            let manifest = broca_session_store::fold::operations::ManifestRef::of_start(&start)
                .map(|reference| broca_session_loop::run::stored_manifest(&reference, &parts))
                .transpose()
                .map_err(|e| format!("the latest run's manifest: {e:?}"))?;
            let tags = broca_session_loop::entries::Tags::for_session(
                start.compaction_plan_version.is_some(),
                manifest.as_deref(),
                &render_config,
            );
            let mut writer = Session::load(&memory, addr.clone(), triple, deployment)
                .await
                .map_err(|e| format!("{e:?}"))?;
            // Compare the same synthetic request on both sides of the move. A real next run
            // may apply a pending manifest before it starts; the snapshot does not admit it.
            start.run_id = NEXT_RUN.into();
            start.submission_id = None;
            start.send_id = None;
            start.request_identity_digest = None;
            start.ts_ms = Some(at);
            start.written_at_ms = Some(at);
            let next = vec![
                Record::RunStart(Box::new(start)),
                Record::Input(Input {
                    run_id: NEXT_RUN.into(),
                    placement: Placement::RunInput,
                    message: std::sync::Arc::new(broca_protocol::content::Message::new(
                        broca_protocol::content::Role::User,
                        vec![broca_protocol::content::ContentBlock::Text {
                            text: "The next message after the move.".into(),
                        }],
                    )),
                    written_at_ms: Some(at),
                    submission_id: None,
                    notice: None,
                    tags_v1: None,
                    receipt_seq: None,
                    pruned: vec![],
                    media: vec![],
                }),
                Record::StepStart(StepStart {
                    run_id: NEXT_RUN.into(),
                    step_id: 1,
                    mid: None,
                }),
            ];
            use broca_session_loop::Writer as _;
            let prefix = writer.message_id_prefix();
            let calls = Default::default();
            let next = broca_session_loop::entries::with_facts(
                next,
                &broca_session_loop::entries::Placing {
                    conversation: writer.conversation(),
                    first_seq: writer.next_seq(),
                    prefix: &prefix,
                    session: &writer.session_id(),
                    tags,
                    view: None,
                    operations: writer.operations(),
                    calls: &calls,
                    rich_ui: None,
                    runner: None,
                },
            )?;
            writer
                .append(&next, true)
                .await
                .map_err(|e| format!("{e:?}"))?;
            let reader = export
                .memory(&addr, deployment)
                .await
                .map_err(|e| format!("{e:?}"))?;
            broca_module::store_media::offline_render(
                &writer,
                reader,
                &parts,
                &bind,
                1,
                &frozen,
                render_tags,
                None,
            )
            .await?
        } else {
            return Err(
                "the latest run has no frozen render config for the next-request move check".into(),
            );
        }
    } else {
        Vec::new()
    };
    digests.insert(NEXT_REQUEST.into(), sha256_hex(&request));
    Ok(digests)
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_session_store::backend::MemoryBackend;
    use broca_session_store::codec::Digest;
    use broca_session_store::head::SessionTriple;

    fn unhex(value: &Value) -> Vec<u8> {
        let hex = value.as_str().unwrap();
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn snapshot_does_not_redeliver_an_unanswered_note_without_admission() {
        use broca_session_store::{
            backend::LocalBackend,
            keys::Cipher,
            record::{PlanNote, PlanNoteKind, PlanNoteTool, PlanNotes},
            setup::SetupStore,
        };
        let fixture: Value = serde_json::from_str(include_str!(
            "../../tests/agent_data/fixtures/finished.json"
        ))
        .unwrap();
        let parts: BTreeMap<Digest, Vec<u8>> = fixture["parts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|part| (Digest(unhex(&part[0]).try_into().unwrap()), unhex(&part[1])))
            .collect();
        let mut records: Vec<_> = fixture["records"]
            .as_array()
            .unwrap()
            .iter()
            .map(|record| {
                Record::decode(
                    record["kind"].as_u64().unwrap() as u32,
                    &unhex(&record["body"]),
                )
                .unwrap()
            })
            .collect();
        let mut end = records
            .iter()
            .find_map(|record| match record {
                Record::RunEnd(end) => Some(end.clone()),
                _ => None,
            })
            .unwrap();
        let start = records
            .iter()
            .find(|record| matches!(record, Record::RunStart(_)))
            .cloned()
            .unwrap();
        records = vec![start];
        let bind = BindIdentity::new("/project", "runner", "unanswered-snapshot");
        let triple = SessionTriple {
            project_root: "/project".into(),
            harness: bind.harness.clone(),
            session: bind.session.clone(),
        };
        let addr = SessionAddr(broca_core::session_addr(&bind));
        let deployment = DeploymentKey::new(7, &[4; 32]);
        let dir = broca_test_support::scratch_root()
            .join(format!("unanswered-snapshot-{}", std::process::id()));
        let store = LocalBackend::new(&dir);
        let key = deployment.setup_key_for(Cipher::XChaCha20Poly1305).unwrap();
        let setup = SetupStore::new(&store, &key);
        let config = serde_json::to_vec(&broca_provider::families::anthropic_frozen_config(
            "claude-opus-5-5",
        ))
        .unwrap();
        for record in &mut records {
            for part in named_parts(record) {
                setup.put(part.kind, &parts[&part.digest]).await.unwrap();
            }
            if let Record::RunStart(start) = record {
                end.run_id.clone_from(&start.run_id);
                start.model = broca_protocol::ModelHandle::new("anthropic", "claude-opus-5-5");
                start.render_identity = None;
                start.compaction_plan_version = None;
                start
                    .setup
                    .retain(|part| part.kind != PartKind::RenderConfig);
                start
                    .setup
                    .push(setup.put(PartKind::RenderConfig, &config).await.unwrap().0);
            }
        }
        let mut session = Session::create(&store, addr.clone(), triple, &deployment, false)
            .await
            .unwrap();
        records.push(Record::Input(Input {
            run_id: end.run_id.clone(),
            placement: Placement::RunInput,
            message: std::sync::Arc::new(broca_protocol::Message::new(
                broca_protocol::Role::User,
                vec![broca_protocol::ContentBlock::Text {
                    text: "This turn never got a reply.".into(),
                }],
            )),
            written_at_ms: Some(1),
            submission_id: None,
            notice: None,
            tags_v1: None,
            receipt_seq: None,
            pruned: vec![],
            media: vec![],
        }));
        let tags = broca_session_loop::entries::Tags::for_session(
            false,
            None,
            &serde_json::from_slice(&config).unwrap(),
        );
        use broca_session_loop::Writer as _;
        let records = broca_session_loop::entries::with_facts(
            records,
            &broca_session_loop::entries::Placing {
                conversation: session.conversation(),
                first_seq: session.next_seq(),
                prefix: &session.message_id_prefix(),
                session: &session.session_id(),
                tags,
                view: None,
                operations: session.operations(),
                calls: &Default::default(),
                rich_ui: None,
                runner: None,
            },
        )
        .unwrap();
        session.append(&records, true).await.unwrap();
        assert_eq!(
            session.conversation.newest().unwrap().role,
            broca_protocol::Role::User
        );
        let (_, without_notes) = store.read_head(&addr).await.unwrap().unwrap();
        let before = Export::pinned(&store, &addr, without_notes, "durable", &deployment)
            .await
            .unwrap();
        let plain = store_snapshot(&before, &deployment).await.unwrap();
        session
            .append(
                &[
                    Record::PlanNotes(PlanNotes {
                        blocks: Some(true),
                        generation: 1,
                        anchor: session.conversation.placed,
                        notes: vec![PlanNote {
                            kind: PlanNoteKind::ToolAddition,
                            text: String::new(),
                            form: "anthropic_tool_addition".into(),
                            tool: Some(PlanNoteTool {
                                name: "find".into(),
                                description: "Find a place".into(),
                                input_schema: r#"{"type":"object"}"#.into(),
                            }),
                            tool_dispatch: None,
                        }],
                        refused_tools: vec![],
                        model_key: "anthropic:claude-opus-5-5".into(),
                    }),
                    Record::RunEnd(end),
                ],
                true,
            )
            .await
            .unwrap();
        drop(session);
        let (_, version) = store.read_head(&addr).await.unwrap().unwrap();
        let export = Export::pinned(&store, &addr, version, "durable", &deployment)
            .await
            .unwrap();
        let result = store_snapshot(&export, &deployment).await.unwrap();
        assert_eq!(
            result[NEXT_REQUEST], plain[NEXT_REQUEST],
            "the synthetic request cannot deliver a note at a new anchor"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn move_read_digest_matches_the_original_array_and_projects_once() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../tests/agent_data/fixtures/finished.json"
        ))
        .unwrap();
        let bind = BindIdentity::new("/project", "runner", "move-digest");
        let triple = SessionTriple {
            project_root: "/project".into(),
            harness: bind.harness.clone(),
            session: bind.session.clone(),
        };
        let addr = SessionAddr(broca_core::session_addr(&bind));
        let backend = MemoryBackend::default();
        let deployment = DeploymentKey::new(7, &[4; 32]);
        let parts = MemoryParts {
            parts: fixture["parts"]
                .as_array()
                .unwrap()
                .iter()
                .map(|part| (Digest(unhex(&part[0]).try_into().unwrap()), unhex(&part[1])))
                .collect(),
        };
        let records: Vec<_> = fixture["records"]
            .as_array()
            .unwrap()
            .iter()
            .map(|record| {
                Record::decode(
                    record["kind"].as_u64().unwrap().try_into().unwrap(),
                    &unhex(&record["body"]),
                )
                .unwrap()
            })
            .collect();
        let mut writer =
            Session::create(&backend, addr.clone(), triple.clone(), &deployment, false)
                .await
                .unwrap();
        writer.append(&records, true).await.unwrap();
        drop(writer);
        let view = Session::view(&backend, addr, &triple, &deployment)
            .await
            .unwrap();

        // Build the expected digest from the complete array. Reuse the view so each page has
        // the same session identifiers as it does in the streaming check.
        let mut pages = Vec::new();
        for (display, tools, rich_ui) in [
            (false, false, false),
            (true, false, false),
            (false, true, false),
            (false, false, true),
            (true, true, true),
        ] {
            let mut request = SessionReadRequest::new(bind.clone(), Some(1), Some(7));
            request.include_display_facts = display;
            request.include_tools = tools;
            request.include_rich_ui = rich_ui;
            let page = broca_module::store_read::serve_view(&view, &parts, &request)
                .await
                .unwrap();
            assert!(page.next_from_ordinal.is_none());
            pages.push(serde_json::to_value(page).unwrap());
        }
        let original = sha256_hex(&serde_json::to_vec(&pages).unwrap());
        let reads = backend.checkpoint_frame_reads();
        let position = view.position();
        let view = StoredView::new(Box::new(view), position).await;
        assert_eq!(read_digest(&view, &parts, &bind).await.unwrap(), original);
        assert_eq!(
            backend.checkpoint_frame_reads() - reads,
            1,
            "only the shared head projects the log"
        );
    }
}
