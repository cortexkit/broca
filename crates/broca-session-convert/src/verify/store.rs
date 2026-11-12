use super::*;
use broca_session_store::{
    record::{Input, PartKind, Placement, Record, StepStart},
    session::moves::Export,
};

/// Canonical answers from the same store readers and renderer used by the WAL move check.
/// The scratch writer lives only in memory, so checking the next send never changes a file.
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
    let key = deployment
        .setup_key_for(export.header.cipher)
        .ok_or("deployment key missing")?;
    let setup = SetupStore::new(&memory, &key);
    let mut parts = MemoryParts {
        parts: BTreeMap::new(),
    };
    for (_, record) in &records {
        for part in crate::measure::named_parts(record) {
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
    let mut digests = BTreeMap::new();
    let mut pages = Vec::new();
    for (_, display_facts, tools, rich_ui) in READ_MODES {
        let mut from = 1;
        loop {
            let mut request = SessionReadRequest::new(bind.clone(), Some(from), Some(PAGE));
            request.include_display_facts = display_facts;
            request.include_tools = tools;
            request.include_rich_ui = rich_ui;
            let page = broca_module::store_read::serve_view(&view, &parts, &request)
                .await
                .map_err(|e| format!("{e:?}"))?;
            pages.push(serde_json::to_value(&page).map_err(|e| e.to_string())?);
            match page.next_from_ordinal {
                Some(next) if next > from => from = next,
                _ => break,
            }
        }
    }
    digests.insert(
        READ.into(),
        sha256_hex(&serde_json::to_vec(&pages).unwrap()),
    );
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
            let frozen =
                serde_json::from_slice(config).map_err(|e| format!("render config: {e}"))?;
            let mut writer = Session::load(&memory, addr.clone(), triple, deployment)
                .await
                .map_err(|e| format!("{e:?}"))?;
            start.run_id = NEXT_RUN.into();
            start.submission_id = None;
            start.send_id = None;
            start.request_identity_digest = None;
            start.ts_ms = Some(at);
            start.written_at_ms = Some(at);
            writer
                .append(
                    &[
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
                    ],
                    true,
                )
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
