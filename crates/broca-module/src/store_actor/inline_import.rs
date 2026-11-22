//! Inline imports use the same validation and transcript normalization as the WAL importer,
//! then commit their provider-free episode as one store batch.

use super::*;
use crate::actor::ImportOutcome;
use broca_core::{ImportError, ImportRequest};
use broca_session_store::record::{Input, Placement, RunEnd, RunStart};

impl<B: SessionBackend + Send + Sync + 'static> StoreActor<B> {
    pub(super) async fn inline_import(&mut self, request: ImportRequest) -> (ImportOutcome, bool) {
        let result = self.import_inline(request).await;
        match result {
            Ok(outcome) => (outcome, false),
            Err(error) => (
                ImportOutcome::Error(ImportError::Wal(format!("{error:?}"))),
                self.running().is_none(),
            ),
        }
    }

    async fn import_inline(
        &mut self,
        request: ImportRequest,
    ) -> Result<ImportOutcome, SessionError> {
        let active = self.running().is_some();
        let previous = self.session.find_run_start(&request.run_id.0).await?;
        if active
            && previous
                .as_ref()
                .is_none_or(|start| start.seeded != Some(true))
        {
            return Ok(ImportOutcome::Busy);
        }
        let (messages, counts, digest, origin) = match broca_core::import::prepare_inline(
            request.messages,
            &request.origin.source_ref,
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                return Ok(ImportOutcome::Error(if active {
                    ImportError::ImportIdReuse
                } else {
                    error
                }))
            }
        };
        let mut outcome = broca_core::ImportOutcome {
            run_id: request.run_id.clone(),
            synthesized_tool_results: counts.synthesized_tool_results,
            dropped_orphan_tool_results: counts.dropped_orphan_tool_results,
            dropped_system_messages: counts.dropped_system_messages,
            already_imported: false,
        };
        if let Some(start) = previous {
            let config = start
                .run_choices
                .as_ref()
                .and_then(|choices| choices.get("config"));
            let recorded = config
                .and_then(|config| config.get("provider_options"))
                .and_then(|options| options.get("import_request_digest"))
                .and_then(serde_json::Value::as_str);
            if start.seeded == Some(true) && recorded == Some(digest.as_str()) {
                outcome.already_imported = true;
                return Ok(ImportOutcome::Imported(outcome));
            }
            // The WAL importer only treats another completed import under this run id as reuse.
            if start.seeded == Some(true) && recorded.is_some() {
                return Ok(ImportOutcome::Error(ImportError::ImportIdReuse));
            }
        }
        if active {
            return Ok(ImportOutcome::Busy);
        }
        if !self.empty() {
            return Ok(ImportOutcome::Error(ImportError::LineageNotEmpty));
        }
        let floor = self.session.operations().tag_high_water;
        let config = broca_core::import::import_marker_config(&digest, floor);
        let now = broca_core::clock::epoch_millis();
        let run_id = request.run_id.0;
        let start = RunStart {
            run_id: run_id.clone(),
            model: config.model.clone(),
            setup: Vec::new(),
            renderer_version: 0,
            submission_id: None,
            send_id: None,
            request_identity_digest: None,
            ts_ms: now,
            written_at_ms: None,
            session_class: None,
            retention_secs: None,
            title: None,
            context_limit: None,
            service_tier: None,
            route_decision_id: None,
            compaction_plan_version: Some(broca_core::compaction_v2::PLAN_VERSION),
            auth_selection: None,
            run_choices: Some(serde_json::json!({"config": config})),
            origin: Some(serde_json::to_value(origin).expect("an import origin serializes")),
            principal: request
                .principal
                .map(|principal| serde_json::to_value(principal).expect("a principal serializes")),
            scope: None,
            hooks: None,
            keep_warm: None,
            on_restart: None,
            seeded: Some(true),
            refusal_schedule: None,
            imported_ids: None,
            message_marks: None,
            named_speaker: None,
            kept_notice: None,
        };
        let mut records = vec![Record::RunStart(Box::new(start))];
        for message in messages {
            records.push(Record::Input(Input {
                run_id: run_id.clone(),
                placement: Placement::RunInput,
                message: std::sync::Arc::new(message),
                written_at_ms: None,
                submission_id: None,
                notice: None,
                tags_v1: None,
                receipt_seq: None,
                pruned: Vec::new(),
                media: Vec::new(),
            }));
        }
        records.push(Record::RunEnd(RunEnd {
            run_id,
            reason: broca_protocol::RunFinishReason::Completed,
            usage: Some(broca_protocol::Usage::default()),
            ts_ms: now,
            retries_used: 0,
            interrupt_reason: None,
            error: None,
        }));
        let prefix = self.session.message_id_prefix();
        let calls = Default::default();
        let mut records = broca_session_loop::entries::with_facts(
            records,
            &broca_session_loop::entries::Placing {
                conversation: self.session.conversation(),
                first_seq: self.session.next_seq(),
                prefix: &prefix,
                session: &self.session.session_id(),
                tags: broca_session_loop::entries::Tags {
                    entries: true,
                    ..Default::default()
                },
                view: None,
                operations: self.session.operations(),
                calls: &calls,
                rich_ui: None,
                runner: None,
            },
        )
        .map_err(SessionError::Fold)?;
        // Imported messages were supplied by the caller, not produced by the synthetic run.
        // Display attribution therefore names no producing run, including on assistant messages.
        for record in &mut records {
            if let Record::EntryFacts(facts) = record {
                std::sync::Arc::make_mut(facts).run_id = None;
            }
        }
        self.append_own_batch_with_media(&records, &[]).await?;
        Ok(ImportOutcome::Imported(outcome))
    }
}
