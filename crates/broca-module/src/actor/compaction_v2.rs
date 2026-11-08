use broca_core::{compaction_v2 as v2, WalStore};
use broca_diagnostics::{spawn_blocking, JobKind};
use serde_json::value::{to_raw_value, RawValue};
use v2::protocol::{
    read::{Detail, ReadRequest, Select},
    view::{AppliedViewRequest, ApplyViewAnswer},
    wire::Refusal,
};

fn internal(message: impl Into<String>) -> Refusal {
    Refusal::new("internal_error", message, None)
}

impl<W: WalStore + 'static> super::LineageActor<W> {
    /// Every compaction entry of the session, as `session.entries` serves them.
    async fn all_compaction_entries(
        &self,
        writer: &broca_core::WalHandle,
        store: Option<std::sync::Arc<dyn broca_core::SessionStore>>,
    ) -> Result<Vec<v2::protocol::read::Entry>, String> {
        let mut entries = Vec::new();
        let mut after = None;
        loop {
            let request = ReadRequest::new(
                Select::After {
                    after_ordinal: after,
                    limit: Some(1000),
                },
                Detail::Full,
            );
            let page = v2::read::entries(
                self.wal_store.as_ref(),
                store.clone(),
                &self.key,
                writer,
                &request,
                None,
            )
            .await?;
            entries.extend(page.entries);
            match page.next_after {
                Some(next) => after = Some(next),
                None => return Ok(entries),
            }
        }
    }

    /// The numbers the session's next request shows, for a template's `part_tag` slots. It
    /// applies to a mode-3 log whose plan names a compaction provider and that has a frozen run
    /// configuration. `None` on any other session, whose numbers come from its replayed
    /// transcript.
    pub(super) async fn next_request_numbers(
        &mut self,
    ) -> Result<Option<std::collections::HashMap<(String, String), u64>>, String> {
        let checkpoint = self.load_checkpoint_at_head().await?;
        let mode3 = match checkpoint.head.as_ref().and_then(|head| head.mode3) {
            Some(mode3) => mode3,
            None => broca_core::mode3_log_from_wal(self.wal_store.as_ref(), &self.key)
                .map_err(|error| error.to_string())?,
        };
        let manifest = checkpoint.ownership.as_ref().and_then(|ownership| {
            ownership
                .latest_manifest
                .as_deref()
                .or_else(|| ownership.first_episode.as_ref()?.manifest.as_deref())
        });
        let has_item = manifest
            .map(broca_core::compaction::item)
            .transpose()?
            .flatten()
            .is_some();
        if !mode3 || !has_item {
            return Ok(None);
        }
        let Some(config) = checkpoint
            .last_episode
            .as_ref()
            .map(|episode| &episode.config)
        else {
            return Ok(None);
        };
        if !self.compaction_session.is_initialized() {
            let wal = self.wal_store.clone();
            let key = self.key.clone();
            let frames = spawn_blocking(JobKind::WalRead, move || wal.read_records(&key))
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            self.compaction_session
                .initialize(&self.key, &frames)
                .await?;
        }
        let writer = self
            .writer
            .as_ref()
            .ok_or("compaction session writer is closed")?
            .clone();
        let store = (!self.retention_lineage)
            .then(|| self.materialized_store.clone())
            .flatten();
        let entries = self.all_compaction_entries(&writer, store).await?;
        let conditions = v2::host::conditions(config, &entries, false, None)?;
        let state = self.compaction_session.state.lock().await;
        v2::shown::next_request(&state, &entries, &conditions).map(Some)
    }

    pub(super) async fn compaction_v2_call(
        &mut self,
        caller: Option<&str>,
        method: &str,
        params: &RawValue,
    ) -> Result<Box<RawValue>, Refusal> {
        let checkpoint = self.load_checkpoint_at_head().await.map_err(internal)?;
        let mode3 = match checkpoint.head.as_ref().and_then(|head| head.mode3) {
            Some(mode3) => mode3,
            None => broca_core::mode3_log_from_wal(self.wal_store.as_ref(), &self.key)
                .map_err(|error| internal(error.to_string()))?,
        };
        let manifest = checkpoint.ownership.as_ref().and_then(|ownership| {
            ownership
                .latest_manifest
                .as_deref()
                .or_else(|| ownership.first_episode.as_ref()?.manifest.as_deref())
        });
        let item = manifest
            .map(broca_core::compaction::item)
            .transpose()
            .map_err(internal)?
            .flatten();
        v2::authority::check_bind(mode3, item.as_ref().map(|item| item.0.as_str()), caller)?;
        if !self.compaction_session.is_initialized() {
            let wal = self.wal_store.clone();
            let key = self.key.clone();
            let frames = spawn_blocking(JobKind::WalRead, move || wal.read_records(&key))
                .await
                .map_err(|error| internal(error.to_string()))?
                .map_err(|error| internal(error.to_string()))?;
            self.compaction_session
                .initialize(&self.key, &frames)
                .await
                .map_err(internal)?;
        }
        let writer = self
            .writer
            .as_ref()
            .ok_or_else(|| internal("compaction session writer is closed"))?
            .clone();
        let store = (!self.retention_lineage)
            .then(|| self.materialized_store.clone())
            .flatten();
        match method {
            // The wire bind calls this before acknowledging the provider's route.
            "bind" => {
                if !self.compaction_session.claim_serving_start() {
                    return to_raw_value(&serde_json::json!({}))
                        .map_err(|error| internal(error.to_string()));
                }
                let changes = v2::session::BoundChanges {
                    session: self.compaction_session.clone(),
                    writer: writer.clone(),
                    wal: self.wal_store.clone(),
                    bind: self.key.clone(),
                    scope: checkpoint
                        .ownership
                        .as_ref()
                        .and_then(|ownership| ownership.scope.clone()),
                    plane: self.executor.compaction_plane(),
                };
                let job = self.actor_jobs.track(writer);
                tokio::spawn(async move {
                    use v2::session::Changes;
                    let _job = job;
                    if let Err(error) = changes.changed().await {
                        tracing::warn!(%error, "compaction serving Setup failed");
                    }
                });
                to_raw_value(&serde_json::json!({})).map_err(|error| internal(error.to_string()))
            }
            "session.entries" => {
                let request: ReadRequest = serde_json::from_str(params.get())
                    .map_err(|error| Refusal::invalid_params("", error.to_string()))?;
                if matches!(request.select, Select::After { limit: Some(0), .. }) {
                    return Err(Refusal::invalid_params(
                        "select.limit",
                        "compaction entries limit must be at least 1",
                    ));
                }
                let applied = self
                    .compaction_session
                    .state
                    .lock()
                    .await
                    .applied_identity();
                let page = v2::read::entries(
                    self.wal_store.as_ref(),
                    store,
                    &self.key,
                    &writer,
                    &request,
                    applied,
                )
                .await
                .map_err(internal)?;
                to_raw_value(&page).map_err(|error| internal(error.to_string()))
            }
            "session.applied_view" => {
                let _: AppliedViewRequest = serde_json::from_str(params.get())
                    .map_err(|error| Refusal::invalid_params("", error.to_string()))?;
                let state = self.compaction_session.state.lock().await;
                let answer = match state.applied_received() {
                    Some(view) => serde_json::json!({"view":view}),
                    None => serde_json::json!({}),
                };
                to_raw_value(&answer).map_err(|error| internal(error.to_string()))
            }
            "session.apply_view" => {
                #[derive(serde::Deserialize)]
                struct RawApply {
                    view: Box<RawValue>,
                }
                let request: RawApply = serde_json::from_str(params.get())
                    .map_err(|error| Refusal::invalid_params("", error.to_string()))?;
                let received = v2::received::ReceivedView::parse(&request.view)
                    .map_err(|error| Refusal::invalid_params("view", error.to_string()))?;
                let config = &checkpoint
                    .last_episode
                    .as_ref()
                    .ok_or_else(|| internal("compaction session has no frozen configuration"))?
                    .config;
                let entries = self
                    .all_compaction_entries(&writer, store)
                    .await
                    .map_err(internal)?;
                let conditions =
                    v2::host::conditions(config, &entries, false, None).map_err(internal)?;
                let answer = match self
                    .compaction_session
                    .state
                    .lock()
                    .await
                    .accept(
                        &v2::journal::WalJournal {
                            writer: &writer,
                            session: &self.key,
                        },
                        &entries,
                        received,
                        &conditions.rules,
                        conditions.accepts_images == Some(true),
                    )
                    .await
                    .map_err(internal)?
                {
                    Ok(()) => ApplyViewAnswer::Accepted,
                    Err(refusal) => ApplyViewAnswer::Refused { refusal },
                };
                self.discard_stale_admission_view();
                to_raw_value(&answer).map_err(|error| internal(error.to_string()))
            }
            _ => Err(Refusal::unknown_method(method)),
        }
    }
}
