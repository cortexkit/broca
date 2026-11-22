//! How a store actor is put together before it serves: each `with_*` method hands it one
//! collaborator or setting and returns the actor.

use super::*;

impl<B: SessionBackend + Send + Sync + 'static> StoreActor<B> {
    /// Stages an import in `staging` and publishes it over this session from there.
    pub fn with_import_staging(
        mut self,
        staging: Arc<dyn crate::store_import_stage::ImportStaging<B>>,
    ) -> Self {
        self.import_staging = Some(staging);
        self
    }

    /// Appends an import at most `records` records at a time, and stops it at `cut` as a crash
    /// would.
    #[cfg(test)]
    pub(crate) fn with_import_cut(
        mut self,
        records: usize,
        cut: Option<crate::store_import_stage::ImportCut>,
    ) -> Self {
        self.import_batch = records;
        self.import_cut = cut;
        self
    }

    pub(crate) fn with_class_placement(
        mut self,
        class: broca_records::session_storage::StorageClass,
        backup_classes: Vec<String>,
    ) -> Self {
        self.class_placement = Some((class, backup_classes));
        self
    }

    pub(crate) fn with_recovery_index(mut self, root: std::path::PathBuf) -> Self {
        self.recovery_index = Some(root);
        self
    }

    /// Reads the time the session's deletions come due by from `now_ms`, in place of the wall
    /// clock.
    #[cfg(test)]
    pub(crate) fn with_clock(mut self, now_ms: Arc<dyn Fn() -> u64 + Send + Sync>) -> Self {
        self.now_ms = now_ms;
        self
    }

    /// Marks the session as one this actor created. If the actor closes with nothing appended
    /// since, as when the send creating the session was refused, it deletes the session under
    /// its lease, so the address reads as having no session again.
    pub fn created_here(mut self) -> Self {
        self.created = Some(self.session.next_seq());
        self
    }

    /// The actor reads a send's attachments from `uploads`.
    pub fn with_uploads(mut self, uploads: broca_core::media_uploads::Uploads) -> Self {
        self.uploads = Some(uploads);
        self
    }

    /// The actor serves `session.subscribe`, catching subscribers up over backends `readers`
    /// makes: handles on the session's storage that only read.
    pub fn with_readers(mut self, readers: Arc<dyn Fn() -> B + Send + Sync>) -> Self {
        self.readers = Some(Arc::clone(&readers));
        self.media = self.fresh_media_index();
        self.fanout = Some(crate::store_fanout::StoreFanout::new(
            &mut self.session,
            readers,
            crate::store_awaits::Labels(Some(self.executor.clone())),
        ));
        self
    }

    /// Holds each build of the session's image index until `gate` is notified, and has the open
    /// build the index from the session's records, as one finding no saved index does, so a test
    /// can act while a run waits for its media.
    #[cfg(test)]
    pub(crate) fn with_media_build_gate(mut self, gate: Arc<tokio::sync::Notify>) -> Self {
        self.media_build_gate = Some(gate);
        self
    }

    /// [`StoreActor::with_readers`], with each seeding of the live projection held until `gate`
    /// is notified.
    #[cfg(test)]
    pub(crate) fn with_readers_held(
        mut self,
        readers: Arc<dyn Fn() -> B + Send + Sync>,
        gate: Arc<tokio::sync::Notify>,
    ) -> Self {
        self.readers = Some(Arc::clone(&readers));
        self.media = self.fresh_media_index();
        self.fanout = Some(crate::store_fanout::StoreFanout::held(
            &mut self.session,
            readers,
            gate,
            crate::store_awaits::Labels(Some(self.executor.clone())),
        ));
        self
    }

    /// Hands the run events of the session's durable records to `sink` from now on (see
    /// [`crate::store_run_events`]).
    pub fn with_run_events(mut self, sink: Arc<dyn crate::store_run_events::RunEventSink>) -> Self {
        crate::store_run_events::observe(&mut self.session, &self.key, sink);
        self
    }

    /// Reports how the session's runs end or pause to diagnostics from now on (see
    /// [`crate::run_outcomes`]).
    pub fn with_run_outcomes(mut self) -> Self {
        crate::run_outcomes::observe(&mut self.session);
        self
    }

    /// Hands the send state notices of the session's durable records to `sink` from now on
    /// (see [`crate::store_send_states`]).
    pub fn with_send_states(
        mut self,
        sink: Arc<dyn crate::store_send_states::SendStateSink>,
    ) -> Self {
        crate::store_send_states::observe(&mut self.session, &self.key, sink);
        self
    }

    /// The actor serves finished runs' facts from `settled`, and keeps the ones it reads there,
    /// as the module's reads of a session no actor holds do.
    pub fn with_settled(mut self, settled: Arc<crate::store_status::SettledRuns>) -> Self {
        self.settled = Some(settled);
        self
    }

    /// The actor writes the spend facts of each run that ends to `billing`.
    pub fn with_billing(mut self, billing: crate::store_billing::StoreBilling) -> Self {
        self.billing = Some(billing);
        self
    }

    /// The actor stores setup parts in `storage` before each append that could name them.
    pub fn with_storage(mut self, storage: broca_session_loop::durable::SetupParts<B>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Install a content-free observer independent of transcript subscription demand.
    pub fn with_step_events(mut self, events: &crate::step_events::StepEvents) -> Self {
        let message_ids = broca_session_loop::entries::MessageIds::default();
        self.step_message_ids = Some(message_ids.clone());
        let mut observer = events
            .store_observer(
                self.key.clone(),
                self.session.head().incarnation.0,
                self.session.next_seq(),
                self.parts.clone(),
                self.step_event_warm_stops.clone(),
            )
            .with_message_ids(message_ids);
        if let Some(run) = self.session.operations().run.as_ref() {
            if let Some(start) = run.start.as_ref() {
                observer.seed_run(
                    start,
                    run.refrozen_render.as_ref(),
                    run.refrozen_auth.as_ref(),
                );
            }
        }
        self.session
            .add_positioned_append_observer(Box::new(move |appended, position| {
                observer.appended(appended, position)
            }));
        self
    }
}
