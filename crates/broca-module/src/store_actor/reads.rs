//! Reads a store actor answers from its own state: any read served from memory, and whether
//! the compaction entries it serves keep `speaks_for`.

use super::*;

impl<B: SessionBackend + Send + Sync + 'static> StoreActor<B> {
    /// Answers a read from memory, flushing pending records first for caller data, but never
    /// for internal authorization (see `store_reads::answer_in_memory`).
    pub(super) async fn store_read(
        &mut self,
        request: &crate::store_reads::StoreReadRequest,
    ) -> crate::store_reads::StoreReadOutcome {
        // Dependency admission may ask for immutable ownership through this same actor.
        // Keep authorization available while caller reads wait for the restart decision.
        if self.restart_admitting && !request.is_authorization() {
            return crate::store_reads::StoreReadOutcome::Refused(crate::serve::ServeError::Open(
                "restart_resume_pending: restart admission has not completed".into(),
                Some(broca_protocol::ErrorClass::Transient),
            ));
        }
        if !request.is_authorization()
            && self.run.is_none()
            && self.session.operations().run.as_ref().is_some_and(|run| {
                run.finished.is_none()
                    && recovery_mode(run).is_some()
                    && !(run.paused == Some(broca_protocol::PauseReason::Restart)
                        && run.cancel.is_none()
                        && !self.restart_resumes_ready()
                        && !self.interrupt_waits_on_pause())
            })
        {
            return crate::store_reads::StoreReadOutcome::Refused(crate::serve::ServeError::Open(
                "session_needs_recovery: the unfinished run could not be recovered before reading"
                    .into(),
                Some(broca_protocol::ErrorClass::Transient),
            ));
        }
        if !request.is_authorization() {
            // Flush before caller reads so their subscribe cursor includes the durable controls
            // projected from every returned record. Broca's own reads take no cursor, so they
            // leave the subscriber projection unseeded.
            if let Some(refused) = crate::store_reads::durable_for_read(&mut self.session).await {
                return refused;
            }
            if let Some(fanout) = self.fanout.as_mut().filter(|_| !request.is_internal()) {
                fanout.ensure_seeded(&self.session);
            }
        }
        let addr = broca_core::session_addr(&self.key);
        let live_step = self
            .run
            .as_ref()
            .map(|active| crate::store_reads::LiveStep {
                run_id: active.run_id.0.clone(),
                step: active.step.lock().unwrap().clone(),
            });
        let entry_speaks_for = self.entry_attribution(request).await;
        let context = crate::store_reads::ReadContext {
            labels: crate::store_awaits::Labels(Some(self.executor.clone())),
            token_counts: Some(&self.compaction_session.runtime.tokens),
            parts: &self.parts,
            held: true,
            open: true,
            settled: self
                .settled
                .as_deref()
                .map(|settled| (settled, addr.as_str())),
            session_name: &self.key.session,
            live_step: live_step.as_ref(),
            stream_head: self.fanout.as_ref().and_then(|fanout| fanout.head()),
            entry_speaks_for,
            class: self.class_placement.as_ref().map(|(class, _)| *class),
        };
        crate::store_reads::answer_in_memory(&mut self.session, &context, request).await
    }

    /// Whether the compaction entries `request` serves keep `speaks_for`: what the latest Setup
    /// this actor sent found in the provider's catalog entry. Before this actor has sent one, a
    /// `session.entries` read asks the catalog for the provider the session's plan names, and the
    /// answer is kept until the next Setup.
    pub(super) async fn entry_attribution(
        &self,
        request: &crate::store_reads::StoreReadRequest,
    ) -> crate::store_entries::EntryAttribution {
        use crate::store_entries::EntryAttribution;
        let runtime = &self.compaction_session.runtime;
        if let Some(declared) = runtime.entry_speaks_for() {
            return EntryAttribution::Known(declared);
        }
        let entries = matches!(request, crate::store_reads::StoreReadRequest::Compaction { method, .. }
            if method == "session.entries");
        let provider = entries
            .then(|| crate::store_entries::frozen_provider(&self.parts, &self.session).ok())
            .flatten()
            .flatten();
        let Some(provider) = provider else {
            return EntryAttribution::Known(false);
        };
        let declaration = EntryAttribution::Catalog(self.executor.compaction_plane())
            .declared(&provider)
            .await;
        let Some(declared) = declaration.value() else {
            return EntryAttribution::Unknown;
        };
        runtime.learn_entry_speaks_for(declared);
        EntryAttribution::Known(runtime.entry_speaks_for().unwrap_or(declared))
    }
}
