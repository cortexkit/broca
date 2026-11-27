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
        if matches!(request, crate::store_reads::StoreReadRequest::Head) {
            // The actor has the folded head facts already. Missing pieces come from the newest
            // page, the latest run's records, or the cursor's log. Resolve those against this
            // session, rather than making the caller reload and fold another copy of its view.
            let reply = Box::pin(crate::store_reads::execute_head(
                &self.session,
                &context,
                Self::head_cursor(&self.session, self.fanout.as_ref(), context.stream_head),
            ))
            .await;
            return crate::store_reads::StoreReadOutcome::Served(Box::new(reply));
        }
        // Keep the generic read future off the actor stack while the head branch walks its
        // cursor's log; it carries the state of all store read variants.
        let mut outcome = Box::pin(crate::store_reads::answer_in_memory(
            &mut self.session,
            &context,
            request,
            Some(&self.compaction_session),
        ))
        .await;
        if let crate::store_reads::StoreReadOutcome::Stored { compaction, .. } = &mut outcome {
            *compaction = Some(Arc::clone(&self.compaction_session));
        }
        outcome
    }

    /// Keeps the session's head where `session.head` of a closed session looks first (see
    /// `store_head::KeptHeads`), as the actor closes idle. A head built after a close loads the
    /// session's whole view, and almost every closed head read follows a write the head was not
    /// yet built at (125 of 126 such loads in one 16-minute sample). The head is built from what
    /// the actor holds, the way a read of the closed session builds it: no live actor or live
    /// step. A head that needs records only reads the missing history before releasing its slot.
    pub(super) async fn keep_closing_head(&mut self, registry: &crate::registry::RegistryHandle) {
        let Some(store) = registry.store_runtime() else {
            return;
        };
        if self.session.unsynced() {
            store.heads().count_not_kept("unsynced");
            return;
        }
        // A kept head skips the expiry check every other read of a closed session makes, so a
        // session in the ephemeral root, which can expire while it stays put, keeps its head only
        // with the deadline it has here, which `StoreRuntime::closed_position` checks; one whose
        // deadline this actor doesn't already know, or that has come, keeps nothing.
        let class = match store.class_of(&self.key) {
            Ok(Some(class)) => class,
            _ => {
                store.heads().count_not_kept("failed");
                return;
            }
        };
        let addr = broca_core::session_addr(&self.key);
        let parts = store.parts_for(class, self.session.head().cipher());
        store.authorization().learn(
            &self.key,
            class,
            &self.session,
            parts,
            self.admission.known(),
        );
        store.keep_parked_status(&self.key, class, &self.session);
        if class == broca_records::session_storage::StorageClass::Ephemeral {
            if let Some(retention) = registry.retention_service() {
                let Some(deadline) = self.retention_timer.known(&self.session) else {
                    store.heads().count_not_kept("ephemeral");
                    return;
                };
                if deadline.is_some_and(|deadline| deadline <= retention.clock.now_ms()) {
                    store.heads().count_not_kept("ephemeral");
                    return;
                }
                retention.keep_deadline(&addr, self.session.position(), deadline);
            }
        }
        let stream_head = self.fanout.as_ref().and_then(|fanout| fanout.head());
        store
            .parked_pages()
            .keep(&addr, class, &self.session, stream_head.flatten());
        let context = crate::store_reads::ReadContext {
            labels: crate::store_awaits::Labels(Some(self.executor.clone())),
            token_counts: None,
            parts,
            held: false,
            open: false,
            settled: Some((store.settled_runs(), addr.as_str())),
            session_name: &self.key.session,
            live_step: None,
            // The cursor the actor's subscribers stand at, which a read of the same records
            // projects again.
            stream_head,
            entry_speaks_for: crate::store_entries::EntryAttribution::Known(false),
            class: Some(class),
        };
        let reply = Box::pin(crate::store_reads::execute_head(
            &self.session,
            &context,
            Self::head_cursor(&self.session, self.fanout.as_ref(), stream_head),
        ))
        .await;
        match reply {
            crate::store_reads::StoreReadReply::Head(Ok(head)) => {
                store
                    .heads()
                    .keep(addr, self.session.position(), &head, "close");
            }
            _ => store.heads().count_not_kept("failed"),
        }
    }

    async fn head_cursor(
        session: &Session<B>,
        fanout: Option<&crate::store_fanout::StoreFanout<B>>,
        known: Option<Option<broca_core::Cursor>>,
    ) -> Result<Option<broca_core::Cursor>, crate::serve::ServeError> {
        match (known, fanout) {
            (None, Some(fanout)) => fanout.cursor_for_read(session).await,
            (known, _) => Box::pin(crate::store_read::stream_head(session, known)).await,
        }
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
