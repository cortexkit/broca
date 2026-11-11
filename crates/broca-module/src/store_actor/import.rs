//! The store actor's import commands: a head's file-source import into the empty session, and
//! installing the compaction provider's first view of the imported history.

use broca_session_store::backend::SessionBackend;
use broca_session_store::record::{Record, ViewApplied};
use broca_wire::head_import::InstallViewReply;

use super::StoreActor;
use crate::store_import::{
    delete_sources, newest_of_entry, payload_digest, read_source, reply_of, seed, ImportCommand,
    ImportRefusal, InstallViewCommand, SeedFacts, StoreCommand, StoreOutcome,
};

impl<B: SessionBackend + Send + Sync + 'static> StoreActor<B> {
    /// Serves a command only this storage serves. Returns the outcome, and whether the actor
    /// must go away: after a failed write the session is poisoned, and the next open reloads it.
    pub(super) async fn store_command(&mut self, command: StoreCommand) -> (StoreOutcome, bool) {
        if self.run.is_some() {
            return (
                StoreOutcome::Refused(ImportRefusal::new(
                    "session_busy",
                    "the session is running a run",
                )),
                false,
            );
        }
        match command {
            StoreCommand::Import(command) => self.import(*command).await,
            StoreCommand::InstallView(command) => (self.install_view(*command).await, false),
            // Served by the command loop, which applies a view while a run runs too.
            StoreCommand::ApplyView(_) => (
                StoreOutcome::Refused(ImportRefusal::new(
                    "internal_error",
                    "session.apply_view reached the import commands",
                )),
                false,
            ),
        }
    }

    /// Whether the session holds nothing but what creating and opening it wrote: no run, no
    /// message, nothing queued and no view. A crash between a session's creation and an
    /// import's batch leaves such a session, and the import's retry writes into it.
    pub(super) fn empty(&self) -> bool {
        let operations = &self.session.operations;
        let conversation = &self.session.conversation;
        operations.run.is_none()
            && operations.owner.episodes == 0
            && operations.queue.is_empty()
            && operations.view_accepted.is_none()
            && conversation.placed == 0
            && conversation.open_tool_turn().is_none()
    }

    fn incarnation(&self) -> String {
        broca_session_store::codec::hex(&self.session.head().incarnation.0)
    }

    /// The reply this session's import kept.
    async fn stored_reply(&self) -> StoreOutcome {
        match self.session.import_receipt().await {
            Ok(Some(receipt)) => self.reply_from_receipt(&receipt).await,
            Ok(None) => StoreOutcome::Refused(ImportRefusal::new(
                "session_exists",
                "the session exists and holds no import",
            )),
            Err(error) => StoreOutcome::Refused(ImportRefusal::new(
                "session_needs_recovery",
                format!("the session's import receipt could not be read: {error:?}"),
            )),
        }
    }

    /// Imported inputs and their entry facts are immutable. Read the receipt's last imported
    /// ordinal, never the current session head, so views and later owner runs cannot change the
    /// import's newest. Reprojection also supports receipts written before newest was served.
    async fn reply_from_receipt(
        &self,
        receipt: &broca_session_store::record::ImportReceipt,
    ) -> StoreOutcome {
        let expected = receipt
            .moves
            .iter()
            .find(|row| row.ordinal == Some(receipt.imported_messages));
        let entries = match self.session.entries(receipt.imported_messages, 1).await {
            Ok(entries) => entries,
            Err(error) => {
                return StoreOutcome::Refused(ImportRefusal::new(
                    "session_needs_recovery",
                    format!("the import's newest entry could not be read: {error:?}"),
                ))
            }
        };
        let Some(entry) = entries.first().filter(|entry| {
            entry.ordinal == receipt.imported_messages
                && entry
                    .state
                    .facts
                    .as_ref()
                    .is_some_and(|facts| expected.is_some_and(|row| row.mid == facts.mid))
        }) else {
            return StoreOutcome::Refused(ImportRefusal::new(
                "session_needs_recovery",
                "the import's newest entry is no longer retained under its recorded id",
            ));
        };
        let newest = match newest_of_entry(entry) {
            Ok(newest) => newest,
            Err(refusal) => return StoreOutcome::Refused(refusal),
        };
        match reply_of(
            receipt,
            self.session.operations.history_version,
            self.incarnation(),
            newest,
        ) {
            Ok(reply) => StoreOutcome::Imported(reply),
            Err(refusal) => StoreOutcome::Refused(refusal),
        }
    }

    async fn import(&mut self, command: ImportCommand) -> (StoreOutcome, bool) {
        let ImportCommand {
            params,
            principal,
            scope,
            roots,
            now_ms,
        } = command;
        let digest = payload_digest(&params);
        // The receipt survives source deletion and the first real owner run, whose start
        // replaces the seed as the session's admitting episode.
        if self.session.operations.imported_ids.is_some() {
            let receipt = match self.session.import_receipt().await {
                Ok(Some(receipt)) => receipt,
                other => {
                    return (
                        StoreOutcome::Refused(ImportRefusal::new(
                            "session_needs_recovery",
                            format!("the import receipt could not be read: {other:?}"),
                        )),
                        false,
                    )
                }
            };
            if receipt.import_id == params.import_id {
                if receipt.digest != digest {
                    return (
                        StoreOutcome::Refused(ImportRefusal::new(
                            "send_id_reuse",
                            "import_id names an import of another payload",
                        )),
                        false,
                    );
                }
                return (self.stored_reply().await, false);
            }
        }
        if !self.empty() {
            return (
                StoreOutcome::Refused(ImportRefusal::new(
                    "session_exists",
                    "an import creates its session, which already exists",
                )),
                false,
            );
        }
        // The import makes its principal the session's first owner, so it meets the scope
        // reservation as a first send does: only the scope whose stamped bind reserved the
        // session's address may import into it. An unreadable reservation refuses, transiently.
        let reservation = match self.scope_reservation().await {
            Ok(reservation) => reservation,
            Err(error) => return (StoreOutcome::Transient(error), false),
        };
        let reserved = broca_core::SessionOwnershipV1 {
            reservation,
            ..Default::default()
        };
        if let Err(refusal) = broca_core::scope_admission::check_reserved_scope(
            &reserved,
            Some(&principal),
            scope.as_ref(),
        ) {
            return (StoreOutcome::ScopeRefused(refusal), false);
        }
        // Only the session's would-be owner imports, on a route scoped to it: the import makes
        // that principal the session's owner.
        match &scope {
            Some(scope) if scope.owner == principal => {}
            Some(_) => {
                return (
                    StoreOutcome::Refused(ImportRefusal::new(
                        "owner_required",
                        "only the owner of the route's scope may import into its session",
                    )),
                    false,
                )
            }
            None => {
                return (
                    StoreOutcome::Refused(ImportRefusal::new(
                        "scope_required",
                        "an import arrives on a scoped route",
                    )),
                    false,
                )
            }
        }
        let read = {
            let params = params.clone();
            let roots = std::sync::Arc::clone(&roots);
            tokio::task::spawn_blocking(move || read_source(&params, &roots)).await
        };
        let (path, history) = match read {
            Ok(Ok(read)) => read,
            Ok(Err(refusal)) => return (StoreOutcome::Refused(refusal), false),
            Err(error) => {
                return (
                    StoreOutcome::Refused(ImportRefusal::new(
                        "import_source_unreadable",
                        format!("reading the source stopped: {error}"),
                    )),
                    false,
                )
            }
        };
        let prefix = self.session.message_id_prefix();
        let facts = SeedFacts {
            params: &params,
            principal: &principal,
            scope: scope.as_ref(),
            now_ms,
            prefix: &prefix,
            first_seq: self.session.next_seq(),
        };
        let seed = match seed(history, &facts) {
            Ok(seed) => seed,
            Err(refusal) => return (StoreOutcome::Refused(refusal), false),
        };
        if let Err(error) = self
            .session
            .append_import(&seed.records, seed.media, seed.ids, &seed.receipt)
            .await
        {
            let refusal = match error {
                broca_session_store::session::SessionError::Media(media) => ImportRefusal::new(
                    "import_media_refused",
                    format!("a medium could not be stored: {media:?}"),
                ),
                other => ImportRefusal::new(
                    "import_write_failed",
                    format!("the import could not be written: {other:?}"),
                ),
            };
            // Nothing of the batch is durable, but the writer may be poisoned: the session
            // reloads on its next open.
            return (StoreOutcome::Refused(refusal), true);
        }
        delete_sources(&roots, params.source.harness, &path);
        (self.reply_from_receipt(&seed.receipt).await, false)
    }

    /// Records the compaction provider's view of an imported session as the view the session
    /// applies, version 1, before its first step. Its owner calls this once the provider has
    /// taken the moved head in, so the first Setup reports the view the head's provider already
    /// applies, and the first request carries its history head and cut. Refused once the
    /// session has run a step, and for a session that isn't an import.
    async fn install_view(&mut self, command: InstallViewCommand) -> StoreOutcome {
        let refuse = |code: &'static str, message: &str| {
            StoreOutcome::Refused(ImportRefusal::new(code, message))
        };
        let operations = &self.session.operations;
        if operations.imported_ids.is_none() {
            return refuse(
                "install_view_refused",
                "only an imported session takes an installed view",
            );
        }
        let owner = operations.owner.first_principal.as_ref().and_then(|owner| {
            serde_json::from_value::<broca_protocol::BindPrincipal>(owner.clone()).ok()
        });
        if owner.as_ref() != Some(&command.principal) {
            return refuse("owner_required", "only the session's owner installs a view");
        }
        let seeded_only = operations.owner.episodes == 1
            && operations
                .run
                .as_ref()
                .and_then(|run| run.start.as_ref())
                .is_some_and(|start| start.seeded == Some(true));
        if !seeded_only {
            return refuse(
                "install_view_refused",
                "the session has run a step, so its view comes from its compaction provider",
            );
        }
        let received = match broca_core::compaction_v2::received::ReceivedView::parse(&command.view)
        {
            Ok(received) => received,
            Err(error) => {
                return refuse(
                    "invalid_params",
                    &format!("view is not a compaction view: {error}"),
                )
            }
        };
        let view = received.view();
        if !view.verify() {
            return refuse("invalid_params", "view's checksum doesn't match its edits");
        }
        if view.version != 1 {
            return refuse(
                "invalid_params",
                "an installed view is the session's first, version 1",
            );
        }
        // The view must fit the imported history as its provider read it: every message and
        // part it names is one the import wrote, by its source id. Rendering filters out an edit
        // naming a message it doesn't hold, so a view naming an id the import never had would
        // otherwise lose that edit silently.
        if let Err(message) = self.fits_history(view).await {
            return refuse("invalid_params", &message);
        }
        let reply = InstallViewReply {
            version: view.version,
            checksum: view.checksum.clone(),
        };
        // A retry of the same install is answered as the first was.
        if let Some(applied) = &operations.view_applied {
            return if applied.view.version == reply.version
                && applied.view.checksum == reply.checksum
            {
                StoreOutcome::ViewInstalled(reply)
            } else {
                refuse(
                    "install_view_refused",
                    "the session already applies another view",
                )
            };
        }
        // An installed view names no provider that handed it over: no `session.apply_view`
        // accepted it.
        let view_record = match broca_session_loop::compaction::view_record(None, &received, None) {
            Ok(record) => record,
            Err(error) => return refuse("invalid_params", &error),
        };
        let records = [
            view_record,
            // No step has run: the view is the one the session's first step applies.
            Record::ViewApplied(ViewApplied {
                step_id: 0,
                version: reply.version,
                checksum: reply.checksum.clone(),
            }),
        ];
        match self.session.append(&records, true).await {
            Ok(_) => StoreOutcome::ViewInstalled(reply),
            Err(error) => refuse(
                "install_view_failed",
                &format!("the view could not be written: {error:?}"),
            ),
        }
    }

    /// Whether `view` fits the session's history as a compaction provider reads it: each
    /// message, part and anchor it names is one the history holds. The refusal names what
    /// doesn't fit.
    async fn fits_history(
        &self,
        view: &broca_core::compaction_v2::protocol::view::View,
    ) -> Result<(), String> {
        let placed = self.session.conversation.placed;
        let stored = self
            .session
            .entries(1, placed as usize)
            .await
            .map_err(|error| format!("the imported history could not be read: {error:?}"))?;
        let entries = stored
            .iter()
            .map(broca_session_loop::entries::project)
            .collect::<Result<Vec<_>, _>>()?;
        cortexkit_compaction_view::render::plan(&entries, &view.edits)
            .map(|_| ())
            .map_err(|refusal| format!("the view doesn't fit the imported history: {refusal:?}"))
    }
}
