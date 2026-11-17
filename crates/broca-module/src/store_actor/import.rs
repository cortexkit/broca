//! The store actor's import commands: a head's file-source import into the empty session, and
//! installing the compaction provider's first view of the imported history.

use broca_session_store::backend::SessionBackend;
use broca_session_store::record::{Record, ViewApplied};
use broca_session_store::session::SessionError;
use broca_wire::head_import::InstallViewReply;

use super::StoreActor;
use crate::store_import::{
    delete_sources, newest_of_entry, page_moves, payload_digest, read_source, reply_of, seed,
    ImportCommand, ImportRefusal, InstallViewCommand, SeedFacts, StoreCommand, StoreOutcome,
};
use crate::store_import_stage::ImportStaging;

/// What an import's command says of its seed besides its history.
struct ImportFacts<'a> {
    params: &'a broca_wire::head_import::FileImportParams,
    principal: &'a broca_protocol::BindPrincipal,
    scope: Option<&'a broca_protocol::SessionScope>,
    now_ms: u64,
}

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
            StoreCommand::Import(command) => {
                // A call asking for a page of the move list gets that page of whichever reply the
                // import answers: a new import's, or the receipt's on a repeat.
                let page = command.params.moves_from;
                let (outcome, retire) = self.import(*command).await;
                let outcome = match (outcome, page) {
                    (StoreOutcome::Imported(reply), Some(from)) => match page_moves(reply, from) {
                        Ok(reply) => StoreOutcome::Imported(reply),
                        Err(refusal) => StoreOutcome::Refused(refusal),
                    },
                    (outcome, _) => outcome,
                };
                (outcome, retire)
            }
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
        // The import is written into a replacement of this empty session in a private staging
        // folder, in as many batches as it needs, and published whole over it (see
        // `store_import_stage`).
        let Some(staging) = self.import_staging.clone() else {
            return (
                StoreOutcome::Refused(ImportRefusal::new(
                    "internal_error",
                    "this session has nowhere to stage an import",
                )),
                false,
            );
        };
        let attempt = broca_session_store::codec::hex(&broca_session_store::keys::random_id().0);
        let facts = ImportFacts {
            params: &params,
            principal: &principal,
            scope: scope.as_ref(),
            now_ms,
        };
        let written = self
            .import_staged(staging.as_ref(), &attempt, history, &facts)
            .await;
        // A published attempt's staging folder holds nothing more, and a failed one's is never
        // published.
        staging.discard(&attempt);
        let receipt = match written {
            Ok(receipt) => receipt,
            Err((refusal, exit)) => return (StoreOutcome::Refused(refusal), exit),
        };
        delete_sources(&roots, params.source.harness, &path);
        (self.reply_from_receipt(&receipt).await, false)
    }

    /// Writes the import of `history` into a replacement of this session staged as `attempt`,
    /// then publishes it over this session and takes the published session in. Returns the
    /// import's receipt, or the refusal and whether the actor must go away: once publishing has
    /// begun, the session on disk may be either one, and the next open reads which.
    async fn import_staged(
        &mut self,
        staging: &dyn ImportStaging<B>,
        attempt: &str,
        history: ck_session_import::head::HeadHistory,
        facts: &ImportFacts<'_>,
    ) -> Result<broca_session_store::record::ImportReceipt, (ImportRefusal, bool)> {
        let write_failed = |error: String| {
            ImportRefusal::new(
                "import_write_failed",
                format!("the import could not be written: {error}"),
            )
        };
        let refused = |error: SessionError| match error {
            SessionError::Media(media) => ImportRefusal::new(
                "import_media_refused",
                format!("a medium could not be stored: {media:?}"),
            ),
            other => write_failed(format!("{other:?}")),
        };
        // Until the publication, nothing is written to the live session, so it stays as it
        // was, and the actor goes on serving it.
        let backend = staging
            .begin(attempt)
            .map_err(|error| (write_failed(error), false))?;
        let mut staged = self
            .session
            .stage_replacement(backend, (self.now_ms)())
            .await
            .map_err(|error| (refused(error), false))?;
        let prefix = staged.message_id_prefix();
        let seed = seed(
            history,
            &SeedFacts {
                params: facts.params,
                principal: facts.principal,
                scope: facts.scope,
                now_ms: facts.now_ms,
                prefix: &prefix,
                first_seq: staged.next_seq(),
            },
        )
        .map_err(|refusal| (refusal, false))?;
        let prepared = staged
            .prepare_import(&seed.records, seed.media, seed.ids, &seed.receipt)
            .await
            .map_err(|error| (refused(error), false))?;
        self.import_step("before_first_batch").await;
        let mut batches = prepared.records.chunks(self.import_batch).peekable();
        while let Some(batch) = batches.next() {
            staged
                .append_import_batch(&prepared, batch)
                .await
                .map_err(|error| (refused(error), false))?;
            if batches.peek().is_some() {
                self.import_step("between_batches").await;
            }
        }
        self.import_step("after_last_batch").await;
        drop(staged);
        let addr = self.session.address().clone();
        if let Err(error) = staging
            .publish(attempt, &addr, self.session.head_version())
            .await
        {
            // The head may have been replaced before the failure, so this writer's session may
            // no longer be the stored one: the actor goes away, and the next open reads which
            // it is. Nor may its close delete the session as one it created and left untouched.
            self.created = None;
            return Err((write_failed(error), true));
        }
        self.import_step("after_publish").await;
        let triple = self.session.head().triple.clone();
        let published = staging.load(addr, &triple).await.map_err(|error| {
            self.created = None;
            (
                ImportRefusal::new(
                    "session_needs_recovery",
                    format!("the published import could not be loaded: {error:?}"),
                ),
                true,
            )
        })?;
        self.session.replace_with(published);
        // The published session is a new history, which the index of the empty one it replaced
        // never saw: the index starts again from what the published session's open hands over.
        self.reopen_media().await;
        Ok(seed.receipt)
    }

    /// Where a test stops an import as a crash would; nothing outside tests.
    async fn import_step(&self, _step: &'static str) {
        #[cfg(test)]
        if let Some(cut) = &self.import_cut {
            cut.hold(_step).await;
        }
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
        let entries = match self.fits_history(view).await {
            Ok(entries) => entries,
            Err(message) => return refuse("invalid_params", &message),
        };
        let reply = InstallViewReply {
            version: view.version,
            checksum: view.checksum.to_string(),
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
        let carry = vec![
            view_record,
            // No step has run: the view is the one the session's first step applies.
            Record::ViewApplied(ViewApplied {
                step_id: 0,
                version: reply.version,
                checksum: reply.checksum.clone(),
                head_kept: None,
            }),
        ];
        // The cut's marker, as the first step would write it when it applies the view
        // (`State::recover_marker`), so the history before the cut is sealed now rather than at
        // the first step: until then every open and read of the session would hold all of it.
        let marker = cut_marker(view, &entries, &self.key);
        let mut records = carry.clone();
        records.extend(marker.clone().map(Record::Marker));
        if let Err(error) = self.session.append(&records, true).await {
            return refuse(
                "install_view_failed",
                &format!("the view could not be written: {error:?}"),
            );
        }
        if let Some(marker) = marker {
            // The image index is brought up to every record now, so the seal saves it: an
            // imported head seals again only at its provider's next cut, and until a seal saves
            // the index, each opening of the session builds it again from every record before
            // its first request can go out.
            self.media_current_now().await;
            // The view is installed whether or not the seal succeeds; a session left unsealed
            // is sealed at its first step instead.
            if let Err(error) = broca_session_loop::compaction::seal_installed_cut(
                &mut self.session,
                marker.first_kept_ordinal,
                &carry,
                (self.now_ms)(),
            )
            .await
            {
                tracing::warn!(
                    session = %self.session.address().0,
                    ?error,
                    "an installed view's cut could not be sealed; the first step seals it"
                );
            }
        }
        StoreOutcome::ViewInstalled(reply)
    }

    /// Whether `view` fits the session's history as a compaction provider reads it: each
    /// message, part and anchor it names is one the history holds. The refusal names what
    /// doesn't fit.
    ///
    /// Only what the view can reach is read and projected: the entries its edits name, the
    /// entries after its cut, and back from the cut to the nearest entry with a numbered part,
    /// which [`cut_marker`] takes its numbering from. Everything else before the cut is dropped
    /// unseen, as it is when a step plans the view over its window. Projecting the whole history
    /// instead hashed every part of an imported history with hundreds of thousands of messages,
    /// about 12 s for 398k.
    async fn fits_history(
        &self,
        view: &broca_core::compaction_v2::protocol::view::View,
    ) -> Result<Vec<broca_core::compaction_v2::protocol::read::Entry>, String> {
        use broca_core::compaction_v2::protocol::view::{EditOp, InsertAt};
        let read_error = |error| format!("the imported history could not be read: {error:?}");
        let project = |stored: &[broca_session_store::session::StoredEntry]| {
            stored
                .iter()
                .map(broca_session_loop::entries::project)
                .collect::<Result<Vec<_>, _>>()
        };
        let named: Vec<String> = view
            .edits
            .iter()
            .filter_map(|edit| match &edit.op {
                EditOp::Drop { mid, .. } | EditOp::Replace { mid, .. } => Some(mid.clone()),
                EditOp::Cut { through } => Some(through.clone()),
                EditOp::Insert {
                    at: InsertAt::Before(mid),
                    ..
                } => Some(mid.clone()),
                _ => None,
            })
            .collect();
        let mut entries = project(
            &self
                .session
                .entries_by_mid(&named)
                .await
                .map_err(read_error)?,
        )?;
        let cut = view.edits.iter().find_map(|edit| match &edit.op {
            EditOp::Cut { through } => entries
                .iter()
                .find(|entry| &entry.mid == through)
                .map(|entry| entry.ordinal),
            _ => None,
        });
        let kept_from = cut.map_or(1, |ordinal| ordinal + 1);
        let placed = self.session.conversation.placed;
        if let Some(cut) = cut {
            // Part numbers only grow with the history, so the nearest numbered entry at or
            // before the cut holds the highest number the cut drops.
            let mut before = cut;
            while before > 1
                && !entries.iter().any(|entry| {
                    entry.ordinal <= cut && entry.parts.iter().any(|p| p.tag.is_some())
                })
            {
                let from = before.saturating_sub(NUMBERED_LOOKBACK).max(1);
                let read = self
                    .session
                    .entries(from, (before - from) as usize)
                    .await
                    .map_err(read_error)?;
                entries.extend(project(&read)?);
                before = from;
            }
        }
        entries.retain(|entry| entry.ordinal < kept_from);
        if kept_from <= placed {
            let kept = self
                .session
                .entries(kept_from, (placed + 1 - kept_from) as usize)
                .await
                .map_err(read_error)?;
            entries.extend(project(&kept)?);
        }
        entries.sort_by_key(|entry| entry.ordinal);
        entries.dedup_by_key(|entry| entry.ordinal);
        cortexkit_compaction_view::render::plan(&entries, &view.edits)
            .map_err(|refusal| format!("the view doesn't fit the imported history: {refusal:?}"))?;
        Ok(entries)
    }
}

/// How many entries [`StoreActor::fits_history`] reads back at a time from a cut whose own
/// entry has no numbered part.
const NUMBERED_LOOKBACK: u64 = 64;

/// The marker of `view`'s cut over `entries`, the session's compaction entries in order: the
/// entry after the one the cut goes through is the first kept, and the provider's numbering
/// resumes after the highest part number up to the cut. `None` when the view cuts nothing, or
/// cuts through the newest entry, whose next message has no id yet.
fn cut_marker(
    view: &broca_core::compaction_v2::protocol::view::View,
    entries: &[broca_core::compaction_v2::protocol::read::Entry],
    key: &broca_protocol::BindIdentity,
) -> Option<broca_session_store::record::Marker> {
    use broca_core::compaction_v2::protocol::view::EditOp;
    let through = view.edits.iter().find_map(|edit| match &edit.op {
        EditOp::Cut { through } => Some(through),
        _ => None,
    })?;
    let position = entries.iter().position(|entry| &entry.mid == through)?;
    let next = entries.get(position + 1)?;
    let checkpoint = entries[..=position]
        .iter()
        .flat_map(|entry| &entry.parts)
        .filter_map(|part| part.tag)
        .max()
        .unwrap_or(0);
    Some(broca_session_store::record::Marker {
        view_version: view.version,
        first_kept_mid: next.mid.clone(),
        first_kept_ordinal: next.ordinal,
        checkpoint,
        harness: Some(key.harness.clone()),
        session: Some(key.session.clone()),
    })
}
