//! `session.apply_view` on a session kept in the session store. The compaction provider hands
//! over a view between steps (a background summary finishing while the session is idle, its
//! answer to the notice after a run's last step, or its re-offer at Setup), and the session
//! records it as the view it accepted. The next step applies it.
//!
//! The view is checked against the session's entries using `State::accept`, the same check
//! used by the run loop. A view that is refused is answered again on retry. The accepted view
//! is written as the session's `View` record and set in the compaction state the actor keeps
//! for its whole life and hands to every run, in the same step, so a run already going or the
//! next one applies it without reading the session again.
//!
//! A run holds that state while it writes its own view through this actor. A command that
//! finds the state held waits in the actor until the run lets it go. The actor continues
//! serving the run while the command waits; waiting on the state there would stop that
//! service.

use broca_core::compaction_v2::{
    self as v2,
    protocol::view::{ApplyViewAnswer, View as ProviderView},
    received::ReceivedView,
    state::{Journal, State},
};
use broca_session_store::backend::SessionBackend;
use broca_session_store::record::Record;
use broca_session_store::session::Session;
use tokio::sync::oneshot;

use super::StoreActor;
use crate::store_import::{ApplyViewCommand, ImportRefusal, StoreOutcome};

/// Writes an accepted view through the actor's own session, keeping what it appended so a
/// running run's copy of the session catches up with it. The view records the provider that
/// handed it over.
struct ActorJournal<'s, B: SessionBackend> {
    session: tokio::sync::Mutex<&'s mut Session<B>>,
    applied_by: Option<&'s str>,
    appended: std::sync::Mutex<Vec<(u64, Record)>>,
}

impl<B: SessionBackend> Journal for ActorJournal<'_, B> {
    async fn accepted(
        &self,
        previous: Option<&ReceivedView>,
        received: &ReceivedView,
    ) -> Result<(), String> {
        let record =
            broca_session_loop::compaction::view_record(previous, received, self.applied_by)?;
        let seq = self
            .session
            .lock()
            .await
            .append(std::slice::from_ref(&record), true)
            .await
            .map_err(|error| format!("the view could not be written: {error:?}"))?;
        self.appended
            .lock()
            .map_err(|_| "the journal's record of its appends is poisoned".to_owned())?
            .push((seq, record));
        Ok(())
    }

    async fn applied(&self, _: u64, _: &ProviderView, _: bool) -> Result<(), String> {
        Err("a view is applied by a run's step, never by session.apply_view".into())
    }

    async fn marker(&self, _: &ProviderView, _: &str, _: u64, _: u64) -> Result<(), String> {
        Err("a cut's marker is written by a run's step, never by session.apply_view".into())
    }

    async fn entry_ordinal(&self, mid: &str) -> Result<Option<u64>, String> {
        let session = self.session.lock().await;
        broca_session_loop::writer::Writer::entry_by_mid(&**session, mid)
            .await
            .map(|entry| entry.map(|entry| entry.ordinal))
            .map_err(|error| format!("the history could not be read: {error:?}"))
    }
}

impl<B: SessionBackend + Send + Sync + 'static> StoreActor<B> {
    /// Serves `command` and answers `reply`, or keeps both until a run lets the compaction
    /// state go.
    pub(super) async fn apply_view_or_wait(
        &mut self,
        command: ApplyViewCommand,
        reply: oneshot::Sender<StoreOutcome>,
    ) {
        match self.try_apply_view(&command).await {
            Some(outcome) => {
                let _ = reply.send(outcome);
            }
            None => self.pending_views.push((command, reply)),
        }
    }

    /// Serves the commands kept while a run held the compaction state, in the order they came.
    pub(super) async fn retry_views(&mut self) {
        if self.pending_views.is_empty() {
            return;
        }
        for (command, reply) in std::mem::take(&mut self.pending_views) {
            self.apply_view_or_wait(command, reply).await;
        }
    }

    /// `None` while a run holds the compaction state.
    async fn try_apply_view(&mut self, command: &ApplyViewCommand) -> Option<StoreOutcome> {
        let compaction = std::sync::Arc::clone(&self.compaction_session);
        if !compaction.is_initialized() {
            // No run of this actor read the state from the session yet; read it as a run would.
            let restored = broca_session_loop::compaction::restore_live(&self.session, &self.key)
                .await
                .map_err(|error| format!("{error:?}"));
            match restored {
                Ok(state) => compaction.restore(state).await,
                Err(error) => return Some(internal(error)),
            }
        }
        let mut state = compaction.state.try_lock().ok()?;
        Some(match self.accept_view(&mut state, command).await {
            Ok(answer) => StoreOutcome::ViewApplied(answer),
            Err(error) => internal(error),
        })
    }

    /// Checks the view against the session's entries under the provider rules the latest run
    /// froze, and records it as the accepted view when it passes. The answer's JSON.
    async fn accept_view(
        &mut self,
        state: &mut State,
        command: &ApplyViewCommand,
    ) -> Result<String, String> {
        let received = ReceivedView::parse(&command.view).map_err(|error| error.to_string())?;
        let start = self
            .session
            .operations
            .run
            .as_ref()
            .and_then(|run| run.start.as_ref())
            .ok_or("compaction session has no frozen configuration")?;
        let config = broca_session_loop::run::run_config_of_start(start, &self.parts)
            .map_err(|error| format!("{error:?}"))?;
        let entries = self.entries_to_check(received.view()).await?;
        let floor = broca_session_loop::run::signed_floor(
            &self.session,
            &self.parts,
            &mut Default::default(),
        )
        .await
        .map_err(|error| format!("{error:?}"))?;
        let floor = broca_session_loop::compaction::floor_through(&self.session, &entries, floor)
            .await
            .map_err(|error| format!("{error:?}"))?;
        let conditions = v2::host::conditions(&config, &entries, false, None, floor.as_deref())?;
        #[cfg(test)]
        self.check_alike_against_every_entry(
            &entries,
            state.latest(),
            received.view(),
            &config,
            floor.as_deref(),
        )
        .await?;
        let journal = ActorJournal {
            session: tokio::sync::Mutex::new(&mut self.session),
            applied_by: command.caller.as_deref(),
            appended: Default::default(),
        };
        let answer = match state
            .accept(
                &journal,
                &entries,
                received,
                &conditions.rules,
                v2::host::view_floor(&conditions),
            )
            .await?
        {
            Ok(()) => {
                // The state names the provider too, so the step that applies the view and a seal
                // carrying it keep who handed it over.
                state.restore_accepted_by(command.caller.clone());
                ApplyViewAnswer::Accepted
            }
            Err(refusal) => ApplyViewAnswer::Refused { refusal },
        };
        let appended = journal
            .appended
            .into_inner()
            .map_err(|_| "the journal's record of its appends is poisoned".to_owned())?;
        if let Some(active) = self.run.as_mut() {
            for (seq, record) in appended {
                active.link.note_own(seq, &[record]);
            }
        }
        serde_json::to_string(&answer).map_err(|error| error.to_string())
    }

    /// The entries `view` is checked against: the ones the session's next step checks it
    /// against (`broca_session_loop::compaction::transcript`), its open window and the earlier
    /// entries the view names. A view cutting inside the window drops everything before the
    /// window, so nothing else there can change its answer. Reading every entry instead decoded
    /// the whole sealed history on every apply: about 38 s for a head imported with 422k
    /// entries, past the provider's 30 s deadline.
    ///
    /// A view with no cut, or one cutting into the sealed history, keeps entries the window
    /// doesn't hold, so it is checked against every entry, as before.
    async fn entries_to_check(
        &self,
        view: &ProviderView,
    ) -> Result<Vec<v2::protocol::read::Entry>, String> {
        use v2::protocol::view::{EditOp, InsertAt};
        let window_start = self.session.conversation.window_start();
        let cut = view.edits.iter().find_map(|edit| match &edit.op {
            EditOp::Cut { through } => Some(through),
            _ => None,
        });
        let Some(cut) = cut.filter(|_| window_start > 1) else {
            return crate::store_entries::all_entries(&self.session).await;
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
        let project = |stored: Vec<broca_session_store::session::StoredEntry>| {
            stored
                .iter()
                .map(broca_session_loop::entries::project)
                .collect::<Result<Vec<_>, _>>()
        };
        let read_error = |error| format!("{error:?}");
        let mut entries = project(
            crate::store_read::served_compaction_entries_by_mid(&self.session, &named)
                .await
                .map_err(read_error)?,
        )?;
        if entries
            .iter()
            .any(|entry| &entry.mid == cut && entry.ordinal < window_start)
        {
            return crate::store_entries::all_entries(&self.session).await;
        }
        entries.retain(|entry| entry.ordinal < window_start);
        entries.extend(project(
            crate::store_read::served_compaction_entries(&self.session, window_start, usize::MAX)
                .await
                .map_err(read_error)?,
        )?);
        entries.sort_by_key(|entry| entry.ordinal);
        entries.dedup_by_key(|entry| entry.ordinal);
        Ok(entries)
    }

    /// Test builds check every apply's answer against the answer over every entry, so each
    /// test that applies a view proves [`Self::entries_to_check`] changes no answer. Skipped
    /// for a session longer than [`CROSS_CHECK_ENTRIES`]: the scale gate's heads are, and
    /// reading them whole is what the gate measures away.
    #[cfg(test)]
    async fn check_alike_against_every_entry(
        &self,
        entries: &[v2::protocol::read::Entry],
        latest: Option<&ProviderView>,
        view: &ProviderView,
        config: &broca_wal::RunConfig,
        floor: Option<&str>,
    ) -> Result<(), String> {
        if crate::store_read::transcript_len(&self.session) > CROSS_CHECK_ENTRIES {
            return Ok(());
        }
        let every = crate::store_entries::all_entries(&self.session).await?;
        let answer = |entries: &[v2::protocol::read::Entry]| -> Result<String, String> {
            let conditions = v2::host::conditions(config, entries, false, None, floor)?;
            Ok(format!(
                "{:?}",
                v2::views::check(
                    entries,
                    latest,
                    view,
                    &conditions.rules,
                    v2::host::view_floor(&conditions),
                )
            ))
        };
        assert_eq!(
            answer(entries)?,
            answer(&every)?,
            "a view checked against the window and its named entries is answered as against \
             every entry"
        );
        Ok(())
    }
}

/// The longest session a test build cross-checks each apply on.
#[cfg(test)]
const CROSS_CHECK_ENTRIES: u64 = 20_000;

fn internal(message: String) -> StoreOutcome {
    StoreOutcome::Refused(ImportRefusal::new("internal_error", message))
}
