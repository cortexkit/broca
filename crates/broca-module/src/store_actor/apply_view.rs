//! `session.apply_view` on a session kept in the new storage. The compaction provider hands
//! over a view between steps (a background summary finishing while the session is idle, its
//! answer to the notice after a run's last step, or its re-offer at Setup), and the session
//! records it as the view it accepted. The next step applies it, as today's actor has today's
//! loop do.
//!
//! The view is checked against the session's entries by the same check the loop runs
//! (`State::accept`), so it is refused, and answered again on a retry, exactly as today's. The
//! accepted view is written as the session's `View` record and set in the compaction state the
//! actor keeps for its whole life and hands to every run, in the same step, so a run already
//! going or the next one applies it without reading the session again.
//!
//! A run holds that state while it writes a view of its own through this actor. A command
//! that finds it held waits, kept by the actor, until the run lets it go: the actor goes on
//! serving the run meanwhile, which waiting on the state there would stop.

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
/// handed it over, as today's journal records `session.apply_view`'s admitted caller.
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

    async fn applied(&self, _: u64, _: &ProviderView) -> Result<(), String> {
        Err("a view is applied by a run's step, never by session.apply_view".into())
    }

    async fn marker(&self, _: &ProviderView, _: &str, _: u64, _: u64) -> Result<(), String> {
        Err("a cut's marker is written by a run's step, never by session.apply_view".into())
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
            let restored = match self.session.open_log_records().await {
                Ok(records) => broca_session_loop::compaction::restore(
                    &records,
                    &self.key,
                    &self.session.operations,
                ),
                Err(error) => Err(format!("{error:?}")),
            };
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
        let entries = crate::store_entries::all_entries(&self.session).await?;
        let floor = broca_session_loop::run::signed_floor(
            &self.session,
            &self.parts,
            &mut Default::default(),
        )
        .await
        .map_err(|error| format!("{error:?}"))?;
        let conditions = v2::host::conditions(&config, &entries, false, None, floor)?;
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
                // The state names the provider too, as today's actor sets it, so the step that
                // applies the view, and a seal carrying it, keep who handed it over.
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
}

fn internal(message: String) -> StoreOutcome {
    StoreOutcome::Refused(ImportRefusal::new("internal_error", message))
}
