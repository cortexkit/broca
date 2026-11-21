//! Scope admission on a session kept in the session store, by today's actor's rules (`crate::actor`)
//! and the shared ones in `broca_core::scope_admission`: a send under an agent-run scope, a send
//! that reopens the session's scope at a later epoch, and a cancel from a module other than the
//! session's owner.
//!
//! - An agent-run send (a route stamped with a `run_id`) runs model-only: one declaring tools, of
//!   its own or through its plan, is refused naming the field, as today's actor refuses it.
//!   Everything else about it (its carrier, its frozen run and agent identifiers) is decided by
//!   the shared `decide_scope`, the same for both storages.
//! - A reopening send is admitted only when it starts its own run at once, since only that
//!   run's start can record the new epoch (`reopen_cannot_start_now`).
//! - A cancel from another module (`LineageCmd::CancelFrom`) is checked again against the
//!   session's owner summary and the senders its latest plan grants, as today's actor checks it
//!   before it stops the run (`cancel_from`).

use broca_protocol::{BindPrincipal, Delivery, RunId, SendScope, SessionScope, SubmissionRequest};
use broca_session_loop::writer::Writer;
use broca_session_store::backend::SessionBackend;

use super::StoreActor;
use crate::actor::{CancelOutcome, ScopeFreezeBusyReason, SendOutcome};

/// The refusal of an agent-run send that declares tools, as today's actor words it, or `None`
/// for any other send. `tools` says whether the send names tools of its own, `planned_tools`
/// whether its plan gives the run any. Only a route stamped with a `run_id` is checked here, as
/// today's actor checks it; a flow route's tools are left to the rest of admission.
pub(super) fn agent_run_tools(
    scope: &SendScope,
    tools: bool,
    planned_tools: bool,
) -> Option<SendOutcome> {
    if !matches!(scope, SendScope::Stamped(bind) if bind.scope.run_id.is_some()) {
        return None;
    }
    broca_core::scope_admission::model_only_scope_tool_field(scope, tools, planned_tools)
        .map(|field| SendOutcome::Error(format!("agent-run sends cannot carry {field}")))
}

impl<B: SessionBackend + Send + Sync + 'static> StoreActor<B> {
    /// Why a send that reopens the session's scope at a later epoch can't be admitted now, or
    /// `None` when it starts its own run at once: no run is running or paused, and nothing queued
    /// would start one first. Only that run's start can record the new epoch, so every other
    /// path is refused, with nothing written, as today's actor refuses it
    /// (`reopen_cannot_start_now`):
    ///
    /// - A steer would join a run (running, or paused) whose tool routes belong to the epoch that
    ///   ended, and a retry of that run's own send would drive it under that epoch. Both are
    ///   refused `scope_owner_mismatch` naming the frozen scope, `frozen`, as any send under
    ///   another epoch was before reopening existed.
    /// - Any other send would wait in the queue, whose record keeps no scope, so its run would
    ///   take the old epoch. It is refused `scope_freeze_busy`, which the caller retries once the
    ///   session is idle. So is an interrupt, though one otherwise always goes through.
    ///
    /// `queue_starts_runs` says whether the queue holds a send that starts a run on its own.
    pub(super) fn reopen_cannot_start_now(
        &self,
        run_id: &RunId,
        delivery: Delivery,
        frozen: &SessionScope,
        request: &SubmissionRequest,
        queue_starts_runs: bool,
    ) -> Option<SendOutcome> {
        let refused = || {
            SendOutcome::ScopeRefused(broca_core::scope_admission::reopen_refused(
                frozen,
                request.principal.as_ref(),
                &request.scope,
            ))
        };
        let busy_with = |run: &RunId| SendOutcome::ScopeFreezeBusy {
            reason: ScopeFreezeBusyReason::RunActive,
            run_id: Some(run.clone()),
        };
        let unfinished = match &self.run {
            // A run whose end is already in the session has ended, and the actor learns it a
            // moment later: the session is about to be idle, so the send is told to retry.
            Some(active) if active.run_id != *run_id && self.ended(&active.run_id) => {
                return Some(busy_with(&active.run_id));
            }
            Some(active) => Some(active.run_id.clone()),
            None => self.paused().map(|(paused, _)| paused),
        };
        if let Some(unfinished) = unfinished {
            if delivery == Delivery::Steer || unfinished == *run_id {
                return Some(refused());
            }
            return Some(busy_with(&unfinished));
        }
        queue_starts_runs.then_some(SendOutcome::ScopeFreezeBusy {
            reason: ScopeFreezeBusyReason::QueueNonempty,
            run_id: None,
        })
    }

    /// Whether the session holds the end of the run `run_id`, its latest.
    fn ended(&self, run_id: &RunId) -> bool {
        self.session
            .operations()
            .run
            .as_ref()
            .is_some_and(|run| run.run_id == run_id.0 && run.finished.is_some())
    }

    /// A cancel of `run_id` from `principal`, over a route that says `scope`, which the serve
    /// layer let through because the session's plan grants that module `interrupt`. The grant is
    /// checked again here, against the session as it stands now, since a plan refresh accepted
    /// since the serve layer read it may have withdrawn it; today's actor checks it again the
    /// same way (`cancel_authority`). The session's owner, over a route that carries owner
    /// authority, cancels as itself. A flow- or run-stamped route never carries it: such a
    /// route's opener sends for an automation, and cancels only with a grant of its own
    /// (`decide_owner_access`). A granted module cancels only at once, never at a step
    /// boundary. Anyone else is refused with the owner check's refusal, writing nothing.
    pub(super) async fn cancel_from(
        &mut self,
        run_id: &RunId,
        at: broca_wal::record::RunCancelAt,
        principal: Option<&BindPrincipal>,
        scope: &SendScope,
    ) -> CancelOutcome {
        let ownership = match broca_session_loop::owner::ownership_with_plan(
            self.session.operations(),
            &self.parts,
        ) {
            Ok(ownership) => ownership,
            Err(error) => {
                return CancelOutcome::Error(format!(
                    "the session's stored owner can't be read: {error}"
                ))
            }
        };
        let refusal =
            match broca_core::scope_admission::decide_owner_access(&ownership, principal, scope) {
                Ok(()) => return self.cancel(run_id, at).await,
                Err(refusal) => refusal,
            };
        let granted = at == broca_wal::record::RunCancelAt::Now
            && matches!(principal, Some(BindPrincipal::Reserved { module_id })
                if crate::sender_admission::interrupt_granted(&ownership, module_id));
        if !granted {
            return CancelOutcome::ScopeRefused(refusal);
        }
        let outcome = self.cancel(run_id, at).await;
        if let (CancelOutcome::Ack, Some(sender)) = (&outcome, principal) {
            tracing::info!(
                session = ?self.key,
                run_id = %run_id.0,
                sender = %sender.as_owner_string(),
                "named sender stopped run"
            );
        }
        outcome
    }
}
