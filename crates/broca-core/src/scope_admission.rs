//! Who may send into a session, decided from its history and the route a send arrived over.
//!
//! A scope is the session identity the subc daemon attaches to routes opened under it:
//! owner, ref and epoch. A stamp is the copy of that scope the daemon sends with each route
//! bind, including its attributes. The bind principal is the route opener's identity,
//! attested by the daemon. For a scoped send, broca receives both the stamp and that principal.
//! A carrier is a module other than the scope's owner that the daemon lets open routes under
//! that scope. A flow scope has `flow_id` in its attributes and is used for agent-owned
//! automations run by Basal. Admission applies these rules:
//!
//! - Ordinary scoped sessions accept only their owner under the frozen scope identity.
//! - Flow scopes also accept a carrier admitted by the daemon. The flow identifier and sending
//!   principal are frozen with the scope on `RunStarted` and must match on every later send.
//! - A send under a later epoch of the frozen scope (same owner, ref and flow identifier, and
//!   for a flow the same sender) reopens the session: its owner started that scope again after
//!   the previous epoch ended, for example when idle expiry removed it. The daemon never stamps
//!   an ended epoch again, so the episode the send opens records the new epoch on its
//!   `RunStarted`, and later sends are judged against it. Only a send that opens its own
//!   episode at once may do this; the caller decides that (see [`ScopeDecision::Reopen`]).
//! - A session's first episode freezes the scope from the stamp. Existing unscoped history may
//!   adopt an ordinary scope only when every episode recorded that scope's owner. It can
//!   never adopt a flow scope: each automation must start its own session history.
//! - An unscoped session sent to without a stamp accepts the send only from the principal of
//!   its first send, when that first episode recorded one. A session whose first episode
//!   recorded no principal (it predates principal recording) keeps accepting sends as before.
//!
//! Admission compares scope identity. `agent_id` is frozen only with an agent-run identifier;
//! ordinary agent scopes still ignore `agent_id` and `delegates`.

use broca_protocol::{BindPrincipal, SendScope, SessionScope};

use crate::SessionOwnershipV1;

/// The daemon gives broca no other signal that a sender may only run flows, so this
/// restriction is specific to Basal's attested reserved principal.
pub const FLOW_ONLY_MODULE: &str = "basal";

/// Agent-run admission and the module's advertised capability must be enabled together:
/// the daemon routes run-scoped binds only to modules that promise to handle them.
pub const AGENT_RUN_SCOPES_WRITER_ENABLED: bool = true;

pub fn is_flow_only(principal: Option<&BindPrincipal>) -> bool {
    matches!(principal, Some(BindPrincipal::Reserved { module_id }) if module_id == FLOW_ONLY_MODULE)
}

/// Require Basal's write-side route to carry a flow stamp, before other admission checks.
pub fn require_flow_scope(
    principal: Option<&BindPrincipal>,
    send: &SendScope,
) -> Result<(), ScopeRefusal> {
    if !AGENT_RUN_SCOPES_WRITER_ENABLED {
        if let SendScope::Stamped(bind) = send {
            if bind.scope.run_id.is_some() || bind.scope.agent_id.is_some() {
                return Err(ScopeRefusal::AgentRunScopesUnsupported {
                    scope: bind.scope.clone().into(),
                });
            }
        }
    }
    require_work_scope(principal, send, AGENT_RUN_SCOPES_WRITER_ENABLED)
}

fn require_work_scope(
    principal: Option<&BindPrincipal>,
    send: &SendScope,
    agent_runs: bool,
) -> Result<(), ScopeRefusal> {
    if is_flow_only(principal)
        && !matches!(send, SendScope::Internal)
        && !matches!(send, SendScope::Stamped(bind) if bind.scope.flow_id.is_some()
            || (agent_runs && bind.scope.run_id.is_some() && bind.scope.agent_id.is_some()))
    {
        return Err(ScopeRefusal::FlowScopeRequired);
    }
    Ok(())
}

/// Reads on unscoped routes are allowed too: they cannot start work. Basal may
/// nevertheless read or affect only its own sessions, regardless of route scope.
pub fn decide_sender_access(
    principal: Option<&BindPrincipal>,
    first_sender: Option<Option<&BindPrincipal>>,
) -> Result<(), ScopeRefusal> {
    if is_flow_only(principal) && first_sender.is_some_and(|sender| sender != principal) {
        return Err(ScopeRefusal::SenderNotOwner);
    }
    Ok(())
}

/// Cancel and retract compare owner principals, not the route's scope or epoch. Cleanup
/// commonly arrives on an unscoped route after the scope that started the work has ended.
/// Only an attested consumer on the first episode protects a session; older, direct and
/// unverified histories retain their unrestricted cancel/retract behavior.
pub fn decide_owner_access(
    ownership: &SessionOwnershipV1,
    principal: Option<&BindPrincipal>,
    send: &SendScope,
) -> Result<(), ScopeRefusal> {
    let Some(first @ BindPrincipal::Reserved { .. }) = ownership.first_principal.as_ref() else {
        return Ok(());
    };
    let is_owner = matches!(principal, Some(BindPrincipal::Reserved { .. }))
        && (principal == Some(first)
            || ownership
                .scope
                .as_ref()
                .is_some_and(|scope| principal == Some(&scope.owner)));
    if is_owner {
        return Ok(());
    }
    let mismatch = match &ownership.scope {
        Some(frozen) => OwnerMismatch::Scoped {
            frozen: frozen.clone(),
            presented_principal: principal.cloned(),
            presented_scope: match send {
                SendScope::Stamped(bind) => Some(bind.scope.clone()),
                _ => None,
            },
        },
        None => OwnerMismatch::FirstPrincipal {
            first: first.clone(),
            presented_principal: principal.cloned(),
        },
    };
    Err(ScopeRefusal::OwnerMismatch(Box::new(
        OwnerMismatch::OwnerOperation {
            first: first.clone(),
            mismatch: Box::new(mismatch),
        },
    )))
}

/// What admission does with a send's scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeDecision {
    /// No scope applies: the episode is unscoped.
    Unscoped,
    /// The session is already scoped and the send matches its admitted identity; the episode
    /// repeats the frozen scope, including automation identity when present.
    Frozen(SessionScope),
    /// The episode freezes this stamped scope: the session's first episode, or an ordinary
    /// scope adoption whose every earlier episode recorded the scope's owner.
    Freeze(SessionScope),
    /// The send presents a later epoch of the frozen scope, everything else about it identical,
    /// and the episode it opens records that epoch. Its episode must start right away: a
    /// queued submission records no scope, so its episode would take the old epoch, and a steer
    /// joins a run whose tool routes belong to the epoch that ended. The caller refuses the
    /// send otherwise, with [`reopen_refused`] for a send joining or retrying an old-epoch run.
    Reopen(SessionScope),
}

impl ScopeDecision {
    /// The scope the episode's `RunStarted` carries.
    pub fn scope(&self) -> Option<&SessionScope> {
        match self {
            ScopeDecision::Unscoped => None,
            ScopeDecision::Frozen(scope)
            | ScopeDecision::Freeze(scope)
            | ScopeDecision::Reopen(scope) => Some(scope),
        }
    }
}

/// Why a send is refused. Each is terminal for that send: nothing was written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopeRefusal {
    /// Basal can start work only under a flow scope.
    FlowScopeRequired,
    /// Basal cannot read or affect a session driven first by another sender.
    SenderNotOwner,
    /// The send does not come from the identity allowed to drive the session.
    /// Boxed: it names two scopes and two principals, and a refusal is off the hot path.
    OwnerMismatch(Box<OwnerMismatch>),
    /// The session is scoped and the daemon does not advertise `scopes/v1`, so no route to it
    /// can carry the owner's stamp.
    Unsupported { scope: Box<SessionScope> },
    /// An unscoped session with earlier episodes cannot adopt the stamped scope.
    AdoptionRefused(AdoptionRefusal),
    /// The automation changed its frozen sending principal or flow identifier.
    FlowMismatch { field: FlowMismatchField },
    /// This build decodes agent-run scope records but cannot create or resume their episodes.
    AgentRunScopesUnsupported { scope: Box<SessionScope> },
    /// A later send changed `run_id` or `agent_id` fixed by the session's first episode.
    RunMismatch { field: RunMismatchField },
}

/// The frozen automation identity component that differed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowMismatchField {
    Sender,
    FlowId,
}

/// The identifier that differed from the value fixed by the session's first episode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunMismatchField {
    RunId,
    AgentId,
}

/// Who the session accepts, and what the refused send presented.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerMismatch {
    /// Cancel/retract accept the attested first sender and, when scoped, the scope owner.
    /// Carry both identities so the refusal can name both accepted principals without
    /// claiming that cancellation or retraction requires a matching route stamp.
    OwnerOperation {
        first: BindPrincipal,
        mismatch: Box<OwnerMismatch>,
    },
    /// A scoped session: only its owner, over a route stamped with the frozen scope.
    Scoped {
        frozen: SessionScope,
        presented_principal: Option<BindPrincipal>,
        presented_scope: Option<SessionScope>,
    },
    /// A stamped send whose route was opened by someone other than the scope's owner. Only the
    /// owner may freeze or adopt its scope.
    NotTheOwner {
        stamped: SessionScope,
        presented_principal: Option<BindPrincipal>,
    },
    /// An unscoped session: only the principal of its first send.
    FirstPrincipal {
        first: BindPrincipal,
        presented_principal: Option<BindPrincipal>,
    },
}

/// Why an unscoped session cannot adopt a stamped scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdoptionRefusal {
    /// An automation must start its own session, never adopt an agent's history.
    FlowScope,
    /// At least one earlier episode recorded no principal.
    NoRecordedPrincipal,
    /// Every earlier episode recorded a principal, and this one is not the scope's owner.
    PrincipalMismatch { recorded: BindPrincipal },
}

/// Decide a caller's send against the session's history.
///
/// `ownership` is the summary of every `RunStarted` already in the session, `principal` the
/// principal the daemon stamped on the send's route, and `send` what that route says about
/// scopes. An `Internal` send (not a caller's) takes whatever scope the session froze.
pub fn decide_scope(
    ownership: &SessionOwnershipV1,
    principal: Option<&BindPrincipal>,
    send: &SendScope,
) -> Result<ScopeDecision, ScopeRefusal> {
    decide_scope_with_writer(ownership, principal, send, AGENT_RUN_SCOPES_WRITER_ENABLED)
}

fn decide_scope_with_writer(
    ownership: &SessionOwnershipV1,
    principal: Option<&BindPrincipal>,
    send: &SendScope,
    agent_runs: bool,
) -> Result<ScopeDecision, ScopeRefusal> {
    let presented = match send {
        SendScope::Stamped(bind) => Some(&bind.scope),
        _ => None,
    };
    if !agent_runs {
        if let Some(scope) = ownership
            .scope
            .as_ref()
            .filter(|scope| scope.run_id.is_some() || scope.agent_id.is_some())
            .or_else(|| {
                presented.filter(|scope| scope.run_id.is_some() || scope.agent_id.is_some())
            })
        {
            return Err(ScopeRefusal::AgentRunScopesUnsupported {
                scope: scope.clone().into(),
            });
        }
    }
    let stamped = match send {
        SendScope::Internal => {
            return Ok(ownership
                .scope
                .clone()
                .map_or(ScopeDecision::Unscoped, ScopeDecision::Frozen));
        }
        SendScope::DaemonWithoutScopes | SendScope::Unstamped => None,
        SendScope::Stamped(bind) => Some(&bind.scope),
    };

    // A frozen agent-run session reports identifier changes (including a removed identifier)
    // before Basal's generic missing-work-scope refusal. Ordinary and flow ordering is unchanged.
    if ownership
        .scope
        .as_ref()
        .is_none_or(|scope| scope.run_id.is_none())
    {
        require_work_scope(principal, send, agent_runs)?;
    }
    decide_sender_access(
        principal,
        (ownership.episodes > 0).then_some(ownership.first_principal.as_ref()),
    )?;

    if let Some(frozen) = &ownership.scope {
        if frozen.run_id.is_some() {
            if stamped.and_then(|scope| scope.run_id.as_ref()) != frozen.run_id.as_ref() {
                return Err(ScopeRefusal::RunMismatch {
                    field: RunMismatchField::RunId,
                });
            }
            if stamped.and_then(|scope| scope.agent_id.as_ref()) != frozen.agent_id.as_ref() {
                return Err(ScopeRefusal::RunMismatch {
                    field: RunMismatchField::AgentId,
                });
            }
            if !admitted_agent_run_sender(principal, frozen)
                || principal != frozen.sender.as_ref()
                || stamped.and_then(|scope| scope.sender.as_ref()) != frozen.sender.as_ref()
            {
                return Err(scoped_mismatch(frozen, principal, stamped));
            }
            if stamped == Some(frozen) {
                return Ok(ScopeDecision::Frozen(frozen.clone()));
            }
        }
        if frozen.flow_id.is_some() {
            if stamped.and_then(|scope| scope.flow_id.as_ref()) != frozen.flow_id.as_ref() {
                return Err(ScopeRefusal::FlowMismatch {
                    field: FlowMismatchField::FlowId,
                });
            }
            if principal != frozen.sender.as_ref()
                || stamped.and_then(|scope| scope.sender.as_ref()) != frozen.sender.as_ref()
            {
                return Err(ScopeRefusal::FlowMismatch {
                    field: FlowMismatchField::Sender,
                });
            }
            if stamped == Some(frozen) && principal.is_some() {
                return Ok(ScopeDecision::Frozen(frozen.clone()));
            }
        }
        // Checked before the frozen-identity match below, so a send repeating the frozen epoch
        // is never taken for a reopen.
        if let Some(reopened) = reopened_scope(frozen, principal, stamped) {
            return Ok(ScopeDecision::Reopen(reopened));
        }
        let owner_sends_under_frozen = frozen.flow_id.is_none()
            && frozen.run_id.is_none()
            && stamped == Some(frozen)
            && principal == Some(&frozen.owner);
        if owner_sends_under_frozen {
            return Ok(ScopeDecision::Frozen(frozen.clone()));
        }
        if matches!(send, SendScope::DaemonWithoutScopes) {
            return Err(ScopeRefusal::Unsupported {
                scope: frozen.clone().into(),
            });
        }
        return Err(scoped_mismatch(frozen, principal, stamped));
    }

    let Some(stamped) = stamped else {
        // Unscoped session, unstamped send: the first send's principal owns the session, when
        // the first episode recorded one.
        if ownership.episodes > 0 {
            if let Some(first) = &ownership.first_principal {
                if principal != Some(first) {
                    return Err(ScopeRefusal::OwnerMismatch(Box::new(
                        OwnerMismatch::FirstPrincipal {
                            first: first.clone(),
                            presented_principal: principal.cloned(),
                        },
                    )));
                }
            }
        }
        return Ok(ScopeDecision::Unscoped);
    };

    // Whether the session's history allows adopting the stamped scope is decided before
    // checking who opened the route, so a session that can never adopt it says so whoever
    // asks: that answer depends only on the history.
    if ownership.episodes > 0 {
        if stamped.run_id.is_some() {
            return Err(scoped_mismatch(stamped, principal, Some(stamped)));
        }
        if stamped.flow_id.is_some() {
            return Err(ScopeRefusal::AdoptionRefused(AdoptionRefusal::FlowScope));
        }
        if ownership.any_unrecorded_principal {
            return Err(ScopeRefusal::AdoptionRefused(
                AdoptionRefusal::NoRecordedPrincipal,
            ));
        }
        if let Some(recorded) = ownership
            .recorded_principals
            .iter()
            .find(|recorded| **recorded != stamped.owner)
        {
            return Err(ScopeRefusal::AdoptionRefused(
                AdoptionRefusal::PrincipalMismatch {
                    recorded: recorded.clone(),
                },
            ));
        }
    }
    // bind.principal records the route opener's identity, attested by the daemon. On a flow
    // route, its presence means the daemon admitted the opener as owner or carrier, so either
    // may send. Without that identity, the send is refused.
    let admitted_flow_sender = stamped.flow_id.is_some() && principal.is_some();
    let admitted_run_sender = admitted_agent_run_sender(principal, stamped);
    if (stamped.run_id.is_some() && !admitted_run_sender)
        || (principal != Some(&stamped.owner) && !admitted_flow_sender && !admitted_run_sender)
    {
        return Err(ScopeRefusal::OwnerMismatch(Box::new(
            OwnerMismatch::NotTheOwner {
                stamped: stamped.clone(),
                presented_principal: principal.cloned(),
            },
        )));
    }
    let mut frozen = stamped.clone();
    if frozen.flow_id.is_some() || frozen.run_id.is_some() {
        frozen.sender = principal.cloned();
    }
    Ok(ScopeDecision::Freeze(frozen))
}

/// Test entry point for evaluating admission with agent-run scope writing enabled.
/// Production calls `decide_scope`, which uses `AGENT_RUN_SCOPES_WRITER_ENABLED`.
#[cfg(feature = "test-clock")]
pub fn decide_agent_run_scope_for_test(
    ownership: &SessionOwnershipV1,
    principal: Option<&BindPrincipal>,
    send: &SendScope,
) -> Result<ScopeDecision, ScopeRefusal> {
    decide_scope_with_writer(ownership, principal, send, true)
}

/// The stamped scope, when it is a later epoch of `frozen` sent by the identity allowed to
/// drive the session: the owner, or on a flow scope its frozen sender. The owner, ref, flow
/// identifier and sender must all equal the frozen ones; only the epoch may move, and only up.
fn reopened_scope(
    frozen: &SessionScope,
    principal: Option<&BindPrincipal>,
    stamped: Option<&SessionScope>,
) -> Option<SessionScope> {
    let stamped = stamped?;
    let admitted_sender = if frozen.run_id.is_some() {
        admitted_agent_run_sender(principal, frozen) && principal == frozen.sender.as_ref()
    } else {
        match frozen.flow_id {
            Some(_) => principal.is_some() && principal == frozen.sender.as_ref(),
            None => principal == Some(&frozen.owner),
        }
    };
    let same_identity = stamped.owner == frozen.owner
        && stamped.scope_ref == frozen.scope_ref
        && stamped.flow_id == frozen.flow_id
        && stamped.run_id == frozen.run_id
        && stamped.agent_id == frozen.agent_id
        && stamped.sender == frozen.sender;
    let later_epoch = stamped.scope_epoch > frozen.scope_epoch;
    (admitted_sender && same_identity && later_epoch).then(|| stamped.clone())
}

fn admitted_agent_run_sender(principal: Option<&BindPrincipal>, scope: &SessionScope) -> bool {
    scope.run_id.is_some()
        && scope.agent_id.is_some()
        && scope.flow_id.is_none()
        && is_flow_only(principal)
        && principal != Some(&scope.owner)
}

/// A model-only automation must not fetch or declare tools, even through a planned send.
/// Returns the offending request field so serve can refuse before admission or WAL opening.
pub fn model_only_scope_tool_field(
    send: &SendScope,
    tools: bool,
    planned_tools: bool,
) -> Option<&'static str> {
    let SendScope::Stamped(bind) = send else {
        return None;
    };
    if bind.scope.flow_id.is_none() && bind.scope.run_id.is_none() {
        return None;
    }
    if tools {
        Some("tools")
    } else if planned_tools {
        Some("plan.tool_items")
    } else {
        None
    }
}

/// The refusal for a send that does not match a scoped session's frozen identity. The body
/// names the frozen scope and what the send presented.
fn scoped_mismatch(
    frozen: &SessionScope,
    principal: Option<&BindPrincipal>,
    stamped: Option<&SessionScope>,
) -> ScopeRefusal {
    ScopeRefusal::OwnerMismatch(Box::new(OwnerMismatch::Scoped {
        frozen: frozen.clone(),
        presented_principal: principal.cloned(),
        presented_scope: stamped.cloned(),
    }))
}

/// The refusal for a [`ScopeDecision::Reopen`] send that would not open its own episode: a
/// steer joining a run (active, or paused), or a retry of a run started under the old epoch.
/// That run's tool routes belong to the epoch that ended, so the send is refused exactly as a
/// send under a different epoch was before reopening existed: `scope_owner_mismatch`, naming
/// the frozen scope. `frozen` is the session's frozen scope.
pub fn reopen_refused(
    frozen: &SessionScope,
    principal: Option<&BindPrincipal>,
    send: &SendScope,
) -> ScopeRefusal {
    let stamped = match send {
        SendScope::Stamped(bind) => Some(&bind.scope),
        _ => None,
    };
    scoped_mismatch(frozen, principal, stamped)
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::BindScope;

    fn core() -> BindPrincipal {
        BindPrincipal::Reserved {
            module_id: "prefrontal-core".into(),
        }
    }
    fn other() -> BindPrincipal {
        BindPrincipal::Reserved {
            module_id: "magic-context".into(),
        }
    }
    fn scope(owner: BindPrincipal, epoch: u64) -> SessionScope {
        SessionScope {
            owner,
            scope_ref: "head-1".into(),
            scope_epoch: epoch,
            flow_id: None,
            sender: None,
            agent_id: None,
            run_id: None,
        }
    }
    fn stamped(scope: SessionScope) -> SendScope {
        SendScope::Stamped(BindScope {
            scope,
            owner_authorized: true,
        })
    }
    fn history(episodes: &[(Option<BindPrincipal>, Option<SessionScope>)]) -> SessionOwnershipV1 {
        let mut ownership = SessionOwnershipV1::default();
        for (principal, scope) in episodes {
            ownership.fold_run_started(principal.as_ref(), scope.as_ref());
        }
        ownership
    }

    fn basal() -> BindPrincipal {
        BindPrincipal::Reserved {
            module_id: FLOW_ONLY_MODULE.into(),
        }
    }

    #[test]
    fn basal_requires_a_flow_scope() {
        let empty = SessionOwnershipV1::default();
        for send in [
            SendScope::Unstamped,
            SendScope::DaemonWithoutScopes,
            stamped(scope(core(), 3)),
        ] {
            assert_eq!(
                decide_scope(&empty, Some(&basal()), &send),
                Err(ScopeRefusal::FlowScopeRequired)
            );
        }
        let stamp = flow(Some(basal()), "flow-1");
        assert_eq!(
            decide_scope(&empty, Some(&basal()), &stamped(stamp.clone())),
            Ok(ScopeDecision::Freeze(stamp))
        );
        assert_eq!(
            decide_scope(&empty, Some(&other()), &SendScope::Unstamped),
            Ok(ScopeDecision::Unscoped)
        );
    }

    #[test]
    fn basal_access_requires_its_frozen_sender() {
        assert_eq!(
            decide_sender_access(Some(&basal()), Some(Some(&other()))),
            Err(ScopeRefusal::SenderNotOwner)
        );
        assert_eq!(
            decide_sender_access(Some(&basal()), Some(None)),
            Err(ScopeRefusal::SenderNotOwner)
        );
        assert_eq!(
            decide_sender_access(Some(&basal()), Some(Some(&basal()))),
            Ok(())
        );
        assert_eq!(decide_sender_access(Some(&basal()), None), Ok(()));
        assert_eq!(
            decide_sender_access(Some(&other()), Some(Some(&core()))),
            Ok(())
        );
    }

    #[test]
    fn a_scoped_session_accepts_only_its_owner_under_the_frozen_scope() {
        let frozen = scope(core(), 3);
        let session = history(&[(Some(core()), Some(frozen.clone()))]);
        assert_eq!(
            decide_scope(&session, Some(&core()), &stamped(frozen.clone())),
            Ok(ScopeDecision::Frozen(frozen.clone()))
        );
        for (principal, send) in [
            (Some(other()), stamped(frozen.clone())),
            (Some(core()), stamped(scope(core(), 2))),
            (Some(core()), stamped(scope(other(), 3))),
            (Some(core()), SendScope::Unstamped),
            (None, SendScope::Unstamped),
        ] {
            assert!(
                matches!(
                    decide_scope(&session, principal.as_ref(), &send),
                    Err(ScopeRefusal::OwnerMismatch(m)) if matches!(*m, OwnerMismatch::Scoped { .. })
                ),
                "{principal:?} {send:?}"
            );
        }
        assert_eq!(
            decide_scope(&session, Some(&core()), &SendScope::DaemonWithoutScopes),
            Err(ScopeRefusal::Unsupported {
                scope: frozen.into()
            })
        );
    }

    /// The specimen: the owner's scope reopened at the next epoch after idle expiry ended the
    /// first one. The same owner and ref under a later epoch reopen the session with that
    /// epoch; the frozen epoch itself is still an ordinary frozen send, never a reopen. A flow
    /// scope reopens the same way when its flow identifier and sender are unchanged.
    #[test]
    fn a_later_epoch_of_the_same_scope_reopens_the_session() {
        let session = history(&[(Some(core()), Some(scope(core(), 1)))]);
        assert_eq!(
            decide_scope(&session, Some(&core()), &stamped(scope(core(), 2))),
            Ok(ScopeDecision::Reopen(scope(core(), 2)))
        );
        assert_eq!(
            decide_scope(&session, Some(&core()), &stamped(scope(core(), 7))),
            Ok(ScopeDecision::Reopen(scope(core(), 7)))
        );
        assert_eq!(
            decide_scope(&session, Some(&core()), &stamped(scope(core(), 1))),
            Ok(ScopeDecision::Frozen(scope(core(), 1)))
        );

        let flow_scope = |epoch| SessionScope {
            scope_epoch: epoch,
            ..flow(Some(other()), "flow-1")
        };
        let flow_session = history(&[(Some(other()), Some(flow_scope(3)))]);
        assert_eq!(
            decide_scope(&flow_session, Some(&other()), &stamped(flow_scope(4))),
            Ok(ScopeDecision::Reopen(flow_scope(4)))
        );
    }

    /// Only a strictly later epoch with everything else identical reopens. An equal or lower
    /// epoch with any other difference, another owner or ref, or an added flow identifier is
    /// refused exactly as before reopening existed; a flow's changed identifier keeps its typed
    /// flow refusal.
    #[test]
    fn an_equal_or_lower_epoch_or_another_identity_is_still_refused() {
        let session = history(&[(Some(core()), Some(scope(core(), 3)))]);
        let other_ref = |epoch| SessionScope {
            scope_ref: "head-2".into(),
            ..scope(core(), epoch)
        };
        let with_flow = SessionScope {
            flow_id: Some("flow-1".into()),
            ..scope(core(), 4)
        };
        for (principal, send) in [
            (Some(core()), stamped(other_ref(3))),
            (Some(core()), stamped(other_ref(4))),
            (Some(core()), stamped(scope(core(), 2))),
            (Some(core()), stamped(scope(other(), 4))),
            (Some(other()), stamped(scope(core(), 4))),
            (None, stamped(scope(core(), 4))),
            (Some(core()), stamped(with_flow)),
        ] {
            assert!(
                matches!(
                    decide_scope(&session, principal.as_ref(), &send),
                    Err(ScopeRefusal::OwnerMismatch(m)) if matches!(*m, OwnerMismatch::Scoped { .. })
                ),
                "{principal:?} {send:?}"
            );
        }

        let flow_session = history(&[(Some(other()), Some(flow(Some(other()), "flow-1")))]);
        let later_other_flow = SessionScope {
            scope_epoch: 4,
            ..flow(Some(other()), "flow-2")
        };
        assert_eq!(
            decide_scope(&flow_session, Some(&other()), &stamped(later_other_flow)),
            Err(ScopeRefusal::FlowMismatch {
                field: FlowMismatchField::FlowId
            })
        );
    }

    /// The refusal for a reopening send that would join an old-epoch run is the scoped owner
    /// mismatch a different epoch always met, naming the frozen scope and the presented one.
    #[test]
    fn a_refused_reopen_is_the_scoped_owner_mismatch() {
        let presented = stamped(scope(core(), 4));
        assert_eq!(
            reopen_refused(&scope(core(), 3), Some(&core()), &presented),
            ScopeRefusal::OwnerMismatch(Box::new(OwnerMismatch::Scoped {
                frozen: scope(core(), 3),
                presented_principal: Some(core()),
                presented_scope: Some(scope(core(), 4)),
            }))
        );
    }

    #[test]
    fn a_new_session_freezes_the_stamp_only_for_its_owner() {
        let empty = SessionOwnershipV1::default();
        let stamp = scope(core(), 3);
        assert_eq!(
            decide_scope(&empty, Some(&core()), &stamped(stamp.clone())),
            Ok(ScopeDecision::Freeze(stamp.clone()))
        );
        assert!(matches!(
            decide_scope(&empty, Some(&other()), &stamped(stamp)),
            Err(ScopeRefusal::OwnerMismatch(m)) if matches!(*m, OwnerMismatch::NotTheOwner { .. })
        ));
    }

    #[test]
    fn adoption_needs_the_owner_on_every_earlier_episode() {
        let stamp = scope(core(), 3);
        let recorded = history(&[(Some(core()), None), (Some(core()), None)]);
        assert_eq!(
            decide_scope(&recorded, Some(&core()), &stamped(stamp.clone())),
            Ok(ScopeDecision::Freeze(stamp.clone()))
        );

        let one_unrecorded = history(&[(Some(core()), None), (None, None), (Some(core()), None)]);
        assert_eq!(
            decide_scope(&one_unrecorded, Some(&core()), &stamped(stamp.clone())),
            Err(ScopeRefusal::AdoptionRefused(
                AdoptionRefusal::NoRecordedPrincipal
            ))
        );

        let different = history(&[(Some(core()), None), (Some(other()), None)]);
        assert_eq!(
            decide_scope(&different, Some(&core()), &stamped(stamp.clone())),
            Err(ScopeRefusal::AdoptionRefused(
                AdoptionRefusal::PrincipalMismatch { recorded: other() }
            ))
        );
        // The adoption answer does not depend on who opened the route.
        assert_eq!(
            decide_scope(&one_unrecorded, Some(&other()), &stamped(stamp)),
            Err(ScopeRefusal::AdoptionRefused(
                AdoptionRefusal::NoRecordedPrincipal
            ))
        );
    }

    fn flow(sender: Option<BindPrincipal>, id: &str) -> SessionScope {
        SessionScope {
            flow_id: Some(id.into()),
            sender,
            ..scope(core(), 3)
        }
    }

    #[test]
    fn flow_carrier_freezes_sender_and_flow_id_and_refuses_changes() {
        let scope = flow(Some(other()), "flow-1");
        assert_eq!(
            decide_scope(
                &SessionOwnershipV1::default(),
                Some(&other()),
                &stamped(scope.clone())
            ),
            Ok(ScopeDecision::Freeze(scope.clone()))
        );
        let history = history(&[(Some(other()), Some(scope.clone()))]);
        assert_eq!(
            decide_scope(&history, Some(&other()), &stamped(scope.clone())),
            Ok(ScopeDecision::Frozen(scope))
        );
        assert_eq!(
            decide_scope(
                &history,
                Some(&other()),
                &stamped(flow(Some(other()), "flow-2"))
            ),
            Err(ScopeRefusal::FlowMismatch {
                field: FlowMismatchField::FlowId
            })
        );
        assert_eq!(
            decide_scope(
                &history,
                Some(&core()),
                &stamped(flow(Some(core()), "flow-1"))
            ),
            Err(ScopeRefusal::FlowMismatch {
                field: FlowMismatchField::Sender
            })
        );
    }

    #[test]
    fn flow_scope_needs_an_attested_opener_and_never_adopts_history() {
        assert!(matches!(
            decide_scope(
                &SessionOwnershipV1::default(),
                None,
                &stamped(flow(None, "flow-1"))
            ),
            Err(ScopeRefusal::OwnerMismatch(_))
        ));
        assert_eq!(
            decide_scope(
                &SessionOwnershipV1::default(),
                Some(&other()),
                &stamped(flow(None, "flow-1"))
            ),
            Ok(ScopeDecision::Freeze(flow(Some(other()), "flow-1")))
        );
        let history = history(&[(Some(core()), None)]);
        assert_eq!(
            decide_scope(
                &history,
                Some(&core()),
                &stamped(flow(Some(core()), "flow-1"))
            ),
            Err(ScopeRefusal::AdoptionRefused(AdoptionRefusal::FlowScope))
        );
    }

    #[test]
    fn an_unscoped_session_accepts_only_its_first_principal() {
        let session = history(&[(Some(core()), None)]);
        assert_eq!(
            decide_scope(&session, Some(&core()), &SendScope::Unstamped),
            Ok(ScopeDecision::Unscoped)
        );
        for principal in [Some(other()), None] {
            assert!(matches!(
                decide_scope(&session, principal.as_ref(), &SendScope::Unstamped),
                Err(ScopeRefusal::OwnerMismatch(m)) if matches!(*m, OwnerMismatch::FirstPrincipal { .. })
            ));
        }
        // A first episode that recorded no principal predates principal recording: the session
        // keeps accepting sends as it always did.
        let legacy = history(&[(None, None)]);
        assert_eq!(
            decide_scope(&legacy, Some(&other()), &SendScope::DaemonWithoutScopes),
            Ok(ScopeDecision::Unscoped)
        );
    }

    #[test]
    fn an_internal_send_carries_whatever_scope_the_session_froze() {
        let frozen = scope(core(), 3);
        let session = history(&[(Some(core()), Some(frozen.clone()))]);
        assert_eq!(
            decide_scope(&session, None, &SendScope::Internal),
            Ok(ScopeDecision::Frozen(frozen))
        );
        assert_eq!(
            decide_scope(&SessionOwnershipV1::default(), None, &SendScope::Internal),
            Ok(ScopeDecision::Unscoped)
        );
    }
}

#[cfg(test)]
mod agent_run_tests {
    use super::*;
    use broca_protocol::BindScope;

    fn basal() -> BindPrincipal {
        BindPrincipal::Reserved {
            module_id: "basal".into(),
        }
    }
    fn scope() -> SessionScope {
        serde_json::from_str(r#"{"owner":{"kind":"reserved","module_id":"prefrontal-core"},"ref":"parent-run","scope_epoch":1,"sender":{"kind":"reserved","module_id":"basal"},"agent_id":"agent-1","run_id":"parent-run"}"#).unwrap()
    }
    fn stamped(scope: SessionScope) -> SendScope {
        SendScope::Stamped(BindScope {
            scope,
            owner_authorized: true,
        })
    }
    fn frozen() -> SessionOwnershipV1 {
        let mut history = SessionOwnershipV1::default();
        history.fold_run_started(Some(&basal()), Some(&scope()));
        history
    }

    #[test]
    fn writer_freezes_and_advances_agent_run_scopes() {
        let stamp = stamped(scope());
        for (history, send, expected) in [
            (
                SessionOwnershipV1::default(),
                stamp.clone(),
                ScopeDecision::Freeze(scope()),
            ),
            (frozen(), stamp, ScopeDecision::Frozen(scope())),
            (
                frozen(),
                SendScope::Internal,
                ScopeDecision::Frozen(scope()),
            ),
        ] {
            assert_eq!(decide_scope(&history, Some(&basal()), &send), Ok(expected));
        }
    }

    #[test]
    fn enabled_agent_run_freezes_and_repeats_both_identifiers() {
        let stamp = stamped(scope());
        assert_eq!(
            decide_scope_with_writer(&SessionOwnershipV1::default(), Some(&basal()), &stamp, true),
            Ok(ScopeDecision::Freeze(scope()))
        );
        assert_eq!(
            decide_scope_with_writer(&frozen(), Some(&basal()), &stamp, true),
            Ok(ScopeDecision::Frozen(scope()))
        );
        let mut later = scope();
        later.scope_epoch = 2;
        assert_eq!(
            decide_scope_with_writer(&frozen(), Some(&basal()), &stamped(later.clone()), true),
            Ok(ScopeDecision::Reopen(later))
        );
    }

    #[test]
    fn enabled_agent_run_refuses_wrong_carrier_and_driving_owner() {
        let wrong = BindPrincipal::Reserved {
            module_id: "other".into(),
        };
        for principal in [Some(wrong), Some(scope().owner), None] {
            for history in [SessionOwnershipV1::default(), frozen()] {
                assert!(matches!(
                    decide_scope_with_writer(&history, principal.as_ref(), &stamped(scope()), true),
                    Err(ScopeRefusal::OwnerMismatch(_))
                ));
            }
        }
    }

    #[test]
    fn enabled_agent_run_requires_agent_and_forbids_flow_identity() {
        let mut missing = scope();
        missing.agent_id = None;
        assert_eq!(
            decide_scope_with_writer(
                &SessionOwnershipV1::default(),
                Some(&basal()),
                &stamped(missing),
                true
            ),
            Err(ScopeRefusal::FlowScopeRequired)
        );
        let mut mixed = scope();
        mixed.flow_id = Some("flow-1".into());
        assert!(matches!(
            decide_scope_with_writer(
                &SessionOwnershipV1::default(),
                Some(&basal()),
                &stamped(mixed),
                true
            ),
            Err(ScopeRefusal::OwnerMismatch(_))
        ));
    }

    #[test]
    fn enabled_agent_run_refuses_changed_run_id() {
        for run_id in [Some("another-run".into()), None] {
            let mut changed = scope();
            changed.run_id = run_id;
            assert_eq!(
                decide_scope_with_writer(&frozen(), Some(&basal()), &stamped(changed), true),
                Err(ScopeRefusal::RunMismatch {
                    field: RunMismatchField::RunId
                })
            );
        }
    }

    #[test]
    fn enabled_agent_run_refuses_changed_agent_id() {
        for agent_id in [Some("another-agent".into()), None] {
            let mut changed = scope();
            changed.agent_id = agent_id;
            assert_eq!(
                decide_scope_with_writer(&frozen(), Some(&basal()), &stamped(changed), true),
                Err(ScopeRefusal::RunMismatch {
                    field: RunMismatchField::AgentId
                })
            );
        }
    }

    #[test]
    fn enabled_agent_run_cannot_adopt_history_and_basal_cannot_touch_foreign_sessions() {
        let mut history = SessionOwnershipV1::default();
        history.fold_run_started(Some(&scope().owner), None);
        assert_eq!(
            decide_scope_with_writer(&history, Some(&basal()), &stamped(scope()), true),
            Err(ScopeRefusal::SenderNotOwner)
        );
        history = SessionOwnershipV1::default();
        history.fold_run_started(Some(&basal()), None);
        assert!(matches!(
            decide_scope_with_writer(&history, Some(&basal()), &stamped(scope()), true),
            Err(ScopeRefusal::OwnerMismatch(_))
        ));
        assert_eq!(
            decide_sender_access(Some(&basal()), Some(Some(&scope().owner))),
            Err(ScopeRefusal::SenderNotOwner)
        );
    }

    #[test]
    fn agent_run_scopes_refuse_declared_and_planned_tools() {
        let stamp = stamped(scope());
        assert_eq!(
            model_only_scope_tool_field(&stamp, true, false),
            Some("tools")
        );
        assert_eq!(
            model_only_scope_tool_field(&stamp, false, true),
            Some("plan.tool_items")
        );
        assert_eq!(model_only_scope_tool_field(&stamp, false, false), None);
    }
}

#[cfg(test)]
mod owner_access_tests {
    use super::*;

    fn reserved(id: &str) -> BindPrincipal {
        BindPrincipal::Reserved {
            module_id: id.into(),
        }
    }

    fn protected() -> SessionOwnershipV1 {
        let mut ownership = SessionOwnershipV1::default();
        ownership.fold_run_started(
            Some(&reserved("basal")),
            Some(&SessionScope {
                owner: reserved("prefrontal-core"),
                scope_ref: "work".into(),
                scope_epoch: 7,
                flow_id: Some("flow".into()),
                sender: Some(reserved("basal")),
                agent_id: None,
                run_id: None,
            }),
        );
        ownership
    }

    #[test]
    fn foreign_principal_is_refused_with_accepted_identity() {
        let ownership = protected();
        let foreign = reserved("operator");
        let Err(ScopeRefusal::OwnerMismatch(refusal)) =
            decide_owner_access(&ownership, Some(&foreign), &SendScope::Unstamped)
        else {
            panic!("foreign principal must be refused")
        };
        let OwnerMismatch::OwnerOperation { first, mismatch } = *refusal else {
            panic!("refusal must name both owner principals")
        };
        assert_eq!(first, reserved("basal"));
        assert!(matches!(
            *mismatch, OwnerMismatch::Scoped { frozen, presented_principal, .. }
                if frozen.owner == reserved("prefrontal-core")
                    && presented_principal.as_ref() == Some(&foreign)
        ));
        assert!(matches!(
            decide_owner_access(&ownership, None, &SendScope::Unstamped),
            Err(ScopeRefusal::OwnerMismatch(_))
        ));
    }

    #[test]
    fn scope_owner_is_admitted_on_unscoped_route_and_any_epoch() {
        let ownership = protected();
        let owner = reserved("prefrontal-core");
        assert_eq!(
            decide_owner_access(&ownership, Some(&owner), &SendScope::Unstamped),
            Ok(())
        );
        for epoch in [1, 7, 9] {
            let mut scope = ownership.scope.clone().unwrap();
            scope.scope_epoch = epoch;
            let send = SendScope::Stamped(broca_protocol::BindScope {
                scope,
                owner_authorized: true,
            });
            assert_eq!(decide_owner_access(&ownership, Some(&owner), &send), Ok(()));
        }
    }

    #[test]
    fn direct_is_never_an_owner_of_a_protected_session() {
        assert!(matches!(
            decide_owner_access(
                &protected(),
                Some(&BindPrincipal::Direct),
                &SendScope::Unstamped
            ),
            Err(ScopeRefusal::OwnerMismatch(_))
        ));
    }

    #[test]
    fn frozen_carrier_and_unscoped_first_principal_are_owners() {
        let mut ownership = protected();
        assert_eq!(
            decide_owner_access(&ownership, Some(&reserved("basal")), &SendScope::Unstamped),
            Ok(())
        );
        ownership.scope = None;
        assert_eq!(
            decide_owner_access(&ownership, Some(&reserved("basal")), &SendScope::Unstamped),
            Ok(())
        );
        let Err(ScopeRefusal::OwnerMismatch(refusal)) = decide_owner_access(
            &ownership,
            Some(&reserved("prefrontal-core")),
            &SendScope::Unstamped,
        ) else {
            panic!("a foreign unscoped caller must be refused")
        };
        let OwnerMismatch::OwnerOperation { mismatch, .. } = *refusal else {
            panic!("owner-only refusal")
        };
        assert!(
            matches!(*mismatch, OwnerMismatch::FirstPrincipal { first, .. } if first == reserved("basal"))
        );
    }

    #[test]
    fn histories_without_an_attested_first_sender_remain_unprotected() {
        for first in [
            None,
            Some(BindPrincipal::Direct),
            Some(BindPrincipal::Unverified),
            Some(BindPrincipal::Unknown(serde_json::json!({"kind":"future"}))),
        ] {
            let mut ownership = SessionOwnershipV1::default();
            ownership.fold_run_started(first.as_ref(), None);
            // A later verified episode cannot retroactively protect earlier anonymous work.
            ownership.fold_run_started(Some(&reserved("prefrontal-core")), None);
            for caller in [
                None,
                Some(reserved("basal")),
                Some(reserved("operator")),
                Some(BindPrincipal::Direct),
            ] {
                assert_eq!(
                    decide_owner_access(&ownership, caller.as_ref(), &SendScope::Unstamped),
                    Ok(())
                );
            }
        }
    }
}
