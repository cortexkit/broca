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
//! - A session with no episode yet, whose storage address a scoped `route.bind` reserved,
//!   accepts its first send only over a route stamped with the reserving scope's owner and
//!   `ref`, at any epoch (see [`check_scope_reservation`]). Addresses reserved without a stamp,
//!   or before reservations were recorded, keep the rules above.
//!
//! Admission compares scope identity. New flow sessions and agent-run sessions freeze
//! `agent_id`; historical flow sessions and ordinary agent scopes keep their earlier rules.

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

/// Owner operations accept plain or unscoped owner routes, including cleanup after the
/// original scope ended. Flow- and run-stamped routes carry senders, never owner authority.
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
    let sender_route = matches!(send, SendScope::Stamped(bind)
        if bind.scope.flow_id.is_some() || bind.scope.run_id.is_some());
    if is_owner && !sender_route {
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

/// Whether broca declares llm-runner's `notice_with_speaks_for` group: a send may combine
/// `notice: true` with `speaks_for`, and a speaker may carry a display `name`. `role.describe`
/// and send admission both read this, so admission accepts exactly what describe declares.
pub const NOTICE_WITH_SPEAKS_FOR: bool = true;

/// Validate display attribution against the frozen owner before any session write.
pub fn admit_message_marks(
    params: &serde_json::Map<String, serde_json::Value>,
    ownership: &SessionOwnershipV1,
    principal: Option<&BindPrincipal>,
    send: &SendScope,
) -> Result<Option<broca_protocol::MessageMarks>, &'static str> {
    admit_message_marks_declaring(params, ownership, principal, send, NOTICE_WITH_SPEAKS_FOR)
}

/// [`admit_message_marks`] for a runner that declares `notice_with_speaks_for` or not. Without
/// it, a notice combined with a speaker refuses naming `notice`, and a speaker's name refuses
/// naming `speaks_for.name`.
pub fn admit_message_marks_declaring(
    params: &serde_json::Map<String, serde_json::Value>,
    ownership: &SessionOwnershipV1,
    principal: Option<&BindPrincipal>,
    send: &SendScope,
    notice_with_speaks_for: bool,
) -> Result<Option<broca_protocol::MessageMarks>, &'static str> {
    let automated = matches!(send, SendScope::Stamped(bind)
        if bind.scope.flow_id.is_some() || bind.scope.run_id.is_some());
    let scope = ownership.scope.as_ref().or(match send {
        SendScope::Stamped(bind) if ownership.episodes == 0 => Some(&bind.scope),
        _ => None,
    });
    // Canonical spellings, so a module's two ids across a rename compare as one owner.
    let owner = scope
        .filter(|_| !automated)
        .map(|scope| scope.owner.canonical_owner_string());
    let sender = principal
        .map(BindPrincipal::canonical_owner_string)
        .unwrap_or_default();
    let marks = cortexkit_role_llm_runner::message_marks::check_with_notice_speaks_for(
        params,
        true,
        notice_with_speaks_for,
        owner.as_deref(),
        &sender,
    )?;
    Ok((marks.speaks_for.is_some() || marks.notice).then_some(marks))
}

/// The scope a stamped `route.bind` carried when it reserved a new session address: its owner
/// and `ref`, without its epoch.
///
/// Until the session's first send is admitted its history names no owner, so the reservation
/// says whose session it is. It is recorded once, by the session inventory, in the transaction
/// that reserves the address, and only for a stamped bind; an unstamped reservation and every
/// address reserved before reservations were recorded have none, and keep the earlier rules.
/// The epoch is left out on purpose: the owner can start its scope again at another epoch
/// between the bind that reserved the address and its first send (core does when an agent's
/// head scope moves to a new residence), and the owner and ref still name the same scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopeReservation {
    /// The scope's owner, compared with a stamp's owner.
    pub owner: BindPrincipal,
    /// The scope's `ref`, compared with a stamp's `ref`.
    pub scope_ref: String,
}

/// Refuse a caller's request that would become the first principal of a session whose storage
/// address a scoped `route.bind` reserved for another scope.
///
/// Applies only while the session has no episode and `ownership.reservation` is set. The
/// request must then arrive over a route stamped with a scope of the reserved owner and `ref`,
/// at any epoch; the opener may still be a carrier the daemon admitted under that scope, which
/// the rest of admission judges as it always has. A request without a stamp, or stamped with
/// another owner's scope or another of the owner's refs, is refused as a send from someone other
/// than the session's owner, naming the reserved owner. An `Internal` request is not a caller's
/// and is never refused here. Once an episode exists, its recorded principal and scope decide
/// instead.
///
/// `session.send` meets this through [`decide_scope`]. Paths that establish ownership
/// without scope admission (an inline `session.import`) and the stamped `route.bind` precheck
/// call it directly.
pub fn check_scope_reservation(
    ownership: &SessionOwnershipV1,
    principal: Option<&BindPrincipal>,
    send: &SendScope,
) -> Result<(), ScopeRefusal> {
    let stamp = match send {
        SendScope::Internal => return Ok(()),
        SendScope::DaemonWithoutScopes | SendScope::Unstamped => None,
        SendScope::Stamped(bind) => Some(&bind.scope),
    };
    check_reserved_scope(ownership, principal, stamp)
}

/// [`check_scope_reservation`] for a caller's request that arrived over a route stamped with
/// `stamp`, or over an unstamped route when `None`. A file-source `session.import`, which keeps
/// only its route's scope, meets the reservation through this.
pub fn check_reserved_scope(
    ownership: &SessionOwnershipV1,
    principal: Option<&BindPrincipal>,
    stamp: Option<&SessionScope>,
) -> Result<(), ScopeRefusal> {
    let Some(reserved) = ownership
        .reservation
        .as_ref()
        .filter(|_| ownership.episodes == 0)
    else {
        return Ok(());
    };
    if stamp
        .is_some_and(|stamp| stamp.owner == reserved.owner && stamp.scope_ref == reserved.scope_ref)
    {
        return Ok(());
    }
    Err(ScopeRefusal::OwnerMismatch(Box::new(
        OwnerMismatch::Reserved {
            owner: reserved.owner.clone(),
            presented_principal: principal.cloned(),
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
    /// The episode would run unscoped while its plan asks a tool provider to confine its calls
    /// (`broca_fetch_plan::CONFINED_TOOL_PRESET`); confinement is keyed on the route's scope.
    ScopeRequired { preset: &'static str },
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
    /// A session with no episode yet whose storage address a scoped bind reserved for `owner`:
    /// only a send stamped with that owner's reserving scope (same `ref`, any epoch) may start
    /// it. On the wire it is the `first_principal` detail with `owner` as `first`, under a
    /// message of its own, because no one has started the session yet.
    Reserved {
        owner: BindPrincipal,
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
    check_scope_reservation(ownership, principal, send)?;

    // The discriminator describes stored policy, not an attested scope attribute. Historical
    // flow sessions discarded agent identity, so normalize only their comparison copy.
    let normalized = stamped.cloned().map(|mut scope| {
        if let Some(frozen) = &ownership.scope {
            scope.agent_id_frozen = frozen.agent_id_frozen;
            if frozen.flow_id.is_some() && !frozen.agent_id_frozen {
                scope.agent_id = frozen.agent_id.clone();
            }
        }
        scope
    });
    let stamped = normalized.as_ref();
    if let Some(frozen) = &ownership.scope {
        check_frozen_sender(frozen, principal, stamped)?;
    }

    // Frozen identities take precedence over the principal-specific work-scope checks.
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
        if frozen.flow_id.is_some() && stamped == Some(frozen) && principal.is_some() {
            return Ok(ScopeDecision::Frozen(frozen.clone()));
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
    if frozen.flow_id.is_some() {
        frozen.agent_id_frozen = true;
    }
    Ok(ScopeDecision::Freeze(frozen))
}

fn check_frozen_sender(
    frozen: &SessionScope,
    principal: Option<&BindPrincipal>,
    stamped: Option<&SessionScope>,
) -> Result<(), ScopeRefusal> {
    use cortexkit_role_llm_runner::scope_admission::{check_frozen, FrozenSender, ScopeKind};
    let Some(sender) = frozen
        .sender
        .as_ref()
        .filter(|_| frozen.flow_id.is_some() || frozen.run_id.is_some())
    else {
        return Ok(());
    };
    fn kind(scope: &SessionScope) -> ScopeKind<'_> {
        if let Some(flow_id) = scope.flow_id.as_deref() {
            ScopeKind::Flow { flow_id }
        } else if let Some(run_id) = scope.run_id.as_deref() {
            ScopeKind::Run { run_id }
        } else {
            ScopeKind::Plain
        }
    }
    // Canonical spellings, so a module's two ids across a rename compare as one sender.
    let frozen_sender = FrozenSender::new(
        &sender.canonical_owner_string(),
        kind(frozen),
        frozen.agent_id.as_deref(),
    );
    let result = check_frozen(
        &frozen_sender,
        &principal
            .map(BindPrincipal::canonical_owner_string)
            .unwrap_or_default(),
        stamped.map(kind).unwrap_or(ScopeKind::Plain),
        stamped.and_then(|scope| scope.agent_id.as_deref()),
    );
    result.map_err(|(_, field)| match field {
        Some("sender") => ScopeRefusal::FlowMismatch {
            field: FlowMismatchField::Sender,
        },
        Some("flow_id") => ScopeRefusal::FlowMismatch {
            field: FlowMismatchField::FlowId,
        },
        Some("run_id") => ScopeRefusal::RunMismatch {
            field: RunMismatchField::RunId,
        },
        Some("agent_id") => ScopeRefusal::RunMismatch {
            field: RunMismatchField::AgentId,
        },
        _ => unreachable!("scope helper returned an unknown mismatch field"),
    })
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
            agent_id_frozen: false,
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
            kind: broca_protocol::ScopeKind::Head,
            parent: None,
            parent_state: None,
        })
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

    fn flow(sender: Option<BindPrincipal>, id: &str) -> SessionScope {
        SessionScope {
            agent_id_frozen: true,
            flow_id: Some(id.into()),
            sender,
            ..scope(core(), 3)
        }
    }

    /// The ownership of an empty session whose address a scoped bind of `owner` reserved under
    /// `scope_ref`.
    fn reserved_for(owner: BindPrincipal, scope_ref: &str) -> SessionOwnershipV1 {
        SessionOwnershipV1 {
            reservation: Some(ScopeReservation {
                owner,
                scope_ref: scope_ref.into(),
            }),
            ..SessionOwnershipV1::default()
        }
    }

    fn reservation_refusal(
        owner: BindPrincipal,
        presented: Option<BindPrincipal>,
    ) -> Result<ScopeDecision, ScopeRefusal> {
        Err(ScopeRefusal::OwnerMismatch(Box::new(
            OwnerMismatch::Reserved {
                owner,
                presented_principal: presented,
            },
        )))
    }

    /// A scoped bind reserved the address for core's scope `head-1`. Core's first send under
    /// that stamp freezes it exactly as on an unreserved session. So does a carrier the daemon
    /// admitted under core's flow scope: the carrier's stamp names the owner's flow scope, and
    /// that scope's owner and ref are the ones its own bind reserved (here `head-1`, the ref
    /// `flow` builds on). Another module stamping its own scope cannot become the session's first
    /// principal: the refusal names the reserving owner.
    #[test]
    fn a_reserved_session_admits_a_first_send_only_under_its_owners_scope() {
        let stamp = scope(core(), 3);
        assert_eq!(
            decide_scope(
                &reserved_for(core(), "head-1"),
                Some(&core()),
                &stamped(stamp.clone())
            ),
            Ok(ScopeDecision::Freeze(stamp))
        );
        assert_eq!(
            decide_scope(
                &reserved_for(core(), "head-1"),
                Some(&other()),
                &stamped(flow(None, "flow-1"))
            ),
            Ok(ScopeDecision::Freeze(flow(Some(other()), "flow-1")))
        );
        assert_eq!(
            decide_scope(
                &reserved_for(core(), "head-1"),
                Some(&other()),
                &stamped(scope(other(), 1))
            ),
            reservation_refusal(core(), Some(other()))
        );
        // A send made inside broca is not a caller's and takes no reservation check.
        assert_eq!(
            decide_scope(&reserved_for(core(), "head-1"), None, &SendScope::Internal),
            Ok(ScopeDecision::Unscoped)
        );
    }

    /// The reservation names the owner's scope by owner and ref, not by epoch: a head's epoch
    /// can move between the bind that reserved its session and its first send. A higher or a
    /// lower epoch of the same scope is admitted and freezes the send's own stamp, epoch
    /// included. Another of the owner's refs is refused like any other stranger.
    #[test]
    fn a_reservation_matches_owner_and_ref_at_any_epoch() {
        let reserved = reserved_for(core(), "head-1");
        for epoch in [4, 2] {
            let stamp = scope(core(), epoch);
            assert_eq!(
                decide_scope(&reserved, Some(&core()), &stamped(stamp.clone())),
                Ok(ScopeDecision::Freeze(stamp)),
                "epoch {epoch}"
            );
        }
        let other_ref = SessionScope {
            scope_ref: "head-2".into(),
            ..scope(core(), 3)
        };
        assert_eq!(
            decide_scope(&reserved, Some(&core()), &stamped(other_ref.clone())),
            reservation_refusal(core(), Some(core()))
        );
        assert_eq!(
            check_scope_reservation(&reserved, Some(&core()), &stamped(other_ref)),
            Err(ScopeRefusal::OwnerMismatch(Box::new(
                OwnerMismatch::Reserved {
                    owner: core(),
                    presented_principal: Some(core()),
                }
            )))
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
            kind: broca_protocol::ScopeKind::Head,
            parent: None,
            parent_state: None,
        })
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
