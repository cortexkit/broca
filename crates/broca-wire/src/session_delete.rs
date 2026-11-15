//! `session.delete`: deletes one session kept in the new storage, for the operator.
//!
//! For sessions nobody uses any more, such as an import that will be redone or a trial copy.
//! The session must hold no work a deletion would lose: no run active or paused, no queued send,
//! and no spend still to export. It is deleted as retention deletes an expired session: reads of
//! its runs answer expired afterwards, and a session created at the same address later continues
//! its counters. A session an actor holds open is refused until the actor closes idle.
//!
//! Operator only, and outside the runner role.

use serde::{Deserialize, Serialize};

pub const OP_SESSION_DELETE: &str = "session.delete";

/// `session.delete` request parameters: none, since the route names the session. An unknown
/// field is refused.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteParams {}

/// `session.delete` reply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteResponse {
    /// Whether this call deleted the session; `false` when no session was stored there.
    pub deleted: bool,
}
