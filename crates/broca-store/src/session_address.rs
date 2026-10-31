//! Resolve route claims through the durable identity inventory before opening a session.

use std::path::Path;

use broca_protocol::BindIdentity;
use rusqlite::TransactionBehavior;

use crate::{session_refs, SqliteStore};

#[derive(Debug, PartialEq, Eq)]
pub enum SessionAddressError {
    Ambiguous,
    Invalid(String),
    Unavailable(String),
}

impl SessionAddressError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Ambiguous => "session_address_ambiguous",
            Self::Invalid(_) => "invalid_bind_identity",
            Self::Unavailable(_) => "session_address_unavailable",
        }
    }
}

impl std::fmt::Display for SessionAddressError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ambiguous => f.write_str("the session-ref inventory records more than one root"),
            Self::Invalid(reason) => f.write_str(reason),
            Self::Unavailable(reason) => write!(f, "session-ref inventory unavailable: {reason}"),
        }
    }
}

impl std::error::Error for SessionAddressError {}

// Outgoing tool binds still require a real folder: a relative namespace would be
// resolved against the daemon's cwd. Switch this branch to
// broca_protocol::session::UNSCOPED_SESSION_ROOT with subc-protocol 0.34.0 adoption,
// when outgoing binds can express no folder without sending a pretend path.
fn new_unscoped_storage_root(bind: &BindIdentity) -> &Path {
    &bind.project_root
}

impl SqliteStore {
    /// Reserve the first root for a pair, or recover its original root. The SQLite
    /// writer transaction serializes competing first binds across connections and
    /// processes; a candidate can survive an interrupted bind without WAL bytes.
    pub fn resolve_session_address(
        &self,
        bind: &BindIdentity,
        stamped_workdir: Option<&Path>,
    ) -> Result<BindIdentity, SessionAddressError> {
        let unavailable = |error: String| SessionAddressError::Unavailable(error);
        let mut conn = self
            .conn
            .lock()
            .map_err(|error| unavailable(error.to_string()))?;
        let root = conn
            .path()
            .and_then(|path| Path::new(path).parent())
            .map(Path::to_path_buf);
        let gap = || {
            session_refs::projection_gap(root.as_deref())
                .map_err(|error| unavailable(error.to_string()))
        };
        if gap()? {
            return Err(unavailable(
                "session-ref projection has an incomplete marker".into(),
            ));
        }
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| unavailable(error.to_string()))?;
        if !session_refs::projection_complete(&tx)
            .map_err(|error| unavailable(error.to_string()))?
        {
            return Err(unavailable("session-ref inventory is incomplete".into()));
        }
        let candidates = {
            let mut query = tx
                .prepare(
                    "SELECT DISTINCT s.project_root FROM agent_data_session_ref_v2 v
                 JOIN agent_data_session_ref s ON s.address=v.address
                 WHERE v.session_ref=?1 AND s.harness=?2 AND s.session=?3 LIMIT 2",
                )
                .map_err(|error| unavailable(error.to_string()))?;
            let entries = query
                .query_map(
                    rusqlite::params![
                        broca_core::agent_session_ref(bind),
                        bind.harness,
                        bind.session
                    ],
                    |row| row.get::<_, String>(0),
                )
                .map_err(|error| unavailable(error.to_string()))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| unavailable(error.to_string()))?;
            entries
        };
        if candidates.len() > 1 {
            return Err(SessionAddressError::Ambiguous);
        }
        let resolved = match candidates.first() {
            Some(root) => BindIdentity::new(root, &bind.harness, &bind.session),
            None => BindIdentity::new(
                stamped_workdir.unwrap_or_else(|| new_unscoped_storage_root(bind)),
                &bind.harness,
                &bind.session,
            ),
        };
        resolved
            .validate_keyable()
            .map_err(SessionAddressError::Invalid)?;
        if gap()? {
            return Err(unavailable(
                "session-ref projection changed during address resolution".into(),
            ));
        }
        if candidates.is_empty() {
            session_refs::insert(&tx, &broca_core::session_addr(&resolved), &resolved)
                .map_err(|error| unavailable(error.to_string()))?;
        }
        tx.commit()
            .map_err(|error| unavailable(error.to_string()))?;
        if gap()? {
            return Err(unavailable(
                "session-ref projection changed while reserving the address".into(),
            ));
        }
        Ok(resolved)
    }
}

#[cfg(test)]
mod tests;
