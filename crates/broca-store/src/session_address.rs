//! Resolve route claims through the durable identity inventory before opening a session.

use std::path::Path;

use broca_protocol::BindIdentity;
use rusqlite::{Connection, TransactionBehavior};

use crate::{session_refs, SqliteStore};

#[derive(Debug, PartialEq, Eq)]
pub enum SessionAddressError {
    Ambiguous,
    Invalid(String),
    Unavailable(InventoryUnavailable),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InventoryUnavailable {
    IncompleteInventory,
    GapMarker,
    StoreError(String),
    WaitExpired,
    MigrationFailed(String),
}

impl InventoryUnavailable {
    pub fn reason(&self) -> &'static str {
        match self {
            Self::IncompleteInventory => "incomplete_inventory",
            Self::GapMarker => "gap_marker",
            Self::StoreError(_) => "store_error",
            Self::WaitExpired => "wait_expired",
            Self::MigrationFailed(_) => "migration_failed",
        }
    }
}

impl SessionAddressError {
    pub fn store_error(error: impl std::fmt::Display) -> Self {
        Self::Unavailable(InventoryUnavailable::StoreError(error.to_string()))
    }
    pub fn code(&self) -> &'static str {
        match self {
            Self::Ambiguous => "session_address_ambiguous",
            Self::Invalid(_) => "invalid_bind_identity",
            Self::Unavailable(_) => "session_address_unavailable",
        }
    }
}

impl std::fmt::Display for InventoryUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IncompleteInventory => f.write_str("incomplete inventory"),
            Self::GapMarker => f.write_str("projection gap marker is present"),
            Self::StoreError(message) => write!(f, "store error: {message}"),
            Self::WaitExpired => f.write_str("inventory readiness wait expired"),
            Self::MigrationFailed(message) => write!(f, "inventory migration failed: {message}"),
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

fn require_no_gap(root: Option<&Path>) -> Result<(), SessionAddressError> {
    if session_refs::projection_gap(root).map_err(SessionAddressError::store_error)? {
        return Err(SessionAddressError::Unavailable(
            InventoryUnavailable::GapMarker,
        ));
    }
    Ok(())
}

fn require_complete(conn: &Connection, root: Option<&Path>) -> Result<(), SessionAddressError> {
    require_no_gap(root)?;
    if !session_refs::projection_complete(conn).map_err(SessionAddressError::store_error)? {
        return Err(SessionAddressError::Unavailable(
            InventoryUnavailable::IncompleteInventory,
        ));
    }
    Ok(())
}

fn known_identity(
    conn: &Connection,
    bind: &BindIdentity,
) -> Result<Option<BindIdentity>, SessionAddressError> {
    let mut query = conn
        .prepare(
            "SELECT DISTINCT s.project_root FROM agent_data_session_ref_v2 v
         JOIN agent_data_session_ref s ON s.address=v.address
         WHERE v.session_ref=?1 AND s.harness=?2 AND s.session=?3 LIMIT 2",
        )
        .map_err(SessionAddressError::store_error)?;
    let roots = query
        .query_map(
            rusqlite::params![
                broca_core::agent_session_ref(bind),
                bind.harness,
                bind.session
            ],
            |row| row.get::<_, String>(0),
        )
        .map_err(SessionAddressError::store_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(SessionAddressError::store_error)?;
    if roots.len() > 1 {
        return Err(SessionAddressError::Ambiguous);
    }
    roots
        .first()
        .map(|root| {
            let identity = BindIdentity::new(root, &bind.harness, &bind.session);
            identity
                .validate_keyable()
                .map_err(SessionAddressError::Invalid)?;
            Ok(identity)
        })
        .transpose()
}

fn read_existing(
    conn: &mut Connection,
    root: Option<&Path>,
    bind: &BindIdentity,
) -> Result<Option<BindIdentity>, SessionAddressError> {
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .map_err(SessionAddressError::store_error)?;
    let found = known_identity(&tx, bind)?;
    // A recorded pair is positive evidence even while discovery is being
    // repaired. Only absence needs a complete inventory before it can be used.
    if found.is_none() {
        require_complete(&tx, root)?;
    }
    tx.commit().map_err(SessionAddressError::store_error)?;
    if found.is_none() {
        require_no_gap(root)?;
    }
    Ok(found)
}

impl SqliteStore {
    /// Existing pairs use a read snapshot, without competing for SQLite's writer
    /// lock. Only an absent pair takes a writer transaction and rechecks discovery
    /// before reserving its first root across connections and processes.
    pub fn resolve_session_address(
        &self,
        bind: &BindIdentity,
        stamped_workdir: Option<&Path>,
    ) -> Result<BindIdentity, SessionAddressError> {
        let mut conn = self.conn.lock().map_err(SessionAddressError::store_error)?;
        let root = conn
            .path()
            .and_then(|path| Path::new(path).parent())
            .map(Path::to_path_buf);
        if let Some(existing) = read_existing(&mut conn, root.as_deref(), bind)? {
            return Ok(existing);
        }
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(SessionAddressError::store_error)?;
        let existing = known_identity(&tx, bind)?;
        let resolved = match existing {
            Some(identity) => {
                tx.commit().map_err(SessionAddressError::store_error)?;
                return Ok(identity);
            }
            None => {
                require_complete(&tx, root.as_deref())?;
                let identity = BindIdentity::new(
                    stamped_workdir.unwrap_or_else(|| new_unscoped_storage_root(bind)),
                    &bind.harness,
                    &bind.session,
                );
                identity
                    .validate_keyable()
                    .map_err(SessionAddressError::Invalid)?;
                require_no_gap(root.as_deref())?;
                session_refs::insert(&tx, &broca_core::session_addr(&identity), &identity)
                    .map_err(SessionAddressError::store_error)?;
                identity
            }
        };
        require_no_gap(root.as_deref())?;
        tx.commit().map_err(SessionAddressError::store_error)?;
        require_no_gap(root.as_deref())?;
        Ok(resolved)
    }
}

#[cfg(test)]
mod tests;
