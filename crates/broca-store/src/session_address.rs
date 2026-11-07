//! Resolve route claims through the durable identity inventory before opening a session.

use std::path::Path;

use broca_protocol::{BindIdentity, BindPrincipal};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior};

use crate::{session_refs, SqliteStore};

/// The scope a stamped `route.bind` carried when it reserved a new session address.
///
/// Until the session's first send is admitted its history names no owner, so the reservation
/// is what says whose session it is: admission accepts a first send only over a route stamped
/// with a scope `owner` owns. Recorded once, in the transaction that reserves the address, and
/// only for a stamped bind; an unstamped reservation and every address reserved before this
/// record existed have none, and keep the earlier admission rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopeReservation {
    /// The scope's owner, as admission compares it against a stamp's owner.
    pub owner: BindPrincipal,
    /// The scope's `ref` at reservation time, kept for diagnosis.
    pub scope_ref: String,
}

/// One row per address a stamped bind reserved. The owner is stored as the principal's JSON,
/// which decodes back to the same `BindPrincipal`, including a kind this build does not know.
/// Shared by the legacy run index and the broca-state/2 inventory.
pub(crate) const SCOPE_RESERVATION_SCHEMA: &str =
    "CREATE TABLE IF NOT EXISTS session_scope_reservation (
    address TEXT PRIMARY KEY, owner TEXT NOT NULL, scope_ref TEXT NOT NULL
) WITHOUT ROWID";

/// Record `reservation` for `address`, inside the caller's reserving transaction. The caller
/// writes it only when the inventory had no root for the pair, so any row already at the
/// address belongs to an inventory entry that no longer exists and is replaced.
pub(crate) fn record_scope_reservation(
    conn: &Connection,
    address: &str,
    reservation: &ScopeReservation,
) -> Result<(), String> {
    let owner = serde_json::to_string(&reservation.owner).map_err(|error| error.to_string())?;
    conn.execute(
        "INSERT INTO session_scope_reservation VALUES(?1,?2,?3)
         ON CONFLICT(address) DO UPDATE SET owner=excluded.owner, scope_ref=excluded.scope_ref",
        rusqlite::params![address, owner, reservation.scope_ref],
    )
    .map_err(|error| error.to_string())?;
    Ok(())
}

/// The reservation recorded for `address`, if any. An owner that does not decode is an error,
/// never an absent reservation: admission must not open a reserved session to anyone because
/// its row could not be read.
pub(crate) fn read_scope_reservation(
    conn: &Connection,
    address: &str,
) -> Result<Option<ScopeReservation>, String> {
    let row = conn
        .query_row(
            "SELECT owner, scope_ref FROM session_scope_reservation WHERE address=?1",
            [address],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(|error| error.to_string())?;
    row.map(|(owner, scope_ref)| {
        let owner = serde_json::from_str(&owner)
            .map_err(|error| format!("scope reservation owner does not decode: {error}"))?;
        Ok(ScopeReservation { owner, scope_ref })
    })
    .transpose()
}

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

// This namespace is only a durable storage key, never an outgoing route folder.
fn new_unscoped_storage_root() -> &'static Path {
    Path::new(broca_protocol::session::UNSCOPED_SESSION_ROOT)
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
    ///
    /// `reservation` is the stamp of a scoped bind. It is recorded only when this call
    /// reserves a new pair, in the same transaction; resolving an existing pair writes nothing.
    pub fn resolve_session_address(
        &self,
        bind: &BindIdentity,
        stamped_workdir: Option<&Path>,
        reservation: Option<&ScopeReservation>,
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
                    stamped_workdir.unwrap_or_else(|| new_unscoped_storage_root()),
                    &bind.harness,
                    &bind.session,
                );
                identity
                    .validate_keyable()
                    .map_err(SessionAddressError::Invalid)?;
                require_no_gap(root.as_deref())?;
                let address = broca_core::session_addr(&identity);
                session_refs::insert(&tx, &address, &identity)
                    .map_err(SessionAddressError::store_error)?;
                if let Some(reservation) = reservation {
                    record_scope_reservation(&tx, &address, reservation)
                        .map_err(SessionAddressError::store_error)?;
                }
                identity
            }
        };
        require_no_gap(root.as_deref())?;
        tx.commit().map_err(SessionAddressError::store_error)?;
        require_no_gap(root.as_deref())?;
        Ok(resolved)
    }

    /// The scope reservation recorded for `session`'s address, if a stamped bind reserved it.
    pub fn scope_reservation(
        &self,
        session: &BindIdentity,
    ) -> Result<Option<ScopeReservation>, SessionAddressError> {
        let conn = self.conn.lock().map_err(SessionAddressError::store_error)?;
        read_scope_reservation(&conn, &broca_core::session_addr(session))
            .map_err(SessionAddressError::store_error)
    }
}

#[cfg(test)]
mod tests;
