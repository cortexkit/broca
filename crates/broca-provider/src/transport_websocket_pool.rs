//! An executor's in-memory, full-bind connection owner. Only sockets cross run boundaries.

use super::*;
use broca_protocol::BindIdentity;
use std::{collections::HashMap, sync::Weak};

const MAX_RETAINED: usize = 64;
const MAX_IDLE: Duration = Duration::from_secs(10 * 60);
const MAX_RECEIPT_BYTES: usize = 64 * 1024 * 1024;

struct Entries {
    sessions: HashMap<BindIdentity, Entry>,
    next_lease: u64,
    max_receipt_bytes: usize,
}

impl Default for Entries {
    fn default() -> Self {
        Self {
            sessions: HashMap::new(),
            next_lease: 0,
            max_receipt_bytes: MAX_RECEIPT_BYTES,
        }
    }
}

struct Entry {
    lease: u64,
    active: Weak<Session>,
    retained: Option<Retained>,
}

struct Retained {
    socket: Arc<Socket>,
    receipt_bytes: usize,
    since: Instant,
    expiry: tokio::task::JoinHandle<()>,
}

impl Drop for Retained {
    fn drop(&mut self) {
        self.expiry.abort();
    }
}

/// Bounded, non-durable ChatGPT connections owned by one shared provider HTTP client.
/// Clones share ownership; independent clients (including after restart) share nothing.
#[derive(Clone, Default)]
pub struct Connections(Arc<Mutex<Entries>>);

impl std::fmt::Debug for Connections {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChatgptConnections").finish_non_exhaustive()
    }
}

pub(in crate::transport) struct Lease {
    owner: Connections,
    bind: BindIdentity,
    id: u64,
}

impl Connections {
    #[cfg(test)]
    pub(in crate::transport) fn with_byte_budget(max_receipt_bytes: usize) -> Self {
        Self(Arc::new(Mutex::new(Entries {
            max_receipt_bytes,
            ..Entries::default()
        })))
    }

    pub(in crate::transport) fn acquire(
        &self,
        bind: &BindIdentity,
        session: &Arc<Session>,
    ) -> Option<Lease> {
        let mut entries = self.0.lock().unwrap();
        if entries
            .sessions
            .get(bind)
            .is_some_and(|entry| entry.active.upgrade().is_some())
        {
            // The actor and WAL lease normally prevent this. A concurrent caller gets a
            // private connection, never the socket another run is still consuming.
            return None;
        }
        let retained = entries
            .sessions
            .remove(bind)
            .and_then(|entry| entry.retained);
        if let Some(retained) = retained {
            let now = session.now();
            if now.saturating_duration_since(retained.since) < MAX_IDLE
                && now.saturating_duration_since(retained.socket.established_at) < MAX_SOCKET_AGE
                && !retained.socket.closed.load(Ordering::Acquire)
                && !retained.socket.shutdown.is_cancelled()
            {
                let mut state = session.state.lock().unwrap();
                state.generation = retained.socket.generation;
                state.socket = Some(Arc::clone(&retained.socket));
            } else {
                retained.socket.shutdown.cancel();
                tracing::info!(target: "provider", reason = "expired_or_closed", "Retiring retained Responses websocket");
            }
        }
        entries.next_lease += 1;
        let id = entries.next_lease;
        entries.sessions.insert(
            bind.clone(),
            Entry {
                lease: id,
                active: Arc::downgrade(session),
                retained: None,
            },
        );
        Some(Lease {
            owner: self.clone(),
            bind: bind.clone(),
            id,
        })
    }

    /// Retire both idle and borrowed state for a deleted or moved session.
    pub fn retire_session(&self, bind: &BindIdentity) {
        let entry = self.0.lock().unwrap().sessions.remove(bind);
        if let Some(entry) = entry {
            close_entry(entry);
        }
    }

    /// A move's busy probe must not disturb an active run, even when the move is refused.
    pub fn retire_idle_session(&self, bind: &BindIdentity) {
        let mut entries = self.0.lock().unwrap();
        if entries
            .sessions
            .get(bind)
            .is_some_and(|entry| entry.retained.is_some())
        {
            if let Some(entry) = entries.sessions.remove(bind) {
                close_entry(entry);
            }
        }
    }

    pub(in crate::transport) fn retire_idle_credential(&self, request: &PreparedHttpRequest) {
        let mut entries = self.0.lock().unwrap();
        let keys: Vec<_> = entries
            .sessions
            .iter()
            .filter_map(|(bind, entry)| {
                let socket = &entry.retained.as_ref()?.socket;
                let matches = match (&socket.credential_id, &request.credential_id) {
                    (Some(saved), Some(current)) => saved == current,
                    (None, None) => {
                        let auth = |headers: &[(String, String)]| {
                            headers
                                .iter()
                                .filter(|(name, _)| {
                                    name.eq_ignore_ascii_case("authorization")
                                        || name.eq_ignore_ascii_case("chatgpt-account-id")
                                })
                                .cloned()
                                .collect::<Vec<_>>()
                        };
                        let saved = auth(&socket.headers);
                        !saved.is_empty() && saved == auth(&request.headers)
                    }
                    _ => false,
                };
                matches.then(|| bind.clone())
            })
            .collect();
        let retired = keys.len();
        for key in keys {
            if let Some(entry) = entries.sessions.remove(&key) {
                close_entry(entry);
            }
        }
        tracing::info!(target: "provider", reason = "connection_limit", retired, "Retiring credential's idle Responses websockets before accounted retry");
    }

    /// Retire all authenticated state on credential revocation or executor shutdown.
    pub fn clear(&self) {
        let entries = std::mem::take(&mut self.0.lock().unwrap().sessions);
        for entry in entries.into_values() {
            close_entry(entry);
        }
    }
}

fn close_entry(entry: Entry) {
    if let Some(session) = entry.active.upgrade() {
        session.retire_socket();
    }
    if let Some(retained) = entry.retained {
        retained.socket.shutdown.cancel();
    }
}

impl Lease {
    pub(in crate::transport) fn complete(self, session: &Session) {
        let socket = session.take_completed_socket();
        let now = session.now();
        let mut entries = self.owner.0.lock().unwrap();
        if !entries
            .sessions
            .get(&self.bind)
            .is_some_and(|entry| entry.lease == self.id)
        {
            if let Some(socket) = socket {
                socket.shutdown.cancel();
            }
            return;
        }
        entries.sessions.remove(&self.bind);
        let Some(socket) = socket else {
            return;
        };
        if now.saturating_duration_since(socket.established_at) >= MAX_SOCKET_AGE {
            socket.shutdown.cancel();
            return;
        }
        let receipt_bytes = socket.continuation.lock().unwrap().as_ref().map_or(
            0,
            crate::chatgpt_continuation::CompletedResponse::retained_bytes,
        );
        if receipt_bytes > entries.max_receipt_bytes {
            socket.shutdown.cancel();
            tracing::info!(target: "provider", reason = "receipt_too_large", "Retiring Responses websocket beyond retained receipt byte budget");
            return;
        }
        loop {
            let (count, bytes) = entries
                .sessions
                .values()
                .filter_map(|entry| entry.retained.as_ref())
                .fold((0usize, 0usize), |(count, bytes), retained| {
                    (count + 1, bytes + retained.receipt_bytes)
                });
            let reason = if count >= MAX_RETAINED {
                "capacity"
            } else if bytes + receipt_bytes > entries.max_receipt_bytes {
                "receipt_bytes"
            } else {
                break;
            };
            let oldest = entries
                .sessions
                .iter()
                .filter_map(|(bind, entry)| {
                    entry
                        .retained
                        .as_ref()
                        .map(|retained| (bind.clone(), retained.since))
                })
                .min_by_key(|(_, since)| *since)
                .map(|(bind, _)| bind);
            if let Some(oldest) = oldest {
                if let Some(entry) = entries.sessions.remove(&oldest) {
                    close_entry(entry);
                }
                tracing::info!(target: "provider", reason, "Retiring retained Responses websocket");
            }
        }
        let deadline = (now + MAX_IDLE).min(socket.established_at + MAX_SOCKET_AGE);
        let owner = Arc::downgrade(&self.owner.0);
        let bind = self.bind.clone();
        let id = self.id;
        let expiry = tokio::spawn(async move {
            tokio::time::sleep_until(deadline).await;
            if let Some(owner) = owner.upgrade() {
                let mut entries = owner.lock().unwrap();
                if entries
                    .sessions
                    .get(&bind)
                    .is_some_and(|entry| entry.lease == id && entry.retained.is_some())
                {
                    if let Some(entry) = entries.sessions.remove(&bind) {
                        close_entry(entry);
                    }
                    tracing::info!(target: "provider", reason = "idle_or_age", "Retiring retained Responses websocket");
                }
            }
        });
        entries.sessions.insert(
            self.bind.clone(),
            Entry {
                lease: self.id,
                active: Weak::new(),
                retained: Some(Retained {
                    socket,
                    receipt_bytes,
                    since: now,
                    expiry,
                }),
            },
        );
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut entries = self.owner.0.lock().unwrap();
        // Completion has already changed the entry from borrowed to retained.
        if entries
            .sessions
            .get(&self.bind)
            .is_some_and(|entry| entry.lease == self.id && entry.retained.is_none())
        {
            if let Some(entry) = entries.sessions.remove(&self.bind) {
                close_entry(entry);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_retirement_preserves_per_run_fallback_and_turn_state() {
        let pool = Connections::default();
        let session = Arc::new(Session::default());
        let bind = BindIdentity::new("/project", "harness", "session");
        let _lease = pool.acquire(&bind, &session).unwrap();
        {
            let mut state = session.state.lock().unwrap();
            state.failures = 3;
            state.force_https = true;
            state.turn_state = Some("current-turn".into());
        }
        pool.retire_session(&bind);
        let state = session.state.lock().unwrap();
        assert_eq!(state.failures, 3);
        assert!(state.force_https);
        assert_eq!(state.turn_state.as_deref(), Some("current-turn"));
    }
}
