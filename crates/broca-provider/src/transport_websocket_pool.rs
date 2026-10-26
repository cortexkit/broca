//! An executor's in-memory, full-bind connection owner. Only sockets cross run boundaries.

use super::*;
use broca_protocol::BindIdentity;
use std::{collections::HashMap, sync::Weak};

const MAX_RETAINED: usize = 64;
const MAX_IDLE: Duration = Duration::from_secs(10 * 60);

#[derive(Default)]
struct Entries {
    sessions: HashMap<BindIdentity, Entry>,
    next_lease: u64,
}

struct Entry {
    lease: u64,
    active: Weak<Session>,
    retained: Option<Retained>,
}

struct Retained {
    socket: Arc<Socket>,
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
        session.close();
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
        if entries
            .sessions
            .values()
            .filter(|entry| entry.retained.is_some())
            .count()
            >= MAX_RETAINED
        {
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
                tracing::info!(target: "provider", reason = "capacity", "Retiring retained Responses websocket");
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
