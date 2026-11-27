use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use broca_protocol::{BindIdentity, BindPrincipal};
use broca_session_store::held::{serde_heap_bytes, HeldBytes};
use broca_wire::{MemoryCache, ParkedSendStatusCounts, SendReplyWire};

use crate::store_reads::StoreReadRequest;

const MAX_ENTRIES: usize = 1024;
const MAX_BYTES: usize = 2 * 1024 * 1024;
const MAX_ENTRY_BYTES: usize = 32 * 1024;

#[derive(Clone, PartialEq, Eq, Hash)]
struct Key {
    session: BindIdentity,
    run_id: String,
    send_id: String,
    principal: Principal,
}

// Do not coalesce old and new spellings of a reserved module id: the spelling can be
// significant when the stored sender's namespace is resolved.
#[derive(Clone, PartialEq, Eq, Hash)]
enum Principal {
    Absent,
    Direct,
    Unverified,
    Reserved(String),
}

impl Key {
    fn of(session: &BindIdentity, request: &StoreReadRequest) -> Option<Self> {
        let StoreReadRequest::SendStatus {
            own_run,
            send_id,
            principal,
        } = request
        else {
            return None;
        };
        let principal = match principal {
            None => Principal::Absent,
            Some(BindPrincipal::Direct) => Principal::Direct,
            Some(BindPrincipal::Unverified) => Principal::Unverified,
            Some(BindPrincipal::Reserved { module_id }) => Principal::Reserved(module_id.clone()),
            Some(BindPrincipal::Unknown(_)) => return None,
        };
        Some(Self {
            session: session.clone(),
            run_id: own_run.0.clone(),
            send_id: send_id.clone(),
            principal,
        })
    }

    fn heap_bytes(&self) -> usize {
        self.session.project_root.capacity()
            + self.session.harness.capacity()
            + self.session.session.capacity()
            + self.run_id.heap_bytes()
            + self.send_id.heap_bytes()
            + match &self.principal {
                Principal::Reserved(module) => module.heap_bytes(),
                _ => 0,
            }
    }
}

struct Entry {
    bytes: usize,
    used: u64,
    reply: Arc<SendReplyWire>,
}

/// Successful send replies belonging to the positions in the enclosing cache. A position
/// change drops every reply for that session. The caller has to prove the position again before
/// asking for a reply, since the stored send or original sender can change after a write.
#[derive(Default)]
pub(super) struct SendMemo {
    entries: HashMap<Key, Entry>,
    order: BTreeMap<u64, Key>,
    clock: u64,
    bytes: usize,
    hits: u64,
    misses: u64,
}

impl SendMemo {
    pub(super) fn get(
        &mut self,
        session: &BindIdentity,
        request: &StoreReadRequest,
        position_matches: bool,
    ) -> Option<Arc<SendReplyWire>> {
        let reply = position_matches
            .then(|| Key::of(session, request))
            .flatten()
            .and_then(|key| {
                let entry = self.entries.get_mut(&key)?;
                self.clock += 1;
                let used = std::mem::replace(&mut entry.used, self.clock);
                let (_, key) = self
                    .order
                    .remove_entry(&used)
                    .expect("each send reply has an order key");
                self.order.insert(self.clock, key);
                Some(Arc::clone(&entry.reply))
            });
        if reply.is_some() {
            self.hits += 1;
        } else {
            self.misses += 1;
        }
        reply
    }

    pub(super) fn keep(
        &mut self,
        session: &BindIdentity,
        request: &StoreReadRequest,
        reply: &SendReplyWire,
    ) {
        let Some(key) = Key::of(session, request) else {
            return;
        };
        let bytes = std::mem::size_of::<(Key, Entry)>()
            + std::mem::size_of::<(u64, Key)>()
            + 2 * key.heap_bytes()
            + std::mem::size_of::<SendReplyWire>()
            + 2 * std::mem::size_of::<usize>()
            + serde_heap_bytes(reply);
        if bytes > MAX_ENTRY_BYTES {
            return;
        }
        self.clock += 1;
        if let Some(old) = self.entries.insert(
            key.clone(),
            Entry {
                bytes,
                used: self.clock,
                reply: Arc::new(reply.clone()),
            },
        ) {
            self.order.remove(&old.used);
            self.bytes -= old.bytes;
        }
        self.order.insert(self.clock, key);
        self.bytes += bytes;
        while self.entries.len() > MAX_ENTRIES || self.bytes > MAX_BYTES {
            let (_, oldest) = self
                .order
                .pop_first()
                .expect("an over-cap cache has entries");
            let old = self
                .entries
                .remove(&oldest)
                .expect("each order key has a send reply");
            self.bytes -= old.bytes;
        }
    }

    pub(super) fn remove_session(&mut self, session: &BindIdentity) {
        self.entries.retain(|key, entry| {
            if &key.session == session {
                self.order.remove(&entry.used);
                self.bytes -= entry.bytes;
                false
            } else {
                true
            }
        });
    }

    pub(super) fn memory(&self) -> ParkedSendStatusCounts {
        ParkedSendStatusCounts {
            hits: self.hits,
            misses: self.misses,
            held: MemoryCache {
                entries: self.entries.len() as u64,
                max_entries: MAX_ENTRIES as u64,
                estimated_bytes: self.bytes as u64,
                counted_bytes: Some(self.bytes as u64),
                max_bytes: Some(MAX_BYTES as u64),
                ..Default::default()
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parked_send_memo_evicts_by_size_and_drops_all_replies_at_a_moved_position() {
        let key = BindIdentity::new("/repo", "broca", "first");
        let other = BindIdentity::new("/repo", "broca", "second");
        let mut memo = SendMemo::default();
        let reply = SendReplyWire {
            result: broca_wire::SendResult::Pending {
                submission_id: "sub-first".into(),
            },
            baseline: None,
            delivered: None,
        };
        let request = |send_id| StoreReadRequest::SendStatus {
            own_run: broca_protocol::RunId("first".into()),
            send_id,
            principal: None,
        };
        let short = request("short".into());
        memo.keep(&key, &short, &reply);
        for n in 0..(MAX_BYTES / (8 * 1024)) {
            memo.keep(
                &key,
                &request(format!("{n}-{}", "x".repeat(8 * 1024))),
                &reply,
            );
        }
        let memory = memo.memory().held;
        assert!(memory.counted_bytes.unwrap() <= MAX_BYTES as u64);
        assert!((1..MAX_ENTRIES as u64).contains(&memory.entries));
        assert!(
            memo.get(&key, &short, true).is_none(),
            "oldest reply was evicted"
        );
        let oversized = request("x".repeat(MAX_ENTRY_BYTES));
        memo.keep(&key, &oversized, &reply);
        assert!(
            memo.get(&key, &oversized, true).is_none(),
            "one request cannot take the entire cache"
        );
        memo.keep(&other, &short, &reply);
        memo.remove_session(&key);
        assert_eq!(memo.memory().held.entries, 1);
        assert!(memo.get(&other, &short, true).is_some());
    }
}
