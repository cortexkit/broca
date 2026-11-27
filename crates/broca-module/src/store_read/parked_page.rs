//! Position-pinned metadata for reading sealed pages after their actor closes.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use broca_core::Cursor;
use broca_records::session_storage::StorageClass;
use broca_session_store::backend::SessionAddr;
use broca_session_store::held::{serde_heap_bytes, HeldBytes};
use broca_session_store::session::{Position, SealedConversation, StoredLedger};

use super::*;
use crate::serve::ServeError;
use crate::store_open::StoreRuntime;
use crate::store_reads::{PageAnswer, PageRequest};

const MAX_ENTRIES: usize = 512;
const MAX_BYTES: usize = 1024 * 1024;
const MAX_ENTRY_BYTES: usize = 8 * 1024;

#[derive(Default)]
pub(crate) struct ParkedPages(Mutex<Cache>);

#[derive(Default)]
struct Cache {
    by_addr: HashMap<String, Entry>,
    order: BTreeMap<u64, String>,
    used: u64,
    bytes: usize,
    hits: u64,
    misses: BTreeMap<String, u64>,
}

struct Entry {
    at: Position,
    class: StorageClass,
    facts: Arc<PageContext>,
    used: u64,
    bytes: usize,
}

pub(crate) struct PageContext {
    lineage: SessionReadLineageState,
    start: Option<Box<RunStart>>,
    latest_finished: bool,
    has_reply_fallbacks: bool,
    prompt_len: u64,
    window: u64,
    cursor: Cursor,
    rules: SealedConversation,
}

impl PageContext {
    fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + 2 * std::mem::size_of::<usize>() // Arc counters
            + self.lineage.last_run_id.as_ref().map_or(0, |id| id.0.heap_bytes())
            + self.lineage.error.as_ref().map_or(0, serde_heap_bytes)
            + self.lineage.usage.as_ref().map_or(0, serde_heap_bytes)
            + self.start.as_ref().map_or(0, HeldBytes::heap_bytes)
    }
}

impl ParkedPages {
    fn lock(&self) -> std::sync::MutexGuard<'_, Cache> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Learn only while closing at a durable position that recovery doesn't change. A queued
    /// send can become due later at the same position; leave those sessions to recovery.
    pub(crate) fn keep<B: SessionBackend>(
        &self,
        addr: &str,
        class: StorageClass,
        session: &Session<B>,
        cursor: Option<Cursor>,
    ) {
        let window = session.conversation.window_start();
        let Some(cursor) = cursor else { return };
        if window <= 1
            || !session.operations.queue.is_empty()
            || crate::store_actor::recovery_changes_reads(&session.operations)
        {
            return;
        }
        let run = session.operations.run.as_ref();
        let mut facts = PageContext {
            lineage: lineage_state(session),
            start: None,
            latest_finished: run.is_some_and(|run| run.finished.is_some()),
            has_reply_fallbacks: session.has_reply_model_fallbacks(),
            prompt_len: transcript_len(session),
            window,
            cursor,
            rules: session.sealed_conversation(),
        };
        let keys = 2 * addr.len()
            + std::mem::size_of::<(String, Entry)>()
            + std::mem::size_of::<(u64, String)>();
        let mut bytes = keys + facts.bytes();
        if bytes > MAX_ENTRY_BYTES {
            return;
        }
        if let Some(start) = run.and_then(|run| run.start.as_ref()) {
            let start_bytes = std::mem::size_of::<RunStart>() + start.heap_bytes();
            if start_bytes <= MAX_ENTRY_BYTES - bytes {
                facts.start = Some(Box::new(start.clone()));
                bytes += start_bytes;
            }
        }
        let mut cache = self.lock();
        cache.remove(addr);
        cache.used += 1;
        let used = cache.used;
        cache.by_addr.insert(
            addr.to_owned(),
            Entry {
                at: session.position(),
                class,
                facts: Arc::new(facts),
                used,
                bytes,
            },
        );
        cache.order.insert(used, addr.to_owned());
        cache.bytes += bytes;
        while cache.bytes > MAX_BYTES || cache.by_addr.len() > MAX_ENTRIES {
            let (_, oldest) = cache.order.first_key_value().expect("over budget");
            let oldest = oldest.clone();
            cache.remove(&oldest);
        }
    }

    fn miss(&self, reason: &'static str) {
        *self.lock().misses.entry(reason.into()).or_default() += 1;
    }

    fn at(&self, addr: &str, class: StorageClass, at: Position) -> Option<Arc<PageContext>> {
        let mut cache = self.lock();
        let matches = cache
            .by_addr
            .get(addr)
            .is_some_and(|entry| entry.class == class && entry.at == at);
        if !matches {
            cache.remove(addr);
            return None;
        }
        cache.used += 1;
        let used = cache.used;
        let entry = cache.by_addr.get_mut(addr).expect("position matched");
        let old = std::mem::replace(&mut entry.used, used);
        let facts = Arc::clone(&entry.facts);
        let addr = cache
            .order
            .remove(&old)
            .expect("each entry has an order key");
        cache.order.insert(used, addr);
        Some(facts)
    }

    pub(crate) fn memory(&self) -> broca_wire::ParkedPageCounts {
        let cache = self.lock();
        broca_wire::ParkedPageCounts {
            hits: cache.hits,
            misses: cache.misses.clone(),
            held: broca_wire::MemoryCache {
                entries: cache.by_addr.len() as u64,
                max_entries: MAX_ENTRIES as u64,
                estimated_bytes: cache.bytes as u64,
                counted_bytes: Some(cache.bytes as u64),
                max_bytes: Some(MAX_BYTES as u64),
                ..Default::default()
            },
        }
    }

    pub(crate) async fn read(
        &self,
        store: &StoreRuntime,
        registry: &crate::LineageRegistry,
        key: &broca_protocol::BindIdentity,
        class: StorageClass,
        expiry_position: Option<Position>,
        request: &PageRequest,
    ) -> Option<(PageAnswer, Position)> {
        let addr = broca_core::session_addr(key);
        if !self.lock().by_addr.contains_key(&addr) {
            self.miss("not_kept");
            return None;
        }
        let ledger = StoredLedger::open(
            store.backend(class),
            SessionAddr(addr.clone()),
            &crate::store_open::triple(key),
            store.deployment(),
        )
        .await;
        let Ok(Some(ledger)) = ledger else {
            self.lock().remove(&addr);
            self.miss("no_position");
            return None;
        };
        let Some(head) = ledger.local_head() else {
            self.lock().remove(&addr);
            self.miss("no_position");
            return None;
        };
        let at = ledger.position();
        let Some(facts) = self.at(&addr, class, at) else {
            self.miss("position_changed");
            return None;
        };
        if expiry_position.is_some_and(|checked| checked != at)
            || registry.slot_state(key).is_some()
        {
            self.miss("position_race");
            return None;
        }
        if request.include_display_facts && facts.has_reply_fallbacks {
            self.miss("reply_model_fallbacks");
            return None;
        }
        let source = PageSource::Sealed {
            facts: &facts,
            ledger: &ledger,
            head,
        };
        let parts = store.parts_for(class, head.cipher());
        match page_from(&source, parts, request, None).await {
            Ok(answer) => {
                self.lock().hits += 1;
                Some((answer, at))
            }
            // Preserve the full reader's error ordering as well as its successful replies.
            Err(_) => {
                self.miss("needs_full_read");
                None
            }
        }
    }
}

impl Cache {
    fn remove(&mut self, addr: &str) {
        if let Some(entry) = self.by_addr.remove(addr) {
            self.order.remove(&entry.used);
            self.bytes -= entry.bytes;
        }
    }
}

pub(crate) enum PageSource<'a, B: SessionBackend> {
    Folded(&'a Session<B>),
    Sealed {
        facts: &'a PageContext,
        ledger: &'a StoredLedger<B>,
        head: &'a broca_session_store::head::Head,
    },
}

impl<'a, B: SessionBackend> PageSource<'a, B> {
    pub(crate) fn folded(&self) -> Option<&'a Session<B>> {
        match self {
            Self::Folded(session) => Some(session),
            Self::Sealed { .. } => None,
        }
    }

    pub(super) fn head(&self) -> &'a broca_session_store::head::Head {
        match self {
            Self::Folded(session) => session.head(),
            Self::Sealed { head, .. } => head,
        }
    }

    pub(super) fn latest(&self) -> Option<(&'a str, bool)> {
        match self {
            Self::Folded(session) => session
                .operations
                .run
                .as_ref()
                .map(|run| (run.run_id.as_str(), run.finished.is_some())),
            Self::Sealed { facts, .. } => facts
                .lineage
                .last_run_id
                .as_ref()
                .map(|id| (id.0.as_str(), facts.latest_finished)),
        }
    }

    pub(super) fn latest_start(&self) -> Option<(&'a str, Option<&'a RunStart>)> {
        match self {
            Self::Folded(session) => session
                .operations
                .run
                .as_ref()
                .map(|run| (run.run_id.as_str(), run.start.as_ref())),
            Self::Sealed { facts, .. } => self.latest().map(|(id, _)| (id, facts.start.as_deref())),
        }
    }

    pub(super) fn prompt_len(&self) -> u64 {
        match self {
            Self::Folded(session) => transcript_len(session),
            Self::Sealed { facts, .. } => facts.prompt_len,
        }
    }

    pub(super) fn lineage(&self) -> SessionReadLineageState {
        match self {
            Self::Folded(session) => lineage_state(session),
            Self::Sealed { facts, .. } => facts.lineage.clone(),
        }
    }

    pub(super) async fn stream_head(
        &self,
        known: Option<Option<Cursor>>,
    ) -> Result<Option<Cursor>, ServeError> {
        match self {
            Self::Folded(session) => super::stream_head(session, known).await,
            Self::Sealed { facts, .. } => Ok(Some(facts.cursor)),
        }
    }

    pub(super) async fn find_message(&self, mid: &str) -> Result<Option<u64>, SessionError> {
        match self {
            Self::Folded(session) => session.find_message(mid).await,
            Self::Sealed { ledger, facts, .. } => {
                ledger
                    .sealed_message_ordinal(facts.rules, facts.window, mid)
                    .await
            }
        }
    }

    pub(super) async fn served_entries(
        &self,
        from: u64,
        limit: usize,
    ) -> Result<Vec<StoredEntry>, SessionError> {
        match self {
            Self::Folded(session) => super::served_entries(session, from, limit).await,
            Self::Sealed { facts, .. } => {
                let remaining = facts.prompt_len.saturating_sub(from.saturating_sub(1));
                let limit = (limit as u64).min(remaining) as usize;
                if limit == 0 {
                    return Ok(Vec::new());
                }
                self.entries(from, limit).await
            }
        }
    }

    pub(crate) async fn entries(
        &self,
        from: u64,
        limit: usize,
    ) -> Result<Vec<StoredEntry>, SessionError> {
        match self {
            Self::Folded(session) => session.entries(from, limit).await,
            Self::Sealed { ledger, facts, .. } => {
                if from.saturating_add(limit as u64) > facts.window {
                    return Err(SessionError::Fold(
                        "page or look-ahead needs the open window".into(),
                    ));
                }
                ledger
                    .sealed_entries(facts.rules, facts.window, from, limit)
                    .await
            }
        }
    }

    pub(crate) async fn run_rules(
        &self,
        parts: &dyn Parts,
        run: &str,
    ) -> Result<Option<broca_protocol::tags::ComputedRules>, String> {
        use crate::store_output_refs::{run_rules, run_rules_from};
        match self {
            Self::Folded(session) => run_rules(session, parts, run).await,
            Self::Sealed { ledger, .. } => {
                run_rules_from(self.head().session_id, parts, run, async {
                    if let Some((_, Some(start))) = self.latest_start().filter(|(id, _)| *id == run)
                    {
                        return Ok(Some(start.clone()));
                    }
                    ledger
                        .sealed_run_start(run)
                        .await?
                        .map(Some)
                        .ok_or_else(|| SessionError::Fold("run start needs the open log".into()))
                })
                .await
            }
        }
    }
}
