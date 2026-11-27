//! `memory.report`: what the daemon's open sessions and shared caches hold in memory.
//!
//! Held sizes are estimated by walking the structures when the report is asked for (see
//! `broca_session_store::held`). The report also includes registry counts.
//! The report carries sizes and counts only: no content, no message ids, nothing from a
//! transcript. A session is named by its store address.

use std::collections::{BTreeMap, BTreeSet};

use broca_wire::{MemoryCache, MemoryHolders, MemoryReport, MemorySession, MemorySetupPart};

/// How long the report waits for each open actor's answer. An actor that doesn't answer in time
/// is counted as unanswered, never waited on longer: the report must not queue behind a busy
/// session.
pub const ACTOR_WAIT: std::time::Duration = std::time::Duration::from_secs(2);

/// How many of the largest sessions the report names.
pub const LARGEST: usize = 10;

/// What an actor answers the report.
#[derive(Debug)]
pub enum MemoryAnswer {
    /// A session in the session store, sized.
    Store(Box<ActorMemory>),
    /// An actor closing its session, which the report counts as unanswered.
    Closing,
}

/// One store actor's answer: its session's sizes, and the setup parts it holds decoded.
#[derive(Debug, Clone, Default)]
pub struct ActorMemory {
    pub session: MemorySession,
    pub setup: Vec<SetupHeld>,
}

/// One decoded setup part an actor holds, with what tells its content and its copy apart from
/// other sessions'. Neither is reported: they only group the parts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetupHeld {
    /// What the part holds, as reported: `tools`, `system_text` or `plan_manifest`.
    pub kind: &'static str,
    /// What identifies the part's content: the composition digest of the plan it came from.
    pub content: String,
    /// The address of the decoded copy: two sessions sharing one copy name the same address.
    pub copy: usize,
    /// The copy's estimated bytes.
    pub bytes: usize,
}

/// The report from the actors' answers, the count of sessions that didn't answer, and the store
/// runtime's caches.
pub fn assemble(
    answers: Vec<ActorMemory>,
    unanswered_sessions: u64,
    closed_session_views: MemoryCache,
    settled_runs: MemoryCache,
) -> MemoryReport {
    let mut totals = MemoryHolders::default();
    // Per part: its estimated bytes, the distinct copies held, and the sessions holding one.
    let mut parts: BTreeMap<(&'static str, String), (usize, BTreeSet<usize>, u64)> =
        BTreeMap::new();
    let mut sessions = Vec::with_capacity(answers.len());
    for answer in answers {
        totals.add(&answer.session.holders);
        let mut seen = BTreeSet::new();
        for part in answer.setup {
            let entry = parts
                .entry((part.kind, part.content.clone()))
                .or_insert_with(|| (part.bytes, BTreeSet::new(), 0));
            entry.0 = entry.0.max(part.bytes);
            entry.1.insert(part.copy);
            if seen.insert((part.kind, part.content)) {
                entry.2 += 1;
            }
        }
        sessions.push(answer.session);
    }
    sessions.sort_by(|a, b| {
        b.holders
            .total
            .cmp(&a.holders.total)
            .then_with(|| a.address.cmp(&b.address))
    });
    let largest = sessions
        .iter()
        .take(LARGEST)
        .map(|session| session.address.clone())
        .collect();
    let running_sessions = sessions
        .iter()
        .filter(|session| session.run_in_progress)
        .count() as u64;
    let mut setup_parts: Vec<MemorySetupPart> = parts
        .into_iter()
        .map(|((kind, _), (bytes, copies, sessions))| MemorySetupPart {
            kind: kind.to_owned(),
            decoded_bytes: bytes as u64,
            decoded_copies: copies.len() as u64,
            sessions,
        })
        .collect();
    setup_parts.sort_by(|a, b| {
        (b.decoded_bytes * b.decoded_copies)
            .cmp(&(a.decoded_bytes * a.decoded_copies))
            .then_with(|| a.kind.cmp(&b.kind))
    });
    MemoryReport {
        sessions,
        running_sessions,
        unanswered_sessions,
        totals,
        largest,
        setup_parts,
        closed_session_views,
        settled_runs,
        setup_stores: Vec::new(),
        kept_heads: None,
        parked_subscribers: Default::default(),
        actor_close_reasons: Default::default(),
    }
}

impl crate::LineageRegistry {
    /// `memory.report`: every open actor's sizes, walked now, and the store runtime's caches.
    /// Actors are asked together; one that doesn't answer within [`ACTOR_WAIT`] is counted as
    /// unanswered.
    pub async fn memory_report(&self) -> MemoryReport {
        let senders = self.open_actor_senders();
        let asks = senders.into_iter().map(|sender| async move {
            let (reply, answer) = tokio::sync::oneshot::channel();
            let asked = async {
                sender
                    .send(crate::actor::LineageCmd::MemoryReport { reply })
                    .await
                    .ok()?;
                answer.await.ok()
            };
            tokio::time::timeout(ACTOR_WAIT, asked).await.ok().flatten()
        });
        let mut answers = Vec::new();
        let mut unanswered = 0;
        for answer in futures::future::join_all(asks).await {
            match answer {
                Some(MemoryAnswer::Store(memory)) => answers.push(*memory),
                Some(MemoryAnswer::Closing) | None => unanswered += 1,
            }
        }
        let (views, settled, setup_stores, kept_heads) = match self.store_runtime() {
            Some(store) => (
                store.views().memory(|view| view.held_bytes().total()),
                store.settled_runs().memory(),
                store.setup_store_usage(),
                Some(broca_wire::KeptHeadCounts {
                    authorization: Some(store.authorization().memory()),
                    parked_pages: Some(store.parked_pages().memory()),
                    ..store.heads().counts()
                }),
            ),
            None => Default::default(),
        };
        let mut report = assemble(answers, unanswered, views, settled);
        report.setup_stores = setup_stores;
        report.kept_heads = kept_heads;
        report.parked_subscribers = self.parked_subscriber_counts();
        report.actor_close_reasons = self.actor_close_reasons();
        report
    }
}

#[cfg(test)]
mod tests;
