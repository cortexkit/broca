//! Memory budgets of a loaded session, measured with the counting allocator on a synthetic
//! session shaped like production ones (`crate::synthetic`), and the estimates the memory
//! report gives for the same session held to what the allocator measured.
//!
//! The budget is the one `.cortexkit/internal-docs/designs/session-storage/27-memory-shape.md`
//! sets: an idle open session holds at most `1.2 x W + 400 B x messages`, where `W` is the
//! window's messages as compact JSON. Each limit below is today's measure plus 10%, with today's
//! measure and the design's target beside it. A change that lowers a holder lowers its limit;
//! none may raise one.

use futures::executor::block_on;

use broca_test_support::alloc::{installed, measure, thousandths, Budget};

use super::{Session, SessionHeld};
use crate::backend::{MemoryBackend, SessionAddr};
use crate::head::SessionTriple;
use crate::keys::{Cipher, DeploymentKey};
use crate::synthetic::{window_bytes, write, Shape};

/// Window messages of the measured session: enough for the proportions to settle, small enough
/// that the test takes about a second.
const MESSAGES: usize = 1_000;

// Limits on what a loaded session holds, in thousandths of `W` (4.56 MB here). Today's measure
// was taken on the synthetic session at 1,000 messages.
/// The whole session. Today: 1.634. Target: the design's idle budget, about 1,288 at this
/// session's 4.5 KB per message (1.2 x W + 400 B x messages).
const TOTAL: u64 = 1_798;
/// The window's messages, the latest run's ones the kept records share included. Today: 1.155;
/// measure plus 10% would be above the limit of 1.226 set before, so the limit stays 1.226.
/// Target: about 1,000, content held at its sent size.
const HELD_MESSAGES: u64 = 1_226;
/// The window messages' entry states, the facts the kept records share included. Today: 0.168
/// (765 bytes per message). Target: 250 bytes per message, about 55.
const ENTRY_FACTS: u64 = 185;
/// The kept records' contents beyond what they share with the conversation. Today: 0.269, most
/// of it the latest run's tool results: the conversation's tool message holds its own copy of
/// each result until a message's text and JSON can be shared between holders. Target: their own
/// small fields only.
const KEPT_RECORDS: u64 = 296;
/// The kept records' buffer beyond its records. Today: 0.023. Target: none, a buffer sized for
/// the latest run.
const KEPT_SPARE: u64 = 26;
/// The id index. Today: 0.012. Target: positions only, a few bytes per id. Rounded up to the
/// next thousandth.
const ID_INDEX: u64 = 14;
/// Everything else, the session's interned values included. Today: 0.008 (39 KB). Target: a
/// small share of the idle budget. Rounded up to the next thousandth.
const REST: u64 = 10;
/// Bytes allocated while loading, freed or not. Today: 6.916. Target: none set by the design;
/// opening folds the log record by record and never holds it whole.
const ALLOCATED_DURING_LOAD: u64 = 7_608;

/// How far an estimate may be from what the allocator measured for the same holder, in percent.
const ESTIMATE_TOLERANCE: usize = 15;

fn triple() -> SessionTriple {
    SessionTriple {
        project_root: "/synthetic".into(),
        harness: "broca".into(),
        session: "budget".into(),
    }
}

fn deployment() -> DeploymentKey {
    DeploymentKey::new(1, &[5; 32]).creating(Cipher::XChaCha20Poly1305)
}

fn addr() -> SessionAddr {
    SessionAddr("b0d9e7b0d9e7b0d9".into())
}

/// What a loaded session held by holder, as the allocator measured it, with its estimate.
struct Measured {
    w: usize,
    messages: usize,
    total: u64,
    allocated: u64,
    held: SessionHeld,
    estimated: SessionHeld,
}

fn freed<T>(value: T) -> u64 {
    let ((), counts) = measure(|| drop(value));
    counts.held.unsigned_abs() as u64
}

/// Writes the synthetic session, then loads it under the counting allocator and takes each
/// holder apart, measuring what dropping it frees. The kept records go first: what they share
/// with the conversation (its messages and their facts) is freed with the conversation's
/// holders, which the memory report counts it under.
fn loaded() -> Measured {
    assert!(
        installed(),
        "the crate's tests count with the counting allocator"
    );
    let backend = MemoryBackend::default();
    let written = block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(), false)
            .await
            .unwrap();
        write(&mut session, &Shape::production(MESSAGES))
            .await
            .unwrap()
    });
    let (session, load) =
        measure(|| block_on(Session::load(&backend, addr(), &triple(), &deployment())).unwrap());
    let mut session = Box::new(session);
    let w = window_bytes(&session.conversation);
    let estimated = session.held_bytes();
    let kept_spare = estimated.kept_capacity as u64;
    let kept = freed(std::mem::take(&mut session.kept));
    let messages = freed(std::mem::take(&mut session.conversation.messages));
    let entry_facts = freed(std::mem::take(&mut session.conversation.entries));
    let id_index = freed(std::mem::replace(
        &mut session.open_ids,
        crate::idindex::OpenIds::new([0; 32]),
    ));
    let total = load.held.max(0) as u64;
    let rest = total.saturating_sub(messages + entry_facts + kept + id_index);
    drop(session);
    Measured {
        w,
        messages: written.messages,
        total,
        allocated: load.allocated,
        held: SessionHeld {
            messages: messages as usize,
            entry_facts: entry_facts as usize,
            kept_records: kept.saturating_sub(kept_spare) as usize,
            kept_capacity: kept_spare as usize,
            id_index: id_index as usize,
            sealed_entries: 0,
            rest: rest as usize,
        },
        estimated,
    }
}

/// A loaded session, idle with no read served, holds no more than its budget, holder by holder,
/// and its load allocates no more than its budget.
#[test]
fn a_loaded_session_holds_no_more_than_its_budget() {
    let m = loaded();
    let w = m.w;
    let budget = Budget::new(
        format!("a loaded session of {} messages, W {} bytes", m.messages, w),
        "W",
        w,
    )
    .holder("total", m.total, thousandths(w, TOTAL))
    .holder(
        "messages",
        m.held.messages as u64,
        thousandths(w, HELD_MESSAGES),
    )
    .holder(
        "entry facts",
        m.held.entry_facts as u64,
        thousandths(w, ENTRY_FACTS),
    )
    .holder(
        "kept records",
        m.held.kept_records as u64,
        thousandths(w, KEPT_RECORDS),
    )
    .holder(
        "kept records' spare buffer",
        m.held.kept_capacity as u64,
        thousandths(w, KEPT_SPARE),
    )
    .holder("id index", m.held.id_index as u64, thousandths(w, ID_INDEX))
    .holder("rest", m.held.rest as u64, thousandths(w, REST))
    .holder(
        "allocated during the load",
        m.allocated,
        thousandths(w, ALLOCATED_DURING_LOAD),
    );
    eprintln!("{}", budget.table());
    budget.check();
}

/// The memory report's estimate of each holder of a loaded session is within
/// [`ESTIMATE_TOLERANCE`] of what the allocator measured for it.
#[test]
fn the_estimates_of_a_loaded_session_match_the_allocator() {
    let m = loaded();
    let rows = [
        ("messages", m.estimated.messages, m.held.messages),
        ("entry facts", m.estimated.entry_facts, m.held.entry_facts),
        (
            "kept records and their buffer",
            m.estimated.kept_records + m.estimated.kept_capacity,
            m.held.kept_records + m.held.kept_capacity,
        ),
        ("id index", m.estimated.id_index, m.held.id_index),
        ("total", m.estimated.total(), m.total as usize),
    ];
    let table: String = rows
        .iter()
        .map(|(holder, estimated, measured)| {
            format!(
                "\n  {holder:<30} estimated {estimated:>10}, measured {measured:>10}, {:+.1}%",
                (*estimated as f64 - *measured as f64) * 100.0 / (*measured).max(1) as f64
            )
        })
        .collect();
    eprintln!("{table}");
    for (holder, estimated, measured) in rows {
        assert!(
            estimated.abs_diff(measured) * 100 <= measured * ESTIMATE_TOLERANCE,
            "{holder}: estimated {estimated} bytes, measured {measured}, off by more than \
             {ESTIMATE_TOLERANCE}%{table}"
        );
    }
}

/// Messages of the session whose entries fill the sealed-entry cache: more than twice what the
/// cache may keep of them.
const SEALED_MESSAGES: usize = 4_000;

/// A session keeps at most [`SEALED_ENTRIES_MAX_BYTES`] of the sealed entries it reads by id, as
/// the allocator measures them: entries of more than twice the cap in all, each a copy of its
/// own as an entry read back from sealed history is, leave no more than the cap kept, the most
/// recently read ones, and the cache counts what it keeps.
#[test]
fn sealed_entries_read_by_id_hold_no_more_than_their_cap() {
    use std::sync::Arc;

    use broca_protocol::content::Message;

    use super::read::SealedEntryCache;
    use super::{window_entries, StoredEntry, SEALED_ENTRIES_MAX_BYTES};
    use crate::held::held_bytes;

    assert!(
        installed(),
        "the crate's tests count with the counting allocator"
    );
    let backend = MemoryBackend::default();
    // The session stays open while the cache is measured, so the values its facts intern stay
    // held by it, as they are by the session a sealed entry is read from.
    let session = block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(), false)
            .await
            .unwrap();
        write(&mut session, &Shape::production(SEALED_MESSAGES))
            .await
            .unwrap();
        session
    });
    let own = |message: &Arc<Message>| Arc::new(Message::clone(message));
    let entries: Vec<(String, StoredEntry)> = window_entries(&session.conversation, 1, usize::MAX)
        .into_iter()
        .map(|entry| {
            let mid = entry.state.facts.as_ref().unwrap().mid.clone();
            let mut state = entry.state.clone();
            state.facts = state.facts.map(|facts| Arc::new((*facts).clone()));
            state.turn = state.turn.map(|turn| Arc::new((*turn).clone()));
            let copy = StoredEntry {
                ordinal: entry.ordinal,
                message: own(&entry.message),
                next: entry.next.as_ref().map(own),
                state,
                open: entry.open,
            };
            (mid, copy)
        })
        .collect();
    let read: usize = entries.iter().map(|(_, entry)| held_bytes(entry)).sum();
    assert!(read > 2 * SEALED_ENTRIES_MAX_BYTES, "only {read} bytes");
    let (first, last) = (entries[0].0.clone(), entries[entries.len() - 1].0.clone());
    let mut cache = SealedEntryCache::default();
    for (mid, entry) in entries {
        cache.insert(&mid, entry);
    }
    assert!(cache.get(&last).is_some());
    assert!(cache.get(&first).is_none());
    let (kept, counted) = (cache.len(), cache.held_bytes());
    // What the cache holds is what dropping it frees.
    let ((), counts) = measure(|| drop(cache));
    let held = counts.held.unsigned_abs();
    eprintln!("read {read} bytes; kept {kept} entries, counted {counted}, held {held}");
    assert!(counted <= SEALED_ENTRIES_MAX_BYTES);
    assert!(
        held <= SEALED_ENTRIES_MAX_BYTES,
        "the cache holds {held} bytes of sealed entries, over their cap of \
         {SEALED_ENTRIES_MAX_BYTES}"
    );
    drop(session);
}
