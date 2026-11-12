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
/// The whole session. Today: 3.063. Target: the design's idle budget, about 1,288 at this
/// session's 4.5 KB per message (1.2 x W + 400 B x messages).
const TOTAL: u64 = 3_370;
/// The window's messages. Today: 1.114. Target: about 1,000, content held at its sent size.
const HELD_MESSAGES: u64 = 1_226;
/// The window messages' entry states. Today: 0.254 (1,160 bytes per message). Target: 250
/// bytes per message, about 55.
const ENTRY_FACTS: u64 = 280;
/// The kept records' contents. Today: 0.371. Target: their own small fields only, once they
/// share content with the folds.
const KEPT_RECORDS: u64 = 408;
/// The kept records' buffer beyond its records. Today: 0.992. Target: none, a buffer sized for
/// the latest run.
const KEPT_SPARE: u64 = 1_092;
/// The id index. Today: 0.323. Target: positions only, a few bytes per id.
const ID_INDEX: u64 = 356;
/// Everything else. Today: 0.008 (38 KB). Target: a small share of the idle budget. Rounded up
/// to the next thousandth.
const REST: u64 = 10;
/// Bytes allocated while loading, freed or not. Today: 10.032. Target: none set by the design;
/// opening folds the log record by record and never holds it whole.
const ALLOCATED_DURING_LOAD: u64 = 11_037;

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
/// holder apart, measuring what dropping it frees.
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
    let messages = freed(std::mem::take(&mut session.conversation.messages));
    let entry_facts = freed(std::mem::take(&mut session.conversation.entries));
    let kept_spare = estimated.kept_capacity as u64;
    let kept = freed(std::mem::take(&mut session.kept));
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
