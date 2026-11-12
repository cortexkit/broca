use broca_wire::{MemoryHolders, MemorySession};

use super::{assemble, ActorMemory, SetupHeld, LARGEST};

fn answer(address: &str, total: u64, running: bool, setup: Vec<SetupHeld>) -> ActorMemory {
    ActorMemory {
        session: MemorySession {
            address: address.into(),
            run_in_progress: running,
            holders: MemoryHolders {
                messages: total,
                ..Default::default()
            }
            .summed(),
            ..Default::default()
        },
        setup,
    }
}

fn tools(content: &str, copy: usize) -> SetupHeld {
    SetupHeld {
        kind: "tools",
        content: content.into(),
        copy,
        bytes: 1_000,
    }
}

/// Sessions come largest first, the largest ten are named, totals add up, and running sessions
/// are counted apart.
#[test]
fn sessions_are_ordered_by_size_and_summed() {
    let answers = (0..12)
        .map(|n| answer(&format!("s{n:02}"), 100 * (n + 1), n % 4 == 0, Vec::new()))
        .collect();
    let report = assemble(answers, 3, 2, Default::default(), Default::default());
    assert_eq!(report.sessions[0].address, "s11");
    assert_eq!(report.largest.len(), LARGEST);
    assert_eq!(report.largest[0], "s11");
    assert_eq!(report.totals.total, (1..=12).map(|n| 100 * n).sum::<u64>());
    assert_eq!(report.running_sessions, 3);
    assert_eq!(report.legacy_sessions, 3);
    assert_eq!(report.unanswered_sessions, 2);
}

/// A part counts the sessions holding it and the distinct copies among them: two sessions
/// sharing one copy hold one, a third decoding its own makes two.
#[test]
fn setup_parts_count_sessions_and_distinct_copies() {
    let answers = vec![
        answer("a", 1, false, vec![tools("plan", 1)]),
        answer("b", 1, false, vec![tools("plan", 1)]),
        answer("c", 1, false, vec![tools("plan", 2), tools("other", 3)]),
    ];
    let report = assemble(answers, 0, 0, Default::default(), Default::default());
    let plan = &report.setup_parts[0];
    assert_eq!((plan.sessions, plan.decoded_copies), (3, 2));
    let other = &report.setup_parts[1];
    assert_eq!((other.sessions, other.decoded_copies), (1, 1));
}
