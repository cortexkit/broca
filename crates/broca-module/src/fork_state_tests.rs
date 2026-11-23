// Tests of the fork fold: what a fork's records leave it holding, and the records it refuses.

use broca_session_store::record::{Billed, Fork, ForkEvent, Record};
use serde_json::json;

use super::{ForkState, Turn};

const FORK: &str = "fork-1";

fn fact(event: ForkEvent, at_ms: u64) -> Fork {
    Fork {
        fork_id: FORK.into(),
        event,
        at_ms,
        ask_id: None,
        attempt: None,
        question: None,
        answer: None,
        finish: None,
        usage: None,
        principal: None,
        base: None,
        admission_id: None,
        idle_ms: None,
        retain_ms: None,
        detail: None,
        effort: None,
        effort_applied: None,
    }
}

fn prepared(at_ms: u64) -> Record {
    Record::Fork(Fork {
        principal: Some("colliculus".into()),
        base: Some(json!({"run_id": "r1", "step_id": 2, "request_body_sha256": "ab"})),
        idle_ms: Some(600_000),
        retain_ms: Some(3_600_000),
        ..fact(ForkEvent::Prepared, at_ms)
    })
}

fn started(ask: &str, attempt: u64, question: &str, at_ms: u64) -> Record {
    Record::Fork(Fork {
        ask_id: Some(ask.into()),
        attempt: Some(attempt),
        question: Some(question.into()),
        admission_id: Some(format!("fork:{ask}:{attempt}")),
        ..fact(ForkEvent::AskStarted, at_ms)
    })
}

fn finished(ask: &str, attempt: u64, answer: &str, finish: &str, at_ms: u64) -> Record {
    Record::Fork(Fork {
        ask_id: Some(ask.into()),
        attempt: Some(attempt),
        answer: Some(answer.into()),
        finish: Some(finish.into()),
        usage: Some(json!({"input_tokens": 17, "output_tokens": 11})),
        ..fact(ForkEvent::AskFinished, at_ms)
    })
}

fn of_attempt(event: ForkEvent, ask: &str, attempt: u64, at_ms: u64) -> Record {
    Record::Fork(Fork {
        ask_id: Some(ask.into()),
        attempt: Some(attempt),
        ..fact(event, at_ms)
    })
}

fn released(ask: &str, attempt: u64, at_ms: u64) -> Record {
    Record::Fork(Fork {
        admission_id: Some(format!("fork:{ask}:{attempt}")),
        ..fact(ForkEvent::Released, at_ms)
    })
}

fn fold(records: &[Record]) -> Result<Option<ForkState>, String> {
    ForkState::fold(records)
}

#[test]
fn a_forks_records_fold_into_its_asks_branch_and_clocks() {
    let records = [
        prepared(1_000),
        // A record of another kind in the session is passed over.
        Record::Billed(Billed {
            run_id: "unrelated".into(),
        }),
        started("one", 1, "what changed?", 2_000),
        Record::Fork(Fork {
            usage: Some(json!({"input_tokens": 17})),
            ..match of_attempt(ForkEvent::AskUsage, "one", 1, 2_100) {
                Record::Fork(fork) => fork,
                _ => unreachable!(),
            }
        }),
        finished("one", 1, "the title", "end_turn", 2_500),
        released("one", 1, 2_501),
        of_attempt(ForkEvent::Billed, "one", 1, 2_502),
        // A failed model call stores its answer but stays out of the branch.
        started("two", 1, "failed question", 3_000),
        finished("two", 1, "", "model_error", 3_500),
        // A refused attempt stores nothing; its retry is attempt 2, with the retry's question.
        started("three", 1, "first wording", 4_000),
        of_attempt(ForkEvent::AskRefused, "three", 1, 4_100),
        released("three", 1, 4_101),
        started("three", 2, "second wording", 5_000),
        finished("three", 2, "before tool", "tool_call_refused", 5_500),
        Record::Fork(fact(ForkEvent::Ended, 5_500)),
    ];
    let state = fold(&records).unwrap().unwrap();
    assert_eq!(state.fork_id, FORK);
    assert_eq!(state.principal, "colliculus");
    assert_eq!(state.base["step_id"], 2);
    assert_eq!(
        state.branch,
        vec![
            Turn {
                question: "what changed?".into(),
                answer: "the title".into(),
            },
            Turn {
                question: "second wording".into(),
                answer: "before tool".into(),
            },
        ]
    );
    let one = &state.asks["one"];
    assert_eq!(one.attempts, 1);
    assert_eq!(one.answer.as_ref().unwrap().finish, "end_turn");
    assert_eq!(
        state.asks["two"].answer.as_ref().unwrap().finish,
        "model_error"
    );
    let three = &state.asks["three"];
    assert_eq!(
        (three.attempts, three.question.as_str()),
        (2, "second wording")
    );
    assert_eq!(three.answer.as_ref().unwrap().answer, "before tool");
    assert_eq!(state.in_flight, None);
    assert_eq!(state.ended_at_ms, Some(5_500));
    assert_eq!(state.revoked_at_ms, None);
    // Released admissions and billed attempts are settled; the rest are still owed.
    assert_eq!(
        state.unreleased.iter().collect::<Vec<_>>(),
        ["fork:three:2", "fork:two:1"]
    );
    assert_eq!(
        state.unbilled.iter().cloned().collect::<Vec<_>>(),
        [("three".to_owned(), 2), ("two".to_owned(), 1)]
    );
    // The idle clock restarts at each attempt's end, including the refused one.
    assert_eq!(state.expires_at_ms(), Some(5_500 + 600_000));
    assert!(!state.expired(5_500 + 599_999));
    assert!(state.expired(5_500 + 600_000));
    assert_eq!(state.last_at_ms(), 5_500);
    assert_eq!(state.retained_until_ms(), 5_500 + 600_000 + 3_600_000);
}

#[test]
fn an_attempt_in_flight_holds_expiry_and_keeps_its_recorded_usage() {
    let usage = json!({"input_tokens": 40});
    let records = [
        prepared(1_000),
        started("held", 1, "question", 2_000),
        Record::Fork(Fork {
            ask_id: Some("held".into()),
            attempt: Some(1),
            usage: Some(usage.clone()),
            ..fact(ForkEvent::AskUsage, 2_100)
        }),
    ];
    let state = fold(&records).unwrap().unwrap();
    let open = state.in_flight.as_ref().unwrap();
    assert_eq!((open.ask_id.as_str(), open.attempt), ("held", 1));
    assert_eq!(open.admission_id.as_deref(), Some("fork:held:1"));
    assert_eq!(open.usage.as_ref(), Some(&usage));
    assert_eq!(state.expires_at_ms(), None);
    assert!(!state.expired(u64::MAX));
    // Retention still has a deadline, from the newest record.
    assert_eq!(state.retained_until_ms(), 2_100 + 600_000 + 3_600_000);
}

#[test]
fn a_session_without_fork_records_is_no_fork() {
    assert_eq!(fold(&[]).unwrap(), None);
    assert_eq!(
        fold(&[Record::Billed(Billed { run_id: "r".into() })]).unwrap(),
        None
    );
}

#[test]
fn records_the_server_never_writes_refuse() {
    let refused = |records: &[Record]| fold(records).unwrap_err();
    assert!(refused(&[started("one", 1, "q", 1)]).contains("not its prepare"));
    assert!(refused(&[prepared(1), prepared(2)]).contains("prepared twice"));
    let mut other = match prepared(1) {
        Record::Fork(fork) => fork,
        _ => unreachable!(),
    };
    other.fork_id = "fork-2".into();
    assert!(refused(&[prepared(1), Record::Fork(other)]).contains("fork fork-2"));
    assert!(refused(&[
        prepared(1),
        started("a", 1, "q", 2),
        started("b", 1, "q", 3)
    ])
    .contains("in flight"));
    assert!(refused(&[prepared(1), started("a", 2, "q", 2)]).contains("attempt 2 after 0"));
    assert!(
        refused(&[prepared(1), finished("a", 1, "x", "end_turn", 2)]).contains("isn't in flight")
    );
    assert!(refused(&[
        prepared(1),
        started("a", 1, "q", 2),
        finished("a", 1, "x", "end_turn", 3),
        started("a", 2, "q", 4),
    ])
    .contains("whose answer is stored"));
    let mut bare = match prepared(1) {
        Record::Fork(fork) => fork,
        _ => unreachable!(),
    };
    bare.principal = None;
    assert!(refused(&[Record::Fork(bare)]).contains("no principal"));
}
