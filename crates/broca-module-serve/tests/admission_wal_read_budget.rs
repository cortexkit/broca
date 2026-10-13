//! Guard admission of a fresh episode in the live executor against reading and decoding the
//! write-ahead log (WAL) history again: the executor already receives it in a persisted snapshot.

#[test]
fn fresh_episode_admission_does_not_add_a_full_wal_read() {
    let source = include_str!("../src/main.rs");
    let executor = source
        .split_once("impl RunExecutor for LiveLoopExecutor")
        .expect("executor implementation")
        .1;
    let execute = executor
        .split_once("async fn execute(")
        .expect("execute method")
        .1;
    let admission = execute
        .split_once("let summary = run_loop_on_writer_admitted_with_prior_snapshot(")
        .expect("run-loop boundary")
        .0;
    let compact: String = admission
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect();
    // The executor already receives the persisted checkpoint and the records after it from
    // the session actor. Reading the WAL here would decode the same history again before the
    // run starts, even when the checkpoint is current.
    assert!(
        !compact.contains(".read_records("),
        "executor admission must use its existing durable snapshot, not a full-WAL read"
    );
}
