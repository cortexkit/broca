//! The media fold refuses to render past a record it didn't take in.

use std::sync::Arc;

use broca_core::media_store::{MediaBytes, MediaReadError};
use broca_protocol::{BindIdentity, MediaRef};
use broca_session_loop::parts::MemoryParts;
use broca_session_store::record::{Record, StepStart};

use super::MediaFold;

fn step(step_id: u64) -> Record {
    Record::StepStart(StepStart {
        run_id: "r".into(),
        step_id,
        mid: None,
    })
}

#[derive(Debug)]
struct NoBytes;

impl MediaBytes for NoBytes {
    fn read_verified(&self, reference: &MediaRef) -> Result<Vec<u8>, MediaReadError> {
        Err(MediaReadError::Missing(reference.sha256.clone()))
    }
}

fn fold() -> MediaFold {
    MediaFold::new(BindIdentity::new("/project", "store-actor", "s1"))
}

#[test]
fn a_batch_that_skips_a_record_stops_the_fold_and_every_later_run_refuses() {
    let parts = MemoryParts::default();
    let mut fold = fold();
    fold.take(&[(1, step(1))], &parts);
    assert_eq!(fold.next_seq(), 2);
    // Record 2 never reached the fold.
    fold.take(&[(3, step(2))], &parts);
    let reason = fold.broken().expect("the fold stopped");
    assert!(reason.contains("expected record 2"), "{reason}");
    let refused = fold
        .for_run(Some(Arc::new(NoBytes)))
        .renderer_inputs()
        .err()
        .expect("a run can't render from a stopped fold");
    assert_eq!(refused, reason);
    // Nothing taken later mends it: the fold can't know what record 2 held.
    fold.take(&[(2, step(2))], &parts);
    assert!(fold.broken().is_some());
}

#[test]
fn records_already_taken_are_passed_over() {
    let parts = MemoryParts::default();
    let mut fold = fold();
    fold.take(&[(1, step(1)), (2, step(2))], &parts);
    // A catch-up that starts before the fold's position repeats nothing.
    fold.take(&[(1, step(1)), (2, step(2)), (3, step(3))], &parts);
    assert_eq!(fold.next_seq(), 4);
    assert!(fold.broken().is_none());
}

#[test]
fn a_run_reads_every_record_after_its_seed_in_order_and_refuses_a_gap() {
    let parts = MemoryParts::default();
    let mut fold = fold();
    fold.take(&[(1, step(1))], &parts);
    let (_, seed, read_tail) = fold
        .for_run(Some(Arc::new(NoBytes)))
        .renderer_inputs()
        .unwrap();
    assert_eq!(seed.at_seq, 1);
    fold.take(&[(2, step(2)), (3, step(3))], &parts);
    let seqs = |frames: Vec<broca_wal::FramedRecord>| -> Vec<u64> {
        frames.iter().map(|frame| frame.seq).collect()
    };
    assert_eq!(seqs(read_tail(1).unwrap()), [2, 3]);
    assert_eq!(seqs(read_tail(3).unwrap()), Vec::<u64>::new());
    fold.take(&[(4, step(4))], &parts);
    // A reader whose head is behind what it already read past is missing record 3.
    let error = read_tail(2).unwrap_err();
    assert!(error.contains("after 2 start at 4"), "{error}");
}

#[test]
fn a_run_without_a_way_to_read_media_objects_refuses_by_name() {
    let error = fold().for_run(None).renderer_inputs().err().unwrap();
    assert!(error.contains("media objects can't be read"), "{error}");
}
