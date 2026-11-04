use broca_protocol::content::{ContentBlock, Message, Role};

use super::{read, ChainBase, Damage, LogEnd, LogWriter};
use crate::frame::{FrameError, FrameKey};
use crate::record::{Input, Placement, Record, StepStart};

const BIND: &[u8] = b"session-1/log-1";

fn key() -> FrameKey {
    FrameKey::new(1, &[7; 32])
}

fn input(text: &str) -> Record {
    Record::Input(Input {
        run_id: "r".into(),
        placement: Placement::RunInput,
        message: Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }]),
        written_at_ms: None,
        submission_id: None,
        notice: None,
        tags_v1: None,
        receipt_seq: None,
        pruned: Vec::new(),
    })
}

fn step(step_id: u64) -> Record {
    Record::StepStart(StepStart {
        run_id: "r".into(),
        step_id,
    })
}

/// A log of a checkpoint and three batches, the second of two records, with a barrier after
/// the first batch. Returns the bytes and where each batch starts.
fn sample() -> (Vec<u8>, Vec<u64>) {
    let mut writer = LogWriter::create(
        Vec::new(),
        key(),
        BIND.to_vec(),
        b"checkpoint",
        1,
        ChainBase::default(),
    )
    .unwrap();
    let mut starts = Vec::new();
    starts.push(writer.offset());
    writer.append(&[input("one")]).unwrap();
    writer.barrier().unwrap();
    starts.push(writer.offset());
    writer.append(&[step(1), input("two")]).unwrap();
    starts.push(writer.offset());
    writer.append(&[step(2)]).unwrap();
    (writer.into_file(), starts)
}

fn no_base(_: &[u8]) -> Result<ChainBase, String> {
    Ok(ChainBase::default())
}

fn read_all(bytes: &[u8]) -> super::ReadLog {
    read(bytes, &key(), BIND, 0, no_base).unwrap()
}

#[test]
fn a_clean_log_reads_back_every_record_in_order() {
    let (bytes, _) = sample();
    let log = read_all(&bytes);
    assert_eq!(log.end, LogEnd::Clean);
    assert_eq!(log.checkpoint, b"checkpoint");
    let seqs: Vec<u64> = log.records.iter().map(|(seq, _)| *seq).collect();
    assert_eq!(seqs, [1, 2, 3, 4]);
    assert_eq!(log.records[0].1, input("one"));
    assert_eq!(log.complete_through, bytes.len() as u64);
}

/// A crash in the middle of a write leaves an incomplete batch, and none of its records count,
/// even the frames that arrived whole.
#[test]
fn a_cut_inside_a_batch_is_a_torn_tail_and_drops_the_whole_batch() {
    let (bytes, starts) = sample();
    let second = starts[1] as usize;
    let third = starts[2] as usize;
    // Cut exactly after the batch's first frame arrived whole, and in the middle of a frame.
    let length = u32::from_le_bytes(bytes[second..second + 4].try_into().unwrap()) as usize;
    let first_frame_end = second + 4 + length;
    assert!(first_frame_end < third, "the batch has a second frame");
    for cut in [first_frame_end, second + 10, third - 10] {
        let log = read_all(&bytes[..cut]);
        assert_eq!(
            log.end,
            LogEnd::TornTail { at: second as u64 },
            "cut at {cut}"
        );
        assert_eq!(log.records.len(), 1, "cut at {cut}");
        assert_eq!(log.complete_through, second as u64);
    }
}

#[test]
fn a_flipped_byte_is_damage_never_a_torn_tail() {
    let (bytes, starts) = sample();
    let second = starts[1] as usize;
    // In the body: authentication fails.
    let mut body = bytes.clone();
    body[second + 70] ^= 1;
    assert_eq!(
        read_all(&body).end,
        LogEnd::Damaged {
            at: second as u64,
            damage: Damage::Frame(FrameError::Authentication),
        }
    );
    // In the length field: the header checksum fails, so a damaged length can't pass for a
    // write that never finished.
    let mut length = bytes.clone();
    length[second] ^= 0x40;
    assert_eq!(
        read_all(&length).end,
        LogEnd::Damaged {
            at: second as u64,
            damage: Damage::Frame(FrameError::HeaderChecksum),
        }
    );
}

/// A frame removed from the middle of a log fails the index check, even though every
/// remaining frame authenticates.
#[test]
fn a_removed_batch_is_an_index_gap() {
    let (bytes, starts) = sample();
    let mut spliced = bytes[..starts[1] as usize].to_vec();
    spliced.extend_from_slice(&bytes[starts[2] as usize..]);
    assert_eq!(
        read_all(&spliced).end,
        LogEnd::Damaged {
            at: starts[1],
            damage: Damage::IndexGap {
                expected: 2,
                found: 4,
            },
        }
    );
}

/// The same bytes under another log's name don't authenticate.
#[test]
fn a_log_read_under_another_name_fails_authentication() {
    let (bytes, _) = sample();
    let log = read(&bytes, &key(), b"session-1/log-2", 0, no_base).unwrap();
    assert_eq!(
        log.end,
        LogEnd::Damaged {
            at: 0,
            damage: Damage::Frame(FrameError::Authentication),
        }
    );
}

/// Frames claim the last completed barrier, and a claim-only frame claims the barrier no later
/// batch did.
#[test]
fn the_durable_bound_follows_barriers_and_claim_only_frames() {
    let (bytes, starts) = sample();
    // The barrier after the first batch covers it; later frames claim that much.
    assert_eq!(read_all(&bytes).durable_bound, starts[1]);
    // The length synced when the log was created counts too.
    let created = read(&bytes, &key(), BIND, starts[0], no_base).unwrap();
    assert_eq!(created.durable_bound, starts[1]);

    let mut writer = LogWriter::create(
        Vec::new(),
        key(),
        BIND.to_vec(),
        b"checkpoint",
        1,
        ChainBase::default(),
    )
    .unwrap();
    writer.append(&[input("one")]).unwrap();
    writer.barrier().unwrap();
    let synced = writer.offset();
    writer.claim().unwrap();
    let bytes = writer.into_file();
    let log = read_all(&bytes);
    assert_eq!(log.end, LogEnd::Clean);
    assert_eq!(log.durable_bound, synced);
    assert_eq!(log.complete_through, bytes.len() as u64);
}

/// A log written to a real file and synced reads back the same.
#[test]
fn a_file_backed_log_reads_back() {
    let dir = std::env::temp_dir().join(format!("broca-log-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("log");
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = LogWriter::create(
        file,
        key(),
        BIND.to_vec(),
        b"checkpoint",
        5,
        ChainBase::default(),
    )
    .unwrap();
    writer.append(&[input("one"), step(1)]).unwrap();
    writer.barrier().unwrap();
    drop(writer);
    let bytes = std::fs::read(&path).unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
    let log = read_all(&bytes);
    assert_eq!(log.end, LogEnd::Clean);
    let seqs: Vec<u64> = log.records.iter().map(|(seq, _)| *seq).collect();
    assert_eq!(seqs, [5, 6]);
}
