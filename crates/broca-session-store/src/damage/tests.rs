use broca_protocol::content::{ContentBlock, Message, Role};

use super::{analyse, Debris, Lost, Verdict};
use crate::frame::{self, FrameHeader, FrameKey, FrameStream};
use crate::log::{ChainBase, LogEnd, LogError, LogWriter};
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

/// A log and where its pieces start.
struct Sample {
    bytes: Vec<u8>,
    /// The length synced when the log was created: its checkpoint.
    created: u64,
    /// The durable bound: the barrier after the first batch.
    synced: u64,
    /// Where each batch and the claim-only frame start, in order: T1, L2, (T3 L4), claim,
    /// (T5 L6), then where L6's frame starts inside the last batch.
    starts: Vec<u64>,
}

/// A checkpoint; `input a` (transcript 1); a barrier; `step 1` (ledger 2); one batch of
/// `input b` and `step 2` (transcript 3, ledger 4); a claim-only frame; one batch of `input c`
/// and `step 3` (transcript 5, ledger 6). Everything after the barrier is unsynced.
fn sample() -> Sample {
    let mut writer = LogWriter::create(
        Vec::new(),
        key(),
        BIND.to_vec(),
        b"checkpoint",
        1,
        ChainBase::default(),
    )
    .unwrap();
    let created = writer.offset();
    let mut starts = vec![writer.offset()];
    writer.append(&[input("a")]).unwrap();
    writer.barrier().unwrap();
    let synced = writer.offset();
    starts.push(writer.offset());
    writer.append(&[step(1)]).unwrap();
    starts.push(writer.offset());
    writer.append(&[input("b"), step(2)]).unwrap();
    starts.push(writer.offset());
    writer.claim().unwrap();
    starts.push(writer.offset());
    writer.append(&[input("c"), step(3)]).unwrap();
    let bytes = writer.into_file();
    // The first frame's length field gives where the batch's second frame starts.
    let last = starts[4] as usize;
    let first_len = u32::from_le_bytes(bytes[last..last + 4].try_into().unwrap());
    starts.push(starts[4] + 4 + u64::from(first_len));
    Sample {
        bytes,
        created,
        synced,
        starts,
    }
}

fn base(_: &[u8]) -> Result<ChainBase, String> {
    Ok(ChainBase::default())
}

fn verdict(bytes: &[u8], created: u64) -> Verdict {
    analyse(bytes, &key(), BIND, created, base, None).unwrap()
}

fn as_debris(verdict: Verdict) -> Debris {
    match verdict {
        Verdict::Debris(debris) => debris,
        other => panic!("{other:?}"),
    }
}

fn flip(bytes: &mut [u8], at: u64) {
    bytes[at as usize] ^= 0x40;
}

#[test]
fn a_clean_log_opens() {
    let sample = sample();
    assert!(matches!(
        verdict(&sample.bytes, sample.created),
        Verdict::Opens(log) if log.end == LogEnd::Clean
    ));
}

/// A cut anywhere past the durable bound is an append that never synced: the log opens, and its
/// reader stops before the cut batch.
#[test]
fn a_cut_after_the_durable_bound_opens() {
    let sample = sample();
    for len in sample.synced as usize + 1..sample.bytes.len() {
        match verdict(&sample.bytes[..len], sample.created) {
            Verdict::Opens(log) => assert!(
                log.complete_through <= len as u64,
                "cut at {len}: {:?}",
                log.end
            ),
            other => panic!("cut at {len}: {other:?}"),
        }
    }
}

/// Zeros from a frame boundary to the end, as a filesystem leaves blocks it allocated for an
/// unsynced append, are a torn tail too, and so is a zero-filled extension of a clean log.
#[test]
fn a_zero_filled_tail_opens() {
    let sample = sample();
    for at in &sample.starts[1..5] {
        let mut bytes = sample.bytes.clone();
        bytes[*at as usize..].fill(0);
        match verdict(&bytes, sample.created) {
            Verdict::Opens(log) => {
                assert_eq!(log.end, LogEnd::TornTail { at: *at }, "zeros from {at}")
            }
            other => panic!("zeros from {at}: {other:?}"),
        }
    }
    let mut bytes = sample.bytes.clone();
    bytes.extend(std::iter::repeat_n(0, 4096));
    let len = sample.bytes.len() as u64;
    assert!(matches!(
        verdict(&bytes, sample.created),
        Verdict::Opens(log) if log.end == LogEnd::TornTail { at: len }
    ));
}

/// Damage to the ledger frame `step 1`, with frames after it. The transcript's next record links
/// back past the damage, so it lost nothing; the ledger's doesn't, so it may have.
#[test]
fn debris_in_the_middle_says_which_stream_may_have_lost_a_record() {
    let sample = sample();
    let mut bytes = sample.bytes.clone();
    flip(&mut bytes, sample.starts[1] + 60);
    let debris = as_debris(verdict(&bytes, sample.created));
    assert_eq!(debris.regions.len(), 1);
    assert_eq!(debris.regions[0].bytes, sample.starts[1]..sample.starts[2]);
    assert_eq!(debris.regions[0].transcript, Lost::Nothing);
    assert_eq!(debris.regions[0].ledger, Lost::CantProve);
    assert!(!debris.loss_free());
    let seqs: Vec<u64> = debris
        .harvested
        .iter()
        .filter(|frame| frame.record.is_some())
        .map(|frame| frame.header.seq)
        .collect();
    assert_eq!(seqs, [1, 3, 4, 5, 6]);
    assert_eq!(debris.torn_tail, None);
}

/// Damage to the claim-only frame removes no record: both streams link across it.
#[test]
fn debris_without_records_is_loss_free() {
    let sample = sample();
    let mut bytes = sample.bytes.clone();
    flip(&mut bytes, sample.starts[3] + 60);
    let debris = as_debris(verdict(&bytes, sample.created));
    assert_eq!(debris.regions.len(), 1);
    assert!(debris.loss_free());
}

/// A frame removed whole leaves no damaged bytes, but its index is missing.
#[test]
fn a_removed_frame_is_a_region() {
    let sample = sample();
    let mut bytes = sample.bytes.clone();
    bytes.drain(sample.starts[1] as usize..sample.starts[2] as usize);
    let debris = as_debris(verdict(&bytes, sample.created));
    assert_eq!(debris.regions.len(), 1);
    assert_eq!(debris.regions[0].bytes, sample.starts[1]..sample.starts[1]);
    assert_eq!(debris.regions[0].transcript, Lost::Nothing);
    assert_eq!(debris.regions[0].ledger, Lost::CantProve);
}

/// Damage to the last frame has nothing after it to prove anything: both streams can't prove.
/// It isn't a torn tail, because the frame is complete and fails authentication.
#[test]
fn damage_at_the_end_cannot_be_proven_loss_free() {
    let sample = sample();
    let mut bytes = sample.bytes.clone();
    flip(&mut bytes, sample.starts[5] + 60);
    let debris = as_debris(verdict(&bytes, sample.created));
    assert_eq!(debris.regions.len(), 1);
    assert_eq!(debris.regions[0].transcript, Lost::CantProve);
    assert_eq!(debris.regions[0].ledger, Lost::CantProve);
    assert_eq!(debris.torn_tail, None);
}

/// Debris in the middle and a torn tail at the end: the tail is set apart, its frames aren't
/// harvested, and it counts as no loss.
#[test]
fn a_torn_tail_after_debris_is_set_apart() {
    let sample = sample();
    let mut bytes = sample.bytes.clone();
    flip(&mut bytes, sample.starts[1] + 60);
    // Cut inside the last batch's second frame: its first frame is intact but the batch is not.
    bytes.truncate(sample.starts[5] as usize + 30);
    let debris = as_debris(verdict(&bytes, sample.created));
    assert_eq!(debris.torn_tail, Some(sample.starts[4]));
    assert_eq!(debris.regions.len(), 1);
    assert!(debris
        .harvested
        .iter()
        .all(|frame| frame.at < sample.starts[4]));
}

/// The same damage inside bytes a barrier acknowledged is corruption: nothing may be cut.
#[test]
fn damage_below_the_durable_bound_is_corruption() {
    let sample = sample();
    let mut bytes = sample.bytes.clone();
    flip(&mut bytes, sample.starts[0] + 60);
    assert!(matches!(
        verdict(&bytes, sample.created),
        Verdict::Corrupt { at, .. } if at == sample.starts[0]
    ));
}

/// A damaged checkpoint is repaired wherever the durable bound lies, because recovery rebuilds
/// it. With the rebuilt chain base, the loss is decided as usual; without it, it can't be.
#[test]
fn a_damaged_checkpoint_is_debris() {
    let sample = sample();
    let mut bytes = sample.bytes.clone();
    flip(&mut bytes, 60);
    let rebuilt = analyse(
        &bytes,
        &key(),
        BIND,
        sample.created,
        base,
        Some(ChainBase::default()),
    );
    let debris = as_debris(rebuilt.unwrap());
    assert_eq!(debris.checkpoint, None);
    assert_eq!(debris.regions[0].bytes, 0..sample.starts[0]);
    assert!(debris.loss_free());
    let unknown = as_debris(analyse(&bytes, &key(), BIND, sample.created, base, None).unwrap());
    assert!(!unknown.loss_free());
}

/// A frame the writer authenticated but this build can't decode refuses, even after damage.
#[test]
fn an_undecodable_frame_refuses_wherever_it_lies() {
    let sample = sample();
    let mut bytes = sample.bytes.clone();
    flip(&mut bytes, sample.starts[1] + 60);
    let header = FrameHeader {
        flags: 0,
        stream: FrameStream::Transcript,
        kind: 63,
        kind_version: 1,
        key_id: 1,
        seq: 7,
        batch_first: 7,
        batch_count: 1,
        frame_index: 8,
        synced_through: sample.synced,
        prev_in_stream: Some(5),
    };
    bytes.extend(frame::seal(&header, b"unknown", &key(), BIND).unwrap());
    assert!(matches!(
        analyse(&bytes, &key(), BIND, sample.created, base, None),
        Err(LogError::Undecodable { .. })
    ));
}

/// One flipped byte anywhere is damage, never a torn tail. In the checkpoint it is debris that
/// recovery rebuilds, in acknowledged bytes it is corruption, and past the durable bound it is
/// debris whose harvested records are exactly the originals at their seqs.
#[test]
fn a_flipped_byte_anywhere_is_classified_by_where_it_lies() {
    let sample = sample();
    let original = match verdict(&sample.bytes, sample.created) {
        Verdict::Opens(log) => log.records,
        other => panic!("{other:?}"),
    };
    for at in 0..sample.bytes.len() as u64 {
        let mut bytes = sample.bytes.clone();
        flip(&mut bytes, at);
        match verdict(&bytes, sample.created) {
            Verdict::Debris(debris) if at < sample.created => {
                assert_eq!(debris.checkpoint, None, "flip at {at}")
            }
            Verdict::Corrupt { .. } if (sample.created..sample.synced).contains(&at) => {}
            Verdict::Debris(debris) if at >= sample.synced => {
                assert!(debris.checkpoint.is_some(), "flip at {at}");
                for frame in &debris.harvested {
                    let Some(record) = &frame.record else {
                        continue;
                    };
                    let kept = original.iter().find(|(seq, _)| *seq == frame.header.seq);
                    assert_eq!(kept.map(|(_, r)| r), Some(record), "flip at {at}");
                }
                assert!(
                    debris.regions.iter().any(|region| {
                        region.bytes.start <= at
                            && at < region.bytes.end.max(region.bytes.start + 1)
                    }),
                    "flip at {at}: {:?}",
                    debris.regions
                );
            }
            other => panic!("flip at {at}: {other:?}"),
        }
    }
}

/// Two frames removed whole are two regions. The log reader only reports the first.
#[test]
fn two_removed_frames_are_two_regions() {
    let sample = sample();
    let mut bytes = sample.bytes.clone();
    bytes.drain(sample.starts[3] as usize..sample.starts[4] as usize);
    bytes.drain(sample.starts[1] as usize..sample.starts[2] as usize);
    let debris = as_debris(verdict(&bytes, sample.created));
    assert_eq!(debris.regions.len(), 2, "{:?}", debris.regions);
}

/// Zeros where a barrier acknowledged bytes are not an unsynced append: they are corruption.
/// Here the whole log counts as synced when it was created.
#[test]
fn zeros_inside_acknowledged_bytes_are_corruption() {
    let sample = sample();
    let mut bytes = sample.bytes.clone();
    bytes[sample.starts[4] as usize..].fill(0);
    let created = sample.bytes.len() as u64;
    assert!(matches!(
        verdict(&bytes, created),
        Verdict::Corrupt { at, .. } if at == sample.starts[4]
    ));
}
