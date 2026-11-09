use broca_protocol::content::{ContentBlock, Message, Role};

use super::{read, ChainBase, Damage, LogEnd, LogWriter};
use crate::frame::{FrameError, FrameKey};
use crate::keys::Cipher;
use crate::record::{Input, Placement, Record, StepStart};

const BIND: &[u8] = b"session-1/log-1";

cipher_tests!(
    a_clean_log_reads_back_every_record_in_order,
    a_cut_inside_a_batch_is_a_torn_tail_and_drops_the_whole_batch,
    a_flipped_byte_is_damage_never_a_torn_tail,
    a_removed_batch_is_an_index_gap,
    a_log_read_under_another_name_fails_authentication,
    the_durable_bound_follows_barriers_and_claim_only_frames,
    a_file_backed_log_reads_back,
    a_copied_batch_keeps_its_seqs_and_stays_all_or_nothing,
);

/// The test key in mode `cipher`.
fn key(cipher: Cipher) -> FrameKey {
    FrameKey::for_tests(cipher, 1, &[7; 32])
}

fn input(text: &str) -> Record {
    Record::Input(Input {
        media: Vec::new(),
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
        mid: None,
    })
}

/// A log of a checkpoint and three batches, the second of two records, with a barrier after
/// the first batch. Returns the bytes and where each batch starts.
fn sample(cipher: Cipher) -> (Vec<u8>, Vec<u64>) {
    let mut writer = LogWriter::create(
        Vec::new(),
        key(cipher),
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

fn read_all(bytes: &[u8], cipher: Cipher) -> super::ReadLog {
    read(bytes, &key(cipher), BIND, 0, no_base).unwrap()
}

fn a_clean_log_reads_back_every_record_in_order(cipher: Cipher) {
    let (bytes, _) = sample(cipher);
    let log = read_all(&bytes, cipher);
    assert_eq!(log.end, LogEnd::Clean);
    assert_eq!(log.checkpoint, b"checkpoint");
    let seqs: Vec<u64> = log.records.iter().map(|(seq, _)| *seq).collect();
    assert_eq!(seqs, [1, 2, 3, 4]);
    assert_eq!(log.records[0].1, input("one"));
    assert_eq!(log.complete_through, bytes.len() as u64);
}

/// A crash in the middle of a write leaves an incomplete batch, and none of its records count,
/// even the frames that arrived whole.
fn a_cut_inside_a_batch_is_a_torn_tail_and_drops_the_whole_batch(cipher: Cipher) {
    let (bytes, starts) = sample(cipher);
    let second = starts[1] as usize;
    let third = starts[2] as usize;
    // Cut exactly after the batch's first frame arrived whole, and in the middle of a frame.
    let length = u32::from_le_bytes(bytes[second..second + 4].try_into().unwrap()) as usize;
    let first_frame_end = second + 4 + length;
    assert!(first_frame_end < third, "the batch has a second frame");
    for cut in [first_frame_end, second + 10, third - 10] {
        let log = read_all(&bytes[..cut], cipher);
        assert_eq!(
            log.end,
            LogEnd::TornTail { at: second as u64 },
            "cut at {cut}"
        );
        assert_eq!(log.records.len(), 1, "cut at {cut}");
        assert_eq!(log.complete_through, second as u64);
    }
}

fn a_flipped_byte_is_damage_never_a_torn_tail(cipher: Cipher) {
    let (bytes, starts) = sample(cipher);
    let second = starts[1] as usize;
    // In the body, which starts after the header and, in an encrypted frame, its nonce:
    // authentication fails, or the plain check does.
    let mut body = bytes.clone();
    body[second + key(cipher).unit_overhead() + 30] ^= 1;
    assert_eq!(
        read_all(&body, cipher).end,
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
        read_all(&length, cipher).end,
        LogEnd::Damaged {
            at: second as u64,
            damage: Damage::Frame(FrameError::HeaderChecksum),
        }
    );
}

/// A frame removed from the middle of a log fails the index check, even though every
/// remaining frame authenticates.
fn a_removed_batch_is_an_index_gap(cipher: Cipher) {
    let (bytes, starts) = sample(cipher);
    let mut spliced = bytes[..starts[1] as usize].to_vec();
    spliced.extend_from_slice(&bytes[starts[2] as usize..]);
    assert_eq!(
        read_all(&spliced, cipher).end,
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
fn a_log_read_under_another_name_fails_authentication(cipher: Cipher) {
    let (bytes, _) = sample(cipher);
    let log = read(&bytes, &key(cipher), b"session-1/log-2", 0, no_base).unwrap();
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
fn the_durable_bound_follows_barriers_and_claim_only_frames(cipher: Cipher) {
    let (bytes, starts) = sample(cipher);
    // The barrier after the first batch covers it; later frames claim that much.
    assert_eq!(read_all(&bytes, cipher).durable_bound, starts[1]);
    // The length synced when the log was created counts too.
    let created = read(&bytes, &key(cipher), BIND, starts[0], no_base).unwrap();
    assert_eq!(created.durable_bound, starts[1]);

    let mut writer = LogWriter::create(
        Vec::new(),
        key(cipher),
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
    let log = read_all(&bytes, cipher);
    assert_eq!(log.end, LogEnd::Clean);
    assert_eq!(log.durable_bound, synced);
    assert_eq!(log.complete_through, bytes.len() as u64);
}

/// A log written to a real file and synced reads back the same.
fn a_file_backed_log_reads_back(cipher: Cipher) {
    let dir = std::env::temp_dir().join(format!(
        "broca-log-test-{}-{}",
        std::process::id(),
        cipher.label()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("log");
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = LogWriter::create(
        file,
        key(cipher),
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
    let log = read_all(&bytes, cipher);
    assert_eq!(log.end, LogEnd::Clean);
    let seqs: Vec<u64> = log.records.iter().map(|(seq, _)| *seq).collect();
    assert_eq!(seqs, [5, 6]);
}

/// A batch copied into a new log keeps its records' seqs, which may skip, and reads back as one
/// batch. A frame cut out of it still reads as damage, and copied seqs that go backwards refuse.
fn a_copied_batch_keeps_its_seqs_and_stays_all_or_nothing(cipher: Cipher) {
    use super::{LogEncoder, LogError};
    let mut encoder = LogEncoder::new(
        key(cipher),
        BIND.to_vec(),
        20,
        ChainBase {
            transcript: Some(3),
            ledger: Some(19),
        },
    );
    let mut bytes = encoder
        .checkpoint(b"checkpoint")
        .unwrap()
        .into_written(&mut encoder);
    let (one, three) = (input("one"), input("three"));
    // Seqs 5 and 8: the ledger records between them stayed in the old log.
    bytes.extend(
        encoder
            .copy_batch(&[(5, &one), (8, &three)])
            .unwrap()
            .into_written(&mut encoder),
    );
    let after_copy = bytes.len();
    // New records follow at the encoder's next seq.
    bytes.extend(
        encoder
            .batch(&[step(1)])
            .unwrap()
            .into_written(&mut encoder),
    );
    let base = |_: &[u8]| {
        Ok(ChainBase {
            transcript: Some(3),
            ledger: Some(19),
        })
    };
    let log = read(&bytes, &key(cipher), BIND, 0, base).unwrap();
    assert_eq!(log.end, LogEnd::Clean);
    let seqs: Vec<u64> = log.records.iter().map(|(seq, _)| *seq).collect();
    assert_eq!(seqs, [5, 8, 20]);
    assert_eq!(log.batches, [0..2, 2..3]);

    // Cutting the copied batch's second frame out leaves a frame-index gap. Every frame of
    // `one` has the same length, so the first frame ends where a one-record copy would.
    let first_frame_end = {
        let mut only = LogEncoder::new(key(cipher), BIND.to_vec(), 20, ChainBase::default());
        let checkpoint = only
            .checkpoint(b"checkpoint")
            .unwrap()
            .into_written(&mut only);
        checkpoint.len() + only.copy_batch(&[(5, &one)]).unwrap().bytes.len()
    };
    let mut gap = bytes[..first_frame_end].to_vec();
    gap.extend_from_slice(&bytes[after_copy..]);
    let log = read(&gap, &key(cipher), BIND, 0, base).unwrap();
    assert!(matches!(log.end, LogEnd::Damaged { .. }), "{:?}", log.end);
    assert!(log.records.is_empty());

    // Copied seqs must increase and follow what the log already holds.
    assert!(matches!(
        encoder.copy_batch(&[(9, &one)]),
        Err(LogError::CopiedSeqs)
    ));
    let mut fresh = LogEncoder::new(key(cipher), BIND.to_vec(), 20, ChainBase::default());
    fresh.checkpoint(b"c").unwrap().into_written(&mut fresh);
    assert!(matches!(
        fresh.copy_batch(&[(8, &one), (5, &three)]),
        Err(LogError::CopiedSeqs)
    ));
    assert!(matches!(
        fresh.copy_batch(&[(20, &one)]),
        Err(LogError::CopiedSeqs)
    ));
}
