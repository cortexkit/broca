use broca_protocol::content::{ContentBlock, Message, Role};

use super::{build, groups, Segment, SegmentError, SegmentMeta};
use crate::codec::Id128;
use crate::frame::FrameKey;
use crate::record::{Event, Input, Placement, Record};

fn key() -> FrameKey {
    FrameKey::new(1, &[7; 32])
}

fn meta() -> SegmentMeta {
    SegmentMeta {
        session_id: Id128([1; 16]),
        created_at_ms: 10,
        prefixes: Vec::new(),
        leading: Some(b"leading state".to_vec()),
        trailer: None,
        placed_before: Vec::new(),
    }
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

/// Many transcript records, each about `size` bytes, alternating user and assistant turns.
fn transcript(count: u64, size: usize) -> Vec<(u64, Record)> {
    (1..=count)
        .map(|seq| (seq * 2, input(&format!("{seq:04}{}", "x".repeat(size)))))
        .collect()
}

#[test]
fn every_record_reads_back_from_its_block() {
    let records = transcript(200, 400);
    let bytes = build(&key(), b"objects/s/c/seg-1", &records, meta()).unwrap();
    let key = key();
    let segment = Segment::open(&bytes, &key, b"objects/s/c/seg-1").unwrap();
    assert!(
        segment.index.blocks.len() > 3,
        "{}",
        segment.index.blocks.len()
    );
    assert_eq!(segment.index.leading.as_ref().unwrap().0, b"leading state");
    for (seq, record) in &records {
        assert_eq!(segment.find(*seq).unwrap().as_ref(), Some(record));
        // Odd seqs fall between records and are absent.
        assert_eq!(segment.find(seq + 1).unwrap(), None);
    }
    assert_eq!(segment.find(1).unwrap(), None);
    let all: Vec<_> = (0..segment.index.blocks.len())
        .flat_map(|block| segment.block(block).unwrap())
        .collect();
    assert_eq!(all, records);
}

#[test]
fn a_group_never_splits_across_blocks() {
    // A user input, then records that join its group: none starts a block.
    let mut records = vec![(1, input("start"))];
    for seq in 2..40 {
        records.push((
            seq,
            Record::ResultAppend(crate::record::ResultAppend {
                run_id: "r".into(),
                tool_call_id: broca_protocol::ToolCallId(format!("c{seq}")),
                text: "y".repeat(1_000),
                notice: None,
                written_at_ms: None,
            }),
        ));
    }
    records.push((40, input("next")));
    assert_eq!(groups(&records), vec![0..39, 39..40]);
    let bytes = build(&key(), b"n", &records, meta()).unwrap();
    let key = key();
    let segment = Segment::open(&bytes, &key, b"n").unwrap();
    // The 39-record group is larger than a block, so it is a block of its own.
    assert_eq!(segment.index.blocks.len(), 2);
    assert_eq!(segment.index.blocks[0].records, 39);
}

#[test]
fn a_segment_read_under_another_name_or_key_refuses() {
    let records = transcript(50, 400);
    let bytes = build(&key(), b"name-a", &records, meta()).unwrap();
    let key = key();
    assert!(matches!(
        Segment::open(&bytes, &key, b"name-b"),
        Err(SegmentError::Authentication)
    ));
    let other = FrameKey::new(1, &[8; 32]);
    assert!(matches!(
        Segment::open(&bytes, &other, b"name-a"),
        Err(SegmentError::Authentication)
    ));
}

#[test]
fn a_changed_or_moved_block_refuses() {
    let records = transcript(200, 400);
    let bytes = build(&key(), b"n", &records, meta()).unwrap();
    let key = key();
    let segment = Segment::open(&bytes, &key, b"n").unwrap();
    let first = segment.index.blocks[0].clone();
    let second = segment.index.blocks[1].clone();

    let mut changed = bytes.clone();
    changed[first.offset as usize + 30] ^= 1;
    let segment = Segment::open(&changed, &key, b"n").unwrap();
    assert!(matches!(
        segment.block(0),
        Err(SegmentError::Authentication)
    ));

    // Swap the first two blocks' bytes when they are the same length, or overwrite the
    // second with the first: either way block 1 now holds block 0's sealed bytes.
    let mut moved = bytes.clone();
    let source = bytes[first.offset as usize..(first.offset + first.len) as usize].to_vec();
    if first.len == second.len {
        moved[second.offset as usize..(second.offset + second.len) as usize]
            .copy_from_slice(&source);
        let segment = Segment::open(&moved, &key, b"n").unwrap();
        assert!(matches!(
            segment.block(1),
            Err(SegmentError::Authentication)
        ));
    }

    // A changed header changes what every unit authenticates.
    let mut header = bytes.clone();
    header[12] ^= 1;
    assert!(Segment::open(&header, &key, b"n").is_err());
}

#[test]
fn records_of_both_streams_or_out_of_order_refuse() {
    let ledger = Record::Event(Event {
        name: "e".into(),
        run_id: None,
        body: None,
    });
    assert!(matches!(
        build(&key(), b"n", &[(1, input("a")), (2, ledger)], meta()),
        Err(SegmentError::BadRecords(_))
    ));
    assert!(matches!(
        build(&key(), b"n", &[(2, input("a")), (1, input("b"))], meta()),
        Err(SegmentError::BadRecords(_))
    ));
}
