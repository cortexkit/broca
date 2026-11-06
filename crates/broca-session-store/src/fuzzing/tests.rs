//! Tests that replay the fuzz targets' committed inputs, and the property tests for the same
//! decoders.

use std::path::{Path, PathBuf};

use broca_protocol::content::ContentBlock;

use super::*;

/// A fuzz target's entry point.
type EntryPoint = fn(&[u8]);

/// Every committed input of every fuzz target: its seed corpus, and the inputs of crashes a fuzz
/// run found, run through the same entry point the target calls.
#[test]
fn every_committed_fuzz_input_replays_clean() {
    let fuzz = Path::new(env!("CARGO_MANIFEST_DIR")).join("fuzz");
    let targets: [(&str, EntryPoint); 5] = [
        ("record", record),
        ("frame_log", frame_log),
        ("segment", segment),
        ("head_checkpoint", head_checkpoint),
        ("directory_idindex", directory_idindex),
    ];
    let mut replayed = 0;
    for (target, entry) in targets {
        for dir in ["corpus", "regressions"] {
            for file in files(&fuzz.join(dir).join(target)) {
                let data = std::fs::read(&file).unwrap();
                entry(&data);
                replayed += 1;
            }
        }
    }
    assert!(
        replayed > 0,
        "no committed fuzz input found under {}",
        fuzz.display()
    );
}

fn files(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut files: Vec<PathBuf> = entries
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.is_file())
        .collect();
    files.sort();
    files
}

// Findings: each test holds the smallest input of one defect a fuzz run found, and the input is
// also committed under `fuzz/regressions/<target>/`, which the replay test runs.

fn refuses<T: std::fmt::Debug>(result: Result<T, DecodeError>) {
    assert!(result.is_err(), "decoded {result:?}");
}

/// A required field holding its type's default is omitted by every writer, so bytes that write
/// it anyway are a second encoding of the same value.
#[test]
fn a_required_field_written_with_its_default_refuses() {
    // StepStart.step_id = 0, written.
    refuses(crate::record::StepStart::decode(&[0x20, 0x00]));
    // SessionTriple.project_root = "", written.
    refuses(head::SessionTriple::decode(&[0x12, 0x00]));
    record(&[66, 0x20, 0x00]);
}

/// Writers emit fields in ascending number order, a repeated field's occurrences together.
#[test]
fn fields_out_of_order_refuse() {
    // StepStart: step_id (2) before run_id (1).
    refuses(crate::record::StepStart::decode(&[
        0x20, 0x01, 0x12, 0x01, b'a',
    ]));
    // BatchStart: planned (4), step_id (3), planned (4).
    refuses(crate::record::BatchStart::decode(&[
        0x42, 0x01, b'a', 0x30, 0x01, 0x42, 0x01, b'b',
    ]));
    record(&[66, 0x20, 0x01, 0x12, 0x01, b'a']);
}

/// A known field's must-understand bit is part of its declaration: bytes that set it on a field
/// declared without it (or on a one-of variant, which never has it) weren't written by a writer.
#[test]
fn a_known_field_with_the_wrong_must_understand_bit_refuses() {
    // StepStart.step_id = 1, with the bit set.
    refuses(crate::record::StepStart::decode(&[0x28, 0x01]));
    // ContentBlock::Text, its variant field with the bit set.
    refuses(ContentBlock::decode(&[0x2a, 0x00]));
    // Residence::Local, likewise.
    refuses(head::Residence::decode(&[0x1a, 0x00]));
    record(&[66, 0x28, 0x01]);
}

/// JSON fields store `serde_json`'s compact text, so any other spelling of a value refuses.
#[test]
fn json_text_other_than_the_compact_form_refuses() {
    let event = |text: &[u8]| {
        let mut body = vec![0x32, text.len() as u8];
        body.extend_from_slice(text);
        crate::record::Event::decode(&body)
    };
    for text in [
        &b" 1"[..],
        br#"{"b":1,"a":2}"#,
        br#""\u0041""#,
        b"1e0",
        b"[1 ]",
    ] {
        refuses(event(text));
    }
    // The same values in their compact form decode.
    for text in [&b"1"[..], br#"{"a":2,"b":1}"#, br#""A""#, b"1.0", b"[1]"] {
        event(text).unwrap();
    }
    record(&[76, 0x32, 0x02, b' ', b'1']);
}

/// Opaque text is split around its longest base64 run only when the run is long enough, so a
/// split the writer wouldn't make is a second encoding of the same text.
#[test]
fn an_opaque_text_split_the_writer_wouldnt_make_refuses() {
    // Reasoning whose signature is one byte stored as base64 ("AA=="), too short to split.
    refuses(ContentBlock::decode(&[
        0x42, 0x05, 0x22, 0x03, 0x22, 0x01, 0x00,
    ]));
    record(&[0, 0x42, 0x05, 0x22, 0x03, 0x22, 0x01, 0x00]);
}

/// The writer always writes an opaque field, so a block without it isn't one it wrote.
#[test]
fn an_absent_opaque_field_refuses() {
    // RedactedReasoning without its data.
    refuses(ContentBlock::decode(&[0x52, 0x00]));
    // TextWithState whose provider state has a format and no data.
    refuses(ContentBlock::decode(&[0x32, 0x04, 0x22, 0x02, 0x10, 0x01]));
    record(&[0, 0x52, 0x00]);
}

/// A log cut to nothing has lost its checkpoint, which creation synced, so it is a torn tail at
/// 0, and the analysis finds it below the durable bound: never a clean, empty log.
#[test]
fn an_empty_log_is_a_torn_tail_not_a_clean_log() {
    let key = frame_key();
    let read = crate::log::read(&[], &key, BIND, 92, chain_base).unwrap();
    assert_eq!(read.end, crate::log::LogEnd::TornTail { at: 0 });
    match crate::damage::analyse(&[], &key, BIND, 92, chain_base, None).unwrap() {
        crate::damage::Verdict::Corrupt { at, durable_bound } => {
            assert_eq!((at, durable_bound), (0, 92));
        }
        _ => panic!("an empty log whose creation synced 92 bytes isn't corrupt"),
    }
    // A log written and cut to nothing, as the frame_log target found it.
    frame_log(&[0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]);
}

/// A segment's footer offsets come from the stored bytes, so adding them must not overflow.
#[test]
fn a_segment_footer_whose_index_range_overflows_refuses() {
    let header = SegmentHeader {
        format: 1,
        session_id: SESSION,
        stream: 0,
        first_seq: 0,
        last_seq: 0,
        created_at_ms: 0,
        prefixes: Vec::new(),
    }
    .encode();
    let mut bytes = b"BSEG\x01".to_vec();
    bytes.extend_from_slice(&(header.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&header);
    bytes.extend_from_slice(&u64::MAX.to_le_bytes());
    bytes.extend_from_slice(&1u32.to_le_bytes());
    bytes.extend_from_slice(b"BSEG");
    let key = frame_key();
    assert!(segment::Segment::open(&bytes, &key, SEGMENT_NAME).is_err());
    let mut input = vec![0x00];
    input.extend_from_slice(&bytes);
    segment(&input);
}

/// An index entry's block range, added the same way, refuses rather than overflowing.
#[test]
fn a_block_entry_whose_range_overflows_refuses() {
    segment(&[
        0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x02, b'a', b'b', 0x00, 0xff,
        0xfe, 0xff, 0x10, 0x01, 0x00, 0x01, 0x00, 0x00,
    ]);
}

/// Reading a block allocates what its compressed frame says it holds, not the most a block may
/// hold: before, every block read took 64 MiB, whatever its size.
#[test]
fn reading_a_small_block_allocates_in_proportion_to_it() {
    let records = vec![(
        1,
        Record::StepStart(crate::record::StepStart {
            run_id: "r".into(),
            step_id: 1,
        }),
    )];
    let meta = segment::SegmentMeta {
        session_id: SESSION,
        created_at_ms: 0,
        prefixes: Vec::new(),
        leading: None,
        trailer: None,
        log_start: None,
        placed_before: Vec::new(),
    };
    let key = frame_key();
    let bytes = segment::build(&key, SEGMENT_NAME, &records, meta).unwrap();
    let opened = segment::Segment::open(&bytes, &key, SEGMENT_NAME).unwrap();
    let read = bounded(bytes.len(), 0, || opened.block(0)).unwrap();
    assert_eq!(read, records);
    segment(&[
        0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x02, b'a', b'b', 0x00, 0x00,
        0x00, 0x01, 0x00, 0x01, 0x00, 0x00,
    ]);
}

/// A directory page naming itself as a child (or any child not one level below it) is refused,
/// so every walk ends. Before, `find` looped forever on such a page.
#[test]
fn a_directory_page_naming_itself_refuses_instead_of_looping() {
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let (backend, addr) = memory_backend();
        let key = frame_key();
        let looping = PageRef {
            object: "p0".into(),
            level: 1,
            first_seq: 0,
            last_seq: 10,
            count: 1,
            first_ordinal: Some(1),
            last_ordinal: Some(10),
        };
        let page = Page {
            level: 1,
            entries: Vec::new(),
            children: vec![looping.clone()],
        };
        let sealed = key.seal_unit(&directory::aad(&SESSION, "p0"), &page.encode());
        backend.set_object(&addr, "p0", sealed);
        let pages = Pages {
            backend: &backend,
            addr: &addr,
            session: SESSION,
            key: &key,
            fanout: 4,
        };
        let root = Some(&looping);
        let corrupt = |result: Result<(), directory::DirectoryError>| {
            matches!(result, Err(directory::DirectoryError::Corrupt(_)))
        };
        let outcomes = [
            corrupt(block_on(pages.find(root, 5)).map(drop)),
            corrupt(block_on(pages.find_before(root, 5)).map(drop)),
            corrupt(block_on(pages.find_ordinal(root, 5)).map(drop)),
            corrupt(block_on(pages.abandoned(root, 5)).map(drop)),
            corrupt(block_on(pages.truncate(root, 5, None, "t", 1)).map(drop)),
        ];
        done.send(outcomes).unwrap();
    });
    let outcomes = finished
        .recv_timeout(std::time::Duration::from_secs(20))
        .expect("a walk over a page naming itself didn't finish");
    assert_eq!(
        outcomes, [true; 5],
        "find, find_before, find_ordinal, abandoned, truncate"
    );
}

/// Pages that name one page many times make a walk over every path read it once per path, which
/// grows exponentially with the levels. `abandoned` reads each page once: a tree names each page
/// once, so the pages it lists don't change.
#[test]
fn abandoning_pages_that_name_one_page_many_times_reads_it_once() {
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let (backend, addr) = memory_backend();
        let key = frame_key();
        let reference = |object: &str, level| PageRef {
            object: object.into(),
            level,
            first_seq: 0,
            last_seq: 10,
            count: 1,
            first_ordinal: None,
            last_ordinal: None,
        };
        // p3 at level 3 names p2 200 times, p2 names p1 200 times, p1 names the leaf p0 200
        // times: 8 million paths to the leaf.
        for level in 0..4u32 {
            let page = Page {
                level,
                entries: Vec::new(),
                children: match level {
                    0 => Vec::new(),
                    _ => vec![reference(&format!("p{}", level - 1), level - 1); 200],
                },
            };
            let name = format!("p{level}");
            let sealed = key.seal_unit(&directory::aad(&SESSION, &name), &page.encode());
            backend.set_object(&addr, &name, sealed);
        }
        let pages = Pages {
            backend: &backend,
            addr: &addr,
            session: SESSION,
            key: &key,
            fanout: 256,
        };
        let abandoned = block_on(pages.abandoned(Some(&reference("p3", 3)), 0)).unwrap();
        done.send(abandoned.pages).unwrap();
    });
    let pages = finished
        .recv_timeout(std::time::Duration::from_secs(20))
        .expect("abandoning a tree of four pages didn't finish");
    assert_eq!(pages, ["p3", "p2", "p1", "p0"]);
}

/// A merge names the runs it reads by counting from its level's oldest, so a root whose merge
/// counts more runs than its level holds refuses as corrupt. Before, `update` sliced past the
/// level's runs and panicked.
#[test]
fn an_index_merge_counting_more_runs_than_its_level_holds_refuses() {
    let (backend, addr) = memory_backend();
    let key = frame_key();
    let pages = IndexPages {
        backend: &backend,
        addr: &addr,
        session: SESSION,
        key: &key,
    };
    let root = IndexRoot {
        levels: vec![idindex::Level {
            runs: Vec::new(),
            merge: Some(idindex::Merge {
                inputs: 170,
                done: Vec::new(),
                written: 0,
                next: None,
            }),
        }],
    };
    assert!(matches!(
        block_on(pages.update(root, Vec::new(), "u", 1)),
        Err(idindex::IndexError::Corrupt(_))
    ));
    directory_idindex(&[0x02, 0x3f, 0x87, 0x2d, 0x8f, 0x00, 0xff, 0xaa]);
}

/// A merge's count of entries written comes from the stored root, so adding a step's entries to
/// it refuses rather than overflowing. Before, a count near the largest number overflowed.
#[test]
fn an_index_merge_whose_written_count_would_overflow_refuses() {
    let (backend, addr) = memory_backend();
    let key = frame_key();
    let pages = IndexPages {
        backend: &backend,
        addr: &addr,
        session: SESSION,
        key: &key,
    };
    let entry = idindex::Entry {
        hash: Id128([5; 16]),
        seq: 9,
    };
    block_on(pages.write(
        "d0",
        &idindex::DataPage {
            entries: vec![entry],
        },
        1,
    ))
    .unwrap();
    let info = idindex::PageInfo {
        object: "d0".into(),
        first: entry.hash,
        last: entry.hash,
        count: 1,
        filter: ByteBuf(Vec::new()),
    };
    block_on(pages.write("r0", &idindex::RefPage { pages: vec![info] }, 1)).unwrap();
    let root = IndexRoot {
        levels: vec![idindex::Level {
            runs: vec![idindex::Run {
                entries: 1,
                refs: vec![idindex::RefInfo {
                    object: "r0".into(),
                    first: entry.hash,
                    last: entry.hash,
                }],
            }],
            merge: Some(idindex::Merge {
                inputs: 1,
                done: Vec::new(),
                written: u64::MAX,
                next: None,
            }),
        }],
    };
    assert!(matches!(
        block_on(pages.update(root, Vec::new(), "u", 1)),
        Err(idindex::IndexError::Corrupt(_))
    ));
}

// Property tests. Each runs a few seconds at its default case count; set PROPTEST_CASES for a
// longer run.

use proptest::prelude::*;

use super::gen::{gen, record_of, G, KINDS};
use crate::codec::{put_varint, DecodeErrorKind, MAX_FIELD_NUMBER};
use crate::log::ChainBase;

fn config(default_cases: u32) -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok())
        .unwrap_or(default_cases);
    ProptestConfig {
        cases,
        ..ProptestConfig::default()
    }
}

fn seed() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..768)
}

/// Encodes `value`, decodes it within the memory bound, and checks the decoded value prints and
/// encodes exactly as the original: for types without equality, the printed form stands in.
fn round_trips<M: Message + std::fmt::Debug>(value: &M) {
    let bytes = value.encode();
    let back = bounded(bytes.len(), 0, || M::decode(&bytes))
        .unwrap_or_else(|error| panic!("{value:?} doesn't decode: {error}"));
    assert_eq!(format!("{back:?}"), format!("{value:?}"));
    assert_eq!(back.encode(), bytes, "re-encoding changed the bytes");
}

/// Applies byte edits to `bytes`: each replaces, inserts, deletes or flips one byte.
fn edit(bytes: &mut Vec<u8>, edits: &[(u16, u8, u8)]) {
    for &(at, byte, op) in edits {
        let at = usize::from(at) % (bytes.len() + 1);
        match (op % 4, at < bytes.len()) {
            (0, true) => bytes[at] = byte,
            (1, _) => bytes.insert(at, byte),
            (2, true) => {
                bytes.remove(at);
            }
            (_, true) => bytes[at] ^= byte.max(1),
            _ => bytes.push(byte),
        }
    }
}

fn edits() -> impl Strategy<Value = Vec<(u16, u8, u8)>> {
    proptest::collection::vec((any::<u16>(), any::<u8>(), any::<u8>()), 1..4)
}

/// An unknown field numbered above every declared one, with or without the must-understand bit.
fn with_unknown_field(bytes: &[u8], must: bool, wire: u8) -> Vec<u8> {
    let mut out = bytes.to_vec();
    put_varint(
        &mut out,
        (u64::from(MAX_FIELD_NUMBER) << 4) | (u64::from(must) << 3) | u64::from(wire),
    );
    match wire {
        0 => put_varint(&mut out, 300),
        1 => out.extend_from_slice(&7u64.to_le_bytes()),
        _ => out.extend_from_slice(&[2, 0xab, 0xcd]),
    }
    out
}

/// A message refuses an unknown field marked must-understand, and decodes to the same value
/// when the field isn't marked.
fn honours_must_understand<M: Message + std::fmt::Debug>(value: &M) {
    let bytes = value.encode();
    for wire in 0..3 {
        let must = with_unknown_field(&bytes, true, wire);
        assert_eq!(
            M::decode(&must).map(drop).map_err(|error| error.kind),
            Err(DecodeErrorKind::MustUnderstand {
                field: MAX_FIELD_NUMBER
            }),
            "{value:?} with a must-understand field"
        );
        let skipped = codec::skipped_unknown_fields();
        let back = M::decode(&with_unknown_field(&bytes, false, wire))
            .unwrap_or_else(|error| panic!("{value:?} with an unknown field: {error}"));
        assert!(codec::skipped_unknown_fields() > skipped);
        assert_eq!(format!("{back:?}"), format!("{value:?}"));
    }
}

proptest! {
    #![proptest_config(config(256))]

    /// Every record kind, with every field generated: absent and present options, empty and
    /// long strings and bytes, the largest numbers.
    #[test]
    fn every_record_kind_round_trips(bytes in seed()) {
        for (n, kind) in KINDS.iter().enumerate() {
            let record = record_of(n, &mut G::new(&bytes));
            prop_assert_eq!(record.kind(), *kind);
            let body = record.encode_body();
            let back = bounded(body.len(), 0, || Record::decode(*kind, &body))
                .unwrap_or_else(|error| panic!("{record:?} doesn't decode: {error}"));
            prop_assert_eq!(&back, &record);
            prop_assert_eq!(back.encode_body(), body);
        }
    }

    #[test]
    fn every_stored_shape_round_trips(bytes in seed()) {
        let g = &mut G::new(&bytes);
        round_trips(&gen::<Checkpoint>(g));
        round_trips(&gen::<Conversation>(g));
        round_trips(&gen::<Operations>(g));
        let head: Head = gen(g);
        round_trips(&head);
        round_trips(&gen::<Page>(g));
        round_trips(&gen::<IndexRoot>(g));
        round_trips(&gen::<idindex::RefPage>(g));
        round_trips(&gen::<idindex::DataPage>(g));
        round_trips(&gen::<idindex::Retired>(g));
        round_trips(&gen::<SegmentHeader>(g));
        round_trips(&gen::<SegmentIndex>(g));
        round_trips(&gen::<BlockBody>(g));
        round_trips(&gen::<ProtocolMessage>(g));
        round_trips(&gen::<ContentBlock>(g));
        // A head also round-trips through its stored, authenticated form.
        let deployment = DeploymentKey::new(DEPLOYMENT_ID, &DEPLOYMENT);
        let key = DataKey::from_bytes(1, DATA_KEY);
        let head = Head {
            format: head::HEAD_FORMAT,
            keys: vec![head::WrappedKey {
                id: 1,
                deployment: DEPLOYMENT_ID,
                wrapped: ByteBuf(crate::keys::wrap(&deployment, &head.session_id, &key)),
            }],
            ..head
        };
        let (opened, _) = head::open(&head::seal(&head, &key), &deployment).unwrap();
        prop_assert_eq!(opened, head);
    }

    /// Valid encodings with a few bytes changed decode only to values whose encoding is
    /// exactly what was read.
    #[test]
    fn a_changed_encoding_decodes_only_if_canonical(bytes in seed(), edits in edits()) {
        let g = &mut G::new(&bytes);
        let n = usize::from(bytes.first().copied().unwrap_or(0));
        let record = record_of(n, g);
        let mut body = record.encode_body();
        edit(&mut body, &edits);
        canonical(&body, |b| Record::decode(record.kind(), b), Record::encode_body);
        for mut encoded in [
            gen::<Checkpoint>(g).encode(),
            gen::<Head>(g).encode(),
            gen::<Page>(g).encode(),
            gen::<IndexRoot>(g).encode(),
            gen::<ContentBlock>(g).encode(),
        ] {
            edit(&mut encoded, &edits);
            message::<Checkpoint>(&encoded);
            message::<Head>(&encoded);
            message::<Page>(&encoded);
            message::<IndexRoot>(&encoded);
            message::<ContentBlock>(&encoded);
        }
    }

    /// An unknown field refuses when marked must-understand and is skipped otherwise, in every
    /// record kind and every stored message.
    #[test]
    fn unknown_fields_refuse_only_when_marked_must_understand(bytes in seed()) {
        for (n, kind) in KINDS.iter().enumerate() {
            let record = record_of(n, &mut G::new(&bytes));
            let body = record.encode_body();
            for wire in 0..3 {
                let must = with_unknown_field(&body, true, wire);
                prop_assert_eq!(
                    Record::decode(*kind, &must).map(drop).map_err(|error| error.kind),
                    Err(DecodeErrorKind::MustUnderstand { field: MAX_FIELD_NUMBER })
                );
                let optional = with_unknown_field(&body, false, wire);
                prop_assert_eq!(Record::decode(*kind, &optional).unwrap(), record.clone());
            }
        }
        let g = &mut G::new(&bytes);
        honours_must_understand(&gen::<Checkpoint>(g));
        honours_must_understand(&gen::<Head>(g));
        honours_must_understand(&gen::<Page>(g));
        honours_must_understand(&gen::<IndexRoot>(g));
        honours_must_understand(&gen::<SegmentIndex>(g));
        honours_must_understand(&gen::<ProtocolMessage>(g));
        honours_must_understand(&gen::<ToolResult>(g));
        honours_must_understand(&gen::<Usage>(g));
        // A one-of is closed: a second field, known or not, refuses.
        let block = gen::<ContentBlock>(g).encode();
        prop_assert!(ContentBlock::decode(&with_unknown_field(&block, false, 2)).is_err());
    }

    /// Bytes of any shape, through every entry point a fuzz target calls: nothing panics,
    /// memory stays proportional to the input, and whatever decodes is canonical.
    #[test]
    fn arbitrary_bytes_keep_every_decoder_property(bytes in proptest::collection::vec(any::<u8>(), 0..2048)) {
        record(&bytes);
        frame_log(&bytes);
        segment(&bytes);
        head_checkpoint(&bytes);
        directory_idindex(&bytes);
    }
}

proptest! {
    #![proptest_config(config(5))]

    /// A log of generated batches, cut at every offset and changed at many, reads soundly: only
    /// written records, only whole batches, never clean when damaged (see `fuzzing::log`).
    #[test]
    fn a_log_reads_soundly_cut_or_changed_anywhere(bytes in seed()) {
        let mut input = Input::new(&bytes);
        let next_seq = 1 + input.number() % 1000;
        let base = ChainBase {
            transcript: (input.byte() & 1 == 1).then(|| input.number() % next_seq),
            ledger: (input.byte() & 1 == 1).then(|| input.number() % next_seq),
        };
        let mut ops = Vec::new();
        for _ in 0..3 + input.byte() % 5 {
            ops.push(match input.byte() % 4 {
                0 | 1 => LogOp::Batch((0..1 + input.byte() % 3).map(|_| small_record(&mut input)).collect()),
                2 => LogOp::Barrier,
                _ => LogOp::Claim,
            });
        }
        let log = write_log(next_seq, base, &ops);
        check_log(&log, &Mutation::None);
        for at in 0..log.bytes.len() {
            check_log(&log, &Mutation::Truncate(at));
        }
        // Every byte of every frame's length and header, and bytes spread over the rest.
        for frame in &log.frames {
            let header = frame.range.start..frame.range.start + 4 + crate::frame::HEADER_LEN + 4;
            let spread = frame.range.clone().step_by(13);
            for at in header.chain(spread).chain([frame.range.end - 1]) {
                check_log(&log, &Mutation::Xor { at, mask: 1 << (at % 8) });
            }
            check_log(&log, &Mutation::Replace { at: frame.range.start + 5, len: 4, with: [0; 4] });
        }
    }
}

/// A record frame of a kind this build doesn't know, without the skippable flag, refuses the
/// log as written by a newer build: it authenticates, so it is never damage.
#[test]
fn a_frame_of_an_unknown_kind_refuses_the_log() {
    let log = write_log(1, ChainBase::default(), &[]);
    let header = crate::frame::FrameHeader {
        flags: 0,
        stream: crate::frame::FrameStream::Ledger,
        kind: 63,
        kind_version: 1,
        key_id: KEY_ID,
        seq: 1,
        batch_first: 1,
        batch_count: 1,
        frame_index: 1,
        synced_through: log.created_len,
        prev_in_stream: None,
    };
    let mut bytes = log.bytes.clone();
    bytes.extend(crate::frame::seal(&header, &[], &frame_key(), BIND).unwrap());
    let key = frame_key();
    let refused = |result: Result<(), crate::log::LogError>| match result {
        Err(crate::log::LogError::Undecodable { offset, error }) => {
            assert_eq!(offset, log.bytes.len() as u64);
            assert_eq!(error.kind, DecodeErrorKind::UnknownVariant(63));
        }
        other => panic!("an unknown kind read as {other:?}"),
    };
    refused(crate::log::read(&bytes, &key, BIND, log.created_len, chain_base).map(drop));
    refused(
        crate::damage::analyse(&bytes, &key, BIND, log.created_len, chain_base, None).map(drop),
    );
}

/// A length prefix claiming more bytes than remain refuses before anything is allocated for it.
#[test]
fn a_length_prefix_past_the_input_refuses_before_allocating() {
    // StepStart.run_id claiming 2^32 - 1 bytes, and 2^62.
    for prefix in [
        &[0xff, 0xff, 0xff, 0xff, 0x0f][..],
        &[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x40],
    ] {
        let mut body = vec![0x12];
        body.extend_from_slice(prefix);
        body.push(b'a');
        let error = bounded(body.len(), 0, || crate::record::StepStart::decode(&body)).unwrap_err();
        assert_eq!(error.kind, DecodeErrorKind::Truncated);
    }
}

/// The checks hold decoders to a memory bound only where the counting allocator is installed:
/// this proves it is, so a bound that passes isn't one that measured nothing.
#[test]
fn the_memory_bound_measures_allocations() {
    let caught = std::panic::catch_unwind(|| {
        bounded(1, 0, || vec![0u8; ALLOC_BASE + 2 * ALLOC_PER_BYTE].len())
    });
    assert!(caught.is_err(), "an allocation over the bound passed it");
}

/// Writes the seed corpus every fuzz target starts from: valid encodings of each family. Run by
/// hand, when the formats change: `cargo test -p broca-session-store --lib write_fuzz_seeds --
/// --ignored`.
#[test]
#[ignore = "writes the committed seed corpus"]
fn write_fuzz_seeds() {
    let corpus = Path::new(env!("CARGO_MANIFEST_DIR")).join("fuzz/corpus");
    let write = |target: &str, name: &str, data: &[u8]| {
        let dir = corpus.join(target);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(format!("seed-{name}")), data).unwrap();
    };
    let pattern = |n: usize| -> Vec<u8> { (0..96).map(|i| (i * 7 + n * 13) as u8).collect() };
    for (n, kind) in KINDS.iter().enumerate() {
        let record = record_of(n, &mut G::new(&pattern(n)));
        let mut data = vec![*kind as u8];
        data.extend(record.encode_body());
        write("record", &format!("{kind:02}-{}", record.name()), &data);
    }
    let ops = [
        LogOp::Batch(vec![small_record(&mut Input::new(b"\x00\x03run\x05"))]),
        LogOp::Barrier,
        LogOp::Batch(vec![
            small_record(&mut Input::new(b"\x01\x02\x01m\x03\x04")),
            small_record(&mut Input::new(b"\x03\x05event\x01\x01r")),
        ]),
        LogOp::Claim,
    ];
    let log = write_log(
        5,
        ChainBase {
            transcript: Some(2),
            ledger: None,
        },
        &ops,
    );
    write("frame_log", "raw-log", &[&[0x00][..], &log.bytes].concat());
    write(
        "frame_log",
        "sealed-frames",
        &[
            0x01, 0x00, 0x00, 0x05, 0x00, 0x00, 0x01, 0x00, 0x03, b'r', b'u', b'n', 0x05,
        ],
    );
    write(
        "frame_log",
        "written-log",
        &[
            0x02, 0x04, 0x01, 0x02, 0x00, 0x06, 0x00, 0x01, 0x00, 0x03, b'r', b'u', b'n', 0x05,
            0x02, 0x03, 0x01, 0x03, 0x01, 0x00, 0x00, 0x00, 0x00,
        ],
    );
    let records: Vec<(u64, Record)> = (0..6)
        .map(|n| {
            (
                n * 2 + 1,
                record_of(10 + n as usize, &mut G::new(&pattern(n as usize))),
            )
        })
        .filter(|(_, record)| record.stream() == crate::record::Stream::Ledger)
        .collect();
    let meta = segment::SegmentMeta {
        session_id: SESSION,
        created_at_ms: 7,
        prefixes: vec![Id128([3; 16])],
        leading: None,
        trailer: Some(b"trailer".to_vec()),
        log_start: None,
        placed_before: Vec::new(),
    };
    let built = segment::build(&frame_key(), SEGMENT_NAME, &records, meta).unwrap();
    write("segment", "raw-segment", &[&[0x00][..], &built].concat());
    write(
        "segment",
        "sealed-units",
        &[
            0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x02, 0x00, 0x03, 0x01,
            0x00, 0x01, 0x00, 0x02, b'a', b'b', 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00,
        ],
    );
    write(
        "segment",
        "built-segment",
        &[
            0x02, 0x05, 0x04, 0x00, 0x03, b'r', b'u', b'n', 0x01, 0x00, 0x00, 0x01, 0x07, 0x00,
            0x00, 0x00, 0x00, 0x00,
        ],
    );
    let head_seed = pattern(3);
    let g = &mut G::new(&head_seed);
    write(
        "head_checkpoint",
        "head",
        &[&[0x01][..], &gen::<Head>(g).encode()].concat(),
    );
    write(
        "head_checkpoint",
        "checkpoint",
        &[&[0x00][..], &gen::<Checkpoint>(g).encode()].concat(),
    );
    let empty = Checkpoint {
        covers_through: 0,
        next_seq: 1,
        conversation: Conversation::new(false),
        operational: Operations::default(),
        transcript_base: None,
        ledger_base: None,
    };
    write(
        "head_checkpoint",
        "empty-checkpoint",
        &[&[0x00][..], &empty.encode()].concat(),
    );
    write(
        "directory_idindex",
        "page",
        &[&[0x00][..], &gen::<Page>(g).encode()].concat(),
    );
    write(
        "directory_idindex",
        "index-root",
        &[&[0x00][..], &gen::<IndexRoot>(g).encode()].concat(),
    );
    write(
        "directory_idindex",
        "directory-walk",
        &[
            0x01, 0x02, 0x01, 0x01, 0x02, 0x01, 0x01, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x02, 0x00, 0x05, 0x0a, 0x01, 0x05, 0x14, 0x00, 0x00, 0x00, 0x03, 0x01,
            0x01, 0x00, 0x14, 0x02, 0x00, 0x00, 0x00, 0x08, 0x00,
        ],
    );
    write(
        "directory_idindex",
        "index-walk",
        &[
            0x02, 0x02, 0x01, 0x03, 0x01, 0x05, 0x01, 0x01, 0x02, 0x01, 0x01, 0x00, 0x01, 0x01,
            0x02, 0x01, 0x01, 0x01, 0x02, 0x04, 0x00, 0x01, 0x00, 0x01, 0x02, 0x00, 0x01, 0x03,
            0x02, 0x01,
        ],
    );
}
