//! Version 4 logs written by an overhaul [`FileWal`]: which frames it accepts, the feature floor
//! it raises first, and how its recovery open treats every possible crash cut.

use super::*;
use crate::framing::{v4, LINEAGE_FRAME_LEN};
use broca_protocol::StepId;
use std::sync::atomic::{AtomicUsize, Ordering};

/// One test's scratch directory, removed on drop.
struct Scratch(PathBuf);

impl Scratch {
    fn new(tag: &str) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-wal-v4-{tag}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn log(&self) -> PathBuf {
        self.0.join("wal").join("session.wal")
    }

    fn floor(&self) -> PathBuf {
        self.0.join(crate::floor::FEATURE_FLOOR_FILENAME)
    }

    /// An overhaul log at [`Self::log`] with the floor beside its state root.
    fn overhaul(&self) -> FileWal {
        FileWal::open(self.log())
            .unwrap()
            .with_feature_floor(self.floor())
            .with_wal_config(WalConfig::OVERHAUL)
            .unwrap()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Requires only `frame-stamps/v1` in a version 4 header.
fn plain(n: u64) -> WalRecord {
    WalRecord::TurnFinished { step_id: StepId(n) }
}

/// Requires `input-token-cap/v1` besides, so its header's requirements array is longer.
fn gated(limit: u64) -> WalRecord {
    WalRecord::InputTokenCapFrozen { limit }
}

/// Where each frame of `bytes` ends, in order, with whether it is a record frame.
fn frame_ends(bytes: &[u8]) -> Vec<(usize, bool)> {
    let mut ends = Vec::new();
    let mut offset = 0;
    while offset < bytes.len() {
        if bytes[offset + 4] == crate::framing::V4_FRAMING_VERSION {
            let header = v4::decode_header(&bytes[offset..])
                .unwrap()
                .expect("a complete log holds complete headers");
            offset += header.frame_len();
            ends.push((offset, true));
        } else {
            offset += LINEAGE_FRAME_LEN;
            ends.push((offset, false));
        }
    }
    ends
}

/// The header of every version 4 frame in `bytes`, in order.
fn record_headers(bytes: &[u8]) -> Vec<v4::Header> {
    let mut start = 0;
    let mut headers = Vec::new();
    for (end, record) in frame_ends(bytes) {
        if record {
            headers.push(v4::decode_header(&bytes[start..]).unwrap().unwrap());
        }
        start = end;
    }
    headers
}

#[test]
fn an_overhaul_log_writes_stamped_version_4_frames_after_raising_the_floor() {
    let scratch = Scratch::new("stamped");
    let mut wal = scratch.overhaul();
    assert_eq!(wal.wal_config(), WalConfig::OVERHAUL);
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    assert_eq!(wal.append(9, &plain(1), true).unwrap(), 1);
    let prepared = PreparedV4Frame::prepare(&gated(5)).unwrap();
    assert_eq!(
        wal.append_v4(9, &gated(5), true, prepared, 1234).unwrap(),
        2
    );

    let headers = record_headers(&std::fs::read(scratch.log()).unwrap());
    assert_eq!(headers.len(), 2);
    assert_eq!(headers[0].requires, ["frame-stamps/v1"]);
    assert_eq!(
        headers[1].requires,
        ["frame-stamps/v1", "input-token-cap/v1"]
    );
    assert!(
        headers[0].metadata.appended_ms >= before,
        "a direct append is stamped with the time it reaches the log"
    );
    assert_eq!(
        headers[1].metadata.appended_ms, 1234,
        "a prepared frame carries the time its writer sampled"
    );
    assert_eq!(headers[0].metadata.writer_id, headers[1].metadata.writer_id);
    assert_ne!(headers[0].metadata.writer_id, [0; 16]);
    assert_eq!((headers[0].metadata.seq, headers[0].metadata.fence), (1, 9));
    assert_eq!((headers[1].metadata.seq, headers[1].metadata.fence), (2, 9));
    assert_eq!(
        crate::floor::read_floor(&scratch.floor()).unwrap(),
        std::collections::BTreeSet::from([
            "frame-stamps/v1".to_owned(),
            "input-token-cap/v1".to_owned()
        ])
    );
    // This build reads its own version 4 log back.
    let records: Vec<_> = wal
        .replay()
        .unwrap()
        .into_iter()
        .map(|f| f.record)
        .collect();
    assert_eq!(records, [plain(1), gated(5)]);

    // A second open of the same log is a second writer, with an id of its own.
    drop(wal);
    let mut reopened = scratch.overhaul();
    reopened.append(9, &plain(3), true).unwrap();
    let reopened_headers = record_headers(&std::fs::read(scratch.log()).unwrap());
    let third = &reopened_headers[2];
    assert_eq!(third.metadata.seq, 3);
    assert_ne!(third.metadata.writer_id, headers[0].metadata.writer_id);
}

#[test]
fn an_overhaul_log_refuses_caller_encoded_frames() {
    let scratch = Scratch::new("untrusted");
    let mut wal = scratch.overhaul();
    let size = std::fs::metadata(scratch.log()).unwrap().len();
    // Even the exact legacy frame for this sequence, fence and record is refused: an overhaul
    // log writes no legacy frame, whoever encoded it.
    let legacy = crate::framing::encode(1, 9, &plain(1)).unwrap();
    let error = wal
        .append_preencoded(9, &plain(1), true, legacy)
        .unwrap_err();
    assert!(
        matches!(
            error,
            WalError::FrameFormat {
                log: WalFormat::Overhaul,
                offered: WalFormat::Legacy
            }
        ),
        "{error:?}"
    );
    assert_eq!(std::fs::metadata(scratch.log()).unwrap().len(), size);
    assert_eq!(wal.last_seq(), 0);
    assert!(
        !scratch.floor().exists(),
        "nothing was written, so nothing was raised"
    );
}

#[test]
fn a_legacy_log_refuses_version_4_frames() {
    let scratch = Scratch::new("legacy-refuses");
    let mut wal = FileWal::open(scratch.log())
        .unwrap()
        .with_feature_floor(scratch.floor());
    assert_eq!(wal.wal_config(), WalConfig::LEGACY);
    let size = std::fs::metadata(scratch.log()).unwrap().len();
    let prepared = PreparedV4Frame::prepare(&plain(1)).unwrap();
    let error = wal.append_v4(9, &plain(1), true, prepared, 1).unwrap_err();
    assert!(
        matches!(
            error,
            WalError::FrameFormat {
                log: WalFormat::Legacy,
                offered: WalFormat::Overhaul
            }
        ),
        "{error:?}"
    );
    assert_eq!(std::fs::metadata(scratch.log()).unwrap().len(), size);
    assert!(!scratch.floor().exists());
}

#[test]
fn an_overhaul_append_writes_nothing_when_the_floor_cannot_list_frame_stamps() {
    // No floor configured: refused by name, nothing written.
    let scratch = Scratch::new("no-floor");
    let mut wal = FileWal::open(scratch.log())
        .unwrap()
        .with_wal_config(WalConfig::OVERHAUL)
        .unwrap();
    let size = std::fs::metadata(scratch.log()).unwrap().len();
    let error = wal.append(9, &plain(1), true).unwrap_err();
    assert!(
        matches!(&error, WalError::FeatureFloor { path: None, requires, .. }
            if requires == &["frame-stamps/v1".to_owned()]),
        "{error:?}"
    );
    assert_eq!(std::fs::metadata(scratch.log()).unwrap().len(), size);

    // A floor that cannot be written (a directory stands where its file belongs): the same
    // refusal on the prepared path, and still nothing written.
    let scratch = Scratch::new("broken-floor");
    std::fs::create_dir_all(scratch.floor()).unwrap();
    let mut wal = scratch.overhaul();
    let size = std::fs::metadata(scratch.log()).unwrap().len();
    let prepared = PreparedV4Frame::prepare(&plain(1)).unwrap();
    let error = wal.append_v4(9, &plain(1), true, prepared, 1).unwrap_err();
    assert!(
        matches!(&error, WalError::FeatureFloor { path: Some(_), .. }),
        "{error:?}"
    );
    assert_eq!(std::fs::metadata(scratch.log()).unwrap().len(), size);
    assert_eq!(wal.last_seq(), 0);
}

#[test]
fn an_encode_error_is_returned_not_swallowed() {
    // `WalRecord::Unknown` holds none of the record it stands for, so the codec refuses it.
    assert!(matches!(
        PreparedV4Frame::prepare(&WalRecord::Unknown),
        Err(FrameError::Serialize(_))
    ));
    let scratch = Scratch::new("encode-error");
    let mut wal = scratch.overhaul();
    let size = std::fs::metadata(scratch.log()).unwrap().len();
    let error = wal.append(9, &WalRecord::Unknown, true).unwrap_err();
    assert!(
        matches!(error, WalError::Frame(FrameError::Serialize(_))),
        "{error:?}"
    );
    assert_eq!(std::fs::metadata(scratch.log()).unwrap().len(), size);
}

/// The log every cut is taken from: four records, alternating between the two requirement-array
/// lengths, each trailed by the identity frame every append writes.
fn complete_log(scratch: &Scratch) -> (Vec<u8>, Vec<WalRecord>) {
    let records = vec![plain(1), gated(2), plain(3), gated(4)];
    let mut wal = scratch.overhaul();
    for record in &records {
        wal.append(7, record, false).unwrap();
    }
    wal.sync().unwrap();
    drop(wal);
    (std::fs::read(scratch.log()).unwrap(), records)
}

/// Recover a copy of `prefix` through the real writer open, as a restart after a crash at
/// that cut does.
fn recover(scratch: &Scratch, prefix: &[u8]) -> Result<(FileWal, u64), WalError> {
    let path = scratch.0.join("cut").join("session.wal");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, prefix).unwrap();
    let wal = FileWal::open(&path)?;
    let len = std::fs::metadata(&path).unwrap().len();
    Ok((wal, len))
}

#[test]
fn every_crash_cut_of_a_version_4_log_recovers_its_complete_prefix() {
    let scratch = Scratch::new("all-cuts");
    let (bytes, records) = complete_log(&scratch);
    let ends = frame_ends(&bytes);
    assert_eq!(ends.iter().filter(|(_, record)| *record).count(), 4);
    // Where each record frame's requirements array lies, to name the cuts that tear it.
    let mut requires_spans = Vec::new();
    let mut start = 0;
    for (end, record) in &ends {
        if *record {
            let header = v4::decode_header(&bytes[start..]).unwrap().unwrap();
            requires_spans.push(start + 47..start + header.header_len - 36);
        }
        start = *end;
    }
    let lengths: std::collections::BTreeSet<usize> =
        requires_spans.iter().map(|span| span.len()).collect();
    assert_eq!(
        lengths.len(),
        2,
        "two requirement-array lengths: {lengths:?}"
    );

    let mut torn_requires = 0;
    for cut in 0..=bytes.len() {
        let complete: Vec<_> = ends.iter().filter(|(end, _)| *end <= cut).collect();
        let valid_end = complete.last().map_or(0, |(end, _)| *end);
        let survivors = complete.iter().filter(|(_, record)| *record).count();
        let (wal, len) = recover(&scratch, &bytes[..cut])
            .unwrap_or_else(|error| panic!("cut at {cut}: recovery refused: {error}"));
        assert_eq!(wal.last_seq(), survivors as u64, "cut at {cut}");
        assert_eq!(
            len, valid_end as u64,
            "cut at {cut}: recovery truncates to the last complete frame"
        );
        let replayed: Vec<_> = wal
            .replay()
            .unwrap()
            .into_iter()
            .map(|f| f.record)
            .collect();
        assert_eq!(replayed, records[..survivors], "cut at {cut}");
        if requires_spans.iter().any(|span| span.contains(&cut)) {
            torn_requires += 1;
        }
    }
    assert!(
        torn_requires > 0,
        "some cuts tear a requirements array mid-way"
    );
}

#[test]
fn a_damaged_header_crc_is_a_torn_tail_only_while_nothing_intact_follows() {
    let scratch = Scratch::new("damaged-crc");
    let (mut bytes, records) = complete_log(&scratch);
    let ends = frame_ends(&bytes);
    let record_starts: Vec<usize> = ends
        .iter()
        .enumerate()
        .filter(|(_, (_, record))| *record)
        .map(|(index, _)| ends[index - 1].0)
        .collect();
    let record_ends: Vec<usize> = ends
        .iter()
        .filter(|(_, record)| *record)
        .map(|(end, _)| *end)
        .collect();
    // Damage the second record frame's header CRC, the last four bytes of its header.
    let damaged = record_starts[1];
    let header_len = v4::decode_header(&bytes[damaged..])
        .unwrap()
        .unwrap()
        .header_len;
    bytes[damaged + header_len - 1] ^= 0xff;

    for cut in 0..=bytes.len() {
        let result = recover(&scratch, &bytes[..cut]);
        if cut < damaged {
            let (wal, _) = result.unwrap_or_else(|error| panic!("cut at {cut}: {error}"));
            let survivors = record_ends.iter().filter(|end| **end <= cut).count();
            assert_eq!(wal.last_seq(), survivors as u64, "cut at {cut}");
        } else if cut < record_ends[2] {
            // Only the damaged frame, its identity frame and a torn third frame follow the
            // first record: none of that is a committed record, so it is discarded.
            let (wal, len) = result.unwrap_or_else(|error| panic!("cut at {cut}: {error}"));
            assert_eq!(wal.last_seq(), 1, "cut at {cut}");
            assert_eq!(len, damaged as u64, "cut at {cut}");
            let replayed: Vec<_> = wal
                .replay()
                .unwrap()
                .into_iter()
                .map(|f| f.record)
                .collect();
            assert_eq!(replayed, records[..1], "cut at {cut}");
        } else {
            // An intact record after the damage: corruption mid-log, refused loudly, never
            // truncated away.
            let error = result
                .err()
                .unwrap_or_else(|| panic!("cut at {cut} recovered"));
            assert!(
                matches!(error, WalError::Corrupt { offset, .. } if offset == damaged),
                "cut at {cut}: {error:?}"
            );
            assert_eq!(
                std::fs::read(scratch.0.join("cut").join("session.wal")).unwrap(),
                &bytes[..cut],
                "cut at {cut}: a refused log is left as it was"
            );
        }
    }
}
