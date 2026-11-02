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
        FileWal::open_with_recovery(self.log(), WalConfig::OVERHAUL, self.floor())
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

#[test]
fn maximum_width_writer_lifecycle_reopens_at_the_ordinary_limit() {
    let scratch = Scratch::new("lifecycle-limit");
    let writer_id = [u8::MAX; 16];
    let opened = WalRecord::WriterOpened {
        writer_id,
        previous_writer_id: None,
        recovered_through_seq: 0,
        cause: "new".into(),
    };
    let closed = WalRecord::WriterClosed { writer_id };
    let mut wal = scratch.overhaul();
    wal.writer_id = Some(writer_id);
    wal = wal.with_append_budget(16_000);
    wal.append(1, &opened, true).unwrap();
    loop {
        match wal.append(1, &plain(1), false) {
            Ok(_) => {}
            Err(WalError::TooLarge { .. }) => break,
            other => panic!("unexpected append outcome: {other:?}"),
        }
    }
    wal.append(1, &closed, true).unwrap();
    let closed_bytes = std::fs::metadata(scratch.log()).unwrap().len();
    assert!(closed_bytes <= 16_000);
    let recovered_through_seq = wal.last_seq();
    drop(wal);

    let mut wal = scratch.overhaul().with_append_budget(16_000);
    wal.writer_id = Some(writer_id);
    let reopened = WalRecord::WriterOpened {
        writer_id,
        previous_writer_id: Some(writer_id),
        recovered_through_seq,
        cause: "reopen".into(),
    };
    wal.append(1, &reopened, true).unwrap_or_else(|error| {
        panic!("closed log is {closed_bytes} bytes under cap 16000, but reopen failed: {error}")
    });
    wal.append(1, &closed, true).unwrap();
    assert!(std::fs::metadata(scratch.log()).unwrap().len() <= 16_000);
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
fn a_legacy_log_refuses_recovery_cut_without_changing_bytes() {
    let scratch = Scratch::new("legacy-recovery-record");
    let mut wal = FileWal::open(scratch.log()).unwrap();
    let before = std::fs::read(scratch.log()).unwrap();
    let record: WalRecord = serde_json::from_value(serde_json::json!({
        "type":"recovery_cut", "cut_id":"cut", "sidecar_basename":"log.cut-1",
        "cut_offset":69, "removed_len":1, "removed_sha256":"digest",
        "reason":"unproven_corruption", "active_run":null, "runs":[], "unattributed_tool_risk":true
    }))
    .unwrap();
    assert!(matches!(
        wal.append(1, &record, true),
        Err(WalError::FrameFormat {
            log: WalFormat::Legacy,
            offered: WalFormat::Overhaul
        })
    ));
    assert_eq!(std::fs::read(scratch.log()).unwrap(), before);
}

#[test]
fn overhaul_repair_preserves_undecodable_payload_but_never_downgrades_unknown_features() {
    for requires in [vec!["frame-stamps/v1"], vec!["future-recovery/v1"]] {
        let scratch = Scratch::new("undecodable-or-newer");
        let mut wal = scratch.overhaul();
        wal.append(1, &plain(1), true).unwrap();
        drop(wal);
        let original = std::fs::read(scratch.log()).unwrap();
        let frame = v4::encode(
            v4::Metadata {
                seq: 2,
                fence: 1,
                appended_ms: 1,
                writer_id: [1; 16],
            },
            &requires,
            br#"{"type":"step_started","step_id":"not-an-integer"}"#,
        )
        .unwrap();
        use std::io::Write;
        std::fs::OpenOptions::new()
            .append(true)
            .open(scratch.log())
            .unwrap()
            .write_all(&frame)
            .unwrap();
        if requires.contains(&"future-recovery/v1") {
            assert!(matches!(
                FileWal::open_with_recovery(scratch.log(), WalConfig::OVERHAUL, scratch.floor()),
                Err(WalError::NewerWriter { .. })
            ));
            let mut unchanged = original;
            unchanged.extend_from_slice(&frame);
            assert_eq!(std::fs::read(scratch.log()).unwrap(), unchanged);
            assert!(
                crate::recovery::manifests(&scratch.log(), DEFAULT_MAX_READ_BYTES)
                    .unwrap()
                    .is_empty()
            );
        } else {
            let mut refused = WalConfig::OVERHAUL;
            refused.recovery = crate::recovery::RecoveryPolicy::Refuse;
            assert!(FileWal::open_with_recovery(scratch.log(), refused, scratch.floor()).is_err());
            let wal =
                FileWal::open_with_recovery(scratch.log(), WalConfig::OVERHAUL, scratch.floor())
                    .unwrap();
            assert!(std::fs::read(scratch.log()).unwrap().starts_with(&original));
            let manifests =
                crate::recovery::manifests(&scratch.log(), DEFAULT_MAX_READ_BYTES).unwrap();
            assert_eq!(
                std::fs::read(
                    scratch
                        .log()
                        .parent()
                        .unwrap()
                        .join(&manifests[0].recovery.sidecar_basename)
                )
                .unwrap(),
                frame
            );
            assert!(wal
                .replay()
                .unwrap()
                .iter()
                .any(|f| matches!(f.record, WalRecord::RecoveryCut { .. })));
        }
    }
}

#[test]
fn an_overhaul_append_writes_nothing_when_the_floor_cannot_list_frame_stamps() {
    // No floor configured: refused by name, nothing written.
    let scratch = Scratch::new("no-floor");
    let mut wal = FileWal::open_with_recovery(scratch.log(), WalConfig::OVERHAUL, scratch.floor())
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
    let path = scratch
        .0
        .join("cut")
        .join(prefix.len().to_string())
        .join("session.wal");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, prefix).unwrap();
    let wal = FileWal::open_with_recovery(&path, WalConfig::OVERHAUL, scratch.floor())?;
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
        let repairs = wal
            .replay()
            .unwrap()
            .iter()
            .filter(|f| matches!(f.record, WalRecord::RecoveryCut { .. }))
            .count();
        assert_eq!(
            repairs, 0,
            "an ordinary byte cut at {cut} is not a recovery cut"
        );
        assert_eq!(wal.last_seq(), (survivors + repairs) as u64, "cut at {cut}");
        assert!(len >= valid_end as u64);
        assert!(std::fs::read(wal.log_path().unwrap())
            .unwrap()
            .starts_with(&bytes[..valid_end]));
        let replayed: Vec<_> = wal
            .replay()
            .unwrap()
            .into_iter()
            .filter(|f| !matches!(f.record, WalRecord::RecoveryCut { .. }))
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
fn a_damaged_header_crc_is_preserved_even_when_intact_records_follow() {
    let scratch = Scratch::new("damaged-crc");
    let (mut bytes, records) = complete_log(&scratch);
    let ends = frame_ends(&bytes);
    let (index, _) = ends
        .iter()
        .enumerate()
        .filter(|(_, (_, record))| *record)
        .nth(1)
        .unwrap();
    let damaged = ends[index - 1].0;
    let header_len = v4::decode_header(&bytes[damaged..])
        .unwrap()
        .unwrap()
        .header_len;
    bytes[damaged + header_len - 1] ^= 0xff;
    for cut in 0..=bytes.len() {
        let retained = if cut <= damaged {
            ends.iter()
                .rev()
                .find(|(end, _)| *end <= cut)
                .map_or(0, |(end, _)| *end)
        } else {
            damaged
        };
        let survivors = ends
            .iter()
            .filter(|(end, record)| *record && *end <= retained)
            .count();
        let (wal, _) =
            recover(&scratch, &bytes[..cut]).unwrap_or_else(|e| panic!("cut {cut}: {e}"));
        let replayed: Vec<_> = wal
            .replay()
            .unwrap()
            .into_iter()
            .filter(|f| !matches!(f.record, WalRecord::RecoveryCut { .. }))
            .map(|f| f.record)
            .collect();
        assert_eq!(replayed, records[..survivors], "cut at {cut}");
        assert!(std::fs::read(wal.log_path().unwrap())
            .unwrap()
            .starts_with(&bytes[..retained]));
        let mut frame_start = 0;
        let intact_later = ends.iter().any(|(end, record)| {
            let follows = *record && frame_start > damaged && *end <= cut;
            frame_start = *end;
            follows
        });
        if intact_later {
            let manifests =
                crate::recovery::manifests(wal.log_path().unwrap(), DEFAULT_MAX_READ_BYTES)
                    .unwrap();
            assert_eq!(manifests.len(), 1);
            let raw = wal
                .log_path()
                .unwrap()
                .parent()
                .unwrap()
                .join(&manifests[0].recovery.sidecar_basename);
            assert_eq!(
                std::fs::read(raw).unwrap(),
                bytes[retained..cut],
                "cut at {cut}"
            );
        } else {
            assert!(
                crate::recovery::manifests(wal.log_path().unwrap(), DEFAULT_MAX_READ_BYTES)
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(
                std::fs::read(wal.log_path().unwrap()).unwrap(),
                bytes[..retained]
            );
        }
    }
}

#[test]
fn a_legacy_log_refuses_recovery_call_closed_without_changing_bytes() {
    let scratch = Scratch::new("legacy-recovery-receipt");
    let mut wal = FileWal::open(scratch.log()).unwrap();
    let before = std::fs::read(scratch.log()).unwrap();
    let record: WalRecord = serde_json::from_str(
        r#"{"type":"recovery_call_closed","cut_id":"cut","run_id":"completed-owner","tool_call_id":"late-call","outcome":"unknown","result":{"tool_call_id":"late-call","output":{"kind":"error","message":"outcome_unknown"},"is_error":true}}"#,
    ).unwrap();
    assert!(matches!(
        wal.append(1, &record, true),
        Err(WalError::FrameFormat {
            log: WalFormat::Legacy,
            offered: WalFormat::Overhaul
        })
    ));
    assert_eq!(std::fs::read(scratch.log()).unwrap(), before);
}

#[test]
fn refuse_ignores_malformed_recovery_manifests_and_matches_decoder() {
    let original = {
        let scratch = Scratch::new("refuse-original");
        let mut wal = scratch.overhaul();
        for step in 1..=3 {
            wal.append(1, &plain(step), true).unwrap();
        }
        std::fs::read(scratch.log()).unwrap()
    };
    let mut corrupt = original.clone();
    corrupt[LINEAGE_FRAME_LEN + 4] = 0;
    for image in [
        original.clone(),
        original[..original.len() - 1].to_vec(),
        corrupt,
    ] {
        let scratch = Scratch::new("refuse-malformed-manifest");
        std::fs::create_dir_all(scratch.log().parent().unwrap()).unwrap();
        std::fs::write(scratch.log(), &image).unwrap();
        let manifest = scratch
            .log()
            .with_file_name("session.wal.cut-malformed.json");
        std::fs::write(&manifest, b"{not-json").unwrap();
        let expected = recover_prefix(&image, 0, None, None);
        let mut config = WalConfig::OVERHAUL;
        config.recovery = crate::recovery::RecoveryPolicy::Refuse;
        let actual = FileWal::open_with_recovery(scratch.log(), config, scratch.floor());
        match expected {
            Ok(prefix) => {
                drop(actual.expect("Refuse must ignore malformed recovery artifacts"));
                assert_eq!(
                    std::fs::read(scratch.log()).unwrap(),
                    image[..prefix.valid_end as usize]
                );
            }
            Err(expected) => {
                let actual = actual
                    .err()
                    .expect("the ordinary decoder refused this image");
                assert_eq!(format!("{actual:?}"), format!("{expected:?}"));
                assert_eq!(std::fs::read(scratch.log()).unwrap(), image);
            }
        }
        assert_eq!(std::fs::read(&manifest).unwrap(), b"{not-json");
        assert!(!crate::head_proof::sidecar_path(&scratch.log()).exists());
    }
}
#[cfg(feature = "fs-override")]
#[test]
fn oracle_prefix_matches_real_recovery_for_damaged_and_zeroed_frames() {
    for zero_version in [false, true] {
        let scratch = Scratch::new("oracle-prefix");
        let mut wal = scratch.overhaul();
        wal.append(1, &plain(1), true).unwrap();
        let first_append = std::fs::read(scratch.log()).unwrap();
        let kept_records = wal.replay().unwrap();
        let damaged_seq = wal.append(1, &plain(2), true).unwrap();
        wal.append(1, &plain(3), true).unwrap();
        drop(wal);
        let mut damaged = std::fs::read(scratch.log()).unwrap();
        let mut second_start = 0;
        let second_end = loop {
            match crate::framing::decode_next(&damaged[second_start..]) {
                DecodeStep::Record { framed, consumed } if framed.seq == damaged_seq => {
                    assert_eq!(framed.record, plain(2));
                    break second_start + consumed;
                }
                DecodeStep::Record { consumed, .. } | DecodeStep::Lineage { consumed, .. } => {
                    second_start += consumed
                }
                _ => panic!("the clean fixture must decode before injecting damage"),
            }
        };
        // Keep the verified log-identity frame immediately before the damaged record.
        let kept_bytes = damaged[..second_start].to_vec();
        assert!(kept_bytes.starts_with(&first_append));
        if zero_version {
            damaged[second_start + 4] = 0;
        } else {
            damaged[second_end - 1] ^= 1;
        }
        std::fs::write(scratch.log(), &damaged).unwrap();

        let selected = crate::recovery::test_recovery_prefix(&damaged).unwrap();
        assert_eq!(selected.valid_end, kept_bytes.len() as u64);
        assert_eq!(selected.last_seq, Some(1));
        assert_eq!(
            std::fs::read(scratch.log()).unwrap(),
            damaged,
            "observation must not modify the damaged image"
        );

        let reopened = scratch.overhaul();
        let recovered = reopened.replay().unwrap();
        assert_eq!(&recovered[..kept_records.len()], kept_records.as_slice());
        assert!(std::fs::read(scratch.log())
            .unwrap()
            .starts_with(&kept_bytes));
        let cut = recovered
            .iter()
            .find_map(|frame| match &frame.record {
                WalRecord::RecoveryCut { recovery } => Some(recovery),
                _ => None,
            })
            .expect("real recovery must record its cut before the intact later frame");
        assert_eq!(cut.cut_offset, selected.valid_end);
    }
}
