use super::*;
use broca_wal::framing;

fn repaired() -> (Scratch, broca_wal::recovery::CutManifest) {
    let (state, _) = super::repair::damaged_image();
    let root = Scratch::new();
    state.install(&root.0).unwrap();
    drop(root.open());
    let manifest = broca_wal::recovery::manifests(&root.log(), 1_000_000)
        .unwrap()
        .remove(0);
    (root, manifest)
}

#[test]
fn v4_recovery_manifest_io_and_size_errors_name_the_evidence_file() {
    let (root, manifest) = repaired();
    let path = root
        .log()
        .parent()
        .unwrap()
        .join(format!("{}.json", manifest.recovery.sidecar_basename));
    let error = broca_wal::recovery::manifests(&root.log(), 1).unwrap_err();
    assert!(
        error.to_string().contains(&path.display().to_string()),
        "{error}"
    );
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    let error = broca_wal::recovery::manifests(&root.log(), 1_000_000).unwrap_err();
    assert!(
        error.to_string().contains(&path.display().to_string()),
        "{error}"
    );
}

#[test]
fn v4_recovery_reversed_manifest_names_keep_every_pending_cut() {
    let (root, first) = repaired();
    let mut bytes = std::fs::read(root.log()).unwrap();
    let offset = first.recovery.cut_offset as usize;
    let framing::DecodeStep::Record { framed, consumed } = framing::decode_next(&bytes[offset..])
    else {
        panic!("first marker must decode")
    };
    let mut metadata = broca_wal::framing::v4::decode_header(&bytes[offset..])
        .unwrap()
        .unwrap()
        .metadata;
    metadata.seq += 1;
    let changed = broca_wal::framing::encode_v4(
        metadata,
        &[broca_wal::framing::FRAME_STAMPS_FEATURE],
        &framed.record,
    )
    .unwrap();
    assert_eq!(changed.len(), consumed);
    bytes[offset..offset + consumed].copy_from_slice(&changed);
    std::fs::write(root.log(), &bytes).unwrap();
    let recording = Arc::new(RecordingFs::new(&root.0).unwrap());
    with_fs_ops(recording.clone(), || drop(root.open()));
    let manifests = broca_wal::recovery::manifests(&root.log(), 1_000_000).unwrap();
    assert_eq!(manifests.len(), 2);
    let second = manifests
        .iter()
        .find(|m| m.recovery.cut_id != first.recovery.cut_id)
        .unwrap();
    let image = recording
        .enumerate(100_000)
        .unwrap()
        .into_iter()
        .find(|image| {
            image
                .state
                .files
                .get(&PathBuf::from("wal/log"))
                .is_some_and(|log| log.len() == offset)
                && image
                    .state
                    .files
                    .keys()
                    .filter(|path| path.to_string_lossy().ends_with(".json"))
                    .count()
                    == 2
        })
        .expect("H1 must model the cut after truncation and before the marker write");
    let next = Scratch::new();
    image.state.install(&next.0).unwrap();
    let parent = next.log().parent().unwrap().to_path_buf();
    // A clock rollback changes filename ordering, not the dependency of the
    // second recovery cut on the first.
    std::fs::rename(
        parent.join(format!("{}.json", first.recovery.sidecar_basename)),
        parent.join("log.cut-9999999999999.json"),
    )
    .unwrap();
    std::fs::rename(
        parent.join(format!("{}.json", second.recovery.sidecar_basename)),
        parent.join("log.cut-0000000000000.json"),
    )
    .unwrap();
    let wal = next.open();
    let cuts: BTreeSet<_> = wal
        .replay()
        .unwrap()
        .iter()
        .filter_map(|frame| match &frame.record {
            WalRecord::RecoveryCut { recovery } => Some(recovery.cut_id.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        cuts,
        BTreeSet::from([
            first.recovery.cut_id.clone(),
            second.recovery.cut_id.clone()
        ])
    );
}

#[test]
fn v4_recovery_refuse_matches_pinned_old_streaming_open_outcomes() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/old-streaming-open.json")).unwrap();
    for case in fixture["cases"].as_array().unwrap() {
        let root = Scratch::new();
        std::fs::create_dir_all(root.log().parent().unwrap()).unwrap();
        let decode = |hex: &str| {
            (0..hex.len())
                .step_by(2)
                .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).unwrap())
                .collect::<Vec<_>>()
        };
        std::fs::write(root.log(), decode(case["input_hex"].as_str().unwrap())).unwrap();
        let outcome = match root.open_policy(broca_wal::recovery::RecoveryPolicy::Refuse) {
            Ok(wal) => {
                serde_json::json!({"sequences":wal.replay().unwrap().iter().map(|frame| frame.seq).collect::<Vec<_>>()})
            }
            Err(error) => serde_json::json!({"error":format!("{error:?}")}),
        };
        assert_eq!(
            outcome, case["outcome"],
            "old open result for {}",
            case["name"]
        );
        assert_eq!(
            std::fs::read(root.log()).unwrap(),
            decode(case["opened_hex"].as_str().unwrap()),
            "old open bytes for {}",
            case["name"]
        );
    }
}

#[test]
fn v4_recovery_ordinary_final_zero_tail_has_no_cut() {
    let (source, states) = images(1);
    let start = boundaries(&source)[2].0;
    let image = states
        .into_iter()
        .find(|state| {
            let log = &state.files[&PathBuf::from("wal/log")];
            log.len() > start && log[start..].iter().all(|byte| *byte == 0)
        })
        .expect("H1 must model a final zero-filled pending append");
    let root = Scratch::new();
    image.install(&root.0).unwrap();
    let wal = root.open();
    assert!(
        broca_wal::recovery::manifests(&root.log(), 1_000_000)
            .unwrap()
            .is_empty(),
        "ordinary unacknowledged final damage is not a cut"
    );
    assert_eq!(std::fs::read(root.log()).unwrap(), source[..start]);
    assert_eq!(
        wal.replay()
            .unwrap()
            .iter()
            .map(|frame| frame.record.clone())
            .collect::<Vec<_>>(),
        [record(1)]
    );
}

#[test]
fn v4_recovery_readable_sequence_damage_has_no_unattributed_bytes() {
    let root = Scratch::new();
    let mut wal = published(&root);
    let start = std::fs::metadata(root.log()).unwrap().len() as usize;
    wal.append(1, &record(2), true).unwrap();
    drop(wal);
    let mut bytes = std::fs::read(root.log()).unwrap();
    let framing::DecodeStep::Record { framed, consumed } = framing::decode_next(&bytes[start..])
    else {
        panic!("source must decode")
    };
    let mut metadata = framing::v4::decode_header(&bytes[start..])
        .unwrap()
        .unwrap()
        .metadata;
    metadata.seq += 1;
    let changed =
        framing::encode_v4(metadata, &[framing::FRAME_STAMPS_FEATURE], &framed.record).unwrap();
    assert_eq!(changed.len(), consumed);
    bytes[start..start + consumed].copy_from_slice(&changed);
    std::fs::write(root.log(), bytes).unwrap();
    drop(root.open());
    let cuts = broca_wal::recovery::manifests(&root.log(), 1_000_000).unwrap();
    assert_eq!(cuts.len(), 1);
    assert!(
        !cuts[0].recovery.unattributed_tool_risk,
        "checksum-valid readable frames must not be labeled undecodable"
    );
}

#[test]
fn v4_recovery_clean_open_uses_streaming_read_buffers() {
    let root = Scratch::new();
    let mut wal = root.open();
    for index in 1..=1000 {
        wal.append(1, &record(index), false).unwrap();
    }
    wal.sync().unwrap();
    drop(wal);
    let trace = Arc::new(crate::io_trace::IoTraceFs::new(
        &root.0,
        Arc::new(broca_wal::fs_ops::RealFs),
    ));
    with_fs_ops(trace.clone(), || drop(root.open()));
    let reads = trace
        .attempts()
        .iter()
        .filter_map(|attempt| {
            attempt
                .strip_prefix("read ")
                .and_then(|text| text.split_whitespace().next())
                .and_then(|size| size.parse::<usize>().ok())
        })
        .collect::<Vec<_>>();
    assert!(!reads.is_empty());
    assert!(
        reads.iter().all(|size| *size <= 8192),
        "normal open must not grow a whole-log buffer: {reads:?}"
    );
}

#[test]
fn v4_recovery_old_manifests_do_not_enter_a_fresh_lineage_at_the_same_path() {
    let (root, old) = repaired();
    let raw = root
        .log()
        .parent()
        .unwrap()
        .join(&old.recovery.sidecar_basename);
    let preserved = std::fs::read(&raw).unwrap();
    std::fs::remove_file(root.log()).unwrap();
    drop(FileWal::open(root.log()).unwrap());
    let fresh = std::fs::read(root.log()).unwrap();
    let wal = root.open();
    assert!(
        wal.replay().unwrap().is_empty(),
        "a new lineage must not inherit old recovery tombstones"
    );
    assert_eq!(std::fs::read(root.log()).unwrap(), fresh);
    assert_eq!(std::fs::read(raw).unwrap(), preserved);
}

#[test]
fn v4_recovery_missing_log_cannot_authorize_a_manifest_replay() {
    let (root, _) = repaired();
    std::fs::remove_file(root.log()).unwrap();
    let error = root
        .open_policy(broca_wal::recovery::RecoveryPolicy::Repair)
        .err()
        .expect("missing lineage must not resurrect an old session");
    assert!(
        error.to_string().contains("lineage")
            && error
                .to_string()
                .contains(&root.log().display().to_string())
    );
    assert!(!root.log().exists());
}
crate::require_process_kill!();
