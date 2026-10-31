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
