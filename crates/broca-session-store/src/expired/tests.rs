use super::*;

fn marker() -> Marker {
    Marker {
        format: FORMAT,
        triple: SessionTriple {
            project_root: "/private/project".into(),
            harness: "harness".into(),
            session: "session".into(),
        },
        expired_at_ms: 1000,
        seq: 0,
        next_tag: 42,
        next_notice_seq: 77,
        runs: vec!["deleted-run".into()],
    }
}

fn root() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/scratch")
        .join(format!("expiry-{}", hex(&crate::keys::random_id().0)));
    std::fs::create_dir_all(&root).unwrap();
    root
}

fn expiry_markers_are_sealed_and_preserve_floors(cipher: Cipher) {
    let root = root();
    let addr = SessionAddr("ab".repeat(32));
    let deployment = DeploymentKey::new(1, &[9; 32]);
    let id = crate::keys::random_id();
    let first = marker();
    write(&root, &addr, &id, first.clone(), cipher, &deployment).unwrap();
    let bytes = std::fs::read(folder(&root, &addr).unwrap().join(hex(&id.0))).unwrap();
    if cipher != Cipher::None {
        assert!(!bytes
            .windows(first.triple.project_root.len())
            .any(|bytes| bytes == first.triple.project_root.as_bytes()));
        assert!(matches!(
            read(&root, &addr, &first.triple, &DeploymentKey::unloaded(1)),
            Err(Error::KeyUnavailable)
        ));
    } else {
        assert_eq!(
            read(&root, &addr, &first.triple, &DeploymentKey::unloaded(1))
                .unwrap()
                .len(),
            1
        );
    }
    let mut second = first.clone();
    second.next_tag = 1;
    second.next_notice_seq = 1;
    let id2 = crate::keys::random_id();
    write(&root, &addr, &id2, second.clone(), cipher, &deployment).unwrap();
    // A replay never invents another deletion sequence or lowers a floor.
    write(&root, &addr, &id2, second, cipher, &deployment).unwrap();
    let markers = read(&root, &addr, &first.triple, &deployment).unwrap();
    assert_eq!(markers.len(), 2);
    let last = markers.iter().max_by_key(|marker| marker.seq).unwrap();
    assert_eq!((last.seq, last.next_tag, last.next_notice_seq), (2, 42, 77));
    std::fs::remove_dir_all(root).unwrap();
}

fn expiry_markers_refuse_damage_and_wrong_identity(cipher: Cipher) {
    let root = root();
    let addr = SessionAddr("ab".repeat(32));
    let deployment = DeploymentKey::new(1, &[9; 32]);
    let id = crate::keys::random_id();
    let first = marker();
    write(&root, &addr, &id, first.clone(), cipher, &deployment).unwrap();
    let mut wrong = first.triple.clone();
    wrong.session = "other".into();
    assert!(matches!(
        read(&root, &addr, &wrong, &deployment),
        Err(Error::Damaged(_))
    ));
    let path = folder(&root, &addr).unwrap().join(hex(&id.0));
    let mut bytes = std::fs::read(&path).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    std::fs::write(path, bytes).unwrap();
    assert!(matches!(
        read(&root, &addr, &first.triple, &deployment),
        Err(Error::Damaged(_))
    ));
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn expiry_markers_bound_run_membership_without_losing_ids() {
    let root = root();
    let mut marker = marker();
    marker.runs = vec![String::new(); MAX_RUNS + 1];
    let body = marker.encode();
    let triple = marker.triple.clone();
    let addr = SessionAddr("ab".repeat(32));
    assert!(matches!(
        write(
            &root,
            &addr,
            &crate::keys::random_id(),
            marker,
            Cipher::None,
            &DeploymentKey::unloaded(1)
        ),
        Err(Error::TooLarge)
    ));
    assert!(!folder(&root, &addr).unwrap().exists());
    let id = crate::keys::random_id();
    let name = hex(&id.0);
    let prefix = [0, 0, 0];
    let mut bytes = prefix.to_vec();
    bytes.extend_from_slice(
        &crate::frame::FrameKey::plain().seal_unit(&aad(&addr, &name, &prefix), &body),
    );
    std::fs::create_dir_all(folder(&root, &addr).unwrap()).unwrap();
    std::fs::write(folder(&root, &addr).unwrap().join(name), bytes).unwrap();
    assert!(matches!(
        read(&root, &addr, &triple, &DeploymentKey::unloaded(1)),
        Err(Error::TooLarge)
    ));
    std::fs::remove_dir_all(root).unwrap();
}

cipher_tests!(
    expiry_markers_are_sealed_and_preserve_floors,
    expiry_markers_refuse_damage_and_wrong_identity
);
