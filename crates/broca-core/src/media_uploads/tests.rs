use super::*;

fn identity() -> BindIdentity {
    BindIdentity {
        project_root: "/project/a".into(),
        harness: "harness".into(),
        session: "same".into(),
    }
}
fn fixture() -> (PathBuf, Uploads) {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let root = broca_test_support::scratch_root().join(format!(
        "uploads-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    broca_owner_only::create_dir_all(&root).unwrap();
    crate::media_store::create_media_directories(&root).unwrap();
    let uploads = Uploads::at(&root);
    (root, uploads)
}
fn request(bytes: &[u8]) -> wire::UploadBeginRequest {
    wire::UploadBeginRequest::new(
        "application/pdf",
        bytes.len() as u64,
        format!("{:x}", Sha256::digest(bytes)),
    )
}
fn snapshot(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let mut files = Vec::new();
    for dir in ["objects", "refs"] {
        for entry in fs::read_dir(root.join("media-uploads").join(dir)).unwrap() {
            let path = entry.unwrap().path();
            files.push((path.clone(), fs::read(path).unwrap()));
        }
    }
    files.sort();
    files
}

#[test]
fn identical_chunk_and_commit_retries_survive_reopen() {
    let (root, uploads) = fixture();
    let id = uploads
        .begin(&identity(), &request(b"abcdef"), 0)
        .unwrap()
        .upload_id;
    assert_eq!(
        uploads
            .append(&identity(), &id, 0, b"abc", 1)
            .unwrap()
            .received,
        3
    );
    assert_eq!(
        uploads
            .append(&identity(), &id, 0, b"abc", 2)
            .unwrap()
            .received,
        3
    );
    let before = snapshot(&root);
    assert_eq!(
        uploads
            .append(&identity(), &id, 0, b"abd", 3)
            .unwrap_err()
            .code,
        "upload_offset_mismatch"
    );
    assert_eq!(snapshot(&root), before);
    uploads.append(&identity(), &id, 3, b"def", 4).unwrap();
    let committed = uploads.commit(&identity(), &id, 5).unwrap();
    let before = snapshot(&root);
    let reopened = Uploads::at(&root);
    assert_eq!(reopened.commit(&identity(), &id, 6).unwrap(), committed);
    assert_eq!(snapshot(&root), before);
    assert_eq!(
        reopened
            .append(&identity(), &id, 6, b"", 7)
            .unwrap_err()
            .field,
        Some("upload_id")
    );
}

#[test]
fn foreign_unknown_and_expired_uploads_write_nothing() {
    let (root, uploads) = fixture();
    let id = uploads
        .begin(&identity(), &request(b"abc"), 0)
        .unwrap()
        .upload_id;
    for foreign in [
        BindIdentity {
            project_root: "/project/b".into(),
            ..identity()
        },
        BindIdentity {
            harness: "other".into(),
            ..identity()
        },
        BindIdentity {
            session: "other".into(),
            ..identity()
        },
    ] {
        let before = snapshot(&root);
        for error in [
            uploads.append(&foreign, &id, 0, b"abc", 1).unwrap_err(),
            uploads.commit(&foreign, &id, 1).unwrap_err(),
        ] {
            assert_eq!(error.code, "unknown_upload");
            assert!(error.subject.contains(&id));
        }
        assert_eq!(uploads.status(&foreign, &id, 1).unwrap().state, "gone");
        assert_eq!(snapshot(&root), before);
    }
    uploads
        .append(&identity(), &id, 0, b"abc", OPEN_TTL_SECONDS - 1)
        .unwrap();
    let before = snapshot(&root);
    let expired = uploads
        .append(&identity(), &id, 0, b"abc", OPEN_TTL_SECONDS)
        .unwrap_err();
    assert_eq!(expired.code, "upload_gone");
    assert!(expired.subject.contains(&id));
    assert_eq!(
        uploads
            .append(&identity(), "unknown", 0, b"abc", 1)
            .unwrap_err()
            .code,
        "unknown_upload"
    );
    assert_eq!(
        uploads
            .commit(&identity(), &id, RETENTION_SECONDS)
            .unwrap_err()
            .code,
        "upload_gone"
    );
    assert_eq!(snapshot(&root), before);
}

#[test]
fn chunk_caps_and_declared_size_are_checked_before_write() {
    let (root, uploads) = fixture();
    let bytes = vec![1; wire::CHUNK_MAX];
    let id = uploads
        .begin(&identity(), &request(&bytes), 0)
        .unwrap()
        .upload_id;
    let before = snapshot(&root);
    let out_of_order = uploads
        .append(&identity(), &id, u64::MAX, b"x", 1)
        .unwrap_err();
    assert_eq!(out_of_order.code, "upload_offset_mismatch");
    assert_eq!(out_of_order.received, Some(0));
    assert_eq!(snapshot(&root), before);
    assert_eq!(
        uploads
            .append(&identity(), &id, 0, &bytes, 1)
            .unwrap()
            .received,
        wire::CHUNK_MAX as u64
    );
    let before = snapshot(&root);
    assert_eq!(
        uploads
            .append(&identity(), &id, wire::CHUNK_MAX as u64, b"x", 2)
            .unwrap_err()
            .code,
        "upload_size_mismatch"
    );
    assert_eq!(
        uploads
            .append(&identity(), &id, 0, &vec![1; wire::CHUNK_MAX + 1], 2)
            .unwrap_err()
            .code,
        "upload_chunk_too_large"
    );
    assert_eq!(snapshot(&root), before);
}

#[test]
fn reservation_limits_include_global_capacity() {
    assert_eq!(UPLOAD_MAX, 67_108_864);
    assert_eq!(SESSION_MAX, 536_870_912);
    assert_eq!(SESSION_UPLOAD_MAX, 128);
    assert_eq!(GLOBAL_MAX, 4_294_967_296);
    assert_eq!(OPEN_TTL_SECONDS, 3_600);
    assert!(check_reservation(UPLOAD_MAX, 0, 0, 0).is_ok());
    assert_eq!(
        check_reservation(UPLOAD_MAX + 1, 0, 0, 0),
        Err("upload_size_mismatch")
    );
    assert!(check_reservation(1, SESSION_MAX - 1, 0, 0).is_ok());
    assert_eq!(
        check_reservation(1, SESSION_MAX, 0, 0),
        Err("upload_session_full")
    );
    assert!(check_reservation(0, 0, SESSION_UPLOAD_MAX - 1, 0).is_ok());
    assert_eq!(
        check_reservation(0, 0, SESSION_UPLOAD_MAX, 0),
        Err("upload_session_full")
    );
    assert!(check_reservation(1, 0, 0, GLOBAL_MAX - 1).is_ok());
    assert_eq!(
        check_reservation(1, 0, 0, GLOBAL_MAX),
        Err("upload_staging_full")
    );
    let (root, uploads) = fixture();
    for n in 0..GLOBAL_MAX / UPLOAD_MAX {
        let owner = BindIdentity {
            session: n.to_string(),
            ..identity()
        };
        let mut req = request(b"");
        req.bytes = UPLOAD_MAX;
        uploads.begin(&owner, &req, 0).unwrap();
    }
    let before = snapshot(&root);
    assert_eq!(
        uploads
            .begin(&identity(), &request(b"x"), 0)
            .unwrap_err()
            .code,
        "upload_staging_full"
    );
    assert_eq!(snapshot(&root), before);
}

#[test]
fn begin_reserves_session_bytes_and_count_before_creating_files() {
    let (root, uploads) = fixture();
    let mut req = request(b"");
    req.bytes = UPLOAD_MAX + 1;
    let before = snapshot(&root);
    assert_eq!(
        uploads.begin(&identity(), &req, 0).unwrap_err().code,
        "upload_size_mismatch"
    );
    assert_eq!(snapshot(&root), before);
    req.bytes = UPLOAD_MAX;
    for _ in 0..SESSION_MAX / UPLOAD_MAX {
        uploads.begin(&identity(), &req, 0).unwrap();
    }
    let before = snapshot(&root);
    assert_eq!(
        uploads
            .begin(&identity(), &request(b"x"), 0)
            .unwrap_err()
            .code,
        "upload_session_full"
    );
    assert_eq!(snapshot(&root), before);
    let other = BindIdentity {
        session: "count".into(),
        ..identity()
    };
    for _ in 0..SESSION_UPLOAD_MAX {
        uploads.begin(&other, &request(b""), 0).unwrap();
    }
    let before = snapshot(&root);
    assert_eq!(
        uploads.begin(&other, &request(b""), 0).unwrap_err().code,
        "upload_session_full"
    );
    assert_eq!(snapshot(&root), before);
}

#[test]
fn commit_checks_size_and_hash_and_retains_for_one_hour() {
    let (root, uploads) = fixture();
    let id = uploads
        .begin(&identity(), &request(b"abc"), 0)
        .unwrap()
        .upload_id;
    assert_eq!(
        uploads.commit(&identity(), &id, 1).unwrap_err().code,
        "upload_size_mismatch"
    );
    uploads.append(&identity(), &id, 0, b"xyz", 1).unwrap();
    let before = snapshot(&root);
    assert_eq!(
        uploads.commit(&identity(), &id, 2).unwrap_err().code,
        "upload_sha256_mismatch"
    );
    assert_eq!(snapshot(&root), before);
    let id = uploads
        .begin(&identity(), &request(b"abc"), 0)
        .unwrap()
        .upload_id;
    uploads.append(&identity(), &id, 0, b"abc", 1).unwrap();
    uploads.commit(&identity(), &id, 100).unwrap();
    uploads.sweep(RETENTION_SECONDS + 99, true).unwrap();
    assert_eq!(
        uploads
            .status(&identity(), &id, RETENTION_SECONDS + 99)
            .unwrap()
            .state,
        "committed"
    );
    assert!(uploads.data_path(&id).exists());
    assert_eq!(uploads.sweep(RETENTION_SECONDS + 100, false).unwrap(), 1);
    assert!(uploads.data_path(&id).exists());
    assert_eq!(uploads.sweep(RETENTION_SECONDS + 100, true).unwrap(), 1);
    assert!(!uploads.data_path(&id).exists());
    assert_eq!(
        uploads
            .commit(&identity(), &id, RETENTION_SECONDS + 101)
            .unwrap_err()
            .code,
        "upload_gone"
    );
}

#[test]
fn maintenance_keeps_everything_if_any_metadata_is_unreadable() {
    let (root, uploads) = fixture();
    let id = uploads
        .begin(&identity(), &request(b"abc"), 0)
        .unwrap()
        .upload_id;
    fs::write(uploads.metadata_path("broken"), b"not json").unwrap();
    let before = snapshot(&root);
    assert!(uploads.sweep(RETENTION_SECONDS + 1, true).is_err());
    assert_eq!(snapshot(&root), before);
    assert!(uploads.data_path(&id).exists());
}

#[test]
fn maintenance_reclaims_abandoned_bodies_but_missing_state_fails_closed() {
    let (root, uploads) = fixture();
    let id = uploads
        .begin(&identity(), &request(b"abc"), 0)
        .unwrap()
        .upload_id;
    fs::write(uploads.data_path("orphan"), b"abandoned").unwrap();
    let before = snapshot(&root);
    assert_eq!(uploads.sweep(1, false).unwrap(), 1);
    assert_eq!(snapshot(&root), before);
    assert_eq!(uploads.sweep(1, true).unwrap(), 1);
    assert!(!uploads.data_path("orphan").exists());
    assert!(uploads.data_path(&id).exists());
    fs::remove_file(uploads.data_path(&id)).unwrap();
    assert_eq!(uploads.status(&identity(), &id, 1).unwrap().state, "gone");
    assert_eq!(uploads.status(&identity(), &id, 1).unwrap().received, 0);
    let lost = uploads.commit(&identity(), &id, 1).unwrap_err();
    assert_eq!(lost.code, "upload_gone");
    assert!(lost.subject.contains(&id));
    fs::write(uploads.data_path("orphan"), b"abandoned").unwrap();
    let before = snapshot(&root);
    assert!(uploads.sweep(RETENTION_SECONDS + 1, true).is_err());
    assert_eq!(snapshot(&root), before);
}
