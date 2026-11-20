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

/// A session the new session storage holds keeps its media there: today's media-class lookup
/// refuses it instead of choosing one of today's stores for it.
#[test]
fn session_media_store_refuses_a_session_the_new_storage_holds() {
    let (root, uploads) = fixture();
    let wal = crate::FileWalStore::new(root.join("wal"));
    drop(crate::WalStore::open(&wal, &identity()).unwrap());
    // On today's storage the session's media store is its WAL's class.
    assert!(uploads.session_media_store(&identity()).is_ok());
    // The head is in the ephemeral root: every root of the new storage claims its session.
    let head = broca_wal::session_storage_head(
        &root,
        broca_wal::StorageClass::Ephemeral,
        &crate::session_addr(&identity()),
    );
    fs::create_dir_all(head.parent().unwrap()).unwrap();
    fs::write(&head, b"head").unwrap();
    let refused = uploads.session_media_store(&identity()).unwrap_err();
    assert!(
        refused.to_string().contains("new session storage"),
        "{refused}"
    );
}
