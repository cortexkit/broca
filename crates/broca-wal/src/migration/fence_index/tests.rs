use super::*;

#[test]
fn boot_fence_rebuild_skips_a_vanished_directory_entry() {
    for directory in ["agent-data/write-fences", "agent-data/cuts"] {
        let state_root = broca_test_support::scratch_root().join(directory);
        let scanned = state_root.join(directory);
        std::fs::create_dir_all(&scanned).unwrap();
        let vanished = scanned.join("vanished.tmp");
        std::fs::write(&vanished, b"temporary").unwrap();
        let cuts = state_root.join("agent-data/cuts");
        std::fs::create_dir_all(&cuts).unwrap();
        let reference = "0123456789abcdef0123456789abcdef";
        std::fs::write(
            cuts.join("pending.json"),
            serde_json::to_vec(&serde_json::json!({
                "intent": {"op": "activate"},
                "refs": [{"ref_kind": "broca-session", "ref": reference}]
            }))
            .unwrap(),
        )
        .unwrap();
        let mut removed = false;
        let result = rebuild_with_read_dir(&state_root, |path| {
            let mut entries = fs::read_dir(path)?.collect::<io::Result<Vec<_>>>()?;
            entries.sort_by_key(|entry| entry.path() != vanished);
            Ok(entries
                .into_iter()
                .map(|entry| {
                    if entry.path() == vanished {
                        std::fs::remove_file(entry.path()).unwrap();
                        removed = true;
                        // Model the metadata lookup performed by RealFs::read_dir
                        // even on filesystems that cache the directory entry's type.
                        std::fs::symlink_metadata(entry.path())?;
                    }
                    Ok(entry)
                })
                .collect::<Vec<_>>()
                .into_iter())
        });
        assert!(removed);
        result.unwrap();
        assert_eq!(
            read_json::<String>(&root(&state_root).join(reference)).unwrap(),
            Some("pending".to_owned())
        );
        assert!(ready(&state_root).exists());
    }
}

#[test]
fn boot_fence_rebuild_preserves_permission_errors() {
    for directory_error in [true, false] {
        let state_root = broca_test_support::scratch_root().join("fence-permission-error");
        std::fs::create_dir_all(&state_root).unwrap();
        let mut injected = false;
        let result = rebuild_with_read_dir(&state_root, |_| {
            injected = true;
            if directory_error {
                Err(io::ErrorKind::PermissionDenied.into())
            } else {
                Ok(vec![Err(io::ErrorKind::PermissionDenied.into())].into_iter())
            }
        });
        assert!(injected);
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
        assert!(!ready(&state_root).exists());
    }
}

#[test]
fn boot_fence_rebuild_rejects_a_missing_scan_root() {
    for vanished_after_listing in [true, false] {
        let state_root = broca_test_support::scratch_root().join("fence-missing-root");
        std::fs::create_dir_all(&state_root).unwrap();
        let mut removed = false;
        let result = rebuild_with_read_dir(&state_root, |path| {
            std::fs::remove_dir_all(path).unwrap();
            removed = true;
            if vanished_after_listing {
                Ok(vec![Err(io::ErrorKind::NotFound.into())].into_iter())
            } else {
                fs::read_dir(path)
            }
        });
        assert!(removed);
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotFound);
        assert!(!ready(&state_root).exists());
    }
}

#[test]
fn boot_fence_rebuild_fails_closed_when_a_listed_cut_vanishes() {
    let state_root = broca_test_support::scratch_root().join("fence-vanished-cut");
    let cuts = state_root.join("agent-data/cuts");
    std::fs::create_dir_all(&cuts).unwrap();
    let cut = cuts.join("pending.json");
    std::fs::write(&cut, br#"{"intent":null}"#).unwrap();
    let mut removed = false;
    let result = rebuild_with_read_dir(&state_root, |path| {
        let entries = fs::read_dir(path)?;
        if path == cuts {
            std::fs::remove_file(&cut).unwrap();
            removed = true;
        }
        Ok(entries)
    });
    assert!(removed);
    let error = result.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        "cut disappeared during fence initialization"
    );
    assert!(!ready(&state_root).exists());
}
