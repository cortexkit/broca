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

/// Models a second broca process rebuilding the same state root: its sweep removes
/// every fence entry this rebuild listed before this rebuild unlinks them.
#[test]
fn boot_fence_rebuild_tolerates_listed_entries_already_removed_by_a_peer() {
    let state_root = broca_test_support::scratch_root().join("fence-peer-unlinked");
    let fences = root(&state_root);
    std::fs::create_dir_all(&fences).unwrap();
    let stale = fences.join("0123456789abcdef0123456789abcdef");
    std::fs::write(&stale, br#""old-cut""#).unwrap();
    std::fs::write(fences.join("ready.json.tmp"), b"true").unwrap();
    let mut removed = 0;
    rebuild_with_read_dir(&state_root, |path| {
        let entries = fs::read_dir(path)?;
        if path == fences {
            for entry in std::fs::read_dir(path)? {
                std::fs::remove_file(entry?.path())?;
                removed += 1;
            }
        }
        Ok(entries)
    })
    .unwrap();
    assert_eq!(removed, 2);
    assert!(!stale.exists());
    assert!(ready(&state_root).exists());
}

/// Stands in for a second broca process starting its boot rebuild on the same state
/// root. It asks for the fence-index lock through its own file description, as a
/// separate process would, and runs only if the lock is free: it unlinks every entry
/// under the fence root (including other writers' temporary files), finds no cut to
/// republish, and publishes `ready`. Returns whether it ran.
fn peer_rebuild(state_root: &Path) -> io::Result<bool> {
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path(state_root))?;
    match lock.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => return Ok(false),
        Err(std::fs::TryLockError::Error(error)) => return Err(error),
    }
    for entry in std::fs::read_dir(root(state_root))? {
        std::fs::remove_file(entry?.path())?;
    }
    std::fs::write(ready(state_root), b"true")?;
    Ok(true)
}

/// A peer's rebuild sweep running while this rebuild publishes `ready` used to unlink
/// `ready.json.tmp` before this rebuild renamed it, failing boot with NotFound.
#[test]
fn boot_fence_rebuild_excludes_a_peer_rebuild_during_ready_publication() {
    let state_root = broca_test_support::scratch_root().join("fence-peer-sweep");
    std::fs::create_dir_all(&state_root).unwrap();
    let target = ready(&state_root);
    let peer_ran = std::rc::Rc::new(std::cell::Cell::new(None));
    let observed = peer_ran.clone();
    let hook_root = state_root.clone();
    let _trace = super::super::io_trace::install(move |event| {
        if event == super::super::io_trace::Event::BeforePublish(target.clone()) {
            observed.set(Some(peer_rebuild(&hook_root)?));
        }
        Ok(())
    });
    let result = rebuild(&state_root);
    // Without the lock this is the boot failure: NotFound renaming `ready.json.tmp`.
    result.unwrap();
    assert_eq!(
        peer_ran.get(),
        Some(false),
        "a peer rebuild ran concurrently"
    );
    assert_eq!(read_json::<bool>(&ready(&state_root)).unwrap(), Some(true));
}

/// A peer's rebuild running between a cut write's fence publication and its cut JSON
/// landing would sweep the new fence, miss the not-yet-written cut in its scan, and
/// publish `ready`, leaving a pending activation that indexed readers do not see.
#[test]
fn cut_write_excludes_a_peer_rebuild_until_the_cut_lands() {
    let state_root = broca_test_support::scratch_root().join("fence-peer-cut-write");
    let cuts = state_root.join("agent-data/cuts");
    std::fs::create_dir_all(&cuts).unwrap();
    rebuild(&state_root).unwrap();
    let reference = "0123456789abcdef0123456789abcdef";
    let cut = cuts.join("pending.json");
    let peer_ran = std::rc::Rc::new(std::cell::Cell::new(None));
    let observed = peer_ran.clone();
    let hook_root = state_root.clone();
    let target = cut.clone();
    let _trace = super::super::io_trace::install(move |event| {
        if event == super::super::io_trace::Event::BeforePublish(target.clone()) {
            observed.set(Some(peer_rebuild(&hook_root)?));
        }
        Ok(())
    });
    atomic_json(
        &state_root,
        &cut,
        &serde_json::json!({
            "intent": {"op": "activate"},
            "refs": [{"ref_kind": "broca-session", "ref": reference}]
        }),
    )
    .unwrap();
    // Checked first: without the lock the peer ran and this read finds no fence.
    assert_eq!(
        super::super::cut_fence(&state_root.join("wal"), reference).unwrap(),
        Some("pending".to_owned())
    );
    assert_eq!(
        peer_ran.get(),
        Some(false),
        "a peer rebuild ran concurrently"
    );
}
