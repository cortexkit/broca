use super::*;
use std::fs;
fn root(tag: &str) -> PathBuf {
    let root = broca_test_support::scratch_root()
        .join(format!("move-sidecars-{}-{tag}", std::process::id()));
    fs::create_dir_all(&root).unwrap();
    root
}
/// What opening a session decides from its fence files, in the order `store_open` reads them:
/// a `.moved` tombstone beside the session's sidecar refuses every open; for a write, so does
/// the sidecar's `.moving` freeze or a pending cut naming `session_ref` under the state root
/// two folders up.
fn check(sidecar: &Path, session_ref: &str, write: bool) -> io::Result<Result<(), MoveRefusal>> {
    if let Some(moved) = read_json::<Moved>(&sidecar.with_extension("moved"))? {
        return Ok(Err(MoveRefusal::Moved {
            holder: moved.holder,
        }));
    }
    if write {
        if let Some(owner) = read_json::<Freeze>(&sidecar.with_extension("moving"))? {
            return Ok(Err(MoveRefusal::Moving {
                cut_id: owner.cut_id,
            }));
        }
        let state_root = sidecar
            .parent()
            .and_then(Path::parent)
            .unwrap_or(Path::new("."));
        if let Some(cut_id) = cut_fence(state_root, session_ref)? {
            return Ok(Err(MoveRefusal::Moving { cut_id }));
        }
    }
    Ok(Ok(()))
}

fn owner(cut: &str) -> Freeze {
    Freeze {
        store_head_version: None,
        cut_id: cut.into(),
        end_offset: 17,
        lineage_id: "lineage".into(),
    }
}

#[test]
#[cfg(unix)]
fn existing_directory_retry_pays_every_parent_barrier_within_state_root() {
    use super::io_trace::{self, Event};
    use std::{cell::RefCell, rc::Rc};
    for suffix in ["agent-data/cuts", "staging/cut", "sessions"] {
        let root = root(&format!("directory-retry-{}", suffix.replace('/', "-")))
            .canonicalize()
            .unwrap();
        let target = root.join(suffix);
        if target.exists() {
            fs::remove_dir_all(&target).unwrap();
        }
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        let child = target.clone();
        let parent = target.parent().unwrap().to_owned();
        {
            let mut injected = false;
            let _trace = io_trace::install(move |event| {
                if event == Event::BeforeDirectorySync(parent.clone())
                    && child.exists()
                    && !injected
                {
                    injected = true;
                    return Err(io::Error::other(
                        "injected after directory creation before parent barrier",
                    ));
                }
                Ok(())
            });
            assert!(durable_dir(&root, &target)
                .unwrap_err()
                .to_string()
                .contains("injected after directory creation"));
        }
        assert!(target.is_dir());
        let events = Rc::new(RefCell::new(Vec::new()));
        let observed = events.clone();
        let _trace = io_trace::install(move |event| {
            if let Event::DirectorySync(path) = event {
                observed.borrow_mut().push(path);
            }
            Ok(())
        });
        durable_dir(&root, &target).unwrap();
        let expected: Vec<_> = target
            .ancestors()
            .take_while(|path| path.starts_with(&root))
            .map(Path::to_owned)
            .collect();
        assert_eq!(
            *events.borrow(),
            expected,
            "a cached child must not hide an unpaid ancestor barrier"
        );
    }
}

#[test]
fn directory_establishment_accepts_resolved_parent_spelling() {
    let root = root("parent-spelling").canonicalize().unwrap();
    let configured = root.join("..").join(root.file_name().unwrap());
    for suffix in ["agent-data/cuts", "staging/cut", "sessions"] {
        let target = configured.join(suffix);
        durable_dir(&configured, &target).unwrap();
        assert_eq!(target.canonicalize().unwrap(), root.join(suffix));
        durable_dir(&configured, &target).unwrap();
    }
    let target = configured.join("missing-component/../resolved-child");
    durable_dir(&configured, &target).unwrap();
    assert!(root.join("resolved-child").is_dir());
    assert!(!root.join("missing-component").exists());
}

#[test]
fn directory_establishment_requires_an_existing_root_and_rejects_escape() {
    let root = root("directory-boundary").canonicalize().unwrap();
    let missing = root.join("missing-state-root");
    let error = durable_dir(&missing, &missing.join("sessions")).unwrap_err();
    assert!(error.to_string().contains("move state root"));
    assert!(!missing.exists());
    let outside = root.with_file_name("outside-move-state-root");
    let error = durable_dir(&root, &outside.join("sessions")).unwrap_err();
    assert!(error.to_string().contains("outside state root"));
    assert!(!outside.exists());
    let escape = root.join("../escape");
    let error = durable_dir(&root, &escape).unwrap_err();
    assert!(error.to_string().contains("outside state root"));
    assert!(error.to_string().contains(escape.to_str().unwrap()));
    assert!(!root.parent().unwrap().join("escape").exists());
    let error = durable_dir(
        &root,
        &root.join("missing-component/../../escape-after-missing"),
    )
    .unwrap_err();
    assert!(error.to_string().contains("outside state root"));
    assert!(!root.join("missing-component").exists());
    assert!(!root.parent().unwrap().join("escape-after-missing").exists());
    #[cfg(unix)]
    {
        let link = root.join("escape-link");
        if link.exists() {
            fs::remove_file(&link).unwrap();
        }
        std::os::unix::fs::symlink(root.parent().unwrap(), &link).unwrap();
        let error = durable_dir(&root, &link.join("escape")).unwrap_err();
        assert!(error.to_string().contains("outside state root"));
        let error = durable_dir(&root, &link.join("missing-component/../escape")).unwrap_err();
        assert!(error.to_string().contains("outside state root"));
    }
}

#[cfg(unix)]
#[test]
fn successful_sync_events_require_the_actual_sync_call_to_succeed() {
    use super::io_trace::{self, Event};
    use std::{cell::RefCell, rc::Rc};
    let root = root("successful-sync-observation");
    let file = crate::fs_ops::File::create(root.join("data")).unwrap();
    let unsyncable = crate::fs_ops::File::open("/dev/null").unwrap();
    let events = Rc::new(RefCell::new(Vec::new()));
    let observed = events.clone();
    let _trace = io_trace::install(move |event| {
        if matches!(event, Event::FileSync | Event::DirectorySync(_)) {
            observed.borrow_mut().push(event);
        }
        Ok(())
    });
    assert!(
        sync_file(&unsyncable).is_err(),
        "a character device cannot acknowledge a file barrier"
    );
    assert!(
        events.borrow().is_empty(),
        "a failed syscall must not emit FileSync"
    );
    assert!(sync_dir(Path::new("/dev/null")).is_err());
    assert!(
        events.borrow().is_empty(),
        "a failed syscall must not emit DirectorySync"
    );
    sync_file(&file).unwrap();
    sync_dir(&root).unwrap();
    assert_eq!(
        *events.borrow(),
        vec![Event::FileSync, Event::DirectorySync(root)]
    );
}

#[test]
fn freeze_is_exclusive_and_abort_never_removes_another_cuts_owner() {
    let root = root("owner");
    let sidecar = root.join("sessions/a");
    assert_eq!(
        freeze(&root, &sidecar, &owner("a")).unwrap(),
        Ok(owner("a"))
    );
    let before = fs::read(sidecar.with_extension("moving")).unwrap();
    assert_eq!(
        freeze(&root, &sidecar, &owner("b")).unwrap(),
        Err(MoveRefusal::Moving { cut_id: "a".into() })
    );
    abort_freeze(&sidecar, "b").unwrap();
    assert_eq!(fs::read(sidecar.with_extension("moving")).unwrap(), before);
    assert_eq!(
        freeze(&root, &sidecar, &owner("a")).unwrap(),
        Ok(owner("a"))
    );
    abort_freeze(&sidecar, "a").unwrap();
    assert!(!sidecar.with_extension("moving").exists());
    let late = root.join("sessions/late");
    let destination = late.with_extension("moving");
    let other = owner("late-owner");
    let copy = other.clone();
    let path = destination.clone();
    let mut injected = false;
    let _trace = super::io_trace::install(move |event| {
        if event == super::io_trace::Event::FileSync && !injected {
            injected = true;
            fs::write(&path, serde_json::to_vec(&copy).unwrap())?;
        }
        Ok(())
    });
    assert_eq!(
        freeze(&root, &late, &owner("a")).unwrap(),
        Err(MoveRefusal::Moving {
            cut_id: "late-owner".into()
        })
    );
    assert_eq!(read_json::<Freeze>(&destination).unwrap(), Some(other));
}

#[test]
fn release_publishes_tombstone_before_unlink_and_repeats_clean_its_own_freeze() {
    let root = root("release");
    let sidecar = root.join("sessions/a");
    freeze(&root, &sidecar, &owner("a")).unwrap().unwrap();
    assert_eq!(
        release(&root, &sidecar, "b", "target").unwrap(),
        Err(MoveRefusal::Moving { cut_id: "a".into() })
    );
    atomic_json(
        &root,
        &sidecar.with_extension("moved"),
        &Moved {
            cut_id: "a".into(),
            holder: "target".into(),
            lineage_id: "lineage".into(),
        },
    )
    .unwrap();
    release(&root, &sidecar, "a", "target").unwrap().unwrap();
    assert!(!sidecar.with_extension("moving").exists());
    assert_eq!(
        check(&sidecar, "ref", false).unwrap(),
        Err(MoveRefusal::Moved {
            holder: "target".into()
        })
    );
    let before = fs::read(sidecar.with_extension("moved")).unwrap();
    abort_freeze(&sidecar, "a").unwrap();
    assert_eq!(fs::read(sidecar.with_extension("moved")).unwrap(), before);
    assert_eq!(
        freeze(&root, &sidecar, &owner("b")).unwrap(),
        Err(MoveRefusal::Moved {
            holder: "target".into()
        })
    );
}

#[test]
fn intent_fence_is_ref_scoped_read_only_and_survives_reader_downgrade() {
    let root = root("fence");
    let sidecar = root.join("sessions/a");
    durable_dir(&root, sidecar.parent().unwrap()).unwrap();
    fs::write(&sidecar, b"installed session").unwrap();
    let path = root.join("agent-data/cuts/abc.json");
    for op in ["release", "activate"] {
        atomic_json(&root, &path, &serde_json::json!({"step":"imported","refs":[{"ref_kind":"broca-session","ref":"wanted"}],"intent":{"op":op,"request":{}}})).unwrap();
        let before = fs::read(&path).unwrap();
        assert_eq!(
            check(&sidecar, "wanted", true).unwrap(),
            Err(MoveRefusal::Moving {
                cut_id: "abc".into()
            })
        );
        assert_eq!(check(&sidecar, "unrelated", true).unwrap(), Ok(()));
        assert_eq!(check(&sidecar, "wanted", false).unwrap(), Ok(()));
        assert_eq!(fs::read(&path).unwrap(), before);
        assert_eq!(fs::read(&sidecar).unwrap(), b"installed session");
    }
    atomic_json(&root, &path, &serde_json::json!({"refs":[],"intent":null})).unwrap();
    assert_eq!(check(&sidecar, "wanted", true).unwrap(), Ok(()));
}

#[test]
fn corrupt_cut_authority_fails_closed_without_repairing_it() {
    let root = root("corrupt");
    let sidecar = root.join("sessions/a");
    let path = root.join("agent-data/cuts/abc.json");
    durable_dir(&root, path.parent().unwrap()).unwrap();
    fs::write(&path, b"not JSON").unwrap();
    assert!(check(&sidecar, "ref", true).is_err());
    assert_eq!(fs::read(&path).unwrap(), b"not JSON");
}

#[test]
fn crash_child() {
    let Some(root) = std::env::var_os("BROCA_MOVE_CRASH_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let sidecar = root.join("sessions/a");
    atomic_json(&root, &root.join("agent-data/cuts/kill.json"), &serde_json::json!({"refs":[{"ref_kind":"broca-session","ref":"ref"}],"intent":{"op":"release","request":{}}})).unwrap();
    freeze(&root, &sidecar, &owner("kill")).unwrap().unwrap();
    release(&root, &sidecar, "kill", "remote").unwrap().unwrap();
    println!("ARTIFACTS_DURABLE");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[test]
fn real_process_kill_keeps_the_intent_and_tombstone_owned_after_restart() {
    use std::io::BufRead;
    let root = root("kill");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "move_fence::tests::crash_child", "--nocapture"])
        .env("BROCA_MOVE_CRASH_ROOT", &root)
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut output = std::io::BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    loop {
        line.clear();
        assert_ne!(output.read_line(&mut line).unwrap(), 0);
        if line.contains("ARTIFACTS_DURABLE") {
            break;
        }
    }
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());
    let path = root.join("agent-data/cuts/kill.json");
    let before = fs::read(&path).unwrap();
    let sidecar = root.join("sessions/a");
    // The tombstone refuses reads and writes alike, and the cut's unfinished release still
    // fences the session on its own.
    for write in [true, false] {
        assert_eq!(
            check(&sidecar, "ref", write).unwrap(),
            Err(MoveRefusal::Moved {
                holder: "remote".into()
            })
        );
    }
    assert_eq!(cut_fence(&root, "ref").unwrap().as_deref(), Some("kill"));
    assert_eq!(
        read_json::<Moved>(&sidecar.with_extension("moved"))
            .unwrap()
            .unwrap()
            .cut_id,
        "kill"
    );
    assert_eq!(fs::read(path).unwrap(), before);
}

#[test]
#[cfg(unix)]
fn same_owner_freeze_retry_reestablishes_both_barriers() {
    use super::io_trace::{self, Event};
    use std::{
        cell::{Cell, RefCell},
        rc::Rc,
    };
    let root = root("retry-barriers");
    let sidecar = root.join("sessions/a");
    let directory = sidecar.parent().unwrap().to_owned();
    fs::create_dir_all(&directory).unwrap();
    fs::write(
        sidecar.with_extension("moving"),
        serde_json::to_vec(&owner("a")).unwrap(),
    )
    .unwrap();
    let events = Rc::new(RefCell::new(Vec::new()));
    let fail = Rc::new(Cell::new(true));
    let observed = events.clone();
    let mut after_file = false;
    let _trace = io_trace::install(move |event| {
        if matches!(event, Event::BeforeDirectorySync(_)) && after_file && fail.replace(false) {
            after_file = false;
            return Err(io::Error::other("injected freeze directory barrier"));
        }
        if matches!(event, Event::BeforeFileSync | Event::BeforeDirectorySync(_)) {
            return Ok(());
        }
        if event == Event::FileSync {
            after_file = true;
        } else if matches!(event, Event::DirectorySync(_)) {
            if !after_file {
                return Ok(());
            }
            after_file = false;
        }
        observed.borrow_mut().push(event.clone());
        Ok(())
    });
    assert!(freeze(&root, &sidecar, &owner("a"))
        .unwrap_err()
        .to_string()
        .contains("injected freeze directory barrier"));
    assert_eq!(
        freeze(&root, &sidecar, &owner("a")).unwrap(),
        Ok(owner("a"))
    );
    assert_eq!(
        *events.borrow(),
        vec![
            Event::FileSync,
            Event::FileSync,
            Event::DirectorySync(directory)
        ]
    );
}

#[test]
#[cfg(unix)]
fn freeze_publishes_only_a_complete_synced_sidecar() {
    use super::io_trace::{self, Event};
    use std::{cell::RefCell, rc::Rc};
    let root = root("atomic-freeze");
    let sidecar = root.join("sessions/a");
    let moving = sidecar.with_extension("moving");
    fs::create_dir_all(sidecar.parent().unwrap()).unwrap();
    {
        let target = moving.clone();
        let _trace = io_trace::install(move |event| {
            if event == Event::FileSync {
                assert!(
                    !target.exists(),
                    "freeze became visible before its file barrier"
                );
                return Err(io::Error::other("injected before freeze publication"));
            }
            Ok(())
        });
        assert!(freeze(&root, &sidecar, &owner("a"))
            .unwrap_err()
            .to_string()
            .contains("injected before freeze publication"));
    }
    assert!(!moving.exists());
    let events = Rc::new(RefCell::new(Vec::new()));
    let observed = events.clone();
    let target = moving.clone();
    let _trace = io_trace::install(move |event| {
        if matches!(event, Event::BeforeFileSync | Event::BeforeDirectorySync(_)) {
            return Ok(());
        }
        if matches!(event, Event::DirectorySync(_)) && observed.borrow().is_empty() {
            return Ok(());
        }
        if event == Event::FileSync {
            assert!(!target.exists());
        }
        if matches!(event, Event::Published(_)) {
            assert_eq!(read_json::<Freeze>(&target)?, Some(owner("a")));
        }
        observed.borrow_mut().push(event);
        Ok(())
    });
    assert_eq!(
        freeze(&root, &sidecar, &owner("a")).unwrap(),
        Ok(owner("a"))
    );
    assert_eq!(
        *events.borrow(),
        vec![
            Event::FileSync,
            Event::BeforePublish(moving.clone()),
            Event::Published(moving),
            Event::DirectorySync(sidecar.parent().unwrap().to_owned())
        ]
    );
}

#[test]
fn unsupported_freeze_publication_fails_closed_without_a_rename_fallback() {
    let root = root("no-hard-links");
    let sidecar = root.join("sessions/a");
    let marker = sidecar.with_extension("moving");
    fs::create_dir_all(sidecar.parent().unwrap()).unwrap();
    let _trace = super::io_trace::install(|event| {
        if matches!(event, super::io_trace::Event::BeforePublish(_)) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "injected unsupported links",
            ));
        }
        Ok(())
    });
    let error = freeze(&root, &sidecar, &owner("a")).unwrap_err();
    assert!(
        !marker.exists(),
        "unsupported publication exposed a freeze marker"
    );
    assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    assert!(error.to_string().contains("cannot publish freeze"));
    assert!(error.to_string().contains(marker.to_str().unwrap()));
}

#[test]
#[cfg(unix)]
fn unlink_retry_syncs_directory_after_a_failed_removal_barrier() {
    use super::io_trace::{self, Event};
    use std::{
        cell::{Cell, RefCell},
        rc::Rc,
    };
    for extension in ["moving", "moved"] {
        let directory = root(&format!("unlink-retry-{extension}"));
        fs::create_dir_all(&directory).unwrap();
        let marker = directory.join(format!("a.{extension}"));
        fs::write(&marker, b"marker").unwrap();
        let events = Rc::new(RefCell::new(Vec::new()));
        let observed = events.clone();
        let fail = Cell::new(true);
        let _trace = io_trace::install(move |event| {
            if let Event::DirectorySync(_) = event {
                observed.borrow_mut().push(event.clone());
            }
            if matches!(event, Event::BeforeDirectorySync(_)) && fail.replace(false) {
                return Err(io::Error::other("injected removal barrier"));
            }
            Ok(())
        });
        assert!(unlink(&marker)
            .unwrap_err()
            .to_string()
            .contains("injected removal barrier"));
        assert!(!marker.exists());
        unlink(&marker).unwrap();
        assert_eq!(*events.borrow(), vec![Event::DirectorySync(directory)]);
    }
}

#[test]
#[cfg(unix)]
fn abort_retry_syncs_directory_when_its_freeze_is_already_absent() {
    use super::io_trace::{self, Event};
    use std::{
        cell::{Cell, RefCell},
        rc::Rc,
    };
    let root = root("abort-barrier-retry");
    let sidecar = root.join("sessions/a");
    freeze(&root, &sidecar, &owner("a")).unwrap().unwrap();
    let events = Rc::new(RefCell::new(Vec::new()));
    let observed = events.clone();
    let fail = Cell::new(true);
    let _trace = io_trace::install(move |event| {
        if let Event::DirectorySync(_) = event {
            observed.borrow_mut().push(event.clone());
        }
        if matches!(event, Event::BeforeDirectorySync(_)) && fail.replace(false) {
            return Err(io::Error::other("injected abort barrier"));
        }
        Ok(())
    });
    assert!(abort_freeze(&sidecar, "a").is_err());
    assert!(!sidecar.with_extension("moving").exists());
    abort_freeze(&sidecar, "a").unwrap();
    assert_eq!(
        *events.borrow(),
        vec![Event::DirectorySync(sidecar.parent().unwrap().to_owned())]
    );
}

#[test]
#[cfg(unix)]
fn release_retry_syncs_directory_when_its_freeze_is_already_absent() {
    use super::io_trace::{self, Event};
    use std::{
        cell::{Cell, RefCell},
        rc::Rc,
    };
    let root = root("release-barrier-retry");
    let sidecar = root.join("sessions/a");
    freeze(&root, &sidecar, &owner("a")).unwrap().unwrap();
    let events = Rc::new(RefCell::new(Vec::new()));
    let observed = events.clone();
    let fail = Cell::new(true);
    let moving = sidecar.with_extension("moving");
    let _trace = io_trace::install(move |event| {
        if let Event::DirectorySync(_) = event {
            observed.borrow_mut().push(event.clone());
        }
        if matches!(event, Event::BeforeDirectorySync(_)) && !moving.exists() && fail.replace(false)
        {
            return Err(io::Error::other("injected release removal barrier"));
        }
        Ok(())
    });
    assert!(release(&root, &sidecar, "a", "holder").is_err());
    assert!(!sidecar.with_extension("moving").exists());
    events.borrow_mut().clear();
    assert_eq!(release(&root, &sidecar, "a", "holder").unwrap(), Ok(()));
    assert_eq!(
        *events.borrow(),
        vec![Event::DirectorySync(sidecar.parent().unwrap().to_owned())]
    );
}

#[test]
#[cfg(unix)]
fn absent_staging_parents_still_sync_the_nearest_existing_directory() {
    use super::io_trace::{self, Event};
    use std::{cell::RefCell, rc::Rc};
    let directory = root("missing-staging-parent");
    fs::create_dir_all(&directory).unwrap();
    let marker = directory.join("staging/cut/a");
    let events = Rc::new(RefCell::new(Vec::new()));
    let observed = events.clone();
    let _trace = io_trace::install(move |event| {
        observed.borrow_mut().push(event);
        Ok(())
    });
    unlink(&marker).unwrap();
    assert_eq!(
        events.borrow().last(),
        Some(&Event::DirectorySync(directory))
    );
}

#[test]
fn indexed_fences_track_intent_publication_completion_and_restart() {
    let root =
        broca_test_support::scratch_root().join(format!("indexed-fences-{}", std::process::id()));
    std::fs::create_dir_all(root.join("sessions")).unwrap();
    let reference = "0123456789abcdef0123456789abcdef";
    let sidecar = root.join("sessions/session");
    let cut = root.join("agent-data/cuts/cut.json");
    let pending = serde_json::json!({"intent":{"op":"activate"},"refs":[{"ref_kind":"broca-session","ref":reference}]});
    rebuild_fence_index(&root).unwrap();
    assert_eq!(check(&sidecar, reference, true).unwrap(), Ok(()));
    atomic_json(&root, &cut, &pending).unwrap();
    assert!(
        matches!(check(&sidecar,reference,true).unwrap(),Err(MoveRefusal::Moving { cut_id }) if cut_id=="cut")
    );
    assert_eq!(check(&sidecar, reference, false).unwrap(), Ok(()));
    assert_eq!(
        check(&sidecar, "00000000000000000000000000000000", true).unwrap(),
        Ok(())
    );
    atomic_json(&root, &cut, &serde_json::json!({"intent":null})).unwrap();
    assert_eq!(check(&sidecar, reference, true).unwrap(), Ok(()));
    // An older binary updates only cut JSON. Boot must rebuild before enabling
    // the shortcut, rather than trusting the previous process's ready marker.
    std::fs::write(&cut, serde_json::to_vec(&pending).unwrap()).unwrap();
    rebuild_fence_index(&root).unwrap();
    assert!(matches!(
        check(&sidecar, reference, true).unwrap(),
        Err(MoveRefusal::Moving { .. })
    ));
    std::fs::write(&cut, b"not json").unwrap();
    assert!(rebuild_fence_index(&root).is_err());
    assert!(check(&sidecar, reference, true).is_err());
}
