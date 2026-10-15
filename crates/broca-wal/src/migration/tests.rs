use super::*;
use crate::{FileWal, Wal, WalRecord};
fn root(tag: &str) -> PathBuf {
    broca_test_support::scratch_root().join(format!("move-sidecars-{}-{tag}", std::process::id()))
}
fn owner(cut: &str) -> Freeze {
    Freeze {
        cut_id: cut.into(),
        end_offset: 17,
        lineage_id: "lineage".into(),
    }
}

#[test]
fn freeze_is_exclusive_and_abort_never_removes_another_cuts_owner() {
    let root = root("owner");
    let wal = root.join("wal/a.wal");
    assert_eq!(freeze(&wal, &owner("a")).unwrap(), Ok(owner("a")));
    let before = fs::read(wal.with_extension("moving")).unwrap();
    assert_eq!(
        freeze(&wal, &owner("b")).unwrap(),
        Err(MoveRefusal::Moving { cut_id: "a".into() })
    );
    abort_freeze(&wal, "b").unwrap();
    assert_eq!(fs::read(wal.with_extension("moving")).unwrap(), before);
    assert_eq!(freeze(&wal, &owner("a")).unwrap(), Ok(owner("a")));
    abort_freeze(&wal, "a").unwrap();
    assert!(!wal.with_extension("moving").exists());
    let late = root.join("wal/late.wal");
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
        freeze(&late, &owner("a")).unwrap(),
        Err(MoveRefusal::Moving {
            cut_id: "late-owner".into()
        })
    );
    assert_eq!(read_json::<Freeze>(&destination).unwrap(), Some(other));
}

#[test]
fn release_publishes_tombstone_before_unlink_and_repeats_clean_its_own_freeze() {
    let root = root("release");
    let wal = root.join("wal/a.wal");
    freeze(&wal, &owner("a")).unwrap().unwrap();
    assert_eq!(
        release(&wal, "b", "target").unwrap(),
        Err(MoveRefusal::Moving { cut_id: "a".into() })
    );
    atomic_json(
        &wal.with_extension("moved"),
        &Moved {
            cut_id: "a".into(),
            holder: "target".into(),
            lineage_id: "lineage".into(),
        },
    )
    .unwrap();
    release(&wal, "a", "target").unwrap().unwrap();
    assert!(!wal.with_extension("moving").exists());
    assert_eq!(
        check(&wal, "ref", false).unwrap(),
        Err(MoveRefusal::Moved {
            holder: "target".into()
        })
    );
    let before = fs::read(wal.with_extension("moved")).unwrap();
    abort_freeze(&wal, "a").unwrap();
    assert_eq!(fs::read(wal.with_extension("moved")).unwrap(), before);
    assert_eq!(
        freeze(&wal, &owner("b")).unwrap(),
        Err(MoveRefusal::Moved {
            holder: "target".into()
        })
    );
}

#[test]
fn intent_fence_is_ref_scoped_read_only_and_survives_reader_downgrade() {
    let root = root("fence");
    let wal = root.join("wal/a.wal");
    durable_dir(wal.parent().unwrap()).unwrap();
    fs::write(&wal, b"installed WAL").unwrap();
    let path = root.join("agent-data/cuts/abc.json");
    for op in ["release", "activate"] {
        atomic_json(&path, &serde_json::json!({"step":"imported","refs":[{"ref_kind":"broca-session","ref":"wanted"}],"intent":{"op":op,"request":{}}})).unwrap();
        let before = fs::read(&path).unwrap();
        assert_eq!(
            check(&wal, "wanted", true).unwrap(),
            Err(MoveRefusal::Moving {
                cut_id: "abc".into()
            })
        );
        assert_eq!(check(&wal, "unrelated", true).unwrap(), Ok(()));
        assert_eq!(check(&wal, "wanted", false).unwrap(), Ok(()));
        assert_eq!(fs::read(&path).unwrap(), before);
        assert_eq!(fs::read(&wal).unwrap(), b"installed WAL");
    }
    atomic_json(&path, &serde_json::json!({"refs":[],"intent":null})).unwrap();
    assert_eq!(check(&wal, "wanted", true).unwrap(), Ok(()));
}

#[test]
fn corrupt_cut_authority_fails_closed_without_repairing_it() {
    let root = root("corrupt");
    let wal = root.join("wal/a.wal");
    let path = root.join("agent-data/cuts/abc.json");
    durable_dir(path.parent().unwrap()).unwrap();
    fs::write(&path, b"not JSON").unwrap();
    assert!(check(&wal, "ref", true).is_err());
    assert_eq!(fs::read(&path).unwrap(), b"not JSON");
}

#[test]
fn staging_digest_and_population_are_not_live_session_inputs() {
    let root = root("staging");
    let path = staging_path(&root.join("wal"), "cut", "addr").unwrap();
    assert!(stage(&path, b"secret", &digest(b"different")).is_err());
    assert!(!path.exists());
    let live = root.join("live.wal");
    let writer = FileWal::open(&live).unwrap();
    writer.sync().unwrap();
    let bytes = fs::read(live).unwrap();
    stage(&path, &bytes, &digest(&bytes)).unwrap();
    assert!(FileWal::open(&path).is_err());
    assert!(staging_path(&root.join("wal"), "../other", "addr").is_err());
}

#[test]
fn existing_writer_rechecks_freeze_before_every_append() {
    let root = root("writer");
    let path = root.join("wal/a.wal");
    let mut wal = FileWal::open(&path).unwrap();
    wal.sync().unwrap();
    let before = fs::read(&path).unwrap();
    freeze(&path, &owner("a")).unwrap().unwrap();
    assert!(matches!(
        wal.append(
            1,
            &WalRecord::ModelStepFinished {
                hooks: None,
                step_id: broca_protocol::StepId(1),
                assistant_message: broca_protocol::content::AssistantMessage {
                    message_id: broca_protocol::MessageId("m".into()),
                    content: vec![]
                }
                .into(),
                usage: broca_protocol::Usage::default(),
                finish_reason: None,
                retries_used: 0,
                service_tier_used: None,
                speed_used: None
            },
            true
        ),
        Err(crate::WalError::SessionMoving { .. })
    ));
    assert_eq!(fs::read(&path).unwrap(), before);
}

#[test]
fn crash_child() {
    let Some(root) = std::env::var_os("BROCA_MOVE_CRASH_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let wal = root.join("wal/a.wal");
    atomic_json(&root.join("agent-data/cuts/kill.json"), &serde_json::json!({"refs":[{"ref_kind":"broca-session","ref":"ref"}],"intent":{"op":"release","request":{}}})).unwrap();
    freeze(&wal, &owner("kill")).unwrap().unwrap();
    release(&wal, "kill", "remote").unwrap().unwrap();
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
        .args(["--exact", "migration::tests::crash_child", "--nocapture"])
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
    let wal = root.join("wal/a.wal");
    assert_eq!(
        check(&wal, "ref", true).unwrap(),
        Err(MoveRefusal::Moving {
            cut_id: "kill".into()
        })
    );
    assert_eq!(
        check(&wal, "ref", false).unwrap(),
        Err(MoveRefusal::Moved {
            holder: "remote".into()
        })
    );
    assert_eq!(
        read_json::<Moved>(&wal.with_extension("moved"))
            .unwrap()
            .unwrap()
            .cut_id,
        "kill"
    );
    assert_eq!(fs::read(path).unwrap(), before);
}

#[test]
fn same_owner_freeze_retry_reestablishes_both_barriers() {
    use super::io_trace::{self, Event};
    use std::{
        cell::{Cell, RefCell},
        rc::Rc,
    };
    let wal = root("retry-barriers").join("wal/a.wal");
    let directory = wal.parent().unwrap().to_owned();
    fs::create_dir_all(&directory).unwrap();
    fs::write(
        wal.with_extension("moving"),
        serde_json::to_vec(&owner("a")).unwrap(),
    )
    .unwrap();
    let events = Rc::new(RefCell::new(Vec::new()));
    let fail = Rc::new(Cell::new(true));
    let observed = events.clone();
    let _trace = io_trace::install(move |event| {
        observed.borrow_mut().push(event.clone());
        if matches!(event, Event::DirectorySync(_)) && fail.replace(false) {
            return Err(io::Error::other("injected freeze directory barrier"));
        }
        Ok(())
    });
    assert!(freeze(&wal, &owner("a"))
        .unwrap_err()
        .to_string()
        .contains("injected freeze directory barrier"));
    assert_eq!(freeze(&wal, &owner("a")).unwrap(), Ok(owner("a")));
    assert_eq!(
        *events.borrow(),
        vec![
            Event::FileSync,
            Event::DirectorySync(directory.clone()),
            Event::FileSync,
            Event::DirectorySync(directory)
        ]
    );
}

#[test]
fn freeze_publishes_only_a_complete_synced_sidecar() {
    use super::io_trace::{self, Event};
    use std::{cell::RefCell, rc::Rc};
    let wal = root("atomic-freeze").join("wal/a.wal");
    let moving = wal.with_extension("moving");
    fs::create_dir_all(wal.parent().unwrap()).unwrap();
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
        assert!(freeze(&wal, &owner("a"))
            .unwrap_err()
            .to_string()
            .contains("injected before freeze publication"));
    }
    assert!(!moving.exists());
    let events = Rc::new(RefCell::new(Vec::new()));
    let observed = events.clone();
    let target = moving.clone();
    let _trace = io_trace::install(move |event| {
        if event == Event::FileSync {
            assert!(!target.exists());
        }
        if matches!(event, Event::Published(_)) {
            assert_eq!(read_json::<Freeze>(&target)?, Some(owner("a")));
        }
        observed.borrow_mut().push(event);
        Ok(())
    });
    assert_eq!(freeze(&wal, &owner("a")).unwrap(), Ok(owner("a")));
    assert_eq!(
        *events.borrow(),
        vec![
            Event::FileSync,
            Event::BeforePublish(moving.clone()),
            Event::Published(moving),
            Event::DirectorySync(wal.parent().unwrap().to_owned())
        ]
    );
}

#[test]
fn unsupported_freeze_publication_fails_closed_without_a_rename_fallback() {
    let wal = root("no-hard-links").join("wal/a.wal");
    let marker = wal.with_extension("moving");
    fs::create_dir_all(wal.parent().unwrap()).unwrap();
    let _trace = super::io_trace::install(|event| {
        if matches!(event, super::io_trace::Event::BeforePublish(_)) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "injected unsupported links",
            ));
        }
        Ok(())
    });
    let error = freeze(&wal, &owner("a")).unwrap_err();
    assert!(
        !marker.exists(),
        "unsupported publication exposed a freeze marker"
    );
    assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    assert!(error.to_string().contains("cannot publish freeze"));
    assert!(error.to_string().contains(marker.to_str().unwrap()));
}
