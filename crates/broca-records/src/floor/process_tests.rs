use super::*;
use std::{
    fs,
    process::{Child, Command, Stdio},
};

fn probe() -> Option<(PathBuf, String)> {
    Some((
        PathBuf::from(std::env::var_os("BROCA_FLOOR_PROBE_DIR")?),
        std::env::var("BROCA_FLOOR_PROBE_ROLE").ok()?,
    ))
}

fn wait_for(path: &Path) {
    broca_test_support::wait_until_sync("floor child marker", || {
        path.exists()
            .then_some(())
            .ok_or_else(|| format!("{} is absent", path.display()))
    });
}

pub(super) fn before_lock(lock: &crate::fs_ops::File) {
    let Some((dir, role)) = probe() else {
        return;
    };
    if role != "b" {
        return;
    }
    // Observe contention without supplying the lock being tested: a successful
    // probe unlocks immediately, before the production lock acquisition.
    let state = match lock.try_lock() {
        Ok(()) => {
            lock.unlock().unwrap();
            "unlocked"
        }
        Err(std::fs::TryLockError::WouldBlock) => "blocked",
        Err(std::fs::TryLockError::Error(error)) => panic!("floor lock probe failed: {error}"),
    };
    let marker = dir.join("b-state.tmp");
    fs::write(&marker, state).unwrap();
    fs::rename(marker, dir.join("b-state")).unwrap();
}

pub(super) fn after_read() {
    let Some((dir, role)) = probe() else {
        return;
    };
    fs::write(dir.join(format!("{role}-read")), b"ready").unwrap();
    wait_for(&dir.join(format!("{role}-release")));
}

#[test]
fn floor_raise_child() {
    let Some((dir, role)) = probe() else {
        return;
    };
    let feature = if role == "a" {
        "scope/v1"
    } else {
        "steer-queue/v1"
    };
    raise_floor(&dir.join(FEATURE_FLOOR_FILENAME), &[feature]).unwrap();
    fs::write(dir.join(format!("{role}-done")), b"done").unwrap();
}

struct Children(Vec<Child>);
impl Drop for Children {
    fn drop(&mut self) {
        for child in &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn start_child(dir: &Path, role: &str) -> Child {
    Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "floor::process_tests::floor_raise_child",
            "--nocapture",
        ])
        .env("BROCA_FLOOR_PROBE_DIR", dir)
        .env("BROCA_FLOOR_PROBE_ROLE", role)
        .stdout(Stdio::null())
        .spawn()
        .unwrap()
}

#[test]
fn concurrent_process_floor_raises_preserve_union() {
    let dir = broca_test_support::scratch_root()
        .join(format!("floor-process-union-{}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join(FEATURE_FLOOR_FILENAME);
    fs::write(&path, "base/v1\n").unwrap();
    let mut children = Children(vec![start_child(&dir, "a")]);
    wait_for(&dir.join("a-read"));
    children.0.push(start_child(&dir, "b"));
    wait_for(&dir.join("b-state"));
    let state = fs::read_to_string(dir.join("b-state")).unwrap();
    match state.as_str() {
        "blocked" => {}
        // Without the production lock, pause both children after the same stale
        // read. Release their renames in order so loss of the first union is deterministic.
        "unlocked" => wait_for(&dir.join("b-read")),
        other => panic!("unexpected floor lock state {other}"),
    }
    fs::write(dir.join("a-release"), b"go").unwrap();
    wait_for(&dir.join("a-done"));
    wait_for(&dir.join("b-read"));
    fs::write(dir.join("b-release"), b"go").unwrap();
    wait_for(&dir.join("b-done"));
    for child in &mut children.0 {
        assert!(child.wait().unwrap().success());
    }
    assert_eq!(
        read_floor(&path).unwrap(),
        BTreeSet::from(["base/v1".into(), "scope/v1".into(), "steer-queue/v1".into()])
    );
    fs::remove_dir_all(dir).unwrap();
}
