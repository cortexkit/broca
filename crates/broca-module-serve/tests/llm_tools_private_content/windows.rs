use super::{hex, Rig, StandInHold, Stub};
use broca_test_support::{wait_until, SupervisedChild};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use tokio::process::{Child, Command};

pub(super) fn module_config(root: &Path, module: &str, fixture: &Path, hold: StandInHold) -> Value {
    // The module that HELLOs must be the child the daemon started. A Windows wrapper
    // cannot replace itself the way POSIX exec does.
    if matches!(hold, StandInHold::Handoff | StandInHold::LateHandoff) {
        return json!({
            "program": broca_test_support::ckdev_binary(std::env::current_exe().unwrap(), broca_test_support::scratch_root()),
            // Stop libtest option parsing before the daemon appends `--subc <path>`.
            "args": ["--exact", "private_content_handoff", "--nocapture", "--"],
            "reserved": true, "enabled": true,
            "env": {"PRIVATE_CONTENT_ROOT": root, "SURFACE_MODULE_ID": module},
        });
    }
    json!({
        "program": super::fake_surface_bin(),
        "args": ["--module-id", module,
                 format!("--SURFACE_FIXTURES={}", hex(fixture)),
                 format!("--SURFACE_REQUESTS={}", hex(&root.join(format!("{module}.requests")))),
                 format!("--SURFACE_NONCE_PATH={}", hex(&root.join(format!("{module}.nonce"))))],
        "reserved": true, "enabled": true,
        "env": {"SUBC_CONNECTION": root.join("runtime/subc-connection.json")},
    })
}

fn live_child(rig: &Rig, module: &str) -> Result<SupervisedChild, String> {
    let roster = rig.root.join("data/cortexkit/run/live-children.json");
    let bytes = std::fs::read(&roster).map_err(|error| format!("{}: {error}", roster.display()))?;
    let record: Value = serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    let child = record["children"]
        .as_array()
        .ok_or("missing children array")?
        .iter()
        .find(|child| child["module_id"] == module)
        .ok_or_else(|| format!("no {module} child"))?;
    let pid = u32::try_from(child["pid"].as_u64().ok_or("missing module PID")?)
        .map_err(|error| error.to_string())?;
    SupervisedChild::capture(
        &rig._daemon,
        pid,
        child["start_time"]
            .as_u64()
            .ok_or("missing module birth time")?,
    )
}

pub(super) async fn restart_broca(rig: &Rig) {
    let old = live_child(rig, "broca").expect("the rig supervises a live broca");
    old.force_stop().unwrap();
    wait_until("replacement broca process", || async {
        let child = live_child(rig, "broca")?;
        (child.stamp() != old.stamp())
            .then_some(())
            .ok_or_else(|| "daemon still lists the old broca".into())
    })
    .await;
}

fn held_standin() -> Command {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "private_content_held_standin", "--nocapture"])
        .env("PRIVATE_CONTENT_HELD_STANDIN", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    command
}

fn ledger(root: &Path, module: &str) -> PathBuf {
    root.join(format!("{module}.stand-in.json"))
}

pub(super) async fn handoff() {
    let Some(root) = std::env::var_os("PRIVATE_CONTENT_ROOT").map(PathBuf::from) else {
        return;
    };
    let module = std::env::var("SURFACE_MODULE_ID").unwrap();
    let mut child = held_standin().spawn().unwrap();
    let pid = child.id().unwrap();
    let process = subc_os::Process::open(pid).unwrap().unwrap();
    let birth = process.observe().unwrap().start_time;
    let path = ledger(&root, &module);
    let staged = root.join(format!("{module}.{pid}.tmp"));
    std::fs::write(&staged, serde_json::to_vec(&(pid, birth)).unwrap()).unwrap();
    if root.join("hold-standin-publication").exists() {
        std::fs::write(root.join(format!("{module}.stand-in-started")), b"started").unwrap();
        wait_until("release of held stand-in publication", || async {
            (!root.join("hold-standin-publication").exists())
                .then_some(())
                .ok_or_else(|| "stand-in publication still held".into())
        })
        .await;
    }
    std::fs::rename(staged, path).unwrap();
    let _ = child.wait().await.unwrap();
}

async fn capture_handoff(rig: &Rig, module: &str) -> (SupervisedChild, Arc<SupervisedChild>) {
    wait_until(&format!("{module} stand-in published"), || async {
        let parent = live_child(rig, module)?;
        let bytes = std::fs::read(ledger(&rig.root, module)).map_err(|error| error.to_string())?;
        let (pid, birth) = serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
        let child = parent.capture_descendant(pid, birth)?;
        Ok((parent, Arc::new(child)))
    })
    .await
}

async fn held_rig(name: &str) -> Option<Rig> {
    Rig::start_with_hold(
        name,
        Stub::Unreachable,
        Stub::Unreachable,
        StandInHold::Handoff,
    )
    .await
}

// The probe waits until assertions have checked for survivors before stopping any process
// the rig missed. Its Drop also runs if an assertion fails.
struct Probe {
    processes: Vec<Arc<SupervisedChild>>,
    lookalike: Child,
}

impl Drop for Probe {
    fn drop(&mut self) {
        for process in &self.processes {
            let _ = process.force_stop();
        }
        broca_test_support::kill_and_reap(&mut self.lookalike);
    }
}

pub(super) async fn cleanup_probe(name: &str, timeout: bool, orphan: bool) {
    let Some(mut rig) = held_rig(name).await else {
        return;
    };
    let mut probe = Probe {
        processes: Vec::new(),
        lookalike: held_standin().spawn().unwrap(),
    };
    for module in ["cerebellum", "basal", "cingulate"] {
        let (parent, child) = capture_handoff(&rig, module).await;
        if orphan {
            parent.force_stop().unwrap();
            assert!(
                !child.has_exited().unwrap(),
                "{module} stand-in did not outlive its helper"
            );
            rig.orphaned_standins.push(child.clone());
        }
        probe.processes.push(Arc::new(parent));
        probe.processes.push(child);
    }
    let task = tokio::spawn(async move {
        let _rig = rig;
        if timeout {
            tokio::time::pause();
            wait_until(
                "deliberately stuck private-content rig",
                std::future::pending::<Result<(), String>>,
            )
            .await;
        }
        panic!("deliberate private-content rig failure");
    });
    let failure = task.await.expect_err("the rig owner must unwind");
    if timeout {
        tokio::time::resume();
    }
    assert!(failure.is_panic());
    assert!(
        probe.lookalike.try_wait().unwrap().is_none(),
        "cleanup killed the unrelated look-alike"
    );
    for process in &probe.processes {
        assert!(
            process.has_exited().unwrap(),
            "rig process {:?} survived unwinding drop",
            process.stamp()
        );
    }
}

pub(super) async fn cleanup_late_standin() {
    let Some(rig) = Rig::start_with_hold(
        "cleanup-late-standin",
        Stub::Unreachable,
        Stub::Unreachable,
        StandInHold::LateHandoff,
    )
    .await
    else {
        return;
    };
    let early = wait_until(
        "the helper exists before publishing its stand-in",
        || async {
            let helper = live_child(&rig, "cerebellum")?;
            std::fs::metadata(rig.root.join("cerebellum.stand-in-started"))
                .map_err(|error| error.to_string())?;
            Ok(helper)
        },
    )
    .await;
    assert!(!ledger(&rig.root, "cerebellum").exists());
    std::fs::remove_file(rig.root.join("hold-standin-publication")).unwrap();
    let (parent, child) = capture_handoff(&rig, "cerebellum").await;
    assert_eq!(
        parent.stamp(),
        early.stamp(),
        "the held helper must make the late publication"
    );
    drop(rig);
    let survived = !child.has_exited().unwrap();
    if survived {
        child.force_stop().unwrap();
    }
    assert!(
        !survived,
        "late stand-in {:?} survived cleanup",
        child.stamp()
    );
    assert!(
        parent.has_exited().unwrap(),
        "the late stand-in's helper survived"
    );
}
