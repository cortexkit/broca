use broca_test_support::SupervisedChild;
use serde::Deserialize;
use std::path::Path;
use std::time::Duration;
use tokio::process::Child;

#[derive(Deserialize)]
struct Roster {
    children: Vec<RosterChild>,
}

#[derive(Deserialize)]
struct RosterChild {
    module_id: String,
    pid: u32,
    start_time: u64,
}

pub(super) fn live_child(
    supervisor: &Child,
    data_home: &Path,
    module_id: &str,
) -> Result<SupervisedChild, String> {
    let roster = data_home.join("cortexkit/run/live-children.json");
    let bytes = std::fs::read(&roster).map_err(|error| format!("{}: {error}", roster.display()))?;
    let record: Roster = serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    let child = record
        .children
        .into_iter()
        .find(|child| child.module_id == module_id)
        .ok_or_else(|| format!("supervisor roster has no {module_id} child"))?;
    SupervisedChild::capture(supervisor, child.pid, child.start_time)
}

pub(super) async fn wait_for_replacement(
    supervisor: &Child,
    data_home: &Path,
    module_id: &str,
    old: (u32, u64),
) -> SupervisedChild {
    broca_test_support::wait_until(&format!("replacement {module_id} process"), || async {
        let replacement = live_child(supervisor, data_home, module_id)?;
        if replacement.stamp() == old {
            return Err(format!("supervisor still names the old child {old:?}"));
        }
        Ok(replacement)
    })
    .await
}

pub(super) async fn restart_broca(connection: &Path, drain: Option<Duration>) {
    let consumer = subc_client_rs::SubcConsumer::connect_local(
        connection,
        subc_client_rs::ConsumerOptions {
            call_timeout: broca_test_support::EVENT_HANG_GUARD,
            ..Default::default()
        },
    )
    .await
    .expect("connect to the rig daemon's control pipe");
    let drain_ms = drain.map(|duration| u64::try_from(duration.as_millis()).unwrap());
    let ack = consumer
        .supervisor_restart("broca", drain_ms)
        .await
        .expect("request broca restart");
    assert_eq!(ack.module_id, "broca");
    assert!(ack.applied, "broca restart not applied: {ack:?}");
}
