//! Exercise the served wire shapes with scripted model replies and real session routes.

mod delivery_rig;

use delivery_rig::{result, Rig, Step};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::time::Duration;
use subc_protocol::Frame;

const SEND: &str = "sample-ordered-send";

fn is_run_finished(frame: &Frame, run_id: &str) -> bool {
    serde_json::from_slice::<Value>(&frame.body).is_ok_and(|body| {
        let event = body.get("event").or_else(|| body.get("unit"));
        event.is_some_and(|event| event["type"] == "run_finished" && event["run_id"] == run_id)
    })
}

async fn capture(
    tag: &str,
    steps: Vec<Step>,
    code: Option<&str>,
    dispatched: usize,
) -> Vec<(String, Vec<u8>)> {
    let mut rig = Rig::start(tag).await;
    rig.script.push(&[Step::Text("first run finished")]);
    rig.script.push(&steps);
    rig.script.hold_model(true);
    let first = rig.send("sample-first-send", "first run", None).await;
    let first_run = result(&first)["run_id"].as_str().unwrap().to_owned();
    rig.wait_entered().await;
    let queued = rig
        .send_with(
            SEND,
            "second run",
            None,
            json!({"delivery":["steer","idle_start"]}),
        )
        .await;
    assert_eq!(result(&queued)["state"], "pending");
    rig.script.hold_model(false);
    rig.wait_for_finished(2).await;
    rig.wait_idle().await;
    assert_eq!(rig.script.dispatched.lock().unwrap().len(), dispatched);

    let send = rig
        .request(
            7,
            json!({"method":"session.send_status","params":{"send_id":SEND}}),
        )
        .await;
    let send_result = result(&send);
    let run_id = send_result["run_id"]
        .as_str()
        .expect("the ordered send started its own run");
    assert_ne!(run_id, first_run);
    assert_eq!(send_result["state"], "finished");
    let reason = if code.is_some() { "error" } else { "completed" };
    assert_eq!(send_result["reason"], reason);

    let status = rig
        .request(7, json!({"method":"run.status","params":{"run_id":run_id}}))
        .await;
    let status_result = result(&status);
    assert_eq!(status_result["state"], reason);
    if let Some(code) = code {
        assert_eq!(status_result["error"]["provider_code"], code);
        assert_eq!(status_result["error"]["class"], "permanent");
    }

    rig.bind(9).await;
    let frames = rig
        .subscribe_collect_bound_until(9, json!({"from":"start"}), Duration::ZERO, |frame| {
            is_run_finished(frame, run_id)
        })
        .await;
    let ended = frames
        .into_iter()
        .find(|frame| is_run_finished(frame, run_id))
        .expect("the replay delivered the second run's end");
    let ended_value: Value = serde_json::from_slice(&ended.body).unwrap();
    let event = ended_value
        .get("event")
        .or_else(|| ended_value.get("unit"))
        .unwrap();
    assert_eq!(event["reason"], reason);
    if tag == "recovered-cut" {
        let transcript = rig
            .request(7, json!({"method":"session.read","params":{"limit":50}}))
            .await;
        assert!(result(&transcript)
            .to_string()
            .contains("the reply hit the output limit before its arguments were complete"));
    }
    rig.finish().await;
    vec![
        (format!("{tag}.run_status.json"), status.body),
        (format!("{tag}.subscribe.run_finished.json"), ended.body),
        (format!("{tag}.send_status.json"), send.body),
    ]
}

#[tokio::test]
async fn runaway_loop_reader_samples() {
    const CALLS: [&str; 20] = [
        "call-01", "call-02", "call-03", "call-04", "call-05", "call-06", "call-07", "call-08",
        "call-09", "call-10", "call-11", "call-12", "call-13", "call-14", "call-15", "call-16",
        "call-17", "call-18", "call-19", "call-20",
    ];
    let mut files = capture(
        "output-limit",
        vec![
            Step::CutCall("cut-1"),
            Step::CutCall("cut-2"),
            Step::CutCall("cut-3"),
            Step::Text("unexpected"),
        ],
        Some("output_limit"),
        0,
    )
    .await;
    let mut repeated: Vec<_> = CALLS.into_iter().map(Step::Call).collect();
    repeated.push(Step::Text("unexpected"));
    files.extend(capture("tool-loop", repeated, Some("tool_loop"), 19).await);
    files.extend(
        capture(
            "recovered-cut",
            vec![
                Step::CutCall("cut-1"),
                Step::Text("recovered after the cut"),
            ],
            None,
            0,
        )
        .await,
    );

    if let Some(dir) = std::env::var_os("BROCA_RUNAWAY_SAMPLE_DIR") {
        let dir = std::path::PathBuf::from(dir);
        std::fs::create_dir_all(&dir).unwrap();
        files.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        let mut sums = String::new();
        for (name, bytes) in files {
            use std::fmt::Write;
            writeln!(sums, "{:x}  {name}", Sha256::digest(&bytes)).unwrap();
            std::fs::write(dir.join(name), bytes).unwrap();
        }
        std::fs::write(dir.join("SHA256SUMS"), sums).unwrap();
    }
}
