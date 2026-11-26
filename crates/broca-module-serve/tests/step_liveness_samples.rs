//! Read live status through the module serve connection while the upstream and a tool are held.

mod delivery_rig;

use broca_module::ServeHandler;
use broca_protocol::RunId;
use delivery_rig::{result, Rig};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::time::Duration;
use subc_protocol::Frame;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const IDLE_MS: u64 = 300_000;
const CONTENT_MS: u64 = 1_800_000;
const SSE_HEADERS: &[u8] =
    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n";

async fn request(listener: &TcpListener) -> TcpStream {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        let mut buf = [0; 4096];
        let end = loop {
            let n = stream.read(&mut buf).await.unwrap();
            assert_ne!(n, 0, "the transport closed before sending HTTP headers");
            bytes.extend_from_slice(&buf[..n]);
            if let Some(i) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                break i + 4;
            }
        };
        let len = std::str::from_utf8(&bytes[..end])
            .unwrap()
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .unwrap()
            .1
            .trim()
            .parse::<usize>()
            .unwrap();
        while bytes.len() < end + len {
            let n = stream.read(&mut buf).await.unwrap();
            assert_ne!(n, 0, "the transport closed before sending the request body");
            bytes.extend_from_slice(&buf[..n]);
        }
        stream
    })
    .await
    .expect("model request")
}

async fn status(rig: &mut Rig, run: &str) -> Frame {
    rig.request(7, json!({"method":"run.status","params":{"run_id":run}}))
        .await
}

async fn observe(rig: &mut Rig, run: &str, check: impl Fn(&Value) -> bool + Copy) -> Frame {
    broca_test_support::wait_until("step liveness phase", || {
        let registry = rig.registry().clone();
        let key = rig.session.clone();
        async move {
            let snapshot = registry.run_status(&key, RunId(run.into())).await.unwrap();
            let body = serde_json::to_value(snapshot).unwrap();
            check(&body).then_some(()).ok_or_else(|| body.to_string())
        }
    })
    .await;
    let frame = status(rig, run).await;
    let body = result(&frame);
    assert!(check(&body), "{body}");
    frame
}

fn assert_absent(step: &Value, keys: &[&str]) {
    for key in keys {
        assert!(step.get(key).is_none(), "{key} must be absent: {step}");
    }
}

#[tokio::test]
async fn step_liveness_no_active_step_preserves_status_bytes() {
    let mut rig = Rig::start("step-liveness-no-step").await;
    let reply = status(&mut rig, "missing-run").await;
    assert_eq!(reply.body, br#"{"result":{"state":"unknown"}}"#);
    rig.script.push(&[delivery_rig::Step::Text("done")]);
    let started = rig.send("liveness-complete", "answer", None).await;
    let run = result(&started)["run_id"].as_str().unwrap().to_owned();
    rig.wait_for_finished(1).await;
    let completed = status(&mut rig, &run).await;
    let expected = br#"{"result":{"state":"completed","final_step_finish_reason":"stop","undelivered_steers":[],"usage":{"steps":1,"unreported_steps":1,"by_model":[{"provider":"fake","model":"fake","steps":1,"unreported_steps":1}]},"retries_used":0}}"#;
    assert_eq!(
        completed.body,
        expected,
        "{}",
        String::from_utf8_lossy(&completed.body)
    );
    rig.finish().await;
}

#[tokio::test]
async fn step_liveness_reader_samples() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut rig = Rig::start("step-liveness").await;
    *rig.script.model_url.lock().unwrap() =
        Some(format!("http://{}", listener.local_addr().unwrap()));
    rig.script.hold_model(true);
    rig.script.hold_tool(true);
    let started = rig
        .send("liveness-sample", "answer with a tool", None)
        .await;
    let run = result(&started)["run_id"].as_str().unwrap().to_owned();
    rig.wait_entered().await;
    let preparing = status(&mut rig, &run).await;
    let step = &result(&preparing)["step"];
    assert_eq!(step["phase"], "preparing");
    assert_absent(
        step,
        &[
            "idle_deadline_ms",
            "content_deadline_ms",
            "last_event_at_ms",
        ],
    );

    rig.script.hold_model(false);
    let mut first = request(&listener).await;
    let awaiting = status(&mut rig, &run).await;
    let step = &result(&awaiting)["step"];
    assert_eq!(step["phase"], "awaiting_response");
    let start = step["attempt_started_at_ms"].as_u64().unwrap();
    assert_eq!(step["idle_deadline_ms"], start + CONTENT_MS);
    assert_eq!(step["content_deadline_ms"], start + CONTENT_MS);
    assert_absent(step, &["last_event_at_ms", "last_content_at_ms"]);

    tokio::time::pause();
    first.write_all(b"HTTP/1.1 503 Service Unavailable\r\nRetry-After: 20\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
    drop(first);
    let retrying = observe(&mut rig, &run, |body| body["step"]["phase"] == "retrying").await;
    assert_absent(
        &result(&retrying)["step"],
        &["idle_deadline_ms", "content_deadline_ms"],
    );
    tokio::time::advance(Duration::from_secs(21)).await;
    tokio::time::resume();

    let mut second = request(&listener).await;
    let reply = status(&mut rig, &run).await;
    let step = &result(&reply)["step"];
    assert_eq!(step["phase"], "awaiting_response");
    assert_eq!(step["attempt"], 1);
    assert_absent(step, &["last_event_at_ms", "last_content_at_ms"]);
    let content_deadline = step["content_deadline_ms"].as_u64().unwrap();
    assert_eq!(
        content_deadline,
        step["attempt_started_at_ms"].as_u64().unwrap() + CONTENT_MS
    );
    second.write_all(SSE_HEADERS).await.unwrap();
    let headers = observe(&mut rig, &run, |body| {
        body["step"]["idle_deadline_ms"]
            .as_u64()
            .is_some_and(|idle| idle < content_deadline)
    })
    .await;
    let step = &result(&headers)["step"];
    assert_eq!(step["phase"], "awaiting_response");
    assert_eq!(step["content_deadline_ms"], content_deadline);
    assert_absent(step, &["last_event_at_ms"]);
    second.write_all(b": keepalive\n\n").await.unwrap();
    let keepalive = observe(&mut rig, &run, |body| body["step"]["phase"] == "streaming").await;
    let step = &result(&keepalive)["step"];
    assert_absent(step, &["last_content_at_ms"]);
    assert_eq!(
        step["idle_deadline_ms"],
        step["last_event_at_ms"].as_u64().unwrap() + IDLE_MS
    );
    assert_eq!(step["content_deadline_ms"], content_deadline);

    second.write_all(b"data: {\"id\":\"tools\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"I will call the tool.\"}}]}\n\n").await.unwrap();
    let streaming = observe(&mut rig, &run, |body| {
        body["step"]["last_content_at_ms"].is_u64()
    })
    .await;
    let step = &result(&streaming)["step"];
    assert_eq!(step["phase"], "streaming");
    let last_event = step["last_event_at_ms"].as_u64().unwrap();
    assert_eq!(step["idle_deadline_ms"], last_event + IDLE_MS);
    assert_eq!(step["content_deadline_ms"], last_event + CONTENT_MS);

    second.write_all(concat!(
        "data: {\"id\":\"tools\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-work\",\"type\":\"function\",\"function\":{\"name\":\"work\",\"arguments\":\"{}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n",
        "data: [DONE]\n\n",
    ).as_bytes()).await.unwrap();
    drop(second);
    rig.wait_entered().await;
    let tools = status(&mut rig, &run).await;
    let step = &result(&tools)["step"];
    assert_eq!(step["phase"], "running_tools");
    assert!(step["last_event_at_ms"].as_u64().unwrap() >= last_event);
    assert_absent(step, &["idle_deadline_ms", "content_deadline_ms"]);

    rig.script.hold_tool(false);
    let mut last = request(&listener).await;
    last.write_all(SSE_HEADERS).await.unwrap();
    last.write_all(concat!(
        "data: {\"id\":\"done\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"done\"},\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n",
    ).as_bytes()).await.unwrap();
    drop(last);
    rig.wait_for_finished(1).await;
    let completed = status(&mut rig, &run).await;
    assert_eq!(result(&completed)["state"], "completed");
    assert!(result(&completed).get("step").is_none());
    write_samples([
        ("awaiting_response.json", awaiting),
        ("completed.json", completed),
        ("preparing.json", preparing),
        ("retrying.json", retrying),
        ("running_tools.json", tools),
        ("streaming.json", streaming),
    ]);
    rig.finish().await;
}

fn write_samples<const N: usize>(samples: [(&str, Frame); N]) {
    let Some(dir) = std::env::var_os("BROCA_STEP_LIVENESS_SAMPLE_DIR") else {
        return;
    };
    let dir = std::path::PathBuf::from(dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut sums = String::new();
    for (name, frame) in samples {
        use std::fmt::Write;
        writeln!(sums, "{:x}  {name}", Sha256::digest(&frame.body)).unwrap();
        std::fs::write(dir.join(name), frame.body).unwrap();
    }
    std::fs::write(dir.join("SHA256SUMS"), sums).unwrap();
    std::fs::write(dir.join("README.md"), "# `run.status` step liveness reader samples\n\nRaw response bodies from the real serve connection, with loopback HTTP and a held tool.\nRegenerate with `BROCA_STEP_LIVENESS_SAMPLE_DIR` pointing at this directory and the\n`step_liveness_reader_samples` test. Timestamps are Unix milliseconds and vary per run.\nVerify with `shasum -a 256 -c SHA256SUMS`.\n").unwrap();
}
