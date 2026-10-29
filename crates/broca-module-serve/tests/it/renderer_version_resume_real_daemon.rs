//! A deploy that bumps a wire family's renderer version must not kill a resumable run whose bytes
//! the new code still reproduces.
//!
//! The real daemon supervises the real `ck-broca`; the model is a local HTTP server answering
//! Gemini `streamGenerateContent` SSE, reached through a catalog LKG that lists one Gemini
//! model. A template run first shows the exact `RunStarted` the current build records. The test
//! then writes, for two fresh sessions, the `RunStarted` an earlier build recorded for the same
//! admission: no `provider_text_state` field and an earlier renderer version, cut before any
//! step, as a run left by a process that stopped right after admitting it. A send on each
//! session resumes that run first:
//! - recorded at Gemini v3, which the current code declares reproducible, the run resumes,
//!   calls the model and completes;
//! - recorded at Gemini v2, which it does not declare, the run is refused with
//!   `renderer_version_drift`, makes no model call and is sealed `Interrupted`.
//!
//! Unix only, like the other real-daemon rigs that drive the supervisor binary.
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use broca_core::WalStore as _;
use broca_protocol::caller::RunFinishReason;
use broca_wal::WalRecord;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Child;
use tokio::time::{sleep, Instant};

const PROVIDER: &str = "google";
const MODEL: &str = "gemini-2.5-flash";

/// Every model response: one Gemini SSE chunk carrying a short answer and its usage.
const TEXT_SSE: &str = concat!(
    "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"done\"}],\"role\":\"model\"},",
    "\"finishReason\":\"STOP\",\"index\":0}],\"usageMetadata\":{\"promptTokenCount\":5,",
    "\"candidatesTokenCount\":1}}\n\n",
);

fn require_subc() -> bool {
    std::env::var_os("BROCA_REQUIRE_SUBC").is_some()
}

/// Answers every request with `TEXT_SSE` and records each request body.
async fn serve_model(bodies: Arc<Mutex<Vec<Vec<u8>>>>) -> String {
    serve_model_sse(bodies, TEXT_SSE).await
}

async fn serve_model_sse(bodies: Arc<Mutex<Vec<Vec<u8>>>>, response: &'static str) -> String {
    serve_model_responses(bodies, vec![response]).await
}

async fn serve_model_responses(
    bodies: Arc<Mutex<Vec<Vec<u8>>>>,
    responses: Vec<&'static str>,
) -> String {
    serve_model_responses_paused(bodies, responses, None).await
}

async fn serve_model_responses_paused(
    bodies: Arc<Mutex<Vec<Vec<u8>>>>,
    responses: Vec<&'static str>,
    pause: Option<(usize, Arc<tokio::sync::Notify>)>,
) -> String {
    assert!(!responses.is_empty());
    let responses = Arc::new(responses);
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind the model server");
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let bodies = bodies.clone();
            let responses = responses.clone();
            let pause = pause.clone();
            tokio::spawn(async move {
                let mut reader = BufReader::new(stream);
                let mut content_length = 0usize;
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                        return;
                    }
                    if line == "\r\n" || line == "\n" {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        content_length = value.trim().parse().unwrap_or(0);
                    }
                }
                let mut body = vec![0u8; content_length];
                if reader.read_exact(&mut body).await.is_err() {
                    return;
                }
                let (index, response) = {
                    let mut bodies = bodies.lock().unwrap();
                    let index = bodies.len();
                    let response = responses[bodies.len().min(responses.len() - 1)];
                    bodies.push(body);
                    (index, response)
                };
                if let Some((_, gate)) = pause.filter(|(at, _)| *at == index) {
                    gate.notified().await;
                }
                let mut stream = reader.into_inner();
                let head = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    response.len()
                );
                let _ = stream.write_all(head.as_bytes()).await;
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            });
        }
    });
    url
}

struct Rig {
    daemon: Child,
    dir: PathBuf,
    connection_file: PathBuf,
    project: PathBuf,
}

impl Drop for Rig {
    fn drop(&mut self) {
        broca_test_support::kill_and_reap_tree(&mut self.daemon);
        broca_test_support::dump_rig_process_logs_on_failure(&self.dir, "daemon");
        broca_test_support::remove_rig_dir(&self.dir);
    }
}

impl Rig {
    async fn start(model_url: &str) -> Option<Self> {
        let catalog = super::routing_fixture::with_served_routing(serde_json::json!({
            "source": "renderer-version-resume", "resolved_at_ms": 1, "catalog_version": 1,
            "models": {format!("{PROVIDER}/{MODEL}"): {
                "existence": "present", "capability.attachment": false,
                "capability.input_modalities": ["text", "image"],
                "capability.reasoning": false, "capability.tool_call": true,
                "limit.context": 1000000, "limit.output": 8192
            }}
        }));
        let rig = Self::launch(model_url, Some(catalog)).await?;
        assert!(rig.wait_for_module_live().await, "broca never became live");
        Some(rig)
    }

    // A caller may register a live catalog producer before opening the first broca route.
    async fn launch(model_url: &str, catalog: Option<Value>) -> Option<Self> {
        let subc_source = broca_test_support::subc_bin("ck-subc", "SUBC_CORE_BIN");
        let fake_aft = broca_test_support::subc_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN");
        for bin in [&subc_source, &fake_aft] {
            if !bin.exists() {
                assert!(
                    !require_subc(),
                    "renderer_version_resume_real_daemon: {bin:?} is missing, but \
                     BROCA_REQUIRE_SUBC requires it"
                );
                println!(
                    "SKIP renderer_version_resume_real_daemon: {bin:?} is missing. This reports \
                     `ok`; set BROCA_REQUIRE_SUBC=1 to make it a failure instead."
                );
                return None;
            }
        }
        let subc_core =
            broca_test_support::ckdev_binary(&subc_source, broca_test_support::scratch_root());
        std::env::remove_var("SUBC_MODULE_ID");
        std::env::remove_var("SUBC_LAUNCH_NONCE");

        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-renderer-resume-e2e-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        broca_test_support::remove_rig_dir(&dir);
        let config_home = dir.join("config");
        let runtime_dir = dir.join("runtime");
        let config_path = config_home.join("cortexkit").join("subc.jsonc");
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::create_dir_all(&runtime_dir).unwrap();
        // A seeded catalog keeps renderer-version tests offline. Leaving it absent
        // requires bootstrap to fetch facts from the live catalog module.
        if let Some(catalog) = catalog {
            let catalog_path = runtime_dir.join("broca-state/catalog/fusiform-lkg.json");
            std::fs::create_dir_all(catalog_path.parent().unwrap()).unwrap();
            std::fs::write(&catalog_path, serde_json::to_vec_pretty(&catalog).unwrap()).unwrap();
        }
        let config = serde_json::json!({
            "version": 1,
            "modules": {
                "broca": {
                    "program": broca_test_support::ckdev_binary(
                        env!("CARGO_BIN_EXE_ck-broca"),
                        broca_test_support::scratch_root(),
                    ),
                    "args": [],
                    "env": {
                        "BROCA_STATE_ROOT": runtime_dir.join("broca-state"),
                        "BROCA_PROVIDER_BASE_URL": model_url,
                        "BROCA_TOOL_MODULE": "aft"
                    },
                    "enabled": true
                },
                "aft": {
                    "program": fake_aft,
                    "args": [],
                    "env": {"FAKE_AFT_MODULE_ID": "aft", "FAKE_AFT_TOOLS": "unused"},
                    "enabled": true
                }
            }
        });
        std::fs::write(&config_path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
        let connection_file = runtime_dir.join("subc-connection.json");
        let daemon = broca_test_support::retry_executable_file_busy(|| {
            broca_test_support::tokio_rig_command(&subc_core, &dir, "daemon")?
                .env("XDG_CONFIG_HOME", &config_home)
                .env("XDG_RUNTIME_DIR", &runtime_dir)
                .env("XDG_DATA_HOME", runtime_dir.join("data-home"))
                .env_remove("BROCA_STATE_ROOT")
                .env("SUBC_PORT", "0")
                .kill_on_drop(true)
                .spawn()
        })
        .expect("spawn ck-subc");
        let project = dir.join("project");
        std::fs::create_dir_all(&project).unwrap();
        // The WAL path is derived from the bound identity, and this test writes WALs itself, so
        // the project root is bound in its canonical form.
        let project = std::fs::canonicalize(&project).unwrap();
        let rig = Self {
            daemon,
            dir,
            connection_file,
            project,
        };
        broca_test_support::wait_until("the daemon's connection file", || async {
            if rig.connection_file.exists() {
                Ok(())
            } else {
                Err("not published yet".to_string())
            }
        })
        .await;
        Some(rig)
    }

    async fn wait_for_module_live(&self) -> bool {
        let probe = broca_protocol::BindIdentity::new(&self.dir, "probe", "renderer-resume");
        let deadline = Instant::now() + broca_test_support::EVENT_HANG_GUARD;
        while Instant::now() < deadline {
            if let Ok(conn) = broca_subc::SubcConnection::connect(&self.connection_file).await {
                if conn.route_open_management("broca", &probe).await.is_ok() {
                    return true;
                }
            }
            sleep(Duration::from_millis(50)).await;
        }
        false
    }

    fn wal_dir(&self) -> PathBuf {
        self.connection_file
            .parent()
            .unwrap()
            .join("broca-state")
            .join("wal")
    }

    fn key(&self, session: &str) -> broca_protocol::BindIdentity {
        broca_protocol::BindIdentity::new(&self.project, "runner", session)
    }

    async fn send(&self, session: &str, prompt: &str) -> Value {
        let conn = broca_subc::SubcConnection::connect(&self.connection_file)
            .await
            .unwrap();
        let route = conn
            .route_open_management("broca", &self.key(session))
            .await
            .unwrap();
        let params = serde_json::json!({
            "prompt": prompt,
            "send_id": format!("{session}-send"),
            "model": {"provider": PROVIDER, "model": MODEL},
            "on_restart": "resume"
        });
        conn.route_request(
            &route,
            serde_json::to_vec(&serde_json::json!({"method": "session.send", "params": params}))
                .unwrap(),
        )
        .await
        .unwrap()
    }
}

#[cfg(feature = "test-attachments")]
#[path = "media_acceptance.rs"]
mod media_acceptance;

fn session_records(wal_dir: &Path, session: &str) -> Option<Vec<WalRecord>> {
    for entry in std::fs::read_dir(wal_dir).ok()?.flatten() {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("wal") {
            continue;
        }
        let Ok(range) =
            broca_wal::read_records_range(&path, broca_wal::DEFAULT_MAX_READ_BYTES, 0, None, None)
        else {
            continue;
        };
        let broca_wal::WalRangeRecords::Complete(frames) = range.records() else {
            continue;
        };
        let records: Vec<_> = frames.iter().map(|frame| frame.record.clone()).collect();
        if records.iter().any(|record| {
            matches!(record, WalRecord::RunStarted { session: bound, .. } if bound.session == session)
        }) {
            return Some(records);
        }
    }
    None
}

async fn wait_records(
    rig: &Rig,
    session: &str,
    predicate: impl Fn(&[WalRecord]) -> bool,
) -> Vec<WalRecord> {
    broca_test_support::wait_until(&format!("the {session} WAL state"), || async {
        match session_records(&rig.wal_dir(), session) {
            Some(records) if predicate(&records) => Ok(records),
            other => Err(format!("{other:?}")),
        }
    })
    .await
}

/// The records of the episode `run_id` started, up to and including its `RunFinished` when it
/// has one.
fn episode<'a>(records: &'a [WalRecord], run_id: &str) -> &'a [WalRecord] {
    let start = records
        .iter()
        .position(
            |record| matches!(record, WalRecord::RunStarted { run_id: id, .. } if id.0 == run_id),
        )
        .unwrap_or(records.len());
    let rest = &records[start..];
    let end = rest
        .iter()
        .position(|record| matches!(record, WalRecord::RunFinished { .. }))
        .map_or(rest.len(), |index| index + 1);
    &rest[..end]
}

fn finished(records: &[WalRecord]) -> Option<&WalRecord> {
    records
        .iter()
        .find(|record| matches!(record, WalRecord::RunFinished { .. }))
}

/// Write, as the only record of `session`'s WAL, the `RunStarted` an earlier build recorded for
/// the admission `template` describes: run `run_id`, renderer version `renderer_version`, and a
/// frozen config without `provider_text_state`, which that build did not have.
fn seed_earlier_build_run(
    rig: &Rig,
    template: &WalRecord,
    session: &str,
    run_id: &str,
    renderer_version: u32,
) {
    let mut record = template.clone();
    let WalRecord::RunStarted {
        run_id: seeded_run,
        session: seeded_session,
        config,
        send_id,
        submission_id,
        request_identity_digest,
        ..
    } = &mut record
    else {
        panic!("the template is a RunStarted");
    };
    *seeded_run = broca_protocol::RunId(run_id.into());
    *seeded_session = rig.key(session);
    *send_id = None;
    *submission_id = None;
    *request_identity_digest = None;
    config.renderer_version = renderer_version;
    let blob = config.frozen_render_config.as_object_mut().unwrap();
    assert_eq!(blob.remove("provider_text_state"), Some(Value::Bool(true)));
    blob.insert("renderer_version".into(), renderer_version.into());
    let store = broca_core::FileWalStore::new(rig.wal_dir());
    let mut wal = store.open(&rig.key(session)).expect("open the seeded WAL");
    wal.append(1, &record, true)
        .expect("seed the earlier build's RunStarted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_admitted_at_a_reproducible_renderer_version_resumes_and_an_undeclared_one_is_refused(
) {
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let url = serve_model(bodies.clone()).await;
    let Some(rig) = Rig::start(&url).await else {
        return;
    };
    let current = broca_provider::families::GEMINI_RENDERER_VERSION;
    // Gemini declares `current - 1` reproducible and `current - 2` not; the declarations
    // themselves are checked in broca-provider. This test checks what the daemon does with them.

    // What the current build records for this admission.
    rig.send("template", "What time is it?").await;
    let template = wait_records(&rig, "template", |records| {
        finished(records).is_some_and(|record| {
            matches!(
                record,
                WalRecord::RunFinished {
                    reason: RunFinishReason::Completed,
                    ..
                }
            )
        })
    })
    .await;
    let template_start = template
        .iter()
        .find(|record| matches!(record, WalRecord::RunStarted { .. }))
        .unwrap()
        .clone();
    let WalRecord::RunStarted { config, .. } = &template_start else {
        unreachable!()
    };
    assert_eq!(config.renderer_version, current);
    assert_eq!(config.frozen_render_config["family_id"], "gemini");
    let template_body = bodies.lock().unwrap()[0].clone();

    // Recorded at the declared version: the resume runs and completes.
    seed_earlier_build_run(
        &rig,
        &template_start,
        "reproducible",
        "admitted-at-v3",
        current - 1,
    );
    let requests_before = bodies.lock().unwrap().len();
    rig.send("reproducible", "Continue after the deploy.").await;
    let records = wait_records(&rig, "reproducible", |records| {
        finished(episode(records, "admitted-at-v3")).is_some()
    })
    .await;
    let resumed = episode(&records, "admitted-at-v3");
    assert!(
        matches!(
            finished(resumed),
            Some(WalRecord::RunFinished {
                reason: RunFinishReason::Completed,
                error: None,
                ..
            })
        ),
        "the run recorded at the reproducible version must resume and complete: {resumed:?}"
    );
    assert!(resumed
        .iter()
        .any(|record| matches!(record, WalRecord::ResumeStarted { .. })));
    assert!(resumed
        .iter()
        .any(|record| matches!(record, WalRecord::StepStarted { .. })));
    // The resumed run's first request is the template's: same prompt, same frozen render
    // inputs apart from the version and the flag the earlier build lacked.
    assert!(bodies.lock().unwrap().len() > requests_before);
    assert_eq!(bodies.lock().unwrap()[requests_before], template_body);

    // Recorded at a version Gemini does not declare: refused, no model call, sealed.
    seed_earlier_build_run(
        &rig,
        &template_start,
        "undeclared",
        "admitted-at-v2",
        current - 2,
    );
    rig.send("undeclared", "Continue after the deploy.").await;
    let records = wait_records(&rig, "undeclared", |records| {
        finished(episode(records, "admitted-at-v2")).is_some()
    })
    .await;
    let refused = episode(&records, "admitted-at-v2");
    assert!(
        matches!(
            finished(refused),
            Some(WalRecord::RunFinished {
                reason: RunFinishReason::Interrupted,
                error: Some(error),
                ..
            }) if error.provider_code.as_deref() == Some("renderer_version_drift")
        ),
        "the run recorded at an undeclared version must be refused: {refused:?}"
    );
    assert!(!refused
        .iter()
        .any(|record| matches!(record, WalRecord::StepStarted { .. })));
}

#[cfg(feature = "test-attachments")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn person_pdf_reaches_debug_gate_and_is_refused_without_any_session_write() {
    use base64::Engine as _;
    use sha2::{Digest, Sha256};
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let url = serve_model(bodies.clone()).await;
    let Some(rig) = Rig::start(&url).await else {
        return;
    };
    let state = rig.wal_dir().parent().unwrap().to_owned();
    let key = rig.key("pdf-gate");
    let uploads = broca_core::media_uploads::Uploads::at(&state);
    let bytes = b"%PDF-1.7\n%%EOF\n";
    let now = broca_module::person_media::now_seconds();
    let id = uploads
        .begin(
            &key,
            &cortexkit_role_llm_runner::attachments::UploadBeginRequest::new(
                "application/pdf",
                bytes.len() as u64,
                format!("{:x}", Sha256::digest(bytes)),
            ),
            now,
        )
        .unwrap()
        .upload_id;
    uploads.append(&key, &id, 0, bytes, now).unwrap();
    uploads.commit(&key, &id, now).unwrap();
    let conn = broca_subc::SubcConnection::connect(&rig.connection_file)
        .await
        .unwrap();
    let route = conn.route_open_management("broca", &key).await.unwrap();
    let describe = conn
        .route_request(
            &route,
            serde_json::to_vec(&serde_json::json!({"method":"role.describe","params":{}})).unwrap(),
        )
        .await
        .unwrap();
    assert!(
        !describe.to_string().contains("\"attachments\""),
        "the debug ingress does not promise media.read: {describe}"
    );
    let reply = conn.route_request(&route, serde_json::to_vec(&serde_json::json!({
        "method":"session.send", "params": {"prompt":"inspect", "send_id":"pdf-gate", "model":{"provider":PROVIDER,"model":MODEL}, "content":[{"type":"upload","upload_id":id}]}
    })).unwrap()).await.expect_err("the person PDF must be refused before admission");
    let text = reply.to_string();
    assert!(
        text.contains("media_unsupported")
            && text.contains("pdf")
            && text.contains("modality")
            && text.contains(MODEL),
        "{reply}"
    );
    let wal = broca_core::FileWalStore::new(rig.wal_dir());
    assert!(
        !wal.wal_path(&key).exists(),
        "a refused fresh send must not even open its WAL"
    );
    for class in ["media", "media-durable", "media-ephemeral"] {
        for dir in ["objects", "refs"] {
            assert_eq!(
                std::fs::read_dir(state.join(class).join(dir))
                    .unwrap()
                    .count(),
                0
            );
        }
    }
    assert!(
        bodies.lock().unwrap().is_empty(),
        "no provider request on refusal"
    );
    let (mime, held) = uploads.read_committed(&key, &id, now).unwrap();
    assert_eq!(mime, "application/pdf");
    assert_eq!(held, bytes);
    // Keep the upload's bytes out of the route request: only the contract reference is sent.
    assert!(!text.contains(&base64::engine::general_purpose::STANDARD.encode(bytes)));
}
