//! Drive the real ck-broca and its daemon with a scoped extractor route and loopback Clairic.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine};
use broca_subc::{RouteHandle, RouteOpenRetry, SubcConnection};
use cortexkit_role_extractor::{
    describe::ImageLimits,
    provider::{catalog_ids::CLAIRIC, clairic_limits::PASS_TOKENS, models},
};
use cortexkit_role_extractor_conformance::{
    run_suite, CheckOutcome, ExtractorRoute, ExtractorSubject, Reply, RouteFailure, Scripted,
    ServedModel, SuiteVerdict, CHECKS,
};
use serde_json::{json, value::RawValue, Value};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

#[path = "extractor_conformance/vault.rs"]
mod vault;

const OWNER: &str = "extractor-owner";
const SCOPE: &str = "extractor-run";
const NON_EXTRACTION: &str = "fixture/decision";
const KINDLESS: &str = "fixture/kindless";

#[derive(Default)]
struct StandIn {
    scripts: Mutex<HashMap<String, Scripted>>,
    received: Mutex<HashMap<String, Vec<String>>>,
}

impl StandIn {
    async fn start(self: &Arc<Self>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let provider = self.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let provider = provider.clone();
                tokio::spawn(async move { provider.answer(stream).await.unwrap() });
            }
        });
        format!("http://{address}/client/v4")
    }

    async fn answer(&self, stream: TcpStream) -> std::io::Result<()> {
        let mut stream = BufReader::new(stream);
        let mut line = String::new();
        if stream.read_line(&mut line).await? == 0 {
            return Ok(());
        }
        let path = format!(
            "/client/v4/accounts/{}/ai/run/@cf/cloudflare/clairic",
            vault::ACCOUNT_ID
        );
        assert_eq!(line.split_whitespace().nth(1), Some(path.as_str()));
        let mut length = None;
        let mut authorized = false;
        loop {
            line.clear();
            if stream.read_line(&mut line).await? == 0 {
                return Ok(());
            }
            if line == "\r\n" {
                break;
            }
            if let Some((key, value)) = line.split_once(':') {
                if key.eq_ignore_ascii_case("content-length") {
                    length = value.trim().parse().ok();
                } else if key.eq_ignore_ascii_case("authorization") {
                    authorized = value.trim().strip_prefix("Bearer ") == Some(vault::TOKEN);
                }
            }
        }
        assert!(
            authorized,
            "Workers AI request must carry the vault's bearer token"
        );
        let mut body = vec![0; length.expect("Workers AI request has a content length")];
        stream.read_exact(&mut body).await?;
        let body = String::from_utf8(body).unwrap();
        let request: Value = serde_json::from_str(&body).unwrap();
        let state = request["state"].as_str().unwrap();
        self.received
            .lock()
            .unwrap()
            .entry(state.into())
            .or_default()
            .push(body);
        let scripted = self.scripts.lock().unwrap().get(state).cloned();
        let Scripted { status, body } = scripted.unwrap_or(Scripted {
            status: 500,
            body: String::new(),
        });
        let head = format!("HTTP/1.1 {status} Scripted\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", body.len());
        let mut stream = stream.into_inner();
        stream.write_all(head.as_bytes()).await?;
        stream.write_all(body.as_bytes()).await?;
        stream.shutdown().await
    }
}

struct Subject {
    _vault: vault::Service,
    _daemon: broca_test_support::RigChild,
    provider: Arc<StandIn>,
    connection: PathBuf,
    project: PathBuf,
    nonce: PathBuf,
    image_fixtures: Option<PathBuf>,
}

fn image_surface(path: PathBuf, windows: bool) -> Option<PathBuf> {
    if path.is_file() {
        return Some(path);
    }
    // The stand-in uses Unix APIs and the Windows fixture build omits it.
    assert!(windows, "extractor image rig needs {}", path.display());
    None
}

impl Subject {
    async fn start() -> Option<Self> {
        let daemon = broca_test_support::subc_bin("ck-subc", "SUBC_CORE_BIN");
        if !daemon.exists() {
            assert!(
                std::env::var_os("BROCA_REQUIRE_SUBC").is_none(),
                "BROCA_REQUIRE_SUBC is set but {} is missing",
                daemon.display()
            );
            println!("SKIP extractor conformance: {} missing", daemon.display());
            return None;
        }
        let stub = broca_test_support::subc_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN");
        assert!(
            stub.exists(),
            "extractor scoped rig needs {}",
            stub.display()
        );
        let surface = image_surface(
            broca_test_support::subc_bin("fake-surface", "FAKE_SURFACE_BIN"),
            cfg!(windows),
        );
        let scratch = broca_test_support::scratch_root();
        let root = scratch.join(format!("extractor-conformance-{}", std::process::id()));
        broca_test_support::remove_rig_dir(&root);
        let config = root.join("config");
        let data = root.join("data-home");
        let runtime = root.join("runtime");
        let home = root.join("home");
        let state = root.join("broca-state");
        let project = root.join("project");
        for dir in [
            &config.join("cortexkit"),
            &data,
            &runtime,
            &home,
            &state.join("catalog"),
            &project,
        ] {
            std::fs::create_dir_all(dir).unwrap();
        }

        let provider = Arc::new(StandIn::default());
        let base = provider.start().await;
        write_json(
            &config.join("cortexkit/broca.jsonc"),
            &json!({
                "session_storage": {"deployment_key_id": 1},
                "llm_tools": {"image_references": surface.is_some()},
                "providers": {"cloudflare-workers-ai": {"base_url": base}},
            }),
        );
        write_json(
            &state.join("catalog/fusiform-lkg.json"),
            &json!({
                "catalog_version": 1, "resolved_at_ms": 1, "source": "models.dev",
                "models": {
                    CLAIRIC: {
                        "existence": "present", "model.kind": "extraction", "capability.attachment": true,
                        "rate.input": {"currency":"USD", "exponent":9, "state":"priced", "units":80_000_000, "unit_provenance":{"kind":"stated"}},
                        "rate.output": {"state":"stated_zero"},
                    },
                    NON_EXTRACTION: {"existence": "present", "model.kind":"decision"},
                    KINDLESS: {"existence": "present"},
                },
            }),
        );

        let connection = runtime.join("subc-connection.json");
        let nonce = root.join("owner.nonce");
        let owner_events = root.join("owner-events.jsonl");
        let scopes = root.join("scopes.json");
        write_json(
            &scopes,
            &json!([{
                "ref": SCOPE, "scope_epoch": 1, "kind":"head", "attributes": {"agent_id":"extractor-agent", "run_id":SCOPE},
            }]),
        );
        let image_requests = root.join("images.requests");
        let mut subc_config = json!({
            "version": 1, "scope_authority_owners": [OWNER],
            "modules": {
                OWNER: {
                    "program": broca_test_support::ckdev_binary(&stub, &scratch),
                    "args": [], "reserved": true, "enabled": true,
                    "env": {"FAKE_AFT_MODULE_ID": OWNER, "FAKE_AFT_SCOPE_SYNC_PATH": scopes,
                           "FAKE_AFT_EVENTS_PATH": owner_events, "FAKE_AFT_LAUNCH_NONCE_PATH": nonce},
                },
                "broca": {
                    "program": broca_test_support::ckdev_binary(Path::new(env!("CARGO_BIN_EXE_ck-broca")), &scratch),
                    "args": [], "enabled": true, "env": {"BROCA_STATE_ROOT": state, "XDG_DATA_HOME": data, "CK_LOG":"info"},
                },
            },
        });
        let image_fixtures = surface.map(|surface| {
            let fixtures = root.join("images.fixtures.json");
            write_json(&fixtures, &json!([]));
            let encoded_path = |path: &Path| {
                path.to_string_lossy()
                    .bytes()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            };
            subc_config["modules"]["cerebellum"] = json!({
                "program": broca_test_support::ckdev_binary(&surface, &scratch),
                "args": ["--module-id", "cerebellum", format!("--SURFACE_FIXTURES={}", encoded_path(&fixtures)), format!("--SURFACE_REQUESTS={}", encoded_path(&image_requests))],
                "reserved": true, "enabled": true, "env": {"SUBC_CONNECTION": connection},
            });
            fixtures
        });
        write_json(&config.join("cortexkit/subc.jsonc"), &subc_config);

        let child = broca_test_support::retry_executable_file_busy(|| {
            broca_test_support::tokio_rig_command(
                broca_test_support::ckdev_binary(&daemon, &scratch),
                &root,
                "daemon",
            )?
            .env("XDG_CONFIG_HOME", &config)
            .env("XDG_RUNTIME_DIR", &runtime)
            .env("XDG_DATA_HOME", &data)
            .env("HOME", &home)
            .env("SUBC_PORT", "0")
            .env_remove("BROCA_STATE_ROOT")
            .env_remove("BROCA_CONFIG_PATH")
            .env_remove("BROCA_PROVIDER_BASE_URL")
            .env_remove("SUBC_LAUNCH_NONCE")
            .env_remove("SUBC_LAUNCH_NONCE_FD")
            .env_remove("SUBC_MODULE_ID")
            .kill_on_drop(true)
            .spawn()
        })
        .unwrap();
        let daemon = broca_test_support::RigChild::new(child, &root, "daemon");
        broca_test_support::wait_until("extractor rig ready", || async {
            if connection.exists()
                && std::fs::read_to_string(&owner_events).is_ok_and(|text| {
                    text.lines().any(|line| {
                        serde_json::from_str::<Value>(line)
                            .is_ok_and(|event| event["kind"] == "scope_sync_response")
                    })
                })
                && (image_fixtures.is_none()
                    || std::fs::read_to_string(&image_requests)
                        .is_ok_and(|text| text.contains("hello-ack")))
                && std::fs::read_to_string(&nonce).is_ok_and(|text| !text.trim().is_empty())
            {
                Ok(())
            } else {
                Err("daemon or a configured rig module still starting".to_owned())
            }
        })
        .await;
        Some(Self {
            _vault: vault::register(&connection).await,
            _daemon: daemon,
            provider,
            connection,
            project: project.canonicalize().unwrap(),
            nonce,
            image_fixtures,
        })
    }
}

fn write_json(path: &Path, value: &Value) {
    std::fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
}

struct Route {
    connection: SubcConnection,
    handle: RouteHandle,
}

#[async_trait]
impl ExtractorRoute for Route {
    async fn request(&self, method: &str, params: &str) -> Result<Reply, RouteFailure> {
        let body = format!("{{\"method\":{},\"params\":{params}}}", json!(method));
        let terminal = self
            .connection
            .route_request_terminal_with_deadline(
                &self.handle,
                body.into_bytes(),
                Duration::from_secs(120),
            )
            .await
            .map_err(|e| RouteFailure::new(e.to_string()))?;
        match terminal {
            Ok(response) => {
                #[derive(serde::Deserialize)]
                struct Envelope<'a> {
                    #[serde(borrow)]
                    result: &'a RawValue,
                }
                let reply: Envelope<'_> = serde_json::from_slice(&response)
                    .map_err(|e| RouteFailure::new(format!("invalid extractor response: {e}")))?;
                Ok(Reply::Response(reply.result.get().into()))
            }
            Err(error) => serde_json::from_slice(&error)
                .map(Reply::Error)
                .map_err(|e| RouteFailure::new(format!("invalid extractor error: {e}"))),
        }
    }
}

#[async_trait]
impl ExtractorSubject for Subject {
    type Route = Route;

    async fn route(&self) -> Result<Route, String> {
        let connection = SubcConnection::connect_with_consumer_identity(
            &self.connection,
            Some(subc_control::ConsumerIdentity {
                module_id: OWNER.into(),
                launch_nonce: std::fs::read_to_string(&self.nonce)
                    .map_err(|e| e.to_string())?
                    .trim()
                    .into(),
            }),
        )
        .await
        .map_err(|e| e.to_string())?;
        let identity = broca_protocol::BindIdentity::new(self.project.clone(), "core", "extractor");
        let selector = subc_protocol::scope::ScopeSelector::new(
            subc_protocol::Principal::Reserved {
                module_id: OWNER.into(),
            },
            SCOPE,
            Some(1),
        );
        let handle = connection
            .route_open_extractor_scoped_within(
                "broca",
                &identity,
                selector,
                RouteOpenRetry::within(Duration::from_secs(60), Duration::from_millis(200)),
            )
            .await
            .map_err(|e| format!("route.open: {e}"))?;
        Ok(Route { connection, handle })
    }

    fn served_models(&self) -> Vec<ServedModel> {
        vec![ServedModel {
            model: CLAIRIC.into(),
            provider_model: models::CLAIRIC.into(),
            priced: true,
            images: Some(ImageLimits::workers_ai(2)),
            pass_tokens: PASS_TOKENS,
            context_tokens: None,
            egress_host: "127.0.0.1".into(),
        }]
    }

    fn non_extraction_model(&self) -> Option<String> {
        Some(NON_EXTRACTION.into())
    }

    fn kindless_model(&self) -> Option<String> {
        Some(KINDLESS.into())
    }

    async fn received(&self, state: &str) -> Vec<String> {
        self.provider
            .received
            .lock()
            .unwrap()
            .get(state)
            .cloned()
            .unwrap_or_default()
    }

    async fn script(&self, state: &str, scripted: Scripted) {
        self.provider
            .scripts
            .lock()
            .unwrap()
            .insert(state.into(), scripted);
    }

    async fn issue_image_reference(&self, content_type: &str, bytes: Vec<u8>) -> Option<String> {
        let path = self.image_fixtures.as_ref()?;
        let mut fixtures: Vec<Value> =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let handle = format!("ih{:032x}", fixtures.len());
        // The image resolver's binary reply starts with a length-prefixed JSON header; the test
        // surface returns the fixture's base64-decoded bytes without interpreting them.
        let head = serde_json::to_vec(&json!({
            "content_type": content_type, "length": bytes.len(), "sha256": format!("{:x}", Sha256::digest(&bytes)),
        })).unwrap();
        let mut body = (head.len() as u32).to_be_bytes().to_vec();
        body.extend(head);
        body.extend(bytes);
        fixtures.push(json!({"match": {"method":"image.resolve", "scope_ref":SCOPE, "handle":handle}, "stdout":STANDARD.encode(body)}));
        let temporary = path.with_extension("new");
        std::fs::write(&temporary, serde_json::to_vec(&fixtures).unwrap()).unwrap();
        std::fs::rename(temporary, path).unwrap();
        Some(handle)
    }

    async fn set_image_source_down(&self, down: bool) {
        let flag = self
            .image_fixtures
            .as_ref()
            .expect("image source configured")
            .with_extension("down");
        if down {
            std::fs::write(flag, "").unwrap()
        } else {
            std::fs::remove_file(flag).unwrap()
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extractor_conformance_suite_passes_against_the_real_daemon() {
    let Some(subject) = Subject::start().await else {
        return;
    };
    let report = run_suite(&subject).await;
    print!("{}", report.render());
    let passed = report
        .checks
        .iter()
        .filter(|check| check.outcome == CheckOutcome::Passed)
        .count();
    println!(
        "extractor conformance: {passed}/{} checks passed",
        CHECKS.len()
    );
    if subject.image_fixtures.is_some() {
        assert_eq!(report.verdict, SuiteVerdict::Passed, "{}", report.render());
        assert_eq!(passed, CHECKS.len());
    } else {
        // Inline images and the refusal of an unsupported handle still apply.
        let not_applicable = vec![
            "image_reference_resolved_and_answered",
            "image_reference_oversized_refused_like_inline",
            "image_source_down_refused_transient",
            "image_reference_identity_is_its_handle",
        ];
        assert_eq!(passed, CHECKS.len() - not_applicable.len());
        for check in &not_applicable {
            assert_eq!(
                report.outcome(check),
                Some(&CheckOutcome::NotApplicable {
                    reason: "role.describe does not declare image_reference".into(),
                }),
                "{check}"
            );
        }
        assert_eq!(
            report.verdict,
            SuiteVerdict::PassedExceptNotApplicable { not_applicable },
            "{}",
            report.render()
        );
    }
}

#[test]
fn extractor_image_surface_is_optional_on_windows() {
    let missing = broca_test_support::scratch_root().join("missing-fake-surface.exe");
    assert_eq!(image_surface(missing, true), None);
    let present = std::env::current_exe().unwrap();
    assert_eq!(image_surface(present.clone(), true), Some(present));
}

#[test]
#[should_panic(expected = "extractor image rig needs")]
fn extractor_image_surface_is_required_off_windows() {
    let missing = broca_test_support::scratch_root().join("missing-fake-surface");
    image_surface(missing, false);
}
