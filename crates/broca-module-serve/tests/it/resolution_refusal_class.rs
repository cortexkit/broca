//! Refusals of a send whose run can't be resolved, through the real serve path: each request
//! goes over the wire into `serve_connection`, into the registry, to the session's actor and on to
//! an executor that refuses the run before it starts, exactly as the module's executor refuses one
//! whose model or variant it can't resolve. The error body that comes back is pinned byte for
//! byte.
//!
//! Every refusal is sent four ways: to a session kept in today's storage and to one kept in the
//! new storage, each with and without a fetch plan. The new-storage, plan-carrying send is the one
//! Core makes (session class `consult`, a fetch plan and a scope stamp). One cause must reach the
//! caller as one body whichever way it was sent: the same code, detail and class.
//!
//! Set `BROCA_RESOLUTION_REFUSAL_SAMPLES` to a folder to also write each body there, named
//! `<case>.<storage>.<plan>.json`, before the bodies are compared.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use broca_core::{FileLeaseStore, FileWalStore, OutputEvent, RunError, RunSummary};
use broca_module::executor::{RunExecutor, RunJob, StoreRunEnd, StoreRunError, StoreRunJob};
use broca_module::{DefaultOpener, LineageRegistry};
use broca_module_serve::{serve_connection, ServeEnd};
use serde_json::{json, Value};
use subc_protocol::session::ModuleControlRequest;
use subc_protocol::{
    BindIdentity as SubcBindIdentity, Flags, Frame, FrameType, ModuleHelloAckBody, Principal,
    Priority, RouteTarget, PROTOCOL_VERSION,
};
use subc_transport::{read_frame, write_frame};
use tokio::sync::mpsc;

/// The session class Core's consults are sent with, which the module keeps in the new storage.
const CONSULT: &str = "consult";

/// Refuses every run before it starts with the error it holds, on both storages, as the module's
/// executor refuses a run whose model or variant it can't resolve: on today's storage through the
/// admission acknowledgement, on the new storage as the run's admission error.
struct Refusing {
    error: Mutex<Option<RunError>>,
}

impl Refusing {
    fn error(&self) -> RunError {
        self.error
            .lock()
            .unwrap()
            .clone()
            .expect("each send sets the refusal first")
    }
}

#[async_trait]
impl RunExecutor for Refusing {
    async fn execute(
        &self,
        job: RunJob,
        _out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        let error = self.error();
        if let Some(ack) = job.on_admitted {
            let _ = ack.send(Err(error.clone()));
        }
        Err(error)
    }

    async fn execute_on_store(
        &self,
        job: StoreRunJob,
        _out: mpsc::Sender<OutputEvent>,
    ) -> StoreRunEnd {
        StoreRunEnd {
            parts: job.parts,
            result: Err(StoreRunError::Admission(self.error())),
        }
    }
}

struct Rig {
    dir: PathBuf,
    executor: Arc<Refusing>,
    reader: tokio::io::ReadHalf<tokio::io::DuplexStream>,
    writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    _serve: tokio::task::JoinHandle<Result<ServeEnd, broca_module_serve::serve::ServeIoError>>,
    corr: u64,
    channel: u16,
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Rig {
    /// A module on a fresh state root. With `new_storage`, a `consult` session is created in the
    /// new storage, as the module's daemon configures it; without, every session is kept in
    /// today's storage.
    async fn start(tag: &str, new_storage: bool) -> Self {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-resolution-refusal-{}-{tag}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        broca_core::media_store::create_media_directories(&dir).unwrap();
        let wal = Arc::new(FileWalStore::new(dir.join("wal")));
        let executor = Arc::new(Refusing {
            error: Mutex::new(None),
        });
        let opener = DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal.clone(),
            executor.clone(),
        )
        .with_idle_ttl(Duration::from_millis(100));
        let opener = if new_storage {
            let runtime = broca_module::store_open::StoreRuntime::open(
                &dir,
                broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
                executor.clone() as Arc<dyn RunExecutor>,
                Duration::from_millis(100),
            )
            .await
            .unwrap();
            opener.with_store_runtime(Arc::new(runtime))
        } else {
            opener
        };
        let registry = LineageRegistry::new_with_reader(Arc::new(opener), wal, None);
        let registry = if new_storage {
            registry.with_store_new_session_classes([CONSULT])
        } else {
            registry
        };
        let store = Arc::new(broca_store::SqliteStore::open(dir.join("store.db")).unwrap());
        broca_store::session_refs::migrate(
            &dir.join("store.db"),
            &broca_core::ReadOnlyWalReader::new(&dir),
        )
        .unwrap();
        registry.set_session_address_inventory(store);
        let registry = Arc::new(registry);
        registry.set_plan_admitter(Arc::new(super::plan_admission::CountingAdmitter::default()));
        let (client, server) = tokio::io::duplex(256 * 1024);
        let (mut reader, mut writer) = tokio::io::split(client);
        let (server_read, server_write) = tokio::io::split(server);
        let served = registry.clone();
        let serve = tokio::spawn(async move {
            serve_connection(
                server_read,
                server_write,
                "broca",
                served.clone(),
                served,
                broca_module_serve::serve::VaultGauge::not_in_use(),
            )
            .await
        });
        let hello = read_frame(&mut reader).await.unwrap().unwrap();
        let ack = Frame::build(
            FrameType::HelloAck,
            Flags::new(false, Priority::Passive, false),
            0,
            0,
            hello.header.corr,
            serde_json::to_vec(&ModuleHelloAckBody {
                negotiated_ver: PROTOCOL_VERSION,
                subc_ops: vec![],
                // The daemon attests scopes, as Core's routes carry them.
                subc_capabilities: vec![subc_protocol::scope::CAP_SCOPES_V1.into()],
                storage: None,
                machine_id: None,
            })
            .unwrap(),
        )
        .unwrap();
        write_frame(&mut writer, &ack).await.unwrap();
        Self {
            dir,
            executor,
            reader,
            writer,
            _serve: serve,
            corr: 1,
            channel: 0,
        }
    }

    async fn request(&mut self, channel: u16, body: Value) -> Frame {
        self.corr += 1;
        let frame = Frame::build(
            FrameType::Request,
            Flags::new(false, Priority::Interactive, false),
            channel,
            u32::from(channel != 0),
            self.corr,
            serde_json::to_vec(&body).unwrap(),
        )
        .unwrap();
        write_frame(&mut self.writer, &frame).await.unwrap();
        tokio::time::timeout(Duration::from_secs(20), read_frame(&mut self.reader))
            .await
            .expect("a reply")
            .unwrap()
            .unwrap()
    }

    /// Binds a new route to a new session `session`, stamped with Core's principal and a head
    /// scope it owns, and returns the route's channel.
    async fn bind(&mut self, session: &str) -> u16 {
        self.channel += 1;
        let channel = self.channel;
        let bind = ModuleControlRequest::RouteBind {
            route_channel: channel,
            epoch: 1,
            target: RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new("runner", session),
            principal: Some(core()),
            consumer_capabilities: None,
            admission_facts: None,
            scope: Some(scope_stamp()),
            role_versions: None,
            call_mode: None,
        };
        let reply = self.request(0, serde_json::to_value(bind).unwrap()).await;
        assert_eq!(
            reply.header.ty,
            FrameType::Response,
            "{}",
            String::from_utf8_lossy(&reply.body)
        );
        channel
    }

    /// Sends a first message to a new session, refused with `error`, and returns the error body.
    async fn refused_body(&mut self, session: &str, error: RunError, plan: bool) -> Vec<u8> {
        *self.executor.error.lock().unwrap() = Some(error);
        let channel = self.bind(session).await;
        let mut params = json!({
            "prompt": "hi",
            "send_id": "turn-1",
            "session_class": CONSULT,
            "model": {"provider": "google", "model": "antigravity-gemini-4-carbon", "variant": "high"},
        });
        if plan {
            // The plan's system-text items select the send's own model, as Core's plans do.
            let mut plan = super::plan_admission::vector("plans/broca-head-no-compaction.json");
            for item in plan["system_text_items"]
                .as_array_mut()
                .into_iter()
                .flatten()
            {
                if item["params"].get("model").is_some() {
                    item["params"]["model"] =
                        json!({"provider": "google", "id": "antigravity-gemini-4-carbon"});
                }
            }
            params["plan"] = plan;
        }
        let reply = self
            .request(channel, json!({"method": "session.send", "params": params}))
            .await;
        assert_eq!(
            reply.header.ty,
            FrameType::Error,
            "{session}: {}",
            String::from_utf8_lossy(&reply.body)
        );
        reply.body.to_vec()
    }
}

fn core() -> Principal {
    Principal::Reserved {
        module_id: "prefrontal-core".into(),
    }
}

fn scope_stamp() -> subc_protocol::scope::ScopeStamp {
    use subc_protocol::scope::{ScopeAttributes, ScopeKind, ScopeStamp};
    ScopeStamp::new(core(), "head-1", 1, ScopeKind::Head)
        .with_attributes(ScopeAttributes::new())
        .with_owner_authorized(true)
}

/// A resolution refusal and the exact body every way of sending it must be answered with.
struct Case {
    name: &'static str,
    error: RunError,
    body: &'static str,
}

/// Each refusal the module's resolution can return before a run starts, with the message its
/// source writes. Bodies differ only by code, message and detail, so every class shape appears.
fn cases() -> Vec<Case> {
    let variant = |model: &str, variant: &str, defined: &[&str], reason: &str| {
        RunError::VariantUnsupported(broca_core::run::VariantRefusal {
            model: model.into(),
            variant: variant.into(),
            defined: defined.iter().map(|name| (*name).to_owned()).collect(),
            reason: reason.into(),
        })
    };
    let config = |message: &str| RunError::Config(message.into());
    vec![
        // broca-catalog live.rs, apply_code_assist_variant: no catalog row for the model.
        Case {
            name: "variant_no_catalog_row",
            error: variant(
                "google/antigravity-gemini-4-carbon",
                "high",
                &[],
                "model 'google/antigravity-gemini-4-carbon' cannot use variant 'high': the \
                 catalog has no row for this model, so its variants cannot be known",
            ),
            body: r#"{"code":"variant_unsupported","message":"open failed: run resolution failed: model 'google/antigravity-gemini-4-carbon' cannot use variant 'high': the catalog has no row for this model, so its variants cannot be known","detail":{"class":"permanent","defined":[],"model":"google/antigravity-gemini-4-carbon","variant":"high"}}"#,
        },
        // broca-provider variant.rs, resolve_variant: the model's row does not define it.
        Case {
            name: "variant_not_defined",
            error: variant(
                "anthropic/claude-opus-5",
                "max",
                &["none", "low", "medium", "high"],
                "model 'anthropic/claude-opus-5' does not define variant 'max'; its variants \
                 are [none, low, medium, high]",
            ),
            body: r#"{"code":"variant_unsupported","message":"open failed: run resolution failed: model 'anthropic/claude-opus-5' does not define variant 'max'; its variants are [none, low, medium, high]","detail":{"class":"permanent","defined":["none","low","medium","high"],"model":"anthropic/claude-opus-5","variant":"max"}}"#,
        },
        // broca-catalog live.rs, apply_code_assist_variant: the route has no tier for it.
        Case {
            name: "variant_no_route_tier",
            error: variant(
                "google/antigravity-gemini-3.8-flash",
                "minimal",
                &["minimal", "low", "high"],
                "model 'google/antigravity-gemini-3.8-flash' cannot use variant 'minimal': \
                 route.reasoning_wire_ids has no tier 'minimal'; available tiers are [low, high]",
            ),
            body: r#"{"code":"variant_unsupported","message":"open failed: run resolution failed: model 'google/antigravity-gemini-3.8-flash' cannot use variant 'minimal': route.reasoning_wire_ids has no tier 'minimal'; available tiers are [low, high]","detail":{"class":"permanent","defined":["minimal","low","high"],"model":"google/antigravity-gemini-3.8-flash","variant":"minimal"}}"#,
        },
        // broca-catalog live.rs, resolve_provider: no served row for the model.
        Case {
            name: "model_no_catalog_row",
            error: config(
                "catalog has no served model 'google/antigravity-gemini-4-carbon' (unknown \
                 provider/model, or the provider is pending — not yet catalog-served; pass a \
                 base_url to reach it as an openai-compatible endpoint)",
            ),
            body: r#"{"code":"open_failed","message":"open failed: run resolution failed: catalog has no served model 'google/antigravity-gemini-4-carbon' (unknown provider/model, or the provider is pending — not yet catalog-served; pass a base_url to reach it as an openai-compatible endpoint)","detail":{"class":"permanent"}}"#,
        },
        // broca-catalog live.rs, resolve_provider: the model was withdrawn upstream.
        Case {
            name: "model_withdrawn",
            error: config(
                "catalog model 'openai/gpt-4' was withdrawn upstream; choose a different model \
                 for this episode",
            ),
            body: r#"{"code":"open_failed","message":"open failed: run resolution failed: catalog model 'openai/gpt-4' was withdrawn upstream; choose a different model for this episode","detail":{"class":"permanent"}}"#,
        },
        // broca-catalog live.rs, refuse_non_chat_model: the row is not a chat model.
        Case {
            name: "model_not_chat",
            error: config(
                "catalog model 'cloudflare/@cf/cloudflare/clef' is a decision model (model.kind \
                 \"decision\"), not a chat model; session.send serves only chat models",
            ),
            body: r#"{"code":"open_failed","message":"open failed: run resolution failed: catalog model 'cloudflare/@cf/cloudflare/clef' is a decision model (model.kind \"decision\"), not a chat model; session.send serves only chat models","detail":{"class":"permanent"}}"#,
        },
        // broca-catalog runtime.rs, resolve_route: the row names no endpoint.
        Case {
            name: "route_no_endpoint",
            error: config("catalog model 'acme/model-1': missing wire.api"),
            body: r#"{"code":"open_failed","message":"open failed: run resolution failed: catalog model 'acme/model-1': missing wire.api","detail":{"class":"permanent"}}"#,
        },
        // broca-catalog runtime.rs, resolve_route: the row's wire family is unknown.
        Case {
            name: "route_unknown_wire_family",
            error: config(
                "catalog model 'acme/model-1': unsupported wire.npm '@acme/sdk' / wire.shape None",
            ),
            body: r#"{"code":"open_failed","message":"open failed: run resolution failed: catalog model 'acme/model-1': unsupported wire.npm '@acme/sdk' / wire.shape None","detail":{"class":"permanent"}}"#,
        },
        // broca-catalog live.rs, resolve_provider: the model id names no provider.
        Case {
            name: "model_not_a_catalog_id",
            error: config(
                "model 'gemini-4-carbon' is not a catalog id: expected '<provider>/<model>' (e.g. \
                 anthropic/claude-opus-4-0), or pass a base_url to use an openai-compatible \
                 endpoint directly",
            ),
            body: r#"{"code":"open_failed","message":"open failed: run resolution failed: model 'gemini-4-carbon' is not a catalog id: expected '<provider>/<model>' (e.g. anthropic/claude-opus-4-0), or pass a base_url to use an openai-compatible endpoint directly","detail":{"class":"permanent"}}"#,
        },
        // broca-module-serve main.rs, media_profile: the catalog has not loaded yet.
        Case {
            name: "catalog_not_loaded",
            error: RunError::Transient(
                "model catalog unavailable: no in-memory fusiform catalog and no \
                 last-known-good catalog is configured"
                    .into(),
            ),
            body: r#"{"code":"transient","message":"transient admission failure: model catalog unavailable: no in-memory fusiform catalog and no last-known-good catalog is configured","detail":{"class":"transient"}}"#,
        },
        // broca-module-serve main.rs, auth decision: a credential lookup that may succeed later.
        Case {
            name: "credential_lookup_unavailable",
            error: RunError::Transient("vault credential lookup timed out".into()),
            body: r#"{"code":"transient","message":"transient admission failure: vault credential lookup timed out","detail":{"class":"transient"}}"#,
        },
        // broca-module-serve main.rs, account selection: the vault listing has not loaded.
        Case {
            name: "credentials_unavailable",
            error: RunError::CredentialsUnavailable,
            body: r#"{"code":"transient","message":"the vault credential listing has not loaded yet","detail":{"cause":"credentials_unavailable","class":"transient"}}"#,
        },
    ]
}

/// Every resolution refusal is answered with the same bytes on both storages, with and without a
/// fetch plan, and each body states its class.
#[tokio::test]
async fn each_resolution_refusal_has_one_body_on_every_path() {
    let samples = std::env::var_os("BROCA_RESOLUTION_REFUSAL_SAMPLES").map(PathBuf::from);
    if let Some(samples) = &samples {
        std::fs::create_dir_all(samples).unwrap();
    }
    let mut mismatches = Vec::new();
    for (storage, new_storage) in [("todays", false), ("new", true)] {
        let mut rig = Rig::start(storage, new_storage).await;
        for case in cases() {
            for (plan_name, plan) in [("plain", false), ("plan", true)] {
                let session = format!("{}-{plan_name}", case.name);
                let body = rig.refused_body(&session, case.error.clone(), plan).await;
                if let Some(samples) = &samples {
                    std::fs::write(
                        samples.join(format!("{}.{storage}.{plan_name}.json", case.name)),
                        &body,
                    )
                    .unwrap();
                }
                if body != case.body.as_bytes() {
                    mismatches.push(format!(
                        "{} on {storage} storage, {plan_name}:\n  got  {}\n  want {}",
                        case.name,
                        String::from_utf8_lossy(&body),
                        case.body
                    ));
                }
            }
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}
