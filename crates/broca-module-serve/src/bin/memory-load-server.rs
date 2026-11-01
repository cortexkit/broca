//! Real serve adapter, actor, WAL, store and HTTP provider; the driver is a separate process.
//! No subc daemon, credential service or outbound tool routes are needed for this load.
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use broca_catalog::live::{AuthMode, ResolveOptions, WireFamilyId};
use broca_core::{
    AllowAll, FileWalStore, OutputEvent, RunError, RunSummary, StartMode, StubToolPlane,
};
use broca_module::executor::{RunExecutor, RunJob, RunKind};
use broca_module::serve::{ServeHandler, SessionImportRequest};
use broca_module::{DefaultOpener, LineageRegistry};
use broca_protocol::{
    BindIdentity, ContentBlock, Message, Role, RunId, ToolCall, ToolCallId, ToolOutput, ToolResult,
};
use tokio::sync::mpsc;

struct Executor {
    wal: Arc<FileWalStore>,
    store: Arc<broca_store::SqliteStore>,
    http: broca_provider::ProviderHttpClient,
    base: String,
}

#[async_trait]
impl RunExecutor for Executor {
    async fn execute(
        &self,
        mut job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        let model = format!(
            "{}/{}",
            job.req.model.provider_module_id, job.req.model.model_id
        );
        // Pin the grammar so a historical catalog's model-family defaults cannot change the load.
        let options = ResolveOptions {
            family_override: (job.req.model.provider_module_id == "openai")
                .then_some(WireFamilyId::OpenAiResponses),
            ..Default::default()
        };
        let resolved = broca_catalog::live::resolve_provider_with_catalog_base_url(
            &model,
            &self.base,
            Some(4096),
            None,
            &AuthMode::None,
            &options,
            self.http.clone(),
        )
        .map_err(|e| RunError::Config(e.to_string()))?;
        let materialized = job.materialized_store(self.store.clone());
        let mode = match job.kind {
            RunKind::AppendEpisode => StartMode::AppendEpisode,
            RunKind::ResumeAfterAuth => StartMode::ResumeAfterAuth,
            RunKind::FreshOrResume => StartMode::FreshOrResume,
        };
        // The production executor leaves durable payload projection to the actor as well.
        job.control.emit_control_payloads = false;
        broca_core::run_loop_on_writer_admitted_with_prior_snapshot(
            job.req,
            &job.writer,
            mode,
            &resolved.provider,
            &StubToolPlane::new(),
            &AllowAll,
            self.wal.as_ref(),
            materialized,
            job.control,
            &out,
            job.on_admitted,
            Some(&job.prior_snapshot),
        )
        .await
    }
}

fn history(session: usize) -> Vec<Message> {
    let pairs = 75 + session % 76;
    let target = (2 + session % 9) * 1024 * 1024;
    let mut messages = vec![Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "Investigate the repository and report findings.".into(),
        }],
    )];
    for i in 0..pairs {
        let id = ToolCallId(format!("call-{session}-{i}"));
        messages.push(Message::new(
            Role::Assistant,
            vec![ContentBlock::ToolCall(ToolCall {
                tool_call_id: id.clone(),
                tool_name: "read_file".into(),
                input: serde_json::json!({"path": format!("src/file-{i}.rs")}),
                thought_signature: None,
            })],
        ));
        // Repeated source-like lines model large textual tool results, not random gzip-resistant data.
        let line = format!("pub fn item_{session}_{i}() -> usize {{ /* source listing */ 42 }}\n");
        let text = line.repeat(target / pairs / line.len());
        messages.push(Message::new(
            Role::Tool,
            vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: id,
                output: ToolOutput::Text { text },
                is_error: false,
            })],
        ));
    }
    messages
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    broca_heap_profile::start();
    let args: Vec<_> = std::env::args().collect();
    serve(&args).await
}

async fn serve(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    if !(4..=5).contains(&args.len()) {
        return Err(
            "usage: memory-load-server <state-root> <stub-base-url> <sessions> [idle-ttl-ms]"
                .into(),
        );
    }
    let root = PathBuf::from(&args[1]);
    broca_store::require_legacy_operator_root(&root, "memory-load-server")?;
    let sessions: usize = args[3].parse()?;
    // Without the argument the opener keeps the production idle TTL. A TTL longer than the
    // whole load keeps every short session's actor warm until the steady snapshot, so two
    // builds that run the one-shot phase at different speeds still hold the same actor count.
    let idle_ttl = args
        .get(4)
        .map(|ms| ms.parse().map(Duration::from_millis))
        .transpose()?;
    std::fs::create_dir_all(root.join("project"))?;
    let catalog = Arc::new(broca_catalog::CatalogCache::open(root.join("catalog.json")));
    assert!(catalog.lkg_load_error().is_none(), "catalog load failed");
    broca_catalog::install_global_catalog_cache(catalog.clone());
    let broca_module_serve::bundle::LegacyBundle { wal, leases, store } =
        broca_module_serve::bundle::open_legacy_bundle(&root, "memory-load-server")?;
    let exec = Arc::new(Executor {
        wal: wal.clone(),
        store: store.clone(),
        http: broca_provider::build_provider_http_client(),
        base: args[2].clone(),
    });
    let resolver = Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1));
    let retention = Arc::new(broca_module::retention::RetentionService::new(
        wal.clone(),
        store.clone(),
        leases.clone(),
        store.clone(),
        resolver.clone(),
    )?);
    let mut opener = DefaultOpener::new(leases, wal.clone(), exec)
        .with_run_index(store.clone())
        .with_materialized_store(store.clone())
        .with_export_index(store.clone())
        .with_charge_basis_resolver(resolver);
    if let Some(ttl) = idle_ttl {
        opener = opener.with_idle_ttl(ttl);
    }
    let registry = Arc::new(LineageRegistry::new_with_sources(
        Arc::new(opener),
        Some(wal),
        Some(store.clone()),
    ));
    registry.set_session_address_inventory(store);
    registry.set_retention_service(retention);
    registry.boot_retention().await?;
    for session in 0..sessions {
        registry
            .import(SessionImportRequest {
                project_id: None,
                principal: None,
                session: BindIdentity::new(
                    root.join("project"),
                    "memory-load",
                    format!("long-{session}"),
                ),
                run_id: RunId(format!("import-{session}")),
                messages: history(session),
                source_ref: "memory-load:canonical-tool-history".into(),
            })
            .await
            .map_err(|e| format!("seed import: {e:?}"))?;
    }
    if let Some(dir) = std::env::var_os("BROCA_HEAP_PROFILE_DIR") {
        let dir = PathBuf::from(dir);
        std::thread::spawn(move || loop {
            let (mut current, mut high) = (0, 0);
            // SQLite uses C malloc, outside Rust's global allocator, across both databases.
            let rc = unsafe {
                rusqlite::ffi::sqlite3_status64(
                    rusqlite::ffi::SQLITE_STATUS_MEMORY_USED,
                    &mut current,
                    &mut high,
                    0,
                )
            };
            assert_eq!(rc, 0);
            std::fs::write(
                dir.join("sqlite-memory.pending"),
                format!("{{\"current\":{current},\"high\":{high}}}"),
            )
            .unwrap();
            std::fs::rename(
                dir.join("sqlite-memory.pending"),
                dir.join("sqlite-memory.json"),
            )
            .unwrap();
            std::thread::sleep(Duration::from_millis(100));
        });
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    std::fs::write(
        root.join("listen.json"),
        serde_json::to_vec(&serde_json::json!({
            "address": listener.local_addr()?.to_string(), "pid": std::process::id(), "sessions": sessions,
        }))?,
    )?;
    let (socket, _) = listener.accept().await?;
    let (read, write) = socket.into_split();
    broca_store::session_refs::migrate(
        &root.join("run-index.db"),
        &broca_core::ReadOnlyWalReader::new(&root),
    )?;
    broca_module_serve::serve_connection_with_catalog(
        read,
        write,
        "broca",
        registry.clone(),
        registry.clone(),
        broca_module_serve::serve::VaultGauge::not_in_use(),
        Some(catalog),
    )
    .await?;
    registry.stop_and_seal().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn load_server_refuses_before_creating_project_catalog_or_index() {
        for (index, marker) in [
            Some(b"broca-state/2\n".as_slice()),
            Some(b"malformed"),
            None,
        ]
        .into_iter()
        .enumerate()
        {
            for existing in [false, true] {
                let root = broca_test_support::scratch_root().join(format!(
                    "load-root-{}-{index}-{existing}",
                    std::process::id()
                ));
                std::fs::create_dir(&root).unwrap();
                match marker {
                    Some(bytes) => std::fs::write(root.join("state-format"), bytes).unwrap(),
                    None => std::fs::create_dir(root.join("state-format")).unwrap(),
                }
                for name in [
                    "run-index.db",
                    "run-index.db-wal",
                    "run-index.db-shm",
                    "operator-data",
                ] {
                    if existing || name == "operator-data" {
                        std::fs::write(root.join(name), name).unwrap();
                    }
                }
                let error = serve(&[
                    "memory-load-server".into(),
                    root.to_str().unwrap().into(),
                    "http://unused.invalid".into(),
                    "0".into(),
                ])
                .await
                .unwrap_err();
                assert_eq!(
                    std::fs::read_dir(&root).unwrap().count(),
                    if existing { 5 } else { 2 }
                );
                let refusal = error
                    .downcast_ref::<broca_store::LegacyOnlyOperator>()
                    .unwrap();
                assert_eq!(refusal.operator, "memory-load-server");
                assert_eq!(refusal.root, root);
                for name in [
                    "run-index.db",
                    "run-index.db-wal",
                    "run-index.db-shm",
                    "operator-data",
                ] {
                    if existing || name == "operator-data" {
                        assert_eq!(std::fs::read(root.join(name)).unwrap(), name.as_bytes());
                    } else {
                        assert!(!root.join(name).exists());
                    }
                }
                std::fs::remove_dir_all(root).unwrap();
            }
        }
    }

    #[tokio::test]
    async fn load_server_missing_marker_keeps_legacy_argument_validation() {
        let root =
            broca_test_support::scratch_root().join(format!("legacy-load-{}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        let error = serve(&[
            "memory-load-server".into(),
            root.to_str().unwrap().into(),
            "http://unused.invalid".into(),
            "not-a-count".into(),
        ])
        .await
        .unwrap_err();
        assert!(error.downcast_ref::<std::num::ParseIntError>().is_some());
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
        std::fs::remove_dir(root).unwrap();
    }
}
