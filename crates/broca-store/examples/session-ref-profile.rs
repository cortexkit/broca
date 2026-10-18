//! Production-scale synthetic fixture and cold-process boot/RSS measurement.
use broca_core::{session_addr, FileWalStore};
use broca_protocol::{BindIdentity, ModelHandle, RunId};
use broca_store::SqliteStore;
use broca_wal::{RunConfig, RunOrigin, WalRecord};
use rusqlite::{params, Connection};
use std::{path::Path, sync::Arc, time::Instant};

fn session(i: usize) -> BindIdentity {
    let suffix = "realistic-project-component/".repeat(1 + i % 3);
    BindIdentity::new(
        format!("/Users/synthetic/Work/Projects/{suffix}repository"),
        if i.is_multiple_of(3) {
            "claude-code"
        } else {
            "broca"
        },
        format!("01978abc-1234-7def-8abc-{i:012x}"),
    )
}
fn record(session: &BindIdentity) -> WalRecord {
    WalRecord::RunStarted {
        session_class: None,
        retention: None,
        hooks: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        marker_order_version: None,
        manifest: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        run_id: RunId("synthetic-run".into()),
        session: session.clone(),
        ts_ms: Some(1),
        config: Box::new(RunConfig {
            model: ModelHandle::new("stub", "synthetic"),
            tools: vec![].into(),
            tool_choice: Default::default(),
            generation: Default::default(),
            response_format: Default::default(),
            provider_options: serde_json::Value::Null,
            cache_tiers: serde_json::Value::Null,
            context_epoch: 0,
            renderer_version: 1,
            frozen_render_config: serde_json::Value::Null,
            auth_selection: None,
            tools_were_omitted: false,
            budget: None,
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: Vec::new(),
            context_management: None,
        }),
        input: Default::default(),
        origin: RunOrigin::Fresh,
        submission_id: None,
    }
}
fn seed(root: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if root.exists() {
        return Err("fixture root must not exist".into());
    }
    std::fs::create_dir_all(root)?;
    drop(SqliteStore::open(root.join("run-index.db"))?);
    let mut conn = Connection::open(root.join("run-index.db"))?;
    conn.execute_batch("DROP TRIGGER IF EXISTS agent_data_run_insert; DROP TRIGGER IF EXISTS agent_data_run_update;
        DROP TABLE IF EXISTS agent_data_session_ref; DROP TABLE IF EXISTS agent_data_projection;
        DROP TABLE IF EXISTS agent_data_census; DROP TABLE IF EXISTS agent_data_census_done;
        DROP TABLE IF EXISTS agent_data_pending_session; DROP TABLE IF EXISTS agent_data_writer;")?;
    {
        let tx = conn.transaction()?;
        for i in 0..165_000 {
            tx.execute(
                "INSERT INTO run_index(run_id,session,state) VALUES(?1,?2,'completed')",
                params![
                    format!("run-{i}"),
                    serde_json::to_string(&session(i % 142_000))?
                ],
            )?;
        }
        tx.commit()?;
    }
    for directory in ["wal", "wal-ephemeral", "wal-durable", "wal-archive"] {
        std::fs::create_dir(root.join(directory))?;
    }
    for i in 0..66_000 {
        let identity = session(i);
        let population = if i < 6000 {
            "wal-ephemeral"
        } else if i < 16000 {
            "wal-durable"
        } else {
            "wal"
        };
        std::fs::write(
            root.join(population)
                .join(format!("{}.wal", session_addr(&identity))),
            broca_wal::framing::encode(1, 1, &record(&identity))?,
        )?;
    }
    for batch in 0..76 {
        let mut writer = broca_wal::archive::ArchiveWriter::create(
            root.join(format!("wal-archive/fold-{}.ark", batch + 1)),
        )?
        .without_feature_floor();
        for i in (66_000 + batch * 1000)..(67_000 + batch * 1000) {
            let identity = session(i);
            writer.append(
                &session_addr(&identity),
                &broca_wal::framing::encode(1, 1, &record(&identity))?,
            )?;
        }
        writer.seal()?;
    }
    println!("rows=165000 distinct_sessions=142000 live=66000 archived=76000");
    Ok(())
}
fn rss_kib() -> u64 {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").unwrap();
        status
            .lines()
            .find(|line| line.starts_with("VmRSS:"))
            .and_then(|line| line.split_whitespace().nth(1))
            .unwrap()
            .parse()
            .unwrap()
    }
    #[cfg(not(target_os = "linux"))]
    {
        let output = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
            .unwrap();
        String::from_utf8(output.stdout)
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    }
}
fn boot(root: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let _runtime = tokio::runtime::Runtime::new()?;
    let start = Instant::now();
    let index = Arc::new(SqliteStore::open(root.join("run-index.db"))?);
    let wal = Arc::new(FileWalStore::new(root.join("wal")));
    wal.set_session_ref_index(index.clone());
    let serving_ms = start.elapsed().as_millis();
    let boot_rss_kib = rss_kib();
    let counts = broca_store::session_refs::migrate(
        &root.join("run-index.db"),
        &broca_core::ReadOnlyWalReader::new(root),
    )?;
    println!("serving_ms={serving_ms} boot_rss_kib={boot_rss_kib} moves_ready_ms={} settled_rss_kib={} census_addresses={} census_payload_bytes={}",start.elapsed().as_millis(),rss_kib(),counts.addresses,counts.bytes_read);
    Ok(())
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 2 {
        return Err("usage: session-ref-profile seed|boot <synthetic-root>".into());
    }
    match args[0].as_str() {
        "seed" => seed(Path::new(&args[1])),
        "boot" => boot(Path::new(&args[1])),
        _ => Err("expected seed or boot".into()),
    }
}
