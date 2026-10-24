//! Measures the model step fact backfill the daemon runs on its first start, through the
//! daemon's own background service ([`broca_module_serve::model_step_facts::Service`]), against a
//! COPY of a broca state root, or against a synthetic production-shaped fixture this example can
//! write.
//!
//! ```text
//! # Write a fixture: <small> sessions of 8 steps each, and <large> sessions whose WAL holds
//! # about <large_mb> MB of steps (production has a few very long sessions among many short ones).
//! cargo run --example model-step-backfill -- fixture <state-root> <small> <large> <large_mb>
//! # Run the daemon's service on a state root until its walk finishes, and report.
//! cargo run --example model-step-backfill -- measure <state-root>
//! ```
//!
//! Never point `measure` at a live state root: it writes facts, checkpoints and walk state into
//! that root's `run-index.db`. It prints only counts, timings and memory, never session content.

use broca_core::{FileWalStore, WalStore};
use broca_module_serve::model_step_facts::{now_ms, Hints, Service};
use broca_protocol::{
    content::{AssistantMessage, ContentBlock, Message, Role},
    provider::{AttemptOutcome, ContentProgress},
    request::{GenerationConfig, ModelHandle, ResponseFormat, ToolChoice},
    BindIdentity, FinishReason, MessageId, RunId, StepId, Usage,
};
use broca_store::SqliteStore;
use broca_wal::{RunConfig, RunOrigin, WalRecord};
use std::{sync::Arc, time::Instant};

fn main() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("fixture") if args.len() == 5 => {
            let number = |index: usize| args[index].parse::<u64>().map_err(|e| e.to_string());
            fixture(args[1].as_ref(), number(2)?, number(3)?, number(4)?)
        }
        Some("measure") if args.len() == 2 => measure(args[1].as_ref()),
        _ => Err("usage: fixture <root> <small> <large> <large_mb> | measure <root>".into()),
    }
}

/// `VmRSS` and `VmHWM` (peak) in MB, read from `/proc` where it exists.
fn memory() -> String {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let field = |name: &str| {
        status
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|kb| kb.parse::<u64>().ok())
            .map_or("unknown".to_string(), |kb| format!("{} MB", kb / 1024))
    };
    format!("rss={} peak_rss={}", field("VmRSS:"), field("VmHWM:"))
}

fn measure(root: &std::path::Path) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    runtime.block_on(async {
        let opened = Instant::now();
        let store =
            Arc::new(SqliteStore::open(root.join("run-index.db")).map_err(|e| e.to_string())?);
        let wal = Arc::new(FileWalStore::new(root.join("wal")));
        println!(
            "opened store in {} ms; before the walk: {}",
            opened.elapsed().as_millis(),
            memory()
        );
        let stop = tokio_util::sync::CancellationToken::new();
        let began = Instant::now();
        let service = Service {
            store: store.clone(),
            wal,
            hints: Arc::new(Hints::default()),
            stop: stop.clone(),
        }
        .spawn();
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            if store
                .model_step_walk()
                .map_err(|e| e.to_string())?
                .is_some_and(|walk| walk.done)
            {
                break;
            }
        }
        let backlog = store.model_step_backlog_len().map_err(|e| e.to_string())?;
        println!(
            "walk finished in {} ms; backlog waiting to publish: {backlog}; {}",
            began.elapsed().as_millis(),
            memory()
        );
        // Watch the capped publisher for a while to confirm its rate.
        let watch = std::time::Duration::from_secs(120);
        tokio::time::sleep(watch).await;
        let after = store.model_step_backlog_len().map_err(|e| e.to_string())?;
        let rate = (backlog - after) as f64 / watch.as_secs_f64();
        println!(
            "published {} backlog rows in {} s ({rate:.1} rows/s); {}",
            backlog - after,
            watch.as_secs(),
            memory()
        );
        stop.cancel();
        let _ = service.await;
        let conn =
            rusqlite::Connection::open(root.join("run-index.db")).map_err(|e| e.to_string())?;
        let mut statement = conn
            .prepare(
                "SELECT COALESCE(json_extract(segment_json, '$.kind'), 'session'), COUNT(*)
                   FROM export_facts GROUP BY 1 ORDER BY 1",
            )
            .map_err(|e| e.to_string())?;
        let rows = statement
            .query_map([], |row| {
                Ok(format!(
                    "{}={}",
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?
                ))
            })
            .map_err(|e| e.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        println!("export_facts by kind: {}", rows.join(" "));
        Ok(())
    })
}

/// A finished run per session, recent enough to be backfilled: `small` sessions of 8 steps with
/// short answers, and `large` sessions whose answers add up to about `large_mb` MB each.
fn fixture(root: &std::path::Path, small: u64, large: u64, large_mb: u64) -> Result<(), String> {
    std::fs::create_dir_all(root).map_err(|e| e.to_string())?;
    let wal = FileWalStore::new(root.join("wal"));
    let store = SqliteStore::open(root.join("run-index.db")).map_err(|e| e.to_string())?;
    drop(store);
    let now = now_ms();
    let mut runs = Vec::new();
    let large_text = "x".repeat(256 * 1024);
    let large_steps = large_mb * 4;
    for index in 0..small + large {
        let is_large = index >= small;
        let session = BindIdentity::new(
            format!("/fixture/project-{}", index % 500),
            "fixture-harness",
            format!("session-{index}"),
        );
        let run_id = format!("run-{index}");
        let t = now - (index % 25 + 1) * 24 * 60 * 60 * 1000;
        let (steps, text) = if is_large {
            (large_steps, large_text.as_str())
        } else {
            (8, "a short synthetic answer of about this length")
        };
        let mut log = wal.open(&session).map_err(|e| e.to_string())?;
        let append = |log: &mut Box<dyn broca_wal::Wal + Send>, record: &WalRecord| {
            log.append(1, record, false)
                .map(|_| ())
                .map_err(|e| e.to_string())
        };
        append(&mut log, &started(&session, &run_id, t))?;
        for step in 1..=steps {
            append(
                &mut log,
                &WalRecord::StepStarted {
                    step_id: StepId(step),
                },
            )?;
            append(&mut log, &attempt(step, t + step * 1000))?;
            append(&mut log, &finished_step(step, text))?;
        }
        append(
            &mut log,
            &WalRecord::RunFinished {
                interrupt_reason: None,
                reason: broca_protocol::caller::RunFinishReason::Completed,
                ts_ms: Some(t + steps * 1000 + 1),
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
        )?;
        log.sync().map_err(|e| e.to_string())?;
        runs.push((
            run_id,
            serde_json::to_string(&session).map_err(|e| e.to_string())?,
            t,
        ));
    }
    let mut conn =
        rusqlite::Connection::open(root.join("run-index.db")).map_err(|e| e.to_string())?;
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    for (run_id, session, t) in &runs {
        tx.execute(
            "INSERT INTO run_index (run_id, session, state, state_changed_ms)
             VALUES (?1, ?2, 'completed', ?3)",
            rusqlite::params![run_id, session, *t as i64],
        )
        .map_err(|e| e.to_string())?;
    }
    tx.commit().map_err(|e| e.to_string())?;
    println!(
        "wrote {} sessions ({small} small, {large} large of about {large_mb} MB)",
        runs.len()
    );
    Ok(())
}

fn started(session: &BindIdentity, run_id: &str, ts_ms: u64) -> WalRecord {
    WalRecord::RunStarted {
        tags: None,
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
        run_id: RunId(run_id.into()),
        session: session.clone(),
        ts_ms: Some(ts_ms),
        config: Box::new(RunConfig {
            turn_directives_v1: None,
            render_tags: false,
            computed_tags: None,
            model: ModelHandle::new("anthropic", "claude-fixture"),
            tools: vec![].into(),
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig::default(),
            response_format: ResponseFormat::default(),
            provider_options: serde_json::Value::Null,
            cache_tiers: serde_json::Value::Null,
            context_epoch: 0,
            renderer_version: 1,
            frozen_render_config: serde_json::json!({
                "family_id": "anthropic_messages",
                "cache": {"version": 1, "class": "markers", "syntax": "anthropic_cache_control",
                          "ttl": "5m", "max_breakpoints": 4, "lookback_blocks": 20}
            }),
            auth_selection: None,
            tools_were_omitted: false,
            budget: None,
            context_limit: Some(200_000),
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: vec![],
            context_management: None,
        }),
        input: vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "a synthetic prompt".into(),
            }],
        )]
        .into(),
        origin: RunOrigin::Fresh,
        submission_id: None,
    }
}

fn attempt(step: u64, end_ms: u64) -> WalRecord {
    WalRecord::ModelAttemptFinished {
        step_id: StepId(step),
        attempt: 0,
        request_body_len: 40_000 + step,
        request_body_sha256: format!("{step:064x}"),
        http_status: Some(200),
        content_type: None,
        response_body_len: 4_096,
        response_body_sha256: "b".repeat(64),
        response_body_cap: None,
        outcome: AttemptOutcome::Complete,
        ts_ms: end_ms,
        content_progress: Box::new(ContentProgress {
            first_content_at_ms: Some(end_ms - 500),
            ..ContentProgress::default()
        }),
        sent_at_ms: Some(end_ms - 900),
        transport: None,
        websocket_request: None,
        websocket_close_code: None,
        input_transformations: None,
        input_transformations_unmeasured: false,
        credential_refresh: None,
        served_account_ref: None,
    }
}

fn finished_step(step: u64, text: &str) -> WalRecord {
    WalRecord::ModelStepFinished {
        tags: None,
        hooks: None,
        step_id: StepId(step),
        assistant_message: AssistantMessage {
            message_id: MessageId(format!("m{step}")),
            content: vec![ContentBlock::Text { text: text.into() }],
        }
        .into(),
        usage: Usage {
            input_tokens: Some(3),
            cached_input_tokens: Some(30_000),
            cache_write_tokens: Some(500),
            output_tokens: Some(400),
            reasoning_tokens: None,
        },
        finish_reason: Some(FinishReason::Stop),
        retries_used: 0,
        service_tier_used: None,
        speed_used: None,
    }
}
