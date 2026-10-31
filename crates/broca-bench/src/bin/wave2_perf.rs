// A `src/bin/` target is its OWN crate root and does NOT inherit the sibling lib/main
// attribute, so this must be stated per binary or the guarantee silently does not apply here.
#![forbid(unsafe_code)]

//! Deterministic exit benchmark for Wave 2's lineage read paths.
//!
//! Run with `cargo run --release -p broca-bench --bin wave2-perf`. The binary builds each
//! fixture through the production WAL writer and asynchronous SQLite projector, prints medians
//! for one warmup plus five measured iterations, then checks the expected asymptotic shape.

use std::{
    collections::HashSet,
    hint::black_box,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use broca_core::{
    advance_control_projection_state, load_prior_wal_snapshot, read_session_page, replay,
    resume_state_from_store, AllowAll, ControlProjectionState, FileWalStore, MaterializedStore,
    Persister, RunControl, RunRequest, SessionStore, StartMode, StubProvider, StubToolPlane,
    WalHandle, WalStore,
};
use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, Message, Role, ToolCall},
    provider::{NormalizedEvent, ProviderControl},
    request::{ExecutionMode, ModelHandle, StopCondition, Tool, ToolChoice},
    BindIdentity, FinishReason, GenerationConfig, MessageId, RunId, StepId, ToolCallId, Usage,
};
use broca_store::SqliteStore;
use broca_wal::{RunOrigin, WalRecord};
use serde_json::json;
use tokio::sync::mpsc;

const LINEAGE_SIZES: [usize; 3] = [50, 200, 800];
const PAGE_LIMIT: u64 = 200;
const WARMUPS: usize = 1;
const SAMPLES: usize = 9;

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(episodes: usize) -> Self {
        let path = std::env::temp_dir().join(format!(
            "broca-wave2-perf-{}-{episodes}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(path.join("project")).expect("create benchmark directory");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

struct Fixture {
    _temp: TempDir,
    session: BindIdentity,
    wal_store: FileWalStore,
    store: Arc<SqliteStore>,
    writer: Option<WalHandle>,
}

impl Fixture {
    async fn build(episodes: usize) -> Self {
        let temp = TempDir::new(episodes);
        let session = BindIdentity::new(temp.path().join("project"), "wave2", "lineage");
        let wal_store = FileWalStore::new(temp.path().join("wal"));
        let store = Arc::new(
            SqliteStore::open(temp.path().join("store.db")).expect("open benchmark SQLite store"),
        );
        let writer = WalHandle::spawn(wal_store.open(&session).expect("open benchmark WAL"), 1)
            .expect("spawn writer");
        let plane = StubToolPlane::new().with_canned_text("lookup", "fixed tool result");
        let (output_tx, mut output_rx) = mpsc::channel(256);
        let collector = tokio::spawn(async move { while output_rx.recv().await.is_some() {} });

        for episode in 0..episodes {
            let tool_turn = (episode + 1) % 10 == 0;
            let provider = StubProvider::new(provider_scripts(episode, tool_turn));
            let mode = if episode == 0 {
                StartMode::FreshOrResume
            } else {
                StartMode::AppendEpisode
            };
            let session_store: Arc<dyn SessionStore> = store.clone();
            let summary = broca_core::run_loop_on_writer(
                request(&session, episode),
                &writer,
                mode,
                &provider,
                &plane,
                &AllowAll,
                &wal_store,
                Some(session_store),
                RunControl::default(),
                &output_tx,
            )
            .await
            .expect("synthesize episode through production run loop");
            assert_eq!(summary.reason, RunFinishReason::Completed);
        }

        drop(output_tx);
        collector.await.expect("output collector joins");

        let frames = wal_store
            .read_records(&session)
            .expect("read synthesized lineage");
        assert_eq!(
            frames
                .iter()
                .filter(|frame| matches!(frame.record, WalRecord::RunStarted { .. }))
                .count(),
            episodes,
            "fixture contains every requested episode"
        );
        assert_eq!(
            frames
                .iter()
                .filter(|frame| matches!(frame.record, WalRecord::ToolBatchFinished { .. }))
                .count(),
            episodes / 10,
            "fixture includes one tool boundary every ten episodes"
        );
        let prefix = MaterializedStore::read_prefix_with_watermark(store.as_ref(), &session)
            .expect("read projected fixture");
        assert!(
            prefix.watermark_seq > 0,
            "S6 projector materialized fixture"
        );
        assert!(
            frames.iter().any(|frame| frame.seq > prefix.watermark_seq),
            "terminal WAL tail remains beyond the projection boundary"
        );

        Self {
            _temp: temp,
            session,
            wal_store,
            store,
            writer: Some(writer),
        }
    }

    async fn close(mut self) {
        self.writer
            .take()
            .expect("writer is present")
            .shutdown()
            .await
            .expect("benchmark writer shutdown must succeed");
    }
}

#[derive(Clone, Copy)]
struct Row {
    episodes: usize,
    resume_prepared: Duration,
    resume_end_to_end: Duration,
    full_replay: Duration,
    session_read: Duration,
    projection: Duration,
    live_attach_seed: Duration,
}

#[tokio::main]
async fn main() {
    let mut rows = Vec::with_capacity(LINEAGE_SIZES.len());
    for episodes in LINEAGE_SIZES {
        eprintln!("building deterministic {episodes}-episode fixture...");
        let mut fixture = Fixture::build(episodes).await;
        rows.push(measure_fixture(&mut fixture, episodes).await);
        fixture.close().await;
    }

    print_table(&rows);
    assert_shape(&rows);
}

async fn measure_fixture(fixture: &mut Fixture, episodes: usize) -> Row {
    let frames = fixture
        .wal_store
        .read_records(&fixture.session)
        .expect("read fixture for resume inputs");
    let prefix =
        MaterializedStore::read_prefix_with_watermark(fixture.store.as_ref(), &fixture.session)
            .expect("read materialized resume prefix");
    let tail: Vec<WalRecord> = frames
        .iter()
        .filter(|frame| frame.seq > prefix.watermark_seq)
        .map(|frame| frame.record.clone())
        .collect();
    let run_config = frames.iter().rev().find_map(|frame| match &frame.record {
        WalRecord::RunStarted { config, .. } => Some((**config).clone()),
        _ => None,
    });
    let run_id = frames.iter().rev().find_map(|frame| match &frame.record {
        WalRecord::RunStarted { run_id, .. } => Some(run_id.clone()),
        _ => None,
    });

    let resume_prepared = measure(|| {
        resume_state_from_store(
            black_box(&prefix),
            black_box(&tail),
            black_box(run_config.clone()),
            black_box(run_id.clone()),
            black_box(None),
        )
        .expect("resume merge succeeds")
    });

    let safe_head = frames.last().map_or(0, |frame| frame.seq);
    let resume_end_to_end = measure(|| {
        let snapshot = load_prior_wal_snapshot(
            &fixture.wal_store,
            &fixture.session,
            safe_head,
            Some(fixture.store.as_ref()),
        )
        .expect("read checkpoint plus WAL tail for end-to-end resume");
        let prefix =
            MaterializedStore::read_prefix_with_watermark(fixture.store.as_ref(), &fixture.session)
                .expect("read materialized resume prefix");
        let tail: Vec<WalRecord> = snapshot
            .tail
            .iter()
            .filter(|frame| frame.seq > prefix.watermark_seq)
            .map(|frame| frame.record.clone())
            .collect();
        let budget_state = snapshot
            .checkpoint_at_base
            .as_ref()
            .and_then(|checkpoint| checkpoint.last_episode.as_ref())
            .and_then(|episode| episode.budget_replay_state.clone());
        let episode = snapshot
            .checkpoint_at_head
            .last_episode
            .expect("fixture has a prior episode");
        resume_state_from_store(
            black_box(&prefix),
            black_box(&tail),
            black_box(Some(episode.config)),
            black_box(Some(episode.run_id)),
            black_box(budget_state),
        )
        .expect("end-to-end resume succeeds")
    });

    let full_replay = measure(|| {
        let records: Vec<WalRecord> = fixture
            .wal_store
            .read_records(&fixture.session)
            .expect("read whole WAL for replay")
            .into_iter()
            .map(|frame| frame.record)
            .collect();
        replay(black_box(&records)).expect("full replay succeeds")
    });

    let session_read = measure(|| {
        let snapshot = MaterializedStore::read_page_with_watermark(
            fixture.store.as_ref(),
            &fixture.session,
            None,
            PAGE_LIMIT,
            broca_core::context::ReadView::Raw,
        )
        .expect("read newest store page");
        let tail = fixture
            .wal_store
            .read_records_range(
                &fixture.session,
                snapshot.watermark_seq,
                None,
                snapshot.anchor.as_ref(),
            )
            .expect("read bounded WAL tail");
        read_session_page(
            &snapshot,
            match tail.records() {
                broca_wal::WalRangeRecords::Complete(records)
                | broca_wal::WalRangeRecords::Partial(records)
                | broca_wal::WalRangeRecords::Unknown(records) => records,
                broca_wal::WalRangeRecords::Absent => &[],
            },
            None,
            Some(PAGE_LIMIT),
        )
        .expect("assemble newest session page")
    });

    let live_attach_seed = measure(|| {
        let seed = Persister::projection_seed(fixture.store.as_ref(), &fixture.session)
            .expect("read live-attach projection seed")
            .expect("fixture has a projection seed");
        let can_seed_projection =
            seed.completed_steps == 0 || seed.last_step_finish_reason.is_some();
        let mut state = if can_seed_projection {
            ControlProjectionState::from_completed_steps_and_final_step_finish_reason(
                seed.completed_steps,
                seed.last_step_finish_reason,
                None,
            )
        } else {
            ControlProjectionState::default()
        };
        if can_seed_projection && seed.watermark_seq < safe_head {
            if let Some(anchor) = seed.anchor.as_ref() {
                let range = fixture
                    .wal_store
                    .read_records_range_from_valid_anchor(
                        &fixture.session,
                        seed.watermark_seq,
                        Some(safe_head),
                        anchor,
                    )
                    .expect("read live-attach suffix")
                    .expect("fixture anchor remains valid");
                black_box(advance_control_projection_state(
                    match range.records() {
                        broca_wal::WalRangeRecords::Complete(records)
                        | broca_wal::WalRangeRecords::Partial(records)
                        | broca_wal::WalRangeRecords::Unknown(records) => records,
                        broca_wal::WalRangeRecords::Absent => &[],
                    },
                    &mut state,
                ));
            } else {
                let records = fixture
                    .wal_store
                    .read_records(&fixture.session)
                    .expect("fall back to full attach replay without byte anchors");
                state = ControlProjectionState::default();
                black_box(advance_control_projection_state(&records, &mut state));
            }
        } else if !can_seed_projection {
            let records = fixture
                .wal_store
                .read_records(&fixture.session)
                .expect("fall back to full replay when the seed cannot prove step presence");
            black_box(advance_control_projection_state(&records, &mut state));
        }
        state
    });

    let projection = measure_projection(fixture, episodes).await;

    Row {
        episodes,
        resume_prepared,
        resume_end_to_end,
        full_replay,
        session_read,
        projection,
        live_attach_seed,
    }
}

fn measure<T>(mut operation: impl FnMut() -> T) -> Duration {
    for _ in 0..WARMUPS {
        black_box(operation());
    }
    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        black_box(operation());
        samples.push(started.elapsed());
    }
    best(samples)
}

async fn measure_projection(fixture: &mut Fixture, episodes: usize) -> Duration {
    let mut samples = Vec::with_capacity(SAMPLES);
    for iteration in 0..(WARMUPS + SAMPLES) {
        let safe_h = append_projection_episode(fixture, episodes, iteration).await;
        let started = Instant::now();
        let seed = Persister::projection_seed(fixture.store.as_ref(), &fixture.session)
            .expect("read projection seed")
            .expect("fixture has complete projection metadata");
        let range = fixture
            .wal_store
            .read_records_range(
                &fixture.session,
                seed.watermark_seq,
                Some(safe_h),
                seed.anchor.as_ref(),
            )
            .expect("read one-turn projection range");
        let watermark = Persister::project_range(fixture.store.as_ref(), &fixture.session, &range)
            .expect("project one new turn and its anchor");
        black_box(watermark);
        let elapsed = started.elapsed();
        if iteration >= WARMUPS {
            samples.push(elapsed);
        }
    }
    best(samples)
}

async fn append_projection_episode(fixture: &Fixture, episodes: usize, iteration: usize) -> u64 {
    let config = fixture
        .wal_store
        .read_records(&fixture.session)
        .expect("read config for projection turn")
        .iter()
        .rev()
        .find_map(|frame| match &frame.record {
            WalRecord::RunStarted { config, .. } => Some(config.clone()),
            _ => None,
        })
        .expect("fixture has a run config");
    let suffix = format!("projection-{episodes}-{iteration}");
    let writer = fixture.writer.as_ref().expect("writer is present");
    writer.record(WalRecord::RunStarted {
        written_at_ms: None,
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
        ts_ms: None,
        run_id: RunId(suffix.clone()),
        session: fixture.session.clone(),
        config,
        input: vec![message(Role::User, format!("input-{suffix}"))].into(),
        origin: RunOrigin::Fresh,
        submission_id: None,
    });
    writer.record(WalRecord::StepStarted { step_id: StepId(1) });
    writer.record(WalRecord::ModelStepFinished {
        fallback: Default::default(),
        written_at_ms: None,
        tags: None,
        hooks: None,
        step_id: StepId(1),
        assistant_message: (AssistantMessage {
            message_id: MessageId(format!("message-{suffix}")),
            content: vec![ContentBlock::Text {
                text: format!("answer-{suffix}"),
            }],
        })
        .into(),
        usage: Usage {
            input_tokens: Some(11),
            output_tokens: Some(7),
            ..Usage::default()
        },
        finish_reason: Some(FinishReason::Stop),
        retries_used: 0,
        service_tier_used: None,
        speed_used: None,
    });
    writer.record(WalRecord::RunFinished {
        interrupt_reason: None,
        ts_ms: None,
        reason: RunFinishReason::Completed,
        usage: Usage {
            input_tokens: Some(11),
            output_tokens: Some(7),
            ..Usage::default()
        },
        error: None,
        retries_used: 0,
    });
    writer.sync().await.expect("sync projection turn")
}

fn request(session: &BindIdentity, episode: usize) -> RunRequest {
    RunRequest {
        input_written_at_ms: None,
        directives: None,
        session_class: None,
        retention: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        run_id: RunId(format!("run-{episode:04}")),
        session: session.clone(),
        model: ModelHandle::new("stub", "wave2"),
        input: vec![message(Role::User, format!("input-{episode:04}"))],
        tools: vec![Tool {
            name: "lookup".into(),
            description: "deterministic benchmark tool".into(),
            input_schema: json!({"type": "object"}),
            module: None,
            execution_mode: ExecutionMode::Parallel,
            reply: None,
        }],
        policy_allowed: HashSet::from(["lookup".to_owned()]),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![StopCondition::StepCount { n: 3 }],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
        structured_output: None,
    }
}

fn provider_scripts(episode: usize, tool_turn: bool) -> Vec<Vec<NormalizedEvent>> {
    let text = text_reply(
        format!("message-{episode:04}-terminal"),
        format!("answer-{episode:04}"),
    );
    if !tool_turn {
        return vec![text];
    }

    let call_id = format!("call-{episode:04}");
    vec![
        vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: MessageId(format!("message-{episode:04}-tool")),
                    content: vec![ContentBlock::ToolCall(ToolCall {
                        thought_signature: None,
                        tool_call_id: ToolCallId(call_id.clone()),
                        tool_name: "lookup".into(),
                        input: json!({"query": "fixed"}),
                    })],
                },
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage {
                    input_tokens: Some(11),
                    output_tokens: Some(7),
                    ..Usage::default()
                },
                finish_reason: FinishReason::ToolCalls,
                retries_used: 0,
            }),
        ],
        text,
    ]
}

fn text_reply(message_id: String, text: String) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(message_id),
                content: vec![ContentBlock::Text { text }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(11),
                output_tokens: Some(7),
                ..Usage::default()
            },
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ]
}

fn message(role: Role, text: String) -> Message {
    Message {
        cache_prefix_blocks: None,
        origin: None,
        ordinal: None,
        tags: Default::default(),
        role,
        content: vec![ContentBlock::Text { text }],
    }
}

/// The fastest sample, which is the statistic these bounds are actually about.
///
/// Every operation here runs in hundreds of microseconds, and the host interrupts one
/// sample in roughly six with a 5x-to-25x stall -- scheduling, page cache, SQLite
/// checkpointing. Those stalls are one-directional: they can only make a sample slower,
/// never faster. A median over few samples inherits them, and the measured shape swung
/// 10.6x run to run on unchanged code, which failed the 3x growth bound about one run in
/// four. The same runs varied 1.4x by minimum.
///
/// The bounds assert an asymptotic SHAPE -- that projecting one turn does not scale with
/// lineage length -- so the question is what the work costs when it is allowed to run,
/// not what the host did to one sample. Taking the minimum answers that question and
/// discards a noise source that cannot mask a real regression: code that genuinely got
/// slower is slower in its best sample too.
fn best(samples: Vec<Duration>) -> Duration {
    samples
        .into_iter()
        .min()
        .expect("at least one sample is measured")
}

fn print_table(rows: &[Row]) {
    println!("| N episodes | resume prepared | resume end-to-end | full replay | session.read newest | project one turn | live attach seed |");
    println!("|---:|---:|---:|---:|---:|---:|---:|");
    for row in rows {
        println!(
            "| {} | {} | {} | {} | {} | {} | {} |",
            row.episodes,
            micros(row.resume_prepared),
            micros(row.resume_end_to_end),
            micros(row.full_replay),
            micros(row.session_read),
            micros(row.projection),
            micros(row.live_attach_seed),
        );
    }
}

fn micros(duration: Duration) -> String {
    format!("{:.3} µs", duration.as_secs_f64() * 1_000_000.0)
}

fn assert_shape(rows: &[Row]) {
    let small = rows.first().expect("N=50 result");
    let large = rows.last().expect("N=800 result");
    // Prepared resume is measured against full replay rather than against its own growth:
    // its output is the whole provider prompt, so the merge does O(lineage) clone
    // and ordinal-stamp work by definition and cannot stay flat as the lineage
    // grows. This bound covers only the prepared merge; the separate end-to-end
    // column exposes production's remaining whole-WAL read and decode cost. The
    // fraction-of-full-replay bound is checked at EVERY N (not just the largest)
    // so a regression at small lineages cannot hide behind the ratio.
    let read_ratio = ratio(large.session_read, small.session_read);
    let projection_ratio = ratio(large.projection, small.projection);
    let live_attach_ratio = ratio(large.live_attach_seed, small.live_attach_seed);
    let replay_ratio = ratio(large.full_replay, small.full_replay);
    let worst_resume_fraction = rows
        .iter()
        .map(|row| row.resume_prepared.as_secs_f64() / row.full_replay.as_secs_f64())
        .fold(0.0_f64, f64::max);
    let checks = [
        (
            "prepared resume must stay <10% of full replay at every N",
            worst_resume_fraction,
            worst_resume_fraction < 0.10,
        ),
        (
            "session.read newest must be <3x",
            read_ratio,
            read_ratio < 3.0,
        ),
        (
            "single-turn projection must be <3x",
            projection_ratio,
            projection_ratio < 3.0,
        ),
        (
            "live attach seed must be <3x when file anchors are available",
            live_attach_ratio,
            cfg!(not(any(unix, windows))) || live_attach_ratio < 3.0,
        ),
        ("full replay must be >5x", replay_ratio, replay_ratio > 5.0),
    ];
    let failures: Vec<String> = checks
        .into_iter()
        .filter(|(_, _, passes)| !passes)
        .map(|(claim, measured, _)| format!("{claim}, measured {measured:.2}x"))
        .collect();
    assert!(
        failures.is_empty(),
        "Wave 2 exit shape failed:\n- {}",
        failures.join("\n- ")
    );
}

fn ratio(large: Duration, small: Duration) -> f64 {
    large.as_secs_f64() / small.as_secs_f64()
}
