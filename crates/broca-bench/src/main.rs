//! The provider-separation latency/jitter benchmark.
//!
//! Question: does putting the provider module in a SEPARATE process with a DIRECT
//! runner↔provider socket add non-trivial latency or jitter at realtime token rate
//! — especially for FAST providers?
//!
//! Arms, all driven by the SAME shared `broca-replay` schedule + clock:
//!  - `inproc`  (floor): producer → tokio mpsc → consumer. No serialize, no
//!    syscall. The theoretical floor.
//!  - `sock-raw` (codec+loopback): producer → `subc_transport::write_frame` over
//!    a loopback TCP conn → `read_frame` → consumer, raw body, ONE process.
//!  - `sock-json` (codec+loopback+serde): same, but the body is a realistic
//!    `DisplayEvent::TextDelta` serialized as JSON — the TRUE per-delta cost a
//!    real token incurs.
//!  - `xproc`  (codec+loopback+PROCESS): the provider binary (`--bench-replay`)
//!    streams the identical raw schedule across a REAL subc-transport handshake
//!    to this process. Two genuine OS processes.
//!
//! Decomposition: `sock-raw − inproc` = codec/loopback; `xproc − sock-raw` =
//! process separation (the (A) answer); `sock-json − sock-raw` = serde cost.
//!
//! Timing: every delta carries a `CLOCK_MONOTONIC` emit timestamp (system-wide,
//! cross-process comparable); the consumer stamps receive time on the SAME
//! clock, so one-way latency = recv − emit, valid even across processes.
#![forbid(unsafe_code)]

mod stats;

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use broca_protocol::{DisplayEvent, MessageId, RunId, StepId};
use broca_replay::{
    build_delta_frame, decode_emit_ns, encode_delta, monotonic_ns, pace, BodyEnvelope, Schedule,
};
use serde::{Deserialize, Serialize};
use subc_protocol::{Flags, Frame, FrameType, Priority};
use subc_transport::{authenticate_client, connection_file, read_frame, write_frame};
use tokio::{
    net::{TcpListener, TcpStream},
    process::Command,
    sync::mpsc,
};

use crate::stats::{bucket_means, jitter, LatencyStats};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// One delivered delta: one-way latency and the receive instant.
struct Sample {
    latency_ns: u64,
    recv_ns: u64,
}

struct ArmResult {
    lat: LatencyStats,
    jitter_sd_ns: f64,
    drift_ns: f64,
    throughput_per_s: f64,
    /// Mean latency per arrival-order decile (ns). A spike in decile 0 that
    /// drains to a low plateau = a startup-backlog transient, not a steady cost.
    decile_profile_ns: [f64; 10],
    /// p99 over the STEADY-STATE window (samples after the first decile, ns) — if
    /// this is clean while the full p99 is not, the tail is purely startup.
    steady_p99_ns: u64,
}

// ───────────────────────── body codecs ─────────────────────────

/// A realistic provider→runner display delta, JSON-serialized, carrying the
/// monotonic emit timestamp for the bench. The serialized body is the real wire
/// body (`BodyEnvelope<DisplayEvent>`) plus one `emit_ns` u64 — so the measured
/// serde cost is the true per-delta cost to within one integer field.
#[derive(Serialize, Deserialize)]
struct JsonReplay {
    emit_ns: u64,
    body: BodyEnvelope<DisplayEvent>,
}

#[derive(Clone, Copy)]
enum BodyCodec {
    Raw,
    Json,
}

impl BodyCodec {
    fn build(self, emit: u64, seq: u64, last: bool, payload: usize) -> Frame {
        match self {
            BodyCodec::Raw => build_delta_frame(emit, seq, last, payload),
            BodyCodec::Json => {
                // Representative coalesced text chunk (~22 chars).
                let ev = DisplayEvent::TextDelta {
                    message_id: MessageId("msg_01HQX7Z9".into()),
                    tool_call_id: None,
                    delta: "the quick brown fox ju".into(),
                };
                let body = BodyEnvelope::new(RunId("run_bench".into()), StepId(0), seq, ev);
                let bytes = serde_json::to_vec(&JsonReplay {
                    emit_ns: emit,
                    body,
                })
                .expect("serialize json replay");
                let flags = Flags::new(false, Priority::Background, last);
                Frame::build(FrameType::StreamData, flags, 1, 1, seq, bytes).expect("json frame")
            }
        }
    }

    fn decode_emit(self, body: &[u8]) -> u64 {
        match self {
            BodyCodec::Raw => decode_emit_ns(body),
            BodyCodec::Json => {
                serde_json::from_slice::<JsonReplay>(body)
                    .expect("deserialize json replay")
                    .emit_ns
            }
        }
    }

    fn body_bytes(self, payload: usize) -> usize {
        self.build(0, 0, false, payload).body.len()
    }
}

// ───────────────────────── arms ─────────────────────────

async fn run_inproc(count: usize, payload: usize, sched: Schedule) -> Vec<Sample> {
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(1024);
    let producer = tokio::spawn(async move {
        let base = monotonic_ns();
        for seq in 0..count as u64 {
            pace(sched, base, seq);
            let emit = monotonic_ns();
            if tx.send(encode_delta(emit, seq, payload)).await.is_err() {
                break;
            }
        }
    });
    let mut samples = Vec::with_capacity(count);
    while let Some(body) = rx.recv().await {
        let recv = monotonic_ns();
        let emit = decode_emit_ns(&body);
        samples.push(Sample {
            latency_ns: recv.saturating_sub(emit),
            recv_ns: recv,
        });
    }
    let _ = producer.await;
    samples
}

async fn run_socket(
    count: usize,
    payload: usize,
    sched: Schedule,
    codec: BodyCodec,
) -> std::io::Result<Vec<Sample>> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let addr = listener.local_addr()?;
    let producer = tokio::spawn(async move {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let (_r, mut w) = stream.into_split();
        let base = monotonic_ns();
        for seq in 0..count as u64 {
            pace(sched, base, seq);
            let emit = monotonic_ns();
            let frame = codec.build(emit, seq, seq + 1 == count as u64, payload);
            if write_frame(&mut w, &frame).await.is_err() {
                break;
            }
        }
        Ok::<(), std::io::Error>(())
    });

    let (server, _peer) = listener.accept().await?;
    server.set_nodelay(true)?;
    let (mut r, _w) = server.into_split();
    let mut samples = Vec::with_capacity(count);
    for _ in 0..count {
        match read_frame(&mut r).await {
            Ok(Some(frame)) => {
                let recv = monotonic_ns();
                let emit = codec.decode_emit(&frame.body);
                samples.push(Sample {
                    latency_ns: recv.saturating_sub(emit),
                    recv_ns: recv,
                });
            }
            Ok(None) | Err(_) => break,
        }
    }
    let _ = producer.await;
    Ok(samples)
}

/// arm3: spawn the provider binary and consume its stream across a real
/// subc-transport handshake. `None` if the provider binary isn't present.
async fn run_xproc(
    provider_bin: &Path,
    count: usize,
    payload: usize,
    sched: Schedule,
) -> Result<Option<Vec<Sample>>, BoxError> {
    if !provider_bin.exists() {
        return Ok(None);
    }
    let conn_path =
        std::env::temp_dir().join(format!("broca-bench-arm3-{}.json", std::process::id()));
    let _ = std::fs::remove_file(&conn_path);

    let (mode, cadence_us) = match sched {
        Schedule::Flood => ("flood", 0u64),
        Schedule::Fixed { cadence_ns } => ("fixed", cadence_ns / 1000),
    };
    let mut child = Command::new(provider_bin)
        .arg("--bench-replay")
        .arg("--connection-file")
        .arg(&conn_path)
        .arg("--mode")
        .arg(mode)
        .arg("--cadence-us")
        .arg(cadence_us.to_string())
        .arg("--count")
        .arg(count.to_string())
        .arg("--payload")
        .arg(payload.to_string())
        .spawn()?;

    // Wait for the provider to publish its connection-file (written before accept).
    let conn = {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(c) = connection_file::read(&conn_path) {
                break c;
            }
            if std::time::Instant::now() > deadline {
                let _ = child.kill().await;
                return Err(
                    "timed out waiting for provider connection-file (built with --features bench?)"
                        .into(),
                );
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    };

    let ep = conn
        .endpoints
        .first()
        .ok_or("provider connection-file has no endpoint")?;
    let mut stream = TcpStream::connect((ep.host.as_str(), ep.port)).await?;
    stream.set_nodelay(true)?;
    authenticate_client(&mut stream, &conn, Duration::from_secs(5)).await?;

    let mut samples = Vec::with_capacity(count);
    for _ in 0..count {
        match read_frame(&mut stream).await {
            Ok(Some(frame)) => {
                let recv = monotonic_ns();
                let emit = decode_emit_ns(&frame.body);
                samples.push(Sample {
                    latency_ns: recv.saturating_sub(emit),
                    recv_ns: recv,
                });
            }
            Ok(None) | Err(_) => break,
        }
    }
    let _ = child.wait().await;
    let _ = std::fs::remove_file(&conn_path);
    Ok(Some(samples))
}

// ───────────────────────── reduce + report ─────────────────────────

fn summarize(samples: &[Sample], sched: Schedule, warmup: usize) -> ArmResult {
    let warm = warmup.min(samples.len().saturating_sub(2));
    let kept = &samples[warm..];
    let lat: Vec<u64> = kept.iter().map(|s| s.latency_ns).collect();
    let gaps: Vec<u64> = kept
        .windows(2)
        .map(|w| w[1].recv_ns.saturating_sub(w[0].recv_ns))
        .collect();
    let target_ns = match sched {
        Schedule::Fixed { cadence_ns } => cadence_ns,
        Schedule::Flood => {
            if gaps.is_empty() {
                0
            } else {
                (gaps.iter().sum::<u64>() / gaps.len() as u64).max(1)
            }
        }
    };
    let (sd, _mad) = jitter(&gaps, target_ns);
    let b = bucket_means(&lat, 10);
    let drift_ns = b.last().copied().unwrap_or(0.0) - b.first().copied().unwrap_or(0.0);
    let mut decile_profile_ns = [0.0f64; 10];
    for (i, v) in b.iter().take(10).enumerate() {
        decile_profile_ns[i] = *v;
    }
    // Steady-state p99: drop the first decile (the startup window) and re-percentile.
    let first_decile = (lat.len() / 10).max(1);
    let steady_p99_ns = if lat.len() > first_decile {
        LatencyStats::from_samples(&lat[first_decile..]).p99
    } else {
        LatencyStats::from_samples(&lat).p99
    };
    let span_ns = kept
        .last()
        .map(|l| l.recv_ns)
        .unwrap_or(0)
        .saturating_sub(kept.first().map(|f| f.recv_ns).unwrap_or(0));
    let throughput_per_s = if span_ns > 0 {
        (kept.len() as f64 - 1.0) / (span_ns as f64 / 1e9)
    } else {
        0.0
    };
    ArmResult {
        lat: LatencyStats::from_samples(&lat),
        jitter_sd_ns: sd,
        drift_ns,
        throughput_per_s,
        decile_profile_ns,
        steady_p99_ns,
    }
}

/// Per-arm metrics for one trial, in microseconds (+ throughput per-second).
#[derive(Clone, Copy)]
struct Metrics {
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
    jitter_sd: f64,
    drift: f64,
    throughput: f64,
    steady_p99: f64,
    decile_us: [f64; 10],
}

impl Metrics {
    fn from(r: &ArmResult) -> Self {
        let us = |ns: u64| ns as f64 / 1000.0;
        let mut decile_us = [0.0f64; 10];
        for (i, v) in r.decile_profile_ns.iter().enumerate() {
            decile_us[i] = v / 1000.0;
        }
        Metrics {
            p50: us(r.lat.p50),
            p95: us(r.lat.p95),
            p99: us(r.lat.p99),
            max: us(r.lat.max),
            jitter_sd: r.jitter_sd_ns / 1000.0,
            drift: r.drift_ns / 1000.0,
            throughput: r.throughput_per_s,
            steady_p99: us(r.steady_p99_ns),
            decile_us,
        }
    }
}

fn median(mut v: Vec<f64>) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

/// Median of each metric across trials; p99 also keeps its min/max spread.
struct ArmAgg {
    p50: f64,
    p95: f64,
    p99: f64,
    p99_min: f64,
    p99_max: f64,
    max: f64,
    jitter_sd: f64,
    drift: f64,
    throughput: f64,
    steady_p99: f64,
    decile_us: [f64; 10],
}

fn aggregate(trials: &[Metrics]) -> ArmAgg {
    let pick = |f: fn(&Metrics) -> f64| trials.iter().map(f).collect::<Vec<_>>();
    let p99s = pick(|m| m.p99);
    let mut decile_us = [0.0f64; 10];
    for (i, d) in decile_us.iter_mut().enumerate() {
        *d = median(trials.iter().map(|m| m.decile_us[i]).collect());
    }
    ArmAgg {
        p50: median(pick(|m| m.p50)),
        p95: median(pick(|m| m.p95)),
        p99: median(p99s.clone()),
        p99_min: p99s.iter().cloned().fold(f64::INFINITY, f64::min),
        p99_max: p99s.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
        max: median(pick(|m| m.max)),
        jitter_sd: median(pick(|m| m.jitter_sd)),
        drift: median(pick(|m| m.drift)),
        throughput: median(pick(|m| m.throughput)),
        steady_p99: median(pick(|m| m.steady_p99)),
        decile_us,
    }
}

fn report(name: &str, a: &ArmAgg) {
    println!(
        "  {name:<9} p50={:>6.1} p95={:>6.1} p99={:>6.1} [{:>5.1}..{:>6.1}] max={:>7.1} | jit_sd={:>5.1} drift={:>+6.1}  (us)",
        a.p50, a.p95, a.p99, a.p99_min, a.p99_max, a.max, a.jitter_sd, a.drift,
    );
}

/// Per-decile latency profile + steady-state p99 — the root-cause instrument for
/// the flood startup-backlog anomaly.
fn report_profile(name: &str, a: &ArmAgg) {
    let cells: Vec<String> = a.decile_us.iter().map(|v| format!("{v:>6.1}")).collect();
    println!(
        "  {name:<9} steady_p99={:>6.1}us | deciles(us): {}",
        a.steady_p99,
        cells.join(" "),
    );
}

async fn run_case(
    label: &str,
    count: usize,
    payload: usize,
    sched: Schedule,
    warmup: usize,
    trials: usize,
    provider_bin: &Path,
) -> Result<(), BoxError> {
    // Interleave: each trial runs all arms back-to-back, so a transient system
    // load episode hits every arm similarly rather than biasing one. Median over
    // trials makes the µs-scale arm differences robust to the occasional OS
    // scheduler hiccup (which shows up identically in every arm's p99/max).
    let (mut t_in, mut t_raw, mut t_json, mut t_x) = (vec![], vec![], vec![], vec![]);
    let mut xproc_available = true;
    for _ in 0..trials {
        t_in.push(Metrics::from(&summarize(
            &run_inproc(count, payload, sched).await,
            sched,
            warmup,
        )));
        t_raw.push(Metrics::from(&summarize(
            &run_socket(count, payload, sched, BodyCodec::Raw).await?,
            sched,
            warmup,
        )));
        t_json.push(Metrics::from(&summarize(
            &run_socket(count, payload, sched, BodyCodec::Json).await?,
            sched,
            warmup,
        )));
        match run_xproc(provider_bin, count, payload, sched).await? {
            Some(s) => t_x.push(Metrics::from(&summarize(&s, sched, warmup))),
            None => xproc_available = false,
        }
    }

    let a_in = aggregate(&t_in);
    let a_raw = aggregate(&t_raw);
    let a_json = aggregate(&t_json);
    let a_x = if xproc_available && !t_x.is_empty() {
        Some(aggregate(&t_x))
    } else {
        None
    };

    println!("\n──────── {label} (n≈{count}, {trials} trials, median) ────────");
    report("inproc", &a_in);
    report("sock-raw", &a_raw);
    report("sock-json", &a_json);
    match &a_x {
        Some(ax) => report("xproc", ax),
        None => println!("  xproc     (skipped — build broca-provider --features bench)"),
    }

    if matches!(sched, Schedule::Flood) {
        // FLOOD latency is socket-buffer queue depth (a backpressure regime the
        // real lossy display lane sheds), NOT per-hop cost — so the per-hop
        // decomposition is meaningless here. The meaningful flood metric is the
        // throughput ceiling; the decile profile is the saturation diagnostic.
        println!(
            "  flood latency = backpressure queue depth, NOT hop cost. Meaningful metric = throughput ceiling:"
        );
        println!(
            "  throughput ceiling (median): sock-raw={:.0}/s sock-json={:.0}/s{}",
            a_raw.throughput,
            a_json.throughput,
            a_x.as_ref()
                .map(|ax| format!(" xproc={:.0}/s", ax.throughput))
                .unwrap_or_default(),
        );
        println!("  ── decile latency profile (saturation queue diagnostic) ──");
        report_profile("inproc", &a_in);
        report_profile("sock-raw", &a_raw);
        report_profile("sock-json", &a_json);
        if let Some(ax) = &a_x {
            report_profile("xproc", ax);
        }
    } else {
        // Per-hop decomposition at p50 AND p95 (steady-state hop cost). p99/max
        // are dominated by shared OS-scheduler hiccups (identical across arms),
        // the tail not the per-hop signal.
        let line = |stage: &str, get: fn(&ArmAgg) -> f64| {
            let proc = a_x
                .as_ref()
                .map(|ax| format!("{:+.1}us", get(ax) - get(&a_raw)))
                .unwrap_or_else(|| "n/a".into());
            println!(
                "  decomp {stage}: codec/loopback(raw−inproc)={:+.1}us | serde(json−raw)={:+.1}us | PROCESS(xproc−raw)={proc}",
                get(&a_raw) - get(&a_in),
                get(&a_json) - get(&a_raw),
            );
        };
        line("p50", |a| a.p50);
        line("p95", |a| a.p95);
    }
    Ok(())
}

struct Config {
    secs: u64,
    flood_count: usize,
    payload: usize,
    warmup: usize,
    trials: usize,
    rates: Vec<u64>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            secs: 5,
            flood_count: 300_000,
            payload: 48,
            warmup: 500,
            trials: 5,
            rates: vec![200, 500, 1000],
        }
    }
}

fn provider_bin_path() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.join("broca-provider")))
        .unwrap_or_else(|| PathBuf::from("broca-provider"))
}

fn parse_config() -> Config {
    let mut c = Config::default();
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--secs" => c.secs = args.next().and_then(|v| v.parse().ok()).unwrap_or(c.secs),
            "--flood-count" => {
                c.flood_count = args
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(c.flood_count)
            }
            "--payload" => {
                c.payload = args
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(c.payload)
            }
            "--warmup" => c.warmup = args.next().and_then(|v| v.parse().ok()).unwrap_or(c.warmup),
            "--trials" => {
                c.trials = args
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(c.trials)
                    .max(1)
            }
            "--rates" => {
                if let Some(v) = args.next() {
                    c.rates = v.split(',').filter_map(|s| s.parse().ok()).collect();
                }
            }
            other => eprintln!("broca-bench: ignoring unknown arg {other}"),
        }
    }
    c
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), BoxError> {
    let c = parse_config();
    let provider_bin = provider_bin_path();
    eprintln!(
        "broca-bench (A): rates={:?} tok/s (~{}s each) + flood({}) | payload={}B warmup={} trials={}\n  raw body={}B  json body={}B  provider={}",
        c.rates, c.secs, c.flood_count, c.payload, c.warmup, c.trials,
        BodyCodec::Raw.body_bytes(c.payload),
        BodyCodec::Json.body_bytes(c.payload),
        provider_bin.display(),
    );

    for &rate in &c.rates {
        let cadence = Duration::from_secs_f64(1.0 / rate as f64);
        let count = (rate * c.secs) as usize;
        let label = format!("{rate} tok/s ({}us cadence)", cadence.as_micros());
        let sched = Schedule::Fixed {
            cadence_ns: cadence.as_nanos() as u64,
        };
        run_case(
            &label,
            count,
            c.payload,
            sched,
            c.warmup,
            c.trials,
            &provider_bin,
        )
        .await?;
    }
    run_case(
        "FLOOD (zero-delay)",
        c.flood_count,
        c.payload,
        Schedule::Flood,
        c.warmup,
        c.trials,
        &provider_bin,
    )
    .await?;

    println!(
        "\n=== (A) verdict guide ===\n\
         HOLDS iff at the highest fixed rate: PROCESS cost (xproc−sock-raw) p99 ≪ cadence,\n\
         xproc drift ≈ 0, AND realistic sock-json per-delta p99 ≪ cadence. Throughput ceiling ≫ token rate."
    );
    Ok(())
}
