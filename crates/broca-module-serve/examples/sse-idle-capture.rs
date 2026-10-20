//! Replay one recorded model step to its provider and log the response stream byte by byte.
//!
//! WHAT IT PROVES. When a step dies with `provider stream idle`, the WAL keeps only lengths and
//! digests of the response, so it cannot say whether the provider went silent or kept sending
//! something broca does not count as progress. This tool rebuilds the request body from the WAL
//! (the episode's `run_started` config and input, replayed and rendered through broca's own
//! replay and wire-family code, never a hand-written copy), refuses to send unless its SHA-256
//! equals the `request_body_sha256` the WAL recorded for that step, then sends it with broca's
//! HTTP client and applies NO idle timeout of its own (only a hard cap, default 30 minutes).
//! Every raw chunk is logged with a monotonic timestamp and size, every SSE record with its kind
//! (data / comment / blank / empty data / done / other field), size and, for JSON data, its shape
//! (top-level keys, `choices[0].delta` keys and string lengths, finish reason, usage). Each chunk
//! is also fed to the family's real stream normalizer, and the log marks whether that chunk moved
//! `progress_marker()` or emitted events: exactly the condition that resets broca's 300 s idle
//! deadline. The summary then compares the longest gap between raw chunks with the longest gap
//! between progress-counting chunks and lists what arrived inside it.
//!
//! RUN. Render and digest-check only (no network, no credential):
//!   cargo run -p broca-module-serve --example sse-idle-capture -- \
//!       --wal ~/.local/share/cortexkit/broca/wal/<id>.wal --out /tmp/capture-<id>
//! Add `--send` to fetch the credential and stream the request. `--run-id` picks the episode
//! (default: the last one), `--step` the step (default: the first step with attempt records),
//! `--subc` the daemon connection file, `--credential-id` overrides the WAL's frozen credential,
//! `--api-key-header` sends the secret in that header instead of `Authorization: Bearer`, and
//! `--hard-cap-secs` bounds the whole capture. `--max-output-tokens N` sends the same step with
//! a different output limit (after the WAL digest still matches, and only if the new body differs
//! in that one field). `--analyze <dir>` reads a stream recorded by `scripts/sse-recording-proxy.py`
//! from another client instead of sending (`--analyze-family openai_responses` for a client that
//! used the Responses API), so both clients' streams are measured by the same code. Output in `--out`: `request-body.json` (the
//! rendered request, which contains the prompt), `response.raw` (the response bytes as
//! received), `timeline.jsonl` (one line per chunk and per SSE record) and `summary.json`.
//!
//! CREDENTIAL. The secret comes from the credentials module over subc by the handle in broca's
//! `dev-tools-handles.json` under the config home (override: `BROCA_DEVTOOLS_HANDLES`).
//! The daemon's `vault-handles.json` is not used. Mint with `ck auth mint-handle --id <credential>`
//! (operator only). The WAL's credential id or `--credential-id` selects the handle.
//! It is attached as a request header
//! only; nothing written by this tool contains request headers, and every output line is checked
//! for the secret before it is written (a hit is replaced and reported).

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[path = "support/wal_render.rs"]
mod wal_render;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use broca_module_serve::vault_config::load_devtools_handle;
use broca_module_serve::CredentialId;
use broca_protocol::{CallOptions, StepId};
use broca_provider::wire::{AuthHandle, FrozenRenderConfig, StreamNormalizer};
use broca_wal::WalRecord;
use clap::Parser;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

/// The idle deadline broca's transport applies between progress-counting chunks. Reported in the
/// summary for comparison only; this tool never enforces it.
const BROCA_STREAM_IDLE_SECS: f64 = 300.0;

#[derive(Parser)]
#[command(about = "Replay one WAL-recorded model step and log its SSE stream with timestamps")]
struct Args {
    /// The session WAL file.
    #[arg(long)]
    wal: PathBuf,
    /// The episode's run id (default: the last episode in the WAL).
    #[arg(long)]
    run_id: Option<String>,
    /// The 1-based step within the episode (default: the first step with attempt records).
    #[arg(long)]
    step: Option<u64>,
    /// Directory for the capture files. Created if missing.
    #[arg(long)]
    out: PathBuf,
    /// Actually send the request. Without it the tool renders, checks the digest and stops.
    #[arg(long)]
    send: bool,
    /// The subc daemon connection file.
    #[arg(long)]
    subc: Option<PathBuf>,
    /// Credentials module id on subc.
    #[arg(long, default_value = "claustrum")]
    credentials_module: String,
    /// Credential id to read (default: the WAL's frozen `auth_selection.credential_id`).
    #[arg(long)]
    credential_id: Option<String>,
    /// Send the secret in this header instead of `Authorization: Bearer`.
    #[arg(long)]
    api_key_header: Option<String>,
    /// Hard cap on the whole capture, in seconds.
    #[arg(long, default_value_t = 1800)]
    hard_cap_secs: u64,
    /// Send the step with this output-token limit instead of the recorded one. The WAL digest
    /// is still checked first, and the new body must differ from the recorded one in that
    /// field only.
    #[arg(long)]
    max_output_tokens: Option<u32>,
    /// Analyze a stream recorded by `scripts/sse-recording-proxy.py` (its output directory)
    /// instead of sending anything. The WAL still supplies the wire family's normalizer.
    #[arg(long, conflicts_with = "send")]
    analyze: Option<PathBuf>,
    /// With `--analyze`: read the recording with this wire family's normalizer (its serialized
    /// name, e.g. `openai_responses`) instead of the WAL step's own family, for a recording
    /// made by a client that speaks a different API to the same provider.
    #[arg(long, requires = "analyze")]
    analyze_family: Option<String>,
}

fn lower_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn default_subc_path() -> PathBuf {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default();
    home.join(".local/share/cortexkit/run/subc-connection.json")
}

/// One attempt record the WAL holds for the chosen step.
struct RecordedAttempt {
    attempt: u32,
    request_body_len: u64,
    request_body_sha256: String,
    response_body_len: u64,
    outcome: String,
    ts_ms: u64,
}

/// Everything needed to send the step again, reconstructed from the WAL.
struct Reconstructed {
    run_id: String,
    step: u64,
    frozen: FrozenRenderConfig,
    opts: CallOptions,
    credential_id: Option<String>,
    body: Vec<u8>,
    attempts: Vec<RecordedAttempt>,
}

fn reconstruct(args: &Args) -> Result<Reconstructed, String> {
    let framed = broca_wal::replay_file(&args.wal, broca_wal::DEFAULT_MAX_READ_BYTES)
        .map_err(|e| format!("read WAL {}: {e}", args.wal.display()))?;
    let records: Vec<WalRecord> = framed.into_iter().map(|f| f.record).collect();

    // Episode bounds: [start, end) where end is the next RunStarted or the end of the WAL.
    let starts: Vec<(usize, String)> = records
        .iter()
        .enumerate()
        .filter_map(|(i, r)| match r {
            WalRecord::RunStarted { run_id, .. } => Some((i, run_id.0.clone())),
            _ => None,
        })
        .collect();
    let pos = match &args.run_id {
        Some(want) => starts
            .iter()
            .position(|(_, id)| id == want)
            .ok_or_else(|| format!("no run_started for run id {want} in the WAL"))?,
        None => starts
            .len()
            .checked_sub(1)
            .ok_or("the WAL has no run_started record")?,
    };
    let (start, run_id) = starts[pos].clone();
    let end = starts.get(pos + 1).map_or(records.len(), |(i, _)| *i);
    let episode = &records[start..end];

    let WalRecord::RunStarted { config, .. } = &records[start] else {
        unreachable!("episode start is a run_started record");
    };

    let step = match args.step {
        Some(step) => step,
        None => episode
            .iter()
            .find_map(|r| match r {
                WalRecord::ModelAttemptFinished { step_id, .. } => Some(step_id.0),
                _ => None,
            })
            .ok_or("the episode has no model_attempt_finished record; pass --step")?,
    };
    let step_started = episode
        .iter()
        .position(|r| matches!(r, WalRecord::StepStarted { step_id } if step_id.0 == step))
        .ok_or_else(|| format!("no step_started for step {step} in episode {run_id}"))?;
    let attempts: Vec<RecordedAttempt> = episode
        .iter()
        .filter_map(|r| match r {
            WalRecord::ModelAttemptFinished {
                step_id,
                attempt,
                request_body_len,
                request_body_sha256,
                response_body_len,
                outcome,
                ts_ms,
                ..
            } if step_id.0 == step => Some(RecordedAttempt {
                attempt: *attempt,
                request_body_len: *request_body_len,
                request_body_sha256: request_body_sha256.clone(),
                response_body_len: *response_body_len,
                outcome: serde_json::to_value(outcome)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_string))
                    .unwrap_or_else(|| format!("{outcome:?}")),
                ts_ms: *ts_ms,
            }),
            _ => None,
        })
        .collect();

    // The prompt the loop rendered for this step is the replayed state of every record written
    // before the step opened: earlier episodes, this episode's input and its committed turns.
    let (frozen, opts, body) =
        wal_render::render_step(&records[..start + step_started], config, &run_id, step)?;

    Ok(Reconstructed {
        run_id,
        step,
        frozen,
        opts,
        credential_id: config
            .auth_selection
            .as_ref()
            .map(|a| a.credential_id.clone()),
        body,
        attempts,
    })
}

async fn fetch_secret(args: &Args, credential_id: &str) -> Result<String, String> {
    let id = CredentialId::parse(credential_id)
        .ok_or_else(|| format!("'{credential_id}' is not a credential id"))?;
    let handle = load_devtools_handle(&id)?;
    let subc = args.subc.clone().unwrap_or_else(default_subc_path);
    let conn = broca_subc::SubcConnection::connect(&subc)
        .await
        .map_err(|e| format!("connect subc ({}): {e}", subc.display()))?;
    // The vault authorizes by the credential handle, not by this identity, but subc still
    // rejects a bind identity whose project root does not exist, so use the current directory.
    let cwd = std::env::current_dir().map_err(|e| format!("current dir: {e}"))?;
    let identity = broca_protocol::BindIdentity::new(cwd, "sse-idle-capture", "sse-idle-capture");
    let client = broca_subc::VaultClient::new(
        Arc::new(conn),
        &args.credentials_module,
        &identity,
        Duration::from_secs(10),
    );
    client
        .get(&handle, None, false)
        .await
        .result
        .map(|token| token.token)
        .map_err(|e| format!("vault credential.get: {e}"))
}

/// Writes capture lines, refusing to let the credential reach disk.
struct Sink {
    file: std::fs::File,
    secret: Option<String>,
    redactions: u64,
}

impl Sink {
    fn create(path: &Path, secret: Option<String>) -> Result<Self, String> {
        let file =
            std::fs::File::create(path).map_err(|e| format!("create {}: {e}", path.display()))?;
        Ok(Self {
            file,
            secret,
            redactions: 0,
        })
    }

    fn line(&mut self, value: &Value) {
        let mut text = value.to_string();
        if let Some(secret) = self.secret.as_deref().filter(|s| s.len() >= 8) {
            if text.contains(secret) {
                text = text.replace(secret, "<REDACTED-CREDENTIAL>");
                self.redactions += 1;
            }
        }
        let _ = writeln!(self.file, "{text}");
    }
}

/// Splits raw bytes into SSE records incrementally. A record ends at a blank line; `\r\n`,
/// `\r` and `\n` line endings are all accepted, as the SSE specification requires.
#[derive(Default)]
struct SseSplitter {
    buf: Vec<u8>,
}

impl SseSplitter {
    fn push(&mut self, chunk: &[u8]) -> Vec<Vec<u8>> {
        self.buf.extend_from_slice(chunk);
        let mut out = Vec::new();
        loop {
            let normalized_end = find_record_end(&self.buf);
            let Some((record_len, consumed)) = normalized_end else {
                break;
            };
            let record = self.buf[..record_len].to_vec();
            self.buf.drain(..consumed);
            out.push(record);
        }
        out
    }
}

/// The end of the first complete record in `buf`: (record length without its terminator,
/// bytes to consume including the terminator). A lone `\r` at the very end of the buffer is not
/// treated as a line end yet, because a following `\n` may still arrive in the next chunk.
fn find_record_end(buf: &[u8]) -> Option<(usize, usize)> {
    let mut i = 0;
    let mut prev_line_empty = true; // at record start a line end means a blank record
    let mut line_start = 0;
    while i < buf.len() {
        let (is_eol, eol_len) = match buf[i] {
            b'\n' => (true, 1),
            b'\r' => {
                if i + 1 >= buf.len() {
                    return None;
                }
                (true, if buf[i + 1] == b'\n' { 2 } else { 1 })
            }
            _ => (false, 0),
        };
        if is_eol {
            let line_empty = i == line_start;
            if line_empty && (!prev_line_empty || line_start == 0) {
                return Some((line_start, i + eol_len));
            }
            prev_line_empty = line_empty;
            i += eol_len;
            line_start = i;
        } else {
            i += 1;
        }
    }
    None
}

/// Classify one SSE record and describe its shape.
fn classify(record: &[u8]) -> Value {
    let text = String::from_utf8_lossy(record);
    let mut data: Vec<&str> = Vec::new();
    let mut comments = 0usize;
    let mut other_fields: Vec<String> = Vec::new();
    for line in text.split(['\n', '\r']).filter(|l| !l.is_empty()) {
        if line.starts_with(':') {
            comments += 1;
            continue;
        }
        let (field, value) = match line.split_once(':') {
            Some((f, v)) => (f, v.strip_prefix(' ').unwrap_or(v)),
            None => (line, ""),
        };
        if field == "data" {
            data.push(value);
        } else {
            other_fields.push(field.to_string());
        }
    }
    let mut out = Map::new();
    out.insert("len".into(), json!(record.len()));
    if comments > 0 {
        out.insert("comment_lines".into(), json!(comments));
        if data.is_empty() {
            // A comment's text is provider keep-alive chatter, never prompt content.
            out.insert(
                "comment_text".into(),
                json!(text.chars().take(80).collect::<String>()),
            );
        }
    }
    if !other_fields.is_empty() {
        out.insert("fields".into(), json!(other_fields));
    }
    let kind = if !data.is_empty() {
        let payload = data.join("\n");
        if payload.is_empty() {
            "empty_data"
        } else if payload == "[DONE]" {
            "done"
        } else {
            out.insert("shape".into(), data_shape(&payload));
            "data"
        }
    } else if comments > 0 && other_fields.is_empty() {
        "comment"
    } else if other_fields.is_empty() {
        "blank"
    } else {
        "other_field"
    };
    out.insert("kind".into(), json!(kind));
    Value::Object(out)
}

/// The JSON shape of one data payload: which keys are present and how much text rides in each
/// delta field, without copying any of the text.
fn data_shape(payload: &str) -> Value {
    let Ok(value) = serde_json::from_str::<Value>(payload) else {
        return json!({"json": false});
    };
    let Some(obj) = value.as_object() else {
        return json!({"json": true, "top": "non-object"});
    };
    let mut shape = Map::new();
    let mut top: Vec<&String> = obj.keys().collect();
    top.sort();
    shape.insert("top".into(), json!(top));
    // Responses-style streams name each event in a `type` field and carry text in `delta`.
    if let Some(kind) = obj.get("type").and_then(Value::as_str) {
        shape.insert("type".into(), json!(kind));
        if let Some(delta) = obj.get("delta").and_then(Value::as_str) {
            shape.insert("delta_len".into(), json!(delta.len()));
        }
        if let Some(usage) = obj
            .get("response")
            .and_then(|r| r.get("usage"))
            .filter(|u| !u.is_null())
        {
            shape.insert("usage".into(), usage.clone());
        }
        if let Some(status) = obj.get("response").and_then(|r| r.get("status")) {
            shape.insert("response_status".into(), status.clone());
        }
    }
    if let Some(choice) = obj.get("choices").and_then(|c| c.get(0)) {
        if let Some(delta) = choice.get("delta").and_then(Value::as_object) {
            let mut keys = Map::new();
            for (k, v) in delta {
                let size = match v {
                    Value::String(s) => json!(s.len()),
                    Value::Null => json!("null"),
                    Value::Array(a) => json!(format!("array[{}]", a.len())),
                    other => json!(other.to_string().len()),
                };
                keys.insert(k.clone(), size);
            }
            shape.insert("delta".into(), Value::Object(keys));
        } else {
            shape.insert("delta".into(), json!("absent"));
        }
        if let Some(reason) = choice.get("finish_reason").filter(|v| !v.is_null()) {
            shape.insert("finish_reason".into(), reason.clone());
        }
    } else if obj.contains_key("choices") {
        shape.insert("choices".into(), json!("empty"));
    }
    if let Some(usage) = obj.get("usage").filter(|v| !v.is_null()) {
        shape.insert("usage".into(), usage.clone());
    }
    if let Some(error) = obj.get("error") {
        shape.insert("error".into(), error.clone());
    }
    Value::Object(shape)
}

/// A short label for a data record, used to tally what arrived during a gap.
fn record_label(rec: &Value) -> String {
    let kind = rec["kind"].as_str().unwrap_or("?");
    if kind != "data" {
        return kind.to_string();
    }
    let shape = &rec["shape"];
    if let Some(kind) = shape.get("type").and_then(Value::as_str) {
        let empty = shape.get("delta_len").and_then(Value::as_u64) == Some(0);
        return format!("data:{kind}{}", if empty { "(empty)" } else { "" });
    }
    match shape.get("delta").and_then(Value::as_object) {
        Some(delta) => {
            let mut nonempty: Vec<&str> = delta
                .iter()
                .filter(|(_, v)| {
                    v.as_u64().is_some_and(|n| n > 0)
                        || v.as_str().is_some_and(|s| s != "null" && s != "array[0]")
                })
                .map(|(k, _)| k.as_str())
                .collect();
            nonempty.sort();
            if nonempty.is_empty() {
                "data:empty_delta".to_string()
            } else {
                format!("data:delta[{}]", nonempty.join(","))
            }
        }
        None if shape.get("usage").is_some() => "data:usage_only".to_string(),
        None => "data:other".to_string(),
    }
}

struct Gap {
    secs: f64,
    from_s: f64,
    to_s: f64,
}

fn longest_gap(times: &[f64]) -> Option<Gap> {
    times
        .windows(2)
        .map(|w| Gap {
            secs: w[1] - w[0],
            from_s: w[0],
            to_s: w[1],
        })
        .max_by(|a, b| a.secs.total_cmp(&b.secs))
}

fn gap_json(gap: &Option<Gap>) -> Value {
    match gap {
        Some(g) => json!({"secs": g.secs, "from_s": g.from_s, "to_s": g.to_s}),
        None => Value::Null,
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    if let Err(error) = run(Args::parse()).await {
        eprintln!("sse-idle-capture: {error}");
        std::process::exit(1);
    }
}

/// Accumulates one response stream: logs each chunk and SSE record as it arrives, feeds the
/// chunk to the family's real normalizer, and turns the result into the gap summary. Shared by
/// the live `--send` path and the offline `--analyze` path, so both read a stream the same way.
struct Recorder {
    timeline: Sink,
    normalizer: Box<dyn StreamNormalizer>,
    splitter: SseSplitter,
    chunk_times: Vec<f64>,
    chunk_lens: Vec<usize>,
    /// Times at which broca's idle deadline would have been reset. Starts at 0.0 because the
    /// transport arms the deadline when the request is sent.
    progress_times: Vec<f64>,
    records: Vec<(f64, Value)>,
    total_bytes: usize,
    marker: u64,
    normalizer_error: Option<String>,
    events_total: usize,
}

impl Recorder {
    fn new(timeline: Sink, normalizer: Box<dyn StreamNormalizer>) -> Self {
        let marker = normalizer.progress_marker();
        Self {
            timeline,
            normalizer,
            splitter: SseSplitter::default(),
            chunk_times: Vec::new(),
            chunk_lens: Vec::new(),
            progress_times: vec![0.0],
            records: Vec::new(),
            total_bytes: 0,
            marker,
            normalizer_error: None,
            events_total: 0,
        }
    }

    fn chunk(&mut self, t: f64, chunk: &[u8]) {
        self.total_bytes += chunk.len();
        self.chunk_times.push(t);
        self.chunk_lens.push(chunk.len());
        // Same test as broca's transport: the deadline resets when the normalizer's progress
        // marker moves or the chunk completed at least one event.
        let (progress, events) = if self.normalizer_error.is_none() {
            match self.normalizer.push(chunk) {
                Ok(events) => {
                    let next = self.normalizer.progress_marker();
                    let moved = next != self.marker || !events.is_empty();
                    self.marker = next;
                    (moved, events.len())
                }
                Err(e) => {
                    self.normalizer_error = Some(format!("{e:?}"));
                    (false, 0)
                }
            }
        } else {
            (false, 0)
        };
        self.events_total += events;
        if progress {
            self.progress_times.push(t);
        }
        self.timeline
            .line(&json!({"ev": "chunk", "t": t, "len": chunk.len(),
            "total": self.total_bytes, "broca_progress": progress, "events": events,
            "progress_marker": self.marker}));
        for record in self.splitter.push(chunk) {
            let mut described = classify(&record);
            described["ev"] = json!("record");
            described["t"] = json!(t);
            self.timeline.line(&described);
            self.records.push((t, described));
        }
    }

    /// Close the stream and build the summary. `clean_eof` runs the normalizer's end-of-body
    /// check, which is where broca decides whether the step completed.
    fn finish(
        mut self,
        end_t: f64,
        end_reason: &str,
        mut summary: Map<String, Value>,
    ) -> (Value, u64) {
        let finish = if end_reason == "eof" && self.normalizer_error.is_none() {
            match self.normalizer.finish() {
                Ok(events) => {
                    self.events_total += events.len();
                    Some(json!({"ok": true, "events": events.len()}))
                }
                Err(e) => Some(json!({"ok": false, "error": format!("{e:?}")})),
            }
        } else {
            None
        };
        if !self.splitter.buf.is_empty() {
            self.timeline
                .line(&json!({"ev": "trailing_partial_record", "t": end_t,
                "len": self.splitter.buf.len()}));
        }
        self.timeline
            .line(&json!({"ev": "end", "t": end_t, "reason": end_reason}));

        // Each series runs from request send to stream end, so a silent head or tail is a gap too.
        let mut progress_series = self.progress_times.clone();
        progress_series.push(end_t);
        let progress_gap = longest_gap(&progress_series);
        let chunk_series: Vec<f64> = std::iter::once(0.0)
            .chain(self.chunk_times.iter().copied())
            .chain(std::iter::once(end_t))
            .collect();
        let chunk_gap = longest_gap(&chunk_series);
        let data_series: Vec<f64> = std::iter::once(0.0)
            .chain(
                self.records
                    .iter()
                    .filter(|(_, r)| r["kind"] == "data")
                    .map(|(t, _)| *t),
            )
            .chain(std::iter::once(end_t))
            .collect();
        let data_gap = longest_gap(&data_series);

        let tally = |records: &mut dyn Iterator<Item = &(f64, Value)>| {
            let mut kinds: Map<String, Value> = Map::new();
            for (_, r) in records {
                let label = record_label(r);
                let n = kinds.get(&label).and_then(Value::as_u64).unwrap_or(0);
                kinds.insert(label, json!(n + 1));
            }
            kinds
        };
        let kinds = tally(&mut self.records.iter());
        // What arrived strictly inside the longest progress gap.
        let (in_gap, chunks_in_gap, bytes_in_gap) = match &progress_gap {
            Some(g) => {
                let inside = |t: f64| t > g.from_s && t < g.to_s;
                let records = tally(&mut self.records.iter().filter(|(t, _)| inside(*t)));
                let (chunks, bytes) = self
                    .chunk_times
                    .iter()
                    .zip(&self.chunk_lens)
                    .filter(|(t, _)| inside(**t))
                    .fold((0usize, 0usize), |(c, b), (_, len)| (c + 1, b + len));
                (records, chunks, bytes)
            }
            None => (Map::new(), 0, 0),
        };
        let over_idle: Vec<Value> = progress_series
            .windows(2)
            .filter(|w| w[1] - w[0] >= BROCA_STREAM_IDLE_SECS)
            .map(|w| json!({"from_s": w[0], "to_s": w[1], "secs": w[1] - w[0]}))
            .collect();
        // Reasoning and content text volume, from the delta string lengths.
        let mut delta_bytes: Map<String, Value> = Map::new();
        for (_, r) in &self.records {
            let shape = &r["shape"];
            if let (Some(kind), Some(n)) = (
                shape.get("type").and_then(Value::as_str),
                shape.get("delta_len").and_then(Value::as_u64),
            ) {
                let total = delta_bytes.get(kind).and_then(Value::as_u64).unwrap_or(0);
                delta_bytes.insert(kind.to_string(), json!(total + n));
            }
            if let Some(delta) = shape.get("delta").and_then(Value::as_object) {
                for (k, v) in delta {
                    if let Some(n) = v.as_u64() {
                        let total = delta_bytes.get(k).and_then(Value::as_u64).unwrap_or(0);
                        delta_bytes.insert(k.clone(), json!(total + n));
                    }
                }
            }
        }

        let last = |key: &str| {
            self.records
                .iter()
                .rev()
                .find_map(|(_, r)| r["shape"].get(key).cloned())
        };
        summary.insert("end_reason".into(), json!(end_reason));
        summary.insert("duration_s".into(), json!(end_t));
        summary.insert("response_bytes".into(), json!(self.total_bytes));
        summary.insert("chunks".into(), json!(self.chunk_times.len()));
        summary.insert("records".into(), json!(self.records.len()));
        summary.insert("record_kinds".into(), Value::Object(kinds));
        summary.insert("delta_text_bytes".into(), Value::Object(delta_bytes));
        summary.insert(
            "saw_done".into(),
            json!(self.records.iter().any(|(_, r)| r["kind"] == "done")),
        );
        summary.insert(
            "finish_reason".into(),
            last("finish_reason").unwrap_or(Value::Null),
        );
        summary.insert("usage".into(), last("usage").unwrap_or(Value::Null));
        summary.insert("error_record".into(), last("error").unwrap_or(Value::Null));
        summary.insert("normalizer_events".into(), json!(self.events_total));
        summary.insert("normalizer_error".into(), json!(self.normalizer_error));
        summary.insert("normalizer_finish".into(), json!(finish));
        summary.insert("first_chunk_s".into(), json!(self.chunk_times.first()));
        summary.insert("first_progress_s".into(), json!(self.progress_times.get(1)));
        summary.insert("longest_gap_raw_chunks".into(), gap_json(&chunk_gap));
        summary.insert("longest_gap_data_records".into(), gap_json(&data_gap));
        summary.insert("longest_gap_broca_progress".into(), gap_json(&progress_gap));
        summary.insert(
            "inside_longest_progress_gap".into(),
            json!({"chunks": chunks_in_gap, "bytes": bytes_in_gap, "records": in_gap}),
        );
        summary.insert(
            "progress_gaps_at_or_over_broca_idle_300s".into(),
            json!(over_idle),
        );
        let summary = Value::Object(summary);
        (summary, self.timeline.redactions)
    }
}

/// Render the same step with a different output-token limit, and prove the result differs from
/// the WAL-identical body in that one field only.
fn render_with_max_tokens(rec: &Reconstructed, max_tokens: u32) -> Result<Vec<u8>, String> {
    let mut frozen = rec.frozen.clone();
    frozen.max_output_tokens = max_tokens;
    if frozen.caller_max_output_tokens.is_some() {
        frozen.caller_max_output_tokens = Some(max_tokens);
    }
    let mut opts = rec.opts.clone();
    let mut generation = (*opts.generation).clone();
    if generation.max_output_tokens.is_some() {
        generation.max_output_tokens = Some(u64::from(max_tokens));
    }
    opts.generation = Arc::new(generation);
    let family = broca_provider::registry::wire_family(frozen.family_id)
        .ok_or("no wire family implementation")?;
    let body = family.render(&frozen, &opts);

    let key = frozen.token_limit_field.key();
    let parse = |bytes: &[u8]| serde_json::from_slice::<Value>(bytes).map_err(|e| e.to_string());
    let (mut original, mut changed) = (parse(&rec.body)?, parse(&body)?);
    if changed.get(key) != Some(&json!(max_tokens)) {
        return Err(format!(
            "the re-rendered body does not carry {key}={max_tokens}"
        ));
    }
    original.as_object_mut().map(|o| o.remove(key));
    changed.as_object_mut().map(|o| o.remove(key));
    if original != changed {
        return Err(format!(
            "the re-rendered body differs from the WAL body beyond {key}; refusing"
        ));
    }
    println!("override: {key}={max_tokens}; the body differs from the WAL-identical one in that field only");
    Ok(body)
}

async fn run(args: Args) -> Result<(), String> {
    std::fs::create_dir_all(&args.out)
        .map_err(|e| format!("create {}: {e}", args.out.display()))?;
    let rec = reconstruct(&args)?;
    let wal_digest = lower_hex(&Sha256::digest(&rec.body));
    println!("episode {} step {}", rec.run_id, rec.step);
    println!(
        "rendered body: {} bytes sha256 {wal_digest} (family {:?}, wire model {}, max_output_tokens {})",
        rec.body.len(),
        rec.frozen.family_id,
        rec.frozen.wire_model_id,
        rec.frozen.max_output_tokens
    );
    for a in &rec.attempts {
        println!(
            "  WAL attempt {}: request {} bytes sha256 {} -> {} response bytes, {} (ts_ms {})",
            a.attempt,
            a.request_body_len,
            a.request_body_sha256,
            a.response_body_len,
            a.outcome,
            a.ts_ms
        );
    }
    // The reconstruction must reproduce the recorded bytes even when an override is sent: an
    // override is only meaningful as "the request broca sent, with this one field changed".
    let matched = !rec.attempts.is_empty()
        && rec.attempts.iter().all(|a| {
            a.request_body_sha256 == wal_digest && a.request_body_len == rec.body.len() as u64
        });
    if !matched {
        return Err(
            "rendered body does NOT match the WAL digest; refusing to replay different bytes"
                .into(),
        );
    }
    println!("digest MATCH: the rendered body is byte-identical to what broca sent");

    let body = match args.max_output_tokens {
        Some(max_tokens) => render_with_max_tokens(&rec, max_tokens)?,
        None => rec.body.clone(),
    };
    let digest = lower_hex(&Sha256::digest(&body));
    std::fs::write(args.out.join("request-body.json"), &body)
        .map_err(|e| format!("write request body: {e}"))?;
    let mut summary = Map::new();
    summary.insert("run_id".into(), json!(rec.run_id));
    summary.insert("step".into(), json!(rec.step));
    summary.insert("wal_request_body_sha256".into(), json!(wal_digest));
    summary.insert("request_body_sha256".into(), json!(digest));
    summary.insert("request_body_len".into(), json!(body.len()));
    summary.insert(
        "max_output_tokens_override".into(),
        json!(args.max_output_tokens),
    );

    let family = broca_provider::registry::wire_family(rec.frozen.family_id)
        .ok_or("no wire family implementation")?;
    let normalizer = family.normalizer(&rec.frozen, StepId(rec.step));

    if let Some(dir) = &args.analyze {
        // The stream being analyzed was recorded for some other client's request; the WAL step is
        // used only to select and configure the stream normalizer.
        let mut summary = Map::new();
        summary.insert(
            "normalizer_from".into(),
            json!(format!("{} step {}", rec.run_id, rec.step)),
        );
        let normalizer = match &args.analyze_family {
            None => normalizer,
            Some(name) => {
                let mut frozen = rec.frozen.clone();
                frozen.family_id = serde_json::from_value(json!(name))
                    .map_err(|_| format!("unknown wire family '{name}'"))?;
                summary.insert("normalizer_family".into(), json!(name));
                broca_provider::registry::wire_family(frozen.family_id)
                    .ok_or("no wire family implementation")?
                    .normalizer(&frozen, StepId(rec.step))
            }
        };
        return analyze_recorded(dir, &args.out, normalizer, summary);
    }
    if !args.send {
        println!("render-only: pass --send to stream the request");
        return Ok(());
    }

    let credential_id = args
        .credential_id
        .clone()
        .or(rec.credential_id.clone())
        .ok_or("the WAL has no auth_selection; pass --credential-id")?;
    println!("credential: {credential_id} (via vault handle)");
    let secret = fetch_secret(&args, &credential_id).await?;
    let auth = match &args.api_key_header {
        Some(header) => AuthHandle::ApiKeyHeader {
            header: header.clone(),
            value: secret.clone(),
        },
        None => AuthHandle::Bearer(secret.clone()),
    };
    let request = family.endpoint(&rec.frozen, &auth, body);
    drop(auth);

    let timeline = Sink::create(&args.out.join("timeline.jsonl"), Some(secret.clone()))?;
    let mut recorder = Recorder::new(timeline, normalizer);
    let mut raw = std::fs::File::create(args.out.join("response.raw"))
        .map_err(|e| format!("create response.raw: {e}"))?;

    let client = broca_provider::build_provider_http_client();
    // The client that does not decode, so `response.raw` keeps the bytes exactly as they arrived.
    let mut builder = client.without_response_decoding().post(&request.url);
    for (name, value) in &request.headers {
        builder = builder.header(name, value);
    }
    let started_wall_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or_default();
    let t0 = Instant::now();
    let secs = move |at: Instant| at.duration_since(t0).as_secs_f64();
    recorder.timeline.line(
        &json!({"ev": "request_sent", "t": 0.0, "wall_ms": started_wall_ms,
        "url": request.url, "body_len": request.body.len(), "body_sha256": digest}),
    );
    println!("sending to {} ...", request.url);

    let mut end_reason = "eof";
    let mut status = 0u16;
    let capture = async {
        let mut response = builder
            .body(request.body.clone())
            .send()
            .await
            .map_err(|e| format!("send: {e}"))?;
        status = response.status().as_u16();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        recorder
            .timeline
            .line(&json!({"ev": "headers", "t": secs(Instant::now()),
            "status": status, "content_type": content_type}));
        println!("status {status} after {:.2}s", secs(Instant::now()));
        loop {
            match response.chunk().await {
                Ok(Some(chunk)) => {
                    let t = secs(Instant::now());
                    let _ = raw.write_all(&chunk);
                    recorder.chunk(t, &chunk);
                }
                Ok(None) => break,
                Err(e) => {
                    recorder.timeline.line(&json!({"ev": "chunk_error",
                        "t": secs(Instant::now()), "error": e.to_string()}));
                    end_reason = "chunk_error";
                    break;
                }
            }
        }
        Ok::<(), String>(())
    };
    match tokio::time::timeout(Duration::from_secs(args.hard_cap_secs), capture).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(e),
        Err(_) => end_reason = "hard_cap",
    }
    let end_t = secs(Instant::now());
    summary.insert("status".into(), json!(status));
    let (summary, redactions) = recorder.finish(end_t, end_reason, summary);
    let mut summary_sink = Sink::create(&args.out.join("summary.json"), Some(secret.clone()))?;
    summary_sink.line(&summary);
    let redactions = redactions + summary_sink.redactions;
    drop(secret);
    println!(
        "{}",
        serde_json::to_string_pretty(&summary).unwrap_or_default()
    );
    if redactions > 0 {
        println!("WARNING: {redactions} output line(s) contained the credential and were redacted");
    }
    Ok(())
}

/// Re-read a stream recorded elsewhere (by `scripts/sse-recording-proxy.py`) through the same
/// classifier and normalizer as a live capture. The recording is `events.jsonl` (one `chunk`
/// line per arrival with `t` and `len`, plus optional `headers` and `end` lines) and
/// `response.raw` (the chunk bytes concatenated in arrival order).
fn analyze_recorded(
    dir: &Path,
    out: &Path,
    normalizer: Box<dyn StreamNormalizer>,
    mut summary: Map<String, Value>,
) -> Result<(), String> {
    let events = std::fs::read_to_string(dir.join("events.jsonl"))
        .map_err(|e| format!("read {}/events.jsonl: {e}", dir.display()))?;
    let raw = std::fs::read(dir.join("response.raw"))
        .map_err(|e| format!("read {}/response.raw: {e}", dir.display()))?;
    let timeline = Sink::create(&out.join("timeline.jsonl"), None)?;
    let mut recorder = Recorder::new(timeline, normalizer);
    let mut offset = 0usize;
    let mut end_t = 0.0f64;
    let mut end_reason = "eof".to_string();
    for line in events.lines().filter(|l| !l.trim().is_empty()) {
        let ev: Value = serde_json::from_str(line).map_err(|e| format!("events.jsonl: {e}"))?;
        let t = ev["t"].as_f64().unwrap_or(0.0);
        end_t = end_t.max(t);
        match ev["ev"].as_str() {
            Some("chunk") => {
                let len = ev["len"].as_u64().unwrap_or(0) as usize;
                let chunk = raw
                    .get(offset..offset + len)
                    .ok_or("events.jsonl names more bytes than response.raw holds")?;
                offset += len;
                recorder.chunk(t, chunk);
            }
            Some("headers") => {
                summary.insert("status".into(), ev["status"].clone());
            }
            Some("end") => {
                end_reason = ev["reason"].as_str().unwrap_or("eof").to_string();
            }
            _ => {}
        }
    }
    if offset != raw.len() {
        return Err(format!(
            "response.raw holds {} bytes but events.jsonl accounts for {offset}",
            raw.len()
        ));
    }
    summary.insert(
        "analyzed_recording".into(),
        json!(dir.display().to_string()),
    );
    let (summary, _) = recorder.finish(end_t, &end_reason, summary);
    let mut sink = Sink::create(&out.join("summary.json"), None)?;
    sink.line(&summary);
    println!(
        "{}",
        serde_json::to_string_pretty(&summary).unwrap_or_default()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitter_handles_split_terminators_and_comments() {
        let mut s = SseSplitter::default();
        let mut got = s.push(b": ping\r");
        assert!(got.is_empty());
        got.extend(s.push(b"\n\r\ndata: {\"a\":1}\n"));
        got.extend(s.push(b"\ndata:\n\n\n"));
        let kinds: Vec<String> = got
            .iter()
            .map(|r| classify(r)["kind"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(kinds, ["comment", "data", "empty_data", "blank"]);
    }
}
