//! The kit's idle keep-warm fixture (llm-runner `cache_policy`, `keep_warm`). Each session runs
//! on a Messages catalog row of its own, priced for the case. Every request that reaches the
//! scripted provider for it is journaled here, outside broca. The daemon's test clock is moved
//! through each due warm in turn, so every warm is sent at its own due time.

use super::*;
use cortexkit_role_llm_runner_conformance::{
    KeepWarmPrices, KeepWarmRequest, KeepWarmRequests, ScriptedTurn,
};

/// Each keep-warm session gets its own catalog row, so the scripted provider knows a request's
/// session by its model: these plan sessions carry no system text to mark them.
const MODEL_PREFIX: &str = "conformance-keep-warm-";
const ROWS: usize = 8;
/// The cache prices every keep-warm row carries, per million tokens: read, then write.
const PRICES: (u64, u64) = (1, 3);
/// The lifetime broca asks Messages for under `ttl: short`.
pub(super) const LIFETIME_MS: u64 = 300_000;
/// Where a test-clock broca notes each session's idle warm decision, one JSON line each, beside
/// the clock's control file.
const DECISIONS: &str = "test-clock-warms.jsonl";
/// How long a clock move waits for broca to send a due warm and decide again.
const SETTLE: Duration = Duration::from_secs(60);

#[derive(Default)]
pub(super) struct Fixture {
    /// The model each keep-warm session runs on.
    models: HashMap<String, String>,
    /// Each keep-warm session's address in broca's session store, which names its decisions.
    addresses: HashMap<String, String>,
    journals: HashMap<String, Vec<Journaled>>,
    /// The fixed daemon time this fixture last set, once it has set one.
    now_ms: Option<u64>,
}

struct Journaled {
    request: KeepWarmRequest,
    /// How many decisions broca had noted for the session when the request was answered: broca
    /// decides again only after that answer.
    decisions_before: usize,
}

/// The catalog rows the keep-warm sessions run on: the Messages fixture row with cache prices,
/// and without reasoning. A reasoning row would freeze a thinking budget, which a warm can't
/// carry: it asks for no output, and Anthropic requires the budget below the output cap.
pub(super) fn catalog_rows() -> impl Iterator<Item = (String, Value)> {
    let rate = |per_million: u64| json!({"currency":"USD","exponent":9,"state":"priced","units":per_million * 1_000_000_000});
    (0..ROWS).map(move |index| {
        let mut row = output_refs::catalog_row();
        row["capability.reasoning"] = json!(false);
        row["rate.input"] = rate(1);
        row["rate.output"] = rate(1);
        row["rate.cache_read"] = rate(PRICES.0);
        row["rate.cache_write"] = rate(PRICES.1);
        (
            format!("{}/{MODEL_PREFIX}{index}", output_refs::PROVIDER),
            row,
        )
    })
}

pub(super) fn is_session(shared: &Shared, session: &str) -> bool {
    shared
        .keep_warm
        .lock()
        .unwrap()
        .models
        .contains_key(session)
}

pub(super) fn model_of(shared: &Shared, session: &str) -> Option<String> {
    shared
        .keep_warm
        .lock()
        .unwrap()
        .models
        .get(session)
        .cloned()
}

/// The keep-warm session a request for `model` belongs to.
pub(super) fn session_for_model(shared: &Shared, model: &str) -> Option<String> {
    let fixture = shared.keep_warm.lock().unwrap();
    fixture
        .models
        .iter()
        .find(|(_, row)| model.ends_with(row.as_str()))
        .map(|(session, _)| session.clone())
}

/// The scripted provider's answer to a keep-warm session's request, journaled first. A real
/// request streams its scripted turn. A warm (`stream` absent) gets the same scripted usage and
/// no content, without using a turn up.
pub(super) fn answer(
    shared: &Shared,
    session: &str,
    id: &str,
    model: &str,
    turn: &ScriptedTurn,
    request: &Value,
    raw: &[u8],
) -> String {
    let usage = turn
        .usage
        .clone()
        .unwrap_or_else(|| ScriptedUsage::new(1, 0, 0, 1));
    // What the cache keys on: the request without the members a warm may change, which only
    // cap the answer or say how it is returned.
    let mut prefix = request.clone();
    if let Some(members) = prefix.as_object_mut() {
        for member in [
            "max_tokens",
            "stream",
            "thinking",
            "output_config",
            "fallbacks",
        ] {
            members.remove(member);
        }
        // A Claude Code persona request opens its system text with a billing line whose `cch`
        // checksums the whole body, so it differs between a warm and the request it warms.
        // Anthropic reads that line for billing and leaves it out of the cached prefix.
        if let Some(system) = members.get_mut("system").and_then(Value::as_array_mut) {
            system.retain(|block| {
                !block["text"]
                    .as_str()
                    .is_some_and(|text| text.starts_with("x-anthropic-billing-header:"))
            });
        }
    }
    let root = shared.session_roots.lock().unwrap().get(session).cloned();
    let address = shared
        .keep_warm
        .lock()
        .unwrap()
        .addresses
        .get(session)
        .cloned();
    let decisions_before = root
        .clone()
        .zip(address)
        .map_or(0, |(root, address)| decisions(&root, &address).len());
    let mut fixture = shared.keep_warm.lock().unwrap();
    // broca stamps a real request's records with the wall clock, while it schedules warms on the
    // daemon's. Earlier cases leave the daemon's clock wherever they moved it, often hours ahead,
    // so a real request brings it back to the wall clock first: broca then decides its warm from
    // two clocks that agree, as outside tests. A warm keeps the time the fixture moved it to.
    let at_ms = match (request["stream"] == true, &root) {
        (true, Some(root)) => {
            let now = wall_ms();
            if let Err(error) = write_clock(root, now) {
                panic!("the keep-warm fixture could not set the daemon's clock: {error}");
            }
            fixture.now_ms = Some(now);
            // broca reads the clock file every 100 ms; answering after two reads makes it
            // decide this request's warm on the clock set here, not the one before.
            std::thread::sleep(Duration::from_millis(250));
            now
        }
        _ => fixture.now_ms.unwrap_or_else(wall_ms),
    };
    fixture
        .journals
        .entry(session.into())
        .or_default()
        .push(Journaled {
            request: KeepWarmRequest::new(
                at_ms,
                raw.to_vec(),
                serde_json::to_vec(&prefix).unwrap(),
                usage.clone(),
            ),
            decisions_before,
        });
    drop(fixture);
    let (body, kind) = if request["stream"] == true {
        (
            output_refs::stream_for_model(id, model, turn),
            "text/event-stream",
        )
    } else {
        (
            json!({"id":id,"type":"message","role":"assistant","model":model,"content":[],
                "stop_reason":"max_tokens","stop_sequence":null,
                "usage":{"input_tokens":usage.input_tokens,
                    "cache_read_input_tokens":usage.cache_read_tokens,
                    "cache_creation_input_tokens":usage.cache_write_tokens,
                    "output_tokens":usage.output_tokens}})
            .to_string(),
            "application/json",
        )
    };
    format!(
        "HTTP/1.1 200 OK\r\ncontent-type: {kind}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Every idle warm decision broca has noted for `session`, oldest first.
fn decisions(state_root: &Path, session_addr: &str) -> Vec<Value> {
    std::fs::read_to_string(state_root.join(DECISIONS))
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|decision| decision["session"] == session_addr)
        .collect()
}

impl BrocaSubject {
    /// Puts `session` on a keep-warm row and answers the prices that row carries in the daemon's
    /// catalog, read back from it. The rows are seeded with the state root, so prices other than
    /// [`PRICES`] can't be installed; the conformance kit refuses prices that differ from the
    /// ones it asked for.
    pub(super) fn configure_keep_warm(
        &self,
        live: &Live,
        session: &str,
    ) -> Result<Option<KeepWarmPrices>, HarnessError> {
        let address = broca_core::session_addr(&self.bind_identity(session));
        let model = {
            let mut fixture = self.shared.keep_warm.lock().unwrap();
            fixture.addresses.insert(session.into(), address);
            let next = fixture.models.len();
            if next >= ROWS && !fixture.models.contains_key(session) {
                return Err(HarnessError::new("every keep-warm catalog row is in use"));
            }
            fixture.journals.entry(session.into()).or_default();
            fixture
                .models
                .entry(session.into())
                .or_insert_with(|| format!("{MODEL_PREFIX}{next}"))
                .clone()
        };
        let catalog: Value = std::fs::read(live.state_root.join("catalog/fusiform-lkg.json"))
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .ok_or_else(|| HarnessError::new("the daemon's seeded catalog is unreadable"))?;
        let row = &catalog["models"][format!("{}/{model}", output_refs::PROVIDER)];
        let per_million = |fact: &str| {
            row[fact]["units"]
                .as_u64()
                .map(|units| units / 1_000_000_000)
                .ok_or_else(|| HarnessError::new(format!("the keep-warm row has no {fact}")))
        };
        Ok(Some(KeepWarmPrices::new(
            per_million("rate.cache_read")?,
            per_million("rate.cache_write")?,
        )))
    }

    pub(super) fn keep_warm_journal(&self, session: &str, cursor: u64) -> Option<KeepWarmRequests> {
        let fixture = self.shared.keep_warm.lock().unwrap();
        let journal = fixture.journals.get(session)?;
        let start = usize::try_from(cursor).ok()?.min(journal.len());
        Some(KeepWarmRequests::new(
            fixture.now_ms.unwrap_or_else(wall_ms),
            journal.len() as u64,
            journal[start..]
                .iter()
                .map(|entry| entry.request.clone())
                .collect(),
        ))
    }

    /// Moves the daemon's clock `by`, stopping at each keep-warm session's due warm in time
    /// order and waiting there until broca has sent it and decided again. The first call fixes
    /// the clock where the daemon reads it now. A fixed clock moves only when something writes
    /// the daemon's `test-clock` file: this fixture, a real keep-warm request ([`answer`]), or
    /// the kit's retention cases through `advance_retention_clock`.
    pub(super) async fn advance_keep_warm_clock(
        &self,
        live: &Live,
        by: Duration,
    ) -> Result<(), HarnessError> {
        let root = live.state_root.clone();
        let now = match self.shared.keep_warm.lock().unwrap().now_ms {
            Some(now) => now,
            None => read_clock(&root),
        };
        let end = now
            .checked_add(u64::try_from(by.as_millis()).unwrap_or(u64::MAX))
            .ok_or_else(|| HarnessError::new("keep-warm clock overflow"))?;
        self.set_clock(&root, now)?;
        loop {
            let sessions: Vec<(String, usize)> = {
                let fixture = self.shared.keep_warm.lock().unwrap();
                fixture
                    .journals
                    .iter()
                    .filter_map(|(session, journal)| {
                        Some((
                            fixture.addresses.get(session)?.clone(),
                            journal.last()?.decisions_before,
                        ))
                    })
                    .collect()
            };
            let mut next: Option<(u64, String)> = None;
            for (address, before) in sessions {
                let latest = settled(&root, &address, before).await?;
                if let Some(due) = latest["armed_due_ms"].as_u64().filter(|due| *due <= end) {
                    if next.as_ref().is_none_or(|(soonest, _)| due < *soonest) {
                        next = Some((due, address));
                    }
                }
            }
            let Some((due, address)) = next else { break };
            let seen = decisions(&root, &address).len();
            // A warm armed late can be due before the clock's reading; it fires at that reading.
            let current = self.shared.keep_warm.lock().unwrap().now_ms.unwrap_or(now);
            self.set_clock(&root, due.max(current))?;
            // The armed warm is now due: wait until broca sent it and decided again, or stopped.
            let deadline = Instant::now() + SETTLE;
            while decisions(&root, &address).len() <= seen {
                if Instant::now() >= deadline {
                    return Err(HarnessError::new(format!(
                        "broca did not send the warm due at {due} for {address}"
                    )));
                }
                sleep(Duration::from_millis(10)).await;
            }
        }
        self.set_clock(&root, end)
    }

    fn set_clock(&self, root: &Path, now: u64) -> Result<(), HarnessError> {
        // Journaled requests take their time from here, so set it before broca can act on it.
        self.shared.keep_warm.lock().unwrap().now_ms = Some(now);
        write_clock(root, now).map_err(|error| HarnessError::new(error.to_string()))
    }
}

/// Fixes the daemon's clock at `now`, replacing its control file in one rename.
fn write_clock(root: &Path, now: u64) -> std::io::Result<()> {
    let temp = root.join("test-clock.keep-warm.tmp");
    std::fs::write(&temp, json!({"now_ms":now,"offset_ms":0}).to_string())
        .and_then(|()| std::fs::rename(&temp, root.join("test-clock")))
}

/// The daemon's time as its control file sets it: fixed, offset from real time, or real time.
fn read_clock(root: &Path) -> u64 {
    let control: Value = std::fs::read(root.join("test-clock"))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or(json!({}));
    control["now_ms"]
        .as_u64()
        .unwrap_or_else(|| (wall_ms() as i64 + control["offset_ms"].as_i64().unwrap_or(0)) as u64)
}

/// The session's newest idle warm decision, once broca has made one since the session's newest
/// request was answered (`before` decisions were noted then).
async fn settled(root: &Path, address: &str, before: usize) -> Result<Value, HarnessError> {
    let deadline = Instant::now() + SETTLE;
    loop {
        let noted = decisions(root, address);
        if noted.len() > before {
            return Ok(noted.last().unwrap().clone());
        }
        if Instant::now() >= deadline {
            return Err(HarnessError::new(format!(
                "broca made no idle warm decision for {address} after its last request"
            )));
        }
        sleep(Duration::from_millis(10)).await;
    }
}
