//! Bounded, content-free evidence of tool providers dumping JSON into model-visible text.
use std::collections::{BTreeMap, VecDeque};
use std::sync::{LazyLock, Mutex};

const CAP: usize = 20_000;
const HORIZON_MS: i64 = 604_800_000;

#[derive(Clone, Debug)]
pub struct Sample {
    pub session: String,
    pub call_key: Option<String>,
    pub text_bytes: usize,
}

#[derive(Clone, Debug)]
struct Observation {
    at_ms: i64,
    module_id: String,
    tool_name: String,
    sample: Sample,
}

#[derive(Clone, Debug)]
pub struct ToolTally {
    pub module_id: String,
    pub tool_name: String,
    pub count: u64,
    /// Distinct sessions those results reached, so a summary can say how widely it happened.
    pub sessions: u64,
    pub first_seen_ms: i64,
    pub last_seen_ms: i64,
    pub samples: Vec<Sample>,
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub since_ms: i64,
    pub truncated: bool,
    pub tools: Vec<ToolTally>,
}

pub struct Tally {
    since_ms: i64,
    state: Mutex<(VecDeque<Observation>, Option<i64>)>,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn bounded(id: &str) -> String {
    if id.len() <= 256 {
        return id.to_owned();
    }
    use sha2::{Digest, Sha256};
    format!("oversized:{:x}", Sha256::digest(id.as_bytes()))
}

impl Tally {
    pub fn new(since_ms: i64) -> Self {
        Self {
            since_ms,
            state: Mutex::new((VecDeque::new(), None)),
        }
    }

    pub fn record(
        &self,
        at_ms: i64,
        module_id: &str,
        tool_name: &str,
        session: &str,
        call_key: Option<&str>,
        text_bytes: usize,
    ) {
        let observation = Observation {
            at_ms,
            module_id: bounded(module_id),
            tool_name: bounded(tool_name),
            sample: Sample {
                session: bounded(session),
                call_key: call_key.map(bounded),
                text_bytes,
            },
        };
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let horizon = at_ms.saturating_sub(HORIZON_MS);
        while state.0.len() >= CAP || state.0.front().is_some_and(|old| old.at_ms < horizon) {
            if let Some(old) = state.0.pop_front() {
                state.1 = Some(state.1.unwrap_or(self.since_ms).max(old.at_ms));
            }
        }
        state.0.push_back(observation);
    }

    pub fn snapshot(&self, since: i64, now: i64) -> Result<Snapshot, String> {
        let state = self
            .state
            .lock()
            .map_err(|_| "tool-result diagnostic tally is poisoned")?;
        let mut tools = BTreeMap::<(String, String), ToolTally>::new();
        let mut sessions = BTreeMap::<(String, String), std::collections::BTreeSet<&str>>::new();
        for observation in state
            .0
            .iter()
            .filter(|o| o.at_ms >= since && o.at_ms <= now)
        {
            let key = (observation.module_id.clone(), observation.tool_name.clone());
            let seen = sessions.entry(key.clone()).or_default();
            seen.insert(&observation.sample.session);
            let distinct = seen.len() as u64;
            let tally = tools.entry(key).or_insert_with(|| ToolTally {
                module_id: observation.module_id.clone(),
                tool_name: observation.tool_name.clone(),
                count: 0,
                sessions: 0,
                first_seen_ms: observation.at_ms,
                last_seen_ms: observation.at_ms,
                samples: Vec::new(),
            });
            tally.count += 1;
            tally.sessions = distinct;
            tally.first_seen_ms = tally.first_seen_ms.min(observation.at_ms);
            tally.last_seen_ms = tally.last_seen_ms.max(observation.at_ms);
            if tally.samples.len() < 5 {
                tally.samples.push(observation.sample.clone());
            }
        }
        Ok(Snapshot {
            since_ms: self
                .since_ms
                .max(if state.1.is_some() {
                    state
                        .0
                        .front()
                        .map_or(now, |o| o.at_ms)
                        .max(state.1.unwrap_or(self.since_ms))
                } else {
                    self.since_ms
                })
                .max(now.saturating_sub(HORIZON_MS)),
            truncated: state.1.is_some_and(|at| at >= since),
            tools: tools.into_values().collect(),
        })
    }
}

pub static TOOL_RESULT_JSON_DUMPS: LazyLock<Tally> = LazyLock::new(|| Tally::new(now_ms()));

pub(crate) fn observe_inline_result(
    ctx: &broca_core::BatchCtx,
    call: &broca_protocol::ToolCall,
    module_id: &str,
    call_key: Option<&str>,
    result: &broca_protocol::ToolResult,
) {
    if let Some(text_bytes) =
        crate::content_v1::json_dump_output_text_bytes(&call.tool_name, &result.output)
    {
        TOOL_RESULT_JSON_DUMPS.record(
            now_ms(),
            module_id,
            &call.tool_name,
            &ctx.bind.session,
            call_key,
            text_bytes,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_result_json_dump_tally_groups_and_bounds_samples_without_losing_counts() {
        let tally = Tally::new(0);
        for at in 1..=7 {
            tally.record(at, "provider", "search", "session", Some("call-key"), 200);
        }
        tally.record(8, "other", "search", "session", None, 300);
        tally.record(9, "provider", "list", "session", None, 400);
        let snapshot = tally.snapshot(2, 8).unwrap();
        assert_eq!(snapshot.tools.len(), 2);
        let tool = snapshot
            .tools
            .iter()
            .find(|t| t.module_id == "provider")
            .unwrap();
        assert_eq!(tool.tool_name, "search");
        assert_eq!(
            (tool.count, tool.first_seen_ms, tool.last_seen_ms),
            (6, 2, 7)
        );
        assert_eq!(tool.samples.len(), 5);
        assert_eq!(tool.samples[0].call_key.as_deref(), Some("call-key"));
        assert_eq!(tool.samples[0].text_bytes, 200);
    }

    #[test]
    fn tool_result_json_dump_tally_bounds_history_and_identifiers() {
        let tally = Tally::new(0);
        let oversized = "x".repeat(257);
        for at in 1..=CAP as i64 + 1 {
            tally.record(
                at,
                &oversized,
                &oversized,
                &oversized,
                Some(&oversized),
                200,
            );
        }
        let snapshot = tally.snapshot(0, CAP as i64 + 1).unwrap();
        assert!(snapshot.truncated);
        assert_eq!(snapshot.since_ms, 2);
        assert_eq!(snapshot.tools[0].count, CAP as u64);
        let tool = &snapshot.tools[0];
        for id in [
            &tool.module_id,
            &tool.tool_name,
            &tool.samples[0].session,
            tool.samples[0].call_key.as_ref().unwrap(),
        ] {
            assert!(id.starts_with("oversized:"));
            assert!(id.len() < 256);
        }
        tally.record(
            HORIZON_MS + CAP as i64 + 2,
            "new",
            "tool",
            "session",
            None,
            200,
        );
        let snapshot = tally.snapshot(0, HORIZON_MS + CAP as i64 + 2).unwrap();
        assert_eq!(snapshot.tools.len(), 1);
        assert_eq!(snapshot.tools[0].module_id, "new");
        assert!(snapshot.truncated);
    }
}
