//! Warn-once visibility for `session.send` params broca receives but does not act on.
//!
//! `SendParams` ignores unknown keys by design, which is how a caller's reasoning `variant`
//! went unread for months without anyone seeing it: serde dropped it and nothing said so. This
//! module reports what a send carries that broca does not use, without refusing the send.
//!
//! Recent distinct notices are deduplicated in a bounded FIFO cache. A caller that sends the
//! same unknown key on every turn would otherwise put one warning per send in the log, burying the signal
//! the warning exists to give.

use std::collections::{HashSet, VecDeque};
use std::sync::{Mutex, OnceLock};

use broca_wire::{model_params_keys, send_params_keys};
use serde_json::Value;

/// Keys handled on `session.send` apart from `SendParams`: a non-null `seed_from` is refused (a
/// `null` one is accepted and ignored), and `plan`, a session's fetch plan, is checked and
/// admitted before the send reaches the session. Neither is an unknown key.
const PRE_DECODE_SEND_KEYS: &[&str] = &["seed_from", "plan"];
const SEND_NOTICE_CACHE_ENTRIES: usize = 4096;

/// One thing worth a single warning. Equality is the dedupe key, so every field is part of it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SendNotice {
    /// A top-level `session.send` key that `SendParams` does not read.
    UnknownSendKey { key: String, harness: String },
    /// A key inside the send's `model` object that `ModelParams` does not read.
    UnknownModelKey { key: String, harness: String },
}

impl SendNotice {
    /// The warning text, naming the key and who sent it.
    pub fn message(&self) -> String {
        match self {
            Self::UnknownSendKey { key, harness } => format!(
                "session.send from harness '{harness}' carried unknown key '{key}'; it was \
                 ignored (repeated notices are deduplicated while cached)"
            ),
            Self::UnknownModelKey { key, harness } => format!(
                "session.send from harness '{harness}' carried unknown key 'model.{key}'; it \
                 was ignored (repeated notices are deduplicated while cached)"
            ),
        }
    }
}

/// Remembers a bounded set of recently reported notices; forgetting one only repeats a log line.
#[derive(Debug, Default)]
pub struct SendAuditor {
    seen: Mutex<NoticeCache>,
}

#[derive(Debug, Default)]
struct NoticeCache {
    entries: HashSet<SendNotice>,
    oldest: VecDeque<SendNotice>,
}

impl SendAuditor {
    /// The notices raised by one send's raw params that are not in the deduplication cache.
    /// Each returned notice is marked seen until FIFO eviction permits it to be reported again.
    pub fn first_seen(&self, params: &Value, harness: &str) -> Vec<SendNotice> {
        let candidates = notices_for(params, harness);
        let mut seen = self
            .seen
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        candidates
            .into_iter()
            .filter(|notice| {
                if seen.entries.contains(notice) {
                    return false;
                }
                if seen.entries.len() >= SEND_NOTICE_CACHE_ENTRIES {
                    let oldest = seen
                        .oldest
                        .pop_front()
                        .expect("full notice cache has an oldest key");
                    seen.entries.remove(&oldest);
                }
                seen.oldest.push_back(notice.clone());
                seen.entries.insert(notice.clone())
            })
            .collect()
    }
}

/// Every notice a send's raw params raise, before deduplication. Keys are reported in the
/// order the params object lists them.
fn notices_for(params: &Value, harness: &str) -> Vec<SendNotice> {
    let Some(object) = params.as_object() else {
        return Vec::new();
    };
    let mut notices = Vec::new();
    for key in object.keys() {
        if !send_params_keys().contains(&key.as_str())
            && !PRE_DECODE_SEND_KEYS.contains(&key.as_str())
        {
            notices.push(SendNotice::UnknownSendKey {
                key: key.clone(),
                harness: harness.to_string(),
            });
        }
    }
    if let Some(model) = object.get("model").and_then(Value::as_object) {
        for key in model.keys() {
            if !model_params_keys().contains(&key.as_str()) {
                notices.push(SendNotice::UnknownModelKey {
                    key: key.clone(),
                    harness: harness.to_string(),
                });
            }
        }
    }
    notices
}

/// The process-wide auditor `session.send` reports through.
pub fn global_send_auditor() -> &'static SendAuditor {
    static AUDITOR: OnceLock<SendAuditor> = OnceLock::new();
    AUDITOR.get_or_init(SendAuditor::default)
}

/// Report a send's first-seen notices at warn under the `wire` target (the fleet logger prefixes it as `broca.wire`).
pub fn warn_send_notices(auditor: &SendAuditor, params: &Value, harness: &str) {
    for notice in auditor.first_seen(params, harness) {
        tracing::warn!(target: "wire", "{}", notice.message());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn variant_send(variant: &str) -> Value {
        json!({
            "prompt": "hi",
            "model": {"provider": "openai", "model": "gpt-6-astra", "variant": variant},
            "reasoning": {"effort": "xhigh"}
        })
    }

    #[test]
    fn unknown_top_level_and_model_keys_each_warn_once_per_process() {
        let auditor = SendAuditor::default();
        let send = json!({
            "prompt": "hi",
            "model": {"provider": "openai", "model": "gpt-6-astra", "effort": "xhigh"},
            "reasoning": {"effort": "xhigh"}
        });
        assert_eq!(
            auditor.first_seen(&send, "prefrontal"),
            vec![
                SendNotice::UnknownSendKey {
                    key: "reasoning".into(),
                    harness: "prefrontal".into()
                },
                SendNotice::UnknownModelKey {
                    key: "effort".into(),
                    harness: "prefrontal".into()
                },
            ]
        );
        assert!(
            auditor.first_seen(&send, "prefrontal").is_empty(),
            "the same keys on a later send must not warn again"
        );
    }

    #[test]
    fn a_variant_is_read_so_it_is_never_an_unknown_key() {
        let auditor = SendAuditor::default();
        assert_eq!(
            auditor.first_seen(&variant_send("xhigh"), "prefrontal"),
            vec![SendNotice::UnknownSendKey {
                key: "reasoning".into(),
                harness: "prefrontal".into()
            }]
        );
    }

    #[test]
    fn a_send_with_only_known_keys_raises_nothing() {
        let auditor = SendAuditor::default();
        let send = json!({
            "prompt": "hi",
            "seed_from": null,
            "model": {"provider": "openai", "model": "gpt-6-astra"},
            "work_class": "mason",
            "service_tier": "priority",
            "append_episode": false
        });
        assert!(auditor.first_seen(&send, "prefrontal").is_empty());
    }

    #[test]
    fn notice_cache_evicts_oldest_and_rederives_the_same_notice() {
        let auditor = SendAuditor::default();
        let send = json!({"unknown-key": true});
        let first = auditor.first_seen(&send, "first");
        assert_eq!(first.len(), 1);
        for _ in 0..100 {
            assert!(auditor.first_seen(&send, "first").is_empty());
        }
        for i in 0..SEND_NOTICE_CACHE_ENTRIES {
            assert_eq!(auditor.first_seen(&send, &format!("harness-{i}")).len(), 1);
        }
        {
            let seen = auditor.seen.lock().unwrap();
            assert_eq!(seen.entries.len(), SEND_NOTICE_CACHE_ENTRIES);
            assert_eq!(seen.oldest.len(), SEND_NOTICE_CACHE_ENTRIES);
            assert!(!seen.entries.contains(&first[0]));
        }
        assert!(auditor.first_seen(&send, "harness-0").is_empty());
        assert_eq!(auditor.first_seen(&send, "first"), first);
        assert!(auditor.first_seen(&send, "first").is_empty());
        assert_eq!(
            auditor.seen.lock().unwrap().oldest.len(),
            SEND_NOTICE_CACHE_ENTRIES
        );
    }
}
