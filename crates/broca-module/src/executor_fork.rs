//! What the fork manager hands an executor for one attempt of a fork's ask (llm-runner 0.8.42
//! §9.20 `fork_answer`), and what the executor hands back.
//!
//! A fork asks questions of a copy of a head session's last served request. The fork manager
//! decides everything the head's records and the fork's own records settle: who may ask, the
//! order of refusals, the base request rebuilt from the head's records, the fork's appended
//! conversation, attempt numbers, and what is stored. The executor does what needs a credential,
//! routing or a provider: it admits the attempt with routing when the head's account is routed,
//! checks routing's answer against the head's frozen account, renders and checks the base, sends
//! the fork's request once, and releases the admission. It writes no session, and nothing of the
//! head's: no transcript record, no lease, no event on its stream, no compaction call.

use std::sync::Arc;

use broca_protocol::BindIdentity;
use cortexkit_role_llm_runner::usage::RunUsage;
use sha2::Digest as _;

/// One attempt of a fork's ask, filled by the fork manager.
pub struct ForkCallJob {
    /// The head session the fork is of. Its records are read only; nothing writes them.
    pub head: BindIdentity,
    /// The fork's id, as `session.fork_prepare` returned it.
    pub fork_id: String,
    /// The ask's id, as the caller sent it (1 to 256 Unicode scalar values).
    pub ask_id: String,
    /// This attempt's number for `(fork_id, ask_id)`, counted from 1 and durable across restart.
    /// Every attempt gets its own routing admission.
    pub attempt: u64,
    /// The fork attempt's own run identity, under which its spend is reported: equal to
    /// [`fork_admission_id`] of this attempt. Never one of the head's run ids.
    pub run_id: String,
    /// The fork's base, rebuilt from the head's records as idle cache warming rebuilds a recorded
    /// request: the head's request (its run, step, config, options and refusal-fallback route),
    /// with the head's answer to it appended when the fork's prepare appended one, and the digest
    /// of the bytes that renders to. `base.config` is what the fork is pinned to: the head's frozen
    /// provider, model, render config, auth selection (account, credential, routing decision
    /// with the head's `conversation_key`, pool) and service tier. The executor builds its
    /// provider from this config alone, renders `base.opts` and sends nothing when the bytes'
    /// digest differs from `base.request_body_sha256` ([`ForkCallRefusal::BaseUnavailable`]).
    pub base: Box<broca_core::RecordedRequest>,
    /// The request to send: `base.opts` with the fork's earlier questions and answers and the new
    /// question appended as messages, and nothing else changed. The tools stay in it byte for
    /// byte; the executor sends it on the base's route (`base.route`) and never runs a tool.
    pub request: broca_protocol::request::CallOptions,
    /// The routing admission id this attempt must use when the account is routed:
    /// [`fork_admission_id`] of `(fork_id, ask_id, attempt)`. The fork manager has recorded it
    /// durably before the call, so a restart can release it. A transport retry of the same
    /// attempt reuses it.
    pub admission_id: String,
    /// The routing conversation key for the fork: [`fork_conversation_key`] of the fork id and
    /// the head's `conversation_key` (its frozen routing decision's), or `None` when the head's
    /// config records no routing decision and so no head key.
    pub conversation_key: Option<String>,
    /// Called with the usage the attempt has incurred whenever the executor learns more of it
    /// before the call ends, so that an attempt cut by a restart is stored with what it spent.
    /// The fork manager records each report; calling it is optional.
    pub on_usage: Option<Arc<dyn Fn(RunUsage) + Send + Sync>>,
    /// Called with the provider's own account of the model call whenever the executor learns
    /// it, just before the matching `on_usage` report. The fork manager keeps the latest one
    /// durably and writes it to the usage export under [`ForkCallJob::run_id`]. An attempt that
    /// never reports one made no model call the manager can bill.
    pub on_step: Option<Arc<dyn Fn(ForkStepSpend) + Send + Sync>>,
}

/// What a fork attempt's model call spent, as the provider reported it: what a run's step records
/// for its usage export fact.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ForkStepSpend {
    pub usage: broca_protocol::Usage,
    /// The service tier the provider says it served the call at, verbatim; absent when it said
    /// none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_tier_used: Option<String>,
    /// How the provider said the reply ended, when it said so.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<broca_protocol::content::FinishReason>,
}

/// How an attempt ended.
#[derive(Debug, Clone, PartialEq)]
pub enum ForkCallOutcome {
    /// The model answered. `answer` is the reply's text; when the reply held a tool call, only
    /// the text before the first tool call, with `finish` `tool_call_refused`, and no tool ran.
    /// Otherwise `finish` is the reply's: `end_turn` or `max_tokens` (llm-runner `ForkFinish`
    /// spellings). `usage` is the attempt's model calls' spend. The manager stores the answer,
    /// adds the question and answer to the fork's conversation, and ends the fork on
    /// `tool_call_refused`.
    Answered {
        answer: String,
        finish: String,
        usage: RunUsage,
    },
    /// A model call was made, or begun, and failed for a reason other than a tool call. `usage`
    /// is what it incurred. The manager stores `{answer: "", finish: "model_error", usage}`;
    /// neither the question nor any partial text joins the fork's conversation.
    ModelFailed { usage: RunUsage, error: String },
    /// Nothing was sent to the model and nothing was spent. The manager stores nothing for the
    /// ask, so a retry is a new attempt.
    Refused(ForkCallRefusal),
}

/// Why an attempt was refused before any model call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForkCallRefusal {
    /// The base rendered by this executor's provider doesn't have the recorded digest, or can't
    /// be rendered. Answered `fork_base_unavailable`.
    BaseUnavailable(String),
    /// Routing held the account (`routing_code` one of `account_hold`, `account_pool_exhausted`,
    /// `account_selection_unavailable`), or chose an account, credential or service tier other
    /// than the head's (`mismatch` one of `account`, `credential`, `service_tier`; absent tier
    /// means `standard`). Answered retryable `fork_account_held {routing_code?, mismatch?}`.
    AccountHeld {
        routing_code: Option<String>,
        mismatch: Option<String>,
    },
    /// Any other routing refusal. Answered non-retryable `fork_account_blocked {routing_code}`.
    AccountBlocked { routing_code: String },
    /// This executor can't serve the fork: no fork support, or a head config it can't send
    /// again. Answered `fork_unsupported`.
    Unsupported(String),
}

/// A fork attempt's admission to release after a restart (see [`RunExecutor::fork_release`]).
///
/// [`RunExecutor::fork_release`]: super::RunExecutor::fork_release
pub struct ForkReleaseJob {
    pub head: BindIdentity,
    /// The head's config the attempt was pinned to (its base's `config`).
    pub config: broca_records::record::RunConfig,
    pub admission_id: String,
    /// The attempt's run identity ([`ForkCallJob::run_id`]).
    pub run_id: String,
}

/// The routing conversation key of fork `fork_id` of a head whose key is `head_key`: `fork:`
/// and the lowercase hex SHA-256 of the RFC 8785 form of `{"fork_id", "head_key"}`.
pub fn fork_conversation_key(fork_id: &str, head_key: &str) -> String {
    fork_hash(&format!(
        "{{\"fork_id\":{},\"head_key\":{}}}",
        canonical_string(fork_id),
        canonical_string(head_key)
    ))
}

/// The routing admission id of attempt `attempt` of ask `ask_id` on fork `fork_id`: `fork:` and
/// the lowercase hex SHA-256 of the RFC 8785 form of `{"ask_id", "attempt", "fork_id"}`, the
/// attempt a JSON integer.
pub fn fork_admission_id(fork_id: &str, ask_id: &str, attempt: u64) -> String {
    fork_hash(&format!(
        "{{\"ask_id\":{},\"attempt\":{attempt},\"fork_id\":{}}}",
        canonical_string(ask_id),
        canonical_string(fork_id)
    ))
}

fn fork_hash(canonical: &str) -> String {
    let digest = sha2::Sha256::digest(canonical.as_bytes());
    let mut out = String::with_capacity(5 + 64);
    out.push_str("fork:");
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// A string in RFC 8785 form, which for a string is its JSON text with only `"`, `\` and control
/// characters escaped, short escapes where JSON has them and lowercase `\u00xx` otherwise: what
/// `serde_json` writes.
fn canonical_string(value: &str) -> String {
    serde_json::to_string(value).expect("a string serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    const FORK_KEY: &str = "2850937d04dd5f49220d04d714f91faf34180c46c2c8b88cdafcf1dfc7dd4bd6";
    const ADMISSION_ID: &str = "1c5eb44ba863f5575b157804a784d72aee9e330ad3e7d5d8806e99272eabd6a0";

    // Expected values computed independently: SHA-256 over the canonical JSON text named in
    // each case.
    #[test]
    fn fork_routing_keys_hash_the_canonical_json_the_contract_names() {
        // sha256(b'{"fork_id":"fork-1","head_key":"head"}')
        assert_eq!(
            fork_conversation_key("fork-1", "head"),
            format!("fork:{}", FORK_KEY)
        );
        // sha256(b'{"ask_id":"retry","attempt":2,"fork_id":"fork-1"}')
        assert_eq!(
            fork_admission_id("fork-1", "retry", 2),
            format!("fork:{}", ADMISSION_ID)
        );
        // A 256-character head key still gives a 69-character key, within routing's 256-character bound on conversation keys.
        assert_eq!(fork_conversation_key("f", &"h".repeat(256)).len(), 69);
        // Quotes and control characters are escaped as RFC 8785 writes them.
        assert_eq!(
            canonical_string("a\"\\\n\u{1f}é"),
            "\"a\\\"\\\\\\n\\u001fé\""
        );
    }
}
