// Retained from cortexkit-role-compaction-provider 0.2.4 (MIT); legacy sync client only.
//! `compaction.setup`: called once per session, before its first model
//! call.
//!
//! Setup returns the initial CompactionMessage (how a session starts with
//! its head already in place; a provider with no head returns an empty
//! replacement), the stability rank of each of the provider's own messages,
//! and `call_when`, the conditions for calling the provider. The runner
//! records the answer before the first model call; once it is recorded,
//! Setup never runs again for the session.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::compaction_provider::{
    answer::CompactionMessage,
    errors::RefuseCode,
    status::{DescendsFrom, MessageRef},
};

/// The `compaction.setup` request. Decoded leniently.
///
/// Non-exhaustive so a later optional member is additive: decode one, or
/// build one with [`SetupRequest::new`] and the `with_*` setters.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct SetupRequest {
    /// The runner's opaque name for the session, as every later status
    /// spells it.
    pub session: String,
    /// The harness named in the session's key, which identifies the caller
    /// (for example `broca`). This differs from the route's bind harness,
    /// the harness a connection declares when it opens the route; a runner
    /// always declares `runner`. Required: a request without it does
    /// not decode. A provider keys a runner conversation on
    /// `(project_root, session, harness)`.
    pub harness: String,
    /// This request's id; the answer names it.
    pub request_id: String,
    /// The session's lineage, absent when nothing has been written yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage_id: Option<String>,
    /// Optional inherited prefix, with the same meaning as on a step status.
    /// Absence leaves descent undeclared; a later step or hook may declare it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub descends_from: Option<DescendsFrom>,
    /// The compaction item's preset, verbatim from the plan. Absent means
    /// the provider's default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preset: Option<String>,
    /// The compaction item's params, verbatim from the plan.
    #[serde(default)]
    pub params: Map<String, Value>,
    /// The session's composition, verbatim from the plan; opaque.
    pub composition: Map<String, Value>,
    /// The model of the session's first step.
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_limit: Option<u64>,
    /// The newest message already written, if any (an imported session, or
    /// the first user message when the runner writes it before Setup).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub newest: Option<MessageRef>,
    /// The runner's clock, in milliseconds since the Unix epoch.
    pub now: u64,
    /// Optional Claude Code capabilities selected at transform.declare. Copy
    /// that selection unchanged; setup cannot change the connection's plan.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude_code:
        Option<cortexkit_role_step_transform_provider::claude_code::ClaudeCodeCapabilities>,
}

impl SetupRequest {
    /// Check host-only plan values only after authenticating a runner principal.
    pub fn check_runner_params(
        &self,
    ) -> Result<(), cortexkit_role_step_transform_provider::subscription::RunnerParamsProblem> {
        cortexkit_role_step_transform_provider::subscription::check_runner_params(&self.params)
    }

    pub fn new(
        session: impl Into<String>,
        harness: impl Into<String>,
        request_id: impl Into<String>,
        composition: Map<String, Value>,
        model: impl Into<String>,
        now: u64,
    ) -> Self {
        Self {
            session: session.into(),
            harness: harness.into(),
            request_id: request_id.into(),
            lineage_id: None,
            descends_from: None,
            preset: None,
            params: Map::new(),
            composition,
            model: model.into(),
            variant: None,
            context_window: None,
            output_limit: None,
            newest: None,
            now,
            claude_code: None,
        }
    }

    pub fn with_claude_code(
        mut self,
        bundle: cortexkit_role_step_transform_provider::claude_code::ClaudeCodeCapabilities,
    ) -> Self {
        self.claude_code = Some(bundle);
        self
    }

    /// Set the plan item's preset and params.
    pub fn with_item(mut self, preset: Option<String>, params: Map<String, Value>) -> Self {
        self.preset = preset;
        self.params = params;
        self
    }

    pub fn with_lineage(mut self, lineage_id: impl Into<String>) -> Self {
        self.lineage_id = Some(lineage_id.into());
        self
    }

    pub fn with_descends_from(mut self, descends_from: DescendsFrom) -> Self {
        self.descends_from = Some(descends_from);
        self
    }

    pub fn with_model_details(
        mut self,
        variant: Option<String>,
        context_window: Option<u64>,
        output_limit: Option<u64>,
    ) -> Self {
        self.variant = variant;
        self.context_window = context_window;
        self.output_limit = output_limit;
        self
    }

    pub fn with_newest(mut self, newest: MessageRef) -> Self {
        self.newest = Some(newest);
        self
    }
}

/// The stability rank of one of the provider's own messages: `index` is the
/// position in every CompactionMessage's `replacement`, and a higher `rank`
/// means the message changes less often. The runner places cache
/// breakpoints and judges change policies on these segments.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct StabilityRank {
    pub index: u32,
    pub rank: u32,
}

/// When the runner calls the provider, frozen with the session. The runner
/// calls when the step's reported usage reaches the model's share of the
/// window ([`CallWhen::share_for`]). It also always calls on a prefix
/// rebuild, when it re-sends the conversation from the start because its
/// cached prefix was lost or invalidated (after a model change, say), and
/// after an execution error. With no `call_when`, or with a
/// condition kind it does not know, it calls on every step
/// ([`calls_every_step`]).
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct CallWhen {
    /// The share of the context window, above 0 and at most 1.
    pub default: f64,
    /// Per-model overrides, keyed by an exact model id or by a pattern that
    /// ends in one `*` and matches every model id starting with the text
    /// before it.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub models: BTreeMap<String, f64>,
    /// Condition kinds this crate does not know. New kinds are additive,
    /// and a runner that meets one it does not know calls on every step
    /// rather than guess ([`CallWhen::has_unknown_conditions`]).
    #[serde(flatten)]
    pub unknown: Map<String, Value>,
}

impl CallWhen {
    pub fn new(default: f64) -> Self {
        Self {
            default,
            models: BTreeMap::new(),
            unknown: Map::new(),
        }
    }

    pub fn with_models(mut self, models: BTreeMap<String, f64>) -> Self {
        self.models = models;
        self
    }

    pub fn with_unknown_conditions(mut self, unknown: Map<String, Value>) -> Self {
        self.unknown = unknown;
        self
    }

    pub fn has_unknown_conditions(&self) -> bool {
        !self.unknown.is_empty()
    }

    /// The share of the window that applies to `model`. Matching is in a
    /// fixed order: a key equal to `model`; else the trailing-`*` key with
    /// the longest prefix that `model` starts with; else `default`. A `*`
    /// anywhere but at the end of a key is literal, so such a key matches
    /// only exactly.
    pub fn share_for(&self, model: &str) -> f64 {
        if let Some(share) = self.models.get(model) {
            return *share;
        }
        self.models
            .iter()
            .filter_map(|(key, share)| {
                let prefix = key.strip_suffix('*')?;
                model.starts_with(prefix).then_some((prefix.len(), *share))
            })
            .max_by_key(|(length, _)| *length)
            .map_or(self.default, |(_, share)| share)
    }

    /// Whether every share is above 0 and at most 1.
    pub fn shares_in_range(&self) -> bool {
        std::iter::once(&self.default)
            .chain(self.models.values())
            .all(|share| *share > 0.0 && *share <= 1.0)
    }
}

/// Whether the runner calls the provider on every step regardless of
/// usage: when Setup declared no `call_when`, or declared a condition kind
/// the runner does not know. A runner never skips a call because it could
/// not read a condition.
pub fn calls_every_step(call_when: Option<&CallWhen>) -> bool {
    call_when.is_none_or(CallWhen::has_unknown_conditions)
}

/// The `compaction.setup` answer. Strict on `answer`, like a step answer.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(tag = "answer", rename_all = "snake_case")]
pub enum SetupAnswer {
    /// The session's initial view and declarations.
    Ready {
        request_id: String,
        /// The initial CompactionMessage. Its range is usually empty at the
        /// start of the lineage (`{from: 0, to: 0}`): the head goes before
        /// every message.
        initial: CompactionMessage,
        /// Ranks for the provider's own messages; empty for a provider that
        /// declares none (a subagent session with no head, say).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        stability: Vec<StabilityRank>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_when: Option<CallWhen>,
    },
    /// Setup failed. No model call is made, and the run ends `error` with
    /// the role's `code` and, when present, the provider's own
    /// `provider_code` beside it. The next send runs Setup again. A
    /// `retryable` member an older provider still sends is ignored: `code`
    /// fixes retryability.
    Refuse {
        request_id: String,
        code: RefuseCode,
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_code: Option<String>,
    },
}

impl SetupAnswer {
    pub fn request_id(&self) -> &str {
        match self {
            Self::Ready { request_id, .. } | Self::Refuse { request_id, .. } => request_id,
        }
    }

    /// Whether a `refuse` may be retried; `None` for `ready`. Its
    /// retryability is fixed by the code: `true` means the runner may
    /// retry without the user acting.
    pub fn retryable(&self) -> Option<bool> {
        match self {
            Self::Refuse { code, .. } => Some(code.retryable()),
            Self::Ready { .. } => None,
        }
    }
}
