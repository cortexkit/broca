// Retained from cortexkit-role-compaction-provider 0.2.4 (MIT); legacy sync client only.
//! `role.describe`: discovery before use.
//!
//! A runner that is handed a plan naming a compaction provider calls
//! `role.describe` on it and refuses the provider by name if a required op
//! is missing, before routing anything to it. The answer describes the
//! provider build, never a session, so a consumer may cache it for as long
//! as it talks to the same module incarnation.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::compaction_provider::{PROVIDES, REQUIRED_OPS};

/// Empty params for discovery. Unknown fields are ignored.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct DescribeRequest {}

impl DescribeRequest {
    pub fn new() -> Self {
        Self::default()
    }
}

/// The `role.describe` answer: every major of the role the module serves,
/// each with its own ops and stability, plus module-level capabilities and
/// the runner groups the provider needs.
///
/// Decoded leniently: unknown fields, majors, ops and capabilities are
/// ignored. [`check_describe`] checks strictly only the
/// `compaction-provider/v1` major and its required ops.
///
/// Non-exhaustive so a later optional member is additive: build one with
/// [`RoleDescribe::new`] and the `with_*` setters, or decode one.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct RoleDescribe {
    pub majors: Vec<Major>,
    /// The module's own build identity, for display. Never a cache key.
    pub implementation_version: String,
    /// Module-level capability identifiers. Existing entries keep their meaning.
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// The `llm-runner/v1` capability groups this provider needs from the
    /// runner besides `compaction`, for example `transcript_reads` for a
    /// provider with no reader of its own. The plan composer checks them
    /// against the groups the runner advertises in its `role.describe` when
    /// it composes the session, and an unmet group fails the launch, named
    /// ([`RoleDescribe::unmet_runner_groups`]).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runner_groups: Vec<String>,
    /// True declares the setup descent rules in CONTRACT.md §4: validate the
    /// inherited history, persist its parent lineage and inclusive cut, and
    /// refuse a later declaration that contradicts that saved ancestry.
    /// Callers may send setup `descends_from` only when this is true;
    /// absent or false means use the first step or hook instead.
    #[serde(default, skip_serializing_if = "is_false")]
    pub setup_descends_from: bool,
    /// Optional Claude Code capabilities, also selected at transform.declare.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude_code:
        Option<cortexkit_role_step_transform_provider::claude_code::ClaudeCodeCapabilities>,
}

fn is_false(value: &bool) -> bool {
    !value
}

fn alpha() -> String {
    "alpha".to_owned()
}

/// One served major.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct Major {
    /// The role and major as the manifest spells it, for example
    /// `compaction-provider/v1`.
    pub version: String,
    pub ops: Vec<String>,
    /// `alpha`, `beta` or `stable`; see [`Major::stability`]. An answer that
    /// omits it is read as `alpha`, the level every major starts at.
    #[serde(default = "alpha")]
    pub stability: String,
}

/// A stability level. Unknown levels decode as [`Stability::Other`] rather
/// than failing the answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Stability {
    Alpha,
    Beta,
    Stable,
    Other(String),
}

impl RoleDescribe {
    /// An answer for the given majors and build identity, declaring no
    /// capability and needing no runner group.
    pub fn new(majors: Vec<Major>, implementation_version: impl Into<String>) -> Self {
        Self {
            majors,
            implementation_version: implementation_version.into(),
            ..Self::default()
        }
    }

    pub fn with_setup_descends_from(mut self) -> Self {
        self.setup_descends_from = true;
        self
    }

    pub fn with_claude_code(
        mut self,
        bundle: cortexkit_role_step_transform_provider::claude_code::ClaudeCodeCapabilities,
    ) -> Self {
        self.claude_code = Some(bundle);
        self
    }

    /// Set the runner groups the provider needs.
    pub fn with_runner_groups(mut self, groups: Vec<String>) -> Self {
        self.runner_groups = groups;
        self
    }

    /// The entry for `version` (for example `compaction-provider/v1`), if
    /// served.
    pub fn major(&self, version: &str) -> Option<&Major> {
        self.majors.iter().find(|major| major.version == version)
    }

    /// The runner groups this provider needs that `declared` (a runner's
    /// `role.describe` capabilities) does not list, in the order the
    /// provider listed them. When this is not empty the plan composer fails the
    /// launch and names each group listed.
    pub fn unmet_runner_groups<'a>(&'a self, declared: &[String]) -> Vec<&'a str> {
        self.runner_groups
            .iter()
            .filter(|group| !declared.contains(group))
            .map(String::as_str)
            .collect()
    }
}

impl Major {
    pub fn new(version: impl Into<String>, ops: Vec<String>, stability: impl Into<String>) -> Self {
        Self {
            version: version.into(),
            ops,
            stability: stability.into(),
        }
    }

    pub fn stability(&self) -> Stability {
        match self.stability.as_str() {
            "alpha" => Stability::Alpha,
            "beta" => Stability::Beta,
            "stable" => Stability::Stable,
            other => Stability::Other(other.to_owned()),
        }
    }

    pub fn serves(&self, op: &str) -> bool {
        self.ops.iter().any(|served| served == op)
    }
}

/// Why a `role.describe` answer does not describe a usable
/// `compaction-provider/v1` module.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DescribeProblem {
    /// The answer is not a JSON object.
    NotAnObject,
    /// A required field is missing or has the wrong type.
    Undecodable(String),
    /// No entry in `majors` is `compaction-provider/v1`.
    MissingMajor { found: Vec<String> },
    /// The `compaction-provider/v1` entry lacks a required op.
    MissingOp(&'static str),
}

impl DescribeProblem {
    /// The snake_case problem name returned to the legacy client.
    pub fn name(&self) -> &'static str {
        match self {
            Self::NotAnObject => "not_an_object",
            Self::Undecodable(_) => "undecodable",
            Self::MissingMajor { .. } => "missing_major",
            Self::MissingOp(_) => "missing_op",
        }
    }
}

/// Decode `raw` and check it lists a `compaction-provider/v1` major with
/// every required op. Every problem is returned, not only the first.
pub fn check_describe(raw: &Value) -> Result<RoleDescribe, Vec<DescribeProblem>> {
    if !raw.is_object() {
        return Err(vec![DescribeProblem::NotAnObject]);
    }
    let describe: RoleDescribe = match serde_json::from_value(raw.clone()) {
        Ok(describe) => describe,
        Err(error) => return Err(vec![DescribeProblem::Undecodable(error.to_string())]),
    };
    let mut problems = Vec::new();
    match describe.major(PROVIDES) {
        None => problems.push(DescribeProblem::MissingMajor {
            found: describe.majors.iter().map(|m| m.version.clone()).collect(),
        }),
        Some(major) => {
            for op in REQUIRED_OPS {
                if !major.serves(op) {
                    problems.push(DescribeProblem::MissingOp(op));
                }
            }
        }
    }
    if problems.is_empty() {
        Ok(describe)
    } else {
        Err(problems)
    }
}
