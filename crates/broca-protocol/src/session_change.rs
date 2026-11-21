//! The transitions of a session's plan changes: what each `session.refresh`,
//! `session.refresh_policy`, `session.flush_prefix`, applied compaction view and run start does
//! to the session's change state. The session store folds its records through these transitions.
//! They live here so the session store's fold crate can depend on this one. Nothing here reads
//! or writes anything.
//!
//! A change never applies inside a running run. The earliest it applies is the start of the next
//! run, and the `immediately` rung means from the next run. The `on_prefix_rebuild` rung waits
//! for a real frozen-prefix rebuild after its acceptance: a compaction view applied, a model
//! switch between runs, or a prefix flush. A new run alone is not a rebuild, because stateless
//! requests still hit the provider's prompt cache across runs.

use serde::{Deserialize, Serialize};

use crate::request::ModelHandle;

/// A pending change: the highest accepted generation not yet applied, with its latest policy and
/// the manifest it will freeze when it applies, held as `M`. The session store's fold holds a
/// reference to the manifest: the setup parts it is stored as (its plan, each provider's
/// answer, its tools, its system text and its declarations) and the rest of its members, as a
/// run start references the manifest it froze.
#[derive(Debug, Clone, PartialEq)]
pub struct Pending<M> {
    pub generation: u64,
    pub policy: String,
    pub manifest: M,
}

/// A pending change that applies at a run start: its generation, and the runner role's
/// `applied_by` name for how (`fold` after a prefix rebuild, `append` with the prefix intact).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Applying {
    pub generation: u64,
    pub applied_by: &'static str,
}

/// A pending change a run start applied, with the facts the session's baseline reports for it:
/// the applied manifest's digest, its composition digest when it has one, and the plan items its
/// fetch left absent. The session store records it in the batch of the run start that applied it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppliedChange {
    pub generation: u64,
    pub applied_by: String,
    pub manifest_digest: String,
    pub composition_digest: Option<String>,
    pub absent_items: Vec<serde_json::Value>,
}

/// The key a run's model is compared by when deciding whether the model switched between runs.
pub fn model_key(model: &ModelHandle) -> String {
    format!("{}:{}", model.provider_module_id, model.model_id)
}

/// Whether a pending change under `policy` applies at a run start on the model keyed `model`, and
/// if it does, its `applied_by` name. `rebuild` and `flushed` say whether a prefix rebuild and a
/// flush happened since the change was accepted, and `last_model` is the previous run's model
/// key: a run on another model is a rebuild too. `immediately` applies at the next run, and
/// `on_prefix_rebuild` once a rebuild happened; a flush applies the change whatever its policy.
/// A policy the session doesn't support never applies, so a record carrying one can't change a
/// run.
pub fn applies_at_run_start(
    policy: &str,
    rebuild: bool,
    flushed: bool,
    last_model: Option<&str>,
    model: &str,
) -> Option<&'static str> {
    let rebuild = rebuild || last_model.is_some_and(|previous| previous != model);
    let applies = flushed
        || match policy {
            "immediately" => true,
            "on_prefix_rebuild" => rebuild,
            _ => false,
        };
    applies.then_some(if rebuild { "fold" } else { "append" })
}

/// The effects of session-change records over a manifest held as `M`. Each method is one
/// record's effect.
///
/// A run start is two steps. [`Self::run_started`] notes the run's model and manifest, and when
/// the pending change applies at it, [`Self::applied`] applies it after. Whether it applies is
/// decided before either, by [`applies_at_run_start`]. The session store records that decision
/// in the run start's batch.
#[derive(Debug, Clone, PartialEq)]
pub struct Transitions<M> {
    /// The highest change generation accepted. `0` before any.
    pub accepted_generation: u64,
    /// The highest change generation applied. `0` before any.
    pub applied_generation: u64,
    pub pending: Option<Pending<M>>,
    /// The latest run's frozen manifest.
    pub current: Option<M>,
    /// The manifest of the latest prefix rebuild, or of the first run before any.
    pub prefix: Option<M>,
    /// A real prefix rebuild happened since the pending acceptance: a compaction view applied,
    /// a model switch between runs, or a prefix flush.
    pub rebuild_since_accept: bool,
    /// A flush has not yet reached a new run, including a flush before the acceptance.
    pub flushed_since_accept: bool,
    /// The latest run's model, by [`model_key`].
    pub last_model: Option<String>,
}

impl<M> Default for Transitions<M> {
    fn default() -> Self {
        Self {
            accepted_generation: 0,
            applied_generation: 0,
            pending: None,
            current: None,
            prefix: None,
            rebuild_since_accept: false,
            flushed_since_accept: false,
            last_model: None,
        }
    }
}

impl<M: Clone> Transitions<M> {
    /// A refresh accepted under `generation`. The writer checks generation order where it
    /// writes, so each acceptance supersedes the pending change in log order. A flush before the
    /// acceptance still counts as a rebuild for it.
    pub fn accepted(&mut self, generation: u64, policy: &str, manifest: M) {
        if generation > self.accepted_generation {
            self.accepted_generation = generation;
        }
        self.pending = Some(Pending {
            generation,
            policy: policy.to_owned(),
            manifest,
        });
        self.rebuild_since_accept = self.flushed_since_accept;
    }

    /// The pending change's policy changed, when `generation` is still the pending one.
    pub fn policy_changed(&mut self, generation: u64, policy: &str) {
        if let Some(pending) = self.pending.as_mut() {
            if pending.generation == generation {
                policy.clone_into(&mut pending.policy);
            }
        }
    }

    /// A prefix flush: a real prefix rebuild for the pending change, which the next run applies
    /// whatever its policy, as a fold.
    pub fn flushed(&mut self) {
        self.rebuild_since_accept = true;
        self.flushed_since_accept = true;
    }

    /// A compaction view applied: a real prefix rebuild, whose prefix is the current manifest.
    pub fn view_applied(&mut self) {
        self.rebuild_since_accept = true;
        self.prefix = self.current.clone();
    }

    /// A run started on the model keyed `model`, freezing `manifest`. A run on another model
    /// than the previous run is a prefix rebuild whose prefix is this run's manifest. A first
    /// run, or one after a flush, starts a new prefix.
    pub fn run_started(&mut self, model: String, manifest: Option<M>) {
        if let Some(previous) = self.last_model.replace(model.clone()) {
            if previous != model {
                self.rebuild_since_accept = true;
                self.prefix = manifest.clone();
            }
        }
        self.current = manifest;
        if self.prefix.is_none() || self.flushed_since_accept {
            self.prefix = self.current.clone();
        }
        self.flushed_since_accept = false;
    }

    /// The pending change applied at the run start just folded, as `applying` decided before
    /// it. Returns the change it applied; `None`, changing nothing, when no change is pending.
    /// Applied by a rebuild (`fold`), its manifest is the new prefix.
    pub fn applied(&mut self, applying: Applying) -> Option<Pending<M>> {
        let pending = self.pending.take()?;
        self.applied_generation = pending.generation;
        if applying.applied_by == "fold" {
            self.prefix = Some(pending.manifest.clone());
        }
        self.rebuild_since_accept = false;
        self.flushed_since_accept = false;
        Some(pending)
    }
}
