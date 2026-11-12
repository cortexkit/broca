//! The store actor's answer to `memory.report` (see `crate::memory_report`): its session's
//! holders and its own, walked when asked. A running run's kept compaction transcript and its
//! request in flight are reached through weak handles the run leaves (see
//! `broca_session_loop::shared::RunWindow` and `InFlightStep::request`), held only for the walk.

use broca_core::FirstEpisodeV1;
use broca_protocol::request::{CallOptions, Tool};
use broca_session_store::backend::SessionBackend;
use broca_session_store::held::{serde_heap_bytes, HeldBytes};
use broca_wire::{MemoryHolders, MemorySession};

use super::StoreActor;
use crate::memory_report::{ActorMemory, SetupHeld};

/// Why the rendered body of a request in flight is not reported.
pub(crate) const BODY_UNTRACKED: &str = "not tracked yet";

impl<B: SessionBackend + Send + Sync + 'static> StoreActor<B> {
    /// What this actor holds, by holder.
    pub(super) fn memory(&self) -> ActorMemory {
        let session = self.session.held_bytes();
        let run = self.run.as_ref();
        let window = run.and_then(|run| run.window.lock().ok().and_then(|slot| slot.clone()));
        let compaction_entries = window.and_then(|window| window.held_bytes()).unwrap_or(0);
        let request = run.and_then(|run| {
            let step = run.step.lock().ok()?;
            step.as_ref()?.request.upgrade()
        });
        let request_in_flight = request.as_deref().map_or(0, request_bytes);
        let media_fold = self.media.held_bytes();
        let first_episode = self.admission.known();
        let mut setup = first_episode.map_or_else(Vec::new, setup_of);
        if let Some(request) = &request {
            setup.push(tools_held(&request.tools, None));
        }
        let rest = std::mem::size_of::<Self>()
            + session.rest
            + self.fanout.as_ref().map_or(0, |fanout| fanout.held_bytes())
            + first_episode.map_or(0, first_episode_bytes)
            + self
                .one_shot_runs
                .iter()
                .map(|run| std::mem::size_of_val(run) + run.0.heap_bytes())
                .sum::<usize>()
            + slots(&self.pending_views);
        let holders = MemoryHolders {
            messages: session.messages as u64,
            entry_facts: session.entry_facts as u64,
            kept_records: session.kept_records as u64,
            kept_records_capacity: session.kept_capacity as u64,
            id_index: session.id_index as u64,
            sealed_entries: session.sealed_entries as u64,
            compaction_entries: compaction_entries as u64,
            media_fold: media_fold as u64,
            request_in_flight: request_in_flight as u64,
            rest: rest as u64,
            total: 0,
        }
        .summed();
        ActorMemory {
            session: MemorySession {
                address: self.session.address().0.clone(),
                session_class: self.frozen_session_class(),
                window_messages: self.session.conversation.messages.len() as u64,
                run_in_progress: run.is_some(),
                holders,
                request_body_bytes: None,
                request_body_untracked: request.is_some().then(|| BODY_UNTRACKED.to_owned()),
            },
            setup,
        }
    }
}

/// The slots a vector holds, by capacity, for elements with no estimator of their own.
fn slots<T>(vector: &Vec<T>) -> usize {
    vector.capacity() * std::mem::size_of::<T>()
}

/// A request's options, without its tool list, which the setup parts count. Its episode history
/// for rendering counts at its inline size.
pub(super) fn request_bytes(options: &CallOptions) -> usize {
    std::mem::size_of::<CallOptions>()
        + options.prompt.heap_bytes()
        + serde_heap_bytes(&*options.provider_options)
        + serde_heap_bytes(&options.declared_markers)
        + serde_heap_bytes(&options.step_context)
}

/// A decoded tool list, named by its tools' names and size so equal lists group together.
fn tools_held(tools: &std::sync::Arc<Vec<Tool>>, plan: Option<&str>) -> SetupHeld {
    let bytes = 2 * std::mem::size_of::<usize>()
        + std::mem::size_of::<Vec<Tool>>()
        + tools.capacity() * std::mem::size_of::<Tool>()
        + serde_heap_bytes(&**tools);
    let names: Vec<&str> = tools.iter().map(|tool| tool.name.as_str()).collect();
    SetupHeld {
        kind: "tools",
        content: format!("{}:{bytes}:{}", plan.unwrap_or(""), names.join(",")),
        copy: std::sync::Arc::as_ptr(tools) as usize,
        bytes,
    }
}

/// The parts of a frozen plan manifest a session's first episode keeps decoded.
fn setup_of(first: &FirstEpisodeV1) -> Vec<SetupHeld> {
    let Some(manifest) = &first.manifest else {
        return Vec::new();
    };
    let plan = manifest.composition_digest.as_str();
    let copy = std::sync::Arc::as_ptr(manifest) as usize;
    let tools = tools_held(&manifest.tools, Some(plan));
    let system = manifest.system_text.heap_bytes();
    // The manifest besides its tool list and system text, which have rows of their own.
    let rest = 2 * std::mem::size_of::<usize>()
        + std::mem::size_of_val(&**manifest)
        + serde_heap_bytes(&manifest.rich_ui)
        + serde_heap_bytes(&manifest.plan)
        + manifest.plan_identity.heap_bytes()
        + manifest.composition_digest.heap_bytes()
        + serde_heap_bytes(&manifest.fetches)
        + manifest.system_digest.heap_bytes()
        + manifest.role_versions.heap_bytes()
        + serde_heap_bytes(&manifest.declarations);
    vec![
        SetupHeld {
            kind: "plan_manifest",
            content: plan.to_owned(),
            copy,
            bytes: rest,
        },
        SetupHeld {
            kind: "system_text",
            content: plan.to_owned(),
            copy,
            bytes: system,
        },
        // The manifest's tool list matches across sessions on one plan even when the tools
        // were decoded twice, so the plan names it.
        SetupHeld {
            content: plan.to_owned(),
            ..tools
        },
    ]
}

/// The first episode besides the manifest it shares, which the setup parts count.
fn first_episode_bytes(first: &FirstEpisodeV1) -> usize {
    first.run_id.0.heap_bytes() + first.tool_names.heap_bytes()
}
