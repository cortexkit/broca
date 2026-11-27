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
        let compaction_entries = window
            .and_then(|window| window.held_bytes(&self.session.conversation))
            .unwrap_or(0);
        let request = run.and_then(|run| {
            let step = run.step.lock().ok()?;
            step.as_ref()?.request.upgrade()
        });
        let request_in_flight = request.as_deref().map_or(0, request_bytes);
        let media_fold = self.media.held_bytes();
        let first_episode = self.admission.known();
        let setup = request
            .iter()
            .map(|request| tools_held(&request.tools))
            .collect();
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
                run_id: run.map(|run| run.run_id.0.clone()),
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
/// for rendering counts at its inline size. A request's messages share their text and JSON with
/// the session's window, which counts them, so the request counts only the leaves it holds alone,
/// such as text its render rewrote.
pub(super) fn request_bytes(options: &CallOptions) -> usize {
    let shared: usize = options
        .prompt
        .iter()
        .flat_map(|message| &message.content)
        .map(broca_session_store::held::shared_leaf_bytes)
        .sum();
    std::mem::size_of::<CallOptions>() + options.prompt.heap_bytes() - shared
        + serde_heap_bytes(&*options.provider_options)
        + serde_heap_bytes(&options.declared_markers)
        + serde_heap_bytes(&options.step_context)
}

/// A decoded tool list, named by its tools' names and size so equal lists group together.
fn tools_held(tools: &std::sync::Arc<Vec<Tool>>) -> SetupHeld {
    let bytes = 2 * std::mem::size_of::<usize>()
        + std::mem::size_of::<Vec<Tool>>()
        + tools.capacity() * std::mem::size_of::<Tool>()
        + serde_heap_bytes(&**tools);
    let names: Vec<&str> = tools.iter().map(|tool| tool.name.as_str()).collect();
    SetupHeld {
        kind: "tools",
        content: format!("{bytes}:{}", names.join(",")),
        copy: std::sync::Arc::as_ptr(tools) as usize,
        bytes,
    }
}

/// The first episode's own facts, not shared decoded setup parts.
fn first_episode_bytes(first: &FirstEpisodeV1) -> usize {
    first.run_id.0.heap_bytes()
        + first.tool_names.heap_bytes()
        + first
            .plan
            .as_deref()
            .map_or(0, crate::store_authorization::frozen_plan_bytes)
}
