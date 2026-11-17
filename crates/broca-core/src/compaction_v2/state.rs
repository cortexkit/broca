//! Accepted and applied views are separate durable facts. An acknowledgement
//! confirms storage, not a change to a request already in progress.

use cortexkit_compaction_view::rules::Floor;
use cortexkit_role_compaction::{
    notice::Event,
    read::Entry,
    rules::ProviderRules,
    view::{AppliedView, EditOp, View, ViewRefusal},
};

use super::{received::ReceivedView, views};

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct State {
    latest: Option<ReceivedView>,
    applied: Option<ReceivedView>,
    marked_version: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) marker: Option<Box<broca_wal::WalRecord>>,
    #[serde(default)]
    step_id: Option<u64>,
    #[serde(default)]
    step_view: Option<View>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    latest_applied_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    applied_by: Option<String>,
}

/// Each method returns only after its record has crossed the durability barrier.
/// The WAL adapter owns record encoding, including deduplication of stored edits.
///
/// Its methods are plain `async fn`s, so a journal whose writer's futures can't move between
/// threads can implement it too; a caller whose future must be `Send` gets that from the
/// journal it passes, since every function here takes the journal by its concrete type.
#[allow(async_fn_in_trait)]
pub trait Journal {
    /// Called when a published view is refused, so the session's journal can log it; the
    /// refusal itself writes nothing. A journal with no log of its own keeps this no-op.
    fn view_refused(&self, _publication: &super::diagnostics::Publication, _code: &str) {}
    async fn accepted(
        &self,
        previous: Option<&ReceivedView>,
        view: &ReceivedView,
    ) -> Result<(), String>;
    /// `view` applied at `step`. `head_kept` says it renders the conversation's first message
    /// as the view applied before it did (see [`head_kept`]).
    async fn applied(&self, step: u64, view: &View, head_kept: bool) -> Result<(), String>;
    async fn marker(
        &self,
        view: &View,
        first_kept_mid: &str,
        first_kept_ordinal: u64,
        checkpoint: u64,
    ) -> Result<(), String>;
}

impl State {
    pub fn latest(&self) -> Option<&View> {
        self.latest.as_ref().map(ReceivedView::view)
    }
    pub fn latest_received(&self) -> Option<&ReceivedView> {
        self.latest.as_ref()
    }
    pub fn applied(&self) -> Option<&View> {
        self.applied.as_ref().map(ReceivedView::view)
    }
    pub fn applied_received(&self) -> Option<&ReceivedView> {
        self.applied.as_ref()
    }
    pub fn applied_identity(&self) -> Option<AppliedView> {
        self.applied().map(View::applied)
    }

    pub fn applied_by(&self) -> Option<&str> {
        self.applied_by.as_deref()
    }

    /// The module that handed the newest accepted view over, which becomes `applied_by` once a
    /// step applies that view. `None` for a view a run's step accepted.
    pub fn accepted_by(&self) -> Option<&str> {
        self.latest_applied_by.as_deref()
    }

    pub fn restore_accepted_by(&mut self, module: Option<String>) {
        self.latest_applied_by = module;
    }

    pub fn restore_marker(&mut self, version: u64) {
        self.marked_version = Some(version);
    }
    pub fn marker_version(&self) -> Option<u64> {
        self.marked_version
    }

    pub fn begin_run(&mut self) {
        self.step_id = None;
        self.step_view = None;
    }

    /// Repeated StepStarted records on resume keep the original request's view.
    /// Accepting a newer view during a step does not change this frozen snapshot.
    pub fn start_step(&mut self, step: u64) {
        if self.step_id != Some(step) {
            self.step_id = Some(step);
            self.step_view = self.applied().cloned();
        }
    }

    pub fn step_is_frozen(&self, step: u64) -> bool {
        self.step_id == Some(step)
    }

    pub fn view_for_step(&self, step: u64) -> Option<&View> {
        if self.step_id == Some(step) {
            self.step_view.as_ref()
        } else {
            self.applied()
        }
    }

    pub fn restore_applied_at(&mut self, step: u64, identity: &AppliedView) -> Result<(), String> {
        self.restore_applied(identity)?;
        if self.step_id == Some(step) {
            self.step_view = self.applied().cloned();
        }
        Ok(())
    }

    /// Restore full views only after rebuilding their stored edit references.
    /// Never trust a checksum carried in a record without recomputing it.
    pub fn restore_accepted(&mut self, received: impl Into<ReceivedView>) -> Result<(), String> {
        let received = received.into();
        received.check_size()?;
        let view = received.view();
        if !view.verify() {
            return Err(format!(
                "compaction view version {} checksum mismatch",
                view.version
            ));
        }
        if self
            .latest
            .as_ref()
            .is_some_and(|previous| previous.view().version >= view.version)
        {
            return Err(format!(
                "compaction view version {} is not newer than the stored view",
                view.version
            ));
        }
        self.latest = Some(received);
        self.latest_applied_by = None;
        Ok(())
    }

    pub fn restore_applied(&mut self, identity: &AppliedView) -> Result<(), String> {
        let applied_by = if self
            .latest()
            .is_some_and(|view| view.applied() == *identity)
        {
            self.latest_applied_by.clone()
        } else {
            self.applied_by.clone()
        };
        let view = self
            .latest_received()
            .filter(|view| view.view().applied() == *identity)
            .or_else(|| {
                self.applied
                    .as_ref()
                    .filter(|view| view.view().applied() == *identity)
            })
            .ok_or_else(|| {
                format!(
                    "compaction applied view version {} checksum {} has no matching stored view",
                    identity.version, identity.checksum
                )
            })?;
        self.applied = Some(view.clone());
        self.applied_by = applied_by;
        Ok(())
    }

    /// `floor` is the session's signed floor (`host::view_floor`).
    pub async fn accept(
        &mut self,
        journal: &impl Journal,
        entries: &[Entry],
        received: impl Into<ReceivedView>,
        rules: &ProviderRules,
        floor: Floor<'_>,
    ) -> Result<Result<(), ViewRefusal>, String> {
        let received = received.into();
        received.check_size()?;
        let view = received.view();
        if let Err(refusal) = views::check(entries, self.latest(), view, rules, floor) {
            return Ok(Err(refusal));
        }
        journal.accepted(self.latest.as_ref(), &received).await?;
        self.latest = Some(received);
        self.latest_applied_by = None;
        Ok(Ok(()))
    }

    /// Run at a boundary before Setup, and again after an accepted ride answer.
    /// `next_mid` is the host's stable id of the first future entry, needed when
    /// the cut covers the whole current transcript.
    pub async fn apply(
        &mut self,
        journal: &impl Journal,
        boundary: Boundary<'_>,
    ) -> Result<Vec<Event>, String> {
        let Some(received) = self.latest.as_ref() else {
            return Ok(vec![]);
        };
        let view = received.view();
        let already_applied = self
            .applied
            .as_ref()
            .is_some_and(|applied| applied.view().applied() == view.applied());
        if !already_applied {
            if let Err(refusal) =
                views::check_boundary(boundary.entries, view, boundary.rules, boundary.floor)
            {
                journal.view_refused(
                    &super::diagnostics::Publication::from_view(view),
                    &super::diagnostics::refusal_code(&refusal),
                );
                return Ok(vec![Event::ViewRefused {
                    version: view.version,
                    refusal,
                }]);
            }
            let previous = self.applied.as_ref().map(ReceivedView::view);
            journal
                .applied(boundary.step, view, head_kept(&boundary, previous, view))
                .await
                .inspect_err(|_| {
                    journal.view_refused(
                        &super::diagnostics::Publication::from_view(view),
                        "internal_error",
                    );
                })?;
            self.applied = Some(received.clone());
            self.applied_by = self.latest_applied_by.clone();
            if self.step_id == Some(boundary.step) {
                self.step_view = self.applied().cloned();
            }
        }
        self.recover_marker(journal, boundary).await
    }

    /// A crash can leave the applied record durable but not its marker. Finish
    /// that marker for the frozen step without adopting a newer accepted view.
    pub async fn recover_marker(
        &mut self,
        journal: &impl Journal,
        boundary: Boundary<'_>,
    ) -> Result<Vec<Event>, String> {
        let Some(view) = self.view_for_step(boundary.step) else {
            return Ok(vec![]);
        };
        if self.marked_version == Some(view.version) {
            return Ok(vec![]);
        }
        let view = view.clone();
        let mut events = Vec::new();
        if let Some(through) = view
            .edits
            .iter()
            .filter(|_| self.marked_version != Some(view.version))
            .find_map(|edit| match &edit.op {
                EditOp::Cut { through } => Some(through),
                _ => None,
            })
        {
            let position = boundary
                .entries
                .iter()
                .position(|entry| &entry.mid == through)
                .expect("checked cut target");
            let checkpoint = boundary.entries[..=position]
                .iter()
                .flat_map(|entry| &entry.parts)
                .filter_map(|part| part.tag)
                .max()
                .unwrap_or(0);
            let next = boundary.entries.get(position + 1);
            let first_kept_mid = next.map_or(boundary.next_mid, |entry| entry.mid.as_str());
            let first_kept_ordinal = next.map_or(
                boundary.entries.last().map_or(1, |entry| entry.ordinal + 1),
                |entry| entry.ordinal,
            );
            journal
                .marker(&view, first_kept_mid, first_kept_ordinal, checkpoint)
                .await?;
            self.marked_version = Some(view.version);
            events.push(Event::MarkerWritten {
                version: view.version,
            });
        }
        Ok(events)
    }
}

/// Whether `view` renders the conversation's first message as `previous` (or no view, before
/// any) did over `boundary`'s entries: the first item of each one's render plan is the same.
/// A view that keeps it rewrites the request only from a later message, so the provider still
/// reads the cached prefix through the first message. A view that changes the first message
/// (as a compaction provider's fold of the oldest history does) rewrites the whole
/// conversation. A plan either view can't make counts as changed, as every view did before
/// this was told apart.
fn head_kept(boundary: &Boundary<'_>, previous: Option<&View>, view: &View) -> bool {
    let first = |view: Option<&View>| {
        super::render::plan_for_render(boundary.entries, view, boundary.rules, boundary.floor)
            .ok()
            .and_then(|(_, plan)| plan.items.into_iter().next())
    };
    match (first(previous), first(Some(view))) {
        (Some(before), Some(after)) => before == after,
        _ => false,
    }
}

pub struct Boundary<'a> {
    pub step: u64,
    pub entries: &'a [Entry],
    pub rules: &'a ProviderRules,
    /// The session's signed floor (`host::view_floor`).
    pub floor: Floor<'a>,
    pub next_mid: &'a str,
}
