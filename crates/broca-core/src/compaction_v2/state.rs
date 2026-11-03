//! Accepted and applied views are separate durable facts. An acknowledgement
//! confirms storage, not a change to a request already in progress.

use async_trait::async_trait;
use cortexkit_role_compaction::{
    notice::Event,
    read::Entry,
    rules::ProviderRules,
    view::{AppliedView, EditOp, View, ViewRefusal},
};

use super::views;

#[derive(Clone, Debug, Default)]
pub struct State {
    latest: Option<View>,
    applied: Option<View>,
    marked_version: Option<u64>,
}

/// Each method returns only after its record has crossed the durability barrier.
/// The WAL adapter owns record encoding, including deduplication of stored edits.
#[async_trait]
pub trait Journal: Send + Sync {
    async fn accepted(&self, previous: Option<&View>, view: &View) -> Result<(), String>;
    async fn applied(&self, step: u64, view: &View) -> Result<(), String>;
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
        self.latest.as_ref()
    }
    pub fn applied(&self) -> Option<&View> {
        self.applied.as_ref()
    }
    pub fn applied_identity(&self) -> Option<AppliedView> {
        self.applied.as_ref().map(View::applied)
    }

    pub fn restore_marker(&mut self, version: u64) {
        self.marked_version = Some(version);
    }

    /// Restore full views only after rebuilding their stored edit references.
    /// Never trust a checksum carried in a record without recomputing it.
    pub fn restore_accepted(&mut self, view: View) -> Result<(), String> {
        if !view.verify() {
            return Err(format!(
                "compaction view version {} checksum mismatch",
                view.version
            ));
        }
        if self
            .latest
            .as_ref()
            .is_some_and(|previous| previous.version >= view.version)
        {
            return Err(format!(
                "compaction view version {} is not newer than the stored view",
                view.version
            ));
        }
        self.latest = Some(view);
        Ok(())
    }

    pub fn restore_applied(&mut self, identity: &AppliedView) -> Result<(), String> {
        let view = self
            .latest
            .as_ref()
            .filter(|view| view.applied() == *identity)
            .or_else(|| {
                self.applied
                    .as_ref()
                    .filter(|view| view.applied() == *identity)
            })
            .ok_or_else(|| {
                format!(
                    "compaction applied view version {} checksum {} has no matching stored view",
                    identity.version, identity.checksum
                )
            })?;
        self.applied = Some(view.clone());
        Ok(())
    }

    pub async fn accept(
        &mut self,
        journal: &dyn Journal,
        entries: &[Entry],
        view: View,
        rules: &ProviderRules,
        accepts_images: bool,
    ) -> Result<Result<(), ViewRefusal>, String> {
        if let Err(refusal) = views::check(entries, self.latest(), &view, rules, accepts_images) {
            return Ok(Err(refusal));
        }
        journal.accepted(self.latest(), &view).await?;
        self.latest = Some(view);
        Ok(Ok(()))
    }

    /// Run at a boundary before Setup, and again after an accepted ride answer.
    /// `next_mid` is the host's stable id of the first future entry, needed when
    /// the cut covers the whole current transcript.
    pub async fn apply(
        &mut self,
        journal: &dyn Journal,
        boundary: Boundary<'_>,
    ) -> Result<Vec<Event>, String> {
        let Some(view) = self.latest.as_ref() else {
            return Ok(vec![]);
        };
        let already_applied = self
            .applied
            .as_ref()
            .is_some_and(|applied| applied.applied() == view.applied());
        if !already_applied {
            if let Err(refusal) = views::check_boundary(
                boundary.entries,
                view,
                boundary.rules,
                boundary.accepts_images,
            ) {
                return Ok(vec![Event::ViewRefused {
                    version: view.version,
                    refusal,
                }]);
            }
            journal.applied(boundary.step, view).await?;
            self.applied = Some(view.clone());
        }
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
                .marker(view, first_kept_mid, first_kept_ordinal, checkpoint)
                .await?;
            self.marked_version = Some(view.version);
            events.push(Event::MarkerWritten {
                version: view.version,
            });
        }
        Ok(events)
    }
}

pub struct Boundary<'a> {
    pub step: u64,
    pub entries: &'a [Entry],
    pub rules: &'a ProviderRules,
    pub accepts_images: bool,
    pub next_mid: &'a str,
}
