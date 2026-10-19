// Retained from cortexkit-role-compaction-provider 0.2.4 (MIT); legacy sync client only.
//! The shared descent rule for setup, step and hook declarations.
//!
//! A provider keeps one link per session lineage in the same conversation.
//! It must validate retained history and durably save the resulting link
//! before replying. Restore links only from provider-owned records, never
//! caller JSON: a caller cannot certify that the provider holds a prefix.

use serde::{Deserialize, Serialize};

use crate::compaction_provider::{errors, status::DescendsFrom};

/// An undeclared lineage can still acquire descent on a later step or hook.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct DescentLink {
    descends_from: Option<DescendsFrom>,
}

/// A rejected declaration never changes the saved link.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DescentProblem {
    HistoryUnreadable,
    Conflict,
}

impl DescentProblem {
    pub fn code(self) -> &'static str {
        match self {
            Self::HistoryUnreadable => errors::refuse_codes::HISTORY_UNREADABLE,
            Self::Conflict => errors::INVALID_PARAMS,
        }
    }

    pub fn field(self) -> Option<&'static str> {
        match self {
            Self::HistoryUnreadable => None,
            Self::Conflict => Some("lineage_id"),
        }
    }
}

impl DescentLink {
    pub fn descends_from(&self) -> Option<&DescendsFrom> {
        self.descends_from.as_ref()
    }

    /// Validate and save the first declaration; absence and identical repeats
    /// are no-ops. `holds_prefix` must check the named parent and cut exist in
    /// this conversation and every retained message through the cut is held.
    /// It is not called for an absent, repeated or conflicting declaration.
    pub fn declare(
        &mut self,
        declaration: Option<&DescendsFrom>,
        holds_prefix: impl FnOnce(&DescendsFrom) -> bool,
    ) -> Result<(), DescentProblem> {
        let Some(declaration) = declaration else {
            return Ok(());
        };
        if let Some(saved) = &self.descends_from {
            return if saved == declaration {
                Ok(())
            } else {
                Err(DescentProblem::Conflict)
            };
        }
        if !holds_prefix(declaration) {
            return Err(DescentProblem::HistoryUnreadable);
        }
        self.descends_from = Some(declaration.clone());
        Ok(())
    }
}
