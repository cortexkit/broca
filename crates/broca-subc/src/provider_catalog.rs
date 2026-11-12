use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use subc_control::CatalogEntry;

/// Read a module manifest's provided capabilities from the daemon catalog.
pub fn catalog_entry_provides(entry: &CatalogEntry, capability: &str) -> bool {
    entry.capabilities.as_ref().is_some_and(|capabilities| {
        capabilities
            .provides
            .iter()
            .any(|provided| provided == capability)
    })
}

pub(crate) const AFT_BACKGROUND_TASKS: &str = "scope-background-tasks/v1";

/// Compatibility decisions belong to one catalog snapshot. Replacing the snapshot permits
/// another real-harness attempt after a provider upgrade, without polling the daemon.
#[derive(Debug)]
pub(crate) struct ProviderCatalog {
    entries: Vec<CatalogEntry>,
    pub(crate) aft_harness: Arc<AftHarness>,
}

impl From<Vec<CatalogEntry>> for ProviderCatalog {
    fn from(entries: Vec<CatalogEntry>) -> Self {
        let accepts_broca = entries.iter().any(|entry| {
            entry.module_id == "aft" && catalog_entry_provides(entry, AFT_BACKGROUND_TASKS)
        });
        Self {
            entries,
            aft_harness: Arc::new(AftHarness(AtomicBool::new(accepts_broca))),
        }
    }
}

impl std::ops::Deref for ProviderCatalog {
    type Target = [CatalogEntry];

    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

#[derive(Debug)]
pub(crate) struct AftHarness(AtomicBool);

impl AftHarness {
    pub(crate) fn accepts_broca(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    /// Only a successful fallback to the `runner` harness establishes that binding as `runner`
    /// still works.
    pub(crate) fn remember_runner(&self) -> bool {
        self.0.swap(false, Ordering::AcqRel)
    }
}
