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

/// The catalog's entry for `module_id`, or, for a module that changed its id
/// ([`broca_protocol::RENAMED_MODULES`]), the entry under its other id when only that one is
/// listed. Frozen tools, plan items and held calls name the id the module had when they were
/// made, so this is how they find the module after its rename (and before it, for a plan that
/// already names the new id).
pub fn find_module<'a>(catalog: &'a [CatalogEntry], module_id: &str) -> Option<&'a CatalogEntry> {
    catalog
        .iter()
        .find(|entry| entry.module_id == module_id)
        .or_else(|| {
            let other = broca_protocol::other_module_name(module_id)?;
            catalog.iter().find(|entry| entry.module_id == other)
        })
}

/// The id `module_id` is listed under in `catalog` ([`find_module`]): `module_id` itself unless
/// only a renamed module's other id is listed.
pub fn live_module_id(catalog: &[CatalogEntry], module_id: &str) -> String {
    find_module(catalog, module_id)
        .map_or_else(|| module_id.to_owned(), |entry| entry.module_id.clone())
}

/// Compatibility decisions belong to one catalog snapshot. Replacing the snapshot permits
/// another real-harness attempt after a provider upgrade, without polling the daemon.
#[derive(Debug)]
pub(crate) struct ProviderCatalog {
    entries: Arc<Vec<CatalogEntry>>,
    pub(crate) aft_harness: Arc<AftHarness>,
}

impl From<Vec<CatalogEntry>> for ProviderCatalog {
    fn from(entries: Vec<CatalogEntry>) -> Self {
        Arc::new(entries).into()
    }
}

/// Shares a connection's catalog reply instead of copying it.
impl From<Arc<Vec<CatalogEntry>>> for ProviderCatalog {
    fn from(entries: Arc<Vec<CatalogEntry>>) -> Self {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_renamed_module_is_found_under_the_id_the_catalog_lists() {
        let after = vec![CatalogEntry::new("prefrontal", vec![], vec![])];
        assert_eq!(live_module_id(&after, "prefrontal-core"), "prefrontal");
        assert_eq!(live_module_id(&after, "prefrontal"), "prefrontal");
        let before = vec![CatalogEntry::new("prefrontal-core", vec![], vec![])];
        assert_eq!(live_module_id(&before, "prefrontal"), "prefrontal-core");
        // While both are listed, each id finds its own entry.
        let both = vec![
            CatalogEntry::new("prefrontal-core", vec![], vec![]),
            CatalogEntry::new("prefrontal", vec![], vec![]),
        ];
        assert_eq!(live_module_id(&both, "prefrontal-core"), "prefrontal-core");
        assert_eq!(live_module_id(&both, "prefrontal"), "prefrontal");
        // Other modules are found only by their own id; an unlisted one keeps it.
        assert!(find_module(&after, "prefrontal2").is_none());
        assert_eq!(live_module_id(&after, "aft"), "aft");
    }
}
