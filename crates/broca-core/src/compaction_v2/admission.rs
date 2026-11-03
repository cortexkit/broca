//! Mode selection and catalog resolution happen at the caller. This check must
//! run before queueing or recording a mode-3 send's resolved configuration.

pub fn check_context_limit(
    mode3: bool,
    manifest: Option<&broca_wal::PlanManifest>,
    context_limit: Option<u64>,
) -> Result<(), crate::RunError> {
    if mode3 && manifest.is_some_and(crate::compaction::has_compaction) && context_limit.is_none() {
        return Err(crate::RunError::CompactionContextLimitMissing);
    }
    Ok(())
}
