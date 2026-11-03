//! Compaction runner routes are bound only to the provider in the applied plan.
//! A pending refresh never conveys authority over a session.

use super::protocol::wire::{not_permitted_reasons, Refusal};

/// `mode3` must already be proved by the shared log classifier or cached head.
/// Unknown history is an I/O failure at the caller, never evidence of legacy mode.
pub fn check_bind(
    mode3: bool,
    provider: Option<&str>,
    caller: Option<&str>,
) -> Result<(), Refusal> {
    if !mode3 {
        return Err(Refusal::compaction_not_permitted(
            not_permitted_reasons::SESSION_NOT_HELD,
        ));
    }
    let provider = provider.ok_or_else(|| {
        Refusal::compaction_not_permitted(not_permitted_reasons::NO_COMPACTION_ITEM)
    })?;
    if caller != Some(provider) {
        return Err(Refusal::compaction_not_permitted(
            not_permitted_reasons::PROVIDER_MISMATCH,
        ));
    }
    Ok(())
}
