use std::future::Future;

use broca_core::{SessionOwnershipV1, WalStore};
use broca_protocol::BindIdentity;

/// Both send and stop admission recognise complete policy before requesting a repair.
/// A checkpoint that records the initial plan and proves no session refresh was accepted
/// already knows the sender grants, so it needs no prefix read.
pub(crate) async fn resolve<F, Fut>(
    ownership: SessionOwnershipV1,
    repair: F,
) -> Result<SessionOwnershipV1, String>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<Option<SessionOwnershipV1>, String>>,
{
    if ownership.sender_admission_complete() {
        return Ok(ownership);
    }
    repair()
        .await?
        .filter(SessionOwnershipV1::sender_admission_complete)
        .ok_or_else(|| "effective sender policy is still unknown".to_owned())
}

/// Authorization cannot publish a repaired checkpoint before the caller is admitted.
/// Older refreshed checkpoints therefore repair only the in-memory policy at this barrier.
pub(crate) fn read_prefix(
    wal: &dyn WalStore,
    key: &BindIdentity,
    safe_head: u64,
) -> Result<Option<SessionOwnershipV1>, String> {
    let range = wal
        .read_records_range_for_authorization(key, 0, Some(safe_head), None)
        .map_err(|error| error.to_string())?;
    let records = crate::durable_read::DurableRead::classify(&range, 0, Some(safe_head))
        .records()
        .map_err(|error| format!("sender policy prefix: {error}"))?;
    broca_core::AdmissionCheckpointV1::from_frames(records)
        .map(|checkpoint| checkpoint.ownership)
        .map_err(|error| error.to_string())
}

pub(crate) fn interrupt_granted(ownership: &SessionOwnershipV1, module: &str) -> bool {
    ownership
        .effective_senders()
        .iter()
        .any(|sender| sender.module == module && sender.interrupt)
}

/// Retain only identity and sender grants, not the first plan's provider text or tool catalog.
pub(crate) fn compact(ownership: &SessionOwnershipV1) -> SessionOwnershipV1 {
    let mut compact = ownership.for_owner_access();
    let mut senders = ownership.effective_senders();
    for sender in &mut senders {
        sender.templates.clear();
    }
    compact.sender_admission = Some(broca_core::admission::SenderAdmissionV1 {
        version: 1,
        accepted_senders: Some(senders),
    });
    compact
}
