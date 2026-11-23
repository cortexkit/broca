//! A session's storage policy under llm-runner `session_retention`: the policy its starter froze,
//! or the one broca assigns from where the session is stored, and when the policy has it deleted.
//!
//! The starter is the session's first send that carries a policy. It freezes that policy (a
//! `Retention` `Set` record), and the policy decides the root a new session is stored in
//! ([`class_for`]). A session without one keeps a policy broca assigns ([`assigned`]): what its
//! storage already did to it before the policy existed, so nothing about the session changes
//! until its owner sends a policy.

use broca_records::session_storage::StorageClass;
use cortexkit_role_llm_runner::session_retention::{Keep, RetentionPolicy, RetentionSource};

pub use broca_wire::{MAX_UNRELEASED_HOLD_MS, RETENTION_MAX_DAYS as MAX_DAYS};

const DAY_MS: u64 = 86_400_000;

/// Why a replacement policy that changes `backup` is refused (`backup_unsupported`): a
/// session's backup class is the root it was created in, and broca never moves a session
/// between roots.
pub const BACKUP_FIXED_AT_CREATION: &str = "backup_fixed_at_creation";

/// The root a new session carrying `policy` is created in: a backed-up session in the durable
/// root, the one backups capture; one kept forever without backup in the ordinary root; one
/// kept for days or until released in the ephemeral root, which every retention pass reads.
pub fn class_for(policy: &RetentionPolicy) -> StorageClass {
    if policy.backup {
        StorageClass::Durable
    } else if policy.keep == Keep::Forever {
        StorageClass::Ordinary
    } else {
        StorageClass::Ephemeral
    }
}

/// The policy broca assigns a session whose starter sent none: what its storage already does.
/// A session sent with the older `retention` seconds is kept that many days, rounded up to a
/// whole day (starters send whole days); its deletion still follows the seconds it was sent
/// with. Otherwise a session in the durable root is kept forever and backed up, and any other
/// is kept forever without backup.
pub fn assigned(class: StorageClass, retention_secs: Option<u64>) -> RetentionPolicy {
    match retention_secs {
        Some(seconds) => {
            let days = seconds.div_ceil(DAY_MS / 1000).max(1);
            RetentionPolicy::new(
                Keep::days(u32::try_from(days).unwrap_or(MAX_DAYS).min(MAX_DAYS)),
                0,
                false,
                false,
            )
        }
        None => RetentionPolicy::new(Keep::forever(), 0, class == StorageClass::Durable, false),
    }
}

/// The policy a session is kept by, and who set it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Effective {
    pub policy: RetentionPolicy,
    pub source: RetentionSource,
}

/// The session's policy: the starter's frozen one when it has one (`frozen`, as its `Set`
/// record stored it), else the one broca assigns it. A frozen policy that no longer decodes is
/// damage, refused rather than replaced by an assigned one.
pub fn effective(
    frozen: Option<&serde_json::Value>,
    class: StorageClass,
    retention_secs: Option<u64>,
) -> Result<Effective, String> {
    match frozen {
        Some(value) => RetentionPolicy::from_value(value)
            .map(|policy| Effective {
                policy,
                source: RetentionSource::starter(),
            })
            .map_err(|field| {
                format!("the session's frozen retention policy is damaged at {field}")
            }),
        None => Ok(Effective {
            policy: assigned(class, retention_secs),
            source: RetentionSource::runner(),
        }),
    }
}

/// The policy the session whose state is `operations`, stored in a root of `class`, is kept by.
/// Its older retention is its latest run's, which a send that shortened it set, else its first
/// run's.
pub fn of_session(
    operations: &broca_session_store::fold::operations::Operations,
    class: StorageClass,
) -> Result<Effective, String> {
    let retention_secs = operations
        .run
        .as_ref()
        .and_then(|run| run.start.as_ref())
        .and_then(|start| start.retention_secs)
        .or_else(|| {
            operations
                .owner
                .first_start
                .as_ref()
                .and_then(|start| start.retention_secs)
        });
    effective(operations.retention_policy.as_ref(), class, retention_secs)
}

/// Fills `reply`'s retention members for the session whose state is `operations`, stored in a
/// root of `class`, with last durable activity `activity_ms`: its policy and who set it, and
/// once its owner released it, when and the earliest time it may be deleted. A session never
/// written has none. broca saves no request dumps (`session.request_dump` rebuilds requests
/// from records instead), so a policy asking for them is flagged.
pub fn fill_head(
    reply: &mut broca_wire::HeadResponse,
    operations: &broca_session_store::fold::operations::Operations,
    class: StorageClass,
    activity_ms: Option<u64>,
) -> Result<(), String> {
    use cortexkit_role_llm_runner::session_retention::{Released, RetentionState};
    if operations.owner.first_start.is_none() {
        return Ok(());
    }
    let Effective { policy, source } = of_session(operations, class)?;
    let mut state = RetentionState::new(source);
    if policy.request_dumps {
        state = state.with_request_dumps_unsupported();
    }
    reply.retention_policy = Some(policy);
    reply.retention_state = Some(state);
    reply.released = operations
        .released_at_ms
        .map(|at_ms| Released::new(at_ms, at_ms.max(activity_ms.unwrap_or(0))));
    Ok(())
}

/// When a session kept by a starter's policy, or released, is due for deletion; `None` when it
/// isn't due yet at any time. Nothing is due while a run is active or paused or a send is queued
/// (`busy`). `activity_ms` is the session's last durable activity (sends and run lifecycle;
/// never cache warms or maintenance). With no floor, a release is due at the later of the
/// release and the last activity. `keep: {days}` is due that many days after the last
/// activity, and an unreleased `keep: "none"` [`MAX_UNRELEASED_HOLD_MS`] after it. A session
/// with no policy frozen by its starter that was sent the older retention seconds has its deadline from
/// them instead (`legacy`). The earliest of these applies.
pub fn deadline(
    starter: Option<&RetentionPolicy>,
    released_at_ms: Option<u64>,
    legacy: Option<u64>,
    busy: bool,
    activity_ms: Option<u64>,
) -> Option<u64> {
    if busy {
        return None;
    }
    let activity = activity_ms.unwrap_or(0);
    let kept = starter.and_then(|policy| match policy.keep {
        Keep::Days { days } => Some(activity.saturating_add(u64::from(days) * DAY_MS)),
        Keep::None => Some(activity.saturating_add(MAX_UNRELEASED_HOLD_MS)),
        // `forever`, and a keep this build doesn't know, which deletes nothing.
        _ => None,
    });
    let released = released_at_ms.map(|at| at.max(activity));
    [kept, released, legacy].into_iter().flatten().min()
}

/// Why a send is refused for its storage policy, or because the session was released. Nothing
/// the send asked for is written.
#[derive(Debug, Clone, PartialEq)]
pub enum Refusal {
    /// The session's owner released it, and it is deleted at `delete_after_ms` or once its
    /// run ends (`session_released`, retryable: after deletion a send starts a new session).
    Released { delete_after_ms: u64 },
    /// The policy differs from the one the session keeps, `frozen` (`retention_conflict`).
    Conflict { frozen: RetentionPolicy },
    /// The policy would change whether the session is backed up (`backup_unsupported`).
    BackupFixed,
}

impl Refusal {
    /// The refusal's wire code.
    pub fn code(&self) -> &'static str {
        use cortexkit_role_llm_runner::errors;
        match self {
            Self::Released { .. } => errors::SESSION_RELEASED,
            Self::Conflict { .. } => errors::RETENTION_CONFLICT,
            Self::BackupFixed => errors::BACKUP_UNSUPPORTED,
        }
    }

    /// The refusal's wire detail.
    pub fn detail(&self) -> serde_json::Value {
        match self {
            Self::Released { delete_after_ms } => serde_json::json!(
                cortexkit_role_llm_runner::session_retention::SessionReleasedDetail::new(
                    *delete_after_ms
                )
            ),
            Self::Conflict { frozen } => serde_json::json!({ "frozen": frozen }),
            Self::BackupFixed => serde_json::json!({ "reason": BACKUP_FIXED_AT_CREATION }),
        }
    }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Released { .. } => {
                f.write_str("the session was released; it takes no send until it is deleted")
            }
            Self::Conflict { .. } => {
                f.write_str("the session keeps another retention policy, which a send can't change")
            }
            Self::BackupFixed => f.write_str(
                "whether a session is backed up is fixed when it is created; a policy can't change it",
            ),
        }
    }
}

#[cfg(test)]
#[path = "retention_policy_tests.rs"]
mod tests;
