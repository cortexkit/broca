//! A retained send's retention. Whether a session is retained is fixed by its first send; a later
//! send may keep its retention or shorten it, never lengthen it.
//!
//! A send's storage policy (llm-runner `session_retention`) is admitted here too: the session's
//! first send carrying one freezes it, and a released session takes no send.

use super::*;
use crate::retention::policy::{self, Effective, Refusal};
use broca_records::session_storage::StorageClass;
use broca_session_store::record::{Retention, RetentionEvent};
use cortexkit_role_llm_runner::session_retention::Keep;

impl<B: SessionBackend + Send + Sync + 'static> StoreActor<B> {
    /// The retention a send's run starts with: the one it asks for, once checked against the
    /// session's current one, else the current one. The current one is the latest run's, which a
    /// send that shortened it set, else the first run's.
    pub(super) async fn send_retention(
        &self,
        requested: Option<u64>,
    ) -> Result<Option<u64>, SendOutcome> {
        let operations = self.session.operations();
        let first = operations.owner.first_start.as_ref();
        let current = operations
            .run
            .as_ref()
            .and_then(|run| run.start.as_ref())
            .and_then(|start| start.retention_secs)
            .or_else(|| first.and_then(|start| start.retention_secs));
        let exists = first.is_some();
        crate::retention::validate_retention(requested, current, exists)
            .map_err(SendOutcome::RetentionInvalid)?;
        Ok(requested.or(current))
    }

    /// The session's storage policy and who set it: its starter's frozen one, else the one broca
    /// assigns from the root it is stored in and its older retention.
    pub(super) fn storage_policy(&self) -> Result<Effective, String> {
        policy::of_session(self.session.operations(), self.storage_class())
    }

    /// The root this actor's session is stored in.
    pub(super) fn storage_class(&self) -> StorageClass {
        self.class_placement
            .as_ref()
            .map_or(StorageClass::Ordinary, |(class, _)| *class)
    }

    /// Admits a send against the session's retention: a released session takes no send, and a
    /// send's storage policy is checked against the one the session keeps. Returns the policy the
    /// send freezes, encoded for its `Retention` `Set` record: only a session's first send
    /// carrying one freezes it. A starter's frozen policy never changes: an equal one is accepted
    /// and freezes nothing, any other is a conflict. One broca assigned is the session's owner's
    /// (its first run's principal) to replace, once, and never in whether the session is backed
    /// up, which the root it was created in decides. A send from anyone else that carries a
    /// policy for such a session is refused as not the owner, even when the policy is equal.
    pub(super) fn admit_storage_policy(
        &self,
        request: &SubmissionRequest,
    ) -> Result<Option<serde_json::Value>, SendOutcome> {
        let operations = self.session.operations();
        if let Some(released_at_ms) = operations.released_at_ms {
            let delete_after_ms =
                released_at_ms.max(self.session.open_log_activity_ms().unwrap_or(0));
            return Err(SendOutcome::RetentionRefused(Refusal::Released {
                delete_after_ms,
            }));
        }
        let Some(sent) = request.retention_policy.as_ref() else {
            return Ok(None);
        };
        let encoded = || {
            self.mark_for_retention(sent.keep != Keep::Forever)
                .map_err(SendOutcome::Error)?;
            serde_json::to_value(sent).map_err(|e| SendOutcome::Error(e.to_string()))
        };
        if operations.owner.first_start.is_none() {
            return encoded().map(Some);
        }
        let kept = self.storage_policy().map_err(SendOutcome::Error)?;
        let conflict = || {
            SendOutcome::RetentionRefused(Refusal::Conflict {
                frozen: kept.policy.clone(),
            })
        };
        if kept.source != cortexkit_role_llm_runner::session_retention::RetentionSource::runner() {
            return if kept.policy == *sent {
                Ok(None)
            } else {
                Err(conflict())
            };
        }
        let first: Option<broca_protocol::BindPrincipal> = operations
            .owner
            .first_principal
            .clone()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| SendOutcome::Error(error.to_string()))?;
        match first {
            Some(first) if request.principal.as_ref() == Some(&first) => {}
            Some(first) => {
                return Err(SendOutcome::ScopeRefused(
                    broca_core::ScopeRefusal::OwnerMismatch(Box::new(
                        broca_core::OwnerMismatch::FirstPrincipal {
                            first,
                            presented_principal: request.principal.clone(),
                        },
                    )),
                ));
            }
            // The first send recorded no principal, so the session has no owner to replace its
            // policy: it stays the one broca assigned.
            None => return Err(conflict()),
        }
        if kept.policy == *sent {
            return Ok(None);
        }
        if sent.backup != kept.policy.backup {
            return Err(SendOutcome::RetentionRefused(Refusal::BackupFixed));
        }
        encoded().map(Some)
    }

    /// Records `session.release` from `principal`, synced, and stops the session's idle cache
    /// warming. Only the principal that made the session's first send releases it, never a
    /// parent scope's owner; a repeat records nothing again. The session is deleted once no run
    /// is active or paused and no send is queued: by this actor when its retention deadline,
    /// the release itself, comes ([`Self::release_reply`] answers then), and by retention's pass
    /// after a restart.
    pub(super) async fn release(
        &mut self,
        principal: Option<broca_protocol::BindPrincipal>,
    ) -> Result<(), crate::actor::ReleaseOutcome> {
        use crate::actor::ReleaseOutcome;
        let operations = self.session.operations();
        let first: Option<broca_protocol::BindPrincipal> = operations
            .owner
            .first_principal
            .clone()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| ReleaseOutcome::Error(error.to_string()))?;
        if principal != first {
            return Err(match first {
                Some(first) => {
                    ReleaseOutcome::NotOwner(Box::new(broca_core::OwnerMismatch::FirstPrincipal {
                        first,
                        presented_principal: principal,
                    }))
                }
                // The first send recorded no principal, so only a caller presenting none matches.
                None => ReleaseOutcome::Unowned,
            });
        }
        if operations.released_at_ms.is_none() {
            self.mark_for_retention(true)
                .map_err(ReleaseOutcome::Error)?;
            let record = Record::Retention(Retention {
                event: RetentionEvent::Released,
                at_ms: (self.now_ms)(),
                policy: None,
            });
            self.append_own(record)
                .await
                .map_err(ReleaseOutcome::Error)?;
        }
        if let Some(registry) = &self.registry {
            registry.stop_idle_warm(&self.key, "released");
        }
        Ok(())
    }

    /// Answers a recorded release still waiting for its answer: `deleted` when this actor just
    /// deleted the session, else `pending`.
    pub(super) fn release_reply(&mut self, deleted: bool) {
        use cortexkit_role_llm_runner::session_retention::ReleaseState;
        if let Some(reply) = self.release_waiter.take() {
            let state = if deleted {
                ReleaseState::deleted()
            } else {
                ReleaseState::pending()
            };
            let _ = reply.send(crate::actor::ReleaseOutcome::Released(state));
        }
    }

    /// Marks the session for retention's passes (`retention::store::marks`) before a record that
    /// may give it a deadline (`due`), when it is kept outside the ephemeral root, which every
    /// pass reads whole. Synced first, so no session comes due unmarked.
    pub(super) fn mark_for_retention(&self, due: bool) -> Result<(), String> {
        let outside = self
            .class_placement
            .as_ref()
            .is_some_and(|(class, _)| *class != StorageClass::Ephemeral);
        let (true, true, Some(root)) = (due, outside, self.recovery_index.as_ref()) else {
            return Ok(());
        };
        crate::retention::store::marks::mark(
            root,
            &broca_session_store::backend::SessionAddr(broca_core::session_addr(&self.key)),
        )
        .map_err(|error| format!("the session couldn't be marked for retention: {error}"))
    }

    /// `record` and, when its send freezes a storage policy, the policy's `Retention` `Set`: one
    /// batch, so the send and its policy are durable together or not at all.
    pub(super) fn with_retention_set(
        &self,
        record: Record,
        policy: Option<serde_json::Value>,
    ) -> Vec<Record> {
        let mut batch = vec![record];
        batch.extend(self.retention_set(policy));
        batch
    }

    /// The `Retention` `Set` record of the storage policy a send freezes, if it freezes one.
    pub(super) fn retention_set(&self, policy: Option<serde_json::Value>) -> Option<Record> {
        policy.map(|policy| {
            Record::Retention(Retention {
                event: RetentionEvent::Set,
                at_ms: (self.now_ms)(),
                policy: Some(policy),
            })
        })
    }

    pub(super) fn retention_timestamp(&self) -> Option<u64> {
        self.session
            .operations()
            .owner
            .first_start
            .as_ref()?
            .retention_secs?;
        Some(
            self.retention_clock
                .as_ref()
                .map_or_else(crate::actor::epoch_ms, |clock| clock.now_ms()),
        )
    }
}
