//! Per-session, in-memory call lane. Notices are serialized and coalesced, not
//! written to the log or published on a bus.

use std::{collections::BTreeMap, sync::Mutex, time::Duration};

use async_trait::async_trait;
use cortexkit_role_compaction::{
    notice::{Conditions, Event, NoticeAnswer, NoticeRequest},
    read::{Cursor, Entry},
    ride::{RideCause, RideRequest},
    setup::{SetupAnswer, SetupRequest},
    view::AppliedView,
    wire::Refusal,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::value::RawValue;

use super::{received::ReceivedView, views};

pub const CALL_DEADLINE: Duration = Duration::from_secs(2);
pub const COLD_RIDE_DEADLINE: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub enum CallError {
    Refused(Refusal),
    Unavailable,
}

/// Transport identity is supplied by the session-bound adapter, never in bodies.
#[async_trait]
pub trait Plane: Send + Sync {
    async fn call(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<Box<RawValue>, CallError>;
}

pub struct BoundPlane<'a> {
    pub plane: Option<&'a dyn crate::CompactionPlane>,
    pub provider: &'a str,
    pub bind: &'a broca_protocol::BindIdentity,
    pub scope: Option<&'a broca_protocol::SessionScope>,
}

#[async_trait]
impl Plane for BoundPlane<'_> {
    async fn call(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<Box<RawValue>, CallError> {
        let plane = self.plane.ok_or(CallError::Unavailable)?;
        let deadline = if method == "compaction.ride" {
            params
                .get("deadline_ms")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(CALL_DEADLINE.as_millis() as u64)
        } else {
            CALL_DEADLINE.as_millis() as u64
        };
        plane
            .call_v2(
                self.provider,
                self.bind,
                self.scope,
                method,
                params,
                deadline,
            )
            .await
    }
}

#[derive(Default)]
struct Pending {
    entries: BTreeMap<u64, (Entry, u64)>,
    entry_bytes: u64,
    entries_omitted: bool,
    changed: bool,
    observed_through: u64,
    // Only the latest assistant can still gain tool results or deliveries.
    // Older native entries are immutable, so no transcript-sized digest ledger is needed.
    assistant: Option<(u64, String)>,
    known_marker: Option<u64>,
    events: Vec<Event>,
    newest: Option<Cursor>,
}

struct Lane {
    seq: u64,
    ready: bool,
    setup_due: bool,
}

impl Default for Lane {
    fn default() -> Self {
        Self {
            seq: 0,
            ready: false,
            setup_due: true,
        }
    }
}

/// A single actor owns this object across runs. The lane lock spans each call;
/// producers only take `pending`, so a slow notice cannot accumulate a queue.
#[derive(Default)]
pub struct Runtime {
    pending: Mutex<Pending>,
    lane: tokio::sync::Mutex<Lane>,
}

impl Runtime {
    /// Observe durable entries. Completion metadata alone changes no digest and
    /// does not emit a notice, whereas final tool results and deliveries do.
    pub fn observe(&self, entries: &[Entry]) {
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(newest) = views::newest(entries) {
            if pending
                .newest
                .as_ref()
                .is_none_or(|previous| previous.ordinal <= newest.ordinal)
            {
                pending.newest = Some(newest);
            }
        }
        let through = pending.observed_through;
        let assistant = pending.assistant.clone();
        for entry in entries {
            let changed = entry.ordinal > through
                || assistant.as_ref().is_some_and(|(ordinal, digest)| {
                    *ordinal == entry.ordinal && digest != &entry.digest
                });
            if changed {
                pending.changed = true;
                if !pending.entries_omitted {
                    let compact = super::entries::compact(entry);
                    let bytes = broca_protocol::serialized_json_bytes(&compact);
                    if let Some((_, old_bytes)) =
                        pending.entries.insert(entry.ordinal, (compact, bytes))
                    {
                        pending.entry_bytes = pending.entry_bytes.saturating_sub(old_bytes);
                    }
                    pending.entry_bytes += bytes;
                    if pending.entry_bytes + 2 + pending.entries.len().saturating_sub(1) as u64
                        > 1_048_576
                    {
                        pending.entries.clear();
                        pending.entry_bytes = 0;
                        pending.entries_omitted = true;
                    }
                }
            }
            pending.observed_through = pending.observed_through.max(entry.ordinal);
            if entry.role == cortexkit_role_compaction::read::Role::Assistant
                && !entry.parts.is_empty()
                && pending
                    .assistant
                    .as_ref()
                    .is_none_or(|(ordinal, _)| *ordinal <= entry.ordinal)
            {
                pending.assistant = Some((entry.ordinal, entry.digest.clone()));
            }
        }
    }

    pub fn event(&self, event: Event) {
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Event::MarkerWritten { version } = &event {
            pending.known_marker = Some(*version);
        }
        if !pending.events.contains(&event) {
            pending.events.push(event);
        }
    }

    pub fn restore_marker(&self, version: Option<u64>) {
        let Some(version) = version else {
            return;
        };
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if pending.known_marker != Some(version) {
            pending.known_marker = Some(version);
            pending.events.push(Event::MarkerWritten { version });
        }
    }

    /// Called when serving starts, after a runner restart, before a send after
    /// failed Setup, and when the provider has lost the session. A prior run's
    /// successful Setup does not suppress these calls. If this returns false
    /// with no applied view, the caller ends the run with compaction_unavailable
    /// before any model I/O.
    pub async fn setup(&self, plane: &dyn Plane, request: &SetupRequest) -> bool {
        let mut lane = self.lane.lock().await;
        setup(&mut lane, plane, request).await
    }

    /// A failed Setup without an applied view is retried before the next send,
    /// not repeatedly within the turn that must already end as unavailable.
    pub async fn begin_send(&self, has_applied_view: bool) {
        let mut lane = self.lane.lock().await;
        if !lane.ready && !has_applied_view {
            lane.setup_due = true;
        }
    }

    pub async fn require_setup(&self) {
        let mut lane = self.lane.lock().await;
        lane.ready = false;
        lane.setup_due = true;
    }

    pub async fn ensure_setup(&self, plane: &dyn Plane, request: &SetupRequest) -> bool {
        let mut lane = self.lane.lock().await;
        if !lane.ready && lane.setup_due {
            setup(&mut lane, plane, request).await
        } else {
            lane.ready
        }
    }

    /// Acquiring the lane first waits for the in-flight notice. Then every
    /// pending batch is sent before the ride may use this same lane (N3).
    pub async fn flush(
        &self,
        plane: &dyn Plane,
        setup_request: &SetupRequest,
        conditions: &Conditions,
        applied: Option<&AppliedView>,
    ) {
        let mut lane = self.lane.lock().await;
        self.flush_locked(&mut lane, plane, setup_request, conditions, applied)
            .await;
    }

    async fn flush_locked(
        &self,
        lane: &mut Lane,
        plane: &dyn Plane,
        setup_request: &SetupRequest,
        conditions: &Conditions,
        applied: Option<&AppliedView>,
    ) {
        let mut restarted = false;
        loop {
            if !lane.ready {
                if !lane.setup_due || !setup(lane, plane, setup_request).await {
                    return;
                }
            }
            let batch = {
                let mut pending = self
                    .pending
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if !pending.changed && pending.events.is_empty() {
                    return;
                }
                lane.seq += 1;
                let mut notice = NoticeRequest::new(lane.seq, conditions.clone());
                notice.newest = pending.newest.clone();
                notice.applied_view = applied.cloned();
                notice.entries = std::mem::take(&mut pending.entries)
                    .into_values()
                    .map(|(entry, _)| entry)
                    .collect();
                pending.entry_bytes = 0;
                pending.entries_omitted = false;
                pending.changed = false;
                notice.events = std::mem::take(&mut pending.events);
                if serde_json::to_vec(&notice.entries)
                    .expect("entries serialize")
                    .len()
                    > 1_048_576
                {
                    notice.entries.clear();
                }
                notice
            };
            let answer: Result<NoticeAnswer, _> =
                call(plane, "compaction.notice", &batch, CALL_DEADLINE).await;
            match answer {
                Ok(NoticeAnswer::Received) => {}
                Ok(_) => {
                    tracing::debug!(
                        method = "compaction.notice",
                        "compaction provider requires Setup before further notices"
                    );
                    lane.ready = false;
                    lane.setup_due = true;
                    if restarted {
                        return;
                    }
                    restarted = true;
                }
                Err(_) => return,
            }
        }
    }

    /// A ride cannot overtake either an in-flight notice or changes accumulated
    /// while it was unanswered. Unknown and late ride answers mean `nothing`.
    pub async fn ride(
        &self,
        plane: &dyn Plane,
        setup_request: &SetupRequest,
        request: &RideRequest,
    ) -> Option<ReceivedView> {
        let mut lane = self.lane.lock().await;
        self.flush_locked(
            &mut lane,
            plane,
            setup_request,
            &request.conditions,
            request.applied_view.as_ref(),
        )
        .await;
        #[derive(Deserialize)]
        struct RawRideAnswer {
            answer: String,
            #[serde(default)]
            view: Option<Box<RawValue>>,
        }
        let answer: Result<RawRideAnswer, _> = call(
            plane,
            "compaction.ride",
            request,
            Duration::from_millis(request.deadline_ms),
        )
        .await;
        match answer {
            Ok(RawRideAnswer {
                answer,
                view: Some(view),
            }) if answer == "view" => ReceivedView::parse(&view).ok(),
            Err(CallError::Refused(ref refusal)) if refusal.code == "unknown_session" => {
                lane.ready = false;
                lane.setup_due = true;
                None
            }
            Ok(RawRideAnswer { answer, .. }) if answer == "nothing" => None,
            Ok(_) => {
                tracing::warn!(
                    method = "compaction.ride",
                    "compaction provider returned an unknown or malformed ride answer"
                );
                None
            }
            Err(_) => None,
        }
    }
}

async fn setup(lane: &mut Lane, plane: &dyn Plane, request: &SetupRequest) -> bool {
    lane.seq = 0;
    let answer: Result<SetupAnswer, _> =
        call(plane, "compaction.setup", request, CALL_DEADLINE).await;
    lane.ready = match answer {
        Ok(SetupAnswer::Ready) => true,
        Ok(_) => {
            tracing::warn!(
                method = "compaction.setup",
                "compaction provider returned an unknown Setup answer"
            );
            false
        }
        Err(_) => false,
    };
    lane.setup_due = false;
    lane.ready
}

async fn call<Q: Serialize, A: DeserializeOwned>(
    plane: &dyn Plane,
    method: &str,
    request: &Q,
    deadline: Duration,
) -> Result<A, CallError> {
    let params = serde_json::to_value(request).map_err(|_| CallError::Unavailable)?;
    let until = tokio::time::Instant::now() + deadline;
    let value = tokio::time::timeout_at(until, plane.call(method, params))
        .await
        .map_err(|_| CallError::Unavailable)??;
    if tokio::time::Instant::now() >= until {
        return Err(CallError::Unavailable);
    }
    serde_json::from_str(value.get()).map_err(|_| CallError::Unavailable)
}

pub fn detects() -> Vec<RideCause> {
    vec![
        RideCause::CacheExpired,
        RideCause::ChainReset,
        RideCause::ModelChange,
        RideCause::TierChange,
        RideCause::AccountChange,
        RideCause::SurfaceChange,
        RideCause::OverLimit,
    ]
}

#[cfg(test)]
mod pending_tests {
    use super::*;
    use cortexkit_role_compaction::{
        part::{Part, PartBody},
        read::Role,
    };

    #[test]
    fn compaction_v2_pending_notice_omits_bulk_without_retaining_a_transcript_queue() {
        let runtime = Runtime::default();
        let entry = Entry::new(
            1,
            "mid",
            Role::User,
            1,
            vec![Part::new(
                "0",
                PartBody::Text {
                    text: "x".repeat(1_048_577),
                },
            )],
        );
        runtime.observe(&[entry]);
        let pending = runtime.pending.lock().unwrap();
        assert!(pending.changed);
        assert!(pending.entries_omitted);
        assert!(pending.entries.is_empty());
        assert_eq!(pending.newest.as_ref().unwrap().ordinal, 1);
    }
}
