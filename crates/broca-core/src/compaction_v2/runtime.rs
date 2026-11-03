//! Per-session, in-memory call lane. Notices are serialized and coalesced, not
//! written to the log or published on a bus.

use std::{collections::BTreeMap, sync::Mutex, time::Duration};

use async_trait::async_trait;
use cortexkit_role_compaction::{
    notice::{Conditions, Event, NoticeAnswer, NoticeRequest},
    read::{Cursor, Entry},
    ride::{RideAnswer, RideCause, RideRequest},
    setup::{SetupAnswer, SetupRequest},
    view::{AppliedView, View},
    wire::Refusal,
};
use serde::{de::DeserializeOwned, Serialize};

use super::views;

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
    ) -> Result<serde_json::Value, CallError>;
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
    ) -> Result<serde_json::Value, CallError> {
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
    entries: BTreeMap<u64, Entry>,
    digests: BTreeMap<u64, String>,
    events: Vec<Event>,
    newest: Option<Cursor>,
}

#[derive(Default)]
struct Lane {
    seq: u64,
    ready: bool,
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
        pending.newest = views::newest(entries);
        for entry in entries {
            if pending.digests.get(&entry.ordinal) != Some(&entry.digest) {
                let mut compact = entry.clone();
                compact.parts = compact.parts.iter().map(|part| part.compact()).collect();
                pending.entries.insert(entry.ordinal, compact);
                pending.digests.insert(entry.ordinal, entry.digest.clone());
            }
        }
    }

    pub fn event(&self, event: Event) {
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !pending.events.contains(&event) {
            pending.events.push(event);
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
            if !lane.ready && !setup(lane, plane, setup_request).await {
                return;
            }
            let batch = {
                let mut pending = self
                    .pending
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if pending.entries.is_empty() && pending.events.is_empty() {
                    return;
                }
                lane.seq += 1;
                let mut notice = NoticeRequest::new(lane.seq, conditions.clone());
                notice.newest = pending.newest.clone();
                notice.applied_view = applied.cloned();
                notice.entries = std::mem::take(&mut pending.entries).into_values().collect();
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
            if !matches!(answer, Ok(NoticeAnswer::Received)) {
                lane.ready = false;
                if restarted {
                    return;
                }
                restarted = true;
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
    ) -> Option<View> {
        let mut lane = self.lane.lock().await;
        self.flush_locked(
            &mut lane,
            plane,
            setup_request,
            &request.conditions,
            request.applied_view.as_ref(),
        )
        .await;
        let answer: Result<RideAnswer, _> = call(
            plane,
            "compaction.ride",
            request,
            Duration::from_millis(request.deadline_ms),
        )
        .await;
        match answer {
            Ok(RideAnswer::View { view }) => Some(view),
            Err(CallError::Refused(ref refusal)) if refusal.code == "unknown_session" => {
                lane.ready = false;
                None
            }
            _ => None,
        }
    }
}

async fn setup(lane: &mut Lane, plane: &dyn Plane, request: &SetupRequest) -> bool {
    lane.seq = 0;
    let answer: Result<SetupAnswer, _> =
        call(plane, "compaction.setup", request, CALL_DEADLINE).await;
    lane.ready = matches!(answer, Ok(SetupAnswer::Ready));
    lane.ready
}

async fn call<Q: Serialize, A: DeserializeOwned>(
    plane: &dyn Plane,
    method: &str,
    request: &Q,
    deadline: Duration,
) -> Result<A, CallError> {
    let params = serde_json::to_value(request).map_err(|_| CallError::Unavailable)?;
    let value = tokio::time::timeout(deadline, plane.call(method, params))
        .await
        .map_err(|_| CallError::Unavailable)??;
    serde_json::from_value(value).map_err(|_| CallError::Unavailable)
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
