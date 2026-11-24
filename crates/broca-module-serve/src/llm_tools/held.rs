//! Durable held-call replay and custodian-scoped late delivery. The consent transport owns
//! filing and answers; this layer owns deadlines and never turns authorized work into a deny.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use broca_protocol::{BindPrincipal, SendScope};
use broca_store::grants::{FinalKind, GrantsStore, HeldCall, HeldState, ScopeIdentity};
use serde::Deserialize;
use serde_json::{json, value::RawValue, Value};

use super::spend::Spend;

type Answer = Result<Box<RawValue>, Vec<u8>>;

/// The wait before the first retry of an authorized call whose attempt recorded no outcome.
/// Each further failure doubles it, up to [`RETRY_MAX_MS`]; a refusal naming a longer
/// `retry_after_ms` waits that long instead.
pub const RETRY_FIRST_MS: i64 = 30_000;
pub const RETRY_MAX_MS: i64 = 30 * 60 * 1000;

/// When an authorized call may be dispatched again. `due_at_ms` is `i64::MAX` while an attempt
/// is running, so neither the timer nor a recovery pass starts a second one beside it.
struct Retry {
    admitted_at_ms: i64,
    failures: u32,
    due_at_ms: i64,
}

type Retries = Mutex<HashMap<String, Retry>>;

pub struct HeldCalls {
    pub store: Arc<GrantsStore>,
    incarnation: String,
    pub(super) images: Arc<super::held_images::HeldImages>,
    /// The retry schedule of authorized calls whose attempt ended without recording an outcome.
    /// A call with no entry is due now, which is how startup recovery dispatches every
    /// authorized call once. Entries go when their call settles.
    retries: Arc<Retries>,
    /// Dispatches the authorized calls that are due. The model-tool route installs it; the
    /// deadline timer calls it on every tick, so a failed call is retried with no card, caller
    /// or restart to prompt it.
    dispatch: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    _on_terminal: Arc<broca_store::grants::HeldCallTerminal>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    provider_incarnation: String,
    seq: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadRequest {
    since: Option<Cursor>,
    limit: Option<u32>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AckRequest {
    through: Cursor,
}

impl HeldCalls {
    pub fn new(store: Arc<GrantsStore>, incarnation: String) -> Self {
        Self::with_image_limit(store, incarnation, super::held_images::HELD_IMAGE_MAX_BYTES)
    }

    pub(super) fn with_image_limit(
        store: Arc<GrantsStore>,
        incarnation: String,
        limit: usize,
    ) -> Self {
        let images = Arc::new(super::held_images::HeldImages::new(limit));
        let retries = Arc::new(Retries::default());
        let weak = Arc::downgrade(&images);
        let weak_retries = Arc::downgrade(&retries);
        let on_terminal: Arc<broca_store::grants::HeldCallTerminal> =
            Arc::new(move |key, admitted_at| {
                if let Some(images) = weak.upgrade() {
                    images.remove(key, admitted_at);
                }
                if let Some(retries) = weak_retries.upgrade() {
                    let mut retries = retries.lock().unwrap_or_else(|p| p.into_inner());
                    if retries
                        .get(key)
                        .is_some_and(|retry| retry.admitted_at_ms == admitted_at)
                    {
                        retries.remove(key);
                    }
                }
            });
        store.on_held_call_terminal(&on_terminal);
        Self {
            store,
            incarnation,
            images,
            retries,
            dispatch: Mutex::new(None),
            _on_terminal: on_terminal,
        }
    }

    pub(super) fn set_dispatch(&self, dispatch: Arc<dyn Fn() + Send + Sync>) {
        *self.dispatch.lock().unwrap_or_else(|p| p.into_inner()) = Some(dispatch);
    }

    /// Whether the authorized `row` is due for an attempt at `now_ms`. A due call is marked
    /// running until [`Self::retry_later`] schedules its next attempt or it settles.
    pub(super) fn start_attempt(&self, row: &HeldCall, now_ms: i64) -> bool {
        let mut retries = self.retries.lock().unwrap_or_else(|p| p.into_inner());
        match retries.get_mut(&row.batch_id) {
            Some(retry) if retry.admitted_at_ms == row.admitted_at_ms => {
                if now_ms < retry.due_at_ms {
                    return false;
                }
                retry.due_at_ms = i64::MAX;
            }
            _ => {
                retries.insert(
                    row.batch_id.clone(),
                    Retry {
                        admitted_at_ms: row.admitted_at_ms,
                        failures: 0,
                        due_at_ms: i64::MAX,
                    },
                );
            }
        }
        true
    }

    /// Schedule the next attempt of an authorized call whose attempt ended without settling
    /// it: an exponential backoff from [`RETRY_FIRST_MS`], or the refusal's own
    /// `retry_after_ms` when that is longer. An attempt already scheduled by another waiter on
    /// the same execution is left as it is.
    pub(super) fn retry_later(&self, row: &HeldCall, retry_after_ms: Option<i64>, now_ms: i64) {
        let mut retries = self.retries.lock().unwrap_or_else(|p| p.into_inner());
        let retry = retries
            .entry(row.batch_id.clone())
            .or_insert_with(|| Retry {
                admitted_at_ms: row.admitted_at_ms,
                failures: 0,
                due_at_ms: i64::MAX,
            });
        if retry.admitted_at_ms != row.admitted_at_ms {
            *retry = Retry {
                admitted_at_ms: row.admitted_at_ms,
                failures: 0,
                due_at_ms: i64::MAX,
            };
        }
        if retry.due_at_ms != i64::MAX {
            return;
        }
        let backoff = RETRY_FIRST_MS
            .saturating_mul(1_i64 << retry.failures.min(16))
            .min(RETRY_MAX_MS);
        retry.failures = retry.failures.saturating_add(1);
        retry.due_at_ms = now_ms.saturating_add(backoff.max(retry_after_ms.unwrap_or(0)));
    }

    pub(super) fn replay(
        &self,
        admission: &super::admission::Admission,
        principal: Option<&BindPrincipal>,
        scope: &SendScope,
        call_key: &str,
        now_ms: i64,
    ) -> Result<Option<HeldCall>, Vec<u8>> {
        let batch_id = &admission.batch_id;
        let digest = admission.args_digest.as_str();
        let row = self.store.lookup(batch_id, now_ms).map_err(internal)?;
        let Some(row) = row else { return Ok(None) };
        let identity = match scope {
            SendScope::Stamped(bind) => Some(ScopeIdentity {
                owner: bind.scope.owner.as_owner_string(),
                scope_ref: bind.scope.scope_ref.clone(),
                scope_epoch: bind.scope.scope_epoch,
            }),
            _ => None,
        };
        let same_scope = identity.as_ref().is_some_and(|identity| {
            broca_protocol::same_owner_string(&identity.owner, &row.scope.owner)
                && identity.scope_ref == row.scope.scope_ref
                && identity.scope_epoch == row.scope.scope_epoch
        });
        if !same_scope
            || row.batch_scope != admission.scope
            || !principal.is_some_and(|principal| {
                broca_protocol::same_owner_string(&principal.as_owner_string(), &row.principal)
            })
            || call_key != row.call_key
        {
            return Err(refusal(
                "batch_scope_mismatch",
                "held call belongs to another stamped scope or caller",
                Spend::Nothing,
            ));
        }
        if digest != row.args_digest {
            return Err(refusal(
                "batch_id_reuse",
                "held call arguments differ from first admission",
                Spend::Nothing,
            ));
        }
        if matches!(row.state, HeldState::Filing | HeldState::Held) && now_ms >= row.deadline_at_ms
        {
            self.store
                .expire_held(batch_id, &not_started("pending"), now_ms)
                .map_err(internal)?;
            return self.store.lookup(batch_id, now_ms).map_err(internal);
        }
        Ok(Some(row))
    }

    pub fn held_reply(row: &HeldCall) -> Answer {
        raw(json!({
            "content":[{"type":"text","text":"Please approve the consent card."}],
            "held":{"reason":"consent","detail":{"call_key":row.call_key,"ask_id":row.card_id}},
        }))
    }

    pub(crate) fn final_reply(row: &HeldCall) -> Answer {
        let result = row
            .final_result
            .as_ref()
            .ok_or_else(|| refusal("internal", "final held call has no result", Spend::Unknown))?;
        // A stored error is answered as stored, with the spend mark its first answer carried.
        if result.is_error {
            return Err(result.body.as_bytes().to_vec());
        }
        RawValue::from_string(result.body.clone()).map_err(lost)
    }

    /// Re-arm from persisted deadlines, including on startup, then dispatch the authorized calls
    /// whose retry is due. A call whose card filing never resolved has the same deadline as one
    /// waiting on a filed card. Authorized calls deliberately have no deadline here: only their
    /// provider execution may settle them.
    pub fn expire_due(&self, now_ms: i64) -> Result<(), broca_store::grants::GrantsError> {
        for batch_id in self.store.due_waiting(now_ms)? {
            self.store
                .expire_held(&batch_id, &not_started("pending"), now_ms)?;
        }
        self.store.sweep(now_ms)?;
        let dispatch = self
            .dispatch
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        if let Some(dispatch) = dispatch {
            dispatch();
        }
        Ok(())
    }

    pub fn spawn_timers(
        self: &Arc<Self>,
        stop: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let service = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = stop.cancelled() => return,
                    _ = interval.tick() => {
                        if let Err(error) = service.expire_due(epoch_ms()) {
                            tracing::warn!(%error, "held-call deadline sweep failed; persisted deadlines will be retried");
                        }
                    }
                }
            }
        })
    }

    pub(super) async fn sweep_image_entries(
        &self,
        consent: &super::consent::Consent,
        now: i64,
    ) -> Result<(), broca_store::grants::GrantsError> {
        for (key, admitted_at, scope) in self.images.entries() {
            let row = self.store.lookup(&key, now)?;
            let gone = row.is_none_or(|row| {
                row.state == HeldState::Final || row.admitted_at_ms != admitted_at
            });
            let ended = !gone
                && scope.flow_id.is_none()
                && scope.run_id.is_some()
                && consent.image_run_live(&scope).await == super::consent::RunLiveness::Ended;
            if gone || ended {
                self.images.remove(&key, admitted_at);
            }
        }
        Ok(())
    }

    pub fn read(
        &self,
        arguments: &Value,
        principal: Option<&BindPrincipal>,
        now_ms: i64,
    ) -> Answer {
        let principal = principal
            .ok_or_else(|| {
                late_refusal(
                    "scope_required",
                    "late results require a daemon-stamped caller principal",
                )
            })?
            .as_owner_string();
        let request: ReadRequest = serde_json::from_value(arguments.clone()).map_err(invalid)?;
        let since = request.since.as_ref().map_or(0, |cursor| cursor.seq);
        let head = self.store.late_head(&principal).map_err(late_internal)?;
        if let Some(cursor) = &request.since {
            self.check_cursor(cursor, head, &principal)?;
        }
        let limit = request.limit.unwrap_or(100).clamp(1, 1000);
        let mut rows = self
            .store
            .late_results(&principal, since, limit + 1, now_ms)
            .map_err(late_internal)?;
        let more = rows.len() > limit as usize;
        rows.truncate(limit as usize);
        let seq = rows.last().map_or(head, |entry| entry.seq);
        let entries = rows
            .iter()
            .map(|entry| late_entry(&entry.call))
            .collect::<Result<Vec<_>, _>>()?;
        late_raw(
            json!({"entries":entries, "cursor":{"provider_incarnation":self.caller_incarnation(&principal),"seq":seq},"more":more}),
        )
    }

    pub fn acknowledge(&self, arguments: &Value, principal: Option<&BindPrincipal>) -> Answer {
        let principal = principal
            .ok_or_else(|| {
                late_refusal(
                    "scope_required",
                    "late-result acknowledgements require a daemon-stamped caller principal",
                )
            })?
            .as_owner_string();
        let request: AckRequest = serde_json::from_value(arguments.clone()).map_err(invalid)?;
        self.check_cursor(
            &request.through,
            self.store.late_head(&principal).map_err(late_internal)?,
            &principal,
        )?;
        self.store
            .acknowledge(&principal, request.through.seq)
            .map_err(late_internal)?;
        late_raw(json!({}))
    }

    fn check_cursor(&self, cursor: &Cursor, head: u64, principal: &str) -> Result<(), Vec<u8>> {
        if cursor.provider_incarnation != self.caller_incarnation(principal) || cursor.seq > head {
            return Err(late_refusal("cursor_incarnation_changed", "late-results cursor is not from this caller's provider incarnation or is past its entries"));
        }
        Ok(())
    }

    // The wire cursor has no principal field. Binding its opaque incarnation to the caller
    // prevents a cursor copied from another custodian from acknowledging this caller's work.
    // The canonical spelling keeps a custodian's cursor valid across its module's rename.
    fn caller_incarnation(&self, principal: &str) -> String {
        let principal = broca_protocol::canonical_owner(principal);
        super::admission::hash(&json!({"launch":self.incarnation,"principal":principal}))
    }
}

pub fn not_started(consent: &str) -> String {
    let mut body = serde_json::to_value(Spend::Nothing.mark(super::admission::refusal(
        "not_started",
        format!("private-content consent ended the call without dispatch: {consent}"),
    )))
    .expect("a consent refusal encodes");
    body["detail"]["consent"] = json!(consent);
    body.to_string()
}

fn late_entry(row: &HeldCall) -> Result<Value, Vec<u8>> {
    let result = row
        .final_result
        .as_ref()
        .ok_or_else(|| late_refusal("internal", "late result has no stored final outcome"))?;
    let mut entry = json!({
        "kind":if result.kind == FinalKind::Result {"result"} else {"not_started"},
        "owner":row.scope.owner, "ref":row.scope.scope_ref, "scope_epoch":row.scope.scope_epoch,
        "custodian":row.principal, "call_key":row.call_key,
        "event_id":format!("llm.classify:{}:{}",row.batch_id,row.admitted_at_ms),
        "settled_at":result.settled_at_ms, "reduced":false,
    });
    let body: Value = serde_json::from_str(&result.body).map_err(late_internal)?;
    if result.kind == FinalKind::Result {
        entry["result"] = if body.get("content").is_some() {
            body
        } else if result.is_error {
            json!({"content":[{"type":"text","text":result.body}],"isError":true,"errorCode":body["code"]})
        } else if body.is_object() {
            // Consumers hand `structuredContent` to the caller, so a late answer reaches it as
            // the same object an immediate answer would.
            json!({"content":[{"type":"text","text":result.body}],"structuredContent":body})
        } else {
            json!({"content":[{"type":"text","text":result.body}]})
        };
    } else {
        entry["reason"] = body["detail"]["consent"].clone();
        entry["outcome"] = body["detail"]["consent"].clone();
    }
    Ok(entry)
}

pub fn epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

fn raw(value: Value) -> Answer {
    serde_json::value::to_raw_value(&value).map_err(internal)
}

/// A late-results reply. Its encoding failure carries no spend member, like every refusal of
/// `late_results` and `late_results.ack`.
fn late_raw(value: Value) -> Answer {
    serde_json::value::to_raw_value(&value).map_err(late_internal)
}

/// A refusal of a model tool call, with the spend it states.
fn refusal(code: &str, message: &str, spend: Spend) -> Vec<u8> {
    crate::system_one::refusal_body(&spend.mark(super::admission::refusal(code, message)))
}

/// A refusal of `late_results` or `late_results.ack`. These operations answer no model call,
/// so their refusals carry no spend member: their bodies stay the ones readers already decode.
fn late_refusal(code: &str, message: &str) -> Vec<u8> {
    crate::system_one::refusal_body(&super::admission::refusal(code, message))
}

fn invalid(error: impl std::fmt::Display) -> Vec<u8> {
    late_refusal(
        "invalid_params",
        &format!("invalid late-results arguments: {error}"),
    )
}

/// A held-call store failure that ends a request before anything is sent to a provider.
fn internal(error: impl std::fmt::Display) -> Vec<u8> {
    crate::system_one::refusal_body(&Spend::Nothing.mark(crate::system_one::internal_refusal(
        &format!("held-call store: {error}"),
    )))
}

/// A held-call store failure answering `late_results` or `late_results.ack`, without a spend
/// member.
fn late_internal(error: impl std::fmt::Display) -> Vec<u8> {
    crate::system_one::refusal_body(&crate::system_one::internal_refusal(&format!(
        "held-call store: {error}"
    )))
}

/// A held call's stored result that cannot be read back. The call may have been executed and
/// billed before its result was lost, so its spend is unknown.
fn lost(error: impl std::fmt::Display) -> Vec<u8> {
    crate::system_one::refusal_body(&Spend::Unknown.mark(crate::system_one::internal_refusal(
        &format!("held-call result: {error}"),
    )))
}

#[cfg(test)]
mod retry_tests {
    use super::*;

    fn authorized() -> HeldCall {
        HeldCall {
            batch_id: "batch".into(),
            project_id: None,
            scope: ScopeIdentity {
                owner: "reserved:owner".into(),
                scope_ref: "run".into(),
                scope_epoch: 1,
            },
            batch_scope: broca_store::system_one::BatchScope::Run {
                agent_id: "agent".into(),
                run_id: "run".into(),
            },
            principal: "reserved:caller".into(),
            call_key: "key".into(),
            agent_id: "agent".into(),
            provider: "typesafe".into(),
            model: "typesafe/jev-1.13.0".into(),
            request: Some("{}".into()),
            args_digest: "digest".into(),
            frozen_row: "{}".into(),
            dedup_key: "card".into(),
            state: HeldState::Authorized,
            admitted_at_ms: 1000,
            deadline_at_ms: 601_000,
            card_id: None,
            final_result: None,
        }
    }

    /// Each failure doubles the wait up to its cap, a refusal naming a longer wait is honoured,
    /// and a running attempt is never joined by a second one.
    #[test]
    fn retries_back_off_honour_retry_after_and_never_overlap() {
        let held = HeldCalls::new(
            Arc::new(GrantsStore::open_in_memory().unwrap()),
            "retries".into(),
        );
        let row = authorized();
        assert!(
            held.start_attempt(&row, 0),
            "a call with no schedule is due"
        );
        assert!(
            !held.start_attempt(&row, 0),
            "an attempt is already running"
        );
        held.retry_later(&row, None, 0);
        assert!(!held.start_attempt(&row, RETRY_FIRST_MS - 1));
        assert!(held.start_attempt(&row, RETRY_FIRST_MS));
        held.retry_later(&row, None, RETRY_FIRST_MS);
        assert!(!held.start_attempt(&row, 3 * RETRY_FIRST_MS - 1));
        assert!(held.start_attempt(&row, 3 * RETRY_FIRST_MS));
        held.retry_later(&row, Some(10 * RETRY_FIRST_MS), 3 * RETRY_FIRST_MS);
        held.retry_later(&row, None, 3 * RETRY_FIRST_MS);
        assert!(!held.start_attempt(&row, 13 * RETRY_FIRST_MS - 1));
        assert!(held.start_attempt(&row, 13 * RETRY_FIRST_MS));
        for _ in 0..20 {
            held.retry_later(&row, None, 0);
            held.retries
                .lock()
                .unwrap()
                .get_mut("batch")
                .unwrap()
                .due_at_ms = i64::MAX;
        }
        held.retry_later(&row, None, 0);
        assert!(!held.start_attempt(&row, RETRY_MAX_MS - 1));
        assert!(held.start_attempt(&row, RETRY_MAX_MS));
    }
}
