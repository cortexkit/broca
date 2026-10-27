//! Durable held-call replay and custodian-scoped late delivery. The consent transport owns
//! filing and answers; this layer owns deadlines and never turns authorized work into a deny.

use std::sync::Arc;

use broca_protocol::{BindPrincipal, SendScope};
use broca_store::grants::{FinalKind, GrantsStore, HeldCall, HeldState, ScopeIdentity};
use serde::Deserialize;
use serde_json::{json, value::RawValue, Value};

use super::spend::Spend;

type Answer = Result<Box<RawValue>, Vec<u8>>;

pub struct HeldCalls {
    pub store: Arc<GrantsStore>,
    incarnation: String,
    pub(super) images: Arc<super::held_images::HeldImages>,
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
        let weak = Arc::downgrade(&images);
        let on_terminal: Arc<broca_store::grants::HeldCallTerminal> =
            Arc::new(move |key, admitted_at| {
                if let Some(images) = weak.upgrade() {
                    images.remove(key, admitted_at);
                }
            });
        store.on_held_call_terminal(&on_terminal);
        Self {
            store,
            incarnation,
            images,
            _on_terminal: on_terminal,
        }
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
        if identity.as_ref() != Some(&row.scope)
            || row.batch_scope != admission.scope
            || principal.map(BindPrincipal::as_owner_string).as_deref() != Some(&row.principal)
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
        if row.state == HeldState::Held && now_ms >= row.deadline_at_ms {
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

    /// Re-arm from persisted deadlines, including on startup. Authorized calls deliberately
    /// have no deadline here: only their provider execution may settle them.
    pub fn expire_due(&self, now_ms: i64) -> Result<(), broca_store::grants::GrantsError> {
        for row in self.store.recovering(now_ms)? {
            if row.state == HeldState::Held && now_ms >= row.deadline_at_ms {
                self.store
                    .expire_held(&row.batch_id, &not_started("pending"), now_ms)?;
            }
        }
        self.store.sweep(now_ms)?;
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
    fn caller_incarnation(&self, principal: &str) -> String {
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
