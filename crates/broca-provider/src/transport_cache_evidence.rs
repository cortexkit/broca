//! Runtime-only prefix hashes shared by project root, harness, and session name.

use super::*;
use crate::cache_evidence::{compare_fingerprints, fingerprint, RequestFingerprint, TtlKind, View};
use crate::wire::{CacheClass, CacheTtl};
use std::{collections::HashMap, sync::Mutex};

#[derive(Clone, Default)]
pub(super) struct Sessions(Arc<Mutex<HashMap<broca_protocol::BindIdentity, Arc<Session>>>>);

impl std::fmt::Debug for Sessions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheEvidenceSessions")
            .finish_non_exhaustive()
    }
}

impl Sessions {
    pub(super) fn bind(&self, bind: &broca_protocol::BindIdentity) -> Arc<Session> {
        self.0
            .lock()
            .unwrap()
            .entry(bind.clone())
            .or_default()
            .clone()
    }
}

#[derive(Default)]
pub(super) struct Session(
    Mutex<State>,
    #[cfg(test)] pub(super) Mutex<Option<Arc<Gate>>>,
);

#[cfg(test)]
#[derive(Default)]
pub(super) struct Gate {
    pub(super) entered: AtomicBool,
    open: Mutex<bool>,
    wake: std::sync::Condvar,
}

#[cfg(test)]
impl Gate {
    fn wait(&self) {
        self.entered.store(true, Ordering::Release);
        let (open, _) = self
            .wake
            .wait_timeout_while(
                self.open.lock().unwrap(),
                broca_test_support::EVENT_HANG_GUARD,
                |open| !*open,
            )
            .unwrap();
        assert!(*open, "fingerprint test gate was not released");
    }
    pub(super) fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.wake.notify_all();
    }
}

#[derive(Default)]
struct State {
    lineage: Option<String>,
    seeded: bool,
    locator: Option<String>,
    view: Option<u64>,
    previous: Option<Previous>,
}

struct Previous {
    fingerprint: RequestFingerprint,
    family: crate::spec::WireFamilyId,
    sent: Option<u64>,
    model: String,
    account: Option<String>,
    ttl: (Option<u64>, TtlKind),
    view: Option<u64>,
}

fn ttl(frozen: &FrozenRenderConfig) -> (Option<u64>, TtlKind) {
    match frozen.cache.as_ref().map(|c| &c.class) {
        Some(CacheClass::Markers { ttl, .. }) => (
            Some(match ttl {
                CacheTtl::FiveMinutes => 300_000,
                CacheTtl::OneHour => 3_600_000,
            }),
            TtlKind::Explicit,
        ),
        Some(CacheClass::Hints { retention, .. }) => (
            retention
                .as_deref()
                .filter(|r| *r == "24h")
                .map(|_| 86_400_000),
            TtlKind::Implicit,
        ),
        _ => (
            None,
            if matches!(
                frozen.family_id,
                crate::spec::WireFamilyId::AnthropicMessages
                    | crate::spec::WireFamilyId::BedrockConverse
            ) {
                TtlKind::Explicit
            } else {
                TtlKind::Implicit
            },
        ),
    }
}

impl Session {
    pub(super) fn needs_seed(&self, lineage: Option<&str>) -> bool {
        let state = self.0.lock().unwrap();
        !state.seeded || state.lineage.as_deref() != lineage
    }

    pub(super) fn seed(&self, frames: &[broca_wal::FramedRecord], lineage: Option<String>) {
        let mut previous = None;
        let mut locator = None;
        let mut view = None;
        let mut previous_view = None;
        let records: Vec<_> = frames.iter().map(|f| f.record.clone()).collect();
        for frame in frames {
            match &frame.record {
                broca_wal::WalRecord::CompactionViewApplied { view_version, .. } => {
                    view = Some(*view_version)
                }
                broca_wal::WalRecord::ModelAttemptFinished { .. } => {
                    locator = lineage.as_ref().map(|id| format!("{id}:{}", frame.seq));
                    previous_view = view;
                }
                _ => {}
            }
        }
        let rebuilt = broca_core::cache_warm::last_recorded_request(&records)
            .ok()
            .and_then(|recorded| {
                let provider = recorded
                    .route
                    .as_ref()
                    .map_or(&recorded.config.model, |route| &route.model)
                    .provider_module_id
                    .clone();
                let frozen: FrozenRenderConfig =
                    serde_json::from_value(recorded.config.frozen_render_config).ok()?;
                let frozen = if recorded.route.is_some() {
                    frozen.refusal_fallback?.frozen
                } else {
                    frozen
                };
                let body =
                    crate::registry::wire_family(frozen.family_id)?.render(&frozen, &recorded.opts);
                (RequestDigest::of(&body).sha256 == recorded.request_body_sha256)
                    .then_some((frozen, body, provider))
            });
        if let Some((frozen, body, provider)) = rebuilt {
            if let Some((sent, account)) =
                frames.iter().rev().find_map(|frame| match &frame.record {
                    broca_wal::WalRecord::ModelAttemptFinished {
                        sent_at_ms,
                        served_account_ref,
                        reservation,
                        envelope,
                        ..
                    } => Some((
                        envelope
                            .as_deref()
                            .and_then(|e| e.sent_at_ms)
                            .or(*sent_at_ms),
                        reservation
                            .as_deref()
                            .and_then(|r| r.credential.account_ref.clone())
                            .or_else(|| served_account_ref.clone()),
                    )),
                    _ => None,
                })
            {
                previous = fingerprint(frozen.family_id, &body)
                    .ok()
                    .map(|fingerprint| Previous {
                        fingerprint,
                        family: frozen.family_id,
                        sent,
                        model: format!("{provider}/{}", frozen.wire_model_id),
                        account,
                        ttl: ttl(&frozen),
                        view: previous_view,
                    });
            }
        }
        *self.0.lock().unwrap() = State {
            lineage,
            seeded: true,
            locator,
            view,
            previous,
        };
    }

    pub(super) fn set_view(&self, view: Option<u64>) {
        self.0.lock().unwrap().view = view;
    }
    pub(super) fn annotate(&self, attempt: &mut ModelAttempt) {
        if let Some(evidence) = attempt.cache_evidence.as_mut() {
            evidence["compared_to"] =
                serde_json::to_value(&self.0.lock().unwrap().locator).expect("locator serializes");
        }
    }
    pub(super) fn recorded(&self, locator: Option<String>) {
        let mut state = self.0.lock().unwrap();
        // A fresh writer can establish its lineage on the first append, after cold seeding.
        if state.lineage.is_none() {
            state.lineage = locator
                .as_deref()
                .and_then(|s| s.rsplit_once(':'))
                .map(|(lineage, _)| lineage.to_owned());
        }
        state.locator = locator;
    }

    fn finish(
        &self,
        previous: Option<Previous>,
        fingerprint: Result<RequestFingerprint, String>,
        frozen: &FrozenRenderConfig,
        model: &str,
        sent: Option<u64>,
        account: Option<String>,
    ) -> Option<Box<Value>> {
        let mut state = self.0.lock().unwrap();
        let current = fingerprint.ok().map(|fingerprint| Previous {
            fingerprint,
            family: frozen.family_id,
            sent,
            model: model.to_owned(),
            account,
            ttl: ttl(frozen),
            view: state.view,
        });
        let evidence = previous
            .as_ref()
            .zip(current.as_ref())
            .filter(|(p, c)| p.family == c.family)
            .map(|(p, c)| {
                let mut evidence = compare_fingerprints(&p.fingerprint, &c.fingerprint);
                evidence.idle_gap_ms = p.sent.zip(c.sent).map_or(0, |(a, b)| b.saturating_sub(a));
                (evidence.cache_ttl_ms, evidence.ttl_kind) = p.ttl.clone();
                evidence.model_changed = p.model != c.model;
                evidence.account_changed = p
                    .account
                    .as_ref()
                    .zip(c.account.as_ref())
                    .is_some_and(|(a, b)| a != b);
                evidence.view = (p.view.is_some() || c.view.is_some()).then_some(View {
                    applied: c.view,
                    previous: p.view,
                });
                Box::new(serde_json::to_value(evidence).expect("cache evidence serializes"))
            });
        state.previous = current;
        state.seeded = state.previous.is_some();
        evidence
    }
}

type Work = tokio::task::JoinHandle<Result<RequestFingerprint, String>>;
#[derive(Default)]
struct InFlight {
    work: Option<Work>,
    previous: Option<Previous>,
    prepared: bool,
}
#[derive(Clone, Default)]
pub(super) struct Pending(Arc<Mutex<InFlight>>);

impl Pending {
    pub(super) fn prepare(&self, request: &PreparedHttpRequest) {
        let Some((session, _)) = &request.cache_session else {
            return;
        };
        let mut pending = self.0.lock().unwrap();
        if !pending.prepared {
            // Remove the predecessor while a request is in flight. An aborted task must not
            // let a later run silently skip over it; that run reconstructs the WAL instead.
            let mut state = session.0.lock().unwrap();
            pending.previous = state.previous.take();
            state.seeded = false;
            pending.prepared = true;
        }
    }

    pub(super) fn start(&self, request: &PreparedHttpRequest) {
        let Some((_session, frozen)) = &request.cache_session else {
            return;
        };
        self.prepare(request);
        let mut pending = self.0.lock().unwrap();
        if pending.work.is_none() {
            let body = request.body.clone();
            let family = frozen.family_id;
            // No fingerprint work runs before the transport has accepted the request. Even
            // small requests use the blocking pool so hashing cannot postpone stream polling.
            #[cfg(test)]
            let gate = _session.1.lock().unwrap().clone();
            pending.work = Some(tokio::task::spawn_blocking(move || {
                #[cfg(test)]
                if let Some(gate) = gate {
                    gate.wait();
                }
                fingerprint(family, &body)
            }));
        }
    }

    pub(super) async fn finish(
        &self,
        request: &PreparedHttpRequest,
        sent: Option<u64>,
        account: Option<String>,
    ) -> Option<Box<Value>> {
        let (session, frozen) = request.cache_session.as_ref()?;
        self.start(request);
        let (work, previous) = {
            let mut pending = self.0.lock().unwrap();
            let work = pending.work.take()?;
            pending.prepared = false;
            (work, pending.previous.take())
        };
        let result = work
            .await
            .unwrap_or_else(|_| Err("fingerprint worker failed".into()));
        session.finish(
            previous,
            result,
            frozen,
            request
                .cache_model
                .as_deref()
                .unwrap_or(&frozen.wire_model_id),
            sent,
            account,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn previous() -> Previous {
        let family = crate::spec::WireFamilyId::OpenAiResponses;
        Previous {
            fingerprint: fingerprint(
                family,
                br#"{"model":"old","input":[{"role":"user","content":"A"}]}"#,
            )
            .unwrap(),
            family,
            sent: Some(100),
            model: "old".into(),
            account: Some("account-one".into()),
            ttl: (Some(3_600_000), TtlKind::Explicit),
            view: Some(1),
        }
    }

    #[test]
    fn cache_evidence_model_identity_includes_provider_without_changing_prefix() {
        let session = Session::default();
        let mut before = previous();
        before.model = "first/old".into();
        let frozen = crate::families::frozen_config_for_family(before.family, "old");
        let body = br#"{"model":"old","input":[{"role":"user","content":"A"}]}"#;
        let evidence = session
            .finish(
                Some(before),
                fingerprint(frozen.family_id, body),
                &frozen,
                "second/old",
                Some(200),
                Some("account-one".into()),
            )
            .unwrap();
        assert_eq!(evidence["extends_previous"], true);
        assert_eq!(evidence["model_changed"], true);
        let before = session.0.lock().unwrap().previous.take();
        let stable = session
            .finish(
                before,
                fingerprint(frozen.family_id, body),
                &frozen,
                "second/old",
                Some(300),
                Some("account-one".into()),
            )
            .unwrap();
        assert_eq!(stable["model_changed"], false);
    }

    #[test]
    fn cache_evidence_unknown_history_is_seeded_once_per_session() {
        let session = Session::default();
        assert!(session.needs_seed(Some("lineage")));
        session.seed(&[], Some("lineage".into()));
        assert!(
            !session.needs_seed(Some("lineage")),
            "a verified empty predecessor does not trigger repeated reads"
        );
        assert!(session.needs_seed(Some("different-lineage")));
    }

    #[tokio::test]
    async fn cache_evidence_live_metadata_and_abort_gap_do_not_retain_a_body() {
        let frozen = Arc::new(crate::families::frozen_config_for_family(
            crate::spec::WireFamilyId::OpenAiResponses,
            "new",
        ));
        let session = Arc::new(Session::default());
        session.0.lock().unwrap().seeded = true;
        session.0.lock().unwrap().previous = Some(previous());
        session.set_view(Some(2));
        let mut request = PreparedHttpRequest::from(HttpRequest {
            method: crate::wire::HttpMethod::Post,
            url: "http://synthetic.invalid".into(),
            headers: Vec::new(),
            body: serde_json::to_vec(
                &json!({"model":"new","input":[{"role":"user","content":"A"}]}),
            )
            .unwrap(),
        });
        request.cache_session = Some((Arc::clone(&session), Arc::clone(&frozen)));
        let pending = Pending::default();
        pending.prepare(&request);
        assert!(
            session.needs_seed(None),
            "an aborted task cannot leave an older predecessor reusable"
        );
        drop(pending);
        assert!(session.needs_seed(None));
        let evidence = session
            .finish(
                Some(previous()),
                fingerprint(frozen.family_id, &request.body),
                &frozen,
                "new",
                Some(350),
                Some("account-two".into()),
            )
            .unwrap();
        assert_eq!(evidence["extends_previous"], false);
        assert_eq!(evidence["first_divergence"]["region"], "provider_state");
        assert!(evidence["first_divergence"].get("field").is_none());
        assert_eq!(evidence["idle_gap_ms"], 250);
        assert_eq!(evidence["cache_ttl_ms"], 3_600_000);
        assert_eq!(evidence["ttl_kind"], "explicit");
        assert_eq!(evidence["model_changed"], true);
        assert_eq!(evidence["account_changed"], true);
        assert_eq!(evidence["view"], json!({"previous":1,"applied":2}));
        session.recorded(Some("lineage:42".into()));
        assert!(!session.needs_seed(Some("lineage")));
        let old = session.0.lock().unwrap().previous.take();
        assert!(session
            .finish(old, Err("unavailable".into()), &frozen, "new", None, None)
            .is_none());
        assert!(
            session
                .finish(
                    None,
                    fingerprint(frozen.family_id, &request.body),
                    &frozen,
                    "new",
                    Some(500),
                    None
                )
                .is_none(),
            "a gap must not be bridged"
        );
    }
}
