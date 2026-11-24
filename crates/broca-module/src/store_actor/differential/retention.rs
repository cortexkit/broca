//! Retained creating sends, read refusals, recreation and actor deadlines through both modules.

use super::*;
use crate::retention::RetentionService;
use crate::serve::{ServeError, ServeHandler, SessionReadRequest};
use broca_session_store::keys::{Cipher, DeploymentKey};

const NOW: u64 = 1_700_000_000_000;
const SECONDS: u64 = 60;
const TTL: Duration = Duration::from_secs(600);

struct Retained {
    module: Module,
    service: Arc<RetentionService>,
    runtime: Arc<crate::store_open::StoreRuntime>,
}

async fn retained(cipher: Cipher, idle: Duration, gate: Option<Arc<Notify>>) -> Retained {
    let dir = scratch_dir();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("test-clock"),
        serde_json::json!({"now_ms": NOW}).to_string(),
    )
    .unwrap();
    let leases = Arc::new(FileLeaseStore::new(dir.join("leases")));
    let service = Arc::new(RetentionService::new(&dir, leases.clone()).unwrap());
    let steps = [hello(), hello()].concat();
    let executor: Arc<dyn RunExecutor> = Arc::new(executor(steps, gate));
    let runtime = Arc::new(
        crate::store_open::StoreRuntime::open(
            &dir,
            DeploymentKey::new(1, &[7; 32]).creating(cipher),
            executor.clone(),
            idle,
        )
        .await
        .unwrap(),
    );
    let registry = LineageRegistry::new(Arc::new(
        crate::opener::DefaultOpener::new(
            leases,
            executor.clone(),
            std::sync::Arc::new(crate::store_open::StoreSlot::loaded(runtime.clone())),
        )
        .with_idle_ttl(idle),
    ));
    registry.set_retention_service(service.clone());
    let key = BindIdentity::new(dir.join("project"), "retention-differential", "s1");
    Retained {
        module: Module {
            dir,
            registry,
            key,
            caller: Default::default(),
        },
        service,
        runtime,
    }
}

impl Retained {
    fn advance(&self, now: u64) {
        std::fs::write(
            self.module.dir.join("test-clock"),
            serde_json::json!({"now_ms": now}).to_string(),
        )
        .unwrap();
        assert!(self.service.clock.refresh().unwrap());
    }

    async fn send(
        &self,
        run: &str,
        retention: Option<u64>,
    ) -> Result<SendOutcome, crate::registry::OpenError> {
        self.send_with_plan(run, retention, None).await
    }

    async fn send_with_plan(
        &self,
        run: &str,
        retention: Option<u64>,
        plan: Option<crate::actor::SendPlan>,
    ) -> Result<SendOutcome, crate::registry::OpenError> {
        self.module
            .registry
            .route_submission_with_retention(
                &self.module.key,
                RunId(run.into()),
                self.module
                    .caller
                    .shape(run, &SendKey::Minted, request("hello")),
                RunKind::FreshOrResume,
                SendKey::Minted,
                false,
                broca_protocol::Delivery::Queue,
                plan,
                None,
                None,
                retention,
                false,
            )
            .await
    }

    async fn idle(&self) {
        broca_test_support::wait_until("retained run finished", || async {
            match self.module.registry.route_status(&self.module.key).await {
                Ok(LineageStatus::Idle) => Ok(()),
                other => Err(format!("{other:?}")),
            }
        })
        .await;
    }

    async fn deleted(&self) {
        broca_test_support::wait_until("retained actor deleted its session", || async {
            let held = self.runtime.holds(&self.module.key).unwrap();
            if self.module.registry.slot_state(&self.module.key).is_none() && !held {
                Ok(())
            } else {
                Err(format!("head={held}"))
            }
        })
        .await;
    }

    async fn expired_read(&self, deadline: u64) {
        assert_eq!(crate::retention::EXPIRED, "expired");
        let error = self
            .module
            .registry
            .read(SessionReadRequest::new(self.module.key.clone(), None, None))
            .await
            .expect_err("a deleted retained session remains expired");
        assert!(
            matches!(error, ServeError::Expired { expired_at_ms } if expired_at_ms == deadline),
            "{error:?}"
        );
        let error = self
            .module
            .registry
            .check_expired(&self.module.key, Some("r1"))
            .await
            .unwrap_err();
        assert!(
            matches!(error, crate::registry::OpenError::Expired { expired_at_ms } if expired_at_ms == deadline)
        );
    }

    async fn stop(&self) {
        assert!(self.module.registry.stop_and_seal().await);
    }
}

async fn retained_creating_sends_validate_alike(cipher: Cipher) {
    let new = retained(cipher, TTL, None).await;
    let new_invalid = new.send("invalid", Some(0)).await.unwrap_err();
    broca_test_support::pin!("an invalid retention", new_invalid.to_string());
    assert!(!new.runtime.holds(&new.module.key).unwrap());
    broca_test_support::pin!("alike", new.send("r1", Some(SECONDS)).await.unwrap());
    assert!(
        new.runtime.holds(&new.module.key).unwrap(),
        "a retained creating send must use the new storage"
    );
    new.idle().await;
    broca_test_support::pin!(
        "alike",
        new.send("changed", Some(SECONDS + 1)).await.unwrap()
    );
    broca_test_support::pin!("alike", new.send("r2", Some(SECONDS)).await.unwrap());
    new.idle().await;
    new.stop().await;
}

async fn swept_retained_reads_and_fresh_sends_match(cipher: Cipher) {
    let new = retained(cipher, Duration::from_millis(1), None).await;
    broca_test_support::pin!("alike", new.send("r1", Some(SECONDS)).await.unwrap());
    new.module.closed().await;
    let deadline = NOW + SECONDS * 1000;
    new.advance(deadline);
    assert_eq!(new.module.registry.sweep_once(deadline).await.unwrap(), 1);
    new.expired_read(deadline).await;
    broca_test_support::pin!("alike", new.send("r2", Some(SECONDS)).await.unwrap());
    assert!(new.runtime.holds(&new.module.key).unwrap());
    new.module.closed().await;
    let error = new
        .module
        .registry
        .check_expired(&new.module.key, Some("r1"))
        .await
        .unwrap_err();
    assert!(
        matches!(error, crate::registry::OpenError::Expired { expired_at_ms } if expired_at_ms == deadline)
    );
    assert!(new
        .module
        .registry
        .read(SessionReadRequest::new(new.module.key.clone(), None, None))
        .await
        .is_ok());
    new.stop().await;
}

async fn closed_retained_sessions_expire_before_any_sweep_alike(cipher: Cipher) {
    let new = retained(cipher, Duration::from_millis(1), None).await;
    broca_test_support::pin!("alike", new.send("r1", Some(SECONDS)).await.unwrap());
    new.module.closed().await;
    let deadline = NOW + SECONDS * 1000;
    new.advance(deadline);
    assert!(new.runtime.holds(&new.module.key).unwrap());
    new.expired_read(deadline).await;
    broca_test_support::pin!("alike", new.send("r2", Some(SECONDS)).await.unwrap());
    new.module.closed().await;
    assert!(new.runtime.holds(&new.module.key).unwrap());
    assert_eq!(
        new.runtime.expiry_markers(&new.module.key).unwrap()[0].expired_at_ms,
        deadline
    );
    new.stop().await;
}

async fn an_open_retained_session_expires_at_its_deadline(cipher: Cipher) {
    let new = retained(cipher, TTL, None).await;
    broca_test_support::pin!("alike", new.send("r1", Some(SECONDS)).await.unwrap());
    new.idle().await;
    assert!(new.module.registry.slot_state(&new.module.key).is_some());
    let (new_sink, mut new_stream) = mpsc::channel(64);
    assert_eq!(
        new.module
            .registry
            .route_subscribe_live(&new.module.key, new_sink)
            .await
            .unwrap(),
        crate::actor::SubscribeOutcome::Attached
    );
    let deadline = NOW + SECONDS * 1000;
    new.advance(deadline);
    new.deleted().await;
    broca_test_support::wait_until("subscriber receives the expiry terminal", || {
        std::future::ready(match new_stream.try_recv() {
            Ok(crate::fanout::Delivery::Expired { expired_at_ms }) => {
                assert_eq!(expired_at_ms, deadline);
                Ok(())
            }
            other => Err(format!("{other:?}")),
        })
    })
    .await;
    new.expired_read(deadline).await;
    new.stop().await;
}

async fn recreated_retained_sessions_carry_numbering_floors_alike(cipher: Cipher) {
    use broca_records::session_storage::StorageClass;
    use broca_session_store::backend::SessionAddr;
    let new = retained(cipher, Duration::from_millis(1), None).await;
    new.send("r1", Some(SECONDS)).await.unwrap();
    new.module.closed().await;
    let deadline = NOW + SECONDS * 1000;
    new.advance(deadline);
    assert_eq!(new.module.registry.sweep_once(deadline).await.unwrap(), 1);
    // Simulate an older deleted session whose numbers are above the most recent one's. Its
    // content is already gone; only the durable floors may decide the next session's numbers.
    let mut marker = new
        .runtime
        .expiry_markers(&new.module.key)
        .unwrap()
        .pop()
        .unwrap();
    marker.next_tag = 42;
    marker.next_notice_seq = 77;
    broca_session_store::expired::write(
        &StorageClass::Ephemeral.store_root(&new.module.dir),
        &SessionAddr(broca_core::session_addr(&new.module.key)),
        &broca_session_store::keys::random_id(),
        marker,
        cipher,
        new.runtime.deployment(),
    )
    .unwrap();
    broca_test_support::pin!("alike", new.send("r2", Some(SECONDS)).await.unwrap());
    new.module.closed().await;
    let view = new
        .runtime
        .current_view(&new.module.key)
        .await
        .unwrap()
        .unwrap();
    let start = view
        .operations
        .run
        .as_ref()
        .unwrap()
        .start
        .as_ref()
        .unwrap();
    let config = broca_session_loop::run::run_config_of_start(
        start,
        new.runtime.parts_for(StorageClass::Ephemeral, cipher),
    )
    .unwrap();
    // The recreated session's numbers start above the floor: 42 is the next number, so the
    // checkpoint is the last handed out.
    assert_eq!(config.computed_tags.map(|tags| tags.checkpoint), Some(41));
    broca_test_support::pin!(
        "alike",
        new.runtime
            .expiry_markers(&new.module.key)
            .unwrap()
            .iter()
            .map(|m| m.next_notice_seq)
            .max()
    );
    new.stop().await;
}

async fn expiry_reads_before_the_key_load_never_become_absent(cipher: Cipher) {
    let rig = retained(cipher, Duration::from_millis(1), None).await;
    rig.send("r1", Some(SECONDS)).await.unwrap();
    rig.module.closed().await;
    let deadline = NOW + SECONDS * 1000;
    rig.advance(deadline);
    assert_eq!(rig.module.registry.sweep_once(deadline).await.unwrap(), 1);
    rig.stop().await;
    let executor: Arc<dyn RunExecutor> = Arc::new(executor(vec![], None));
    let runtime = Arc::new(
        crate::store_open::StoreRuntime::open_without_key(
            &rig.module.dir,
            1,
            cipher,
            executor.clone(),
            TTL,
        )
        .await
        .unwrap(),
    );
    let registry = LineageRegistry::new(Arc::new(crate::opener::DefaultOpener::new(
        Arc::new(FileLeaseStore::new(rig.module.dir.join("leases"))),
        executor,
        std::sync::Arc::new(crate::store_open::StoreSlot::loaded(runtime)),
    )));
    let error = registry
        .read(SessionReadRequest::new(rig.module.key.clone(), None, None))
        .await
        .unwrap_err();
    match cipher {
        Cipher::None => assert!(
            matches!(error, ServeError::Expired { expired_at_ms } if expired_at_ms == deadline)
        ),
        Cipher::XChaCha20Poly1305 => assert!(
            matches!(error, ServeError::Open(ref message, Some(broca_protocol::ErrorClass::Transient)) if message.starts_with("key_unavailable:"))
        ),
    }
    assert!(registry.stop_and_seal().await);
}

async fn computed_numbering_resets_after_retention_alike(cipher: Cipher) {
    use broca_records::session_storage::StorageClass;
    let new = retained(cipher, Duration::from_millis(1), None).await;
    let plan = || {
        let mut manifest =
            (*super::controls::manifest(serde_json::json!({"render_tags":true}), vec![])).clone();
        manifest.system_text.clear();
        crate::actor::SendPlan {
            identity: manifest.plan_identity.clone(),
            manifest: Some(Arc::new(manifest)),
            model_check: None,
        }
    };
    broca_test_support::pin!(
        "alike",
        new.send_with_plan("r1", Some(SECONDS), Some(plan()))
            .await
            .unwrap()
    );
    new.module.closed().await;
    let deadline = NOW + SECONDS * 1000;
    new.advance(deadline);
    assert_eq!(new.module.registry.sweep_once(deadline).await.unwrap(), 1);
    let next_tag = new.runtime.expiry_markers(&new.module.key).unwrap()[0].next_tag;
    broca_test_support::pin!("the next number after the sweep", next_tag);
    assert_eq!(next_tag, 1);
    broca_test_support::pin!(
        "alike",
        new.send_with_plan("r2", Some(SECONDS), Some(plan()))
            .await
            .unwrap()
    );
    new.module.closed().await;
    let checkpoint = |view: &broca_session_store::session::Session<_>| {
        let start = view
            .operations
            .run
            .as_ref()
            .unwrap()
            .start
            .as_ref()
            .unwrap();
        broca_session_loop::run::run_config_of_start(
            start,
            new.runtime.parts_for(StorageClass::Ephemeral, cipher),
        )
        .unwrap()
    };
    let view = new
        .runtime
        .current_view(&new.module.key)
        .await
        .unwrap()
        .unwrap();
    let config = checkpoint(&view);
    assert!(config.render_tags);
    assert_eq!(config.computed_tags.unwrap().checkpoint, 0);
    broca_test_support::pin!("alike", read_json(&new.module, None, None, None).await.0);
    broca_test_support::pin!("alike", new.send("r3", None).await.unwrap());
    new.module.closed().await;
    let view = new
        .runtime
        .current_view(&new.module.key)
        .await
        .unwrap()
        .unwrap();
    let high = checkpoint(&view).computed_tags.unwrap().checkpoint;
    assert!(high > 0, "the first recreated run handed out numbers");
    broca_test_support::pin!("the checkpoint r3 froze", high);
    new.stop().await;
}

async fn stored_ledger_tags_set_the_next_number(cipher: Cipher) {
    use broca_session_store::record::Record;
    let new = retained(cipher, Duration::from_millis(1), None).await;
    new.send("r1", Some(SECONDS)).await.unwrap();
    new.module.closed().await;
    // Converted sessions retain v1's arrays. Give the recorded messages ledger-stamped numbers,
    // instead of the computed rules that new runs always freeze.
    let view = new
        .runtime
        .current_view(&new.module.key)
        .await
        .unwrap()
        .unwrap();
    let mut records = view.records_from(0).await.unwrap();
    for (_, record) in &mut records {
        match record {
            Record::RunStart(start) => {
                start.run_choices.as_mut().unwrap()["config"]["computed_tags"] =
                    serde_json::Value::Null;
            }
            Record::Input(input) => input.tags_v1 = Some(serde_json::json!([41])),
            Record::Assistant(assistant) => assistant.tags_v1 = Some(serde_json::json!([42])),
            _ => {}
        }
    }
    // The next number is one above the highest stamped.
    assert_eq!(
        crate::retention::store::counters::next_tag(&records).unwrap(),
        Some(43)
    );
    new.stop().await;
}

async fn an_active_run_outlives_the_retention_deadline_alike(cipher: Cipher) {
    let new_gate = Arc::new(Notify::new());
    let new = retained(cipher, TTL, Some(new_gate.clone())).await;
    broca_test_support::pin!("alike", new.send("r1", Some(SECONDS)).await.unwrap());
    let crossed = NOW + SECONDS * 1000;
    new.advance(crossed);
    new.module
        .registry
        .check_expired(&new.module.key, None)
        .await
        .unwrap();
    assert!(new.module.registry.slot_state(&new.module.key).is_some());
    new_gate.notify_one();
    new.idle().await;
    new.module
        .registry
        .check_expired(&new.module.key, None)
        .await
        .unwrap();
    let deadline = crossed + SECONDS * 1000;
    new.advance(deadline);
    new.deleted().await;
    new.expired_read(deadline).await;
    new.stop().await;
}

/// A later send may shorten a retained session's retention, and the session then expires at the
/// shorter deadline; once shortened, a send can't lengthen it back.
async fn a_later_send_shortens_the_retention(cipher: Cipher) {
    let new = retained(cipher, Duration::from_millis(1), None).await;
    let shorter = SECONDS / 2;
    assert!(matches!(
        new.send("r1", Some(SECONDS)).await.unwrap(),
        SendOutcome::Active { .. }
    ));
    new.idle().await;
    assert!(matches!(
        new.send("r2", Some(shorter)).await.unwrap(),
        SendOutcome::Active { .. }
    ));
    new.idle().await;
    let longer = new.send("r3", Some(SECONDS)).await.unwrap();
    assert!(
        matches!(longer, SendOutcome::RetentionInvalid(_)),
        "{longer:?}"
    );
    new.module.closed().await;
    let deadline = NOW + shorter * 1000;
    new.advance(deadline);
    new.expired_read(deadline).await;
    new.stop().await;
}

fn principal(module_id: &str) -> broca_protocol::BindPrincipal {
    broca_protocol::BindPrincipal::Reserved {
        module_id: module_id.into(),
    }
}

fn owner() -> broca_protocol::BindPrincipal {
    principal("starter")
}

impl Retained {
    /// Sends `run` from `principal`, carrying the storage policy `keep` (llm-runner
    /// `session_retention`) with no archive, backup or dumps.
    async fn send_kept(
        &self,
        run: &str,
        keep: cortexkit_role_llm_runner::session_retention::Keep,
        principal: Option<broca_protocol::BindPrincipal>,
    ) -> SendOutcome {
        let policy = cortexkit_role_llm_runner::session_retention::RetentionPolicy::new(
            keep, 0, false, false,
        );
        self.send_policy(run, Some(policy), principal).await
    }

    /// Sends `run` from `principal`, carrying the storage policy `policy` when there is one.
    async fn send_policy(
        &self,
        run: &str,
        policy: Option<cortexkit_role_llm_runner::session_retention::RetentionPolicy>,
        principal: Option<broca_protocol::BindPrincipal>,
    ) -> SendOutcome {
        let mut request = request("hello");
        request.retention_policy = policy;
        request.principal = principal;
        self.submit(run, request).await.unwrap()
    }

    async fn submit(
        &self,
        run: &str,
        request: broca_protocol::SubmissionRequest,
    ) -> Result<SendOutcome, crate::registry::OpenError> {
        self.module
            .registry
            .route_submission_with_retention(
                &self.module.key,
                RunId(run.into()),
                self.module.caller.shape(run, &SendKey::Minted, request),
                RunKind::FreshOrResume,
                SendKey::Minted,
                false,
                broca_protocol::Delivery::Queue,
                None,
                None,
                None,
                None,
                false,
            )
            .await
    }

    /// Moves the retention clock past the wall clock the run loop stamps its records with, as
    /// the two agree outside tests: the session's last activity is then behind it.
    fn caught_up(&self) {
        self.advance(broca_core::clock::epoch_millis().unwrap() + 1_000);
    }

    async fn release(
        &self,
        principal: Option<broca_protocol::BindPrincipal>,
    ) -> crate::actor::ReleaseOutcome {
        self.module
            .registry
            .route_session_release(&self.module.key, principal)
            .await
            .unwrap()
    }

    /// Asserts the session reads as never there: deleted by its policy or its release, not
    /// expired.
    async fn never_there(&self) {
        let error = self
            .module
            .registry
            .read(SessionReadRequest::new(self.module.key.clone(), None, None))
            .await
            .expect_err("a released session is gone once deleted");
        assert!(matches!(error, ServeError::UnknownSession), "{error:?}");
        let error = self
            .module
            .registry
            .check_expired(&self.module.key, Some("r1"))
            .await
            .unwrap_err();
        assert!(
            matches!(error, crate::registry::OpenError::UnknownSession),
            "{error:?}"
        );
    }
}

fn released(
    state: cortexkit_role_llm_runner::session_retention::ReleaseState,
) -> crate::actor::ReleaseOutcome {
    crate::actor::ReleaseOutcome::Released(state)
}

/// Its owner's release deletes an idle session at once, which then reads as never there, answers
/// a repeated release `absent`, and starts again at its next send.
async fn a_released_idle_session_is_deleted_at_once(cipher: Cipher) {
    use cortexkit_role_llm_runner::session_retention::{Keep, ReleaseState};
    let new = retained(cipher, TTL, None).await;
    new.send_kept("r1", Keep::forever(), Some(owner())).await;
    new.idle().await;
    new.caught_up();
    assert_eq!(
        new.release(Some(owner())).await,
        released(ReleaseState::deleted())
    );
    new.deleted().await;
    new.never_there().await;
    assert_eq!(
        new.release(Some(owner())).await,
        released(ReleaseState::absent())
    );
    assert!(matches!(
        new.send_kept("r2", Keep::forever(), Some(owner())).await,
        SendOutcome::Active { .. }
    ));
    assert!(new.runtime.holds(&new.module.key).unwrap());
    new.stop().await;
}

/// Only the principal of the session's first send releases it: another principal, or a caller
/// with none, is refused and records nothing.
async fn only_the_first_sender_releases_a_session(cipher: Cipher) {
    use cortexkit_role_llm_runner::session_retention::Keep;
    let new = retained(cipher, TTL, None).await;
    new.send_kept("r1", Keep::forever(), Some(owner())).await;
    new.idle().await;
    let stranger = principal("stranger");
    for principal in [Some(stranger), None] {
        assert!(
            matches!(
                new.release(principal).await,
                crate::actor::ReleaseOutcome::NotOwner(_)
            ),
            "only the first sender may release"
        );
    }
    assert!(new.runtime.holds(&new.module.key).unwrap());
    new.stop().await;
}

/// A release during a run answers `pending`, and the session is deleted when the run ends.
async fn a_release_during_a_run_deletes_once_it_ends(cipher: Cipher) {
    use cortexkit_role_llm_runner::session_retention::{Keep, ReleaseState};
    let gate = Arc::new(Notify::new());
    let new = retained(cipher, TTL, Some(gate.clone())).await;
    new.send_kept("r1", Keep::forever(), Some(owner())).await;
    assert_eq!(
        new.release(Some(owner())).await,
        released(ReleaseState::pending())
    );
    assert_eq!(
        new.release(Some(owner())).await,
        released(ReleaseState::pending())
    );
    assert!(new.runtime.holds(&new.module.key).unwrap());
    gate.notify_one();
    new.idle().await;
    new.caught_up();
    new.deleted().await;
    new.never_there().await;
    new.stop().await;
}

/// A send queued before its session's release still runs to its end: a release only refuses
/// sends that arrive after it. The session is deleted once the queued run has ended.
async fn a_send_queued_before_a_release_still_runs(cipher: Cipher) {
    use cortexkit_role_llm_runner::session_retention::{Keep, ReleaseState};
    let gate = Arc::new(Notify::new());
    let new = retained(cipher, TTL, Some(gate.clone())).await;
    new.send_kept("r1", Keep::forever(), Some(owner())).await;
    let queued = new.send_kept("r2", Keep::forever(), Some(owner())).await;
    assert!(matches!(queued, SendOutcome::Pending { .. }), "{queued:?}");
    assert_eq!(
        new.release(Some(owner())).await,
        released(ReleaseState::pending())
    );
    let late = new.send_kept("r3", Keep::forever(), Some(owner())).await;
    assert!(
        matches!(late, SendOutcome::RetentionRefused(_)),
        "a send after the release is refused: {late:?}"
    );
    broca_test_support::wait_until("the queued run ran", || async {
        // A notify holds at most one permit, so each poll lets the waiting model step go.
        gate.notify_one();
        let page = new
            .module
            .registry
            .read(SessionReadRequest::new(new.module.key.clone(), None, None))
            .await
            .map_err(|error| format!("{error:?}"))?;
        // Each run adds its input and its reply.
        if page.messages.len() == 4 {
            Ok(())
        } else {
            Err(format!("{} messages", page.messages.len()))
        }
    })
    .await;
    new.idle().await;
    assert!(new.runtime.holds(&new.module.key).unwrap());
    new.caught_up();
    new.deleted().await;
    new.never_there().await;
    new.stop().await;
}

/// Only the session's owner replaces the policy broca assigned it (llm-runner §9.14): a send
/// carrying any policy from another principal, even the assigned one, is refused as not the
/// owner and writes nothing, while the owner's replacement is taken.
async fn only_the_owner_replaces_an_assigned_policy(cipher: Cipher) {
    use cortexkit_role_llm_runner::session_retention::{Keep, RetentionPolicy};
    let new = retained(cipher, TTL, None).await;
    assert!(matches!(
        new.send_policy("r1", None, Some(owner())).await,
        SendOutcome::Active { .. }
    ));
    new.idle().await;
    let messages = || async {
        new.module
            .registry
            .read(SessionReadRequest::new(new.module.key.clone(), None, None))
            .await
            .unwrap()
            .messages
            .len()
    };
    let before = messages().await;
    for policy in [
        RetentionPolicy::new(Keep::days(7), 0, false, false),
        RetentionPolicy::new(Keep::forever(), 0, false, false),
    ] {
        let refused = new
            .send_policy("r2", Some(policy), Some(principal("stranger")))
            .await;
        assert!(
            matches!(
                &refused,
                SendOutcome::ScopeRefused(broca_core::ScopeRefusal::OwnerMismatch(mismatch))
                    if matches!(&**mismatch, broca_core::OwnerMismatch::FirstPrincipal { first, .. } if *first == owner())
            ),
            "{refused:?}"
        );
    }
    assert_eq!(messages().await, before, "a refused send writes nothing");
    let replaced = new
        .send_policy(
            "r3",
            Some(RetentionPolicy::new(Keep::days(7), 0, false, false)),
            Some(owner()),
        )
        .await;
    assert!(
        matches!(replaced, SendOutcome::Active { .. }),
        "{replaced:?}"
    );
    new.stop().await;
}

/// A notice never creates a session (llm-runner §9.7.3): to a name with no session, never
/// created or deleted after its release, it is refused `unknown_session` and writes nothing. To
/// a session that is there it is taken.
async fn a_notice_never_creates_a_session(cipher: Cipher) {
    use cortexkit_role_llm_runner::session_retention::{Keep, ReleaseState};
    let new = retained(cipher, TTL, None).await;
    let notice = |principal: broca_protocol::BindPrincipal| {
        let mut request = request("news for the session");
        request.message_marks = serde_json::json!({"notice": true})
            .as_object()
            .unwrap()
            .clone();
        request.principal = Some(principal);
        request
    };
    let refused = new.submit("n1", notice(owner())).await;
    assert!(
        matches!(refused, Err(crate::registry::OpenError::UnknownSession)),
        "{refused:?}"
    );
    assert!(!new.runtime.holds(&new.module.key).unwrap());
    new.send_kept("r1", Keep::forever(), Some(owner())).await;
    new.idle().await;
    let taken = new.submit("n2", notice(owner())).await.unwrap();
    assert!(
        !matches!(
            taken,
            SendOutcome::Error(_) | SendOutcome::StructuredOutputInvalid { .. }
        ),
        "{taken:?}"
    );
    new.idle().await;
    new.caught_up();
    assert_eq!(
        new.release(Some(owner())).await,
        released(ReleaseState::deleted())
    );
    new.deleted().await;
    let refused = new.submit("n3", notice(owner())).await;
    assert!(
        matches!(refused, Err(crate::registry::OpenError::UnknownSession)),
        "{refused:?}"
    );
    new.never_there().await;
    new.stop().await;
}

cipher_tests!(
    only_the_owner_replaces_an_assigned_policy,
    a_notice_never_creates_a_session,
    a_send_queued_before_a_release_still_runs,
    a_released_idle_session_is_deleted_at_once,
    only_the_first_sender_releases_a_session,
    a_release_during_a_run_deletes_once_it_ends,
    a_later_send_shortens_the_retention,
    retained_creating_sends_validate_alike,
    swept_retained_reads_and_fresh_sends_match,
    closed_retained_sessions_expire_before_any_sweep_alike,
    an_open_retained_session_expires_at_its_deadline,
    recreated_retained_sessions_carry_numbering_floors_alike,
    expiry_reads_before_the_key_load_never_become_absent,
    computed_numbering_resets_after_retention_alike,
    stored_ledger_tags_set_the_next_number,
    an_active_run_outlives_the_retention_deadline_alike,
);
