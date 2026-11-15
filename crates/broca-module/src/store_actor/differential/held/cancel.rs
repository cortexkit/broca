//! Cancelling a parked run ends the same episode and closes held obligations on both stores.

use super::*;
use crate::actor::CancelOutcome;
use crate::held_withdrawals::{HeldWithdrawals, WithdrawTransport};
use crate::late_results_store::LateResultsStore;
use broca_core::tool_plane::LateResultRegistration;
use broca_protocol::PauseReason;
use broca_session_store::record::Record;
use broca_wal::{record::RunCancelAt, WalRecord};
use cortexkit_role_tool_provider::withdraw::WithdrawVerdict;
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;

cipher_tests!(
    a_parked_held_run_cancels_alike,
    other_parked_pauses_cancel_alike,
    a_parked_cancel_request_survives_restart
);

#[derive(Default)]
pub(super) struct Withdrawals(pub(super) Mutex<Vec<(String, bool)>>);

#[async_trait]
impl WithdrawTransport for Withdrawals {
    async fn withdraw(
        &self,
        call: &LateResultRegistration,
        identity: bool,
    ) -> (WithdrawVerdict, Value) {
        self.0
            .lock()
            .unwrap()
            .push((call.call_key.clone(), identity));
        (WithdrawVerdict::NeverRan, json!({"answer": "never_ran"}))
    }
}

pub(super) async fn terminal(side: &Side) -> (Value, Vec<Value>) {
    if side.store {
        let view = side
            .registry
            .store_runtime()
            .unwrap()
            .current_view(&side.key)
            .await
            .unwrap()
            .unwrap();
        let records = view.records_from(0).await.unwrap();
        let end = records
            .iter()
            .find_map(|(_, record)| match record {
                Record::RunEnd(end) => Some(json!(end.reason)),
                _ => None,
            })
            .unwrap();
        let calls = records.iter().filter_map(|(_, record)| match record {
            Record::ToolOutcome(outcome) => Some(json!({"outcome": format!("{:?}", outcome.outcome).to_ascii_lowercase(), "reason": outcome.reason})),
            _ => None,
        }).collect();
        (end, calls)
    } else {
        let records = side.executor.wal.read_records(&side.key).unwrap();
        let end = records
            .iter()
            .find_map(|frame| match &frame.record {
                WalRecord::RunFinished { reason, .. } => Some(json!(reason)),
                _ => None,
            })
            .unwrap();
        let calls = records.iter().filter_map(|frame| match &frame.record {
            WalRecord::ToolResult {outcome, reason, ..} => Some(json!({"outcome": format!("{outcome:?}").to_ascii_lowercase(), "reason": reason})),
            _ => None,
        }).collect();
        (end, calls)
    }
}

pub(super) async fn cancelled(side: &Side) {
    broca_test_support::wait_until("parked run cancellation finishes", || async {
        let status = side.status().await;
        if status["state"] == "cancelled" {
            Ok(())
        } else {
            Err(status.to_string())
        }
    })
    .await;
    side.idle().await;
}

pub(super) async fn position(side: &Side) -> u64 {
    if side.store {
        side.registry
            .store_runtime()
            .unwrap()
            .current_view(&side.key)
            .await
            .unwrap()
            .unwrap()
            .next_seq()
    } else {
        side.executor
            .wal
            .read_records(&side.key)
            .unwrap()
            .last()
            .unwrap()
            .seq
            + 1
    }
}

async fn a_parked_held_run_cancels_alike(cipher: Cipher) {
    for at in [RunCancelAt::Now, RunCancelAt::StepBoundary] {
        let mut observations = Vec::new();
        for store in [false, true] {
            let side = Side::new(
                store,
                cipher,
                vec![super::super::super::tests::read_step("m1")],
            )
            .await;
            side.send().await;
            side.idle().await;
            let status = side.status().await;
            assert_eq!(status["state"], "active");
            let key = status["held_calls"][0]["call_key"]
                .as_str()
                .unwrap()
                .to_owned();
            let db = Arc::new(LateResultsStore::open(&side.dir.join("late-results.db")).unwrap());
            let call = LateResultRegistration {
                lineage: key.rsplit_once(':').unwrap().0.into(),
                call_key: key.clone(),
                module: "tools".into(),
                bind: side.key.clone(),
                scope: None,
            };
            db.register_calls(std::slice::from_ref(&call)).unwrap();
            let transport = Arc::new(Withdrawals::default());
            let withdrawals =
                HeldWithdrawals::new(db.clone(), side.executor.wal.clone(), transport.clone());
            withdrawals.bind_stopping_signal(CancellationToken::new());
            withdrawals.with_store_runtime(side.registry.store_runtime().unwrap());
            if at == RunCancelAt::Now {
                side.executor.slots.with_withdrawals(withdrawals.clone());
            }
            let reply = side
                .registry
                .route_cancel_at(&side.key, RunId("r1".into()), at)
                .await
                .unwrap();
            assert_eq!(reply, CancelOutcome::Ack, "store={store}, at={at:?}");
            cancelled(&side).await;
            if at == RunCancelAt::StepBoundary {
                // Recover the durable end-to-withdrawal gap without a live withdrawal hook.
                withdrawals.recover().await.unwrap();
            }
            broca_test_support::wait_until("held withdrawal sent", || async {
                if transport.0.lock().unwrap().len() == 1 {
                    Ok(())
                } else {
                    Err(withdrawals.health().to_string())
                }
            })
            .await;
            assert_eq!(
                transport.0.lock().unwrap().as_slice(),
                [(key.clone(), false)]
            );
            assert_eq!(
                db.withdrawal_state("tools", &key).unwrap(),
                Some(("done".into(), 1))
            );
            // Withdrawal is not delivery: the registration stays after withdrawal.
            assert!(db.registration("tools", &key).unwrap().is_some());
            let before = (
                side.status().await,
                side.result().await,
                terminal(&side).await,
            );
            assert_eq!(before.2 .0, "cancelled");
            assert_eq!(
                before.2 .1,
                [json!({"outcome": "interrupted", "reason": "provider_held_cancelled"})]
            );
            assert!(side.executor.slots.is_empty());
            let before_resume = position(&side).await;
            side.registry
                .resume_for_late_result(&side.key)
                .await
                .unwrap();
            side.idle().await;
            assert_eq!(
                position(&side).await,
                before_resume,
                "late resume must append nothing"
            );
            assert_eq!(
                (
                    side.status().await,
                    side.result().await,
                    terminal(&side).await
                ),
                before
            );
            assert_eq!(side.executor.tools.0.load(Ordering::SeqCst), 1);
            assert_eq!(side.executor.provider.inner.calls().len(), 1);
            assert_eq!(
                side.registry
                    .route_cancel(&side.key, RunId("r1".into()))
                    .await
                    .unwrap(),
                CancelOutcome::NotActive
            );
            observations.push((format!("{reply:?}"), before));
            assert!(side.registry.stop_and_seal().await);
        }
        alike!(observations[1], observations[0]);
    }
}

async fn other_parked_pauses_cancel_alike(cipher: Cipher) {
    for reason in [
        PauseReason::AuthRequired,
        PauseReason::AccountLimit,
        PauseReason::Restart,
    ] {
        for at in [RunCancelAt::Now, RunCancelAt::StepBoundary] {
            let mut observations = Vec::new();
            for store in [false, true] {
                let side = Side::new(store, cipher, vec![auth_failure()]).await;
                *side.executor.pause_as.lock().unwrap() = Some(reason);
                side.send().await;
                side.idle().await;
                let status = side.status().await;
                assert_eq!(status["state"], "paused");
                assert_eq!(status["reason"], json!(reason));
                assert_eq!(
                    side.registry
                        .route_cancel_at(&side.key, RunId("other".into()), at)
                        .await
                        .unwrap(),
                    CancelOutcome::NotActive
                );
                let reply = side
                    .registry
                    .route_cancel_at(&side.key, RunId("r1".into()), at)
                    .await
                    .unwrap();
                assert_eq!(
                    reply,
                    CancelOutcome::Ack,
                    "store={store}, reason={reason:?}, at={at:?}"
                );
                cancelled(&side).await;
                let end = terminal(&side).await;
                assert_eq!(end.0, "cancelled");
                assert!(end.1.is_empty());
                assert_eq!(side.executor.provider.inner.calls().len(), 1);
                observations.push((
                    format!("{reply:?}"),
                    side.status().await,
                    side.result().await,
                    end,
                ));
                assert!(side.registry.stop_and_seal().await);
            }
            alike!(
                observations[1],
                observations[0],
                "reason={reason:?}, at={at:?}"
            );
        }
    }
}

/// A crash after the durable request must not strand the paused run before its finalizer starts.
async fn a_parked_cancel_request_survives_restart(cipher: Cipher) {
    use broca_session_store::backend::{LocalBackend, SessionAddr};
    use broca_session_store::record::{CancelPoint, CancelRequest};

    for reason in [
        PauseReason::ToolHeld,
        PauseReason::AuthRequired,
        PauseReason::AccountLimit,
        PauseReason::Restart,
    ] {
        let source = Side::new(
            true,
            cipher,
            vec![if reason == PauseReason::ToolHeld {
                super::super::super::tests::read_step("m1")
            } else {
                auth_failure()
            }],
        )
        .await;
        if reason != PauseReason::ToolHeld {
            *source.executor.pause_as.lock().unwrap() = Some(reason);
        }
        source.send().await;
        source.idle().await;
        let recovered = source.restarted().await;
        let runtime = recovered.registry.store_runtime().unwrap();
        let class = runtime.class_of(&recovered.key).unwrap().unwrap();
        let mut session = Session::load(
            LocalBackend::new(class.store_root(&recovered.dir)),
            SessionAddr(broca_core::session_addr(&recovered.key)),
            &crate::store_open::triple(&recovered.key),
            runtime.deployment(),
        )
        .await
        .unwrap();
        assert_eq!(
            session.operations.run.as_ref().unwrap().paused,
            Some(reason)
        );
        Writer::append(
            &mut session,
            &[Record::CancelRequest(CancelRequest {
                run_id: "r1".into(),
                at: CancelPoint::StepBoundary,
            })],
            true,
        )
        .await
        .unwrap();
        drop(session);
        recovered.idle().await;
        cancelled(&recovered).await;
        assert_eq!(terminal(&recovered).await.0, "cancelled");
        assert!(recovered.executor.provider.inner.calls().is_empty());
        assert!(source.registry.stop_and_seal().await);
        assert!(recovered.registry.stop_and_seal().await);
    }
}
