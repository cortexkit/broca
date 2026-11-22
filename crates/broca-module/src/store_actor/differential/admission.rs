//! The first send's baseline, including retries and actor reopen, pinned.

use std::sync::Arc;

use broca_protocol::RunId;
use broca_session_store::keys::Cipher;

use super::super::tests::{request, text_step};
use super::head::both_with_cipher;
use super::Module;
use crate::actor::{SendKey, SendOutcome, SendPlan};
use crate::executor::RunKind;
use crate::serve::ServeHandler;

async fn send(
    module: &Module,
    run: &str,
    manifest: &Arc<broca_records::record::PlanManifest>,
) -> SendOutcome {
    let mut request = request("hello");
    request.tools = None;
    module
        .registry
        .route_submission_with_order(
            &module.key,
            RunId(run.into()),
            module.caller.shape(run, &SendKey::RunId, request),
            RunKind::FreshOrResume,
            SendKey::RunId,
            false,
            broca_protocol::Delivery::Queue,
            Some(SendPlan {
                identity: manifest.plan_identity.clone(),
                manifest: Some(Arc::clone(manifest)),
                model_check: None,
            }),
            None,
            None,
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn the_admitting_sends_baseline_and_retries_match_in_both_cipher_modes() {
    compare(false).await;
}

#[tokio::test]
async fn a_planned_admitting_send_carries_its_cache_declaration_in_both_cipher_modes() {
    compare(true).await;
}

struct DeclaredCache;

impl crate::cache::CacheDeclarer for DeclaredCache {
    fn declare(
        &self,
        _: &broca_records::record::RunConfig,
    ) -> Option<crate::cache::CacheDeclaration> {
        Some(crate::cache::CacheDeclaration {
            ttl_ms: Some(300_000),
            ttl_options_ms: vec![300_000, 3_600_000],
            read_refreshes_ttl: Some(true),
            warm: Ok(()),
            basis: Some("frozen-policy".into()),
            requested_ttl_ms: Some(300_000),
        })
    }
}

async fn compare(cache: bool) {
    for cipher in [Cipher::XChaCha20Poly1305, Cipher::None] {
        let steps = vec![text_step("a1", "first"), text_step("a2", "second")];
        let (store, store_gate) = both_with_cipher(steps, true, cipher).await;
        if cache {
            store.registry.set_cache_declarer(Arc::new(DeclaredCache));
        }
        let manifest = Arc::new(super::refresh::first_plan());
        let mut first = None;
        for run in ["r1", "r2"] {
            let mut baselines = Vec::new();
            for (module, gate) in [(&store, &store_gate)] {
                let sent = send(module, run, &manifest).await;
                assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
                let baseline = module
                    .registry
                    .admission_baseline(&module.key, &RunId(run.into()));
                let retry = send(module, run, &manifest).await;
                assert!(matches!(retry, SendOutcome::Active { .. }), "{retry:?}");
                assert_eq!(
                    module
                        .registry
                        .admission_baseline(&module.key, &RunId(run.into())),
                    baseline
                );
                if run == "r2" {
                    assert_eq!(
                        module
                            .registry
                            .admission_baseline(&module.key, &RunId("r1".into())),
                        first
                    );
                }
                baselines.push(baseline);
                gate.as_ref().unwrap().notify_one();
                module.closed().await;
            }
            broca_test_support::pin!(format!("baseline of {run}"), baselines[0]);
            if run == "r1" {
                assert!(
                    baselines[0].is_some(),
                    "the admitting send carries a baseline"
                );
                assert_eq!(baselines[0].as_ref().unwrap().cache.is_some(), cache);
                first = baselines[0].clone();
            } else {
                assert!(baselines[0].is_none(), "a later send carries no baseline");
            }
        }
    }
}
