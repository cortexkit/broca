//! A completion held for the person's consent, through the model-tool route: the card, the
//! held call and the approved run.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use broca_store::grants::{GrantsStore, HeldState};
use broca_subc::{ConsentRecord, ConsentState};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use super::complete::tests::{completed, metered, route, text_call, FakeSelect, FakeSessions};
use super::complete::Complete;
use super::consent::Consent;
use super::held::HeldCalls;
use super::held_images_tests::{principal, stamp, Catalog, Io, Marked, Provider};
use super::LlmTools;
use crate::system_one::{Config, ModelPin, SystemOne, LLM_CLASSIFY_STORE_ROLE};

/// A completion asked by a run that read the person's private content waits for the person's
/// consent as a classify call does: a consent card is filed and the call held, nothing runs, and
/// a repeat of its key answers as held. Once the person allows it, the completion runs on the
/// route it was held with, under the caller's stamp kept beside the hold, and its answer becomes
/// the held call's result, which a repeat of the key then answers.
#[tokio::test]
async fn a_completion_waits_for_consent_and_runs_once_approved() {
    let clock = Arc::new(AtomicI64::new(1000));
    let now = clock.clone();
    let system = Arc::new(
        SystemOne::new(
            Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
            "complete-consent",
            Arc::new(Catalog),
            vec![ModelPin::clef(Arc::new(Provider::default()))],
            Config::default(),
            Arc::default(),
        )
        .with_store_role(LLM_CLASSIFY_STORE_ROLE)
        .with_clock(Arc::new(move || now.load(Ordering::SeqCst))),
    );
    let grants = Arc::new(GrantsStore::open_in_memory().unwrap());
    let held = Arc::new(HeldCalls::new(grants.clone(), "process".into()));
    let io = Arc::new(Io::default());
    let now = clock.clone();
    let consent = Arc::new(
        Consent::new(grants, io.clone()).with_clock(Arc::new(move || now.load(Ordering::SeqCst))),
    );
    // The test decides the card itself rather than racing a background reader.
    let stop = CancellationToken::new();
    stop.cancel();
    let sessions = FakeSessions::ending(vec![completed("Teal.", Some(1_000), Some(500))]);
    let complete = Arc::new(
        Complete::new(
            Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
            "process",
            FakeSelect::answering(Ok(route("anthropic", "claude-sonnet", Some(metered())))),
            sessions.clone(),
            "broca",
        )
        .with_poll(Duration::from_millis(1)),
    );
    let tools = Arc::new(
        LlmTools::new(
            system,
            // No provider is approved for the person's private content.
            crate::config::LlmToolsConfig {
                private_content_providers: vec![],
                ..Default::default()
            },
            Arc::new(Marked),
        )
        .with_held(held.clone())
        .with_consent(consent.clone(), stop)
        .with_complete(complete),
    );
    let call = || async {
        let body = json!({"name": "llm_complete", "call_key": "k1", "arguments": text_call()});
        let answer = tools
            .answer(
                &serde_json::to_vec(&body).unwrap(),
                Some(&principal()),
                &stamp(),
            )
            .await
            .unwrap();
        serde_json::from_str::<Value>(answer.get()).unwrap()
    };

    assert_eq!(call().await["held"]["reason"], "consent");
    assert!(
        sessions.sent().is_empty(),
        "nothing runs before the person answers"
    );
    let row = held
        .store
        .recovering(clock.load(Ordering::SeqCst))
        .unwrap()
        .into_iter()
        .find(|row| row.call_key == "k1")
        .expect("the completion is held");
    assert!(
        row.batch_id.starts_with("llm.complete:"),
        "{}",
        row.batch_id
    );
    assert_eq!(row.provider, "anthropic");
    assert_eq!(call().await["held"]["reason"], "consent");
    assert!(sessions.sent().is_empty());

    *io.answer.lock().unwrap() = Some(ConsentRecord {
        elicitation_id: "card".into(),
        state: ConsentState::Answered,
        fields: BTreeMap::from([
            ("requester_ref".into(), json!(row.dedup_key)),
            ("answered_choice_id".into(), json!("allow_run")),
        ]),
    });
    consent
        .wait_once(&row.dedup_key, clock.load(Ordering::SeqCst))
        .await
        .unwrap();
    tools.recover_authorized().unwrap();
    let settled = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current = held
                .store
                .lookup(&row.batch_id, clock.load(Ordering::SeqCst))
                .unwrap()
                .expect("the held call is kept");
            if current.state == HeldState::Final {
                break current;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the approved completion runs");
    let result = settled.final_result.expect("a result");
    assert!(!result.is_error);
    assert!(result.body.contains("Teal."), "{}", result.body);
    let sent = sessions.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(
        sent[0].request.scope,
        stamp(),
        "charged to the caller's run"
    );
    assert_eq!(sent[0].request.model.provider_module_id, "anthropic");
    assert_eq!(call().await["text"], "Teal.");
    assert_eq!(sessions.sent().len(), 1);
}
