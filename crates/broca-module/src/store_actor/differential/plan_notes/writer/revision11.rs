use super::*;
use broca_provider::{
    families::{anthropic_frozen_config, AnthropicMessages},
    wire::{FrozenRenderConfig, WireFamily},
};

fn config() -> FrozenRenderConfig {
    let mut frozen = anthropic_frozen_config("claude-opus-5-5");
    frozen.renderer_version = 4;
    frozen
}

fn on_route(steps: Vec<Vec<NormalizedEvent>>, frozen: &FrozenRenderConfig) -> LoopExecutor {
    let mut executor = executor(steps, None);
    executor.provider.render_config = Some(serde_json::to_value(frozen).unwrap());
    executor
}

#[tokio::test]
async fn an_open_v2_attempt_rebuilds_without_a_bulk_sealed_history_reader() {
    let mut frozen = config();
    frozen.renderer_version = 2;
    let first = manifest(&plan("same", &["read"], None, None), "plan-1");
    let (module, executor) = module_with_executor(
        on_route(
            vec![
                confirmed(text_step("a1", "before rotate")),
                confirmed(text_step("a2", "after rotate")),
            ],
            &frozen,
        ),
        true,
        false,
    )
    .await;
    send_claude(&module, "r1", Some(&first)).await;
    let mut session = module.load().await;
    session.seal(None, 0).await.unwrap();
    drop(session);
    send_claude(&module, "r2", None).await;
    let session = module.load().await;
    let open = session.open_log_records().await.unwrap();
    assert!(open.first().unwrap().0 > 1);
    assert!(open
        .iter()
        .any(|(_, record)| matches!(record, Record::Attempt(attempt) if attempt.run_id == "r2")));
    let runtime = crate::store_open::StoreRuntime::open(
        &module.dir,
        broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
        executor.clone(),
        IDLE,
    )
    .await
    .unwrap();
    let parts = runtime.parts_for(
        broca_records::session_storage::StorageClass::Ordinary,
        broca_session_store::keys::Cipher::XChaCha20Poly1305,
    );
    let rebuilt = crate::store_actor::recorded_warm::recorded_request(
        &session,
        None::<&broca_session_store::backend::LocalBackend>,
        parts,
        &module.key,
        "r2",
        Some(1),
        crate::store_actor::recorded_warm::Through::Request,
    )
    .await
    .unwrap();
    assert_eq!(
        AnthropicMessages.render(&frozen, &rebuilt.request.opts),
        AnthropicMessages.render(&frozen, &executor.provider.inner.calls()[1])
    );
}

#[tokio::test]
async fn open_attempt_after_sealed_step_start_rebuilds_through_the_readers() {
    use broca_session_store::{
        backend::{MemoryBackend, SessionAddr},
        head::SessionTriple,
        keys::DeploymentKey,
        session::Session,
    };
    let mut frozen = config();
    frozen.renderer_version = 2;
    let first = manifest(&plan("same", &["read"], None, None), "plan-1");
    let (module, executor) = module_with_executor(
        on_route(
            vec![confirmed(text_step("a1", "sent before rotation"))],
            &frozen,
        ),
        true,
        false,
    )
    .await;
    send_claude(&module, "r1", Some(&first)).await;
    let records = records(&module).await;
    let attempt = records
        .iter()
        .position(|record| matches!(record, Record::Attempt(_)))
        .unwrap();
    let backend = MemoryBackend::default();
    let mut session = Session::create(
        &backend,
        SessionAddr("abcdef0123456789".into()),
        SessionTriple {
            project_root: "/project".into(),
            harness: "test".into(),
            session: "s1".into(),
        },
        &DeploymentKey::new(1, &[7; 32]),
        false,
    )
    .await
    .unwrap();
    assert!(records[..attempt]
        .iter()
        .any(|record| matches!(record, Record::StepStart(_))));
    session.append(&records[..attempt], true).await.unwrap();
    session.seal(None, 0).await.unwrap();
    session.append(&records[attempt..], true).await.unwrap();
    let open = session.open_log_records().await.unwrap();
    assert!(!open
        .iter()
        .any(|(_, record)| matches!(record, Record::StepStart(_))));
    assert!(open
        .iter()
        .any(|(_, record)| matches!(record, Record::Attempt(_))));
    let runtime = crate::store_open::StoreRuntime::open(
        &module.dir,
        DeploymentKey::new(1, &[7; 32]),
        executor.clone(),
        IDLE,
    )
    .await
    .unwrap();
    let parts = runtime.parts_for(
        broca_records::session_storage::StorageClass::Ordinary,
        broca_session_store::keys::Cipher::XChaCha20Poly1305,
    );
    let rebuilt = crate::store_actor::recorded_warm::recorded_request(
        &session,
        Some(&backend),
        parts,
        &module.key,
        "r1",
        Some(1),
        crate::store_actor::recorded_warm::Through::Request,
    )
    .await
    .unwrap();
    assert_eq!(
        AnthropicMessages.render(&frozen, &rebuilt.request.opts),
        AnthropicMessages.render(&frozen, &executor.provider.inner.calls()[0])
    );
}

#[tokio::test]
async fn v4_warm_rebuild_avoids_bulk_sealed_reads_for_an_open_attempt() {
    use broca_session_store::{
        backend::{MemoryBackend, SessionAddr},
        head::SessionTriple,
        keys::DeploymentKey,
        session::Session,
    };
    let frozen = config();
    let first = manifest(&plan("same", &["read"], None, None), "plan-1");
    let (module, executor) = module_with_executor(
        on_route(
            vec![
                confirmed(text_step("a1", "before seal")),
                confirmed(text_step("a2", "after seal")),
            ],
            &frozen,
        ),
        true,
        false,
    )
    .await;
    send_claude(&module, "r1", Some(&first)).await;
    send_claude(&module, "r2", None).await;
    let records = records(&module).await;
    let split = records
        .iter()
        .position(|record| matches!(record, Record::StepStart(start) if start.run_id == "r2"))
        .unwrap();
    let backend = MemoryBackend::default();
    let mut session = Session::create(
        &backend,
        SessionAddr("abcdef0123456789".into()),
        SessionTriple {
            project_root: "/project".into(),
            harness: "test".into(),
            session: "s1".into(),
        },
        &DeploymentKey::new(1, &[7; 32]),
        false,
    )
    .await
    .unwrap();
    session.append(&records[..split], true).await.unwrap();
    session.seal(None, 0).await.unwrap();
    session.append(&records[split..], true).await.unwrap();
    let runtime = crate::store_open::StoreRuntime::open(
        &module.dir,
        DeploymentKey::new(1, &[7; 32]),
        executor.clone(),
        IDLE,
    )
    .await
    .unwrap();
    let parts = runtime.parts_for(
        broca_records::session_storage::StorageClass::Ordinary,
        broca_session_store::keys::Cipher::XChaCha20Poly1305,
    );
    let readers = backend.snapshot();
    let (objects, logs) = (backend.object_reads(), backend.log_reads());
    let rebuilt = crate::store_actor::recorded_warm::recorded_request(
        &session,
        Some(&readers),
        parts,
        &module.key,
        "r2",
        Some(1),
        crate::store_actor::recorded_warm::Through::Request,
    )
    .await
    .unwrap();
    let counts = (backend.object_reads() - objects, backend.log_reads() - logs);
    println!(
        "WARM_READS bulk_objects={} indexed_objects={} open_logs={}",
        readers.object_reads(),
        counts.0,
        counts.1
    );
    assert_eq!(readers.object_reads(), 0);
    assert_eq!(
        AnthropicMessages.render(&frozen, &rebuilt.request.opts),
        AnthropicMessages.render(&frozen, &executor.provider.inner.calls()[1])
    );
}

#[tokio::test]
async fn recorded_warm_uses_the_model_attempt_not_the_later_warm_reservation() {
    use broca_session_store::{
        backend::{MemoryBackend, SessionAddr},
        codec::{hex, Digest},
        head::SessionTriple,
        keys::DeploymentKey,
        session::Session,
    };
    let frozen = config();
    let first = manifest(&plan("same", &["read"], None, None), "plan-1");
    let (module, executor) = module_with_executor(
        on_route(vec![confirmed(text_step("a1", "model answer"))], &frozen),
        true,
        false,
    )
    .await;
    send_claude(&module, "r1", Some(&first)).await;
    let mut records = records(&module).await;
    let mut warm = records
        .iter()
        .find_map(|record| match record {
            Record::Attempt(attempt) => Some(attempt.clone()),
            _ => None,
        })
        .unwrap();
    let digest = hex(&warm.request_body_sha256.0);
    warm.attempt += 1;
    warm.request_body_sha256 = Digest([23; 32]);
    warm.billing = Some(serde_json::json!({"reservation":{"warm":true}}).into());
    records.push(Record::Attempt(warm));
    let backend = MemoryBackend::default();
    let mut session = Session::create(
        &backend,
        SessionAddr("abcdef0123456789".into()),
        SessionTriple {
            project_root: "/project".into(),
            harness: "test".into(),
            session: "s1".into(),
        },
        &DeploymentKey::new(1, &[7; 32]),
        false,
    )
    .await
    .unwrap();
    session.append(&records, true).await.unwrap();
    let runtime = crate::store_open::StoreRuntime::open(
        &module.dir,
        DeploymentKey::new(1, &[7; 32]),
        executor,
        IDLE,
    )
    .await
    .unwrap();
    let parts = runtime.parts_for(
        broca_records::session_storage::StorageClass::Ordinary,
        broca_session_store::keys::Cipher::XChaCha20Poly1305,
    );
    let request = crate::store_actor::recorded_warm::recorded_request(
        &session,
        Some(&backend),
        parts,
        &module.key,
        "r1",
        Some(1),
        crate::store_actor::recorded_warm::Through::Request,
    )
    .await
    .unwrap();
    assert_eq!(request.request.request_body_sha256, digest);
}
