use super::transport_failure_tests::HttpReceiver;
use super::*;
use broca_core::{
    provider::StepRuntime,
    run::{AttemptHook, PhysicalSend},
    Provider, WalStore,
};
use broca_protocol::{
    CallOptions, ModelAttempt, NormalizedEvent, ProviderControl, ProviderError, StepId,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio_util::sync::CancellationToken;

struct PermissionSequence {
    calls: AtomicUsize,
    settled: AtomicUsize,
    entered: tokio::sync::mpsc::UnboundedSender<(usize, PhysicalSend)>,
    unreceipted: Arc<dyn AttemptHook>,
}

#[async_trait::async_trait]
impl AttemptHook for PermissionSequence {
    async fn before_send(
        &self,
        send: PhysicalSend,
        cancel: CancellationToken,
    ) -> Result<broca_wal::record::AttemptId, ProviderError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.send((call, send.clone())).unwrap();
        match call {
            0 => Err(serde_json::from_value(json!({"class":"permanent","message":"reservation denied by fixture","provider_code":"attempt_durability_failed"})).unwrap()),
            1 | 2 => self.unreceipted.before_send(send, cancel).await,
            3 => Ok(broca_core::attempt_id::mint_attempt_id().unwrap()),
            _ => panic!("an ungranted send must not be silently retried"),
        }
    }

    async fn finished(
        &self,
        _: broca_wal::record::AttemptId,
        _: ModelAttempt,
        _: broca_wal::record::BillingAttemptOutcome,
    ) -> Result<(), ProviderError> {
        self.settled.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

async fn drain(mut events: tokio::sync::mpsc::Receiver<NormalizedEvent>) -> Vec<NormalizedEvent> {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut out = Vec::new();
        while let Some(event) = events.recv().await {
            out.push(event);
        }
        out
    })
    .await
    .expect("each permission outcome or granted HTTP request terminates")
}

async fn next_permission(
    permissions: &mut tokio::sync::mpsc::UnboundedReceiver<(usize, PhysicalSend)>,
) -> (usize, PhysicalSend) {
    tokio::time::timeout(std::time::Duration::from_secs(5), permissions.recv())
        .await
        .expect("render must reach the reservation hook")
        .expect("hook stays attached")
}

fn append_failed_attempts(
    writer: &mut (dyn broca_wal::Wal + Send),
    head: &AtomicUsize,
    events: &[NormalizedEvent],
) -> usize {
    let mut written = 0;
    for event in events {
        if let NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) = event {
            let seq = writer
                .append(
                    1,
                    &WalRecord::model_attempt_finished(StepId(2), attempt.clone()),
                    true,
                )
                .unwrap();
            head.store(seq as usize, Ordering::Release);
            written += 1;
        }
    }
    written
}

fn assert_full_replay_aging(
    fixture: &Fixture,
    frozen: &crate::wire::FrozenRenderConfig,
    opts: &CallOptions,
    records: &[WalRecord],
    media: &LiveMedia,
    provider: &impl Provider,
) -> Vec<u8> {
    let profile = frozen.media.as_ref().unwrap();
    let mut full = fixture.session(&[]);
    let aging = MediaAging::replay_wal(profile, &mut full, records).unwrap();
    let mut bundle = prepare(&full, Some(profile), &opts.prompt, &aging).unwrap();
    assert_eq!(
        aging.aged.len(),
        2,
        "one boundary ages two old occurrences down to the target"
    );
    assert_eq!(
        bundle.media.len(),
        1,
        "the last tool occurrence must remain visible"
    );
    let expected = render_prepared(
        &*crate::registry::wire_family(frozen.family_id).unwrap(),
        &frozen.for_step(opts),
        opts,
        Some(&mut bundle),
        &|prompt| prompt.to_vec(),
    )
    .unwrap();
    assert_eq!(provider.try_render_request(opts).unwrap(), expected);
    let memory = media.aging_for_test();
    assert_eq!(
        memory.aged, aging.aged,
        "denial/cancellation must not evict another occurrence"
    );
    assert_eq!(memory.totals(), aging.totals());
    expected
}

#[tokio::test]
async fn reservation_denial_timeout_cancel_then_retry_keep_media_aging_equal_to_full_replay() {
    let fixture = Fixture::new();
    let receiver = HttpReceiver::start();
    let mut frozen = super::live_tests::frozen(WireFamilyId::AnthropicMessages);
    frozen.base_url = Some(receiver.url.clone());
    frozen.speed = Some("fast".into());
    let encoded = STANDARD.encode(&fixture.bytes).len() as u64;
    let profile = frozen.media.as_mut().unwrap();
    profile.aging.byte_trigger = encoded * 2;
    profile.aging.byte_target = encoded;
    profile.aging.token_trigger = u64::MAX;
    profile.aging.token_target = u64::MAX;
    let mut records = super::live_tests::history(&fixture, &frozen);
    for record in &mut records {
        if let WalRecord::ToolResult { result, .. } = record {
            if let ToolOutput::Parts { parts } = &mut Arc::make_mut(result).output {
                parts.push(ToolOutputPart::Media {
                    media: fixture.media.clone(),
                });
            }
        }
    }
    let WalRecord::RunStarted {
        session: identity, ..
    } = &records[0]
    else {
        unreachable!()
    };
    let wal = Arc::new(broca_core::FileWalStore::new(fixture.root.join("wal")));
    let mut writer = wal.open(identity).unwrap();
    for record in &records {
        writer.append(1, record, true).unwrap();
    }
    let range = wal.read_records_range(identity, 0, None, None).unwrap();
    let anchor = Mutex::new(range.next_anchor);
    let frames = match range.into_records() {
        broca_wal::WalRangeRecords::Complete(frames) => frames,
        _ => panic!("file WAL is complete"),
    };
    let checkpoint = broca_core::AdmissionCheckpointV1::from_frames(&frames).unwrap();
    let head = Arc::new(AtomicUsize::new(checkpoint.at_seq as usize));
    let Some(broca_core::media_request::DurableMediaSeed::Ready(seed)) = checkpoint.media else {
        panic!("captured metadata seed")
    };
    let source = wal.clone();
    let session = identity.clone();
    let durable_head = head.clone();
    let media = Arc::new(
        LiveMedia::from_checkpoint(
            fixture.store.clone(),
            *seed,
            None,
            frozen.media.clone(),
            Box::new(move |after| {
                let through = durable_head.load(Ordering::Acquire) as u64;
                if after == through {
                    return Ok(Vec::new());
                }
                let mut anchor = anchor.lock().unwrap();
                let range = source
                    .read_records_range_from_valid_anchor(
                        &session,
                        after,
                        Some(through),
                        anchor.as_ref().unwrap(),
                    )
                    .map_err(|error| error.to_string())?
                    .expect("append keeps the anchor valid");
                *anchor = range.next_anchor;
                match range.into_records() {
                    broca_wal::WalRangeRecords::Complete(frames) => Ok(frames),
                    _ => panic!("durable suffix is complete"),
                }
            }),
        )
        .unwrap(),
    );
    let provider = crate::transport::LiveProvider::new(
        crate::families::AnthropicMessages,
        frozen.clone(),
        crate::wire::AuthHandle::None,
    );
    let shared = media.clone();
    provider.set_media_request_renderer(Arc::new(move |config, opts, tags, commit, provenance| {
        shared.render_with_provenance(config, opts, tags, commit, provenance)
    }));
    provider.defer_media_tags(false);
    let opts = Arc::new(CallOptions::new(
        broca_core::replay::replay(&records).unwrap().prompt,
    ));
    let (unreceipted, _receipt_guard) = broca_core::run::unreceipted_attempt_hook_for_test();
    let (entered, mut permissions) = tokio::sync::mpsc::unbounded_channel();
    let hook = Arc::new(PermissionSequence {
        calls: AtomicUsize::new(0),
        settled: AtomicUsize::new(0),
        entered,
        unreceipted,
    });
    let start = |first_attempt, cancel| StepRuntime {
        cancel,
        deadline: None,
        seal: None,
        progress: None,
        first_attempt,
        attempt_hook: Some(hook.clone()),
    };

    let denied = drain(
        provider
            .step_with_runtime(StepId(2), opts.clone(), start(0, CancellationToken::new()))
            .await,
    )
    .await;
    let (call, send) = next_permission(&mut permissions).await;
    assert_eq!(call, 0);
    assert_eq!(send.speed_requested.as_deref(), Some("fast"));
    assert!(denied.iter().any(|event| matches!(event, NormalizedEvent::Control(ProviderControl::Error { error, .. }) if error.message.contains("reservation denied"))));
    assert_eq!(media.aging_for_test().aged.len(), 2);
    assert_eq!(append_failed_attempts(writer.as_mut(), &head, &denied), 1);
    let prefix = wal
        .read_records(identity)
        .unwrap()
        .into_iter()
        .map(|frame| frame.record)
        .collect::<Vec<_>>();
    assert_full_replay_aging(&fixture, &frozen, &opts, &prefix, &media, &provider);
    assert_eq!(receiver.accepted(), 0);

    let timed = provider
        .step_with_runtime(StepId(2), opts.clone(), start(1, CancellationToken::new()))
        .await;
    assert_eq!(
        next_permission(&mut permissions).await.0,
        1,
        "rendering reached the real receipt wait"
    );
    tokio::time::pause();
    tokio::time::advance(std::time::Duration::from_secs(31)).await;
    let timed = drain(timed).await;
    tokio::time::resume();
    assert!(timed.iter().any(|event| matches!(event, NormalizedEvent::Control(ProviderControl::Error { error, .. }) if error.message.contains("permission deadline"))));
    assert_eq!(media.aging_for_test().aged.len(), 2);
    assert_eq!(append_failed_attempts(writer.as_mut(), &head, &timed), 1);
    let prefix = wal
        .read_records(identity)
        .unwrap()
        .into_iter()
        .map(|frame| frame.record)
        .collect::<Vec<_>>();
    assert_full_replay_aging(&fixture, &frozen, &opts, &prefix, &media, &provider);
    assert_eq!(receiver.accepted(), 0);

    let cancel = CancellationToken::new();
    let cancelled = provider
        .step_with_runtime(StepId(2), opts.clone(), start(2, cancel.clone()))
        .await;
    assert_eq!(
        next_permission(&mut permissions).await.0,
        2,
        "cancel only after rendering and entering permission wait"
    );
    cancel.cancel();
    let cancelled = drain(cancelled).await;
    assert_eq!(media.aging_for_test().aged.len(), 2);
    assert_eq!(
        append_failed_attempts(writer.as_mut(), &head, &cancelled),
        1
    );
    let prefix = wal
        .read_records(identity)
        .unwrap()
        .into_iter()
        .map(|frame| frame.record)
        .collect::<Vec<_>>();
    let expected = assert_full_replay_aging(&fixture, &frozen, &opts, &prefix, &media, &provider);
    assert_eq!(receiver.accepted(), 0);

    let allowed = drain(
        provider
            .step_with_runtime(StepId(2), opts.clone(), start(3, CancellationToken::new()))
            .await,
    )
    .await;
    assert!(!allowed.is_empty());
    let (call, send) = next_permission(&mut permissions).await;
    assert_eq!(call, 3);
    assert_eq!(send.speed_requested.as_deref(), Some("fast"));
    assert_eq!(send.request_body_bytes, expected.len() as u64);
    assert_eq!(hook.calls.load(Ordering::SeqCst), 4);
    assert_eq!(hook.settled.load(Ordering::SeqCst), 1);
    assert_eq!(receiver.bodies(), vec![expected]);
    assert_eq!(
        receiver.finish().await,
        1,
        "only the granted retry may make a physical send"
    );
    assert_full_replay_aging(&fixture, &frozen, &opts, &prefix, &media, &provider);
}
