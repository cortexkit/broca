use super::*;
use broca_core::media_request::{DurableMediaSeed, MediaCheckpoint};
use broca_protocol::CallOptions;
use broca_wal::FramedRecord;
use std::sync::{Arc, Mutex};

fn frames(records: &[WalRecord]) -> Vec<FramedRecord> {
    records
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, record)| FramedRecord {
            seq: index as u64 + 1,
            fence: 1,
            record,
        })
        .collect()
}

fn renderer(
    fixture: &Fixture,
    prefix: &[WalRecord],
    tail: &[WalRecord],
    profile: MediaProfile,
) -> LiveMedia {
    let mut checkpoint = MediaCheckpoint::empty();
    for frame in frames(prefix) {
        checkpoint.fold(frame.seq, &frame.record).unwrap();
    }
    let checkpoint = MediaCheckpoint::decode(&checkpoint.encode()).unwrap();
    let watermark = prefix.len() as u64;
    let records = Arc::new(frames(&[prefix, tail].concat()));
    LiveMedia::from_checkpoint(
        fixture.store.clone(),
        checkpoint,
        None,
        Some(profile),
        Box::new(move |after| {
            assert!(
                after >= watermark,
                "a seeded render must not read its WAL prefix"
            );
            Ok(records
                .iter()
                .filter(|frame| frame.seq > after)
                .cloned()
                .collect())
        }),
    )
    .unwrap()
}

#[test]
fn live_media_sparse_view_seed_keeps_captured_payloads_without_prefix_reads() {
    let fixture = Fixture::new();
    let frozen = frozen(WireFamilyId::AnthropicMessages);
    let mut records = history(&fixture, &frozen);
    let replacement = Message::new(
        Role::User,
        vec![
            ContentBlock::Text {
                text: "summary".into(),
            },
            ContentBlock::Media(fixture.media.clone()),
        ],
    );
    records.push(
        serde_json::from_value(json!({
            "type":"compaction_message", "provider":"compactor", "kind":"setup",
            "compaction_id":"media-view", "version":1, "range":{"from":0,"to":1},
            "replacement":[replacement], "sha256":"fixture"
        }))
        .unwrap(),
    );
    let prior = broca_core::replay::replay(&records).unwrap();
    let opts = broca_core::compaction::model_options(
        &CallOptions::new(prior.prompt),
        prior.compaction.state().unwrap(),
    )
    .unwrap();
    assert!(opts.prompt.iter().any(|message| message.ordinal.is_none()));
    let mut session = fixture.session(&[]);
    let profile = frozen.media.as_ref().unwrap();
    let aging = MediaAging::replay_wal(profile, &mut session, &records).unwrap();
    let mut bundle = prepare(&session, Some(profile), &opts.prompt, &aging).unwrap();
    assert_eq!(
        bundle.media.len(),
        2,
        "replacement and tool media are actually hydrated"
    );
    let expected = render_prepared(
        &*crate::registry::wire_family(frozen.family_id).unwrap(),
        &frozen,
        &opts,
        Some(&mut bundle),
        &|prompt| prompt.to_vec(),
    )
    .unwrap();
    for cut in 0..=records.len() {
        let live = renderer(&fixture, &records[..cut], &records[cut..], profile.clone());
        assert_eq!(
            live.render(&serde_json::to_value(&frozen).unwrap(), &opts, false, true)
                .unwrap(),
            expected,
            "sparse seed cut {cut}"
        );
    }
}

fn frozen(id: WireFamilyId) -> crate::wire::FrozenRenderConfig {
    let mut frozen = crate::families::frozen_config_for_family(id, "gemini-3-test");
    if id == WireFamilyId::GoogleCodeAssist {
        frozen.project_id = Some("fixture-project".into());
    }
    let quirk = match id {
        WireFamilyId::AnthropicMessages => Some(crate::media_profile::MediaQuirk::Anthropic),
        WireFamilyId::OpenAiResponses | WireFamilyId::OpenAiChat => {
            Some(crate::media_profile::MediaQuirk::Openai)
        }
        WireFamilyId::Gemini => Some(crate::media_profile::MediaQuirk::Google),
        _ => None,
    };
    frozen.media = Some(select_media_profile(
        id,
        quirk,
        "gemini-3-test",
        profile().modalities,
        Some(1_000_000),
    ));
    frozen
}

fn assert_media_free_bytes_and_zero_tail_reads(id: WireFamilyId) {
    let fixture = Fixture::new();
    let reads = Arc::new(Mutex::new(0));
    let count = reads.clone();
    let renderer = LiveMedia::new(
        fixture.store.clone(),
        Box::new(move |_| {
            *count.lock().unwrap() += 1;
            panic!("a media-free request must not request a WAL tail");
        }),
    );
    let frozen = frozen(id);
    let mut opts = CallOptions::new(vec![Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "cache this exact text".into(),
        }],
    )]);
    opts.step_context = Some(broca_protocol::StepContext {
        run_id: "media-free".into(),
        step_index: 1,
        session_address: None,
        credential_id: None,
        account_ref: None,
        max_output_tokens: None,
    });
    let family = crate::registry::wire_family(id).unwrap();
    let legacy = family.render(&frozen, &opts);
    assert_eq!(
        renderer
            .render(&serde_json::to_value(&frozen).unwrap(), &opts, true, true)
            .unwrap(),
        legacy,
        "{id:?}"
    );
    assert_eq!(*reads.lock().unwrap(), 0);
}

macro_rules! media_free_family {
    ($test:ident, $family:ident) => {
        #[test]
        fn $test() {
            assert_media_free_bytes_and_zero_tail_reads(WireFamilyId::$family);
        }
    };
}
media_free_family!(
    live_media_free_anthropic_bytes_and_zero_reads,
    AnthropicMessages
);
media_free_family!(
    live_media_free_bedrock_bytes_and_zero_reads,
    BedrockConverse
);
media_free_family!(live_media_free_openai_chat_bytes_and_zero_reads, OpenAiChat);
media_free_family!(
    live_media_free_openai_responses_bytes_and_zero_reads,
    OpenAiResponses
);
media_free_family!(
    live_media_free_chatgpt_bytes_and_zero_reads,
    OpenAiResponsesChatgpt
);
media_free_family!(live_media_free_gemini_bytes_and_zero_reads, Gemini);
media_free_family!(
    live_media_free_codeassist_bytes_and_zero_reads,
    GoogleCodeAssist
);

fn history(fixture: &Fixture, frozen: &crate::wire::FrozenRenderConfig) -> Vec<WalRecord> {
    let mut start = start(&[fixture.person()]);
    if let WalRecord::RunStarted { config, .. } = &mut start {
        config.frozen_render_config = serde_json::to_value(frozen).unwrap();
    }
    let mut records = vec![start, step(1)];
    for value in [
        json!({"type":"model_step_finished","step_id":1,"usage":{},"assistant_message":{"message_id":"m","content":[{"type":"tool_call","tool_call_id":"call","tool_name":"capture","input":{}}]}}),
        json!({"type":"tool_batch_started","step_id":1,"batch_id":"batch","planned_calls":["call"]}),
    ] {
        records.push(serde_json::from_value(value).unwrap());
    }
    records.push(tool_record(&fixture.tool("call")));
    records.push(
        serde_json::from_value(json!({"type":"tool_batch_finished","batch_id":"batch"})).unwrap(),
    );
    records.push(serde_json::from_value(json!({"type":"turn_finished","step_id":1})).unwrap());
    records.push(step(2));
    records
}

#[test]
fn live_media_seed_plus_tail_equals_full_replay_at_every_cut_and_profile_change() {
    let fixture = Fixture::new();
    let mut frozen = frozen(WireFamilyId::AnthropicMessages);
    let profile = frozen.media.as_mut().unwrap();
    profile.aging.byte_trigger = 1;
    profile.aging.byte_target = 0;
    profile.aging.token_trigger = u64::MAX;
    profile.aging.token_target = u64::MAX;
    let mut records = history(&fixture, &frozen);
    records.extend([
        serde_json::from_value(json!({"type":"model_step_finished","step_id":2,"usage":{},"assistant_message":{"message_id":"done","content":[{"type":"text","text":"done"}]}})).unwrap(),
        serde_json::from_value(json!({"type":"turn_finished","step_id":2})).unwrap(),
        serde_json::from_value(json!({"type":"run_finished","reason":"completed","usage":{}})).unwrap(),
    ]);
    let aged_opts = CallOptions::new(broca_core::replay::replay(&records).unwrap().prompt);
    let aged = renderer(&fixture, &[], &records, frozen.media.clone().unwrap())
        .render(
            &serde_json::to_value(&frozen).unwrap(),
            &aged_opts,
            false,
            true,
        )
        .unwrap();
    assert!(String::from_utf8(aged).unwrap().contains("reason=aged"));
    let next_start = records.len();
    records.extend([start(&[]), step(1)]);
    let mut next_profile = frozen.media.clone().unwrap();
    next_profile.aging.byte_trigger = u64::MAX;
    if let WalRecord::RunStarted { config, run_id, .. } = &mut records[next_start] {
        *run_id = broca_protocol::RunId("next".into());
        let mut next = frozen.clone();
        next.media = Some(next_profile.clone());
        config.frozen_render_config = serde_json::to_value(&next).unwrap();
    }
    frozen.media = Some(next_profile.clone());
    let prompt = broca_core::replay::replay(&records).unwrap().prompt;
    let opts = CallOptions::new(prompt.clone());
    let mut session = fixture.session(&[]);
    let aging = MediaAging::replay_wal(&next_profile, &mut session, &records).unwrap();
    let mut bundle = prepare(&session, Some(&next_profile), &prompt, &aging).unwrap();
    let expected = render_prepared(
        &*crate::registry::wire_family(frozen.family_id).unwrap(),
        &frozen,
        &opts,
        Some(&mut bundle),
        &|prompt| prompt.to_vec(),
    )
    .unwrap();
    for cut in 0..=records.len() {
        let live = renderer(
            &fixture,
            &records[..cut],
            &records[cut..],
            next_profile.clone(),
        );
        let actual = live
            .render(&serde_json::to_value(&frozen).unwrap(), &opts, false, true)
            .unwrap();
        assert_eq!(actual, expected, "seed cut {cut}");
        assert_eq!(
            live.render(&serde_json::to_value(&frozen).unwrap(), &opts, false, true)
                .unwrap(),
            expected,
            "retry after cut {cut}"
        );
    }
}

#[test]
fn live_media_person_and_tool_payloads_are_native_on_three_wire_shapes() {
    let fixture = Fixture::new();
    for (id, person_path, tool_path) in [
        (
            WireFamilyId::AnthropicMessages,
            "/messages/0/content/0/source/data",
            "/messages/2/content/0/content/0/source/data",
        ),
        (
            WireFamilyId::OpenAiResponses,
            "/input/0/content/0/image_url",
            "/input/2/output/0/image_url",
        ),
        (
            WireFamilyId::Gemini,
            "/contents/0/parts/0/inlineData/data",
            "/contents/2/parts/0/functionResponse/parts/0/inlineData/data",
        ),
    ] {
        let frozen = frozen(id);
        let records = history(&fixture, &frozen);
        let opts = CallOptions::new(broca_core::replay::replay(&records).unwrap().prompt);
        let live = renderer(&fixture, &[], &records, frozen.media.clone().unwrap());
        let body = live
            .render(&serde_json::to_value(&frozen).unwrap(), &opts, false, true)
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let encoded = STANDARD.encode(&fixture.bytes);
        assert!(
            body.pointer(person_path)
                .and_then(|value| value.as_str())
                .is_some_and(|value| value.ends_with(&encoded)),
            "{id:?}: {body}"
        );
        assert!(
            body.pointer(tool_path)
                .and_then(|value| value.as_str())
                .is_some_and(|value| value.ends_with(&encoded)),
            "{id:?}: {body}"
        );
    }
}

#[test]
fn live_media_missing_file_and_complete_request_cap_refuse_by_name() {
    let fixture = Fixture::new();
    let mut frozen = frozen(WireFamilyId::AnthropicMessages);
    let records = [start(&[fixture.person()]), step(1)];
    let opts = CallOptions::new(vec![fixture.person()]);
    let live = renderer(&fixture, &[], &records, frozen.media.clone().unwrap());
    frozen.media.as_mut().unwrap().request_cap_bytes = 100;
    let error = live
        .render(&serde_json::to_value(&frozen).unwrap(), &opts, false, true)
        .unwrap_err();
    assert_eq!(error.provider_code.as_deref(), Some("request_too_large"));
    frozen.media.as_mut().unwrap().request_cap_bytes = u64::MAX;
    std::fs::remove_file(
        fixture
            .root
            .join("media-durable/objects")
            .join(&fixture.media.sha256),
    )
    .unwrap();
    let error = live
        .render(&serde_json::to_value(&frozen).unwrap(), &opts, false, true)
        .unwrap_err();
    assert_eq!(error.provider_code.as_deref(), Some("media_object_missing"));
    assert!(error.message.contains(&fixture.media.sha256));
}

#[test]
fn live_media_checkpoint_is_content_free_and_invalid_versions_fall_back_by_name() {
    let fixture = Fixture::new();
    let frozen = frozen(WireFamilyId::AnthropicMessages);
    let mut records = history(&fixture, &frozen);
    if let WalRecord::RunStarted { input, .. } = &mut records[0] {
        Arc::make_mut(input)[0].content.push(ContentBlock::Text {
            text: "private transcript sentinel".into(),
        });
    }
    let mut checkpoint = broca_core::AdmissionCheckpointV1::from_frames(&frames(&records)).unwrap();
    let seed = serde_json::to_value(checkpoint.media.as_ref().unwrap()).unwrap();
    let bytes = serde_json::to_string(&seed).unwrap();
    assert!(!bytes.contains("private transcript sentinel"));
    assert!(!bytes.contains("content"));
    assert!(!bytes.contains(&STANDARD.encode(&fixture.bytes)));
    let mut corrupt = seed.clone();
    corrupt["index"]["boundary"] = json!(999);
    assert!(MediaCheckpoint::decode(&corrupt)
        .unwrap_err()
        .contains("metadata digest mismatch"));
    assert!(MediaCheckpoint::decode(&json!({"version":99}))
        .unwrap_err()
        .contains("media_seed_unknown_version"));
    let before = broca_core::media_request::seed_fallback_count();
    for value in [
        json!({"version":99}),
        json!({"version":1,"index":"damaged"}),
        corrupt,
    ] {
        checkpoint.media = Some(serde_json::from_value::<DurableMediaSeed>(value).unwrap());
        assert!(!checkpoint.media_seed_complete());
    }
    checkpoint.media = None;
    assert!(!checkpoint.media_seed_complete());
    assert!(broca_core::media_request::seed_fallback_count() >= before + 4);
}

async fn assert_transport_media<F: crate::wire::WireFamily + 'static>(family: F, id: WireFamilyId) {
    use broca_core::Provider;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let fixture = Fixture::new();
    let mut frozen = frozen(id);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    frozen.base_url = Some(format!("http://{}", listener.local_addr().unwrap()));
    let records = history(&fixture, &frozen);
    let opts = CallOptions::new(broca_core::replay::replay(&records).unwrap().prompt);
    let media = Arc::new(renderer(
        &fixture,
        &[],
        &records,
        frozen.media.clone().unwrap(),
    ));
    let provider =
        crate::transport::LiveProvider::new(family, frozen, crate::wire::AuthHandle::None);
    provider.set_media_request_renderer(Arc::new(move |config, opts, tags, commit| {
        media.render(config, opts, tags, commit)
    }));
    provider.defer_media_tags(false);
    let expected = provider.try_render_request(&opts).unwrap();
    let encoded = STANDARD.encode(&fixture.bytes);
    assert_eq!(
        String::from_utf8_lossy(&expected).matches(&encoded).count(),
        2
    );
    let mock = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let header_end = loop {
            let mut bytes = [0; 4096];
            let n = socket.read(&mut bytes).await.unwrap();
            assert_ne!(n, 0);
            request.extend_from_slice(&bytes[..n]);
            if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                break end + 4;
            }
        };
        let headers = String::from_utf8_lossy(&request[..header_end]);
        let length: usize = headers
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(|value| value.trim().parse().unwrap())
            })
            .unwrap();
        while request.len() < header_end + length {
            let mut bytes = [0; 4096];
            let n = socket.read(&mut bytes).await.unwrap();
            assert_ne!(n, 0);
            request.extend_from_slice(&bytes[..n]);
        }
        socket
            .write_all(
                b"HTTP/1.1 400 Bad Request\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
            )
            .await
            .unwrap();
        request[header_end..header_end + length].to_vec()
    });
    let mut stream = provider.step(broca_protocol::StepId(2), &opts).await;
    while stream.recv().await.is_some() {}
    let body = mock.await.unwrap();
    assert_eq!(
        body, expected,
        "{id:?}: sizing and transport must share exact bytes"
    );
}

#[tokio::test]
async fn live_transport_person_and_tool_media_reach_three_mock_wire_shapes() {
    assert_transport_media(
        crate::families::AnthropicMessages,
        WireFamilyId::AnthropicMessages,
    )
    .await;
    assert_transport_media(
        crate::families::OpenAiResponses,
        WireFamilyId::OpenAiResponses,
    )
    .await;
    assert_transport_media(crate::families::Gemini, WireFamilyId::Gemini).await;
}

#[test]
fn live_media_hydration_rechecks_type_pixels_and_unreadable_objects() {
    let fixture = Fixture::new();
    let frozen = frozen(WireFamilyId::AnthropicMessages);
    for wrong_type in [false, true] {
        let mut reference = fixture.media.clone();
        if wrong_type {
            reference.mime = "image/jpeg".into();
        } else {
            reference.width = Some(2);
        }
        let person = Message::new(Role::User, vec![ContentBlock::Media(reference)]);
        let records = [start(std::slice::from_ref(&person)), step(1)];
        let live = renderer(&fixture, &[], &records, frozen.media.clone().unwrap());
        let error = live
            .render(
                &serde_json::to_value(&frozen).unwrap(),
                &CallOptions::new(vec![person]),
                false,
                true,
            )
            .unwrap_err();
        assert_eq!(error.provider_code.as_deref(), Some("media_object_corrupt"));
        assert!(error.message.contains(&fixture.media.sha256));
    }
    let path = fixture
        .root
        .join("media-durable/objects")
        .join(&fixture.media.sha256);
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    let records = [start(&[fixture.person()]), step(1)];
    let live = renderer(&fixture, &[], &records, frozen.media.clone().unwrap());
    let error = live
        .render(
            &serde_json::to_value(&frozen).unwrap(),
            &CallOptions::new(vec![fixture.person()]),
            false,
            true,
        )
        .unwrap_err();
    assert!(error.message.contains(&fixture.media.sha256));
    assert!(!error.message.contains("reason=carrier"));
}
