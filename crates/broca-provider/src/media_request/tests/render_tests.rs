use super::*;
use crate::{
    families::{frozen_config_for_family, AnthropicMessages, BedrockConverse},
    wire::WireFamily,
};
use broca_protocol::{
    tags::{number_message, render_tags_into, ComputedRules, TagRendering},
    CallOptions,
};

fn families() -> [(WireFamilyId, &'static dyn WireFamily); 2] {
    [
        (WireFamilyId::AnthropicMessages, &AnthropicMessages),
        (WireFamilyId::BedrockConverse, &BedrockConverse),
    ]
}

#[test]
fn native_requests_measure_complete_bytes_and_fail_closed_at_the_cap() {
    let fixture = Fixture::new();
    let mut person = fixture.person();
    person.content.insert(
        0,
        ContentBlock::Text {
            text: "large envelope".repeat(100),
        },
    );
    let prompt = vec![person];
    let session = fixture.session(&[start(&prompt), step(1)]);
    for (id, family) in families() {
        let mut frozen = frozen_config_for_family(id, "model");
        let mut profile = profile();
        if id == WireFamilyId::BedrockConverse {
            profile = select_media_profile(id, None, "model", profile.modalities, Some(1_000_000));
        }
        frozen.media = Some(profile.clone());
        let (mut bundle, _) = planned(&session, &profile, &prompt, &mut MediaAging::default());
        let opts = CallOptions::new(prompt.clone());
        let bytes = render_prepared(family, &frozen, &opts, Some(&mut bundle), &|prompt| {
            prompt.to_vec()
        })
        .unwrap();
        assert!(bytes.len() as u64 > bundle.encoded_media_bytes());
        frozen.media.as_mut().unwrap().request_cap_bytes = bytes.len() as u64;
        assert_eq!(
            render_prepared(family, &frozen, &opts, Some(&mut bundle), &|prompt| prompt
                .to_vec())
            .unwrap(),
            bytes
        );
        frozen.media.as_mut().unwrap().request_cap_bytes -= 1;
        assert_eq!(
            render_prepared(family, &frozen, &opts, Some(&mut bundle), &|prompt| prompt
                .to_vec())
            .unwrap_err()
            .code(),
            "request_too_large"
        );
    }
}

#[test]
fn tagged_text_image_text_stays_numbered_once_across_retry_and_replay() {
    let fixture = Fixture::new();
    let mut tool = fixture.tool("call");
    let ContentBlock::ToolResult(result) = &mut tool.content[0] else {
        unreachable!()
    };
    let ToolOutput::Parts { parts } = &mut result.output else {
        unreachable!()
    };
    parts.insert(
        0,
        ToolOutputPart::Text {
            text: "before".into(),
        },
    );
    parts.push(ToolOutputPart::Text {
        text: "after".into(),
    });
    number_message(
        &mut tool,
        ComputedRules {
            render: true,
            clean: false,
        },
        &mut 6,
    )
    .unwrap();
    let records = [start(&[]), step(1), tool_record(&tool), step(2)];
    let prompt = vec![tool];
    for (id, family) in families() {
        let mut frozen = frozen_config_for_family(id, "model");
        let profile = select_media_profile(
            id,
            Some(crate::media_profile::MediaQuirk::Anthropic),
            "model",
            profile().modalities,
            Some(1_000_000),
        );
        frozen.media = Some(profile.clone());
        let render_tags = |prompt: &[Message]| {
            let mut prompt = prompt.to_vec();
            render_tags_into(&mut prompt, TagRendering::default());
            prompt
        };
        let run = || {
            let session = fixture.session(&records);
            let (mut bundle, _) = planned(&session, &profile, &prompt, &mut MediaAging::default());
            let opts = CallOptions::new(prompt.clone());
            let first =
                render_prepared(family, &frozen, &opts, Some(&mut bundle), &render_tags).unwrap();
            assert_eq!(
                first,
                render_prepared(family, &frozen, &opts, Some(&mut bundle), &render_tags).unwrap()
            );
            let body: serde_json::Value = serde_json::from_slice(&first).unwrap();
            let result = &body["messages"][0]["content"][0];
            let content = if id == WireFamilyId::AnthropicMessages {
                &result["content"]
            } else {
                &result["toolResult"]["content"]
            };
            assert_eq!(content[0]["text"], "§7§ before");
            assert_eq!(content[2]["text"], "after");
            assert_eq!(
                serde_json::to_string(&body).unwrap().matches("§7§").count(),
                1
            );
            assert!(serde_json::to_string(&content[1])
                .unwrap()
                .contains(&base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    &fixture.bytes
                )));
            // Limit marking replaces only the render copy; the stored tag and later text stay put.
            bundle.mark(0, MediaMarkerReason::Aged);
            let aged =
                render_prepared(family, &frozen, &opts, Some(&mut bundle), &render_tags).unwrap();
            let aged = String::from_utf8(aged).unwrap();
            assert_eq!(aged.matches("§7§").count(), 1);
            assert!(aged.contains("reason=aged"));
            first
        };
        assert_eq!(run(), run());
    }
}

#[test]
fn image_only_tag_label_does_not_detach_the_prepared_payload() {
    let fixture = Fixture::new();
    let mut tool = fixture.tool("call");
    number_message(
        &mut tool,
        ComputedRules {
            render: true,
            clean: false,
        },
        &mut 6,
    )
    .unwrap();
    let prompt = vec![tool];
    let session = fixture.session(&[start(&[]), step(1), tool_record(&prompt[0]), step(2)]);
    for (id, family) in families() {
        let mut frozen = frozen_config_for_family(id, "model");
        let profile = select_media_profile(
            id,
            Some(crate::media_profile::MediaQuirk::Anthropic),
            "model",
            profile().modalities,
            Some(1_000_000),
        );
        frozen.media = Some(profile.clone());
        let (mut bundle, _) = planned(&session, &profile, &prompt, &mut MediaAging::default());
        let bytes = render_prepared(
            family,
            &frozen,
            &CallOptions::new(prompt.clone()),
            Some(&mut bundle),
            &|prompt| {
                let mut prompt = prompt.to_vec();
                render_tags_into(&mut prompt, TagRendering::default());
                prompt
            },
        )
        .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let result = &body["messages"][0]["content"][0];
        let content = if id == WireFamilyId::AnthropicMessages {
            &result["content"]
        } else {
            &result["toolResult"]["content"]
        };
        assert_eq!(content.as_array().unwrap().len(), 2);
        assert!(content[0]["text"].as_str().unwrap().contains('7'));
        assert!(serde_json::to_string(&content[1])
            .unwrap()
            .contains(&base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                &fixture.bytes
            )));
    }
}

#[test]
fn native_media_provenance_uses_the_same_bytes_and_durable_source_for_every_family() {
    let fixture = Fixture::new();
    let mut person = fixture.person();
    person.ordinal = Some(0);
    let prompt = vec![person];
    let session = fixture.session(&[start(&prompt), step(1)]);
    for id in [
        WireFamilyId::AnthropicMessages,
        WireFamilyId::BedrockConverse,
        WireFamilyId::OpenAiChat,
        WireFamilyId::OpenAiResponses,
        WireFamilyId::OpenAiResponsesChatgpt,
        WireFamilyId::Gemini,
        WireFamilyId::GoogleCodeAssist,
    ] {
        let family = crate::registry::wire_family(id).unwrap();
        let mut frozen = frozen_config_for_family(id, "model");
        frozen.project_id = Some("synthetic-project".into());
        let profile =
            select_media_profile(id, None, "model", profile().modalities, Some(1_000_000));
        frozen.media = Some(profile.clone());
        let (mut bundle, _) = planned(&session, &profile, &prompt, &mut MediaAging::default());
        let mut opts = CallOptions::new(prompt.clone());
        opts.step_context = Some(broca_protocol::StepContext {
            run_id: "media-provenance".into(),
            step_index: 1,
            max_output_tokens: None,
            session_address: None,
            credential_id: None,
            account_ref: None,
        });
        let plain =
            render_prepared(&*family, &frozen, &opts, Some(&mut bundle), &|p| p.to_vec()).unwrap();
        let mut provenance = broca_protocol::render_provenance::RenderProvenance::default();
        let traced = render_prepared_with_provenance(
            &*family,
            &frozen,
            &opts,
            Some(&mut bundle),
            &|p| p.to_vec(),
            Some(&mut provenance),
        )
        .unwrap();
        assert_eq!(plain, traced, "{id:?}");
        assert!(
            (0..provenance.message_count())
                .any(|index| provenance.message_ordinal(index, Some(0)) == Some(0)),
            "{id:?}: source ordinal missing"
        );
    }
}
