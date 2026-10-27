use super::*;
use crate::{
    media_profile::{select_media_profile, ByteLimit},
    spec::{InputModalities, WireFamilyId},
};
use broca_protocol::content::Role;

fn profile(family: WireFamilyId, modalities: &[&str]) -> MediaProfile {
    select_media_profile(
        family,
        Some(crate::media_profile::MediaQuirk::Anthropic),
        "gemini-3-pro",
        InputModalities::from_value(&serde_json::json!(modalities)),
        Some(200_000),
    )
}
fn reference(kind: MediaKind, mime: &str) -> MediaRef {
    MediaRef {
        kind,
        mime: mime.into(),
        byte_size: 8,
        sha256: "a".repeat(64),
        width: (kind == MediaKind::Image).then_some(100),
        height: (kind == MediaKind::Image).then_some(100),
    }
}
fn input(media: MediaRef) -> Message {
    Message::new(Role::User, vec![ContentBlock::Media(media)])
}

#[test]
fn pdf_without_catalog_modality_names_pdf_model_and_reason() {
    let profile = profile(WireFamilyId::AnthropicMessages, &["text", "image"]);
    let error = check_item(
        Some(&profile),
        "anthropic/text-only",
        &reference(MediaKind::Document, "application/pdf"),
    )
    .unwrap_err();
    assert_eq!(error.modality, "pdf");
    assert_eq!(error.model, "anthropic/text-only");
    assert_eq!(error.reason, MediaMarkerReason::Modality);
    assert_eq!(
        error.to_string(),
        "media_unsupported: pdf for model anthropic/text-only: modality"
    );
}

#[test]
fn chatgpt_audio_without_live_flag_is_carrier_refusal() {
    let profile = profile(WireFamilyId::OpenAiResponsesChatgpt, &["text", "audio"]);
    let error = check_item(
        Some(&profile),
        "openai/audio-model",
        &reference(MediaKind::Audio, "audio/wav"),
    )
    .unwrap_err();
    assert_eq!(error.reason, MediaMarkerReason::Carrier);
    assert_eq!(error.modality, "audio");
    assert_eq!(error.model, "openai/audio-model");
}

#[test]
fn item_gate_accepts_detected_mime_and_refuses_profile_size_and_mime() {
    let mut profile = profile(WireFamilyId::AnthropicMessages, &["text", "image"]);
    let image = reference(MediaKind::Image, "image/png");
    check_item(Some(&profile), "m", &image).unwrap();
    profile.image.max_item_bytes = Some(ByteLimit::Decoded(7));
    assert_eq!(
        check_item(Some(&profile), "m", &image).unwrap_err().reason,
        MediaMarkerReason::TooLarge
    );
    profile.image.max_item_bytes = None;
    let mut bad = image;
    bad.mime = "image/bmp".into();
    assert_eq!(
        check_item(Some(&profile), "m", &bad).unwrap_err().reason,
        MediaMarkerReason::Mime
    );
    assert_eq!(
        check_item(None, "m", &bad).unwrap_err().reason,
        MediaMarkerReason::Modality
    );
}

#[test]
fn request_cap_counts_every_protected_input_and_duplicate_reference() {
    let mut profile = profile(WireFamilyId::AnthropicMessages, &["text", "image"]);
    profile.image.max_count = Some(1);
    let first = input(reference(MediaKind::Image, "image/png"));
    let second = first.clone();
    check_protected_payload(Some(&profile), "m", [&first]).unwrap();
    check_protected_payload(Some(&profile), "m", [&second]).unwrap();
    assert_eq!(
        check_protected_payload(Some(&profile), "m", [&first, &second])
            .unwrap_err()
            .reason,
        MediaMarkerReason::RequestCap
    );
}

#[test]
fn protected_encoded_payload_bound_omits_text_and_envelope_bytes() {
    let mut profile = profile(WireFamilyId::AnthropicMessages, &["text", "image"]);
    let mut image = reference(MediaKind::Image, "image/png");
    image.byte_size = 768;
    let mut message = input(image.clone());
    message.content.insert(
        0,
        ContentBlock::Text {
            text: "x".repeat(2000),
        },
    );
    profile.request_cap_bytes = 1024;
    check_protected_payload(Some(&profile), "m", [&message]).unwrap();
    profile.request_cap_bytes = 1023;
    assert_eq!(
        check_protected_payload(Some(&profile), "m", [&message])
            .unwrap_err()
            .reason,
        MediaMarkerReason::RequestCap
    );
    profile.request_cap_bytes = 32_000_000;
    image.width = Some(2500);
    let message = input(image);
    check_protected_payload(Some(&profile), "m", std::iter::repeat_n(&message, 20)).unwrap();
    assert_eq!(
        check_protected_payload(Some(&profile), "m", std::iter::repeat_n(&message, 21))
            .unwrap_err()
            .reason,
        MediaMarkerReason::RequestCap
    );
}

#[test]
fn per_family_mime_gate_uses_detected_container_spellings() {
    use crate::media_profile::MediaQuirk;
    let google = select_media_profile(
        WireFamilyId::Gemini,
        Some(MediaQuirk::Google),
        "gemini-3-pro",
        InputModalities::from_value(&serde_json::json!(["image", "audio", "video"])),
        Some(200_000),
    );
    assert_eq!(
        check_item(
            Some(&google),
            "google/m",
            &reference(MediaKind::Image, "image/gif")
        )
        .unwrap_err()
        .reason,
        MediaMarkerReason::Mime
    );
    check_item(
        Some(&google),
        "google/m",
        &reference(MediaKind::Video, "video/x-ms-wmv"),
    )
    .unwrap();
    assert_eq!(
        check_item(
            Some(&google),
            "google/m",
            &reference(MediaKind::Video, "video/x-matroska")
        )
        .unwrap_err()
        .reason,
        MediaMarkerReason::Mime
    );
    let bedrock = select_media_profile(
        WireFamilyId::BedrockConverse,
        None,
        "nova",
        google.modalities,
        Some(200_000),
    );
    check_item(
        Some(&bedrock),
        "bedrock/m",
        &reference(MediaKind::Video, "video/x-matroska"),
    )
    .unwrap();
    assert_eq!(
        check_item(
            Some(&bedrock),
            "bedrock/m",
            &reference(MediaKind::Audio, "audio/aiff")
        )
        .unwrap_err()
        .reason,
        MediaMarkerReason::Mime
    );
    check_item(
        Some(&bedrock),
        "bedrock/m",
        &reference(MediaKind::Audio, "audio/mp4"),
    )
    .unwrap();
}
