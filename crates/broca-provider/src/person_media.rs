//! Validation of person media before it can become durable input.
//!
//! The aggregate byte gate is an encoded-payload bound, not a whole-request
//! measurement. Admission also uses the prepared renderer's measurement of the
//! would-be request, including history, tools and new media. Payload bytes alone
//! do not establish that the wire body fits.

use crate::media_profile::{base64_encoded_len, MediaCarrier, MediaProfile};
use broca_protocol::content::{ContentBlock, MediaKind, MediaMarkerReason, MediaRef, Message};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaRefusal {
    pub modality: &'static str,
    pub model: String,
    pub reason: MediaMarkerReason,
}

impl std::fmt::Display for MediaRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "media_unsupported: {} for model {}: {}",
            self.modality,
            self.model,
            self.reason.as_str()
        )
    }
}

pub fn modality(kind: MediaKind) -> &'static str {
    match kind {
        MediaKind::Image => "image",
        MediaKind::Document => "pdf",
        MediaKind::Audio => "audio",
        MediaKind::Video => "video",
    }
}

pub fn refusal(model: &str, kind: MediaKind, reason: MediaMarkerReason) -> MediaRefusal {
    MediaRefusal {
        modality: modality(kind),
        model: model.into(),
        reason,
    }
}

/// False and unknown catalog facts both fail closed. A missing frozen profile is
/// an older text-only admission, not permission to consult today's catalog.
pub fn check_item(
    profile: Option<&MediaProfile>,
    model: &str,
    media: &MediaRef,
) -> Result<(), MediaRefusal> {
    let fail = |reason| refusal(model, media.kind, reason);
    let Some(profile) = profile.filter(|profile| profile.accepts_modality(media.kind)) else {
        return Err(fail(MediaMarkerReason::Modality));
    };
    let kind = profile.kind(media.kind);
    if kind.user_message == MediaCarrier::Marker {
        return Err(fail(MediaMarkerReason::Carrier));
    }
    if !accepts_mime(profile, media) {
        return Err(fail(MediaMarkerReason::Mime));
    }
    if kind
        .max_item_bytes
        .is_some_and(|limit| !limit.admits(media.byte_size))
    {
        return Err(fail(MediaMarkerReason::TooLarge));
    }
    Ok(())
}

pub fn references(input: &[Message]) -> impl Iterator<Item = &MediaRef> {
    input
        .iter()
        .flat_map(|message| message.content.iter())
        .filter_map(|block| match block {
            ContentBlock::Media(media) => Some(media),
            _ => None,
        })
}

/// Check the protected inputs' encoded media payload bound and count/dimension limits.
/// The caller supplies every still-protected input under the lock that appends
/// the new input. Text, tools and wire-envelope bytes are not measured here.
pub fn check_protected_payload<'a>(
    profile: Option<&MediaProfile>,
    model: &str,
    inputs: impl IntoIterator<Item = &'a Message>,
) -> Result<(), MediaRefusal> {
    let messages: Vec<_> = inputs.into_iter().collect();
    let media: Vec<_> = messages
        .iter()
        .flat_map(|message| message.content.iter())
        .filter_map(|block| match block {
            ContentBlock::Media(media) => Some(media),
            _ => None,
        })
        .collect();
    let Some(first) = media.first() else {
        return Ok(());
    };
    let Some(profile) = profile else {
        return Err(refusal(model, first.kind, MediaMarkerReason::Modality));
    };
    let mut counts = [0u32; 4];
    let mut encoded = 0u64;
    for message in messages {
        for block in &message.content {
            if let ContentBlock::Media(media) = block {
                let index = match media.kind {
                    MediaKind::Image => 0,
                    MediaKind::Document => 1,
                    MediaKind::Audio => 2,
                    MediaKind::Video => 3,
                };
                counts[index] = counts[index].saturating_add(1);
                encoded = encoded.saturating_add(base64_encoded_len(media.byte_size));
                if profile
                    .kind(media.kind)
                    .max_count
                    .is_some_and(|max| counts[index] > max)
                {
                    return Err(refusal(model, media.kind, MediaMarkerReason::RequestCap));
                }
            }
        }
    }
    if encoded > profile.request_cap_bytes
        || profile
            .max_image_side(counts[0], counts[1])
            .is_some_and(|max| {
                media.iter().any(|media| {
                    media.kind == MediaKind::Image
                        && (media.width.unwrap_or(0) > max || media.height.unwrap_or(0) > max)
                })
            })
    {
        return Err(refusal(model, first.kind, MediaMarkerReason::RequestCap));
    }
    Ok(())
}

pub(crate) fn accepts_mime(profile: &MediaProfile, media: &MediaRef) -> bool {
    use crate::media_profile::MediaProfileId::*;
    let mime = media.mime.as_str();
    match media.kind {
        MediaKind::Image => match profile.profile {
            Xai => matches!(mime, "image/png" | "image/jpeg"),
            Gemini | Gemini3 | CodeAssistGemini | CodeAssistGemini3 => {
                matches!(mime, "image/png" | "image/jpeg" | "image/webp")
            }
            _ => matches!(
                mime,
                "image/png" | "image/jpeg" | "image/webp" | "image/gif"
            ),
        },
        MediaKind::Document => mime == "application/pdf",
        MediaKind::Audio => match profile.profile {
            OpenaiChat | MetaChat | MetaResponses => matches!(mime, "audio/wav" | "audio/mpeg"),
            Openrouter => mime == "audio/wav",
            Bedrock => matches!(
                mime,
                "audio/wav" | "audio/mpeg" | "audio/aac" | "audio/ogg" | "audio/flac" | "audio/mp4"
            ),
            Gemini | Gemini3 | CodeAssistGemini | CodeAssistGemini3 => matches!(
                mime,
                "audio/wav"
                    | "audio/mp3"
                    | "audio/mpeg"
                    | "audio/aiff"
                    | "audio/aac"
                    | "audio/ogg"
                    | "audio/flac"
                    | "audio/m4a"
                    | "audio/mp4"
            ),
            _ => false,
        },
        MediaKind::Video => match profile.profile {
            Openrouter | MetaChat | MetaResponses => mime == "video/mp4",
            Bedrock => matches!(
                mime,
                "video/mp4"
                    | "video/quicktime"
                    | "video/webm"
                    | "video/x-flv"
                    | "video/mpeg"
                    | "video/x-ms-wmv"
                    | "video/3gpp"
                    | "video/x-matroska"
            ),
            Kimi | Gemini | Gemini3 | CodeAssistGemini | CodeAssistGemini3 => matches!(
                mime,
                "video/mp4"
                    | "video/mpeg"
                    | "video/mov"
                    | "video/quicktime"
                    | "video/avi"
                    | "video/x-msvideo"
                    | "video/x-flv"
                    | "video/mpg"
                    | "video/webm"
                    | "video/x-ms-wmv"
                    | "video/3gpp"
            ),
            _ => false,
        },
    }
}

#[cfg(test)]
mod tests;
