//! Media parts shared by the two OpenAI grammars. Payloads remain request-local.

use broca_protocol::{MediaKind, MediaMarkerReason, MediaRef};
use serde_json::{json, Value};

use crate::{media_profile::MediaCarrier, media_render::MediaRender};

#[derive(Clone, Copy)]
pub(super) enum Grammar {
    Chat,
    Responses,
}

impl Grammar {
    pub fn text(self, text: &str) -> Value {
        let kind = match self {
            Self::Chat => "text",
            Self::Responses => "input_text",
        };
        json!({"type":kind, "text":text})
    }

    pub fn marker(self, reference: &MediaRef, reason: MediaMarkerReason) -> Value {
        self.text(&broca_protocol::content::media_not_shown_marker(
            reference, reason,
        ))
    }

    pub fn block(
        self,
        reference: &MediaRef,
        media: Option<&MediaRender<'_>>,
        tool: bool,
        carrier: MediaCarrier,
    ) -> Result<Value, MediaMarkerReason> {
        let media = media.ok_or(MediaMarkerReason::Carrier)?;
        let base64 = media.for_carrier(reference, tool, carrier)?;
        if carrier == MediaCarrier::FollowUpUserMessage
            && !media
                .frozen
                .media
                .as_ref()
                .is_some_and(|profile| profile.live_gated.follow_up_user_message)
        {
            return Err(MediaMarkerReason::Carrier);
        }
        let url = format!("data:{};base64,{base64}", reference.mime);
        match (self, reference.kind) {
            (Self::Chat, MediaKind::Image) => {
                Ok(json!({"type":"image_url", "image_url":{"url":url}}))
            }
            (Self::Chat, MediaKind::Document) => Ok(json!({"type":"file", "file":{
                "filename":"document.pdf", "file_data":url
            }})),
            (Self::Chat, MediaKind::Audio) => {
                let format = match reference.mime.as_str() {
                    "audio/wav" => "wav",
                    "audio/mpeg" => "mp3",
                    _ => return Err(MediaMarkerReason::Mime),
                };
                Ok(json!({"type":"input_audio", "input_audio":{"data":base64, "format":format}}))
            }
            (Self::Chat, MediaKind::Video) => {
                Ok(json!({"type":"video_url", "video_url":{"url":url}}))
            }
            (Self::Responses, MediaKind::Image) => {
                Ok(json!({"type":"input_image", "image_url":url}))
            }
            (Self::Responses, MediaKind::Document) => {
                Ok(json!({"type":"input_file", "filename":"document.pdf", "file_data":url}))
            }
            (Self::Responses, MediaKind::Audio | MediaKind::Video) => {
                Err(MediaMarkerReason::Carrier)
            }
        }
    }
}
