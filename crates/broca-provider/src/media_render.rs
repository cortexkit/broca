//! Explicit, already-hydrated media inputs for the pure wire renderers.
//!
//! Payloads are per request, never part of the frozen configuration. The live preparation
//! layer verifies the objects and applies provenance, aging and request limits before sending.

use broca_protocol::{MediaMarkerReason, MediaRef};

use crate::{media_profile::MediaCarrier, wire::FrozenRenderConfig};

/// A borrowed payload from a prepared bundle. Matching by the complete reference rather than
/// a block index lets tag rendering insert a label without detaching the image from its bytes.
pub struct InlineMedia<'a> {
    pub reference: &'a MediaRef,
    pub base64: &'a str,
    pub tool: bool,
}

pub(crate) struct MediaRender<'a> {
    pub frozen: &'a FrozenRenderConfig,
    pub payloads: &'a [InlineMedia<'a>],
}

impl MediaRender<'_> {
    pub fn native(&self, media: &MediaRef, tool: bool) -> Result<&str, MediaMarkerReason> {
        self.for_carrier(media, tool, MediaCarrier::Native)
    }

    pub fn for_carrier(
        &self,
        media: &MediaRef,
        tool: bool,
        expected: MediaCarrier,
    ) -> Result<&str, MediaMarkerReason> {
        let profile = self
            .frozen
            .media
            .as_ref()
            .ok_or(MediaMarkerReason::Carrier)?;
        if !profile.accepts_modality(media.kind) {
            return Err(MediaMarkerReason::Modality);
        }
        let kind = profile.kind(media.kind);
        let carrier = if tool {
            kind.tool_result
        } else {
            kind.user_message
        };
        if carrier != expected || carrier == MediaCarrier::Marker {
            return Err(MediaMarkerReason::Carrier);
        }
        if !crate::person_media::accepts_mime(profile, media) {
            return Err(MediaMarkerReason::Mime);
        }
        if kind
            .max_item_bytes
            .is_some_and(|limit| !limit.admits(media.byte_size))
        {
            return Err(MediaMarkerReason::TooLarge);
        }
        self.payloads
            .iter()
            .find(|payload| payload.reference == media && payload.tool == tool)
            .map(|payload| payload.base64)
            .ok_or(MediaMarkerReason::Carrier)
    }
}
