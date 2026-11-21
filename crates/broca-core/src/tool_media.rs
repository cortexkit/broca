//! Capture tool bytes independently of the model's current media profile.

use broca_protocol::{
    BatchId, MediaMarkerReason, MediaRef, ToolCallId, ToolOutput, ToolOutputPart, ToolResult,
};

#[derive(Debug)]
pub struct CaptureFailure {
    pub reason: MediaMarkerReason,
    /// Detected metadata is available after validation, even if storage failed.
    pub media: Option<MediaRef>,
}

/// Where a tool's media goes when a result arrives, and which references were captured for each
/// call. The tool plane captures through it, and captures media at all only when a batch carries
/// one. The run loop checks a completed result against `captured_for` before hooks or the log see
/// it ([`enforce_provenance`]). The run loop's `PendingMedia` implements it.
#[async_trait::async_trait]
pub trait MediaCapture: Send + Sync + std::fmt::Debug {
    /// Validates `bytes` and keeps them for the call until its result is stored with them. A
    /// failure describes a tool that ran, so the caller shows it as a marker part, never as an
    /// unsent call.
    async fn capture(
        &self,
        batch_id: &BatchId,
        call_id: &ToolCallId,
        bytes: Vec<u8>,
        declared_mime: String,
    ) -> Result<MediaRef, CaptureFailure>;
    /// The exact references captured for this call, in capture order.
    fn captured_for(&self, batch_id: &BatchId, call_id: &ToolCallId) -> Vec<MediaRef>;
}

/// Only exact references produced for this call may survive into hook metadata
/// and the session log. Store existence, or a matching digest with altered metadata, is
/// not evidence that a tool captured those bytes.
pub fn enforce_provenance(result: &mut ToolResult, captured: &[MediaRef]) {
    if let ToolOutput::Parts { parts } = &mut result.output {
        for part in parts {
            if let ToolOutputPart::Media { media } = part {
                if !captured.contains(media) {
                    *part = ToolOutputPart::Text {
                        text: broca_protocol::content::media_not_shown_marker(
                            media,
                            MediaMarkerReason::Provenance,
                        )
                        .into(),
                    };
                }
            }
        }
    }
}
