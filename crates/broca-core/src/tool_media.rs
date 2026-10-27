//! Capture tool bytes independently of the model's current media profile.

use crate::media_store::MediaManifest;
use crate::media_uploads::Uploads;
use broca_protocol::{
    BatchId, BindIdentity, MediaMarkerReason, MediaRef, ToolCallId, ToolOutput, ToolOutputPart,
    ToolResult,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// A session-bound capture handle. Opening the manifest is deferred until a tool
/// actually returns media, so text-only dispatch does not create storage entries.
#[derive(Clone)]
pub struct ToolMediaCapture(Arc<Mutex<CaptureState>>);

#[derive(Debug)]
pub struct CaptureFailure {
    pub reason: MediaMarkerReason,
    /// Detected metadata is available after validation, even if storage failed.
    pub media: Option<MediaRef>,
}

struct CaptureState {
    uploads: Uploads,
    identity: BindIdentity,
    manifest: Option<MediaManifest>,
    captured: HashMap<(BatchId, ToolCallId), Vec<MediaRef>>,
}

impl std::fmt::Debug for ToolMediaCapture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolMediaCapture").finish_non_exhaustive()
    }
}

impl ToolMediaCapture {
    pub fn new(uploads: Uploads, identity: BindIdentity) -> Self {
        Self(Arc::new(Mutex::new(CaptureState {
            uploads,
            identity,
            manifest: None,
            captured: HashMap::new(),
        })))
    }

    /// Complete publication and manifest barriers before exposing a reference to
    /// dispatch's fence or WAL. Storage failures describe an executed tool, not an
    /// unsent call, and therefore become marker parts at the caller.
    pub async fn capture(
        &self,
        batch_id: &BatchId,
        call_id: &ToolCallId,
        bytes: Vec<u8>,
        declared_mime: String,
    ) -> Result<MediaRef, CaptureFailure> {
        let state = self.0.clone();
        let key = (batch_id.clone(), call_id.clone());
        tokio::task::spawn_blocking(move || {
            let reference = crate::media_validation::validate_media(&bytes, &declared_mime)
                .map_err(|reason| CaptureFailure {
                    reason,
                    media: None,
                })?;
            let store_failed = || CaptureFailure {
                reason: MediaMarkerReason::StoreFailed,
                media: Some(reference.clone()),
            };
            let mut state = state.lock().map_err(|_| store_failed())?;
            if state.manifest.is_none() {
                state.manifest = Some(
                    state
                        .uploads
                        .session_media_store(&state.identity)
                        .and_then(|store| store.session(&state.identity))
                        .map_err(|_| store_failed())?,
                );
            }
            state
                .manifest
                .as_mut()
                .expect("manifest opened above")
                .register_and_append(&reference, &bytes, || Ok(()))
                .map_err(|_| store_failed())?;
            state
                .captured
                .entry(key)
                .or_default()
                .push(reference.clone());
            Ok(reference)
        })
        .await
        .unwrap_or(Err(CaptureFailure {
            reason: MediaMarkerReason::StoreFailed,
            media: None,
        }))
    }

    pub fn captured_for(&self, batch_id: &BatchId, call_id: &ToolCallId) -> Vec<MediaRef> {
        self.0
            .lock()
            .ok()
            .and_then(|state| {
                state
                    .captured
                    .get(&(batch_id.clone(), call_id.clone()))
                    .cloned()
            })
            .unwrap_or_default()
    }
}

/// Only exact references produced for this call may survive into hook metadata
/// and the WAL. Store existence, or a matching digest with altered metadata, is
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
                        ),
                    };
                }
            }
        }
    }
}
