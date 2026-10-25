//! Read-only staging and the publication boundary for person attachments.
//!
//! Admission currently checks an encoded-payload bound for protected media.
//! Before attachments are enabled in release builds, the byte check must use a
//! shared measurement of the real rendered would-be request, including history,
//! tools and new media. This gate must call that measurement before publication;
//! neither falsely refusing a fitting bundle nor admitting an oversized wire body
//! is acceptable for the shipping attachment contract.

use broca_core::{media_uploads::Uploads, RunError};
use broca_protocol::{
    content::{ContentBlock, MediaKind, Message, Role},
    request::SubmissionRequest,
    BindIdentity,
};
use broca_provider::{
    media_profile::MediaProfile,
    person_media::{check_item, check_protected_payload},
};

pub type StagedMedia = broca_protocol::request::StagedUpload;

fn model(request: &SubmissionRequest) -> String {
    format!(
        "{}/{}",
        request.model.provider_module_id, request.model.model_id
    )
}

pub fn now_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// The committed bytes are held until publication. No send-side read extends an
/// upload's expiry or creates an object or manifest.
pub fn stage(
    uploads: Option<&Uploads>,
    identity: &BindIdentity,
    request: &mut SubmissionRequest,
) -> Result<Vec<StagedMedia>, RunError> {
    if request.content.is_empty() {
        return Ok(Vec::new());
    }
    if !broca_protocol::attachments_enabled() {
        return Err(RunError::Config(format!(
            "upload {} cannot be sent: attachment admission is unavailable",
            request.content[0].upload_id()
        )));
    }
    // A queued request already owns durable references; its upload may have expired.
    if broca_provider::person_media::references(&request.input).count() == request.content.len() {
        return Ok(Vec::new());
    }
    let held = request.staged_uploads.clone();
    let staged = if let Some(held) = held {
        if held.identity != *identity {
            return Err(RunError::Config(
                "staged uploads belong to another session".into(),
            ));
        }
        held.items.clone()
    } else {
        let uploads =
            uploads.ok_or_else(|| RunError::Config("upload staging is unavailable".into()))?;
        let mut staged = Vec::new();
        for part in &request.content {
            let id = part.upload_id();
            let (mime, bytes) = uploads
                .read_committed(identity, id, now_seconds())
                .map_err(|error| RunError::Config(error.to_string()))?;
            let reference =
                broca_core::media_validation::validate_media(&bytes, &mime).map_err(|reason| {
                    let kind = if mime == "application/pdf" {
                        MediaKind::Document
                    } else if mime.starts_with("audio/") {
                        MediaKind::Audio
                    } else if mime.starts_with("video/") {
                        MediaKind::Video
                    } else {
                        MediaKind::Image
                    };
                    RunError::MediaUnsupported(
                        broca_provider::person_media::refusal(&model(request), kind, reason)
                            .to_string(),
                    )
                })?;
            staged.push(StagedMedia {
                reference,
                bytes: std::sync::Arc::new(bytes),
            });
        }
        request.staged_uploads = Some(std::sync::Arc::new(
            broca_protocol::request::StagedUploads {
                identity: identity.clone(),
                items: staged.clone(),
                profile: None,
            },
        ));
        staged
    };
    let user = request
        .input
        .iter_mut()
        .rev()
        .find(|message| message.role == Role::User)
        .ok_or_else(|| RunError::Config("attachments require a user message".into()))?;
    user.content.extend(
        staged
            .iter()
            .map(|media| ContentBlock::Media(media.reference.clone())),
    );
    Ok(staged)
}

pub fn check(
    profile: Option<&MediaProfile>,
    model: &str,
    input: &[Message],
) -> Result<(), RunError> {
    for media in broca_provider::person_media::references(input) {
        check_item(profile, model, media)
            .map_err(|refusal| RunError::MediaUnsupported(refusal.to_string()))?;
    }
    check_protected_payload(profile, model, input.iter())
        .map_err(|refusal| RunError::MediaUnsupported(refusal.to_string()))
}

pub fn frozen(config: &broca_wal::RunConfig) -> Option<Box<MediaProfile>> {
    serde_json::from_value::<broca_provider::wire::FrozenRenderConfig>(
        config.frozen_render_config.clone(),
    )
    .ok()
    .and_then(|config| config.media.map(Box::new))
}

/// All storage barriers complete before an inbox lock or a WAL append can name
/// these references. A refused steer can leave extra manifest entries until sweep.
pub fn publish(
    uploads: Option<&Uploads>,
    identity: &BindIdentity,
    staged: &[StagedMedia],
) -> Result<(), RunError> {
    if staged.is_empty() {
        return Ok(());
    }
    let uploads = uploads.ok_or_else(|| RunError::Wal("upload staging is unavailable".into()))?;
    let mut manifest = uploads
        .session_media_store(identity)
        .and_then(|store| store.session(identity))
        .map_err(|error| RunError::Wal(error.to_string()))?;
    for media in staged {
        manifest
            .register_and_append(&media.reference, &media.bytes, || Ok(()))
            .map_err(|error| RunError::Wal(error.to_string()))?;
    }
    Ok(())
}

pub(crate) fn outcome(error: RunError) -> crate::actor::SendOutcome {
    match error {
        RunError::MediaUnsupported(message) => crate::actor::SendOutcome::DeliveryRefused {
            code: "media_unsupported",
            field: "content",
            message,
        },
        error => crate::actor::SendOutcome::Error(error.to_string()),
    }
}

/// Recover the protection boundary from durable step identities, not provider sends.
/// Repeated attempts of the same step keep exactly the same protected inputs.
pub fn protection_state(
    records: &[broca_wal::WalRecord],
) -> (Vec<Message>, Vec<Message>, Option<broca_protocol::StepId>) {
    let mut pending = Vec::new();
    let mut current = Vec::new();
    let mut last_step = None;
    for frame in records {
        match frame {
            broca_wal::WalRecord::RunStarted { input, .. } => {
                pending = input.as_ref().clone();
                current.clear();
                last_step = None;
            }
            broca_wal::WalRecord::Steered { input, .. } => pending.extend(input.iter().cloned()),
            broca_wal::WalRecord::StepStarted { step_id, .. }
                if last_step.as_ref() != Some(step_id) =>
            {
                current = std::mem::take(&mut pending);
                last_step = Some(*step_id);
            }
            broca_wal::WalRecord::RunFinished { .. } => {
                current.clear();
                pending.clear();
            }
            _ => {}
        }
    }
    (current, pending, last_step)
}

pub fn protected_input(records: &[broca_wal::WalRecord]) -> Vec<Message> {
    let (mut current, pending, _) = protection_state(records);
    current.extend(pending);
    current
}

#[cfg(test)]
mod protection_tests {
    use super::*;
    use serde_json::json;
    fn steer(text: &str) -> broca_wal::WalRecord {
        serde_json::from_value(json!({"type":"steered","input":[{"role":"user","content":[{"type":"text","text":text}]}]})).unwrap()
    }
    #[test]
    fn durable_step_identity_preserves_retries_and_retires_only_the_previous_step() {
        let mut records = vec![
            steer("first"),
            broca_wal::WalRecord::StepStarted {
                step_id: broca_protocol::StepId(1),
            },
            steer("pending"),
            broca_wal::WalRecord::StepStarted {
                step_id: broca_protocol::StepId(1),
            },
        ];
        assert_eq!(protected_input(&records).len(), 2);
        records.push(broca_wal::WalRecord::StepStarted {
            step_id: broca_protocol::StepId(2),
        });
        let protected = protected_input(&records);
        assert_eq!(
            protected,
            vec![Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "pending".into()
                }]
            )]
        );
    }
}
