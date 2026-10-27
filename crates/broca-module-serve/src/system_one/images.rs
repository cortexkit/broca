//! Resolve screenshot handles before admission writes, keeping bytes out of replay identity.

use std::sync::Arc;

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine};
use broca_protocol::SendScope;
use cortexkit_model_batch::image::{
    DecodedImage, Image, InlineImage, ResolveFailure, IMAGE_MAX_PIXELS,
};
use cortexkit_role_classifier::{
    errors::Refusal,
    request::{
        check_request, check_request_with_references, check_resolved_item_images, parse_request,
    },
};
use sha2::{Digest, Sha256};
use zeroize::Zeroize;

use super::SystemOne;

#[async_trait]
pub trait ImageResolver: Send + Sync {
    async fn resolve(
        &self,
        handle: &str,
        scope_ref: &str,
        parent_scope_ref: Option<&str>,
    ) -> Result<broca_subc::ResolvedImage, broca_subc::ImageResolveError>;
}

#[async_trait]
impl ImageResolver for broca_subc::PrivateContentClient {
    async fn resolve(
        &self,
        handle: &str,
        scope_ref: &str,
        parent_scope_ref: Option<&str>,
    ) -> Result<broca_subc::ResolvedImage, broca_subc::ImageResolveError> {
        self.resolve_image(handle, scope_ref, parent_scope_ref)
            .await
    }
}

/// Provider-only replacements. The original request and its identity retain handles.
#[derive(Default)]
pub(crate) struct PreparedImages(pub Vec<Option<String>>);

impl PreparedImages {
    pub(crate) fn retained_bytes(&self) -> usize {
        self.0.iter().flatten().fold(0usize, |total, image| {
            total.saturating_add(image.capacity())
        })
    }
}

impl Drop for PreparedImages {
    fn drop(&mut self) {
        for image in self.0.iter_mut().flatten() {
            image.zeroize();
        }
    }
}

impl SystemOne {
    pub fn with_image_resolver(mut self, resolver: Arc<dyn ImageResolver>) -> Self {
        self.image_resolver = Some(resolver);
        self
    }

    pub(crate) fn image_references(&self) -> bool {
        self.image_resolver.is_some()
    }

    pub(super) fn check_images_request(
        &self,
        request: &cortexkit_role_classifier::request::ClassifyRequest,
        entry: &cortexkit_role_classifier::describe::ModelEntry,
    ) -> Result<(), Refusal> {
        if self.image_references() {
            check_request_with_references(request, entry)
        } else {
            check_request(request, entry)
        }
    }

    pub(crate) async fn prepare_images(
        &self,
        params: &str,
        send: &SendScope,
        stored: Option<&broca_store::system_one::StoredBatch>,
    ) -> Result<PreparedImages, Refusal> {
        let Some(resolver) = &self.image_resolver else {
            return Ok(PreparedImages::default());
        };
        cortexkit_role_classifier::request::check_request_bytes(
            params.len(),
            &cortexkit_role_classifier::describe::Limits::new(
                self.config.max_items,
                self.config.max_request_bytes,
            ),
        )?;
        let request = parse_request(params)?;
        let row = self.catalog.row(&request.model);
        cortexkit_role_classifier::request::check_catalog_row(
            &request.model,
            super::classify::catalog_row(row.as_ref()),
        )?;
        let row = row.expect("catalog admission checked presence");
        let pin = self
            .pin(&request.model)
            .ok_or_else(|| Refusal::model_unavailable(&request.model))?;
        let entry = self.model_entry(pin, &row);
        self.check_images_request(&request, &entry)?;
        let mut prepared = Vec::with_capacity(request.items.len());
        for (i, item) in request.items.iter().enumerate() {
            let final_item = stored.is_some_and(|batch| {
                batch.outcomes.iter().any(|outcome| {
                    outcome.index == i as u64 && match &outcome.outcome {
                        broca_store::system_one::Outcome::Answered(_) => true,
                        broca_store::system_one::Outcome::Failed(text) => {
                            serde_json::from_str::<cortexkit_role_classifier::errors::ItemError>(
                                text,
                            )
                            .is_ok_and(|error| !error.is_retried_on_resend())
                        }
                    }
                })
            });
            if final_item {
                prepared.push(None);
                continue;
            }
            let mut resolved = Vec::new();
            let mut has_reference = false;
            for (j, image) in item.images.iter().flatten().enumerate() {
                let field = format!("items[{i}].images[{j}]");
                let decoded = if let Image::Reference(reference) = image {
                    has_reference = true;
                    let SendScope::Stamped(bind) = send else {
                        return Err(ResolveFailure::Unavailable.refusal(&field));
                    };
                    let parent = if bind.scope.flow_id.is_some()
                        || bind.parent_state != Some(broca_protocol::ParentState::Linked)
                    {
                        None
                    } else {
                        bind.parent.as_ref().map(|parent| parent.scope_ref.as_str())
                    };
                    let reply = resolver
                        .resolve(&reference.handle, &bind.scope.scope_ref, parent)
                        .await
                        .map_err(|error| match error {
                            broca_subc::ImageResolveError::SourceUnavailable => {
                                ResolveFailure::SourceUnavailable {
                                    retry_after_ms: 1_000,
                                }
                                .refusal(&field)
                            }
                            broca_subc::ImageResolveError::Unavailable => {
                                ResolveFailure::Unavailable.refusal(&field)
                            }
                            broca_subc::ImageResolveError::InvalidReply => invalid_reply(&field),
                        })?;
                    let bytes = STANDARD
                        .decode(&reply.bytes)
                        .map_err(|_| invalid_reply(&field))?;
                    if bytes.is_empty() || format!("{:x}", Sha256::digest(&bytes)) != reply.sha256 {
                        return Err(invalid_reply(&field));
                    }
                    DecodedImage::new(reply.mime, bytes)
                } else {
                    image
                        .decode()
                        .ok_or_else(|| Refusal::invalid_params(&field, "unsupported image"))?
                        .map_err(|reason| Refusal::invalid_params(&field, reason))?
                };
                resolved.push(decoded);
            }
            check_resolved_item_images(&entry, i, &resolved)?;
            for (j, decoded) in resolved.iter().enumerate() {
                let field = format!("items[{i}].images[{j}]");
                let media = broca_core::media_validation::validate_media(
                    &decoded.bytes,
                    &decoded.content_type,
                )
                .map_err(|_| {
                    Refusal::invalid_params(&field, "image bytes have no readable image dimensions")
                })?;
                if media.mime != decoded.content_type
                    || media
                        .width
                        .zip(media.height)
                        .is_none_or(|(w, h)| u64::from(w) * u64::from(h) > IMAGE_MAX_PIXELS)
                {
                    return Err(Refusal::invalid_params(
                        &field,
                        "image content type or pixel limit is invalid",
                    ));
                }
            }
            prepared.push(if has_reference {
                Some(
                    serde_json::to_string(
                        &resolved
                            .iter()
                            .map(InlineImage::from_decoded)
                            .collect::<Vec<_>>(),
                    )
                    .expect("inline images encode"),
                )
            } else {
                None
            });
        }
        Ok(PreparedImages(prepared))
    }
}

fn invalid_reply(field: &str) -> Refusal {
    // A corrupt resolution is not safe to retry as the same image. No reply fields are logged.
    tracing::warn!(target: "system_one", "image.resolve returned invalid image integrity metadata or bytes");
    ResolveFailure::Unavailable.refusal(field)
}
