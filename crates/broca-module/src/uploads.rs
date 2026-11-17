use crate::registry::{LineageKey, LineageRegistry};
use crate::serve::ServeError;
use base64::Engine;
use broca_core::media_uploads::UploadError;
use broca_protocol::{BindPrincipal, SendScope};
use cortexkit_role_llm_runner::attachments as wire;
use serde::de::DeserializeOwned;

pub(crate) async fn upload(
    registry: &LineageRegistry,
    session: &LineageKey,
    principal: Option<&BindPrincipal>,
    scope: &SendScope,
    method: &str,
    params: serde_json::Value,
) -> Result<serde_json::Value, ServeError> {
    let ownership = registry.upload_ownership(session).await?;
    if ownership.episodes == 0 {
        return Err(ServeError::Upload(UploadError::new(
            "unknown_session",
            &session.session,
        )));
    }
    let owner = broca_core::scope_admission::decide_owner_access(&ownership, principal, scope);
    let named = principal.is_some_and(|principal| {
        let BindPrincipal::Reserved { module_id } = principal else {
            return false;
        };
        ownership
            .first_episode
            .as_ref()
            .and_then(|episode| episode.manifest.as_ref())
            .and_then(|manifest| {
                serde_json::from_str::<serde_json::Value>(manifest.plan.as_json_text()).ok()
            })
            .and_then(|plan| {
                plan.as_object()
                    .and_then(|plan| broca_fetch_plan::plan_senders(plan).ok())
            })
            .is_some_and(|senders| {
                senders
                    .iter()
                    .any(|sender| broca_protocol::same_module(&sender.module, module_id))
            })
    });
    if !named {
        owner.map_err(ServeError::ScopeRefused)?;
    }
    let store = registry.upload_store().ok_or_else(|| {
        ServeError::Upload(UploadError::new("upload_store_failed", &session.session))
    })?;
    let session = session.clone();
    let method = method.to_owned();
    tokio::task::spawn_blocking(move || apply(store, &session, &method, params))
        .await
        .map_err(|error| ServeError::Open(format!("upload worker failed: {error}"), None))?
}

fn apply(
    store: broca_core::media_uploads::Uploads,
    session: &LineageKey,
    method: &str,
    params: serde_json::Value,
) -> Result<serde_json::Value, ServeError> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| ServeError::Open(error.to_string(), None))?
        .as_secs();
    let result = match method {
        "session.upload_begin" => {
            let request: wire::UploadBeginRequest = decode(params)?;
            serde_json::to_value(
                store
                    .begin(session, &request, now)
                    .map_err(ServeError::Upload)?,
            )
        }
        "session.upload_append" => {
            let request: wire::UploadAppendRequest = decode(params)?;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(&request.data)
                .map_err(|_| {
                    ServeError::Upload(UploadError::invalid("data", &request.upload_id))
                })?;
            let reply =
                store.append_strict(session, &request.upload_id, request.offset, &bytes, now);
            serde_json::to_value(reply.map_err(ServeError::Upload)?)
        }
        "session.upload_commit" => {
            let request: wire::UploadCommitRequest = decode(params)?;
            serde_json::to_value(
                store
                    .commit(session, &request.upload_id, now)
                    .map_err(ServeError::Upload)?,
            )
        }
        "session.upload_status" => {
            let request: wire::UploadStatusRequest = decode(params)?;
            serde_json::to_value(
                store
                    .status(session, &request.upload_id, now)
                    .map_err(ServeError::Upload)?,
            )
        }
        _ => {
            return Err(ServeError::InvalidParams {
                field: "method".into(),
                message: format!("unsupported upload method {method}"),
            })
        }
    };
    result.map_err(|error| ServeError::Open(error.to_string(), None))
}

fn decode<T: DeserializeOwned>(params: serde_json::Value) -> Result<T, ServeError> {
    serde_json::from_value(params.clone()).map_err(|error| ServeError::InvalidParams {
        field: broca_wire::failing_field::<T>(&params, &error)
            .unwrap_or_else(|| "upload_id".into()),
        message: error.to_string(),
    })
}

pub(crate) async fn media_read(
    registry: &LineageRegistry,
    session: &LineageKey,
    params: serde_json::Value,
) -> Result<serde_json::Value, ServeError> {
    let request: wire::MediaReadRequest = decode(params)?;
    if request.length > wire::CHUNK_MAX as u64 {
        return Err(ServeError::Upload(UploadError::invalid(
            "length",
            &request.sha256,
        )));
    }
    // Only a medium the session's own durable records name authorizes a digest, so a session
    // the store doesn't hold names none.
    match registry.store_holding(session)? {
        Some(store) => store_media_read(registry, &store, session, &request).await,
        None => Err(ServeError::Upload(UploadError::new(
            "unknown_media",
            &request.sha256,
        ))),
    }
}

/// `session.media_read` on a session the store holds. Only a medium the session's own durable
/// records name authorizes a digest. Another session's media, and an upload no send named,
/// read as `unknown_media`. The bytes come from the object the naming record stored, checked
/// against the reference.
async fn store_media_read(
    registry: &LineageRegistry,
    store: &crate::store_open::StoreRuntime,
    session: &LineageKey,
    request: &wire::MediaReadRequest,
) -> Result<serde_json::Value, ServeError> {
    use crate::store_reads::{StoreReadReply, StoreReadRequest};
    let read = StoreReadRequest::Media {
        sha256: request.sha256.clone(),
    };
    let media = match store.read(registry, session, read).await? {
        Some(StoreReadReply::Media(media)) => media?,
        Some(_) => return Err(crate::store_reads::mismatched()),
        // The session's creation never committed, so no record names anything.
        None => None,
    };
    let media = media
        .ok_or_else(|| ServeError::Upload(UploadError::new("unknown_media", &request.sha256)))?;
    request
        .check(media.reference.byte_size)
        .map_err(|field| ServeError::Upload(UploadError::invalid(field, &request.sha256)))?;
    let bytes = media.bytes.map_err(|error| {
        ServeError::Open(format!("read media {}: {error}", request.sha256), None)
    })?;
    media_reply(request, media.reference, &bytes)
}

/// The reply to `request` for the medium `reference`, whose verified bytes are `bytes`: its size
/// and type, with the requested range of its bytes unless the request asked for none.
fn media_reply(
    request: &wire::MediaReadRequest,
    reference: broca_protocol::MediaRef,
    bytes: &[u8],
) -> Result<serde_json::Value, ServeError> {
    let mut reply = wire::MediaReadReply::new(reference.byte_size, reference.mime);
    if request.length != 0 {
        let start = request.offset as usize;
        let end = bytes
            .len()
            .min(start.saturating_add(request.length as usize));
        reply =
            reply.with_data(base64::engine::general_purpose::STANDARD.encode(&bytes[start..end]));
    }
    serde_json::to_value(reply).map_err(|error| ServeError::Open(error.to_string(), None))
}
