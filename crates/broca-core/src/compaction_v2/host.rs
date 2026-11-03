//! The run loop and served operations share the same frozen configuration.

use super::{
    conditions::{self, SendContext},
    protocol::{notice::Conditions, setup::ModelRef},
};
use broca_wal::{FramedRecord, PlanManifest, RunConfig, WalRecord};

pub fn context(config: &RunConfig, manifest: Option<&PlanManifest>) -> SendContext {
    let mut model = ModelRef::new(&config.model.provider_module_id, &config.model.model_id);
    model.variant = config.variant.clone();
    model.service_tier = Some(
        config
            .service_tier
            .map_or_else(
                || broca_protocol::frozen_service_tier(&config.frozen_render_config),
                |tier| tier.as_str(),
            )
            .to_owned(),
    );
    let surface = serde_json::to_string(&serde_json::json!({
        "tools": config.tools,
        "system": manifest.map(|manifest| manifest.system_text.as_str()),
        "global_system": config.global_system,
    }))
    .expect("frozen configuration serializes");
    SendContext {
        model,
        account: config
            .auth_selection
            .as_ref()
            .and_then(crate::compaction::view::selection_account_label),
        surface,
    }
}

pub fn conditions(config: &RunConfig, chained: bool) -> Result<Conditions, String> {
    let window = config
        .context_limit
        .ok_or("compaction requires a frozen context_limit")?;
    let reserve = config.generation.max_output_tokens.unwrap_or(0);
    if window > 9_007_199_254_740_991 || reserve > 9_007_199_254_740_991 {
        return Err(
            "compaction context_limit or output reserve exceeds the protocol integer range".into(),
        );
    }
    let mut conditions =
        conditions::conditions(context(config, None).model, window, reserve, chained);
    conditions.accepts_images = config
        .frozen_render_config
        .pointer("/media/modalities/image")
        .and_then(serde_json::Value::as_bool);
    Ok(conditions)
}

/// Ride causes compare the last accepted request's frozen session model.
/// A provider's temporary resend of one rejected step is not a session model change.
pub fn previous(records: &[FramedRecord]) -> Option<SendContext> {
    let mut current: Option<RunConfig> = None;
    let mut manifest = None;
    let mut sent = None;
    for frame in records {
        match &frame.record {
            WalRecord::RunStarted {
                config,
                manifest: plan,
                ..
            } => {
                current = Some((**config).clone());
                manifest = plan.as_deref();
            }
            WalRecord::ModelStepFinished { .. } => {
                sent = current.as_ref().map(|config| context(config, manifest))
            }
            WalRecord::ModelAttemptFinished {
                http_status,
                outcome,
                ..
            } if crate::cache_warm::attempt_reached_the_cache(*http_status, outcome) => {
                sent = current.as_ref().map(|config| context(config, manifest));
            }
            record => {
                if let Some(config) = current.as_mut() {
                    crate::cache_warm::fold_refreeze(config, record);
                }
            }
        }
    }
    sent
}

pub fn frozen(records: &[FramedRecord]) -> Result<(RunConfig, Option<&PlanManifest>), String> {
    let start = records
        .iter()
        .rposition(|frame| matches!(frame.record, WalRecord::RunStarted { .. }))
        .ok_or("compaction session has no frozen run configuration")?;
    let WalRecord::RunStarted {
        config, manifest, ..
    } = &records[start].record
    else {
        unreachable!()
    };
    let mut config = (**config).clone();
    for frame in &records[start + 1..] {
        crate::cache_warm::fold_refreeze(&mut config, &frame.record);
    }
    Ok((config, manifest.as_deref()))
}

pub fn image(
    writer: &crate::WalHandle,
    image: &super::protocol::view::InsertedImage,
) -> Result<broca_protocol::MediaRef, String> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(&image.data)
        .map_err(|error| error.to_string())?;
    writer
        .media_capture()
        .ok_or("compaction image requires session media storage")?
        .capture_view_image(&bytes, &image.media_type)
}

pub fn image_reference(
    image: &super::protocol::view::InsertedImage,
) -> Result<broca_protocol::MediaRef, String> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(&image.data)
        .map_err(|error| error.to_string())?;
    crate::media_validation::validate_media(&bytes, &image.media_type)
        .map_err(|reason| format!("compaction image refused: {reason:?}"))
}

pub async fn records(
    writer: &crate::WalHandle,
    store: Option<&dyn crate::WalStore>,
    session: &broca_protocol::BindIdentity,
) -> Result<Vec<FramedRecord>, String> {
    let head = writer.sync().await.map_err(|error| error.to_string())?;
    let store = store.ok_or("compaction requires a durable session reader")?;
    let range = store
        .read_records_range(session, 0, Some(head), None)
        .map_err(|error| error.to_string())?;
    range_records(range.into_records())
}

pub fn range_records<T>(range: broca_wal::WalRangeRecords<T>) -> Result<T, String> {
    match range {
        broca_wal::WalRangeRecords::Complete(records)
        | broca_wal::WalRangeRecords::Unknown(records) => Ok(records),
        broca_wal::WalRangeRecords::Partial(_) => {
            Err("compaction history range is incomplete".into())
        }
        broca_wal::WalRangeRecords::Absent => Err("compaction session WAL is absent".into()),
    }
}

pub async fn notice(
    session: &super::session::Session,
    writer: &crate::WalHandle,
    store: Option<&dyn crate::WalStore>,
    bind: &broca_protocol::BindIdentity,
    scope: Option<&broca_protocol::SessionScope>,
    plane: Option<&dyn crate::CompactionPlane>,
) -> Result<(), String> {
    let frames = records(writer, store, bind).await?;
    let (config, manifest) = frozen(&frames)?;
    let Some((provider, preset, _, _)) =
        manifest.map(crate::compaction::item).transpose()?.flatten()
    else {
        return Ok(());
    };
    let preset = preset.ok_or("mode-3 compaction preset must be a string")?;
    // Without a frozen window no conditions can be stated, so nothing is announced. Admission
    // never lets such a run start; if one is resumed anyway, its next step boundary ends it
    // as `compaction_unavailable` before any model I/O.
    if config.context_limit.is_none() {
        return Ok(());
    }
    let lineage = writer
        .lineage_id()
        .ok_or("mode-3 compaction requires a WAL lineage")?
        .to_string();
    let transcript = super::transcript::project(&frames, &lineage)?;
    let conditions = conditions(&config, false)?;
    let applied = session.state.lock().await.applied_identity();
    let mut setup = conditions::setup_request(&preset, &conditions, crate::compaction::now());
    setup.applied_view = applied.clone();
    setup.newest = super::views::newest(&transcript.entries);
    session.runtime.observe(&transcript.entries);
    session
        .runtime
        .flush(
            &super::runtime::BoundPlane {
                plane,
                provider: &provider,
                bind,
                scope,
            },
            &setup,
            &conditions,
            applied.as_ref(),
        )
        .await;
    Ok(())
}
