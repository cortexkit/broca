//! The run loop and served operations share the same frozen configuration.

use super::{
    conditions::{self, SendContext},
    protocol::{
        notice::Conditions,
        read::Entry,
        rules::{ProviderRules, SignedParts},
        setup::ModelRef,
    },
};
use broca_records::{record::PlanManifest, record::RunConfig};
use cortexkit_compaction_view::rules::Floor;

/// What a send's compaction conditions and context read from its run's frozen configuration.
pub struct Frozen<'a> {
    pub model: &'a broca_protocol::request::ModelHandle,
    pub variant: Option<&'a str>,
    pub service_tier: Option<broca_protocol::request::ServiceTier>,
    pub frozen_render_config: &'a serde_json::Value,
    pub tools: &'a [broca_protocol::request::Tool],
    pub global_system: Option<&'a [broca_protocol::Message]>,
    pub auth_selection: Option<&'a broca_records::record::AuthSelection>,
    pub context_limit: Option<u64>,
    pub max_output_tokens: Option<u64>,
}

impl<'a> Frozen<'a> {
    pub fn of(config: &'a RunConfig) -> Self {
        Self {
            model: &config.model,
            variant: config.variant.as_deref(),
            service_tier: config.service_tier,
            frozen_render_config: &config.frozen_render_config,
            tools: &config.tools,
            global_system: config.global_system.as_deref().map(Vec::as_slice),
            auth_selection: config.auth_selection.as_ref(),
            context_limit: config.context_limit,
            max_output_tokens: config.generation.max_output_tokens,
        }
    }
}

pub fn context_of(config: &Frozen<'_>, manifest: Option<&PlanManifest>) -> SendContext {
    let mut model = ModelRef::new(&config.model.provider_module_id, &config.model.model_id);
    model.variant = config.variant.map(str::to_owned);
    model.service_tier = Some(
        config
            .service_tier
            .map_or_else(
                || broca_protocol::frozen_service_tier(config.frozen_render_config),
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
            .and_then(crate::compaction::view::selection_account_label),
        surface,
    }
}

/// The conditions a notice, ride or view check states. Entries retain billed usage; the
/// host-only fill overrides it only for the newest step that reported input usage.
/// `signed_floor` is the id of the entry the session's signed floor goes through ([`signed_floor`]).
pub fn conditions(
    config: &RunConfig,
    entries: &[Entry],
    chained: bool,
    newest_step_fill: Option<(&str, u64)>,
    signed_floor: Option<&str>,
) -> Result<Conditions, String> {
    // Conditions are not tied to the view the request was rendered under (`last_prompt_view` stays
    // absent).
    conditions_of(
        &Frozen::of(config),
        entries,
        chained,
        newest_step_fill,
        signed_floor,
        &|_| None,
        None,
    )
}

/// The session's signed floor as the compaction contract names it (`signed_floor.through`): the
/// entry it goes through, by its stable id. `through` is that entry's id. The floor itself is
/// the ordinal of the first message whose signed reasoning requests still send
/// ([`crate::signed_floor::SignedFloor::floor`]), so the floor goes through the message before
/// it, whose entry's one-based ordinal is that same number; the caller finds that entry
/// wherever the session holds it, sealed history included, so the floor is stated the same on
/// every call, whatever entries the call has in hand. This is the one place broca turns its
/// floor into what the compaction provider and the view checks read.
///
/// `None` when the session has no floor (no run changed the tool set, or the change came before
/// any message, so there is no earlier reasoning to drop), or when `rules` say the model doesn't
/// bind signed parts to the request prefix (the floor then constrains nothing). An absent floor
/// keeps every view check as strict as it was before floors existed.
pub fn signed_floor(
    through: Option<&str>,
    rules: &ProviderRules,
) -> Option<cortexkit_role_compaction::SignedFloor> {
    if rules.signed_parts == SignedParts::None {
        return None;
    }
    through.map(cortexkit_role_compaction::SignedFloor::new)
}

/// The id of the entry with ordinal `floor` among `entries`, for a caller that holds every
/// entry its floor can name. `None` when `floor` is absent or zero, or `entries` don't hold it.
pub fn floor_entry(floor: Option<u64>, entries: &[Entry]) -> Option<&str> {
    let through = floor.filter(|floor| *floor > 0)?;
    entries
        .iter()
        .find(|entry| entry.ordinal == through)
        .map(|entry| entry.mid.as_str())
}

/// The floor the view checks honour under `conditions`: signed parts it covers are already
/// gone from every request, so a view that keeps them is never refused for them (compaction
/// contract W3.5), whether or not the provider was told of the floor.
pub fn view_floor(conditions: &Conditions) -> Floor<'_> {
    conditions
        .signed_floor
        .as_ref()
        .map_or(Floor::none(), |floor| Floor::through(&floor.through))
}

/// [`conditions`] from a run's frozen choices in any shape. `prompt_view` names, for the entry
/// whose usage gives the session's fill ([`last_prompt`]), the version of the view the request
/// that produced it was rendered under: `Some(0)` for no view, `None` when the caller can't tie
/// that request to a view. `statement_seq` is the newest log record the statement reflects;
/// rule-only checks may omit it. The runtime gates the stamp on the provider's declaration.
pub fn conditions_of(
    config: &Frozen<'_>,
    entries: &[Entry],
    chained: bool,
    newest_step_fill: Option<(&str, u64)>,
    signed_floor: Option<&str>,
    prompt_view: &dyn Fn(&Entry) -> Option<u64>,
    statement_seq: Option<u64>,
) -> Result<Conditions, String> {
    let window = config
        .context_limit
        .ok_or("compaction requires a frozen context_limit")?;
    // What the model may still write. A run that froze no `max_output_tokens` of its own (the
    // ChatGPT route never sends one, since that backend refuses the field) still writes up to
    // the model's output cap, which the frozen render config holds.
    let reserve = config
        .max_output_tokens
        .or_else(|| {
            config
                .frozen_render_config
                .pointer("/max_output_tokens")
                .and_then(serde_json::Value::as_u64)
        })
        .unwrap_or(0);
    if window > 9_007_199_254_740_991 || reserve > 9_007_199_254_740_991 {
        return Err(
            "compaction context_limit or output reserve exceeds the protocol integer range".into(),
        );
    }
    let mut conditions =
        conditions::conditions(context_of(config, None).model, window, reserve, chained);
    if statement_seq.is_some_and(|seq| seq > 9_007_199_254_740_991) {
        return Err("compaction statement_seq exceeds the protocol integer range".into());
    }
    conditions.statement_seq = statement_seq;
    conditions.accepts_images = config
        .frozen_render_config
        .pointer("/media/modalities/image")
        .and_then(serde_json::Value::as_bool);
    // The provider measures the session's fill from this field alone; without it a session
    // never reaches the provider's compaction threshold.
    // The view is read off the same entry as the reading, so the two always describe one
    // request: the provider ignores a reading taken under an older view than its newest.
    let reading = last_prompt(entries, newest_step_fill);
    conditions.last_prompt_tokens = reading.map(|(_, tokens)| tokens);
    conditions.last_prompt_view = reading.and_then(|(entry, _)| prompt_view(entry));
    // Every condition carries the floor; the runtime sends it only to a provider that declares
    // it reads one (`runtime::Runtime`).
    conditions.signed_floor = self::signed_floor(signed_floor, &conditions.rules);
    Ok(conditions)
}

/// The newest measured prompt size among `entries`, with the entry whose usage gave it.
pub fn last_prompt<'e>(
    entries: &'e [Entry],
    newest_step_fill: Option<(&str, u64)>,
) -> Option<(&'e Entry, u64)> {
    entries.iter().rev().find_map(|entry| {
        let prompt = entry.usage.as_ref()?.prompt_tokens?;
        Some((
            entry,
            match newest_step_fill {
                Some((mid, fill)) if entry.mid == mid => fill,
                _ => prompt,
            },
        ))
    })
}

/// Billed iterations can include a refused request on another model. Only the last
/// serving model's prompt measures context fill; single-model steps need no override.
/// A last model that reports zero for every input count gives `None`, as `step_usage` treats
/// zero as unmeasured: a real request always has a prompt, and a fill of 0 would read as empty.
pub fn fallback_fill(
    served_model: &str,
    usage: broca_protocol::Usage,
    usage_iterations: Option<&[broca_protocol::usage::UsageIteration]>,
) -> Option<u64> {
    let serving = broca_protocol::usage::model_usage(served_model, usage, usage_iterations);
    let (last_model, last_usage) = serving.last()?;
    if !serving.iter().any(|(model, _)| model != last_model) {
        return None;
    }
    super::metadata::step_usage(*last_usage)?.prompt_tokens
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
