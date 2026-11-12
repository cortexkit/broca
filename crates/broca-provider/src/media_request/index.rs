//! A session's image index, as one owner keeps it and every run of the session reads it.
//!
//! The index is the engine's state for one session: which media it captured and where
//! ([`SessionMedia`]), which of them are shown and which aged out ([`MediaAging`]), the media
//! limits in force, and whether a model step is open. Its owner applies each [`MediaEvent`] the
//! session's records stand for once, in record order, through [`MediaIndex::apply`]. A run
//! renders a request from the index as it stands ([`MediaIndex::render_with_provenance`]) and
//! changes nothing: the step boundary a request crosses is worked out on a copy of the aging
//! state, and the attempt's own record applies the same boundary to the owner's index later. A
//! boundary applied twice changes nothing, so the two agree.

use broca_core::media_request::SessionMedia;
use broca_protocol::media_event::{MediaEvent, MediaLimits};
use broca_protocol::{CallOptions, ProviderError};

use super::live::refusal;
use super::{prepare, AgingEvent, MediaAging};
use crate::{media_profile::MediaProfile, wire::FrozenRenderConfig};

/// One session's image index (see the module docs).
#[derive(Debug, Clone)]
pub struct MediaIndex {
    session: SessionMedia,
    aging: MediaAging,
    /// The media limits in force: the latest run's, or the latest refreeze's. `None` until a run
    /// with media limits started, and aging is then not tracked.
    profile: Option<MediaProfile>,
    /// Whether a model step is open: one started and hasn't ended. A request rendered in an
    /// open step crosses that step's boundary.
    step_open: bool,
}

/// The media limits a render config's `media` member states, decoded.
pub fn media_profile(limits: &MediaLimits) -> Result<MediaProfile, String> {
    serde_json::from_value(limits.0.clone())
        .map_err(|error| format!("invalid frozen media profile: {error}"))
}

impl MediaIndex {
    /// An index over `session` and `aging`, with `profile` in force.
    pub fn new(
        session: SessionMedia,
        aging: MediaAging,
        profile: Option<MediaProfile>,
        step_open: bool,
    ) -> Self {
        Self {
            session,
            aging,
            profile,
            step_open,
        }
    }

    /// What the index holds, estimated as `broca_core::media_request::held` estimates the
    /// engine: its inline size, the engine and the aging state. The heap of the media limits in
    /// force is left to the caller, which can walk a profile's serialized form.
    pub fn held_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            - std::mem::size_of::<SessionMedia>()
            - std::mem::size_of::<MediaAging>()
            + self.session.held_bytes()
            + self.aging.held_bytes()
    }

    pub fn session(&self) -> &SessionMedia {
        &self.session
    }

    pub fn aging(&self) -> &MediaAging {
        &self.aging
    }

    pub fn profile(&self) -> Option<&MediaProfile> {
        self.profile.as_ref()
    }

    pub fn step_open(&self) -> bool {
        self.step_open
    }

    /// Applies one event, with the transitions today's fold of the old records makes
    /// ([`MediaAging::fold_media_event`]). The first media limits an index learns are taken as
    /// the ones in force from the start, as today's renderer takes the run's own limits: nothing
    /// was aged under others. Without limits, the engine's state moves and aging isn't tracked.
    pub fn apply(&mut self, event: &MediaEvent) -> Result<(), String> {
        if self.profile.is_none() {
            let limits = match event {
                MediaEvent::RunStart {
                    limits: Some(limits),
                    ..
                }
                | MediaEvent::LimitsChanged { limits } => Some(limits),
                _ => None,
            };
            if let Some(limits) = limits {
                self.profile = Some(media_profile(limits)?);
            }
        }
        match self.profile.as_mut() {
            Some(profile) => self
                .aging
                .fold_media_event(profile, &mut self.session, event)?,
            None => {
                self.session.apply(event)?;
                self.session.take_changes();
            }
        }
        match event {
            MediaEvent::StepStart { .. } => self.step_open = true,
            MediaEvent::RunStart { .. } | MediaEvent::StepEnd | MediaEvent::Truncated { .. } => {
                self.step_open = false
            }
            _ => {}
        }
        Ok(())
    }

    /// The request body for `opts` under the frozen render config `config`, with the media the
    /// prompt names read and placed as the index says. A prompt naming no media renders as it
    /// always does. Nothing is written back: a request in an open step crosses the step's
    /// boundary on a copy of the aging state.
    pub fn render_with_provenance(
        &self,
        config: &serde_json::Value,
        opts: &CallOptions,
        render_tags: bool,
        provenance: Option<&mut broca_protocol::render_provenance::RenderProvenance>,
    ) -> Result<Vec<u8>, Box<ProviderError>> {
        let frozen = frozen(config)?;
        let family = family(&frozen)?;
        if super::references(&opts.prompt).is_empty() {
            return Ok(family.render_with_provenance(&frozen, opts, provenance));
        }
        // A fallback route can select a different profile without refreezing the episode.
        let mut aging = self.aging.clone();
        if let Some(profile) = &frozen.media {
            if self.profile.as_ref() != Some(profile) {
                aging.select_view(profile, &self.session.selected_bindings());
            } else if self.step_open {
                aging.apply(profile, AgingEvent::Boundary);
            }
        }
        let mut bundle = prepare(&self.session, frozen.media.as_ref(), &opts.prompt, &aging)
            .map_err(|error| refusal(error.code(), error.to_string()))?;
        super::render_prepared_with_provenance(
            &*family,
            &frozen,
            opts,
            Some(&mut bundle),
            &|prompt| tagged(config, prompt, render_tags),
            provenance,
        )
        .map_err(|error| refusal(error.code(), error.to_string()))
    }
}

/// The request body for `opts` when the session's index can't be had: a prompt naming no media
/// renders as it always does, and one naming media is refused with `code` and `reason`, never
/// sent without them.
pub fn render_without_index(
    config: &serde_json::Value,
    opts: &CallOptions,
    provenance: Option<&mut broca_protocol::render_provenance::RenderProvenance>,
    code: &str,
    reason: &str,
) -> Result<Vec<u8>, Box<ProviderError>> {
    let frozen = frozen(config)?;
    let family = family(&frozen)?;
    if super::references(&opts.prompt).is_empty() {
        return Ok(family.render_with_provenance(&frozen, opts, provenance));
    }
    Err(refusal(code, reason.to_owned()))
}

fn frozen(config: &serde_json::Value) -> Result<FrozenRenderConfig, Box<ProviderError>> {
    serde_json::from_value(config.clone())
        .map_err(|error| refusal("media_config", error.to_string()))
}

fn family(
    frozen: &FrozenRenderConfig,
) -> Result<Box<dyn crate::wire::WireFamily>, Box<ProviderError>> {
    crate::registry::wire_family(frozen.family_id)
        .ok_or_else(|| refusal("media_config", "unknown media wire family".into()))
}

/// The prompt with its tags rendered or cleaned, as the run chose.
fn tagged(
    config: &serde_json::Value,
    prompt: &[broca_protocol::Message],
    render_tags: bool,
) -> Vec<broca_protocol::Message> {
    let mut prompt = prompt.to_vec();
    if render_tags {
        broca_protocol::tags::render_tags_into(
            &mut prompt,
            broca_protocol::tags::TagRendering::for_render_config(config),
        );
    } else {
        broca_protocol::tags::clean_computed_into(&mut prompt);
    }
    prompt
}
