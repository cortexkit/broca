//! Session-owned bridge from durable captures to the request transport.
use std::sync::Mutex;

use broca_core::{media_request::SessionMedia, media_store::MediaStore};
use broca_protocol::{CallOptions, ErrorClass, ProviderError};
use broca_wal::{FramedRecord, WalRecord};

use super::{prepare, AgingEvent, MediaAging};
use crate::{media_profile::MediaProfile, wire::FrozenRenderConfig};

type ReadTail = dyn Fn(u64) -> Result<Vec<FramedRecord>, String> + Send + Sync;

struct State {
    session: SessionMedia,
    aging: MediaAging,
    profile: Option<MediaProfile>,
    head: u64,
    step_open: bool,
}

/// One executor-owned projection. Each reader call supplies the complete contiguous range
/// after its sequence through the writer's durable head. A stored prompt cannot reconstruct
/// capture identity or protection; resume uses a durable metadata seed plus this tail.
pub struct LiveMedia {
    state: Mutex<State>,
    read_tail: Box<ReadTail>,
}

impl LiveMedia {
    #[cfg(test)]
    pub(super) fn aging_for_test(&self) -> MediaAging {
        self.state.lock().unwrap().aging.clone()
    }

    pub fn new(store: MediaStore, read_tail: Box<ReadTail>) -> Self {
        Self {
            state: Mutex::new(State {
                session: SessionMedia::new(store),
                aging: MediaAging::default(),
                profile: None,
                head: 0,
                step_open: false,
            }),
            read_tail,
        }
    }

    pub fn from_checkpoint(
        store: MediaStore,
        checkpoint: broca_core::media_request::MediaCheckpoint,
        budget: Option<broca_protocol::request::BudgetConfig>,
        profile: Option<MediaProfile>,
        read_tail: Box<ReadTail>,
    ) -> Result<Self, String> {
        use broca_core::media_request::MediaHistory;
        let (session, history, step_open, head) = checkpoint.into_session(store, budget);
        let mut active = profile;
        let mut aging = MediaAging::default();
        for event in history {
            match event {
                MediaHistory::Profile { media, selected } => {
                    let next: MediaProfile = serde_json::from_value(media)
                        .map_err(|error| format!("invalid media seed profile: {error}"))?;
                    if active.as_ref() != Some(&next) {
                        aging = MediaAging::default();
                        aging.reconcile_view(&next, &selected);
                        active = Some(next);
                    }
                }
                MediaHistory::Changes(changes) => {
                    if let Some(profile) = &active {
                        aging.fold_changes(profile, changes);
                    }
                }
                MediaHistory::Boundary => {
                    if let Some(profile) = &active {
                        aging.apply(profile, AgingEvent::Boundary);
                    }
                }
            }
        }
        Ok(Self {
            state: Mutex::new(State {
                session,
                aging,
                profile: active,
                head,
                step_open,
            }),
            read_tail,
        })
    }

    pub fn render(
        &self,
        config: &serde_json::Value,
        opts: &CallOptions,
        render_tags: bool,
        commit_boundary: bool,
    ) -> Result<Vec<u8>, Box<ProviderError>> {
        self.render_with_provenance(config, opts, render_tags, commit_boundary, None)
    }

    pub fn render_with_provenance(
        &self,
        config: &serde_json::Value,
        opts: &CallOptions,
        render_tags: bool,
        commit_boundary: bool,
        provenance: Option<&mut broca_protocol::render_provenance::RenderProvenance>,
    ) -> Result<Vec<u8>, Box<ProviderError>> {
        let frozen: FrozenRenderConfig = serde_json::from_value(config.clone())
            .map_err(|error| refusal("media_config", error.to_string()))?;
        let family = crate::registry::wire_family(frozen.family_id)
            .ok_or_else(|| refusal("media_config", "unknown media wire family".into()))?;
        if super::references(&opts.prompt).is_empty() {
            return Ok(family.render_with_provenance(&frozen, opts, provenance));
        }
        let mut state = self.state.lock().unwrap();
        for frame in (self.read_tail)(state.head).map_err(|error| refusal("media_wal", error))? {
            if frame.seq != state.head + 1 {
                return Err(refusal(
                    "media_wal",
                    "media WAL tail is out of order".into(),
                ));
            }
            let State {
                session,
                aging,
                profile,
                ..
            } = &mut *state;
            if profile.is_none() {
                *profile = frozen.media.clone();
            }
            if let Some(profile) = profile {
                aging
                    .fold_wal_record(profile, session, &frame.record)
                    .map_err(|error| refusal("media_wal", error))?;
            } else {
                session
                    .fold(&frame.record)
                    .map_err(|error| refusal("media_wal", error))?;
                session.take_changes();
            }
            match frame.record {
                WalRecord::RunStarted { .. } | WalRecord::ModelStepFinished { .. } => {
                    state.step_open = false
                }
                WalRecord::StepStarted { .. } => state.step_open = true,
                _ => {}
            }
            state.head = frame.seq;
        }
        // A fallback route can select a different profile without refreezing the episode.
        // Estimate on a copy: a view still being selected must not age durable captures.
        let mut aging = state.aging.clone();
        if let Some(profile) = &frozen.media {
            if state.profile.as_ref() != Some(profile) {
                aging.select_view(profile, &state.session.selected_bindings());
            } else if state.step_open {
                aging.apply(profile, AgingEvent::Boundary);
            }
        }
        let mut bundle = prepare(&state.session, frozen.media.as_ref(), &opts.prompt, &aging)
            .map_err(|error| refusal(error.code(), error.to_string()))?;
        let body = super::render_prepared_with_provenance(
            &*family,
            &frozen,
            opts,
            Some(&mut bundle),
            &|prompt| {
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
            },
            provenance,
        )
        .map_err(|error| refusal(error.code(), error.to_string()))?;
        if commit_boundary && state.profile == frozen.media {
            state.aging = aging;
        }
        Ok(body)
    }
}

fn refusal(code: &str, message: String) -> Box<ProviderError> {
    Box::new(ProviderError {
        class: ErrorClass::Permanent,
        message,
        provider_code: Some(code.into()),
        provider_details: None,
        provider_subcode: None,
        refusal: None,
        status: None,
        retry_after_secs: None,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    })
}
