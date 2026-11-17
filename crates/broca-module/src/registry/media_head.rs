use super::*;
use broca_protocol::request::SubmissionRequest;
use cortexkit_role_llm_runner::read::{HeadInput, HeadModel};

fn input(profile: &broca_provider::media_profile::MediaProfile) -> HeadInput {
    use broca_protocol::MediaKind;
    let modalities: Vec<String> = [
        MediaKind::Image,
        MediaKind::Document,
        MediaKind::Audio,
        MediaKind::Video,
    ]
    .into_iter()
    .filter(|kind| {
        profile.accepts_modality(*kind)
            && profile.kind(*kind).user_message
                != broca_provider::media_profile::MediaCarrier::Marker
    })
    .map(|kind| broca_provider::person_media::modality(kind).into())
    .collect();
    if modalities.is_empty() {
        return broca_wire::head_input();
    }
    let mut input = HeadInput::new("yes")
        .with_modalities(modalities)
        .with_mime_types(broca_provider::person_media::input_mime_types(profile));
    (input.max_image_bytes, input.max_image_dimension) = profile.image_limits();
    input
}

/// Whether the head of a session whose latest run is in `state` quotes the catalog's media
/// profile for the selected model, rather than reading the profile the run froze.
pub(crate) fn quotes_catalog(state: &str) -> bool {
    matches!(state, "completed" | "cancelled" | "error" | "exhausted")
}

/// The input profile a run's frozen render config describes, from its `media` member: attachments
/// off when the run froze none, and unknown when the member doesn't read as a media profile.
pub(crate) fn frozen_input(media: Option<serde_json::Value>) -> HeadInput {
    match media {
        Some(value) => serde_json::from_value(value)
            .ok()
            .as_ref()
            .map(input)
            .unwrap_or_else(|| HeadInput::new("unknown")),
        None => broca_wire::head_input(),
    }
}

fn quote_request(model: &HeadModel) -> SubmissionRequest {
    SubmissionRequest {
        message_marks: Default::default(),
        account_route: None,
        route_decision_id: None,
        service_tier_level: None,
        staged_uploads: None,
        content: Vec::new(),
        directives: None,
        on_restart: None,
        scope: Default::default(),
        project_id: None,
        principal: None,
        model: broca_protocol::ModelHandle::new(&model.provider, &model.model),
        input: Vec::new(),
        tools: None,
        policy_allowed: Default::default(),
        tool_choice: Default::default(),
        generation: Default::default(),
        cache: None,
        budget: None,
        auth: None,
        context_limit: None,
        work_class: None,
        variant: model.variant.clone(),
        service_tier: None,
        stop_when: Vec::new(),
        context_management: None,
        inherit_frozen: Default::default(),
        structured_output: None,
        client_message_id: None,
    }
}

impl LineageRegistry {
    pub(crate) async fn quoted_media_head(&self, model: Option<&HeadModel>) -> HeadInput {
        let Some(model) = model else {
            return HeadInput::new("unknown");
        };
        // The executor quotes its in-memory catalog and adapter tables, never a provider call.
        match self.media_profile(&quote_request(model)).await {
            Ok(Some(profile)) => input(&profile),
            _ => HeadInput::new("unknown"),
        }
    }
}
