use std::{collections::HashSet, path::PathBuf};

use broca_provider::{
    families::frozen_config_for_family,
    media_profile::{select_media_profile, MediaCarrier, MediaProfileId, MediaQuirk},
    spec::{InputModalities, WireFamilyId},
    wire::FrozenRenderConfig,
};
use serde::Deserialize;
use serde_json::json;

pub const MAX_REQUESTS: usize = 3;

#[derive(Default)]
pub struct Budget {
    pub sent: usize,
}

impl Budget {
    pub fn take(&mut self) -> Result<(), &'static str> {
        if self.sent >= MAX_REQUESTS {
            return Err("family_request_cap");
        }
        self.sent += 1;
        Ok(())
    }
}

#[derive(Default)]
pub struct Args {
    pub live: bool,
    pub plan: Option<PathBuf>,
    pub subc: Option<PathBuf>,
    pub error_detail: Option<PathBuf>,
    pub timeout_secs: u64,
}

impl Args {
    pub fn parse(args: impl Iterator<Item = String>) -> Result<Self, &'static str> {
        let mut result = Self {
            timeout_secs: 180,
            ..Self::default()
        };
        let mut dry = false;
        let mut args = args;
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--live" => result.live = true,
                "--dry-run" => dry = true,
                "--plan" => result.plan = Some(args.next().ok_or("plan_path_required")?.into()),
                "--subc" => result.subc = Some(args.next().ok_or("subc_path_required")?.into()),
                "--error-detail" => result.error_detail = Some(args.next().ok_or("error_detail_path_required")?.into()),
                "--timeout-secs" => {
                    result.timeout_secs = args.next().ok_or("timeout_required")?.parse().map_err(|_| "invalid_timeout")?;
                    if !(1..=600).contains(&result.timeout_secs) { return Err("timeout_out_of_range"); }
                }
                _ => return Err("usage: media-live-check [--dry-run | --live --plan FILE] [--subc FILE] [--error-detail FILE] [--timeout-secs 1..600]"),
            }
        }
        if dry && result.live {
            return Err("dry_run_conflicts_with_live");
        }
        if result.live && result.plan.is_none() {
            return Err("explicit_live_plan_required");
        }
        Ok(result)
    }

    pub fn routes(&self) -> Result<Vec<Route>, &'static str> {
        if self.live
            && (!cfg!(target_os = "macos")
                || std::env::var("BROCA_LIVE_TESTS").as_deref() != Ok("1"))
        {
            return Err("live_requires_mac_operator_and_BROCA_LIVE_TESTS_1");
        }
        let contents = match &self.plan {
            Some(path) => {
                let metadata = std::fs::metadata(path).map_err(|_| "plan_read_failed")?;
                if metadata.len() > 1024 * 1024 {
                    return Err("plan_too_large");
                }
                std::fs::read_to_string(path).map_err(|_| "plan_read_failed")?
            }
            None => include_str!("plan.json").into(),
        };
        let routes: Vec<Route> = serde_json::from_str(&contents).map_err(|_| "invalid_plan")?;
        validate_routes(&routes)?;
        if self.live
            && routes.iter().any(|route| {
                route.model.contains("REPLACE")
                    || route
                        .project_id
                        .as_deref()
                        .is_some_and(|project| project.contains("REPLACE"))
            })
        {
            return Err("live_plan_contains_placeholder");
        }
        Ok(routes)
    }

    pub fn subc_path(&self) -> PathBuf {
        self.subc
            .clone()
            .or_else(|| {
                std::env::var_os("BROCA_LIVE_SUBC")
                    .or_else(|| std::env::var_os("SUBC_SOCKET"))
                    .map(PathBuf::from)
            })
            .unwrap_or_else(|| {
                PathBuf::from(std::env::var_os("HOME").unwrap_or_default())
                    .join(".local/share/cortexkit/run/subc-connection.json")
            })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    pub family: WireFamilyId,
    pub model: String,
    #[serde(default)]
    pub credential: Option<String>,
    #[serde(default)]
    pub quirk: Option<MediaQuirk>,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub aws_region: Option<String>,
    /// Optional complete non-secret serving config, to preserve endpoint-specific quirks and
    /// identity. Its media profile is replaced only in memory with the probe candidates.
    #[serde(default)]
    pub frozen: Option<FrozenRenderConfig>,
}

fn validate_routes(routes: &[Route]) -> Result<(), &'static str> {
    if routes.is_empty() || routes.len() > 7 {
        return Err("plan_requires_one_to_seven_families");
    }
    let mut families = HashSet::new();
    for route in routes {
        // One route consumes all three slots. Two profiles of the same family cannot
        // accidentally multiply the spend; untested provider profiles stay gated off.
        if !families.insert(route.family) {
            return Err("duplicate_family_exceeds_request_cap");
        }
        if route.model.is_empty()
            || route.model.len() > 200
            || !route
                .model
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_/.:".contains(&b))
        {
            return Err("invalid_wire_model_label");
        }
        if let Some(frozen) = &route.frozen {
            if frozen.family_id != route.family || frozen.wire_model_id != route.model {
                return Err("frozen_config_route_mismatch");
            }
        }
    }
    Ok(())
}

impl Route {
    pub fn is_anthropic_oauth(&self) -> bool {
        self.family == WireFamilyId::AnthropicMessages
            && self
                .credential
                .as_deref()
                .is_some_and(|name| name.starts_with("oauth:"))
    }

    pub fn live_skip_reason(&self, frozen: &FrozenRenderConfig) -> Option<&'static str> {
        (self.family == WireFamilyId::GoogleCodeAssist
            && frozen.project_id.as_deref().is_none_or(str::is_empty))
        .then_some("project_id_required")
    }

    pub fn missing_live_field(&self, frozen: &FrozenRenderConfig) -> Option<&'static str> {
        match self.live_skip_reason(frozen) {
            Some("project_id_required") => Some("project_id"),
            _ => None,
        }
    }

    pub fn frozen(&self) -> Result<FrozenRenderConfig, &'static str> {
        let mut frozen = self
            .frozen
            .clone()
            .unwrap_or_else(|| frozen_config_for_family(self.family, &self.model));
        if self.is_anthropic_oauth() {
            frozen.system_prompt =
                broca_provider::policy::SystemPromptPolicy::RequiredIdentityLead {
                    identity: broca_provider::families::CLAUDE_CODE_IDENTITY.into(),
                };
        }
        frozen.max_output_tokens = 512;
        frozen.caller_max_output_tokens = Some(512);
        frozen.prompt_cache_key = Some("broca-synthetic-media-live".into());
        if self.family == WireFamilyId::AnthropicMessages && frozen.cache.is_none() {
            // Use the same no-override default as production cache routing. A prompt cache
            // key is an OpenAI hint and cannot enable Anthropic's explicit breakpoints.
            frozen.cache = broca_module_serve::cache_routing::default_cache_policy(
                self.family,
                "anthropic",
                None,
            );
        }
        if let Some(url) = &self.base_url {
            frozen.base_url = Some(url.clone());
        }
        if let Some(project) = &self.project_id {
            frozen.project_id = Some(project.clone());
        }
        if let Some(region) = &self.aws_region {
            frozen.aws_region = Some(region.clone());
        }
        let quirk = self.quirk.or(match self.family {
            WireFamilyId::AnthropicMessages => Some(MediaQuirk::Anthropic),
            WireFamilyId::OpenAiChat
            | WireFamilyId::OpenAiResponses
            | WireFamilyId::OpenAiResponsesChatgpt => Some(MediaQuirk::Openai),
            WireFamilyId::Gemini | WireFamilyId::GoogleCodeAssist => Some(MediaQuirk::Google),
            WireFamilyId::BedrockConverse => None,
        });
        let mut profile = select_media_profile(
            self.family,
            quirk,
            &self.model,
            InputModalities::from_value(&json!(["image", "pdf", "audio", "video"])),
            Some(200_000),
        );
        if profile.profile == MediaProfileId::Conservative {
            return Err("probe_requires_known_profile");
        }
        use MediaCarrier::{FollowUpUserMessage, Marker, Native};
        use MediaProfileId::*;
        if matches!(
            profile.profile,
            OpenaiChat
                | Openrouter
                | Xai
                | Deepseek
                | Kimi
                | MetaChat
                | MetaResponses
                | Gemini
                | CodeAssistGemini
        ) {
            profile.live_gated.follow_up_user_message = true;
            profile.image.tool_result = FollowUpUserMessage;
            if matches!(
                profile.profile,
                OpenaiChat | Openrouter | MetaChat | MetaResponses | Gemini | CodeAssistGemini
            ) {
                profile.document.tool_result = FollowUpUserMessage;
            }
        }
        if matches!(profile.profile, CodeAssistGemini | CodeAssistGemini3) {
            profile.live_gated.code_assist_media = true;
            profile.image.user_message = Native;
            profile.document.user_message = Native;
            profile.audio.user_message = Native;
            if profile.profile == CodeAssistGemini3 {
                profile.image.tool_result = Native;
                profile.document.tool_result = Native;
            }
        }
        // No video fixture is probed. A Code Assist pass cannot justify its coarse all-kind
        // production gate, and audio/video tool results on Gemini remain markers.
        profile.video.user_message = Marker;
        profile.video.tool_result = Marker;
        frozen.media = Some(profile);
        Ok(frozen)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dry_run_is_default_and_live_requires_an_explicit_plan() {
        let default = Args::parse(std::iter::empty()).unwrap();
        assert!(!default.live);
        assert!(default.error_detail.is_none());
        assert!(!Args::parse(["--dry-run".into()].into_iter()).unwrap().live);
        assert!(Args::parse(["--live".into()].into_iter()).is_err());
        assert!(Args::parse(["--dry-run".into(), "--live".into()].into_iter()).is_err());
        let opted_in = Args::parse(
            [
                "--dry-run".into(),
                "--error-detail".into(),
                "private.jsonl".into(),
            ]
            .into_iter(),
        )
        .unwrap();
        assert_eq!(opted_in.error_detail, Some(PathBuf::from("private.jsonl")));
        assert!(Args::parse(["--error-detail".into()].into_iter()).is_err());
    }

    #[test]
    fn fourth_attempt_and_duplicate_family_are_refused() {
        let mut budget = Budget::default();
        for _ in 0..3 {
            budget.take().unwrap();
        }
        assert_eq!(budget.take(), Err("family_request_cap"));
        assert_eq!(budget.sent, 3);
        let routes: Vec<Route> = serde_json::from_value(json!([
            {"family":"openai_chat", "model":"one"},
            {"family":"openai_chat", "model":"two"}
        ]))
        .unwrap();
        assert_eq!(
            validate_routes(&routes),
            Err("duplicate_family_exceeds_request_cap")
        );
    }

    #[test]
    fn candidates_do_not_enable_the_production_registry() {
        let routes = Args::default().routes().unwrap();
        assert_eq!(routes.len(), 6);
        for route in routes {
            route.frozen().unwrap();
        }
        assert!(broca_provider::media_profile::LIVE_CHECKED_GATES.is_empty());
    }

    #[test]
    fn serving_plan_pins_operator_supplied_models_and_credential_names() {
        let routes = Args::default().routes().unwrap();
        let expected = [
            (
                WireFamilyId::AnthropicMessages,
                "claude-haiku-5-5",
                "oauth:anthropic",
            ),
            (WireFamilyId::OpenAiResponses, "gpt-6-luna", "apikey:openai"),
            (
                WireFamilyId::OpenAiResponsesChatgpt,
                "gpt-6-luna",
                "chatgpt:openai",
            ),
            (WireFamilyId::OpenAiChat, "gpt-6-luna", "apikey:openai"),
            (WireFamilyId::Gemini, "gemini-3.8-flash", "apikey:google"),
            (
                WireFamilyId::GoogleCodeAssist,
                "gemini-3.8-flash-medium",
                "antigravity:google",
            ),
        ];
        for (family, model, credential) in expected {
            let route = routes.iter().find(|route| route.family == family).unwrap();
            assert_eq!(route.model, model);
            assert_eq!(route.credential.as_deref(), Some(credential));
        }
        assert!(!routes
            .iter()
            .any(|route| route.family == WireFamilyId::BedrockConverse));
        let codeassist = routes
            .iter()
            .find(|route| route.family == WireFamilyId::GoogleCodeAssist)
            .unwrap();
        assert_eq!(codeassist.credential.as_deref(), Some("antigravity:google"));
        assert_eq!(codeassist.project_id, None);
        assert_eq!(
            codeassist.live_skip_reason(&codeassist.frozen().unwrap()),
            Some("project_id_required")
        );
    }
}
