//! The broca management-surface wire adapter.
//!
//! Client-facing JSON types live in [`broca_wire`] and are re-exported here so existing
//! `broca_module_serve::wire` call sites keep their paths. This module retains only conversions
//! into the coordinator's transport-agnostic request types and import-specific types.

use broca_module::executor::RunKind;
use broca_module::serve::{SendRequest, SessionImportResponse, SubscribeFrom};
use broca_protocol::content::{ContentBlock, Message, Role};
use broca_protocol::request::{AuthRequest, CacheRequest, ModelHandle};
use broca_protocol::{BindIdentity, RunId, SubmissionRequest};
use serde::{Deserialize, Serialize};

pub use broca_wire::{
    AuthParams, CacheParams, CancelParams, CapInstallParams, CapMutationResponse,
    CapMutationResult, CursorWire, FromWire, ModelParams, ReadParams, ReadResult, RetractParams,
    RouteRequest, RunStatusResponse, SendParams, SendResult, SessionReadLineageState,
    SessionReadMessage, SessionReadResponse, SpendDeltaParams, StatusParams, SubscribeEvent,
    SubscribeParams, VaultAuditMissing, VaultAuditParams, VaultAuditResponse, VaultAuditRow,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportParams {
    pub messages: Vec<Message>,
    pub import_id: String,
    pub origin: ImportOriginParams,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportOriginParams {
    pub kind: ImportOriginKind,
    pub source_ref: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImportOriginKind {
    Import,
}

pub type ImportResult = SessionImportResponse;

/// Serve-boundary conversion from the shared wire attach point to the coordinator attach point.
pub trait FromWireExt {
    fn to_subscribe_from(&self) -> Result<SubscribeFrom, String>;
}

impl FromWireExt for FromWire {
    fn to_subscribe_from(&self) -> Result<SubscribeFrom, String> {
        match self {
            FromWire::Named(value) => match value.as_str() {
                "start" => Ok(SubscribeFrom::Start),
                "live" => Ok(SubscribeFrom::Live),
                other => Err(format!(
                    "unknown 'from' value '{other}' (want start|live|cursor)"
                )),
            },
            FromWire::Cursor { wal_seq, sub_index } => Ok(SubscribeFrom::After {
                wal_seq: *wal_seq,
                sub_index: *sub_index,
            }),
        }
    }
}

/// Serve-boundary conversion from shared wire send params to a coordinator request.
pub trait SendParamsExt {
    fn into_send_request(
        self,
        session: BindIdentity,
        run_id: RunId,
    ) -> Result<SendRequest, broca_core::RunError>;
}

impl SendParamsExt for SendParams {
    fn into_send_request(
        self,
        session: BindIdentity,
        run_id: RunId,
    ) -> Result<SendRequest, broca_core::RunError> {
        let SendParams {
            directives,
            session_class,
            on_restart,
            keep_warm,
            retention,
            prompt,
            prompt_blocks,
            system,
            send_id,
            conversation_key,
            title,
            model,
            tools,
            tool_choice,
            generation,
            stop_when,
            cache,
            budget,
            structured_output,
            auth,
            context_limit,
            work_class,
            context_management,
            service_tier,
            one_shot,
            delivery,
            dedupe_key,
            expires_at_ms,
            template,
            append_episode,
        } = self;
        // The one decode of the `delivery_order` group; the serve adapter has already refused a
        // malformed one by field, so an error here is a caller bug surfaced as a config refusal.
        let mut order =
            broca_wire::delivery_order(delivery.as_ref(), dedupe_key.as_deref(), expires_at_ms)
                .map_err(|(_, message)| broca_core::RunError::Config(message))?;
        if let Some(order) = &mut order {
            order.template = template.clone();
        }
        let delivery = broca_wire::legacy_delivery(delivery.as_ref());
        // The serve adapter fills an omitted model before converting, from the session's frozen
        // one where the send may omit it, and refuses the send otherwise.
        let Some(model) = model else {
            return Err(broca_core::RunError::Config("model is required".into()));
        };
        let (prompt_content, cache_prefix_blocks) = match prompt_blocks {
            Some(_) if !prompt.is_empty() => {
                return Err(broca_core::RunError::Config(
                    "caller sent both non-empty prompt and prompt_blocks; send exactly one prompt input"
                        .into(),
                ));
            }
            None if prompt.is_empty() && template.is_none() => {
                return Err(broca_core::RunError::Config(
                    "caller sent neither prompt nor prompt_blocks; send one prompt input".into(),
                ));
            }
            Some(blocks) if blocks.len() < 2 => {
                return Err(broca_core::RunError::Config(format!(
                    "caller sent prompt_blocks with {} element(s); prompt_blocks requires at least 2",
                    blocks.len()
                )));
            }
            Some(blocks) => {
                if let Some((index, _)) = blocks
                    .iter()
                    .enumerate()
                    .find(|(_, block)| block.trim().is_empty())
                {
                    return Err(broca_core::RunError::Config(format!(
                        "caller sent empty or whitespace-only prompt_blocks[{index}]; every prompt_blocks element must contain text"
                    )));
                }
                let prefix_count = u32::try_from(blocks.len() - 1).map_err(|_| {
                    broca_core::RunError::Config(format!(
                        "caller sent prompt_blocks with {} elements, exceeding the supported count",
                        blocks.len()
                    ))
                })?;
                (
                    blocks
                        .into_iter()
                        .map(|text| ContentBlock::Text { text })
                        .collect(),
                    Some(prefix_count),
                )
            }
            None => (vec![ContentBlock::Text { text: prompt }], None),
        };
        let _ = append_episode;
        let policy_allowed = tools
            .as_ref()
            .map(|surface| surface.iter().map(|tool| tool.name.clone()).collect())
            .unwrap_or_default();
        let system = system.filter(|system| !system.trim().is_empty());
        let mut input = Vec::with_capacity(if system.is_some() { 2 } else { 1 });
        if let Some(system) = system {
            input.push(Message::new(
                Role::System,
                vec![ContentBlock::Text { text: system }],
            ));
        }
        let mut user_message = Message::new(Role::User, prompt_content);
        user_message.cache_prefix_blocks = cache_prefix_blocks;
        input.push(user_message);
        let submission = SubmissionRequest {
            directives,
            on_restart,
            keep_warm,
            scope: Default::default(),
            project_id: None,
            principal: None,
            // The variant rides beside the model handle, never inside it: the handle is the
            // model identity resume compares, and a variant is a per-send request option.
            model: ModelHandle::new(model.provider, model.model),
            variant: model.variant,
            service_tier,
            input,
            tools,
            policy_allowed,
            tool_choice,
            generation,
            cache: cache.map(|cache| CacheRequest {
                role: cache.role,
                ttl: cache.ttl,
                key: cache.key,
                tail: cache.tail,
                mode: cache.mode,
            }),
            budget,
            auth: auth.map(|auth| AuthRequest {
                method: auth.method,
                account: auth.account,
            }),
            context_limit,
            work_class,
            stop_when,
            context_management,
            inherit_frozen: Default::default(),
            structured_output,
        };
        Ok(SendRequest {
            session_class,
            retention,
            stop_sequences_named: false,
            conversation_key,
            session,
            run_id,
            send_id,
            title,
            request: Box::new(submission),
            skip_materialization: one_shot == Some(true),
            kind: RunKind::FreshOrResume,
            delivery,
            plan: None,
            order,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::request::{CacheRole, CacheTail, CallOptions, ServiceTier};
    use broca_provider::families::{anthropic_frozen_config, AnthropicMessages};
    use broca_provider::wire::{CacheClass, CachePolicy, CacheTtl, MarkerSyntax, WireFamily};
    use sha2::{Digest, Sha256};

    #[test]
    fn retention_reaches_admission_for_every_delivery_mode() {
        for delivery in ["queue", "steer", "interrupt"] {
            let params: SendParams = serde_json::from_value(serde_json::json!({
                "prompt": "ephemeral", "retention": 3600, "delivery": delivery,
                "model": {"provider": "mock", "model": "m"}
            }))
            .unwrap();
            let request = params
                .into_send_request(
                    BindIdentity::new("/tmp", "runner", "retention"),
                    RunId("r-retention".into()),
                )
                .unwrap();
            assert_eq!(request.retention, Some(3600), "{delivery}");
        }
    }

    fn assert_send_request_model_variant_golden(
        file: &str,
        expected_hash: &str,
        delivery: Option<&str>,
    ) {
        let path = format!(
            "{}/../broca-wire/tests/golden/{file}",
            env!("CARGO_MANIFEST_DIR")
        );
        let bytes = std::fs::read(path).unwrap();
        let digest = format!("{:x}", Sha256::digest(&bytes));
        assert_eq!(digest, expected_hash, "golden bytes changed: {file}");
        let params: SendParams = serde_json::from_slice(&bytes).unwrap();
        let request = params
            .into_send_request(
                BindIdentity::new("/tmp", "runner", "variant"),
                RunId("r-variant".into()),
            )
            .unwrap();
        let model = request.request.model;
        assert_eq!(model.provider_module_id, "openai");
        assert_eq!(model.model_id, "gpt-6.1-sol");
        assert_eq!(request.request.variant.as_deref(), Some("high"));
        if delivery.is_some() {
            assert_eq!(request.delivery, broca_protocol::Delivery::Steer);
        }
    }

    #[test]
    fn send_request_model_variant_goldens_convert_into_variant_requests() {
        assert_send_request_model_variant_golden(
            "send_request.model_variant_queue.json",
            "5510a982218491d1c40660c4a4ddb1399906795c32dd7a4a09273a2f7ee616ee",
            None,
        );
        assert_send_request_model_variant_golden(
            "send_request.model_variant_steer.json",
            "21c6af3bfbf98e6b32ca6499e0870c3bbdf8a0f351c9e99d8eccaf199ffcb1ba",
            Some("steer"),
        );
    }

    /// A send's title rides beside the request, never inside it. The golden fixture request with
    /// a title and the same request without one convert into coordinator requests whose
    /// submission payloads (what is rendered, routed and queued) and send-identity digests are
    /// byte-identical; only the titled one carries the title. So a title never reaches a provider
    /// request, and a resend whose title differs is not refused as `send_id_reuse`.
    #[test]
    fn a_title_converts_beside_the_request_and_leaves_its_identity_unchanged() {
        let path = format!(
            "{}/../broca-wire/tests/golden/send_request.title.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let bytes = std::fs::read(path).unwrap();
        assert_eq!(
            format!("{:x}", Sha256::digest(&bytes)),
            "51c0652eba5b5a8bbe59d66908f11af7b4415138514613045efdb67132cd8da7",
            "golden bytes changed: send_request.title.json"
        );
        let mut untitled: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        untitled.as_object_mut().unwrap().remove("title");
        let convert = |params: SendParams| {
            params
                .into_send_request(
                    BindIdentity::new("/tmp", "runner", "titled"),
                    RunId("r-titled".into()),
                )
                .unwrap()
        };
        let titled = convert(serde_json::from_slice(&bytes).unwrap());
        let untitled = convert(serde_json::from_value(untitled).unwrap());
        assert_eq!(titled.title.as_deref(), Some("Fix the login bug"));
        assert_eq!(untitled.title, None);
        assert_eq!(
            serde_json::to_vec(&titled.request).unwrap(),
            serde_json::to_vec(&untitled.request).unwrap()
        );
        for delivery in [
            broca_protocol::Delivery::Queue,
            broca_protocol::Delivery::Steer,
            broca_protocol::Delivery::Interrupt,
        ] {
            assert_eq!(
                broca_core::send_identity_digest_with_conversation_key(
                    &titled.request,
                    delivery,
                    None
                ),
                broca_core::send_identity_digest_with_conversation_key(
                    &untitled.request,
                    delivery,
                    None
                )
            );
        }
    }

    fn make_send_params(system: Option<String>, prompt: &str) -> SendParams {
        SendParams {
            directives: None,
            session_class: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            retention: None,
            conversation_key: None,
            service_tier: None,
            prompt: prompt.to_string(),
            prompt_blocks: None,
            system,
            send_id: None,
            model: Some(ModelParams {
                provider: "mock".into(),
                model: "m".into(),
                variant: None,
            }),
            tools: Some(vec![]),
            tool_choice: Default::default(),
            generation: Default::default(),
            stop_when: vec![],
            cache: None,
            budget: None,
            structured_output: None,
            auth: None,
            context_limit: None,
            work_class: None,
            one_shot: None,
            delivery: None,
            dedupe_key: None,
            expires_at_ms: None,
            template: None,
            append_episode: false,
            context_management: None,
        }
    }

    #[test]
    fn send_params_map_cache_preferences_to_run_request() {
        let mut params = make_send_params(None, "hello");
        params.cache = Some(CacheParams {
            role: Some(CacheRole::Ephemeral),
            ttl: Some("5m".into()),
            key: Some("alfonso-classify-v1".into()),
            tail: Some(CacheTail::Volatile),
            mode: None,
        });
        let request = params
            .into_send_request(
                BindIdentity::new("/tmp/proj", "runner", "s1"),
                RunId("run-cache".into()),
            )
            .unwrap();
        let cache = request.request.cache.expect("cache request is carried");
        assert_eq!(cache.role, Some(CacheRole::Ephemeral));
        assert_eq!(cache.ttl.as_deref(), Some("5m"));
        assert_eq!(cache.key.as_deref(), Some("alfonso-classify-v1"));
        assert_eq!(cache.tail, Some(CacheTail::Volatile));
        assert_eq!(cache.mode, None, "absent mode stays absent");
    }

    #[test]
    fn send_params_carry_an_explicit_cache_opt_out() {
        let params: SendParams = serde_json::from_value(serde_json::json!({
            "prompt": "hello",
            "model": {"provider": "anthropic", "model": "claude-opus-4-8"},
            "cache": {"mode": "none"}
        }))
        .unwrap();
        let request = params
            .into_send_request(
                BindIdentity::new("/tmp/proj", "runner", "s1"),
                RunId("run-no-cache".into()),
            )
            .unwrap();
        let cache = request.request.cache.expect("cache request is carried");
        assert_eq!(cache.mode, Some(broca_protocol::request::CacheMode::None));
        assert!(
            serde_json::from_value::<SendParams>(serde_json::json!({
                "prompt": "hello",
                "model": {"provider": "anthropic", "model": "claude-opus-4-8"},
                "cache": {"mode": "off"}
            }))
            .is_err(),
            "an unknown mode is refused rather than read as no request"
        );
    }

    #[test]
    fn only_caller_send_ids_enable_durable_idempotency_lookup() {
        let session = BindIdentity::new("/tmp/proj", "runner", "s1");
        let nonce = make_send_params(None, "hello")
            .into_send_request(session.clone(), RunId("run-nonce".into()))
            .unwrap();
        assert_eq!(nonce.send_id, None);

        let mut keyed = make_send_params(None, "hello");
        keyed.send_id = Some("logical-send-1".into());
        let keyed = keyed
            .into_send_request(session, RunId("run-keyed".into()))
            .unwrap();
        assert_eq!(keyed.send_id.as_deref(), Some("logical-send-1"));

        let defaulted: SendParams = serde_json::from_value(serde_json::json!({
            "prompt": "hello",
            "model": {"provider": "mock", "model": "m"}
        }))
        .expect("an older payload without one_shot still deserializes");
        assert_eq!(defaulted.one_shot, None);
        let defaulted = defaulted
            .into_send_request(
                BindIdentity::new("/tmp/proj", "runner", "defaulted"),
                RunId("run-defaulted".into()),
            )
            .unwrap();
        assert!(!defaulted.skip_materialization);

        let mut one_shot = make_send_params(None, "hello");
        one_shot.one_shot = Some(true);
        let one_shot = one_shot
            .into_send_request(
                BindIdentity::new("/tmp/proj", "runner", "one-shot"),
                RunId("run-one-shot".into()),
            )
            .unwrap();
        assert!(one_shot.skip_materialization);

        let mut explicit_false = make_send_params(None, "hello");
        explicit_false.one_shot = Some(false);
        let explicit_false = explicit_false
            .into_send_request(
                BindIdentity::new("/tmp/proj", "runner", "explicit-false"),
                RunId("run-explicit-false".into()),
            )
            .unwrap();
        assert!(!explicit_false.skip_materialization);
    }

    #[test]
    fn send_params_prepend_only_non_blank_system_prompts() {
        let system = "  system α\nline δύο 🧪  ";
        let request = make_send_params(Some(system.into()), "user prompt")
            .into_send_request(
                BindIdentity::new("/tmp/proj", "runner", "s1"),
                RunId("run-1".into()),
            )
            .unwrap();
        assert_eq!(request.request.input[0].role, Role::System);
        assert_eq!(request.request.input[1].role, Role::User);

        for blank in ["", " \n\t "] {
            let request = make_send_params(Some(blank.into()), "user prompt")
                .into_send_request(
                    BindIdentity::new("/tmp/proj", "runner", "s1"),
                    RunId("run-2".into()),
                )
                .unwrap();
            assert_eq!(request.request.input.len(), 1);
            assert_eq!(request.request.input[0].role, Role::User);
        }
    }

    #[test]
    fn send_params_pass_auth_pin_through_without_parsing_the_method() {
        let mut params = make_send_params(None, "hello");
        params.auth = Some(AuthParams {
            method: "api-key".into(),
            account: Some("astro".into()),
        });

        let request = params
            .into_send_request(BindIdentity::new("/tmp", "h", "s"), RunId("r".into()))
            .unwrap();
        assert_eq!(
            request.request.auth,
            Some(AuthRequest {
                method: "api-key".into(),
                account: Some("astro".into()),
            })
        );
    }

    #[test]
    fn send_params_pass_context_limit_through() {
        let params: SendParams = serde_json::from_value(serde_json::json!({
            "prompt": "hello",
            "model": {"provider": "stub", "model": "m"},
            "context_limit": 64_000
        }))
        .unwrap();
        let request = params
            .into_send_request(
                BindIdentity::new("/tmp", "runner", "context-limit"),
                RunId("r-context-limit".into()),
            )
            .unwrap();
        assert_eq!(request.request.context_limit, Some(64_000));
    }

    #[test]
    fn send_params_pass_work_class_through_verbatim() {
        let params: SendParams = serde_json::from_value(serde_json::json!({
            "prompt": "hello",
            "model": {"provider": "stub", "model": "m"},
            "work_class": "mason"
        }))
        .unwrap();
        let request = params
            .into_send_request(
                BindIdentity::new("/tmp", "runner", "work-class"),
                RunId("r-work-class".into()),
            )
            .unwrap();
        assert_eq!(request.request.work_class.as_deref(), Some("mason"));
    }

    #[test]
    fn send_params_carry_model_variant_beside_the_model_handle() {
        let params: SendParams = serde_json::from_value(serde_json::json!({
            "prompt": "hello",
            "model": {"provider": "openai", "model": "gpt-6-astra", "variant": "xhigh"}
        }))
        .unwrap();
        let request = params
            .into_send_request(
                BindIdentity::new("/tmp", "runner", "variant"),
                RunId("r-variant".into()),
            )
            .expect("a variant send is admitted, not refused");
        assert_eq!(request.request.variant.as_deref(), Some("xhigh"));
        // The handle is the model identity resume compares; the variant never joins it.
        assert_eq!(
            request.request.model,
            ModelHandle::new("openai", "gpt-6-astra")
        );
    }

    #[test]
    fn send_params_carry_the_service_tier_to_the_submission() {
        for (wire, tier) in [
            ("flex", ServiceTier::Flex),
            ("default", ServiceTier::Default),
            ("priority", ServiceTier::Priority),
        ] {
            let params: SendParams = serde_json::from_value(serde_json::json!({
                "prompt": "hello",
                "model": {"provider": "openai", "model": "gpt-5.6"},
                "service_tier": wire,
            }))
            .unwrap();
            let request = params
                .into_send_request(
                    BindIdentity::new("/tmp", "runner", "tier"),
                    RunId("r-tier".into()),
                )
                .unwrap();
            assert_eq!(request.request.service_tier, Some(tier));
        }
        // Absent and null both mean the provider's default: nothing is requested.
        for params in [
            serde_json::json!({"prompt": "hello", "model": {"provider": "openai", "model": "m"}}),
            serde_json::json!({"prompt": "hello", "model": {"provider": "openai", "model": "m"}, "service_tier": null}),
        ] {
            let params: SendParams = serde_json::from_value(params).unwrap();
            assert_eq!(params.service_tier, None);
        }
    }

    /// `fast` is OpenAI's marketing name for priority. It is refused like any unknown value, and
    /// the refusal names the field and the three accepted spellings, so the caller learns the
    /// vocabulary rather than guessing at an alias.
    #[test]
    fn service_tier_fast_is_refused_naming_the_three_tiers() {
        for bad in [
            serde_json::json!("fast"),
            serde_json::json!("Priority"),
            serde_json::json!("auto"),
            serde_json::json!("standard"),
            serde_json::json!(1),
        ] {
            let error = serde_json::from_value::<SendParams>(serde_json::json!({
                "prompt": "hello",
                "model": {"provider": "openai", "model": "gpt-5.6"},
                "service_tier": bad,
            }))
            .expect_err("an unknown service tier must be refused")
            .to_string();
            assert!(
                error.contains("service_tier must be \"flex\", \"default\" or \"priority\""),
                "refusal for {bad} must name the three tiers: {error}"
            );
        }
    }

    #[test]
    fn prompt_blocks_render_as_two_blocks_in_one_user_message() {
        let mut params = make_send_params(None, "");
        params.prompt_blocks = Some(vec!["head".into(), "tail".into()]);

        let request = params
            .into_send_request(BindIdentity::new("/tmp", "h", "s"), RunId("r".into()))
            .unwrap();
        assert_eq!(request.request.input.len(), 1);
        let user = &request.request.input[0];
        assert_eq!(user.role, Role::User);
        assert_eq!(
            user.content,
            vec![
                ContentBlock::Text {
                    text: "head".into()
                },
                ContentBlock::Text {
                    text: "tail".into()
                }
            ]
        );
        assert_eq!(user.cache_prefix_blocks, Some(1));
    }

    #[test]
    fn prompt_alone_still_renders_one_user_text_block() {
        let request = make_send_params(None, "legacy prompt")
            .into_send_request(BindIdentity::new("/tmp", "h", "s"), RunId("r".into()))
            .unwrap();
        assert_eq!(request.request.input.len(), 1);
        assert_eq!(
            request.request.input[0].content,
            vec![ContentBlock::Text {
                text: "legacy prompt".into()
            }]
        );
        assert_eq!(request.request.input[0].cache_prefix_blocks, None);
    }

    fn config_error(params: SendParams) -> String {
        match params.into_send_request(BindIdentity::new("/tmp", "h", "s"), RunId("invalid".into()))
        {
            Err(broca_core::RunError::Config(message)) => message,
            Err(error) => panic!("expected typed Config refusal, got {error}"),
            Ok(_) => panic!("expected typed Config refusal, got a request"),
        }
    }

    #[test]
    fn non_empty_prompt_and_prompt_blocks_are_refused_as_config() {
        let mut params = make_send_params(None, "legacy");
        params.prompt_blocks = Some(vec!["head".into(), "tail".into()]);
        let message = config_error(params);
        assert!(message.contains("both non-empty prompt and prompt_blocks"));
    }

    #[test]
    fn missing_prompt_and_prompt_blocks_are_refused_as_config() {
        let params = make_send_params(None, "");
        let message = config_error(params);
        assert!(message.contains("neither prompt nor prompt_blocks"));
    }

    #[test]
    fn prompt_blocks_with_fewer_than_two_elements_are_refused_as_config() {
        let mut params = make_send_params(None, "");
        params.prompt_blocks = Some(vec!["only".into()]);
        let message = config_error(params);
        assert!(message.contains("prompt_blocks with 1 element(s)"));
        assert!(message.contains("at least 2"));
    }

    #[test]
    fn blank_prompt_block_is_refused_as_config_and_names_its_index() {
        let mut params = make_send_params(None, "");
        params.prompt_blocks = Some(vec!["head".into(), " \n\t".into()]);
        let message = config_error(params);
        assert!(message.contains("whitespace-only prompt_blocks[1]"));
    }

    #[test]
    fn prompt_block_cache_boundary_marks_only_the_shared_anthropic_block() {
        let mut params = make_send_params(None, "");
        params.prompt_blocks = Some(vec!["invariant head".into(), "varying tail".into()]);
        params.one_shot = Some(true);
        params.cache = Some(CacheParams {
            role: None,
            ttl: None,
            key: Some("comment-review-3f9a1c2b7d5e0a91".into()),
            tail: None,
            mode: None,
        });
        let request = params
            .into_send_request(BindIdentity::new("/tmp", "h", "s"), RunId("r".into()))
            .unwrap();
        assert!(request.skip_materialization);
        let mut frozen = anthropic_frozen_config("claude-opus-4-8");
        let cache_policy = crate::cache_routing::resolve_cache_policy(
            broca_catalog::live::WireFamilyId::AnthropicMessages,
            "anthropic",
            request.request.cache.as_ref(),
            &crate::cache_routing::CacheTtlTable::shipped_defaults(),
            &request.session,
        )
        .unwrap();
        match &cache_policy
            .as_ref()
            .expect("the key-only request gets first-party Anthropic defaults")
            .class
        {
            CacheClass::Markers { ttl, omit_ttl, .. } => {
                assert_eq!(*ttl, CacheTtl::FiveMinutes);
                assert!(
                    *omit_ttl,
                    "the default five-minute TTL is omitted on the wire"
                );
            }
            other => panic!("expected Anthropic markers, got {other:?}"),
        }
        frozen.cache = cache_policy;

        let body = AnthropicMessages.shape(&frozen, &CallOptions::new(request.request.input));
        let blocks = body["messages"][0]["content"].as_array().unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0]["text"], "invariant head");
        assert_eq!(
            blocks[0]["cache_control"],
            serde_json::json!({"type": "ephemeral"})
        );
        assert_eq!(blocks[1]["text"], "varying tail");
        assert!(blocks[1].get("cache_control").is_none());

        fn count_cache_control(value: &serde_json::Value) -> usize {
            match value {
                serde_json::Value::Object(fields) => {
                    usize::from(fields.contains_key("cache_control"))
                        + fields.values().map(count_cache_control).sum::<usize>()
                }
                serde_json::Value::Array(values) => values.iter().map(count_cache_control).sum(),
                _ => 0,
            }
        }
        assert_eq!(count_cache_control(&body), 1);
    }

    #[test]
    fn queued_prompt_block_boundary_renders_identically_after_durable_round_trip() {
        let mut params = make_send_params(None, "");
        params.prompt_blocks = Some(vec!["invariant head".into(), "varying tail".into()]);
        let request = params
            .into_send_request(BindIdentity::new("/tmp", "h", "s"), RunId("r".into()))
            .unwrap();
        let submission = *request.request;
        let mut frozen = anthropic_frozen_config("claude-opus-4-8");
        frozen.cache = Some(CachePolicy {
            version: 1,
            class: CacheClass::Markers {
                syntax: MarkerSyntax::AnthropicCacheControl,
                ttl: CacheTtl::FiveMinutes,
                max_breakpoints: 4,
                lookback_blocks: 20,
                tail: None,
                omit_ttl: false,
            },
        });
        let fresh = AnthropicMessages.shape(&frozen, &CallOptions::new(submission.input.clone()));
        let queued = broca_wal::WalRecord::Queued {
            retention: None,
            ts_ms: None,
            hooks: None,
            title: None,
            keep_warm: None,
            conversation_key: None,
            send_id: None,
            delivery: None,
            submission_id: broca_protocol::SubmissionId("queued".into()),
            request: Box::new(submission),
            request_identity_digest: None,
            principal: None,
        };
        let durable_bytes = serde_json::to_vec(&queued).unwrap();
        let replayed: broca_wal::WalRecord = serde_json::from_slice(&durable_bytes).unwrap();
        let drained = broca_core::pending_submissions(&[replayed])
            .pop()
            .expect("queued request survives replay");
        let after_drain =
            AnthropicMessages.shape(&frozen, &CallOptions::new(drained.request.input));

        assert_eq!(after_drain, fresh);
    }

    #[test]
    fn send_params_pass_budget_through() {
        let params: SendParams = serde_json::from_value(serde_json::json!({
            "prompt": "hello",
            "model": {"provider": "stub", "model": "m"},
            "budget": {
                "nudges": [{"at_tool_calls": 20, "text": "slow down"}],
                "finalize_at_tool_calls": 30,
                "finalize_text": "answer now"
            }
        }))
        .unwrap();
        let request = params
            .into_send_request(
                BindIdentity::new("/tmp", "runner", "budget"),
                RunId("r-budget".into()),
            )
            .unwrap();
        let budget = request.request.budget.unwrap();
        assert_eq!(budget.nudges[0].at_tool_calls, 20);
        assert_eq!(budget.finalize_at_tool_calls, 30);
    }
}
